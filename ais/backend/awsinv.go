//go:build aws

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	invSrcExt = ".csv.gz"
	invDstExt = ".csv"
)

func (awsp *awsProvider) getInventory(cloudBck *cmn.Bck, svc *s3.Client) (string, error) {
	var (
		latest time.Time
		found  string
		params = &s3.ListObjectsV2Input{Bucket: aws.String(cloudBck.Name)}
		prefix = path.Join(env.BucketInventory(), cloudBck.Name) // env "S3_BUCKET_INVENTORY" or const
	)
	params.Prefix = aws.String(prefix)
	params.MaxKeys = aws.Int32(apc.MaxPageSizeAWS)

	// 1. ls inventory
	resp, err := svc.ListObjectsV2(context.Background(), params)
	if err != nil {
		return "", err
	}
	for _, obj := range resp.Contents {
		name := *obj.Key
		if cos.Ext(name) != invSrcExt {
			continue
		}
		mtime := *(obj.LastModified)
		if mtime.After(latest) {
			latest = mtime
			found = name
		}
	}
	if found == "" {
		return "", cos.NewErrNotFound(cloudBck, "S3 bucket inventory")
	}

	// 2. one bucket, one inventory (and one statically produced name)
	mi, _, err := fs.Hrw(prefix)
	if err != nil {
		return "", err
	}
	fqn := mi.MakePathFQN(cloudBck, fs.WorkfileType, prefix) + invDstExt
	if cos.Stat(fqn) == nil {
		return fqn, nil
	}

	// 3. get and save unzipped locally
	input := s3.GetObjectInput{Bucket: aws.String(cloudBck.Name), Key: aws.String(found)}
	obj, err := svc.GetObject(context.Background(), &input)
	if err != nil {
		return "", err
	}
	defer cos.Close(obj.Body)
	gzr, err := gzip.NewReader(obj.Body)
	if err != nil {
		return _errInv(err)
	}
	if err = cos.CreateDir(filepath.Dir(fqn)); err != nil {
		gzr.Close()
		return _errInv(err)
	}
	wfh, err := os.OpenFile(fqn, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, cos.PermRWR)
	if err != nil {
		return _errInv(err)
	}
	size := *obj.ContentLength
	buf, slab := awsp.t.PageMM().AllocSize(min(size, 64*cos.KiB))
	_, err = cos.CopyBuffer(wfh, gzr, buf)
	slab.Free(buf)
	wfh.Close()

	if err != nil {
		return _errInv(err)
	}
	err = gzr.Close()
	debug.AssertNoErr(err)

	// 4. cleanup older inventories
	b := aws.String(cloudBck.Name)
	for _, obj := range resp.Contents {
		name := *obj.Key
		mtime := *(obj.LastModified)
		if name == found || latest.Sub(mtime) < 23*time.Hour {
			continue
		}
		_, errN := svc.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: b, Key: obj.Key})
		if errN != nil {
			nlog.Errorln(name, errN)
		}
	}

	return fqn, nil
}

func _errInv(err error) (string, error) {
	return "", fmt.Errorf("bucket-inventory: %w", err)
}

// TODO -- FIXME:
// 1. len(line) == 4: expecting strictly (bucket, objname, size, etag) - use manifests
// 2. reuse SGL across list-page calls
// 3. restrict SGL growth - read part fh and wrap around
func (*awsProvider) listInventory(cloudBck *cmn.Bck, fh *os.File, sgl *memsys.SGL, msg *apc.LsoMsg, lst *cmn.LsoResult) (err error) {
	var (
		i    uint
		skip = msg.ContinuationToken != ""
		lbuf = make([]byte, 128)
	)
	msg.PageSize = calcPageSize(msg.PageSize, apc.MaxPageSizeAIS /* 10k */)
	for j := uint(len(lst.Entries)); j < msg.PageSize; j++ {
		lst.Entries = append(lst.Entries, &cmn.LsoEntry{})
	}
	if _, err := io.Copy(sgl, fh); err != nil {
		return err
	}
	for {
		lbuf, err = sgl.ReadLine(lbuf)
		if err != nil {
			break
		}
		line := strings.Split(string(lbuf), ",")
		debug.Assert(len(line) == 4)
		debug.Assert(strings.Contains(line[0], cloudBck.Name))

		objName := cmn.UnquoteCEV(line[1])
		if msg.Prefix != "" && !strings.HasPrefix(objName, msg.Prefix) {
			continue
		}
		if skip && objName == msg.ContinuationToken {
			skip = false
		}
		if skip {
			continue
		}

		// have page?
		if i >= msg.PageSize {
			lst.ContinuationToken = objName
			break
		}

		// next entry
		entry := lst.Entries[i]
		i++
		entry.Name = objName
		size := cmn.UnquoteCEV(line[2])
		entry.Size, err = strconv.ParseInt(size, 10, 64)
		if err != nil {
			return err
		}
		if msg.IsFlagSet(apc.LsNameOnly) || msg.IsFlagSet(apc.LsNameSize) {
			continue
		}
		if msg.WantProp(apc.GetPropsCustom) {
			custom := cos.StrKVs{cmn.ETag: cmn.UnquoteCEV(line[3])}
			entry.Custom = cmn.CustomMD2S(custom)
		}
	}
	lst.Entries = lst.Entries[:i]
	if err == io.EOF {
		err = nil
	}
	return err
}
