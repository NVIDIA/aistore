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
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TODO -- FIXME:
// 1. len(line) == 4: expecting strictly (bucket, objname, size, etag) - use manifests
// 2: the offset must correspond to the previously returned ContinuationToken
// 3: cleanup older inventories
// 4: cached inventory must be stored with its own content-type (or, might disappear during pagination)
// 5: opt-out ListObjectsV2 when ctx.Offset > 0

const (
	invSizeSGL = cos.MiB
	invMaxPage = 8 * apc.MaxPageSizeAWS // roughly, 2MB given 256B lines
)

const (
	invSrcExt = ".csv.gz"
	invDstExt = ".csv"
)

func _errInv(tag string, err error) (string, error) {
	return "", fmt.Errorf("bucket-inventory: %s: %w", tag, err)
}

func sinceInv(t1, t2 time.Time) time.Duration {
	if t1.After(t2) {
		return t1.Sub(t2)
	}
	return t2.Sub(t1)
}

func (awsp *awsProvider) getInventory(cloudBck *cmn.Bck, svc *s3.Client, ctx *core.LsoInventoryCtx) (string, error) {
	var (
		latest time.Time
		found  string
		bn     = aws.String(cloudBck.Name)
		params = &s3.ListObjectsV2Input{Bucket: bn}
		prefix = path.Join(env.BucketInventory(), cloudBck.Name) // env "S3_BUCKET_INVENTORY" or const
	)
	params.Prefix = aws.String(prefix)
	params.MaxKeys = aws.Int32(apc.MaxPageSizeAWS) // no more than 1000 manifests

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
	// 1.1. optionally, cleanup older inventories
	for _, obj := range resp.Contents {
		name := *obj.Key
		mtime := *(obj.LastModified)
		if name == found || latest.Sub(mtime) < 23*time.Hour {
			continue
		}
		if _, errN := svc.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: bn, Key: obj.Key}); errN != nil {
			nlog.Errorln("delete", name, errN)
		}
	}

	// 2. one bucket, one inventory (and one statically produced name)
	mi, _, err := fs.Hrw(prefix)
	if err != nil {
		return "", err
	}
	fqn := mi.MakePathFQN(cloudBck, fs.WorkfileType, prefix) + invDstExt
	if finfo, err := os.Stat(fqn); err == nil {
		if sinceInv(finfo.ModTime(), latest) < 4*time.Second { // allow for a few seconds difference
			debug.Assert(ctx.Size == 0 || ctx.Size == finfo.Size())
			ctx.Size = finfo.Size()
			return fqn, nil
		}
		nlog.Infoln("Warning: updating bucket inventory", prefix, finfo.ModTime(), latest)
	} else {
		nlog.Infoln("Warning: getting bucket inventory ...", prefix, latest)
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
		return _errInv("gzip", err)
	}
	if err = cos.CreateDir(filepath.Dir(fqn)); err != nil {
		gzr.Close()
		return _errInv("create-dir", err)
	}
	wfh, err := os.OpenFile(fqn, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, cos.PermRWR)
	if err != nil {
		return _errInv("wopen", err)
	}
	buf, slab := awsp.t.PageMM().AllocSize(min(*obj.ContentLength*3, 64*cos.KiB)) // wrt "uncompressed"
	ctx.Size, err = cos.CopyBuffer(wfh, gzr, buf)
	slab.Free(buf)
	wfh.Close()

	if err != nil {
		return _errInv("io.copy", err)
	}
	err = gzr.Close()
	debug.AssertNoErr(err)

	if err = os.Chtimes(fqn, latest, latest); err != nil {
		return _errInv("chtimes", err)
	}

	nlog.Infoln("new bucket inventory:", *ctx, fqn)
	return fqn, nil
}

func (*awsProvider) listInventory(cloudBck *cmn.Bck, fh *os.File, sgl *memsys.SGL, ctx *core.LsoInventoryCtx, msg *apc.LsoMsg,
	lst *cmn.LsoResult) error {
	msg.PageSize = calcPageSize(msg.PageSize, invMaxPage)
	for j := uint(len(lst.Entries)); j < msg.PageSize; j++ {
		lst.Entries = append(lst.Entries, &cmn.LsoEntry{})
	}

	// seek to the previous offset - the starting-from offset for the next page
	if ctx.Offset > 0 {
		if _, err := fh.Seek(ctx.Offset, io.SeekStart); err != nil {
			lst.Entries = lst.Entries[:0]
			return err
		}
	}

	// NOTE: upper limit hardcoded (assuming enough space to hold msg.PageSize)
	if written, err := io.CopyN(sgl, fh, invSizeSGL-cos.KiB); err != nil || written == 0 {
		lst.Entries = lst.Entries[:0]
		return err
	}

	var (
		i    uint
		off  = ctx.Offset
		skip = msg.ContinuationToken != ""
		lbuf = make([]byte, 256)
		err  error
	)
	for {
		ctx.Offset = off + sgl.Roff()  // advance
		lbuf, err = sgl.ReadLine(lbuf) // reuse
		if err != nil {
			break
		}
		line := strings.Split(string(lbuf), ",")
		debug.Assertf(len(line) == 4 && strings.Contains(line[0], cloudBck.Name), "%q %d", line, ctx.Offset)

		objName := cmn.UnquoteCEV(line[1])
		if msg.Prefix != "" && !strings.HasPrefix(objName, msg.Prefix) {
			continue
		}
		if skip && off > 0 {
			// expecting fseek to position precisely at the next
			debug.Assert(objName == msg.ContinuationToken, objName, " vs ", msg.ContinuationToken)
			// TODO -- FIXME: recover
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
	return err
}
