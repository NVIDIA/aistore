//go:build aws

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
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

// TODO:
// - the schema<=>entry correspondence can be made more flexible (is partially hardcoded)
// - handle partial inventory get+unzip download (w/ subsequent EOF or worse)
// - the offset must correspond to the previously returned ContinuationToken ====> recover or fail?
// - cleanup older inventories
// - cached inventory must be stored with its own content-type (or, might disappear during pagination)

const (
	invSizeSGL = cos.MiB
	invMaxPage = 8 * apc.MaxPageSizeAWS // roughly, 2MB given 256B lines
)

const (
	invName     = ".inventory"
	invSrcExt   = ".csv.gz"
	invDstExt   = ".csv"
	invManifest = "manifest.json"
	invSchema   = "fileSchema"
)

type invT struct {
	oname string
	mtime time.Time
}

func _errInv(tag string, err error) (string, error) {
	return "", fmt.Errorf("bucket-inventory: %s: %w", tag, err)
}

func sinceInv(t1, t2 time.Time) time.Duration {
	if t1.After(t2) {
		return t1.Sub(t2)
	}
	return t2.Sub(t1)
}

func prefixInv(cloudBck *cmn.Bck, ctx *core.LsoInventoryCtx) (prefix string) {
	prefix = cos.Either(ctx.Name, invName) + cos.PathSeparator + cloudBck.Name
	if ctx.ID != "" {
		prefix += cos.PathSeparator + ctx.ID
	}
	return prefix
}

func checkInventory(cloudBck *cmn.Bck, latest time.Time, ctx *core.LsoInventoryCtx) (fqn string, _ bool, _ error) {
	prefix := prefixInv(cloudBck, ctx)
	mi, _, err := fs.Hrw(prefix)
	if err != nil {
		return "", false, err
	}
	// one bucket, one inventory - and one statically defined name
	fqn = mi.MakePathFQN(cloudBck, fs.WorkfileType, prefix) + invDstExt
	if finfo, err := os.Stat(fqn); err == nil {
		if ctx.Offset > 0 {
			debug.Assert(latest.IsZero())
			// keep using the one
			return fqn, true, nil
		}
		if sinceInv(finfo.ModTime(), latest) < 4*time.Second { // allow for a few seconds difference
			debug.Assert(ctx.Size == 0 || ctx.Size == finfo.Size())
			ctx.Size = finfo.Size()
			// start using the one
			return fqn, true, nil
		}
		nlog.Infoln("Warning: updating bucket inventory", prefix, finfo.ModTime(), latest)
	} else {
		nlog.Infoln("Warning: getting bucket inventory ...", prefix, latest)
	}
	return fqn, false, nil
}

func (awsp *awsProvider) getManifest(cloudBck *cmn.Bck, svc *s3.Client, oname string) (fileSchema string, _ error) {
	input := s3.GetObjectInput{Bucket: aws.String(cloudBck.Name), Key: aws.String(oname)}
	obj, err := svc.GetObject(context.Background(), &input)
	if err != nil {
		return "", err
	}

	sgl := awsp.t.PageMM().NewSGL(0)
	_, err = io.Copy(sgl, obj.Body)
	cos.Close(obj.Body)

	if err != nil {
		sgl.Free()
		return fileSchema, err
	}

	lbuf := make([]byte, 256)
	for {
		lbuf, err = sgl.ReadLine(lbuf) // reuse
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
		if len(lbuf) < len(invSchema)+10 {
			continue
		}
		line := strings.Split(string(lbuf), ":")
		if len(line) < 2 {
			continue
		}
		if strings.Contains(line[0], invSchema) {
			s := strings.TrimSpace(line[1])
			fileSchema = cmn.UnquoteCEV(strings.TrimSuffix(s, ","))
			break
		}
	}
	sgl.Free()

	if err == nil && fileSchema == "" {
		err = errors.New("failed to parse '" + invSchema + "' of the " + oname)
	}
	return fileSchema, err
}

func (awsp *awsProvider) getInventory(cloudBck *cmn.Bck, svc *s3.Client, ctx *core.LsoInventoryCtx) (string, error) {
	var (
		csv      invT
		manifest invT
		bn       = aws.String(cloudBck.Name)
		params   = &s3.ListObjectsV2Input{Bucket: bn}
		prefix   = prefixInv(cloudBck, ctx)
	)
	params.Prefix = aws.String(prefix)
	params.MaxKeys = aws.Int32(apc.MaxPageSizeAWS) // no more than 1000 manifests

	//
	// 1. ls inventory
	//
	resp, err := svc.ListObjectsV2(context.Background(), params)
	if err != nil {
		return "", err
	}
	for _, obj := range resp.Contents {
		name := *obj.Key
		if cos.Ext(name) == invSrcExt {
			mtime := *(obj.LastModified)
			if csv.mtime.IsZero() || mtime.After(csv.mtime) {
				csv.mtime = mtime
				csv.oname = name
			}
			continue
		}
		if filepath.Base(name) == invManifest {
			mtime := *(obj.LastModified)
			if manifest.mtime.IsZero() || mtime.After(manifest.mtime) {
				manifest.mtime = mtime
				manifest.oname = name
			}
		}
	}
	if csv.oname == "" {
		what := prefix
		if ctx.ID == "" {
			what = cos.Either(ctx.Name, invName)
		}
		return "", cos.NewErrNotFound(cloudBck, "S3 bucket inventory '"+what+"'")
	}

	//
	// 2. read the manifest and extract `fileSchema`
	//
	fileSchema, err := awsp.getManifest(cloudBck, svc, manifest.oname)
	if err != nil {
		return "", err
	}

	// e.g. [Bucket Key Size ETag]
	schema := strings.Split(fileSchema, ", ")
	debug.Assert(len(schema) > 1, schema)       // expecting always
	debug.Assert(schema[0] == "Bucket", schema) // ditto
	debug.Assert(schema[1] == "Key", schema)
	ctx.Schema = schema

	//
	// 3. optionally, cleanup older inventories
	//
	for _, obj := range resp.Contents {
		name := *obj.Key
		mtime := *(obj.LastModified)
		if name == csv.oname || csv.mtime.Sub(mtime) < 23*time.Hour {
			continue
		}
		if _, errN := svc.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: bn, Key: obj.Key}); errN != nil {
			nlog.Errorln("delete", name, errN)
		}
	}

	fqn, usable, err := checkInventory(cloudBck, csv.mtime, ctx)
	if err != nil || usable {
		return fqn, err
	}

	//
	// 4. get+unzip and save locally
	//
	input := s3.GetObjectInput{Bucket: aws.String(cloudBck.Name), Key: aws.String(csv.oname)}
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

	if err = os.Chtimes(fqn, csv.mtime, csv.mtime); err != nil {
		return _errInv("chtimes", err)
	}

	nlog.Infoln("new bucket inventory:", *ctx, fqn)
	return fqn, nil
}

func (*awsProvider) listInventory(cloudBck *cmn.Bck, fh *os.File, sgl *memsys.SGL, ctx *core.LsoInventoryCtx, msg *apc.LsoMsg,
	lst *cmn.LsoResult) error {
	msg.PageSize = calcPageSize(msg.PageSize, invMaxPage)
	for j := len(lst.Entries); j < int(msg.PageSize); j++ {
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
		i    int64
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
		debug.Assertf(strings.Contains(line[0], cloudBck.Name), "%q %d", line, ctx.Offset)

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

		if len(ctx.Schema) > 2 && ctx.Schema[2] == "Size" {
			size := cmn.UnquoteCEV(line[2])
			entry.Size, err = strconv.ParseInt(size, 10, 64)
			if err != nil {
				return err
			}
		}
		if msg.IsFlagSet(apc.LsNameOnly) || msg.IsFlagSet(apc.LsNameSize) || len(ctx.Schema) < 4 {
			continue
		}
		if msg.WantProp(apc.GetPropsCustom) && ctx.Schema[3] == "ETag" {
			custom := cos.StrKVs{cmn.ETag: cmn.UnquoteCEV(line[3])}
			entry.Custom = cmn.CustomMD2S(custom)
		}
	}

	lst.Entries = lst.Entries[:i]
	return err
}
