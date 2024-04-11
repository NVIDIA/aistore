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
	"net/http"
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
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Main assumption/requirement:
// one bucket, one inventory (for this same bucket), and one statically defined cached FQN.csv

// TODO:
// - use blob downloader (****)
// - handle partial inventory get+unzip download (w/ subsequent EOF or worse)
// - cached inventory must be stored with its own content-type (or, might disappear during pagination)
// separately:
// - cleanup older inventories

const (
	invSizeSGL = cos.MiB
	invMaxPage = 8 * apc.MaxPageSizeAWS // roughly, 2MB given 256B lines
	invMaxLine = 256
)

const (
	invName   = ".inventory"
	invSrcExt = ".csv.gz"
	invDstExt = ".csv"
)

// NOTE: hardcoding two groups of constants - cannot find any of them in https://github.com/aws/aws-sdk-go-v2
// Generally, instead of reading inventory manifest line by line (and worrying about duplicated constants)
// it'd be much nicer to have an official JSON.

const (
	invManifest = "manifest.json"
	invSchema   = "fileSchema" // e.g. "fileSchema" : "Bucket, Key, Size, ETag"
)

// canonical schema
const (
	invSchemaBucket = "Bucket" // must be the first field, always present
	invBucketPos    = 0
	invSchemaKey    = "Key" // must be the second mandatory field
	invKeyPos       = 1
)

type invT struct {
	oname string
	mtime time.Time
}

func _errInv(tag string, err error) error {
	return fmt.Errorf("bucket-inventory: %s: %w", tag, err)
}

func sinceInv(t1, t2 time.Time) time.Duration {
	if t1.After(t2) {
		return t1.Sub(t2)
	}
	return t2.Sub(t1)
}

func prefixInv(cloudBck *cmn.Bck, ctx *core.LsoInvCtx) (prefix string) {
	prefix = cos.Either(ctx.Name, invName) + cos.PathSeparator + cloudBck.Name
	if ctx.ID != "" {
		prefix += cos.PathSeparator + ctx.ID
	}
	return prefix
}

func checkInventory(cloudBck *cmn.Bck, latest time.Time, ctx *core.LsoInvCtx) (fqn string, _ bool, _ error) {
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

// NOTE: see "manifest" comment above;
// with JSON-tagged manifest structure (that'd include `json:"fileSchema"`)
// it'd then make sense to additionally validate: format == csv and source bucket == destination bucket == this bucket
func (s3bp *s3bp) getManifest(cloudBck *cmn.Bck, svc *s3.Client, oname string) (schema []string, _ int, _ error) {
	input := s3.GetObjectInput{Bucket: aws.String(cloudBck.Name), Key: aws.String(oname)}
	obj, err := svc.GetObject(context.Background(), &input)
	if err != nil {
		ecode, e := awsErrorToAISError(err, cloudBck, oname)
		return nil, ecode, e
	}

	sgl := s3bp.t.PageMM().NewSGL(0)
	_, err = io.Copy(sgl, obj.Body)
	cos.Close(obj.Body)

	if err != nil {
		sgl.Free()
		return nil, 0, err
	}

	var (
		fileSchema string
		lbuf       = make([]byte, invMaxLine)
		cname      = cloudBck.Cname(oname)
	)
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

	// parse, validate
	if err != nil || fileSchema == "" {
		err = _parseErr(cname, sgl, lbuf, err)
	} else {
		// e.g. "Bucket, Key, Size, ETag"
		schema = strings.Split(fileSchema, ", ")
		if len(schema) < 2 {
			err = _parseErr(cname, sgl, lbuf, errors.New("invalid schema '"+fileSchema+"'"))
		} else if schema[invBucketPos] != invSchemaBucket || schema[invKeyPos] != invSchemaKey {
			err = _parseErr(cname, sgl, lbuf,
				errors.New("unexpected schema '"+fileSchema+"': expecting Bucket followed by Key"))
		}
	}

	sgl.Free()
	return schema, 0, err
}

func _parseErr(cname string, sgl *memsys.SGL, lbuf []byte, err error) error {
	out := fmt.Sprintf("failed to parse %s for %q", cname, invSchema)
	if s := _bhead(sgl, lbuf); s != "" {
		out += ": [" + s + "]"
	}
	if err != nil {
		out += ", err: " + err.Error()
	}
	return errors.New(out)
}

func _bhead(sgl *memsys.SGL, lbuf []byte) (s string) {
	sgl.Rewind()
	n, _ := sgl.Read(lbuf)
	if n > 0 {
		s = cos.BHead(lbuf, invMaxLine)
	}
	return s
}

func (s3bp *s3bp) getInventory(cloudBck *cmn.Bck, svc *s3.Client, ctx *core.LsoInvCtx) (string, int, error) {
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
		ecode, e := awsErrorToAISError(err, cloudBck, "")
		return "", ecode, e
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
		return "", http.StatusNotFound, cos.NewErrNotFound(cloudBck, "S3 bucket inventory '"+what+"'")
	}

	//
	// 2. read the manifest and extract `fileSchema`
	//
	schema, ecode, err := s3bp.getManifest(cloudBck, svc, manifest.oname)
	if err != nil {
		return "", ecode, err
	}

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
			ecode, e := awsErrorToAISError(errN, cloudBck, name)
			nlog.Errorln("delete", name, e, ecode)
		}
	}

	fqn, usable, err := checkInventory(cloudBck, csv.mtime, ctx)
	if err != nil || usable {
		return fqn, 0, err
	}

	//
	// 4. get+unzip and save locally
	//
	input := s3.GetObjectInput{Bucket: aws.String(cloudBck.Name), Key: aws.String(csv.oname)}
	obj, err := svc.GetObject(context.Background(), &input)
	if err != nil {
		ecode, e := awsErrorToAISError(err, cloudBck, csv.oname)
		return "", ecode, e
	}
	defer cos.Close(obj.Body)

	gzr, err := gzip.NewReader(obj.Body)
	if err != nil {
		return "", 0, _errInv("gzip", err)
	}
	if err = cos.CreateDir(filepath.Dir(fqn)); err != nil {
		gzr.Close()
		return "", 0, _errInv("create-dir", err)
	}
	wfh, err := os.OpenFile(fqn, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, cos.PermRWR)
	if err != nil {
		return "", 0, _errInv("wopen", err)
	}
	buf, slab := s3bp.t.PageMM().AllocSize(min(*obj.ContentLength*3, 64*cos.KiB)) // wrt "uncompressed"
	ctx.Size, err = cos.CopyBuffer(wfh, gzr, buf)
	slab.Free(buf)
	wfh.Close()

	if err != nil {
		return "", 0, _errInv("io.copy", err)
	}
	err = gzr.Close()
	debug.AssertNoErr(err)

	if err = os.Chtimes(fqn, csv.mtime, csv.mtime); err != nil {
		return "", 0, _errInv("chtimes", err)
	}

	nlog.Infoln("new bucket inventory:", *ctx, fqn)
	return fqn, 0, nil
}

func (*s3bp) listInventory(cloudBck *cmn.Bck, fh *os.File, sgl *memsys.SGL, ctx *core.LsoInvCtx, msg *apc.LsoMsg, lst *cmn.LsoRes) error {
	msg.PageSize = calcPageSize(msg.PageSize, invMaxPage)
	for j := len(lst.Entries); j < int(msg.PageSize); j++ {
		lst.Entries = append(lst.Entries, &cmn.LsoEnt{})
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
		err    error
		i      int64
		off    = ctx.Offset
		skip   = msg.ContinuationToken != ""
		lbuf   = make([]byte, 256) // m.b. enough for all lines
		custom cos.StrKVs
	)
	if msg.WantProp(apc.GetPropsCustom) {
		custom = make(cos.StrKVs, 2)
	}
	for {
		ctx.Offset = off + sgl.Roff()  // advance
		lbuf, err = sgl.ReadLine(lbuf) // reuse
		if err != nil {
			break
		}
		line := strings.Split(string(lbuf), ",")
		debug.Assertf(strings.Contains(line[invBucketPos], cloudBck.Name), "%q %d", line, ctx.Offset)

		objName := cmn.UnquoteCEV(line[invKeyPos])

		// prefix
		if msg.IsFlagSet(apc.LsNoRecursion) {
			if _, err := cmn.HandleNoRecurs(msg.Prefix, objName); err != nil {
				continue
			}
		} else if msg.Prefix != "" && !strings.HasPrefix(objName, msg.Prefix) {
			continue
		}

		// skip
		if skip && off > 0 {
			// expecting fseek to position precisely at the next (TODO: recover?)
			debug.Assert(objName == msg.ContinuationToken, objName, " vs ", msg.ContinuationToken)
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

		clear(custom)
		for i := invKeyPos + 1; i < len(ctx.Schema); i++ {
			switch types.InventoryOptionalField(ctx.Schema[i]) {
			case types.InventoryOptionalFieldSize:
				size := cmn.UnquoteCEV(line[i])
				entry.Size, err = strconv.ParseInt(size, 10, 64)
				if err != nil {
					return err
				}
			case types.InventoryOptionalFieldETag:
				if custom != nil {
					custom[cmn.ETag] = cmn.UnquoteCEV(line[i])
				}
			case types.InventoryOptionalFieldLastModifiedDate:
				if custom != nil {
					custom[cmn.LastModified] = cmn.UnquoteCEV(line[i])
				}
			}
		}
		if len(custom) > 0 {
			entry.Custom = cmn.CustomMD2S(custom)
		}
	}

	lst.Entries = lst.Entries[:i]
	return err
}
