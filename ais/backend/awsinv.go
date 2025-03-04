//go:build aws

// Package backend contains core/backend interface implementations for supported backend providers.
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

	aiss3 "github.com/NVIDIA/aistore/ais/s3"
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

// NOTE currently implemented main assumption/requirement:
// - one bucket, one inventory (for this same bucket), and one statically defined .csv

// TODO:
// - LsoMsg.StartAfter (a.k.a. ListObjectsV2Input.StartAfter); see also "expecting to resume" below

// constant and tunables (see also: ais/s3/inventory)
const numBlobWorkers = 10

const invTag = "bucket-inventory"

const invBusyTimeout = 10 * time.Second

const (
	invMaxLine = cos.KiB >> 1 // line buf
	invSwapSGL = invMaxLine

	invMaxPage = 8 * apc.MaxPageSizeAWS
	invPageSGL = max(invMaxPage*invMaxLine, 2*cos.MiB)
)

// NOTE: hardcoding two groups of constants - cannot find any of them in https://github.com/aws/aws-sdk-go-v2
// Generally, instead of reading inventory manifest line by line (and worrying about duplicated constants)
// it'd be much nicer to have an official JSON.

const (
	invManifest = "manifest.json"
	invSchema   = "fileSchema" // e.g. "fileSchema" : "Bucket, Key, Size, ETag"
	invKey      = "\"key\""
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
	size  int64
}

// list inventories, read and parse manifest, return schema and unique oname
func (s3bp *s3bp) initInventory(cloudBck *cmn.Bck, svc *s3.Client, ctx *core.LsoInvCtx, prefix string) (*s3.ListObjectsV2Output,
	invT, invT, int, error) {
	var (
		csv      invT
		manifest invT
		bn       = aws.String(cloudBck.Name)
		params   = &s3.ListObjectsV2Input{Bucket: bn}
	)

	params.Prefix = aws.String(prefix)
	params.MaxKeys = aws.Int32(apc.MaxPageSizeAWS) // no more than 1000 manifests

	// 1. ls inventory
	resp, err := svc.ListObjectsV2(context.Background(), params)
	if err != nil {
		ecode, e := awsErrorToAISError(err, cloudBck, "")
		return nil, csv, manifest, ecode, e
	}
	for _, obj := range resp.Contents {
		name := *obj.Key
		if cos.Ext(name) == aiss3.InvSrcExt {
			mtime := *(obj.LastModified)
			if csv.mtime.IsZero() || mtime.After(csv.mtime) {
				csv.mtime = mtime
				csv.oname = name
				csv.size = *(obj.Size)
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
			what = cos.Left(ctx.Name, aiss3.InvName)
		}
		return nil, csv, manifest, http.StatusNotFound, cos.NewErrNotFound(cloudBck, invTag+":"+what)
	}
	if csv.mtime.After(manifest.mtime) {
		a, b := cos.FormatTime(manifest.mtime, cos.StampSec), cos.FormatTime(csv.mtime, cos.StampSec)
		nlog.Warningln("using an older manifest:", manifest.oname, a, "to parse:", csv.oname, b)
	}

	// 2. read the manifest and extract `fileSchema` --> ctx
	schema, ecode, err := s3bp._getManifest(cloudBck, svc, manifest.oname, csv.oname)
	if err != nil {
		return nil, csv, manifest, ecode, err
	}

	ctx.Schema = schema
	return resp, csv, manifest, 0, nil
}

func cleanupOldInventory(cloudBck *cmn.Bck, svc *s3.Client, lsV2resp *s3.ListObjectsV2Output, csv, manifest invT) {
	var (
		num int
		bn  = aws.String(cloudBck.Name)
	)
	for _, obj := range lsV2resp.Contents {
		name := *obj.Key
		mtime := *(obj.LastModified)
		if name == csv.oname || name == manifest.oname || csv.mtime.Sub(mtime) < 23*time.Hour {
			continue
		}
		if _sinceAbs(csv.mtime, mtime) < 23*time.Hour {
			continue
		}
		if _, errN := svc.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: bn, Key: obj.Key}); errN != nil {
			ecode, e := awsErrorToAISError(errN, cloudBck, name)
			nlog.Errorln("delete", name, e, ecode)
			continue
		}
		num++
	}
	if num > 0 {
		nlog.Infoln("cleanup: removed", num, "older", invTag, "file"+cos.Plural(num))
	}
}

func checkInvLom(latest time.Time, ctx *core.LsoInvCtx) (time.Time, bool) {
	size, _, mtime, err := ctx.Lom.Fstat(false /*get-atime*/)
	if err != nil {
		debug.Assert(os.IsNotExist(err), err)
		nlog.Infoln(invTag, "does not exist, getting a new one for the timestamp:", latest)
		return time.Time{}, false
	}

	if cmn.Rom.FastV(5, cos.SmoduleBackend) {
		nlog.Infoln(core.T.String(), "checking", ctx.Lom.String(), ctx.Lom.FQN, ctx.Lom.HrwFQN)
	}
	abs := _sinceAbs(mtime, latest)
	if abs < time.Second {
		debug.Assert(ctx.Size == 0 || ctx.Size == size)
		ctx.Size = size

		// start (or rather, keep) using this one
		errN := ctx.Lom.Load(true, true)
		debug.AssertNoErr(errN)
		debug.Assert(ctx.Lom.Lsize() == size, ctx.Lom.Lsize(), size)
		return time.Time{}, true
	}

	nlog.Infoln(invTag, ctx.Lom.Cname(), "is likely being updated: [", mtime.String(), latest.String(), abs, "]")
	return mtime, false
}

// get+unzip and write lom
func (s3bp *s3bp) getInventory(cloudBck *cmn.Bck, ctx *core.LsoInvCtx, csv invT) error {
	lom := &core.LOM{ObjName: csv.oname}
	if err := lom.InitBck(cloudBck); err != nil {
		return err
	}
	lom.SetSize(csv.size)

	wfqn := fs.CSM.Gen(ctx.Lom, fs.WorkfileType, "")
	wfh, err := ctx.Lom.CreateWork(wfqn)
	if err != nil {
		return _errInv("create-file", err)
	}

	var (
		r = &reader{
			workCh: make(chan *memsys.SGL, 1),
			doneCh: make(chan *memsys.SGL, 1),
		}
		uzw = &unzipWriter{
			r:   r,
			wfh: wfh,
		}
		params = &core.BlobParams{
			Lom:      lom,
			Msg:      &apc.BlobMsg{NumWorkers: numBlobWorkers},
			WriteSGL: uzw.writeSGL,
		}
		xblob core.Xact
		gzr   *gzip.Reader
	)
	// run x-blob-downloader with default (num-readers, chunk-size) tunables
	xblob, err = s3bp.t.GetColdBlob(params, lom.ObjAttrs())
	if err == nil {
		if cmn.Rom.FastV(4, cos.SmoduleBackend) {
			nlog.Infoln("started", xblob.String(), "->", wfqn)
		}
		gzr, err = gzip.NewReader(r)
	}
	if err != nil {
		wfh.Close()
		cos.RemoveFile(wfqn)
		return _errInv("blob-gunzip", err)
	}

	buf, slab := s3bp.mm.AllocSize(memsys.DefaultBuf2Size)
	ctx.Size, err = cos.CopyBuffer(uzw, gzr, buf)

	slab.Free(buf)
	wfh.Close()
	gzr.Close()

	// finalize (NOTE a lighter version of FinalizeObj - no redundancy, no locks)
	if err == nil {
		lom := ctx.Lom
		if err = lom.RenameFinalize(wfqn); err == nil {
			if err = os.Chtimes(lom.FQN, csv.mtime, csv.mtime); err == nil {
				nlog.Infoln("new", invTag+":", lom.Cname(), ctx.Schema)

				lom.SetSize(ctx.Size)
				lom.SetAtimeUnix(csv.mtime.UnixNano())
				if errN := lom.PersistMain(); errN != nil {
					debug.AssertNoErr(errN) // (unlikely)
					nlog.Errorln("failed to persist", lom.Cname(), "err:", err, "- proceeding anyway...")
				} else if cmn.Rom.FastV(4, cos.SmoduleBackend) {
					nlog.Infoln("done", xblob.String(), "->", lom.Cname(), ctx.Size)
				}
				return nil
			}
		}
	}

	// otherwise
	if nerr := cos.RemoveFile(wfqn); nerr != nil && !os.IsNotExist(nerr) {
		nlog.Errorf("get-inv (%v), nested fail to remove (%v)", err, nerr)
	}
	if abrt := xblob.AbortErr(); abrt != nil {
		return _errInv("get-inv-abort", abrt)
	}
	return _errInv("get-inv-gzr-uzw-fail", err)
}

func (*s3bp) listInventory(cloudBck *cmn.Bck, ctx *core.LsoInvCtx, msg *apc.LsoMsg, lst *cmn.LsoRes) (err error) {
	var (
		custom cos.StrKVs
		i      int64
	)
	msg.PageSize = calcPageSize(msg.PageSize, invMaxPage)
	for j := len(lst.Entries); j < int(msg.PageSize); j++ {
		lst.Entries = append(lst.Entries, &cmn.LsoEnt{})
	}
	lst.ContinuationToken = ""

	// when little remains: read some more unless eof
	sgl := ctx.SGL
	if sgl.Len() < 2*invSwapSGL && !ctx.EOF {
		_, err = io.CopyN(sgl, ctx.Lmfh, invPageSGL-sgl.Len()-256)
		if err != nil {
			ctx.EOF = err == io.EOF
			if !ctx.EOF {
				nlog.Errorln("Warning: error reading csv", err)
				return err
			}
			if sgl.Len() == 0 {
				return err
			}
		}
	}

	if msg.WantProp(apc.GetPropsCustom) {
		custom = make(cos.StrKVs, 2)
	}

	skip := msg.ContinuationToken != "" // (tentatively)
	lbuf := make([]byte, invMaxLine)    // reuse for all read lines

	// avoid having line split across SGLs
	for i < msg.PageSize && (sgl.Len() > invSwapSGL || ctx.EOF) {
		lbuf, err = sgl.NextLine(lbuf, true)
		if err != nil {
			break
		}

		line := strings.Split(string(lbuf), ",")
		debug.Assert(strings.Contains(line[invBucketPos], cloudBck.Name), line)

		objName := cmn.UnquoteCEV(line[invKeyPos])

		if skip {
			skip = false
			if objName != msg.ContinuationToken {
				nlog.Errorln("Warning: expecting to resume from the previously returned:",
					msg.ContinuationToken, "vs", objName)
			}
		}

		// prefix
		if msg.IsFlagSet(apc.LsNoRecursion) {
			if _, errN := cmn.HandleNoRecurs(msg.Prefix, objName); errN != nil {
				continue
			}
		} else if msg.Prefix != "" && !strings.HasPrefix(objName, msg.Prefix) {
			continue
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
					nlog.Errorln(ctx.Lom.String(), "failed to parse size", size, err)
				}
			case types.InventoryOptionalFieldETag:
				if custom != nil {
					custom[cmn.ETag] = line[i]
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

	// set next continuation token
	lbuf, err = sgl.NextLine(lbuf, false /*advance roff*/)
	if err == nil {
		line := strings.Split(string(lbuf), ",")
		debug.Assert(strings.Contains(line[invBucketPos], cloudBck.Name), line)
		lst.ContinuationToken = cmn.UnquoteCEV(line[invKeyPos])
	}
	return err
}

// GET, parse, and validate inventory manifest
// (see "hardcoding" comment above)
// with JSON-tagged manifest structure (that'd include `json:"fileSchema"`)
// it'd then make sense to additionally validate: format == csv and source bucket == destination bucket == this bucket
func (s3bp *s3bp) _getManifest(cloudBck *cmn.Bck, svc *s3.Client, mname, csvname string) (schema []string, _ int, _ error) {
	input := s3.GetObjectInput{Bucket: aws.String(cloudBck.Name), Key: aws.String(mname)}
	obj, err := svc.GetObject(context.Background(), &input)
	if err != nil {
		ecode, e := awsErrorToAISError(err, cloudBck, mname)
		return nil, ecode, e
	}

	sgl := s3bp.mm.NewSGL(0)
	_, err = io.Copy(sgl, obj.Body)
	cos.Close(obj.Body)

	if err != nil {
		sgl.Free()
		return nil, 0, err
	}

	var (
		fileSchema string
		size       int64
		lbuf       = make([]byte, invMaxLine)
		cname      = cloudBck.Cname(mname)
	)
	for fileSchema == "" || size == 0 {
		lbuf, err = sgl.NextLine(lbuf, true)
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
			debug.Assert(fileSchema == "", fileSchema)
			s := strings.TrimSpace(line[1])
			fileSchema = cmn.UnquoteCEV(strings.TrimSuffix(s, ","))
		} else if strings.Contains(line[0], invKey) {
			s := strings.TrimSpace(line[1])
			oname := cmn.UnquoteCEV(strings.TrimSuffix(s, ","))
			if oname != csvname {
				nlog.Warningln("manifested object", oname, "vs latest csv.gz", csvname)
			}
		}
	}

	// parse, validate
	if err != nil || fileSchema == "" {
		err = _parseErr(cname, sgl, lbuf, err)
	} else {
		if cmn.Rom.FastV(4, cos.SmoduleBackend) {
			nlog.Infoln("parsed manifest", cname, fileSchema, "compressed size", size)
		}
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

//
// internal
//

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

func _errInv(tag string, err error) error {
	return fmt.Errorf("%s: %s: %v", invTag, tag, err)
}

func _sinceAbs(t1, t2 time.Time) time.Duration {
	if t1.After(t2) {
		return t1.Sub(t2)
	}
	return t2.Sub(t1)
}

//
// chunk reader; serial reader; unzip unzipWriter
//

type (
	reader struct {
		sgl    *memsys.SGL
		workCh chan *memsys.SGL
		doneCh chan *memsys.SGL
	}
	unzipWriter struct {
		r   *reader
		wfh cos.LomWriter
	}
)

/////////////////
// unzipWriter //
/////////////////

// callback of the type `core.WriteSGL`
func (uzw *unzipWriter) writeSGL(sgl *memsys.SGL) error {
	uzw.r.workCh <- sgl
	<-uzw.r.doneCh // block here
	return nil
}

func (uzw *unzipWriter) Write(p []byte) (int, error) {
	return uzw.wfh.Write(p)
}

////////////
// reader //
////////////

func (r *reader) Read(b []byte) (n int, err error) {
	if r.sgl == nil {
		goto next
	}
read:
	n, err = r.sgl.Read(b)
	if err == nil {
		debug.Assert(n > 0)
		if r.sgl.Len() == 0 {
			r.doneCh <- r.sgl // recycle
			r.sgl = nil
		}
		return n, nil
	}
	if err == io.EOF {
		// done reading multi-SGL input
		debug.Assert(r.sgl.Len() == 0)
		debug.Assert(n > 0)
		err = nil
	}
	r.doneCh <- r.sgl // return on: sgl is fully read (EOF above) or any error
	r.sgl = nil
	return n, err

next: // (nil indicates EOF or error)
	r.sgl = <-r.workCh

	if r.sgl == nil {
		// user done as well
		close(r.workCh)
		close(r.doneCh)
		return 0, io.EOF
	}
	debug.Assert(r.sgl.Len() > 0)
	goto read
}
