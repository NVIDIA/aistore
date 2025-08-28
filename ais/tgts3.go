// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
)

const fmtErrBckObj = "invalid %s request: expecting bucket and object (names) in the URL, have %v"

// [METHOD] /s3
func (t *target) s3Handler(w http.ResponseWriter, r *http.Request) {
	if cmn.Rom.FastV(5, cos.SmoduleS3) {
		nlog.Infoln("s3Handler", t.String(), r.Method, r.URL)
	}
	apiItems, err := t.parseURL(w, r, apc.URLPathS3.L, 0, true)
	if err != nil {
		return
	}

	switch r.Method {
	case http.MethodHead:
		t.headObjS3(w, r, apiItems)
	case http.MethodGet:
		t.getObjS3(w, r, apiItems)
	case http.MethodPut:
		config := cmn.GCO.Get()
		t.putCopyMpt(w, r, config, apiItems)
	case http.MethodDelete:
		q := r.URL.Query()
		if q.Has(s3.QparamMptUploadID) {
			t.abortMpt(w, r, apiItems, q)
		} else {
			t.delObjS3(w, r, apiItems)
		}
	case http.MethodPost:
		t.postObjS3(w, r, apiItems)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodHead, http.MethodPut, http.MethodPost)
	}
}

// PUT /s3/<bucket-name>/<object-name>
// [switch] mpt | put | copy
func (t *target) putCopyMpt(w http.ResponseWriter, r *http.Request, config *cmn.Config, items []string) {
	cs := fs.Cap()
	if cs.IsOOS() {
		s3.WriteErr(w, r, cs.Err(), http.StatusInsufficientStorage)
		return
	}
	bck, ecode, err := meta.InitByNameOnly(items[0], t.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, ecode)
		return
	}
	q := r.URL.Query()
	switch {
	case q.Has(s3.QparamMptPartNo) && q.Has(s3.QparamMptUploadID):
		if r.Header.Get(cos.S3HdrObjSrc) != "" {
			// TODO: copy another object (or its range) => part of the specified multipart upload.
			// https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html
			s3.WriteErr(w, r, errors.New("UploadPartCopy not implemented yet"), http.StatusNotImplemented)
			return
		}
		if cmn.Rom.FastV(5, cos.SmoduleS3) {
			nlog.Infoln("putMptPart", bck.String(), items, q)
		}
		t.putMptPartS3(w, r, items, q, bck)
	case r.Header.Get(cos.S3HdrObjSrc) == "":
		objName := s3.ObjName(items)
		lom := core.AllocLOM(objName)
		t.putObjS3(w, r, bck, config, lom)
		core.FreeLOM(lom)
	default:
		t.copyObjS3(w, r, config, items)
	}
}

// Copy object (maybe from another bucket)
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html
// Note:
// S3 copy object API use the "destination" bucket in the URL path, but AIStore target use "source" bucket
// we need this extra `copyObjS3` handler at target to address the translation
func (t *target) copyObjS3(w http.ResponseWriter, r *http.Request, config *cmn.Config, items []string) {
	src := r.Header.Get(cos.S3HdrObjSrc)

	// [HACK]
	// it appears, 'x-amz-copy-source' header gets double-escaped upon http redirect
	// (s3cmd and aws clients, both)
	srcUnescaped, err := url.QueryUnescape(src)
	if err != nil {
		nlog.Errorf("Warning: failed to unescape '%s=%s' header: %v", cos.S3HdrObjSrc, src, err)
	} else if src != srcUnescaped {
		if cmn.Rom.FastV(5, cos.SmoduleS3) {
			nlog.Infoln("Warning: header", cos.S3HdrObjSrc, "is double-escaped - unescaping from", src, "to", srcUnescaped)
		}
		src = srcUnescaped
	}

	src = strings.Trim(src, "/") // in AWS examples the path starts with "/"
	parts := strings.SplitN(src, "/", 2)
	if len(parts) < 2 {
		s3.WriteErr(w, r, errS3Obj, 0)
		return
	}
	// src
	bckSrc, ecode, err := meta.InitByNameOnly(parts[0], t.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, ecode)
		return
	}
	objSrc := strings.Trim(parts[1], "/")
	if err := bckSrc.Init(t.owner.bmd); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	lom := core.AllocLOM(objSrc)
	defer core.FreeLOM(lom)
	if err := lom.InitBck(bckSrc.Bucket()); err != nil {
		if cmn.IsErrRemoteBckNotFound(err) {
			t.BMDVersionFixup(r)
			err = lom.InitBck(bckSrc.Bucket())
		}
		if err != nil {
			s3.WriteErr(w, r, err, 0)
		}
		return
	}

	// dst
	bckTo, ecode, err := meta.InitByNameOnly(items[0], t.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, ecode)
		return
	}

	// NOTE: lom will be safely loaded, locked, unlocked during the call
	ecode, err = t.copyObject(lom, bckTo, s3.ObjName(items), nil /*dpq*/, config)
	if err != nil {
		if err == cmn.ErrSkip {
			name := lom.Cname()
			s3.WriteErr(w, r, cos.NewErrNotFound(t, name), http.StatusNotFound)
		} else {
			s3.WriteErr(w, r, err, ecode)
		}
		return
	}

	// TODO -- FIXME: remote (source) get stats (t.rgetstats)

	var cksumValue string
	if cksum := lom.Checksum(); cksum != nil && cksum.Type() == cos.ChecksumMD5 {
		cksumValue = cksum.Value()
	}
	result := s3.CopyObjectResult{
		LastModified: cos.FormatNanoTime(lom.AtimeUnix(), cos.ISO8601),
		ETag:         cksumValue,
	}
	sgl := t.gmm.NewSGL(0)
	result.MustMarshal(sgl)
	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	sgl.WriteTo2(w)
	sgl.Free()
}

func (t *target) putObjS3(w http.ResponseWriter, r *http.Request, bck *meta.Bck, config *cmn.Config, lom *core.LOM) {
	if err := lom.InitBck(bck.Bucket()); err != nil {
		if cmn.IsErrRemoteBckNotFound(err) {
			t.BMDVersionFixup(r)
			err = lom.InitBck(bck.Bucket())
		}
		if err != nil {
			s3.WriteErr(w, r, err, 0)
			return
		}
	}
	started := time.Now()
	lom.SetAtimeUnix(started.UnixNano())

	// TODO: dual checksumming, e.g. lom.SetCustom(apc.AWS, ...)

	dpq := dpqAlloc()
	if err := dpq.parse(r.URL.RawQuery); err != nil {
		s3.WriteErr(w, r, err, 0)
		dpqFree(dpq)
		return
	}
	poi := allocPOI()
	{
		poi.atime = started.UnixNano()
		poi.t = t
		poi.lom = lom
		poi.config = config
		poi.skipVC = cmn.Rom.Features().IsSet(feat.SkipVC) || dpq.skipVC // apc.QparamSkipVC
		poi.restful = true
	}
	ecode, err := poi.do(nil /*response hdr*/, r, dpq)
	freePOI(poi)
	if err != nil {
		t.FSHC(err, lom.Mountpath(), lom.FQN)
		s3.WriteErr(w, r, err, ecode)
	} else {
		s3.SetS3Headers(w.Header(), lom)
	}
	dpqFree(dpq)
}

// GET s3/<bucket-name[/<object-name>]
func (t *target) getObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	bucket := items[0]
	bck, ecode, err := meta.InitByNameOnly(bucket, t.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, ecode)
		return
	}
	q := r.URL.Query()
	if len(items) == 1 && q.Has(s3.QparamMptUploads) {
		if cmn.Rom.FastV(5, cos.SmoduleS3) {
			nlog.Infoln("listMptUploads", bck.String(), q)
		}
		t.listMptUploads(w, bck, q)
		return
	}
	if len(items) < 2 {
		err := fmt.Errorf(fmtErrBckObj, r.Method, items)
		s3.WriteErr(w, r, err, 0)
		return
	}
	objName := s3.ObjName(items)
	if q.Has(s3.QparamMptPartNo) {
		if cmn.Rom.FastV(5, cos.SmoduleS3) {
			nlog.Infoln("getMptPart", bck.String(), objName, q)
		}
		lom := core.AllocLOM(objName)
		t.getMptPart(w, r, bck, lom, q)
		core.FreeLOM(lom)
		return
	}
	uploadID := q.Get(s3.QparamMptUploadID)
	if uploadID != "" {
		if cmn.Rom.FastV(5, cos.SmoduleS3) {
			nlog.Infoln("listMptParts", bck.String(), objName, q)
		}
		t.listMptParts(w, r, bck, objName, q)
		return
	}

	dpq := dpqAlloc()
	if err := dpq.parse(r.URL.RawQuery); err != nil {
		dpqFree(dpq)
		s3.WriteErr(w, r, err, 0)
		return
	}
	lom := core.AllocLOM(objName)
	dpq.isS3 = true
	lom, err = t.getObject(w, r, dpq, bck, lom)
	core.FreeLOM(lom)

	if err != nil {
		s3.WriteErr(w, r, err, 0)
	}
	dpqFree(dpq)
}

// HEAD /s3/<bucket-name>/<object-name> (TODO: s3.HdrMptCnt)
// See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
func (t *target) headObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	bucket, objName := items[0], s3.ObjName(items)
	bck, ecode, err := meta.InitByNameOnly(bucket, t.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, ecode)
		return
	}
	lom := core.AllocLOM(objName)
	defer core.FreeLOM(lom)
	if err := lom.InitBck(bck.Bucket()); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	exists := true
	err = lom.Load(true /*cache it*/, false /*locked*/)
	if err != nil {
		exists = false
		if !cos.IsNotExist(err) {
			s3.WriteErr(w, r, err, 0)
			return
		}
		if bck.IsAIS() {
			s3.WriteErr(w, r, cos.NewErrNotFound(t, lom.Cname()), http.StatusNotFound)
			return
		}
	}

	var (
		hdr = w.Header()
		op  cmn.ObjectProps
	)
	if exists {
		op.ObjAttrs = *lom.ObjAttrs()
	} else {
		// cold HEAD
		objAttrs, ecode, err := t.HeadCold(lom, r)
		if err != nil {
			s3.WriteErr(w, r, err, ecode)
			return
		}
		op.ObjAttrs = *objAttrs
	}

	custom := op.GetCustomMD()
	lom.SetCustomMD(custom)

	// set s3 response headers
	s3.SetS3Headers(hdr, lom)
	hdr.Set(cos.HdrContentLength, strconv.FormatInt(op.Size, 10))
	if v, ok := custom[cos.HdrContentType]; ok {
		hdr.Set(cos.HdrContentType, v)
	}
	// - https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
	// - https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html
	if cksum := lom.Checksum(); cksum != nil && cksum.Ty() != cos.ChecksumNone {
		hdr.Set(cos.S3MetadataChecksumType, cksum.Ty())
		hdr.Set(cos.S3MetadataChecksumVal, cksum.Val())
	}
}

// DELETE /s3/<bucket-name>/<object-name>
func (t *target) delObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	bck, ecode, err := meta.InitByNameOnly(items[0], t.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, ecode)
		return
	}
	objName := s3.ObjName(items)
	lom := core.AllocLOM(objName)
	defer core.FreeLOM(lom)
	if err := lom.InitBck(bck.Bucket()); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	ecode, err = t.DeleteObject(lom, false)
	if err != nil {
		name := lom.Cname()
		if ecode == http.StatusNotFound {
			s3.WriteErr(w, r, cos.NewErrNotFound(t, name), http.StatusNotFound)
		} else {
			s3.WriteErr(w, r, fmt.Errorf("error deleting %s: %v", name, err), ecode)
		}
		return
	}
	// EC cleanup if EC is enabled
	ec.ECM.CleanupObject(lom)
}

// POST /s3/<bucket-name>/<object-name>
func (t *target) postObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	bck, ecode, err := meta.InitByNameOnly(items[0], t.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, ecode)
		return
	}
	q := r.URL.Query()
	if q.Has(s3.QparamMptUploads) {
		if cmn.Rom.FastV(5, cos.SmoduleS3) {
			nlog.Infoln("startMpt", bck.String(), items, q)
		}
		t.startMpt(w, r, items, bck)
		return
	}
	if q.Has(s3.QparamMptUploadID) {
		if cmn.Rom.FastV(5, cos.SmoduleS3) {
			nlog.Infoln("completeMpt", bck.String(), items, q)
		}
		t.completeMpt(w, r, items, q, bck)
		return
	}
	err = fmt.Errorf("set query parameter %q to start multipart upload or %q to complete the upload",
		s3.QparamMptUploads, s3.QparamMptUploadID)
	s3.WriteErr(w, r, err, 0)
}
