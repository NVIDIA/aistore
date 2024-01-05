// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"fmt"
	"net/http"
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

// [METHOD] /s3
func (t *target) s3Handler(w http.ResponseWriter, r *http.Request) {
	config := cmn.GCO.Get()
	if config.FastV(5, cos.SmoduleS3) {
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
		t.getObjS3(w, r, config, apiItems)
	case http.MethodPut:
		t.putCopyMpt(w, r, config, apiItems)
	case http.MethodDelete:
		q := r.URL.Query()
		if q.Has(s3.QparamMptUploadID) {
			t.abortMptUpload(w, r, apiItems, q)
		} else {
			t.delObjS3(w, r, apiItems)
		}
	case http.MethodPost:
		t.postObjS3(w, r, config, apiItems)
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
	bck, err, errCode := meta.InitByNameOnly(items[0], t.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, errCode)
		return
	}
	q := r.URL.Query()
	switch {
	case q.Has(s3.QparamMptPartNo) && q.Has(s3.QparamMptUploadID):
		if r.Header.Get(cos.S3HdrObjSrc) != "" {
			if config.FastV(5, cos.SmoduleS3) {
				nlog.Infoln("putMptCopy", items)
			}
			t.putMptCopy(w, r, items)
		} else {
			if config.FastV(5, cos.SmoduleS3) {
				nlog.Infoln("putMptPart", bck.String(), items, q)
			}
			t.putMptPart(w, r, items, q, bck)
		}
	case r.Header.Get(cos.S3HdrObjSrc) == "":
		t.putObjS3(w, r, bck, config, items)
	default:
		t.copyObjS3(w, r, config, items)
	}
}

// Copy object (maybe from another bucket)
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html
func (t *target) copyObjS3(w http.ResponseWriter, r *http.Request, config *cmn.Config, items []string) {
	if len(items) < 2 {
		s3.WriteErr(w, r, errS3Obj, 0)
		return
	}
	src := r.Header.Get(cos.S3HdrObjSrc)
	src = strings.Trim(src, "/") // in AWS examples the path starts with "/"
	parts := strings.SplitN(src, "/", 2)
	if len(parts) < 2 {
		s3.WriteErr(w, r, errS3Obj, 0)
		return
	}
	// src
	bckSrc, err, errCode := meta.InitByNameOnly(parts[0], t.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, errCode)
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
	if err := lom.Load(false /*cache it*/, false /*locked*/); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	// dst
	bckTo, err, errCode := meta.InitByNameOnly(items[0], t.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, errCode)
		return
	}

	coiParams := core.AllocCOI()
	{
		coiParams.Config = config
		coiParams.BckTo = bckTo
		coiParams.ObjnameTo = s3.ObjName(items)
		coiParams.OWT = cmn.OwtMigrateRepl
	}
	coi := (*copyOI)(coiParams)
	_, err = coi.do(t, nil /*DM*/, lom)
	core.FreeCOI(coiParams)

	if err != nil {
		if err == cmn.ErrSkip {
			name := lom.Cname()
			s3.WriteErr(w, r, cos.NewErrNotFound("%s: %s", t.si, name), http.StatusNotFound)
		} else {
			s3.WriteErr(w, r, err, 0)
		}
		return
	}

	var cksumValue string
	if cksum := lom.Checksum(); cksum.Type() == cos.ChecksumMD5 {
		cksumValue = cksum.Value()
	}
	result := s3.CopyObjectResult{
		LastModified: cos.FormatNanoTime(lom.AtimeUnix(), cos.ISO8601),
		ETag:         cksumValue,
	}
	sgl := t.gmm.NewSGL(0)
	result.MustMarshal(sgl)
	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	sgl.WriteTo(w)
	sgl.Free()
}

func (t *target) putObjS3(w http.ResponseWriter, r *http.Request, bck *meta.Bck, config *cmn.Config, items []string) {
	if len(items) < 2 {
		s3.WriteErr(w, r, errS3Obj, 0)
		return
	}
	objName := s3.ObjName(items)
	lom := core.AllocLOM(objName)
	defer core.FreeLOM(lom)
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
	defer dpqFree(dpq)
	if err := dpq.parse(r.URL.RawQuery); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	poi := allocPOI()
	{
		poi.atime = started.UnixNano()
		poi.t = t
		poi.lom = lom
		poi.config = config
		poi.skipVC = cmn.Rom.Features().IsSet(feat.SkipVC) || cos.IsParseBool(dpq.skipVC) // apc.QparamSkipVC
		poi.restful = true
	}
	errCode, err := poi.do(nil /*response hdr*/, r, dpq)
	freePOI(poi)
	if err != nil {
		t.fsErr(err, lom.FQN)
		s3.WriteErr(w, r, err, errCode)
		return
	}
	s3.SetETag(w.Header(), lom)
}

// GET s3/<bucket-name[/<object-name>]
func (t *target) getObjS3(w http.ResponseWriter, r *http.Request, config *cmn.Config, items []string) {
	bucket := items[0]
	bck, err, errCode := meta.InitByNameOnly(bucket, t.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, errCode)
		return
	}
	q := r.URL.Query()
	if len(items) == 1 && q.Has(s3.QparamMptUploads) {
		if config.FastV(5, cos.SmoduleS3) {
			nlog.Infoln("listMptUploads", bck.String(), q)
		}
		t.listMptUploads(w, bck, q)
		return
	}
	if len(items) < 2 {
		s3.WriteErr(w, r, errS3Obj, 0)
		return
	}
	objName := s3.ObjName(items)
	if q.Has(s3.QparamMptPartNo) {
		if config.FastV(5, cos.SmoduleS3) {
			nlog.Infoln("getMptPart", bck.String(), objName, q)
		}
		t.getMptPart(w, r, bck, objName, q)
		return
	}
	uploadID := q.Get(s3.QparamMptUploadID)
	if uploadID != "" {
		if config.FastV(5, cos.SmoduleS3) {
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
	if config.FastV(5, cos.SmoduleS3) {
		nlog.Infoln("getObject", lom.String(), dpq)
	}
	t.getObject(w, r, dpq, bck, lom)
	s3.SetETag(w.Header(), lom) // add etag/md5
	core.FreeLOM(lom)
	dpqFree(dpq)
}

// HEAD /s3/<bucket-name>/<object-name> (TODO: s3.HdrMptCnt)
// See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
func (t *target) headObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	if len(items) < 2 {
		s3.WriteErr(w, r, errS3Obj, 0)
		return
	}
	bucket, objName := items[0], s3.ObjName(items)
	bck, err, errCode := meta.InitByNameOnly(bucket, t.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, errCode)
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
		if !cmn.IsObjNotExist(err) {
			s3.WriteErr(w, r, err, 0)
			return
		}
		if bck.IsAIS() {
			s3.WriteErr(w, r, cos.NewErrNotFound("%s: object %s", t.si, lom.Cname()), 0)
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
		objAttrs, errCode, err := t.Backend(lom.Bck()).HeadObj(context.Background(), lom)
		if err != nil {
			s3.WriteErr(w, r, err, errCode)
			return
		}
		op.ObjAttrs = *objAttrs
	}

	custom := op.GetCustomMD()
	lom.SetCustomMD(custom)
	if v, ok := custom[cos.HdrETag]; ok {
		hdr.Set(cos.HdrETag, v)
	}
	s3.SetETag(hdr, lom)
	hdr.Set(cos.HdrContentLength, strconv.FormatInt(op.Size, 10))
	if v, ok := custom[cos.HdrContentType]; ok {
		hdr.Set(cos.HdrContentType, v)
	}
	// e.g. https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_Examples
	// (compare w/ `p.listObjectsS3()`
	lastModified := cos.FormatNanoTime(op.Atime, cos.RFC1123GMT)
	hdr.Set(cos.S3LastModified, lastModified)

	// TODO: lom.Checksum() via apc.HeaderPrefix+apc.HdrObjCksumType/Val via
	// s3 obj Metadata map[string]*string
}

// DELETE /s3/<bucket-name>/<object-name>
func (t *target) delObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	bck, err, errCode := meta.InitByNameOnly(items[0], t.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, errCode)
		return
	}
	if len(items) < 2 {
		s3.WriteErr(w, r, errS3Obj, 0)
		return
	}
	objName := s3.ObjName(items)
	lom := core.AllocLOM(objName)
	defer core.FreeLOM(lom)
	if err := lom.InitBck(bck.Bucket()); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	errCode, err = t.DeleteObject(lom, false)
	if err != nil {
		name := lom.Cname()
		if errCode == http.StatusNotFound {
			s3.WriteErr(w, r, cos.NewErrNotFound("%s: %s", t.si, name), http.StatusNotFound)
		} else {
			s3.WriteErr(w, r, fmt.Errorf("error deleting %s: %v", name, err), errCode)
		}
		return
	}
	// EC cleanup if EC is enabled
	ec.ECM.CleanupObject(lom)
}

// POST /s3/<bucket-name>/<object-name>
func (t *target) postObjS3(w http.ResponseWriter, r *http.Request, config *cmn.Config, items []string) {
	bck, err, errCode := meta.InitByNameOnly(items[0], t.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, errCode)
		return
	}
	q := r.URL.Query()
	if q.Has(s3.QparamMptUploads) {
		if len(items) < 2 {
			err := fmt.Errorf(fmtErrBO, items)
			s3.WriteErr(w, r, err, 0)
			return
		}
		if config.FastV(5, cos.SmoduleS3) {
			nlog.Infoln("startMpt", bck.String(), items, q)
		}
		t.startMpt(w, r, items, bck)
		return
	}
	if q.Has(s3.QparamMptUploadID) {
		if len(items) < 2 {
			err := fmt.Errorf(fmtErrBO, items)
			s3.WriteErr(w, r, err, 0)
			return
		}
		if config.FastV(5, cos.SmoduleS3) {
			nlog.Infoln("completeMpt", bck.String(), items, q)
		}
		t.completeMpt(w, r, items, q, bck)
		return
	}
	err = fmt.Errorf("set query parameter %q to start multipart upload or %q to complete the upload",
		s3.QparamMptUploads, s3.QparamMptUploadID)
	s3.WriteErr(w, r, err, 0)
}
