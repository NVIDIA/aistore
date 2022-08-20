// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
)

// PUT s3/bckName/objName
func (t *target) s3Handler(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 0, true, apc.URLPathS3.L)
	if err != nil {
		return
	}
	switch r.Method {
	case http.MethodHead:
		t.headObjS3(w, r, apiItems)
	case http.MethodGet:
		t.getObjS3(w, r, apiItems)
	case http.MethodPut:
		t.putObjS3(w, r, apiItems)
	case http.MethodDelete:
		q := r.URL.Query()
		if q.Has(s3.QparamMptUploadID) {
			t.abortMptUpload(w, r, apiItems, q)
		} else {
			t.delObjS3(w, r, apiItems)
		}
	case http.MethodPost:
		t.postObjS3(w, r, apiItems)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodHead, http.MethodPut, http.MethodPost)
	}
}

// Create a new object by copying the data from another bucket/object.
func (t *target) copyObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	if len(items) < 2 {
		t.writeErr(w, r, errS3Obj)
		return
	}
	src := r.Header.Get(s3.HdrObjSrc)
	src = strings.Trim(src, "/") // in AWS examples the path starts with "/"
	parts := strings.SplitN(src, "/", 2)
	if len(parts) < 2 {
		t.writeErr(w, r, errS3Obj)
		return
	}
	// src
	bckSrc, err := cluster.InitByNameOnly(parts[0], t.owner.bmd)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	objSrc := strings.Trim(parts[1], "/")
	if err := bckSrc.Init(t.owner.bmd); err != nil {
		t.writeErr(w, r, err)
		return
	}
	lom := cluster.AllocLOM(objSrc)
	defer cluster.FreeLOM(lom)
	if err := lom.InitBck(bckSrc.Bucket()); err != nil {
		if cmn.IsErrRemoteBckNotFound(err) {
			t.BMDVersionFixup(r)
			err = lom.InitBck(bckSrc.Bucket())
		}
		if err != nil {
			t.writeErr(w, r, err)
		}
		return
	}
	if err := lom.Load(false /*cache it*/, false /*locked*/); err != nil {
		t.writeErr(w, r, err)
		return
	}
	// dst
	bckDst, err := cluster.InitByNameOnly(items[0], t.owner.bmd)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	coi := allocCopyObjInfo()
	{
		coi.t = t
		coi.BckTo = bckDst
		coi.owt = cmn.OwtMigrate
	}
	objName := s3.ObjName(items)
	_, err = coi.copyObject(lom, objName)
	freeCopyObjInfo(coi)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}

	var cksumValue string
	if cksum := lom.Checksum(); cksum.Type() == cos.ChecksumMD5 {
		cksumValue = cksum.Value()
	}
	result := s3.CopyObjectResult{
		LastModified: s3.FormatTime(lom.Atime()),
		ETag:         cksumValue,
	}
	sgl := t.gmm.NewSGL(0)
	result.MustMarshal(sgl)
	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	sgl.WriteTo(w)
	sgl.Free()
}

func (t *target) doPutObjS3(w http.ResponseWriter, r *http.Request, items []string, bck *cluster.Bck) {
	if len(items) < 2 {
		t.writeErr(w, r, errS3Obj)
		return
	}
	objName := s3.ObjName(items)
	lom := cluster.AllocLOM(objName)
	defer cluster.FreeLOM(lom)
	if err := lom.InitBck(bck.Bucket()); err != nil {
		if cmn.IsErrRemoteBckNotFound(err) {
			t.BMDVersionFixup(r)
			err = lom.InitBck(bck.Bucket())
		}
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
	}
	started := time.Now()
	lom.SetAtimeUnix(started.UnixNano())

	// TODO: dual checksumming, e.g. lom.SetCustom(apc.ProviderAmazon, ...)

	dpq := dpqAlloc()
	defer dpqFree(dpq)
	if err := dpq.fromRawQ(r.URL.RawQuery); err != nil {
		t.writeErr(w, r, err)
		return
	}
	poi := allocPutObjInfo()
	{
		poi.atime = started
		poi.t = t
		poi.lom = lom
		poi.skipVC = cmn.Features.IsSet(feat.SkipVC) || cos.IsParseBool(dpq.skipVC) // apc.QparamSkipVC
		poi.restful = true
	}
	errCode, err := poi.do(r, dpq)
	freePutObjInfo(poi)
	if err != nil {
		t.fsErr(err, lom.FQN)
		t.writeErr(w, r, err, errCode)
		return
	}
	s3.SetETag(w.Header(), lom)
}

// PUT s3/bckName/objName
func (t *target) putObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	if cs := fs.GetCapStatus(); cs.OOS {
		t.writeErr(w, r, cs.Err, http.StatusInsufficientStorage)
		return
	}
	bck, err := cluster.InitByNameOnly(items[0], t.owner.bmd)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	q := r.URL.Query()
	if q.Has(s3.QparamMptPartNo) && r.URL.Query().Has(s3.QparamMptUploadID) {
		if r.Header.Get(s3.HdrObjSrc) != "" {
			t.putObjMptCopy(w, r, items)
		} else {
			t.putObjMptPart(w, r, items, q, bck)
		}
		return
	}
	if r.Header.Get(s3.HdrObjSrc) == "" {
		t.doPutObjS3(w, r, items, bck)
		return
	}
	t.copyObjS3(w, r, items)
}

// GET s3/<bucket-name[/<object-name>
func (t *target) getObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	bucket := items[0]
	bck, err := cluster.InitByNameOnly(bucket, t.owner.bmd)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	q := r.URL.Query()
	if len(items) == 1 && q.Has(s3.QparamMptUploads) {
		t.listMptUploads(w, bck, q)
		return
	}
	if len(items) < 2 {
		t.writeErr(w, r, errS3Obj)
		return
	}
	objName := s3.ObjName(items)
	if q.Has(s3.QparamMptPartNo) {
		t.getMptPart(w, r, bck, objName, q)
		return
	}
	uploadID := q.Get(s3.QparamMptUploadID)
	if uploadID != "" {
		t.listMptParts(w, r, bck, objName, q)
		return
	}

	dpq := dpqAlloc()
	if err := dpq.fromRawQ(r.URL.RawQuery); err != nil {
		dpqFree(dpq)
		t.writeErr(w, r, err)
		return
	}
	lom := cluster.AllocLOM(objName)
	t.getObject(w, r, dpq, bck, lom)
	s3.SetETag(w.Header(), lom) // add etag/md5
	cluster.FreeLOM(lom)
	dpqFree(dpq)
}

// HEAD s3/bckName/objName (TODO: s3.HdrMptCnt)
// See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
func (t *target) headObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	if len(items) < 2 {
		t.writeErr(w, r, errS3Obj)
		return
	}
	bucket, objName := items[0], s3.ObjName(items)
	bck, err := cluster.InitByNameOnly(bucket, t.owner.bmd)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	lom := cluster.AllocLOM(objName)
	t.headObject(w, r, r.URL.Query(), bck, lom)
	s3.SetETag(w.Header(), lom) // add etag/md5
	cluster.FreeLOM(lom)
}

// DEL s3/bckName/objName
func (t *target) delObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	bck, err := cluster.InitByNameOnly(items[0], t.owner.bmd)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	if len(items) < 2 {
		t.writeErr(w, r, errS3Obj)
		return
	}
	objName := s3.ObjName(items)
	lom := cluster.AllocLOM(objName)
	defer cluster.FreeLOM(lom)
	if err := lom.InitBck(bck.Bucket()); err != nil {
		t.writeErr(w, r, err)
		return
	}
	errCode, err := t.DeleteObject(lom, false)
	if err != nil {
		if errCode == http.StatusNotFound {
			err := cmn.NewErrNotFound("%s: %s", t.si, lom.FullName())
			t.writeErrSilent(w, r, err, http.StatusNotFound)
		} else {
			t.writeErrStatusf(w, r, errCode, "error deleting %s: %v", lom, err)
		}
		return
	}
	// EC cleanup if EC is enabled
	ec.ECM.CleanupObject(lom)
}

// POST s3/bckName/objName
func (t *target) postObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	bck, err := cluster.InitByNameOnly(items[0], t.owner.bmd)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	q := r.URL.Query()
	if q.Has(s3.QparamMptUploads) {
		if len(items) < 2 {
			t.writeErrf(w, r, fmtErrBO, items)
			return
		}
		t.startMpt(w, r, items, bck)
		return
	}
	if q.Has(s3.QparamMptUploadID) {
		if len(items) < 2 {
			t.writeErrf(w, r, fmtErrBO, items)
			return
		}
		t.completeMpt(w, r, items, q, bck)
		return
	}
	t.writeErrStatusf(w, r, http.StatusBadRequest,
		"set query parameter %q to start multipart upload or %q to complete the upload",
		s3.QparamMptUploads, s3.QparamMptUploadID)
}
