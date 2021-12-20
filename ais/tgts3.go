// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/ais/s3compat"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

// PUT s3/bckName/objName
func (t *targetrunner) s3Handler(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 0, true, cmn.URLPathS3.L)
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
		t.delObjS3(w, r, apiItems)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodHead, http.MethodPut)
	}
}

func (t *targetrunner) copyObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	if len(items) < 2 {
		t.writeErr(w, r, errS3Obj)
		return
	}
	src := r.Header.Get(s3compat.HeaderObjSrc)
	src = strings.Trim(src, "/") // in AWS examples the path starts with "/"
	parts := strings.SplitN(src, "/", 2)
	if len(parts) < 2 {
		t.writeErr(w, r, errS3Obj)
		return
	}
	bckSrc := cluster.NewBck(parts[0], cmn.ProviderAIS, cmn.NsGlobal)
	objSrc := strings.Trim(parts[1], "/")
	if err := bckSrc.Init(t.owner.bmd); err != nil {
		t.writeErr(w, r, err)
		return
	}
	lom := cluster.AllocLOM(objSrc)
	defer cluster.FreeLOM(lom)
	if err := lom.Init(bckSrc.Bck); err != nil {
		if cmn.IsErrRemoteBckNotFound(err) {
			t.BMDVersionFixup(r)
			err = lom.Init(bckSrc.Bck)
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
	bckDst := cluster.NewBck(items[0], cmn.ProviderAIS, cmn.NsGlobal)
	if err := bckDst.Init(t.owner.bmd); err != nil {
		t.writeErr(w, r, err)
		return
	}

	coi := copyObjInfo{t: t}
	coi.BckTo = bckDst
	objName := path.Join(items[1:]...)
	if _, err := coi.copyObject(lom, objName); err != nil {
		t.writeErr(w, r, err)
		return
	}

	var cksumValue string
	if cksum := lom.Checksum(); cksum.Type() == cos.ChecksumMD5 {
		cksumValue = cksum.Value()
	}
	result := s3compat.CopyObjectResult{
		LastModified: s3compat.FormatTime(lom.Atime()),
		ETag:         cksumValue,
	}
	sgl := memsys.PageMM().NewSGL(0)
	result.MustMarshal(sgl)
	w.Header().Set(cmn.HdrContentType, cmn.ContentXML)
	sgl.WriteTo(w)
	sgl.Free()
}

func (t *targetrunner) directPutObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	started := time.Now()
	if cs := fs.GetCapStatus(); cs.OOS {
		t.writeErr(w, r, cs.Err)
		return
	}
	bck := cluster.NewBck(items[0], cmn.ProviderAIS, cmn.NsGlobal)
	if err := bck.Init(t.owner.bmd); err != nil {
		t.writeErr(w, r, err)
		return
	}
	if len(items) < 2 {
		t.writeErr(w, r, errS3Obj)
		return
	}

	var (
		err     error
		objName = path.Join(items[1:]...)
		lom     = cluster.AllocLOM(objName)
	)
	defer cluster.FreeLOM(lom)
	if err = lom.Init(bck.Bck); err != nil {
		if cmn.IsErrRemoteBckNotFound(err) {
			t.BMDVersionFixup(r)
			err = lom.Init(bck.Bck)
		}
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
	}
	if lom.Bck().IsAIS() && lom.VersionConf().Enabled {
		// load it to see the current version; TODO: skipVC
		lom.Load(true /*cache it*/, false /*locked*/)
	}
	lom.SetAtimeUnix(started.UnixNano())

	// TODO: dual checksumming, e.g. lom.SetCustom(cmn.ProviderAmazon, ...)

	if errCode, err := t.doPut(r, lom, started, r.URL.Query(), false /*skipVC*/); err != nil {
		t.fsErr(err, lom.FQN)
		t.writeErr(w, r, err, errCode)
		return
	}
	s3compat.SetETag(w.Header(), lom)
}

// PUT s3/bckName/objName
func (t *targetrunner) putObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	if r.Header.Get(s3compat.HeaderObjSrc) == "" {
		t.directPutObjS3(w, r, items)
		return
	}
	t.copyObjS3(w, r, items)
}

// GET s3/<bucket-name/<object-name>[?uuid=<etl-uuid>]
func (t *targetrunner) getObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	if len(items) < 2 {
		t.writeErr(w, r, errS3Obj)
		return
	}
	bck := cluster.NewBck(items[0], cmn.ProviderAIS, cmn.NsGlobal)
	if err := bck.Init(t.owner.bmd); err != nil {
		t.writeErr(w, r, err)
		return
	}
	lom := cluster.AllocLOM(path.Join(items[1:]...))
	t.getObject(w, r, r.URL.Query(), bck, lom)
	s3compat.SetETag(w.Header(), lom) // add etag/md5
	cluster.FreeLOM(lom)
}

// HEAD s3/bckName/objName
func (t *targetrunner) headObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	if len(items) < 2 {
		t.writeErr(w, r, errS3Obj)
		return
	}
	bucket, objName := items[0], path.Join(items[1:]...)
	bck := cluster.NewBck(bucket, cmn.ProviderAIS, cmn.NsGlobal)
	if err := bck.Init(t.owner.bmd); err != nil {
		t.writeErr(w, r, err)
		return
	}

	lom := cluster.AllocLOM(objName)
	t.headObject(w, r, r.URL.Query(), bck, lom)
	s3compat.SetETag(w.Header(), lom) // add etag/md5
	cluster.FreeLOM(lom)
}

// DEL s3/bckName/objName
func (t *targetrunner) delObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	bck := cluster.NewBck(items[0], cmn.ProviderAIS, cmn.NsGlobal)
	if err := bck.Init(t.owner.bmd); err != nil {
		t.writeErr(w, r, err)
		return
	}
	if len(items) < 2 {
		t.writeErr(w, r, errS3Obj)
		return
	}
	objName := path.Join(items[1:]...)
	lom := cluster.AllocLOM(objName)
	defer cluster.FreeLOM(lom)
	if err := lom.Init(bck.Bck); err != nil {
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
