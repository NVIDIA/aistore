// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/ais/s3compat"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
)

// PUT s3/bckName/objName
func (t *targetrunner) s3Handler(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 0, true, cmn.S3)
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
		t.invalmsghdlrf(w, r, "Invalid HTTP Method: %v %s", r.Method, r.URL.Path)
	}
}

func (t *targetrunner) copyObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	if len(items) < 2 {
		t.invalmsghdlr(w, r, "object name is undefined")
		return
	}
	config := cmn.GCO.Get()
	src := r.Header.Get(s3compat.HeaderObjSrc)
	src = strings.Trim(src, "/") // in AWS examples the path starts with "/"
	parts := strings.SplitN(src, "/", 2)
	if len(parts) < 2 {
		t.invalmsghdlr(w, r, "copy is not an object name")
		return
	}
	bckSrc := cluster.NewBck(parts[0], cmn.ProviderAIS, cmn.NsGlobal)
	objSrc := strings.Trim(parts[1], "/")
	if err := bckSrc.Init(t.owner.bmd, nil); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	lom := &cluster.LOM{T: t, ObjName: objSrc}
	if err := lom.Init(bckSrc.Bck, config); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); ok {
			t.BMDVersionFixup(r, cmn.Bck{}, true /* sleep */)
			err = lom.Init(bckSrc.Bck, config)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
		return
	}
	if err := lom.Load(); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	bckDst := cluster.NewBck(items[0], cmn.ProviderAIS, cmn.NsGlobal)
	if err := bckDst.Init(t.owner.bmd, nil); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	coi := copyObjInfo{t: t}
	coi.BckTo = bckDst
	objName := path.Join(items[1:]...)
	if _, err := coi.copyObject(lom, objName); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	var cksumValue string
	if cksum := lom.Cksum(); cksum != nil && cksum.Type() == cmn.ChecksumMD5 {
		cksumValue = cksum.Value()
	}
	result := s3compat.CopyObjectResult{
		LastModified: s3compat.FormatTime(lom.Atime()),
		ETag:         cksumValue,
	}
	w.Write(result.MustMarshal())
}

func (t *targetrunner) directPutObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	started := time.Now()
	config := cmn.GCO.Get()
	if cs := fs.GetCapStatus(); cs.OOS {
		t.invalmsghdlr(w, r, cs.Err.Error())
		return
	}
	bck := cluster.NewBck(items[0], cmn.ProviderAIS, cmn.NsGlobal)
	if err := bck.Init(t.owner.bmd, nil); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	if len(items) < 2 {
		t.invalmsghdlr(w, r, "object name is undefined")
		return
	}
	var err error
	objName := path.Join(items[1:]...)
	lom := &cluster.LOM{T: t, ObjName: objName}
	if err = lom.Init(bck.Bck, config); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); ok {
			t.BMDVersionFixup(r, cmn.Bck{}, true /*sleep*/)
			err = lom.Init(bck.Bck, config)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
	}
	if lom.Bck().IsAIS() && lom.VersionConf().Enabled {
		lom.Load() // need to know the current version if versioning enabled
	}
	lom.SetAtimeUnix(started.UnixNano())

	// TODO: lom.SetCustomMD(cluster.AmazonMD5ObjMD, checksum)

	if errCode, err := t.doPut(r, lom, started); err != nil {
		t.fsErr(err, lom.FQN)
		t.invalmsghdlr(w, r, err.Error(), errCode)
		return
	}
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
		t.invalmsghdlr(w, r, "object name is undefined")
		return
	}
	started := time.Now()
	config := cmn.GCO.Get()
	bck := cluster.NewBck(items[0], cmn.ProviderAIS, cmn.NsGlobal)
	if err := bck.Init(t.owner.bmd, nil); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	var (
		err     error
		objSize int64

		objName = path.Join(items[1:]...)
	)
	uuid := r.URL.Query().Get(cmn.URLParamUUID)
	if uuid != "" {
		t.doETL(w, r, uuid, bck, objName)
		return
	}

	lom := &cluster.LOM{T: t, ObjName: objName}
	if err = lom.Init(bck.Bck, config); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); ok {
			t.BMDVersionFixup(r, cmn.Bck{}, true /* sleep */)
			err = lom.Init(bck.Bck, config)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
		return
	}
	if err = lom.Load(true); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	objSize = lom.Size()
	goi := &getObjInfo{
		started: started,
		t:       t,
		lom:     lom,
		w:       w,
		ctx:     context.Background(),
		ranges:  cmn.RangesQuery{Range: r.Header.Get(cmn.HeaderRange), Size: objSize},
	}
	s3compat.SetHeaderFromLOM(w.Header(), lom, objSize)
	if errCode, err := goi.getObject(); err != nil {
		if cmn.IsErrConnectionReset(err) {
			glog.Errorf("GET %s: %v", lom, err)
		} else {
			t.invalmsghdlr(w, r, err.Error(), errCode)
		}
	}
}

// HEAD s3/bckName/objName
func (t *targetrunner) headObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	var (
		err    error
		config = cmn.GCO.Get()
	)
	if len(items) < 2 {
		t.invalmsghdlr(w, r, "object name is undefined")
		return
	}
	bucket, objName := items[0], path.Join(items[1:]...)
	bck := cluster.NewBck(bucket, cmn.ProviderAIS, cmn.NsGlobal)
	if err := bck.Init(t.owner.bmd, nil); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	lom := &cluster.LOM{T: t, ObjName: objName}
	if err = lom.Init(bck.Bck, config); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); ok {
			t.BMDVersionFixup(r, cmn.Bck{}, true /* sleep */)
			err = lom.Init(bck.Bck, config)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
		return
	}

	lom.Lock(false)
	if err = lom.Load(true); err != nil && !cmn.IsObjNotExist(err) { // (doesnotexist -> ok, other)
		lom.Unlock(false)
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	lom.Unlock(false)

	exists := err == nil
	if !exists {
		t.invalmsghdlrstatusf(w, r, http.StatusNotFound, "%s/%s %s", bucket, objName, cmn.DoesNotExist)
		return
	}

	if isETLRequest(r.URL.Query()) {
		s3compat.SetETLHeader(w.Header(), lom)
		return
	}
	s3compat.SetHeaderFromLOM(w.Header(), lom, lom.Size())
}

// DEL s3/bckName/objName
func (t *targetrunner) delObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	var (
		config = cmn.GCO.Get()
		bck    = cluster.NewBck(items[0], cmn.ProviderAIS, cmn.NsGlobal)
	)
	if err := bck.Init(t.owner.bmd, nil); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	if len(items) < 2 {
		t.invalmsghdlr(w, r, "object name is undefined")
		return
	}
	objName := path.Join(items[1:]...)
	lom := &cluster.LOM{T: t, ObjName: objName}
	if err := lom.Init(bck.Bck, config); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	errCode, err := t.DeleteObject(context.Background(), lom, false)
	if err != nil {
		if errCode == http.StatusNotFound {
			t.invalmsghdlrsilent(w, r,
				fmt.Sprintf("object %s/%s doesn't exist", lom.Bck(), lom.ObjName),
				http.StatusNotFound,
			)
		} else {
			t.invalmsghdlrstatusf(w, r, errCode, "error deleting %s: %v", lom, err)
		}
		return
	}
	// EC cleanup if EC is enabled
	ec.ECM.CleanupObject(lom)
}
