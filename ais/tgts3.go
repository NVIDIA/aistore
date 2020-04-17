// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/ais/s3compat"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ec"
)

// PUT s3/bckName/objName
func (t *targetrunner) s3Handler(w http.ResponseWriter, r *http.Request) {
	apitems, err := t.checkRESTItems(w, r, 0, true, s3compat.Root)
	if err != nil {
		return
	}

	switch r.Method {
	case http.MethodHead:
		t.headObjS3(w, r, apitems)
	case http.MethodGet:
		t.getObjS3(w, r, apitems)
	case http.MethodPut:
		t.putObjS3(w, r, apitems)
	case http.MethodDelete:
		t.delObjS3(w, r, apitems)
	default:
		s := fmt.Sprintf("Invalid HTTP Method: %v %s", r.Method, r.URL.Path)
		t.invalmsghdlr(w, r, s)
	}
}

// PUT s3/bckName/objName
// TODO: add correct header to response (with Version/etc)
func (t *targetrunner) putObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	started := time.Now()
	config := cmn.GCO.Get()
	if capInfo := t.AvgCapUsed(config); capInfo.OOS {
		t.invalmsghdlr(w, r, capInfo.Err.Error())
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
	var (
		err error
	)
	if err = bck.AllowPUT(); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	objName := path.Join(items[1:]...)
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
	if lom.Bck().IsAIS() && lom.VerConf().Enabled {
		lom.Load() // need to know the current version if versioning enabled
	}
	lom.SetAtimeUnix(started.UnixNano())
	if err, errCode := t.doPut(r, lom, started); err != nil {
		t.fshc(err, lom.FQN)
		t.invalmsghdlr(w, r, err.Error(), errCode)
	}
}

// GET s3/bckName/objName
// TODO: add correct header to response (with Version/etc)
// TODO: Range
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
		err error
	)
	if err = bck.AllowPUT(); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	objName := path.Join(items[1:]...)
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

	goi := &getObjInfo{
		started: started,
		t:       t,
		lom:     lom,
		w:       w,
		ctx:     t.contextWithAuth(r.Header),
		offset:  0, //TODO: rangeOff,
		length:  0, // TODO: rangeLen,
	}
	s3compat.SetHeaderFromLOM(w.Header(), lom)
	if err, errCode := goi.getObject(); err != nil {
		if cmn.IsErrConnectionReset(err) {
			glog.Errorf("GET %s: %v", lom, err)
		} else {
			t.invalmsghdlr(w, r, err.Error(), errCode)
		}
	}
}

// HEAD s3/bckName/objName
func (t *targetrunner) headObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	if len(items) < 2 {
		t.invalmsghdlr(w, r, "object name is undefined")
		return
	}
	config := cmn.GCO.Get()
	bucket, objName := items[0], path.Join(items[1:]...)
	bck := cluster.NewBck(bucket, cmn.ProviderAIS, cmn.NsGlobal)
	if err := bck.Init(t.owner.bmd, nil); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	var (
		err error
	)
	if err = bck.AllowHEAD(); err != nil {
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
		t.invalmsghdlr(w, r, fmt.Sprintf("%s/%s %s", bucket, objName, cmn.DoesNotExist), http.StatusNotFound)
		return
	}
	s3compat.SetHeaderFromLOM(w.Header(), lom)
}

// DEL s3/bckName/objName
func (t *targetrunner) delObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	config := cmn.GCO.Get()
	bck := cluster.NewBck(items[0], cmn.ProviderAIS, cmn.NsGlobal)
	if err := bck.Init(t.owner.bmd, nil); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	if len(items) < 2 {
		t.invalmsghdlr(w, r, "object name is undefined")
		return
	}
	var (
		err error
	)
	if err = bck.AllowDELETE(); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	objName := path.Join(items[1:]...)
	lom := &cluster.LOM{T: t, ObjName: objName}
	if err = lom.Init(bck.Bck, config); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	err = t.objDelete(t.contextWithAuth(r.Header), lom, false)
	if err != nil {
		if cmn.IsObjNotExist(err) {
			t.invalmsghdlrsilent(w, r,
				fmt.Sprintf("object %s/%s doesn't exist", lom.Bck(), lom.ObjName), http.StatusNotFound)
		} else {
			t.invalmsghdlr(w, r, fmt.Sprintf("error deleting %s: %v", lom, err))
		}
		return
	}
	// EC cleanup if EC is enabled
	ec.ECM.CleanupObject(lom)
}
