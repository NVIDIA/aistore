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
)

// [METHOD] /s3
func (p *proxyrunner) s3Handler(w http.ResponseWriter, r *http.Request) {
	apitems, err := p.checkRESTItems(w, r, 0, true, s3compat.Root)
	if err != nil {
		return
	}

	switch r.Method {
	case http.MethodHead:
		if len(apitems) == 0 {
			p.invalmsghdlr(w, r, "HEAD reuires a bucket or an object")
			return
		}
		if len(apitems) == 1 {
			p.invalmsghdlr(w, r, "HEAD for bucket is not implemnted yet")
			return
		}
		p.headObjS3(w, r, apitems)
	case http.MethodGet:
		if len(apitems) == 0 {
			// nothing  - list all the buckets
			p.bckNamesToS3(w)
			return
		}
		if len(apitems) == 1 {
			// only bucket name - list objects in the bucket
			p.bckListS3(w, r, apitems)
			return
		}
		// object data otherwise
		p.getObjS3(w, r, apitems)
	case http.MethodPut:
		if len(apitems) < 2 {
			p.invalmsghdlr(w, r, "objectname required")
			return
		}
		p.putObjS3(w, r, apitems)
	default:
		s := fmt.Sprintf("Invalid HTTP Method: %v %s", r.Method, r.URL.Path)
		p.invalmsghdlr(w, r, s)
	}
}

// GET s3/
func (p *proxyrunner) bckNamesToS3(w http.ResponseWriter) {
	bmd := p.owner.bmd.get()
	bck := cluster.NewBck("", cmn.ProviderAIS, cmn.NsGlobal)
	bcks := p.selectBMDBuckets(bmd, bck)
	resp := s3compat.NewListBucketResult()
	for _, bck := range bcks {
		resp.Add(&bck)
	}
	b := resp.MustMarshal()
	w.Header().Set("Content-Type", s3compat.ContentType)
	w.Write(b)
}

// GET s3/bckName
func (p *proxyrunner) bckListS3(w http.ResponseWriter, r *http.Request, items []string) {
	bck := cluster.NewBck(items[0], cmn.ProviderAIS, cmn.NsGlobal)
	if err := bck.Init(p.owner.bmd, nil); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	var (
		bckList *cmn.BucketList
		taskID  string
		err     error
	)
	smsg := cmn.SelectMsg{Fast: false, TimeFormat: time.RFC3339}
	smsg.AddProps(cmn.GetPropsSize, cmn.GetPropsChecksum, cmn.GetPropsAtime, cmn.GetPropsVersion)
	s3compat.FillMsgFromS3Query(r.URL.Query(), &smsg)
	_, taskID, err = p.listAISBucket(bck, smsg)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	smsg.TaskID = taskID
	for {
		bckList, taskID, err = p.listAISBucket(bck, smsg)
		if err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		if bckList != nil {
			break
		}
		// just in case
		smsg.TaskID = taskID
		time.Sleep(time.Second)
	}
	resp := s3compat.NewListObjectResult()
	resp.PageMarker = smsg.PageMarker
	resp.FillFromAisBckList(bckList)
	b := resp.MustMarshal()
	w.Header().Set("Content-Type", s3compat.ContentType)
	w.Write(b)
}

// PUT s3/bckName/objName
func (p *proxyrunner) putObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	started := time.Now()
	bck := cluster.NewBck(items[0], cmn.ProviderAIS, cmn.NsGlobal)
	if err := bck.Init(p.owner.bmd, nil); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if len(items) < 2 {
		p.invalmsghdlr(w, r, "object name is undefined")
		return
	}
	var (
		si   *cluster.Snode
		smap = p.owner.smap.get()
		err  error
	)
	if err = bck.AllowPUT(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	objName := path.Join(items[1:]...)
	si, err = cluster.HrwTarget(bck.MakeUname(objName), &smap.Smap)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("AISS3: %s %s/%s => %s", r.Method, bck, objName, si)
	}
	redirectURL := p.redirectURL(r, si, started, cmn.NetworkIntraData)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}

// GET s3/bckName/objName
func (p *proxyrunner) getObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	started := time.Now()
	bck := cluster.NewBck(items[0], cmn.ProviderAIS, cmn.NsGlobal)
	if err := bck.Init(p.owner.bmd, nil); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if len(items) < 2 {
		p.invalmsghdlr(w, r, "object name is undefined")
		return
	}
	var (
		si   *cluster.Snode
		smap = p.owner.smap.get()
		err  error
	)
	if err = bck.AllowGET(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	objName := path.Join(items[1:]...)
	si, err = cluster.HrwTarget(bck.MakeUname(objName), &smap.Smap)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("AISS3: %s %s/%s => %s", r.Method, bck, objName, si)
	}
	redirectURL := p.redirectURL(r, si, started, cmn.NetworkIntraData)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}

func (p *proxyrunner) headObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	started := time.Now()
	if len(items) < 2 {
		p.invalmsghdlr(w, r, "object name is missing")
		return
	}
	bucket, objName := items[0], path.Join(items[1:]...)
	bck := cluster.NewBck(items[0], cmn.ProviderAIS, cmn.NsGlobal)
	if err := bck.Init(p.owner.bmd, nil); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if err := bck.AllowHEAD(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	smap := p.owner.smap.get()
	si, err := cluster.HrwTarget(bck.MakeUname(objName), &smap.Smap)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusInternalServerError)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("AISS3 %s %s/%s => %s", r.Method, bucket, objName, si)
	}
	redirectURL := p.redirectURL(r, si, started, cmn.NetworkIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}
