// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
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
			p.headBckS3(w, r, apitems[0])
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
			q := r.URL.Query()
			_, versioning := q[s3compat.URLParamVersioning]
			if versioning {
				p.getBckVersioningS3(w, r, apitems[0])
				return
			}
			// only bucket name - list objects in the bucket
			p.bckListS3(w, r, apitems[0])
			return
		}
		// object data otherwise
		p.getObjS3(w, r, apitems)
	case http.MethodPut:
		if len(apitems) == 0 {
			p.invalmsghdlr(w, r, "object or bucket name required")
			return
		}
		if len(apitems) == 1 {
			q := r.URL.Query()
			_, versioning := q[s3compat.URLParamVersioning]
			if versioning {
				p.putBckVersioningS3(w, r, apitems[0])
				return
			}
			p.putBckS3(w, r, apitems[0])
			return
		}
		p.putObjS3(w, r, apitems)
	case http.MethodDelete:
		if len(apitems) == 0 {
			p.invalmsghdlr(w, r, "object or bucket name required")
			return
		}
		if len(apitems) == 1 {
			q := r.URL.Query()
			_, multiple := q[s3compat.URLParamMultiDelete]
			if multiple {
				p.delMultipleObjs(w, r, apitems[0])
				return
			}
			p.delBckS3(w, r, apitems[0])
			return
		}
		p.delObjS3(w, r, apitems)
	default:
		s := fmt.Sprintf("Invalid HTTP Method: %v %s", r.Method, r.URL.Path)
		p.invalmsghdlr(w, r, s)
	}
}

// GET s3/
func (p *proxyrunner) bckNamesToS3(w http.ResponseWriter) {
	bmd := p.owner.bmd.get()
	query := cmn.QueryBcks{Provider: cmn.ProviderAIS}
	cp := &query.Provider

	resp := s3compat.NewListBucketResult()
	bmd.Range(cp, nil, func(bck *cluster.Bck) bool {
		if query.Equal(bck.Bck) || query.Contains(bck.Bck) {
			resp.Add(bck)
		}
		return false
	})

	b := resp.MustMarshal()
	w.Header().Set("Content-Type", s3compat.ContentType)
	w.Write(b)
}

// PUT s3/bck-name
func (p *proxyrunner) putBckS3(w http.ResponseWriter, r *http.Request, bucket string) {
	bck := cluster.NewBck(bucket, cmn.ProviderAIS, cmn.NsGlobal)
	if err := cmn.ValidateBckName(bucket); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	msg := cmn.ActionMsg{Action: cmn.ActCreateLB}
	if p.forwardCP(w, r, &msg, bucket, nil) {
		return
	}
	if err := p.createBucket(&msg, bck); err != nil {
		errCode := http.StatusInternalServerError
		if _, ok := err.(*cmn.ErrorBucketAlreadyExists); ok {
			errCode = http.StatusConflict
		}
		p.invalmsghdlr(w, r, err.Error(), errCode)
	}
}

// DEL s3/bck-name
// TODO: AWS allows to delete bucket only if it is empty
func (p *proxyrunner) delBckS3(w http.ResponseWriter, r *http.Request, bucket string) {
	bck := cluster.NewBck(bucket, cmn.ProviderAIS, cmn.NsGlobal)
	msg := cmn.ActionMsg{Action: cmn.ActDestroyLB}
	if err := bck.Init(p.owner.bmd, p.si); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusNotFound)
		return
	}
	if err := bck.AllowDELETE(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if p.forwardCP(w, r, &msg, bucket, nil) {
		return
	}
	if err := p.destroyBucket(&msg, bck); err != nil {
		errCode := http.StatusInternalServerError
		if _, ok := err.(*cmn.ErrorBucketAlreadyExists); ok {
			glog.Infof("%s: %s already %q-ed, nothing to do", p.si, bck, msg.Action)
			return
		}
		p.invalmsghdlr(w, r, err.Error(), errCode)
	}
}

// DEL s3/bck-name?delete
// Delete list of objects
func (p *proxyrunner) delMultipleObjs(w http.ResponseWriter, r *http.Request, bucket string) {
	defer r.Body.Close()
	bck := cluster.NewBck(bucket, cmn.ProviderAIS, cmn.NsGlobal)
	if err := bck.Init(p.owner.bmd, p.si); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusNotFound)
		return
	}
	if err := bck.AllowDELETE(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	decoder := xml.NewDecoder(r.Body)
	objList := &s3compat.Delete{}
	if err := decoder.Decode(objList); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if len(objList.Object) == 0 {
		return
	}
	msg := cmn.ActionMsg{Action: cmn.ActDelete}
	query := make(url.Values)
	query.Set(cmn.URLParamProvider, cmn.ProviderAIS)
	listMsg := &cmn.ListMsg{ObjNames: make([]string, len(objList.Object))}
	for _, obj := range objList.Object {
		listMsg.ObjNames = append(listMsg.ObjNames, obj.Key)
	}
	msg.Value = listMsg
	if err := p.doListRange(http.MethodDelete, bucket, &msg, query); err != nil {
		p.invalmsghdlr(w, r, err.Error())
	}
}

// HEAD s3/bck-name
// TODO: AWS CLI does not use this API, so it is not tested for comatibility.
// Requesting with cURL works.
func (p *proxyrunner) headBckS3(w http.ResponseWriter, r *http.Request, bucket string) {
	bck := cluster.NewBck(bucket, cmn.ProviderAIS, cmn.NsGlobal)
	if err := bck.Init(p.owner.bmd, p.si); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusNotFound)
		return
	}
	if err := bck.AllowHEAD(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	// From AWS docs:
	// This operation is useful to determine if a bucket exists and you have
	// permission to access it. The operation returns a 200 OK if the bucket
	// exists and you have permission to access it. Otherwise, the operation
	// might return responses such as 404 Not Found and 403 Forbidden.
	//
	// So, as a basic implementation, it is enough to return status 200.
}

// GET s3/bckName
func (p *proxyrunner) bckListS3(w http.ResponseWriter, r *http.Request, bucket string) {
	bck := cluster.NewBck(bucket, cmn.ProviderAIS, cmn.NsGlobal)
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

// DEL s3/bckName/objName
func (p *proxyrunner) delObjS3(w http.ResponseWriter, r *http.Request, items []string) {
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
	if err = bck.AllowDELETE(); err != nil {
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

// GET s3/bk-name?versioning
func (p *proxyrunner) getBckVersioningS3(w http.ResponseWriter, r *http.Request, bucket string) {
	bck := cluster.NewBck(bucket, cmn.ProviderAIS, cmn.NsGlobal)
	if err := bck.Init(p.owner.bmd, nil); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	props, exists := p.owner.bmd.get().Get(bck)
	if !exists {
		p.invalmsghdlr(w, r, "bucket does not exists", http.StatusNotFound)
		return
	}
	resp := s3compat.NewVersioningConfiguration(props.Versioning.Enabled)
	b := resp.MustMarshal()
	w.Header().Set("Content-Type", s3compat.ContentType)
	w.Write(b)
}

// PUT s3/bk-name?versioning
func (p *proxyrunner) putBckVersioningS3(w http.ResponseWriter, r *http.Request, bucket string) {
	msg := &cmn.ActionMsg{Action: cmn.ActSetBprops}
	if p.forwardCP(w, r, msg, bucket, nil) {
		return
	}
	bck := cluster.NewBck(bucket, cmn.ProviderAIS, cmn.NsGlobal)
	if err := bck.Init(p.owner.bmd, nil); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	_, exists := p.owner.bmd.get().Get(bck)
	if !exists {
		p.invalmsghdlr(w, r, "bucket does not exists", http.StatusNotFound)
		return
	}
	decoder := xml.NewDecoder(r.Body)
	defer r.Body.Close()
	vconf := &s3compat.VersioningConfiguration{}
	if err := decoder.Decode(vconf); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	enabled := vconf.Enabled()
	propsToUpdate := cmn.BucketPropsToUpdate{
		Versioning: &cmn.VersionConfToUpdate{Enabled: &enabled},
	}
	if err := p.setBucketProps(msg, bck, propsToUpdate); err != nil {
		p.invalmsghdlr(w, r, err.Error())
	}
}
