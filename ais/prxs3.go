// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/ais/s3compat"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
)

// [METHOD] /s3
func (p *proxyrunner) s3Handler(w http.ResponseWriter, r *http.Request) {
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("S3Request: %s - %s", r.Method, r.URL)
	}
	apiItems, err := p.checkRESTItems(w, r, 0, true, cmn.S3)
	if err != nil {
		return
	}
	// TODO: Fix the hack, https://github.com/tensorflow/tensorflow/issues/41798
	cmn.ReparseQuery(r)

	switch r.Method {
	case http.MethodHead:
		if len(apiItems) == 0 {
			p.invalmsghdlr(w, r, "HEAD reuires a bucket or an object")
			return
		}
		if len(apiItems) == 1 {
			p.headBckS3(w, r, apiItems[0])
			return
		}
		p.headObjS3(w, r, apiItems)
	case http.MethodGet:
		if len(apiItems) == 0 {
			// nothing  - list all the buckets
			p.bckNamesToS3(w)
			return
		}
		if len(apiItems) == 1 {
			q := r.URL.Query()
			_, versioning := q[s3compat.URLParamVersioning]
			if versioning {
				p.getBckVersioningS3(w, r, apiItems[0])
				return
			}
			// only bucket name - list objects in the bucket
			p.bckListS3(w, r, apiItems[0])
			return
		}
		// object data otherwise
		p.getObjS3(w, r, apiItems)
	case http.MethodPut:
		if len(apiItems) == 0 {
			p.invalmsghdlr(w, r, "object or bucket name required")
			return
		}
		if len(apiItems) == 1 {
			q := r.URL.Query()
			_, versioning := q[s3compat.URLParamVersioning]
			if versioning {
				p.putBckVersioningS3(w, r, apiItems[0])
				return
			}
			p.putBckS3(w, r, apiItems[0])
			return
		}
		p.putObjS3(w, r, apiItems)
	case http.MethodPost:
		if len(apiItems) != 1 {
			p.invalmsghdlr(w, r, "bucket name expected")
			return
		}
		q := r.URL.Query()
		if _, multiple := q[s3compat.URLParamMultiDelete]; !multiple {
			p.invalmsghdlr(w, r, "invalid request")
			return
		}
		p.delMultipleObjs(w, r, apiItems[0])
	case http.MethodDelete:
		if len(apiItems) == 0 {
			p.invalmsghdlr(w, r, "object or bucket name required")
			return
		}
		if len(apiItems) == 1 {
			q := r.URL.Query()
			_, multiple := q[s3compat.URLParamMultiDelete]
			if multiple {
				p.delMultipleObjs(w, r, apiItems[0])
				return
			}
			p.delBckS3(w, r, apiItems[0])
			return
		}
		p.delObjS3(w, r, apiItems)
	default:
		p.invalmsghdlrf(w, r, "Invalid HTTP Method: %v %s", r.Method, r.URL.Path)
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
	w.Header().Set(cmn.HeaderContentType, cmn.ContentXML)
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
	if p.forwardCP(w, r, &msg, bucket) {
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
	if err := bck.Allow(cmn.AccessBckDELETE); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
		return
	}
	if p.forwardCP(w, r, &msg, bucket) {
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
	defer cmn.Close(r.Body)
	bck := cluster.NewBck(bucket, cmn.ProviderAIS, cmn.NsGlobal)
	if err := bck.Init(p.owner.bmd, p.si); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusNotFound)
		return
	}
	if err := bck.Allow(cmn.AccessObjDELETE); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
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
	listMsg := &cmn.ListMsg{ObjNames: make([]string, 0, len(objList.Object))}
	for _, obj := range objList.Object {
		listMsg.ObjNames = append(listMsg.ObjNames, obj.Key)
	}
	msg.Value = listMsg
	bt := cmn.MustMarshal(&msg)
	// Marshal+Unmashal to new struct:
	// hack to make `doListRange` treat `listMsg` as `map[string]interface`
	var msg2 cmn.ActionMsg
	err := jsoniter.Unmarshal(bt, &msg2)
	cmn.AssertNoErr(err)
	if _, err := p.doListRange(http.MethodDelete, bucket, &msg2, query); err != nil {
		p.invalmsghdlr(w, r, err.Error())
	}
}

// HEAD s3/bck-name
func (p *proxyrunner) headBckS3(w http.ResponseWriter, r *http.Request, bucket string) {
	bck := cluster.NewBck(bucket, cmn.ProviderAIS, cmn.NsGlobal)
	if err := bck.Init(p.owner.bmd, p.si); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusNotFound)
		return
	}
	if err := bck.Allow(cmn.AccessBckHEAD); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
		return
	}
	// From AWS docs:
	// This operation is useful to determine if a bucket exists and you have
	// permission to access it. The operation returns a 200 OK if the bucket
	// exists and you have permission to access it. Otherwise, the operation
	// might return responses such as 404 Not Found and 403 Forbidden.
	//
	// But, after checking Amazon response, it appeared that Amazon adds
	// region name to the response, and at least AWS CLI uses it.
	w.Header().Set("Server", s3compat.AISSever)
	w.Header().Set("x-amz-bucket-region", s3compat.AISRegion)
}

// GET s3/bckName
func (p *proxyrunner) bckListS3(w http.ResponseWriter, r *http.Request, bucket string) {
	bck := cluster.NewBck(bucket, cmn.ProviderAIS, cmn.NsGlobal)
	if err := bck.Init(p.owner.bmd, nil); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	smsg := cmn.SelectMsg{TimeFormat: time.RFC3339}
	smsg.AddProps(cmn.GetPropsSize, cmn.GetPropsChecksum, cmn.GetPropsAtime, cmn.GetPropsVersion)
	s3compat.FillMsgFromS3Query(r.URL.Query(), &smsg)

	objList, err := p.listObjectsAIS(bck, smsg)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	resp := s3compat.NewListObjectResult()
	resp.ContinuationToken = smsg.ContinuationToken
	resp.FillFromAisBckList(objList)
	b := resp.MustMarshal()
	w.Header().Set(cmn.HeaderContentType, cmn.ContentXML)
	w.Write(b)
}

// PUT s3/bckName/objName - with HeaderObjSrc in request header - a source
func (p *proxyrunner) copyObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	started := time.Now()
	src := r.Header.Get(s3compat.HeaderObjSrc)
	src = strings.Trim(src, "/") // in examples the path starts with "/"
	parts := strings.SplitN(src, "/", 2)
	if len(parts) < 2 {
		p.invalmsghdlr(w, r, "copy is not an object name")
		return
	}
	bckSrc := cluster.NewBck(parts[0], cmn.ProviderAIS, cmn.NsGlobal)
	if err := bckSrc.Init(p.owner.bmd, nil); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if err := bckSrc.Allow(cmn.AccessGET); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
		return
	}
	bckDst := cluster.NewBck(items[0], cmn.ProviderAIS, cmn.NsGlobal)
	if err := bckDst.Init(p.owner.bmd, nil); err != nil {
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
	if err = bckDst.Allow(cmn.AccessPUT); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
		return
	}
	objName := strings.Trim(parts[1], "/")
	si, err = cluster.HrwTarget(bckSrc.MakeUname(objName), &smap.Smap)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("AISS3 COPY: %s %s/%s => %s/%v %s", r.Method, bckSrc, objName, bckDst, items, si)
	}
	redirectURL := p.redirectURL(r, si, started, cmn.NetworkIntraData)
	s3Redirect(w, redirectURL, bckDst.Name)
}

func s3Redirect(w http.ResponseWriter, url, bck string) {
	h := w.Header()

	h.Set(cmn.HeaderLocation, url)
	h.Set(cmn.HeaderContentType, "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusTemporaryRedirect)
	fmt.Fprint(w, s3compat.MakeRedirectBody(url, bck))
}

// PUT s3/bckName/objName - without extra info in request header
func (p *proxyrunner) directPutObjS3(w http.ResponseWriter, r *http.Request, items []string) {
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
	if err = bck.Allow(cmn.AccessPUT); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
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
	s3Redirect(w, redirectURL, bck.Name)
}

// PUT s3/bckName/objName
func (p *proxyrunner) putObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	if r.Header.Get(s3compat.HeaderObjSrc) == "" {
		p.directPutObjS3(w, r, items)
		return
	}
	p.copyObjS3(w, r, items)
}

// GET s3/<bucket-name/<object-name>[?uuid=<etl-uuid>]
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
	if err = bck.Allow(cmn.AccessGET); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
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
	s3Redirect(w, redirectURL, bck.Name)
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
	if err := bck.Allow(cmn.AccessObjHEAD); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
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
	s3Redirect(w, redirectURL, bck.Name)
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
	if err = bck.Allow(cmn.AccessObjDELETE); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
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
	s3Redirect(w, redirectURL, bck.Name)
}

// GET s3/bk-name?versioning
func (p *proxyrunner) getBckVersioningS3(w http.ResponseWriter, r *http.Request, bucket string) {
	bck := cluster.NewBck(bucket, cmn.ProviderAIS, cmn.NsGlobal)
	if err := bck.Init(p.owner.bmd, nil); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	resp := s3compat.NewVersioningConfiguration(bck.Props.Versioning.Enabled)
	b := resp.MustMarshal()
	w.Header().Set(cmn.HeaderContentType, cmn.ContentXML)
	w.Write(b)
}

// PUT s3/bk-name?versioning
func (p *proxyrunner) putBckVersioningS3(w http.ResponseWriter, r *http.Request, bucket string) {
	msg := &cmn.ActionMsg{Action: cmn.ActSetBprops}
	if p.forwardCP(w, r, msg, bucket) {
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
	defer cmn.Close(r.Body)
	vconf := &s3compat.VersioningConfiguration{}
	if err := decoder.Decode(vconf); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	enabled := vconf.Enabled()
	propsToUpdate := cmn.BucketPropsToUpdate{
		Versioning: &cmn.VersionConfToUpdate{Enabled: &enabled},
	}
	if _, err := p.setBucketProps(w, r, msg, bck, propsToUpdate); err != nil {
		p.invalmsghdlr(w, r, err.Error())
	}
}
