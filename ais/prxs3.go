// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	jsoniter "github.com/json-iterator/go"
)

var (
	errS3Req = errors.New("invalid s3 request")
	errS3Obj = errors.New("missing or empty object name")
)

// [METHOD] /s3
func (p *proxy) s3Handler(w http.ResponseWriter, r *http.Request) {
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("S3Request: %s - %s", r.Method, r.URL)
	}

	// TODO: Fix the hack, https://github.com/tensorflow/tensorflow/issues/41798
	cos.ReparseQuery(r)
	apiItems, err := p.apiItems(w, r, 0, true, apc.URLPathS3.L)
	if err != nil {
		return
	}

	switch r.Method {
	case http.MethodHead:
		if len(apiItems) == 0 {
			s3.WriteErr(w, r, errS3Req, 0)
			return
		}
		if len(apiItems) == 1 {
			p.headBckS3(w, r, apiItems[0])
			return
		}
		p.headObjS3(w, r, apiItems)
	case http.MethodGet:
		if len(apiItems) == 0 {
			// list all buckets; NOTE: compare with `p.easyURLHandler` and see
			// "list buckets for a given provider" comment there
			p.bckNamesFromBMD(w)
			return
		}
		var (
			q            = r.URL.Query()
			_, lifecycle = q[s3.QparamLifecycle]
			_, policy    = q[s3.QparamPolicy]
			_, cors      = q[s3.QparamCORS]
			_, acl       = q[s3.QparamACL]
		)
		if lifecycle || policy || cors || acl {
			p.unsupported(w, r, apiItems[0])
			return
		}
		listMultipart := q.Has(s3.QparamMptUploads)
		if len(apiItems) == 1 && !listMultipart {
			_, versioning := q[s3.QparamVersioning]
			if versioning {
				p.getBckVersioningS3(w, r, apiItems[0])
				return
			}
			// only bucket name - list objects in the bucket
			p.bckListS3(w, r, apiItems[0])
			return
		}
		// object data otherwise
		p.getObjS3(w, r, apiItems, q, listMultipart)
	case http.MethodPut:
		if len(apiItems) == 0 {
			s3.WriteErr(w, r, errS3Req, 0)
			return
		}
		if len(apiItems) == 1 {
			q := r.URL.Query()
			_, versioning := q[s3.QparamVersioning]
			if versioning {
				p.putBckVersioningS3(w, r, apiItems[0])
				return
			}
			p.putBckS3(w, r, apiItems[0])
			return
		}
		p.putObjS3(w, r, apiItems)
	case http.MethodPost:
		q := r.URL.Query()
		if q.Has(s3.QparamMptUploadID) || q.Has(s3.QparamMptUploads) {
			p.handleMptUpload(w, r, apiItems)
			return
		}
		if len(apiItems) != 1 {
			s3.WriteErr(w, r, errS3Req, 0)
			return
		}
		if _, multiple := q[s3.QparamMultiDelete]; !multiple {
			s3.WriteErr(w, r, errS3Req, 0)
			return
		}
		p.delMultipleObjs(w, r, apiItems[0])
	case http.MethodDelete:
		if len(apiItems) == 0 {
			s3.WriteErr(w, r, errS3Req, 0)
			return
		}
		if len(apiItems) == 1 {
			q := r.URL.Query()
			_, multiple := q[s3.QparamMultiDelete]
			if multiple {
				p.delMultipleObjs(w, r, apiItems[0])
				return
			}
			p.delBckS3(w, r, apiItems[0])
			return
		}
		p.delObjS3(w, r, apiItems)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodHead,
			http.MethodPost, http.MethodPut)
	}
}

// GET s3/
// NOTE: unlike native API, this one is limited to list only those that are currently present in the BMD.
func (p *proxy) bckNamesFromBMD(w http.ResponseWriter) {
	var (
		bmd  = p.owner.bmd.get()
		resp = s3.NewListBucketResult() // https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
	)
	bmd.Range(nil /*any provider*/, nil /*any namespace*/, func(bck *cluster.Bck) bool {
		resp.Add(bck)
		return false
	})
	sgl := p.gmm.NewSGL(0)
	resp.MustMarshal(sgl)
	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	sgl.WriteTo(w)
	sgl.Free()
}

// PUT s3/bck-name (i.e., create bucket)
func (p *proxy) putBckS3(w http.ResponseWriter, r *http.Request, bucket string) {
	msg := apc.ActionMsg{Action: apc.ActCreateBck}
	if p.forwardCP(w, r, nil, msg.Action+"-"+bucket) {
		return
	}
	bck := cluster.NewBck(bucket, apc.ProviderAIS, cmn.NsGlobal)
	if err := bck.Validate(); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	if err := p.createBucket(&msg, bck); err != nil {
		errCode := http.StatusInternalServerError
		if _, ok := err.(*cmn.ErrBucketAlreadyExists); ok {
			errCode = http.StatusConflict
		}
		s3.WriteErr(w, r, err, errCode)
	}
}

// DEL s3/bck-name
// TODO: AWS allows to delete bucket only if it is empty
func (p *proxy) delBckS3(w http.ResponseWriter, r *http.Request, bucket string) {
	bck, err, errCode := cluster.InitByNameOnly(bucket, p.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, errCode)
		return
	}
	if err := bck.Allow(apc.AceDestroyBucket); err != nil {
		s3.WriteErr(w, r, err, http.StatusForbidden)
		return
	}
	msg := apc.ActionMsg{Action: apc.ActDestroyBck}
	if p.forwardCP(w, r, nil, msg.Action+"-"+bucket) {
		return
	}
	if err := p.destroyBucket(&msg, bck); err != nil {
		errCode := http.StatusInternalServerError
		if _, ok := err.(*cmn.ErrBucketAlreadyExists); ok {
			glog.Infof("%s: %s already %q-ed, nothing to do", p, bck, msg.Action)
			return
		}
		s3.WriteErr(w, r, err, errCode)
	}
}

func (p *proxy) handleMptUpload(w http.ResponseWriter, r *http.Request, parts []string) {
	bucket := parts[0]
	bck, err, errCode := cluster.InitByNameOnly(bucket, p.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, errCode)
		return
	}
	if err := bck.Allow(apc.AcePUT); err != nil {
		s3.WriteErr(w, r, err, http.StatusForbidden)
		return
	}
	smap := p.owner.smap.get()
	objName := s3.ObjName(parts)
	si, err := cluster.HrwTarget(bck.MakeUname(objName), &smap.Smap)
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	started := time.Now()
	redirectURL := p.redirectURL(r, si, started, cmn.NetIntraData)
	p.s3Redirect(w, r, si, redirectURL, bck.Name)
}

// DEL s3/bck-name?delete
// Delete list of objects
func (p *proxy) delMultipleObjs(w http.ResponseWriter, r *http.Request, bucket string) {
	bck, err, errCode := cluster.InitByNameOnly(bucket, p.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, errCode)
		return
	}
	if err := bck.Allow(apc.AceObjDELETE); err != nil {
		s3.WriteErr(w, r, err, http.StatusForbidden)
		return
	}
	decoder := xml.NewDecoder(r.Body)
	objList := &s3.Delete{}
	if err := decoder.Decode(objList); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	if len(objList.Object) == 0 {
		return
	}

	var (
		msg   = apc.ActionMsg{Action: apc.ActDeleteObjects}
		lrMsg = &cmn.SelectObjsMsg{ObjNames: make([]string, 0, len(objList.Object))}
	)
	for _, obj := range objList.Object {
		lrMsg.ObjNames = append(lrMsg.ObjNames, obj.Key)
	}
	msg.Value = lrMsg

	// Marshal+Unmashal to new struct:
	// hack to make `doListRange` treat `listMsg` as `map[string]interface`
	var (
		msg2  apc.ActionMsg
		bt    = cos.MustMarshal(&msg)
		query = make(url.Values, 1)
	)
	query.Set(apc.QparamProvider, apc.ProviderAIS)
	if err := jsoniter.Unmarshal(bt, &msg2); err != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, p, "list-range action message", cos.BHead(bt), err)
		s3.WriteErr(w, r, err, 0)
		return
	}
	if _, err := p.doListRange(r.Method, bucket, &msg2, query); err != nil {
		s3.WriteErr(w, r, err, 0)
	}
}

// HEAD s3/bck-name
func (p *proxy) headBckS3(w http.ResponseWriter, r *http.Request, bucket string) {
	bck, err, errCode := cluster.InitByNameOnly(bucket, p.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, errCode)
		return
	}
	if err := bck.Allow(apc.AceBckHEAD); err != nil {
		s3.WriteErr(w, r, err, http.StatusForbidden)
		return
	}
	// From https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html:
	//
	// "This operation is useful to determine if a bucket exists and you have
	// permission to access it. The operation returns a 200 OK if the bucket
	// exists and you have permission to access it. Otherwise, the operation
	// might return responses such as 404 Not Found and 403 Forbidden."
	//
	// But it appears that Amazon always adds region to the response,
	// and AWS CLI uses it.
	w.Header().Set(cos.HdrServer, s3.AISServer)
	w.Header().Set(s3.HdrBckRegion, s3.AISRegion)
}

// GET s3/bckName
func (p *proxy) bckListS3(w http.ResponseWriter, r *http.Request, bucket string) {
	bck, err, errCode := cluster.InitByNameOnly(bucket, p.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, errCode)
		return
	}
	lsmsg := apc.ListObjsMsg{UUID: cos.GenUUID(), TimeFormat: time.RFC3339}
	lsmsg.AddProps(apc.GetPropsSize, apc.GetPropsChecksum, apc.GetPropsAtime, apc.GetPropsVersion)
	s3.FillMsgFromS3Query(r.URL.Query(), &lsmsg)

	var (
		objList       *cmn.BucketList
		locationIsAIS = bck.IsAIS() || lsmsg.IsFlagSet(apc.LsPresent)
	)
	if locationIsAIS {
		objList, err = p.listObjectsAIS(bck, &lsmsg)
	} else {
		objList, err = p.listObjectsRemote(bck, &lsmsg)
	}
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}

	resp := s3.NewListObjectResult()
	resp.ContinuationToken = lsmsg.ContinuationToken
	resp.FillFromAisBckList(objList, &lsmsg)
	sgl := p.gmm.NewSGL(0)
	resp.MustMarshal(sgl)
	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	sgl.WriteTo(w)
	sgl.Free()
}

// PUT s3/bckName/objName - with HeaderObjSrc in request header - a source
func (p *proxy) copyObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	src := r.Header.Get(s3.HdrObjSrc)
	src = strings.Trim(src, "/")
	parts := strings.SplitN(src, "/", 2)
	if len(parts) < 2 {
		s3.WriteErr(w, r, errS3Obj, 0)
		return
	}
	// src
	bckSrc, err, errCode := cluster.InitByNameOnly(parts[0], p.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, errCode)
		return
	}
	if err := bckSrc.Allow(apc.AceGET); err != nil {
		s3.WriteErr(w, r, err, http.StatusForbidden)
		return
	}
	// dst
	bckDst, err, errCode := cluster.InitByNameOnly(items[0], p.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, errCode)
		return
	}
	var (
		si   *cluster.Snode
		smap = p.owner.smap.get()
	)
	if err = bckDst.Allow(apc.AcePUT); err != nil {
		s3.WriteErr(w, r, err, http.StatusForbidden)
		return
	}
	objName := strings.Trim(parts[1], "/")
	si, err = cluster.HrwTarget(bckSrc.MakeUname(objName), &smap.Smap)
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("AISS3 COPY: %s %s/%s => %s/%v %s", r.Method, bckSrc, objName, bckDst, items, si)
	}
	started := time.Now()
	redirectURL := p.redirectURL(r, si, started, cmn.NetIntraData)
	p.s3Redirect(w, r, si, redirectURL, bckDst.Name)
}

// PUT s3/bckName/objName - without extra info in request header
func (p *proxy) directPutObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	bucket := items[0]
	bck, err, errCode := cluster.InitByNameOnly(bucket, p.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, errCode)
		return
	}
	var (
		si   *cluster.Snode
		smap = p.owner.smap.get()
	)
	if err = bck.Allow(apc.AcePUT); err != nil {
		s3.WriteErr(w, r, err, http.StatusForbidden)
		return
	}
	if len(items) < 2 {
		s3.WriteErr(w, r, errS3Obj, 0)
		return
	}
	objName := s3.ObjName(items)
	si, err = cluster.HrwTarget(bck.MakeUname(objName), &smap.Smap)
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("AISS3: %s %s/%s => %s", r.Method, bck, objName, si)
	}
	started := time.Now()
	redirectURL := p.redirectURL(r, si, started, cmn.NetIntraData)
	p.s3Redirect(w, r, si, redirectURL, bck.Name)
}

// PUT s3/bckName/objName
func (p *proxy) putObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	if r.Header.Get(s3.HdrObjSrc) == "" {
		p.directPutObjS3(w, r, items)
		return
	}
	p.copyObjS3(w, r, items)
}

// GET s3/<bucket-name/<object-name>
func (p *proxy) getObjS3(w http.ResponseWriter, r *http.Request, items []string, q url.Values, listMultipart bool) {
	bucket := items[0]
	bck, err, errCode := cluster.InitByNameOnly(bucket, p.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, errCode)
		return
	}
	var (
		si   *cluster.Snode
		smap = p.owner.smap.get()
	)
	if err = bck.Allow(apc.AceGET); err != nil {
		s3.WriteErr(w, r, err, http.StatusForbidden)
		return
	}
	if listMultipart {
		p.listMultipart(w, r, bck, q)
		return
	}
	if len(items) < 2 {
		s3.WriteErr(w, r, errS3Obj, 0)
		return
	}
	objName := s3.ObjName(items)
	si, err = cluster.HrwTarget(bck.MakeUname(objName), &smap.Smap)
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("AISS3: %s %s/%s => %s", r.Method, bck, objName, si)
	}
	started := time.Now()
	redirectURL := p.redirectURL(r, si, started, cmn.NetIntraData)
	p.s3Redirect(w, r, si, redirectURL, bck.Name)
}

func (p *proxy) listMultipart(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, q url.Values) {
	smap := p.owner.smap.get()
	if smap.CountActiveTargets() == 1 {
		si, err := cluster.HrwTarget(bck.MakeUname(""), &smap.Smap)
		if err != nil {
			s3.WriteErr(w, r, err, 0)
			return
		}
		started := time.Now()
		redirectURL := p.redirectURL(r, si, started, cmn.NetIntraData)
		p.s3Redirect(w, r, si, redirectURL, bck.Name)
		return
	}
	// bcast & aggregate
	all := &s3.ListMptUploadsResult{}
	for _, si := range smap.Tmap {
		var (
			url   = si.URL(cmn.NetPublic)
			cargs = allocCargs()
		)
		cargs.si = si
		cargs.req = cmn.HreqArgs{Method: http.MethodGet, Base: url, Path: r.URL.Path, Query: q}
		res := p.call(cargs)
		b, err := res.bytes, res.err
		freeCargs(cargs)
		freeCR(res)
		if err == nil {
			results := &s3.ListMptUploadsResult{}
			if err := xml.Unmarshal(b, results); err == nil {
				if len(results.Uploads) > 0 {
					if len(all.Uploads) == 0 {
						*all = *results
						all.Uploads = make([]s3.UploadInfoResult, 0)
					}
					all.Uploads = append(all.Uploads, results.Uploads...)
				}
			}
		}
	}
	sgl := p.gmm.NewSGL(0)
	all.MustMarshal(sgl)
	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	sgl.WriteTo(w)
	sgl.Free()
}

func (p *proxy) headObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	if len(items) < 2 {
		s3.WriteErr(w, r, errS3Obj, 0)
		return
	}
	bucket, objName := items[0], s3.ObjName(items)
	bck, err, errCode := cluster.InitByNameOnly(bucket, p.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, errCode)
		return
	}
	if err := bck.Allow(apc.AceObjHEAD); err != nil {
		s3.WriteErr(w, r, err, http.StatusForbidden)
		return
	}
	smap := p.owner.smap.get()
	si, err := cluster.HrwTarget(bck.MakeUname(objName), &smap.Smap)
	if err != nil {
		s3.WriteErr(w, r, err, http.StatusInternalServerError)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("AISS3 %s %s/%s => %s", r.Method, bucket, objName, si)
	}

	p.reverseNodeRequest(w, r, si)
}

// DEL s3/bckName/objName
func (p *proxy) delObjS3(w http.ResponseWriter, r *http.Request, items []string) {
	bucket := items[0]
	bck, err, errCode := cluster.InitByNameOnly(bucket, p.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, errCode)
		return
	}
	var (
		si   *cluster.Snode
		smap = p.owner.smap.get()
	)
	if err = bck.Allow(apc.AceObjDELETE); err != nil {
		s3.WriteErr(w, r, err, http.StatusForbidden)
		return
	}
	if len(items) < 2 {
		s3.WriteErr(w, r, errS3Obj, 0)
		return
	}
	objName := s3.ObjName(items)
	si, err = cluster.HrwTarget(bck.MakeUname(objName), &smap.Smap)
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("AISS3: %s %s/%s => %s", r.Method, bck, objName, si)
	}
	started := time.Now()
	redirectURL := p.redirectURL(r, si, started, cmn.NetIntraData)
	p.s3Redirect(w, r, si, redirectURL, bck.Name)
}

// GET s3/bk-name?versioning
func (p *proxy) getBckVersioningS3(w http.ResponseWriter, r *http.Request, bucket string) {
	bck, err, errCode := cluster.InitByNameOnly(bucket, p.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, errCode)
		return
	}
	resp := s3.NewVersioningConfiguration(bck.Props.Versioning.Enabled)
	sgl := p.gmm.NewSGL(0)
	resp.MustMarshal(sgl)
	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	sgl.WriteTo(w)
	sgl.Free()
}

// GET s3/bk-name?lifecycle|cors|policy|acl
func (p *proxy) unsupported(w http.ResponseWriter, r *http.Request, bucket string) {
	if _, err, errCode := cluster.InitByNameOnly(bucket, p.owner.bmd); err != nil {
		s3.WriteErr(w, r, err, errCode)
		return
	}
	w.WriteHeader(http.StatusNotImplemented)
}

// PUT s3/bk-name?versioning
func (p *proxy) putBckVersioningS3(w http.ResponseWriter, r *http.Request, bucket string) {
	msg := &apc.ActionMsg{Action: apc.ActSetBprops}
	if p.forwardCP(w, r, nil, msg.Action+"-"+bucket) {
		return
	}
	bck, err, errCode := cluster.InitByNameOnly(bucket, p.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, errCode)
		return
	}
	decoder := xml.NewDecoder(r.Body)
	vconf := &s3.VersioningConfiguration{}
	if err := decoder.Decode(vconf); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	enabled := vconf.Enabled()
	propsToUpdate := cmn.BucketPropsToUpdate{
		Versioning: &cmn.VersionConfToUpdate{Enabled: &enabled},
	}
	// make and validate new props
	nprops, err := p.makeNewBckProps(bck, &propsToUpdate)
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	if _, err := p.setBucketProps(msg, bck, nprops); err != nil {
		s3.WriteErr(w, r, err, 0)
	}
}
