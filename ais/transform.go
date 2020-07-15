// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"io/ioutil"
	"net/http"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/transform"
)

//==========
//
// Handlers
//
//==========

// [METHOD] /v1/transform
func (t *targetrunner) transformHandler(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost:
		t.initTransform(w, r)
	case r.Method == http.MethodDelete:
		t.stopTransform(w, r)
	default:
		t.invalmsghdlrf(w, r, "Invalid HTTP Method: %v %s", r.Method, r.URL.Path)
	}
}

// [METHOD] /v1/transform
func (p *proxyrunner) transformHandler(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost:
		p.httpproxyinittransform(w, r)
	case r.Method == http.MethodDelete:
		p.httpproxystoptransform(w, r)
	default:
		p.invalmsghdlrf(w, r, "Invalid HTTP Method: %v %s", r.Method, r.URL.Path)
	}
}

// POST /v1/transform/init
func (p *proxyrunner) httpproxyinittransform(w http.ResponseWriter, r *http.Request) {
	_, err := p.checkRESTItems(w, r, 0, false, cmn.Version, cmn.Transform, cmn.TransformInit)
	if err != nil {
		return
	}

	var (
		transformID = cmn.GenUUID()
	)
	spec, err := ioutil.ReadAll(r.Body)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	r.Body.Close()

	pod, err := transform.ParsePodSpec(spec)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	if err = transform.ValidateSpec(pod); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	var (
		timeout, _  = transform.PodTransformTimeout(pod)
		commType, _ = transform.PodTransformCommType(pod)
		msg         = transform.Msg{
			ID:          transformID,
			Spec:        spec,
			WaitTimeout: cmn.DurationJSON(timeout),
			CommType:    commType,
		}

		results = p.bcastTo(bcastArgs{
			req: cmn.ReqArgs{
				Method: http.MethodPost,
				Path:   r.URL.Path,
				Body:   cmn.MustMarshal(msg),
			},
			timeout: cmn.LongTimeout,
			to:      cluster.Targets,
		})
	)

	for res := range results {
		if res.err != nil {
			p.invalmsghdlr(w, r, res.err.Error())
			return
		}
	}
	w.Write([]byte(transformID))
}

// DELETE /v1/transform/stop/uuid
func (p *proxyrunner) httpproxystoptransform(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Transform, cmn.TransformStop)
	if err != nil {
		return
	}

	id := apiItems[0]
	if id == "" {
		p.invalmsghdlr(w, r, "transform id cannot be empty")
		return
	}

	results := p.callTargets(http.MethodDelete, r.URL.Path, nil)
	for res := range results {
		if res.err != nil {
			p.invalmsghdlr(w, r, res.err.Error())
			return
		}
	}
}

func (t *targetrunner) initTransform(w http.ResponseWriter, r *http.Request) {
	_, err := t.checkRESTItems(w, r, 0, false, cmn.Version, cmn.Transform, cmn.TransformInit)
	if err != nil {
		return
	}

	var msg transform.Msg
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		return
	}
	if err := transform.StartTransformationPod(t, msg); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
}

func (t *targetrunner) stopTransform(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Transform, cmn.TransformStop)
	if err != nil {
		return
	}
	id := apiItems[0]
	if err := transform.StopTransformationPod(id); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
}

func (t *targetrunner) doTransform(w http.ResponseWriter, r *http.Request, transformID string, bck *cluster.Bck, objName string) {
	comm, err := transform.GetCommunicator(transformID)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	if err := comm.DoTransform(w, r, bck, objName); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
}
