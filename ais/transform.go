// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */

package ais

import (
	"net/http"

	"github.com/NVIDIA/aistore/cmn"
)

//==========
//
// Handlers
//
//==========

// [METHOD] /v1/transform
func (t *targetrunner) transformHandler(w http.ResponseWriter, r *http.Request) {
	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Transform)
	if err != nil {
		return
	}

	switch {
	case r.Method == http.MethodPost && apitems[0] == cmn.TransformInit:
		t.initTransform(w, r)
	default:
		t.invalmsghdlrf(w, r, "Invalid HTTP Method: %v %s", r.Method, r.URL.Path)
	}
}

// [METHOD] /v1/transform
func (p *proxyrunner) transformHandler(w http.ResponseWriter, r *http.Request) {
	apitems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Transform)
	if err != nil {
		return
	}

	switch {
	case r.Method == http.MethodPost && apitems[0] == cmn.TransformInit:
		p.httpproxyinittransform(w, r)
	default:
		p.invalmsghdlrf(w, r, "Invalid HTTP Method: %v %s", r.Method, r.URL.Path)
	}
}

// POST /v1/transform/init
func (p *proxyrunner) httpproxyinittransform(w http.ResponseWriter, r *http.Request) {
	transformID := cmn.GenUUID()
	// Perform validation on body
	results := p.callTargets(http.MethodPost, cmn.URLPath(cmn.Version, cmn.Transform, cmn.TransformInit), nil)
	for res := range results {
		if res.err != nil {
			p.invalmsghdlr(w, r, res.err.Error())
			return
		}
	}
	w.Write([]byte(transformID))

}

func (t *targetrunner) initTransform(w http.ResponseWriter, r *http.Request) {
	return
}
