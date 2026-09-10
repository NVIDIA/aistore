// Package ais: internal unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools/tassert"

	jsoniter "github.com/json-iterator/go"
)

// pub-net /v1/daemon: read-only and only when direct target access is permitted
func TestDaePubReadOnly(t *testing.T) {
	for _, method := range []string{http.MethodPut, http.MethodPost, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			w := _daePub(method, "")
			tassert.Fatalf(t, w.Code == http.StatusForbidden, "expected %s to be rejected, got status %d", method, w.Code)
		})
	}
}

func TestDaePubProxyMediation(t *testing.T) {
	tests := []struct {
		name               string
		clientAuthRequired bool
		intraRequestAuth   bool
		expectedCode       int
	}{
		{"no-auth", false, false, http.StatusOK},
		{"client-auth-required", true, false, http.StatusForbidden},
		{"intra-request-auth", false, true, http.StatusForbidden},
		{"both", true, true, http.StatusForbidden},
	}
	orig := cmn.GCO.Get().Auth
	t.Cleanup(func() { _setAuth(&orig) })

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_setAuth(&cmn.AuthConf{
				ClientAuthRequired: test.clientAuthRequired,
				IntraCluster:       &cmn.IntraClusterConf{RequestAuth: test.intraRequestAuth},
			})

			w := _daePub(http.MethodGet, apc.WhatSnode)
			tassert.Fatalf(t, w.Code == test.expectedCode,
				"(client_auth_required=%t, intra_cluster.request_auth=%t): expected status %d, got %d",
				test.clientAuthRequired, test.intraRequestAuth, test.expectedCode, w.Code)

			if test.expectedCode != http.StatusForbidden {
				return
			}
			herr := &cmn.ErrHTTP{}
			tassert.CheckFatal(t, jsoniter.Unmarshal(w.Body.Bytes(), herr))
			tassert.Fatalf(t, herr.Message == errDirectTargetAccess.Error(),
				"expected %q, got %q", errDirectTargetAccess, herr.Message)
		})
	}
}

func _setAuth(auth *cmn.AuthConf) {
	config := cmn.GCO.BeginUpdate()
	config.Auth = *auth
	cmn.GCO.CommitUpdate(config)
	cmn.Rom.Set(&config.ClusterConfig)
}

func _daePub(method, what string) *httptest.ResponseRecorder {
	u := apc.URLPathDae.S
	if what != "" {
		u += "?" + url.Values{apc.QparamWhat: []string{what}}.Encode()
	}
	req := httptest.NewRequest(method, u, http.NoBody)
	req = req.WithContext(context.WithValue(req.Context(), keyReqNet, reqNetPub))

	w := httptest.NewRecorder()
	mockTarget.daePubHandler(w, req)
	return w
}

const (
	testTargetID  = "t1-verify"
	testPrimaryID = "p1-verify"
)

func newTestTarget(t *testing.T, auth *cmn.AuthConf) *target {
	t.Helper()

	orig := cmn.GCO.Get()
	t.Cleanup(func() {
		cmn.GCO.Put(orig)
		cmn.Rom.Set(&orig.ClusterConfig)
	})

	cos.InitShortID(0)

	config := cmn.GCO.BeginUpdate()
	config.Auth = *auth
	cmn.GCO.CommitUpdate(config)
	cmn.Rom.Set(&config.ClusterConfig)

	pub, priv, err := cos.GenerateNodeKeyPair()
	tassert.CheckFatal(t, err)

	tgt := newTarget(newConfigOwner(config))
	tgt.htrun.nodeKeyPair = cos.NewNodeKeyPair(priv, pub)
	tgt.si = &meta.Snode{}
	tgt.si.Init(testTargetID, apc.Target, pub)
	tgt.si.PubNet.URL = "http://target:8081"
	tgt.si.ControlNet.URL = "http://target:9081"

	ppub, _, err := cos.GenerateNodeKeyPair()
	tassert.CheckFatal(t, err)
	psi := &meta.Snode{}
	psi.Init(testPrimaryID, apc.Proxy, ppub)
	psi.PubNet.URL = "http://proxy:8080"
	psi.ControlNet.URL = "http://proxy:9080"

	smap := newSmap()
	smap.Tmap = meta.NodeMap{tgt.si.ID(): tgt.si}
	smap.Pmap = meta.NodeMap{psi.ID(): psi}
	smap.Primary = psi
	smap.Version = 1
	tgt.owner.smap = newSmapOwner(config, true /*isTarget*/)
	tgt.owner.smap.put(smap)

	tgt.htrun.svs.init()
	tgt.htrun.toggleSignVerify(cmn.Rom.SignVerifyEnabled())

	return tgt
}

// past the post-toggle grace window
func expireSignVerifyGrace(h *htrun) {
	cur := h.svs.cur.Load()
	h.svs.cur.Store(&_sv{on: cur.on, last: cur.last - int64(time.Hour)})
}

func forgedRedirect(method string) (*http.Request, *dpq) {
	u := &url.URL{Path: "/v1/objects/bck/obj", RawQuery: "pid=attacker&utm=1"}
	r := &http.Request{Method: method, URL: u, Header: http.Header{}}
	r = r.WithContext(context.WithValue(r.Context(), keyReqNet, reqNetPub))

	dpq := &dpq{}
	dpq.sys.pid = "attacker"
	dpq.sys.ptime = "1"
	return r, dpq
}

func TestObjVerbForgedRedirect(t *testing.T) {
	tests := []struct {
		name               string
		clientAuthRequired bool
		intraRequestAuth   bool
		expectedCode       int
	}{
		{"no-auth", false, false, 0},
		{"client-auth-required", true, false, 0},
		{"intra-request-auth", false, true, http.StatusUnauthorized},
		{"both", true, true, http.StatusUnauthorized},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tgt := newTestTarget(t, &cmn.AuthConf{
				ClientAuthRequired: test.clientAuthRequired,
				IntraCluster:       &cmn.IntraClusterConf{RequestAuth: test.intraRequestAuth},
			})
			expireSignVerifyGrace(&tgt.htrun)

			for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
				r, dpq := forgedRedirect(method)
				ecode, err := tgt.checkObjVerb(r, dpq)
				if test.expectedCode == 0 {
					tassert.Fatalf(t, err == nil, "%s: expected accept, got ecode=%d err=%v", method, ecode, err)
					continue
				}
				tassert.Fatalf(t, err != nil && ecode == test.expectedCode,
					"%s: expected ecode=%d, got ecode=%d err=%v", method, test.expectedCode, ecode, err)
			}
		})
	}
}
