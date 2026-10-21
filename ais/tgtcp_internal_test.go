// Package ais: internal unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/memsys"
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

func hdbTestReq(n int) *cmn.HdbReq {
	req := &cmn.HdbReq{}
	bck := cmn.Bck{Name: "hdb", Provider: apc.AIS, Ns: cmn.NsGlobal}
	for i := range n {
		in := cmn.HdbIn{Name: fmt.Sprintf("%064d", i), Bidx: req.AddBck(&bck)}
		in.Size = int64(i)
		in.Cksum = cos.NewCksum(cos.ChecksumOneXxh, "0123456789abcdef")
		in.SetCustomKey(cmn.ETag, "etag-0123456789abcdef")
		in.SetCustomKey(cmn.MD5ObjMD, "md5-0123456789abcdef")
		req.In = append(req.In, in)
	}
	return req
}

// the body comes from the slab (small request) or the heap (large request)
func TestHdbPack(t *testing.T) {
	for _, test := range []struct {
		n    int
		slab bool
	}{
		{apc.HdbSizeDflt, true},
		{apc.HdbSizeMax, false},
	} {
		req := hdbTestReq(test.n)
		body, bodyR := hdbPack(memsys.PageMM(), req)
		tassert.Errorf(t, (bodyR != nil) == test.slab, "%d items: slab %v, expected %v", test.n, bodyR != nil, test.slab)
		tassert.Errorf(t, len(body) == req.PackedSize(), "%d items: body size %d, expected %d", test.n, len(body), req.PackedSize())
		var got cmn.HdbReq
		tassert.CheckFatal(t, got.Unpack(body))
		tassert.Fatalf(t, len(got.In) == test.n, "%d items: unpacked %d", test.n, len(got.In))
		for i := range got.In {
			a, b := &got.In[i], &req.In[i]
			tassert.Errorf(t, a.Name == b.Name && a.Bidx == b.Bidx && a.Size == b.Size &&
				a.Cksum.Equal(b.Cksum) && maps.Equal(a.CustomMD, b.CustomMD), "item %d: %v, expected %v", i, &a.ObjAttrs, &b.ObjAttrs)
		}
		if bodyR != nil {
			tassert.CheckFatal(t, bodyR.Close())
		}
	}
}

func TestHdbBody(t *testing.T) {
	body, bodyR := hdbPack(memsys.PageMM(), hdbTestReq(apc.HdbSizeDflt))
	want := bytes.Clone(body)
	got, err := io.ReadAll(bodyR)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, bytes.Equal(got, want), "body differs from the packed request")

	tassert.CheckFatal(t, bodyR.Close())
	tassert.CheckFatal(t, bodyR.Close()) // must not free the buffer again
	_, err = bodyR.Read(make([]byte, 1))
	tassert.Fatalf(t, err == io.ErrClosedPipe, "read after close: %v", err)
}

// a body that waits for the test before it reads and tells when net/http closes it
type hdbSpyBody struct {
	io.ReadCloser
	gate   chan struct{}
	closed chan struct{}
	once   sync.Once
}

func (s *hdbSpyBody) Read(p []byte) (int, error) {
	<-s.gate
	return s.ReadCloser.Read(p)
}

func (s *hdbSpyBody) Close() error {
	s.once.Do(func() { close(s.closed) })
	return s.ReadCloser.Close()
}

// the peer replies before it reads the body
func TestHdbBodyEarlyReply(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		rc := http.NewResponseController(w)
		rc.EnableFullDuplex() // reply without a read of the body
		w.WriteHeader(http.StatusBadRequest)
		rc.Flush()
	}))
	defer srv.Close()

	body, bodyR := hdbPack(memsys.PageMM(), hdbTestReq(apc.HdbSizeDflt))
	spy := &hdbSpyBody{ReadCloser: bodyR, gate: make(chan struct{}), closed: make(chan struct{})}
	release := sync.OnceFunc(func() { close(spy.gate) })
	defer release()
	args := cmn.HreqArgs{Method: http.MethodPost, Base: srv.URL, Path: apc.URLPathObjects.S, Body: body, BodyR: spy}
	req, err := args.Req()
	tassert.CheckFatal(t, err)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	tassert.CheckFatal(t, err)
	resp.Body.Close()

	select {
	case <-spy.closed:
		t.Fatal("net/http closed the body, so the buffer is free, before it read the body")
	default:
	}
	release()
	select {
	case <-spy.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("net/http did not close the request body")
	}
}

// at critical load the receiver rejects the request with 429 before it reads the body
func TestHdbReceiverBusy(t *testing.T) {
	tgt := &target{}
	tgt.hdbGate.crit.Store(true)
	tgt.hdbGate.next.Store(mono.NanoTime() + int64(time.Hour))

	packed := hdbTestReq(apc.HdbSizeDflt).NewPack()
	body := bytes.NewReader(packed)
	ecode, err := tgt.objHeadBatch(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", body))
	tassert.Fatalf(t, err != nil && ecode == http.StatusTooManyRequests, "expecting 429, got %d (%v)", ecode, err)
	tassert.Errorf(t, body.Len() == len(packed), "the body is read: %d of %d bytes left", body.Len(), len(packed))
}

// the sender reads 400 as unsupported (per-object HEADs) and 429 as peer-busy (no fallback)
func TestHdbErr(t *testing.T) {
	tsi := &meta.Snode{DaeID: "t1"}
	for _, test := range []struct {
		status          int
		unsupp, tooMany bool
	}{
		{http.StatusBadRequest, true, false},
		{http.StatusTooManyRequests, false, true},
		{http.StatusServiceUnavailable, false, false},
	} {
		err := _hdbErr(errors.New("cause"), test.status, tsi)
		unsupp, tooMany := cmn.IsErrHdbUnsupported(err), cmn.IsErrTooManyRequests(err)
		tassert.Errorf(t, unsupp == test.unsupp && tooMany == test.tooMany,
			"status %d: unsupported %t, too many requests %t, expected (%t, %t)", test.status, unsupp, tooMany, test.unsupp, test.tooMany)
	}
}
