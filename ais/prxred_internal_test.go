// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func newTestProxy(t *testing.T, cskEnabled bool) *proxy {
	t.Helper()

	cos.InitShortID(0)

	p := &proxy{}
	p.owner.csk.init()

	p.owner.csk.store(&clusterKey{
		secret:  []byte("0123456789abcdef"), // 16 bytes
		ver:     42,
		created: 1,
	})
	p.owner.csk.nonce.Store(100)

	p.si = &meta.Snode{}
	p.si.Init("p1", apc.Proxy)
	p.si.PubNet.URL = "http://proxy:8080"
	p.si.ControlNet.URL = "http://proxy:8080"

	config := cmn.GCO.BeginUpdate()
	config.Auth.ClusterKey = new(cmn.ClusterKeyConf)
	config.Auth.ClusterKey.Enabled = cskEnabled
	cmn.GCO.CommitUpdate(config)

	cmn.Rom.Set(&config.ClusterConfig)

	return p
}

func newTestSnode(t *testing.T) *meta.Snode {
	t.Helper()
	si := &meta.Snode{}
	si.Init("t1", apc.Target)
	si.PubNet.URL = "http://dst:8080"
	si.ControlNet.URL = "http://dst:8080"
	return si
}

func newReq(method, path, rawQuery string) *http.Request {
	u := &url.URL{
		Scheme:   "http",
		Host:     "proxy:8080",
		Path:     path,
		RawQuery: rawQuery,
	}
	r := &http.Request{
		Method:        method,
		URL:           u,
		Host:          u.Host,
		ContentLength: -1,
	}
	return r
}

func parseRedirect(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatalf("failed to parse redirect %q: %v", s, err)
	}
	return u
}

func TestRedurl_PlainFast(t *testing.T) {
	p := newTestProxy(t, false /*CSK disabled*/)
	dst := newTestSnode(t)
	now := time.Now().UnixNano()
	smapVer := int64(123)

	r := newReq(http.MethodGet, "/v1/buckets", "a=1&b=2")
	out := p.redurl(r, dst, smapVer, now, cmn.NetIntraControl, "")

	u := parseRedirect(t, out)

	if u.Host != "dst:8080" {
		t.Fatalf("expected host dst:8080, got %s", u.Host)
	}
	if u.Path != r.URL.Path {
		t.Fatalf("expected path %q, got %q", r.URL.Path, u.Path)
	}

	q := u.Query()
	if got := q.Get("a"); got != "1" {
		t.Fatalf("expected original query a=1, got %q", got)
	}
	if got := q.Get("b"); got != "2" {
		t.Fatalf("expected original query b=2, got %q", got)
	}
	if q.Get(apc.QparamPID) == "" || q.Get(apc.QparamUnixTime) == "" {
		t.Fatalf("expected pid & utm in query, got %v", q)
	}
	if q.Get(apc.QparamSmapVer) != "" || q.Get(apc.QparamNonce) != "" || q.Get(apc.QparamHMAC) != "" {
		t.Fatalf("plain redirect must not contain CSK params, got %v", q)
	}
}

func TestRedurl_PlainSlow(t *testing.T) {
	// path with special character to force slow path
	p := newTestProxy(t, false)
	dst := newTestSnode(t)
	now := time.Now().UnixNano()

	r := newReq(http.MethodPut, "/v1/obj with space", "x=1")
	out := p.redurl(r, dst, 10, now, cmn.NetIntraControl, "")

	u := parseRedirect(t, out)
	// ensure we can parse and the query is intact
	q := u.Query()
	if q.Get("x") != "1" {
		t.Fatalf("expected x=1, got %v", q)
	}
	if q.Get(apc.QparamPID) == "" || q.Get(apc.QparamUnixTime) == "" {
		t.Fatalf("expected pid & utm, got %v", q)
	}
}

func TestRedurl_SignedFast(t *testing.T) {
	p := newTestProxy(t, true /*CSK enabled*/)
	dst := newTestSnode(t)
	now := time.Now().UnixNano()
	smapVer := int64(777)

	r := newReq(http.MethodPatch, "/v1/signed", "q=ok")
	out := p.redurl(r, dst, smapVer, now, cmn.NetIntraControl, "")

	u := parseRedirect(t, out)
	q := u.Query()

	// original query must remain
	if q.Get("q") != "ok" {
		t.Fatalf("expected q=ok, got %v", q)
	}
	// control params
	if q.Get(apc.QparamPID) == "" || q.Get(apc.QparamUnixTime) == "" {
		t.Fatalf("missing pid/utm: %v", q)
	}
	// CSK params present
	vpams := q.Get(apc.QparamSmapVer)
	nonce := q.Get(apc.QparamNonce)
	sig := q.Get(apc.QparamHMAC)
	if vpams == "" || nonce == "" || sig == "" {
		t.Fatalf("missing CSK params: %v", q)
	}
	if len(sig) != cskSigLen {
		t.Fatalf("expected HMAC len %d, got %d", cskSigLen, len(sig))
	}
	// base36 parseability sanity check
	if _, err := strconv.ParseInt(vpams, cskBase, 64); err != nil {
		t.Fatalf("vpams=%q not valid base36: %v", vpams, err)
	}
	if _, err := strconv.ParseUint(nonce, cskBase, 64); err != nil {
		t.Fatalf("nonce=%q not valid base36: %v", nonce, err)
	}
}

func TestRedurl_SignedSlow(t *testing.T) {
	p := newTestProxy(t, true)
	dst := newTestSnode(t)
	now := time.Now().UnixNano()

	// path that triggers HasSpecialSymbols
	r := newReq(http.MethodGet, "/v1/signed slow/файл", "a=1")
	out := p.redurl(r, dst, 5, now, cmn.NetIntraControl, "")

	u := parseRedirect(t, out)
	q := u.Query()

	if q.Get("a") != "1" {
		t.Fatalf("expected a=1, got %v", q)
	}
	if q.Get(apc.QparamHMAC) == "" {
		t.Fatalf("expected HMAC, got %v", q)
	}
}

func TestSigner_VerifyRoundTrip(t *testing.T) {
	p := newTestProxy(t, true /* CSK enabled */)
	dst := newTestSnode(t)
	now := time.Now().UnixNano()
	smapVer := int64(888)

	// 1) generate signed redirect URL
	orig := newReq(http.MethodGet, "/v1/signed-verify", "q=ok")
	signed := p.redurl(orig, dst, smapVer, now, cmn.NetIntraControl, "")

	u := parseRedirect(t, signed)

	// 2) reconstruct incoming request on receiver side
	rOK := &http.Request{
		Method:        orig.Method,
		URL:           u,
		Host:          u.Host,
		ContentLength: orig.ContentLength,
	}

	// assert
	if !cmn.Rom.CSKEnabled() {
		t.Fatal("CSK must be enabled for verify test")
	}

	// 3) verify with signer
	q := rOK.URL.Query()
	pid := q.Get(apc.QparamPID)
	cskgrp, err := cskFromQ(rOK.URL.Query())
	tassert.CheckFatal(t, err)

	sign := &signer{
		r: rOK,
		h: &p.htrun,
	}
	status, err := sign.verify(pid, cskgrp)
	if err != nil || status != 0 {
		t.Fatalf("verify() failed for valid signed URL: status=%d, err=%v, url=%q", status, err, signed)
	}

	// 4) negative case: tamper a field that is part of HMAC (path)
	uBad := *u // shallow copy
	uBad.Path = u.Path + "-tampered"

	rBad := &http.Request{
		Method:        orig.Method,
		URL:           &uBad,
		Host:          uBad.Host,
		ContentLength: orig.ContentLength,
	}

	q = rBad.URL.Query()
	pid = q.Get(apc.QparamPID)
	cskgrp, err = cskFromQ(rBad.URL.Query())
	tassert.CheckFatal(t, err)
	signBad := &signer{
		r: rBad,
		h: &p.htrun,
	}
	status, err = signBad.verify(pid, cskgrp)
	if err == nil || status != http.StatusUnauthorized {
		t.Fatalf("expected verify() to fail with 401 for tampered path, got status=%d, err=%v", status, err)
	}
}
