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
)

const (
	testProxyID = "p1"
	testDstID   = "t1"
)

func newTestProxy(t *testing.T, signVerifyEnabled bool) *proxy {
	t.Helper()

	old := cmn.GCO.Get()
	t.Cleanup(func() {
		cmn.GCO.Put(old)
		cmn.Rom.Set(&old.ClusterConfig)
	})

	cos.InitShortID(0)

	config := cmn.GCO.BeginUpdate()
	config.Auth.IntraCluster = new(cmn.IntraClusterConf)
	config.Auth.IntraCluster.Enabled = signVerifyEnabled
	cmn.GCO.CommitUpdate(config)
	cmn.Rom.Set(&config.ClusterConfig)

	p := &proxy{}
	p.owner.csk.init()
	p.owner.csk.store(&clusterKey{
		secret:  []byte("0123456789abcdef"), // 16 bytes
		ver:     42,
		created: 1,
	})
	p.owner.csk.nonce.Store(100)

	p.si = &meta.Snode{}
	p.si.Init(testProxyID, apc.Proxy)
	p.si.PubNet.URL = "http://proxy:8080"
	p.si.ControlNet.URL = "http://proxy:8080"

	return p
}

func newTestSnode(t *testing.T) *meta.Snode {
	t.Helper()

	si := &meta.Snode{}
	si.Init(testDstID, apc.Target)
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
	return &http.Request{
		Method:        method,
		URL:           u,
		Host:          u.Host,
		ContentLength: -1,
	}
}

func parseRedirect(t *testing.T, s string) *url.URL {
	t.Helper()

	u, err := url.Parse(s)
	if err != nil {
		t.Fatalf("failed to parse redirect %q: %v", s, err)
	}
	return u
}

func makeRedirect(t *testing.T, signVerifyEnabled bool, method, path, rawQuery string, smapVer int64) (*proxy, *http.Request, *url.URL) {
	t.Helper()

	p := newTestProxy(t, signVerifyEnabled)
	dst := newTestSnode(t)
	req := newReq(method, path, rawQuery)

	out := p.redurl(req, dst, smapVer, time.Now().UnixNano(), cmn.NetIntraControl, "")
	return p, req, parseRedirect(t, out)
}

func requireDstURL(t *testing.T, u *url.URL, wantPath string) {
	t.Helper()

	if u.Host != "dst:8080" {
		t.Fatalf("expected host dst:8080, got %q", u.Host)
	}
	if u.Path != wantPath {
		t.Fatalf("expected path %q, got %q", wantPath, u.Path)
	}
}

func requireRedirectParams(t *testing.T, q url.Values) {
	t.Helper()

	if q.Get(apc.QparamPID) == "" {
		t.Fatalf("missing %s: %v", apc.QparamPID, q)
	}
	if q.Get(apc.QparamUnixTime) == "" {
		t.Fatalf("missing %s: %v", apc.QparamUnixTime, q)
	}
}

func requireNoCSKParams(t *testing.T, q url.Values) {
	t.Helper()

	if q.Get(apc.QparamSmapVer) != "" || q.Get(apc.QparamNonce) != "" || q.Get(apc.QparamHMAC) != "" {
		t.Fatalf("plain redirect must not contain sign/verify params, got %v", q)
	}
}

func requireCSKParams(t *testing.T, q url.Values) {
	t.Helper()

	smapVer := q.Get(apc.QparamSmapVer)
	nonce := q.Get(apc.QparamNonce)
	sig := q.Get(apc.QparamHMAC)
	if smapVer == "" || nonce == "" || sig == "" {
		t.Fatalf("missing sign/verify params: %v", q)
	}
	if len(sig) != cskSigLen {
		t.Fatalf("expected HMAC len %d, got %d", cskSigLen, len(sig))
	}
	if _, err := strconv.ParseInt(smapVer, cskBase, 64); err != nil {
		t.Fatalf("%s=%q not valid base%d: %v", apc.QparamSmapVer, smapVer, cskBase, err)
	}
	if _, err := strconv.ParseUint(nonce, cskBase, 64); err != nil {
		t.Fatalf("%s=%q not valid base%d: %v", apc.QparamNonce, nonce, cskBase, err)
	}
}

func requireQueryValue(t *testing.T, q url.Values, key, want string) {
	t.Helper()

	if got := q.Get(key); got != want {
		t.Fatalf("expected %s=%q, got %q; query=%v", key, want, got, q)
	}
}

func TestRedurlPlain(t *testing.T) {
	tests := []struct {
		name    string
		method  string
		path    string
		query   string
		wantKey string
		wantVal string
		smapVer int64
	}{
		{
			name:    "fast path",
			method:  http.MethodGet,
			path:    "/v1/buckets",
			query:   "a=1&b=2",
			wantKey: "a",
			wantVal: "1",
			smapVer: 123,
		},
		{
			name:    "slow path with special path",
			method:  http.MethodPut,
			path:    "/v1/obj with space",
			query:   "orig=1",
			wantKey: "orig",
			wantVal: "1",
			smapVer: 10,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, req, u := makeRedirect(t, false /*signVerifyEnabled*/, tc.method, tc.path, tc.query, tc.smapVer)
			requireDstURL(t, u, req.URL.Path)

			q := u.Query()
			requireQueryValue(t, q, tc.wantKey, tc.wantVal)
			requireRedirectParams(t, q)
			requireNoCSKParams(t, q)
		})
	}
}

func TestRedurlSigned(t *testing.T) {
	tests := []struct {
		name    string
		method  string
		path    string
		query   string
		wantKey string
		wantVal string
		smapVer int64
	}{
		{
			name:    "fast path",
			method:  http.MethodPatch,
			path:    "/v1/signed",
			query:   "q=ok",
			wantKey: "q",
			wantVal: "ok",
			smapVer: 777,
		},
		{
			name:    "slow path with special path",
			method:  http.MethodGet,
			path:    "/v1/signed slow/файл",
			query:   "a=1",
			wantKey: "a",
			wantVal: "1",
			smapVer: 5,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, req, u := makeRedirect(t, true /*signVerifyEnabled*/, tc.method, tc.path, tc.query, tc.smapVer)
			requireDstURL(t, u, req.URL.Path)

			q := u.Query()
			requireQueryValue(t, q, tc.wantKey, tc.wantVal)
			requireRedirectParams(t, q)
			requireCSKParams(t, q)
		})
	}
}

func TestSignerVerifyRoundTrip(t *testing.T) {
	p, orig, u := makeRedirect(t, true /*signVerifyEnabled*/, http.MethodGet, "/v1/signed-verify", "q=ok", 888)

	if !cmn.Rom.SignVerifyEnabled() {
		t.Fatal("sign/verify must be enabled for verify test")
	}

	rOK := &http.Request{
		Method:        orig.Method,
		URL:           u,
		Host:          u.Host,
		ContentLength: orig.ContentLength,
	}

	q := rOK.URL.Query()
	pid := q.Get(apc.QparamPID)
	cskgrp, err := cskFromQ(q)
	if err != nil {
		t.Fatalf("failed to parse sign/verify params: %v", err)
	}
	if cskgrp == nil {
		t.Fatal("expected sign/verify params, got nil")
	}

	sign := &signer{r: rOK, h: &p.htrun}
	status, err := sign.verify(pid, cskgrp)
	if err != nil || status != 0 {
		t.Fatalf("verify() failed for valid signed URL: status=%d, err=%v, url=%q", status, err, u.String())
	}

	uBad := *u
	uBad.Path = u.Path + "-tampered"

	rBad := &http.Request{
		Method:        orig.Method,
		URL:           &uBad,
		Host:          uBad.Host,
		ContentLength: orig.ContentLength,
	}

	q = rBad.URL.Query()
	pid = q.Get(apc.QparamPID)
	cskgrp, err = cskFromQ(q)
	if err != nil {
		t.Fatalf("failed to parse sign/verify params from tampered URL: %v", err)
	}
	if cskgrp == nil {
		t.Fatal("expected sign/verify params from tampered URL, got nil")
	}

	signBad := &signer{r: rBad, h: &p.htrun}
	status, err = signBad.verify(pid, cskgrp)
	if err == nil || status != http.StatusUnauthorized {
		t.Fatalf("expected verify() to fail with 401 for tampered path, got status=%d, err=%v", status, err)
	}
}
