// Package ais: internal unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestLoadNodeJoinSecret(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "secret")
	tassert.CheckFatal(t, os.WriteFile(path, []byte(" secret\n"), 0o400))
	secret, err := loadNodeJoinSecret(path)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, string(secret) == " secret", "unexpected secret %q", secret)
	for _, mode := range []os.FileMode{0o440, 0o404, 0o622} {
		tassert.CheckFatal(t, os.Chmod(path, mode))
		_, err = loadNodeJoinSecret(path)
		tassert.Errorf(t, err != nil, "expected permissions %04o to fail", mode)
	}

	empty := filepath.Join(dir, "empty")
	tassert.CheckFatal(t, os.WriteFile(empty, nil, 0o400))
	_, err = loadNodeJoinSecret(empty)
	tassert.Errorf(t, err != nil, "expected empty secret to fail")
	_, err = loadNodeJoinSecret(filepath.Join(dir, "missing"))
	tassert.Errorf(t, err != nil, "expected missing secret to fail")
}

func TestSelfJoinAuthentication(t *testing.T) {
	var (
		secret  = []byte("0123456789abcdef0123456789abcdef")
		body    = []byte(`{"node":"t1"}`)
		hdr     = make(http.Header, 2)
		maxSkew = 10 * time.Minute
	)
	signNodeJoin(nodeJoinRequestHMACDomain, secret, body, hdr)
	tassert.CheckFatal(t, verifyNodeJoin(nodeJoinRequestHMACDomain, secret, body, hdr, maxSkew))

	tests := []struct {
		name   string
		secret []byte
		body   []byte
		hdr    http.Header
		want   string
	}{
		{name: "unsigned", secret: secret, body: body, hdr: http.Header{}, want: "invalid node-join timestamp"},
		{name: "wrong secret", secret: []byte("wrong"), body: body, hdr: hdr, want: "invalid node-join signature"},
		{name: "tampered body", secret: secret, body: []byte("other"), hdr: hdr, want: "invalid node-join signature"},
		{name: "invalid timestamp", secret: secret, body: body, hdr: http.Header{
			apc.HdrJoinTime: []string{"invalid"}, apc.HdrJoinSig: hdr.Values(apc.HdrJoinSig),
		}, want: "invalid node-join timestamp"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := verifyNodeJoin(nodeJoinRequestHMACDomain, test.secret, test.body, test.hdr, maxSkew)
			tassert.Errorf(t, err != nil && err.Error() == test.want, "expected %q, got %v", test.want, err)
		})
	}
	err := verifyNodeJoin(nodeJoinResponseHMACDomain, secret, body, hdr, maxSkew)
	tassert.Errorf(t, err != nil && err.Error() == "invalid node-join signature", "expected reflected signature to fail, got %v", err)
	signNodeJoin(nodeJoinResponseHMACDomain, secret, body, hdr)
	tassert.CheckFatal(t, verifyNodeJoin(nodeJoinResponseHMACDomain, secret, body, hdr, maxSkew))
	p := &proxy{}
	p.joinSecret = secret

	rec := httptest.NewRecorder()
	(&clupost{p: p, w: rec, apiOp: apc.SelfJoin}).dispatch(false)
	tassert.CheckFatal(t, verifyNodeJoin(nodeJoinResponseHMACDomain, secret, rec.Body.Bytes(), rec.Header(), maxSkew))
	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	p.writeJoinJSON(rec, req, map[string]any{"config": map[string]string{"key": "value"}}, "test",
		nodeJoinResponseHMACDomain)
	tassert.CheckFatal(t, verifyNodeJoin(nodeJoinResponseHMACDomain, secret, rec.Body.Bytes(), rec.Header(), maxSkew))

	h := &htrun{}
	rec = httptest.NewRecorder()
	h.writeJoinJSON(rec, req, body, "test", nodeJoinResponseHMACDomain) // no secret => unsigned
	tassert.Errorf(t, rec.Body.Len() > 0 && rec.Header().Get(apc.HdrJoinSig) == "", "unexpected unsigned response")

	timestamp := strconv.FormatInt(time.Now().Add(-2*time.Minute).Unix(), 10)
	hdr.Set(apc.HdrJoinTime, timestamp)
	hdr.Set(apc.HdrJoinSig, nodeJoinSignature(nodeJoinRequestHMACDomain, secret, body, timestamp))
	tassert.CheckFatal(t, verifyNodeJoin(nodeJoinRequestHMACDomain, secret, body, hdr, maxSkew))

	timestamp = strconv.FormatInt(time.Now().Add(-2*maxSkew).Unix(), 10)
	hdr.Set(apc.HdrJoinTime, timestamp)
	hdr.Set(apc.HdrJoinSig, nodeJoinSignature(nodeJoinRequestHMACDomain, secret, body, timestamp))
	err = verifyNodeJoin(nodeJoinRequestHMACDomain, secret, body, hdr, maxSkew)
	tassert.Errorf(t, err != nil && err.Error() == "expired node-join timestamp", "expected expired timestamp, got %v", err)
}
