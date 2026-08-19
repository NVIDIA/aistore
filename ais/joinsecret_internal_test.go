// Package ais: internal unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/tools/tassert"
)

const (
	tstJoinSecret = "0123456789abcdef0123456789abcdef"
	tstJoinBody   = `{"node":"t1"}`
	tstJoinTime   = "1755561600"
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

func TestNodeJoinSignatureKAT(t *testing.T) {
	var (
		secret = []byte(tstJoinSecret)
		body   = []byte(tstJoinBody)
	)
	for _, test := range []struct{ domain, want string }{
		{nodeJoinRequestHMACDomain, "a9NK5Ua2ZMix-krdfAcVdQDbBAafDfJP2BZ3C4kCN40"},
		{nodeJoinResponseHMACDomain, "kEBxqfZvidUz3s_VieJ3KaFmA19dl0ZKwrZQu36lhNQ"},
	} {
		t.Run(test.domain, func(t *testing.T) {
			got := nodeJoinSignature(test.domain, secret, body, tstJoinTime)
			tassert.Errorf(t, got == test.want, "wire format changed: expected %q, got %q", test.want, got)
		})
	}
}

// every field must be bound: flipping any one of them must change the proof
func TestNodeJoinSignatureBinding(t *testing.T) {
	var (
		secret = []byte(tstJoinSecret)
		body   = []byte(tstJoinBody)
		base   = nodeJoinSignature(nodeJoinRequestHMACDomain, secret, body, tstJoinTime)
	)
	for _, test := range []struct {
		name string
		sig  string
	}{
		{"domain", nodeJoinSignature(nodeJoinResponseHMACDomain, secret, body, tstJoinTime)},
		{"secret", nodeJoinSignature(nodeJoinRequestHMACDomain, []byte("other"), body, tstJoinTime)},
		{"body", nodeJoinSignature(nodeJoinRequestHMACDomain, secret, []byte(`{"node":"t2"}`), tstJoinTime)},
		{"timestamp", nodeJoinSignature(nodeJoinRequestHMACDomain, secret, body, "1755561601")},
	} {
		t.Run(test.name, func(t *testing.T) {
			tassert.Errorf(t, test.sig != base, "%s is not bound into the proof", test.name)
		})
	}

	// without separators, both tuples would produce the same "abcd" input
	a := nodeJoinSignature("a", secret, []byte("d"), "bc")
	b := nodeJoinSignature("ab", secret, []byte("d"), "c")
	tassert.Errorf(t, a != b, "field boundaries are ambiguous")
}

func TestVerifyNodeJoin(t *testing.T) {
	var (
		secret  = []byte(tstJoinSecret)
		body    = []byte(tstJoinBody)
		hdr     = make(http.Header, 2)
		maxSkew = 10 * time.Minute
	)
	signNodeJoin(nodeJoinRequestHMACDomain, secret, body, hdr)
	tassert.CheckFatal(t, verifyNodeJoin(nodeJoinRequestHMACDomain, secret, body, hdr, maxSkew))

	for _, test := range []struct {
		name   string
		secret []byte
		body   []byte
		hdr    http.Header
		want   error
	}{
		{name: "unsigned", secret: secret, body: body, hdr: http.Header{}, want: errJoinTimestamp},
		{name: "wrong secret", secret: []byte("wrong"), body: body, hdr: hdr, want: errJoinSignature},
		{name: "tampered body", secret: secret, body: []byte("other"), hdr: hdr, want: errJoinSignature},
		{name: "invalid timestamp", secret: secret, body: body, hdr: http.Header{
			apc.HdrJoinTime: []string{"invalid"}, apc.HdrJoinSig: hdr.Values(apc.HdrJoinSig),
		}, want: errJoinTimestamp},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := verifyNodeJoin(nodeJoinRequestHMACDomain, test.secret, test.body, test.hdr, maxSkew)
			tassert.Errorf(t, errors.Is(err, test.want), "expected %v, got %v", test.want, err)
		})
	}

	// a request proof must not verify as a response proof, and vice versa
	err := verifyNodeJoin(nodeJoinResponseHMACDomain, secret, body, hdr, maxSkew)
	tassert.Errorf(t, errors.Is(err, errJoinSignature), "expected reflected signature to fail, got %v", err)
	signNodeJoin(nodeJoinResponseHMACDomain, secret, body, hdr)
	tassert.CheckFatal(t, verifyNodeJoin(nodeJoinResponseHMACDomain, secret, body, hdr, maxSkew))

	// within the skew window, then outside it
	timestamp := strconv.FormatInt(time.Now().Add(-2*time.Minute).Unix(), 10)
	hdr.Set(apc.HdrJoinTime, timestamp)
	hdr.Set(apc.HdrJoinSig, nodeJoinSignature(nodeJoinRequestHMACDomain, secret, body, timestamp))
	tassert.CheckFatal(t, verifyNodeJoin(nodeJoinRequestHMACDomain, secret, body, hdr, maxSkew))

	timestamp = strconv.FormatInt(time.Now().Add(-2*maxSkew).Unix(), 10)
	hdr.Set(apc.HdrJoinTime, timestamp)
	hdr.Set(apc.HdrJoinSig, nodeJoinSignature(nodeJoinRequestHMACDomain, secret, body, timestamp))
	err = verifyNodeJoin(nodeJoinRequestHMACDomain, secret, body, hdr, maxSkew)
	tassert.Errorf(t, errors.Is(err, errJoinExpired), "expected expired timestamp, got %v", err)

	// a future timestamp within the window is accepted (clock skew cuts both ways)
	timestamp = strconv.FormatInt(time.Now().Add(2*time.Minute).Unix(), 10)
	hdr.Set(apc.HdrJoinTime, timestamp)
	hdr.Set(apc.HdrJoinSig, nodeJoinSignature(nodeJoinRequestHMACDomain, secret, body, timestamp))
	tassert.CheckFatal(t, verifyNodeJoin(nodeJoinRequestHMACDomain, secret, body, hdr, maxSkew))
}
