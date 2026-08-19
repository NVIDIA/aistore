// Package ais: internal unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestCluPostVerify(t *testing.T) {
	var (
		secret = []byte(tstJoinSecret)
		body   = []byte(tstJoinBody)
		p      = &proxy{}
		config = &cmn.Config{}
	)
	p.joinSecret = secret

	for _, test := range []struct {
		name     string
		apiOp    string
		flags    cos.NodeStateFlags
		signed   bool
		wantErr  bool
		wantAuth bool
	}{
		{name: "unsigned self-join", apiOp: apc.SelfJoin, wantErr: true},
		{name: "signed self-join", apiOp: apc.SelfJoin, signed: true, wantAuth: true},
		{name: "unsigned restarted keepalive", apiOp: apc.Keepalive, flags: cos.NodeRestarted, wantErr: true},
		{name: "signed restarted keepalive", apiOp: apc.Keepalive, flags: cos.NodeRestarted, signed: true, wantAuth: true},
		{name: "ordinary keepalive deferred", apiOp: apc.Keepalive},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
			if test.signed {
				signNodeJoin(nodeJoinRequestHMACDomain, secret, body, req.Header)
			}
			c := &clupost{
				p: p, r: req, config: config, apiOp: test.apiOp, body: body,
				regReq: cluMeta{Flags: test.flags},
			}

			err := c.verify()
			tassert.Fatalf(t, (err != nil) == test.wantErr, "expected error %t, got %v", test.wantErr, err)
			tassert.Fatalf(t, c.joinVerified == test.wantAuth,
				"expected authenticated %t, got %t", test.wantAuth, c.joinVerified)
			if err != nil {
				herr := cmn.AsErrHTTP(err)
				tassert.Fatalf(t, herr != nil && herr.Status == http.StatusUnauthorized,
					"expected status %d, got %v", http.StatusUnauthorized, err)
			}

			if c.joinVerified {
				req.Header = make(http.Header)
				tassert.CheckFatal(t, c._verify())
			}
		})
	}
}

// primary => joining node: the self-join response carries a verifiable proof, including the
// empty-body no-op case; with no secret configured the response is written unsigned
func TestSelfJoinResponseSigning(t *testing.T) {
	var (
		secret  = []byte(tstJoinSecret)
		body    = []byte(tstJoinBody)
		maxSkew = 10 * time.Minute
	)
	p := &proxy{}
	p.joinSecret = secret

	// no-op self-join (msync == false): empty body, signed
	rec := httptest.NewRecorder()
	(&clupost{p: p, w: rec, apiOp: apc.SelfJoin}).dispatch(false)
	tassert.CheckFatal(t, verifyNodeJoin(nodeJoinResponseHMACDomain, secret, rec.Body.Bytes(), rec.Header(), maxSkew))

	// cluster-meta response
	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	p.writeJoinJSON(rec, req, map[string]any{"config": map[string]string{"key": "value"}}, "test",
		nodeJoinResponseHMACDomain)
	tassert.CheckFatal(t, verifyNodeJoin(nodeJoinResponseHMACDomain, secret, rec.Body.Bytes(), rec.Header(), maxSkew))

	// not configured => unsigned, and still a well-formed response
	h := &htrun{}
	rec = httptest.NewRecorder()
	h.writeJoinJSON(rec, req, body, "test", nodeJoinResponseHMACDomain)
	tassert.Errorf(t, rec.Body.Len() > 0 && rec.Header().Get(apc.HdrJoinSig) == "", "unexpected unsigned response")
}
