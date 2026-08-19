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
	"github.com/NVIDIA/aistore/tools/tassert"
)

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
