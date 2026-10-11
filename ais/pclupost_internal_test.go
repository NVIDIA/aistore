// Package ais: internal unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestKeepaliveConfirmationSnapshot(t *testing.T) {
	osi := &meta.Snode{}
	osi.Init("target", apc.Target, nil)
	osi.ControlNet.Init("http", "127.0.0.1", "9080")
	nsi := osi.Clone()
	nsi.ControlNet.Init("http", "127.0.0.1", "9081")
	p := &proxy{}
	p.si = &meta.Snode{}
	p.si.Init("primary", apc.Proxy, nil)
	restarted := osi.Clone()
	restarted.VerifyingKey = make([]byte, 32)
	changed := osi.Clone()
	changed.ControlNet.Init("http", "127.0.0.1", "9082")
	for _, test := range []struct {
		name    string
		current *meta.Snode
		sver    int64
		ok      bool
		err     error
		reject  bool
	}{
		{name: "confirmed", current: osi, sver: 5, ok: true},
		{name: "same version, replaced pointer", current: osi.Clone(), sver: 5, ok: true, reject: true},
		{name: "unrelated Smap update", current: osi.Clone(), sver: 6, ok: true},
		{name: "restarted at same endpoints", current: restarted, sver: 6, ok: true, reject: true},
		{name: "changed endpoints", current: changed, sver: 6, ok: true, reject: true},
		{name: "duplicate ID", current: osi, sver: 5, reject: true},
		{name: "failed probe", current: osi, sver: 5, err: errors.New("probe timed out"), reject: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			smap := newSmap()
			smap.Version = test.sver
			c := &clupost{p: p, nsi: nsi, smap: smap}
			c.confirm.osi, c.confirm.ok, c.confirm.err = osi, test.ok, test.err
			c.confirm.smapVer = 5
			update, err := c.kalive(test.current)
			if test.reject {
				tassert.Fatalf(t, !update && errors.Is(err, errKaliveRejected),
					"expected rejected keepalive, got update=%t, err=%v", update, err)
			} else {
				tassert.Fatalf(t, update && err == nil,
					"expected accepted keepalive, got update=%t, err=%v", update, err)
			}
		})
	}
}

func TestKeepaliveRejectVersion(t *testing.T) {
	p := &proxy{}
	p.si = &meta.Snode{}
	p.si.Init("primary", apc.Proxy, nil)
	rejected := fmt.Errorf("%w: test", errKaliveRejected)
	for _, test := range []struct {
		ver    string
		reject bool
	}{
		{"5.1", false},
		{"5.2.rc1", true},
		{"5.2", true},
		{"6.0", true},
	} {
		c := &clupost{p: p, nversStr: test.ver}
		err := c.rejectKalive(rejected)
		if test.reject {
			tassert.Fatalf(t, errors.Is(err, errKaliveRejected),
				"version %q: expected errKaliveRejected, got %v", test.ver, err)
		} else {
			tassert.Fatalf(t, err == nil, "version %q: expected nil error, got %v", test.ver, err)
		}
	}
}

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
