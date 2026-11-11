// Package ais: internal unit tests
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/authn/tok"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/tools/tassert"

	"github.com/golang-jwt/jwt/v5"
	jsoniter "github.com/json-iterator/go"
)

//
// redirect signing
//

const (
	testProxyID = "p1"
	testDstID   = "t1"
)

func newTestProxy(t *testing.T, requestAuthConfigured bool) *proxy {
	t.Helper()

	old := cmn.GCO.Get()
	t.Cleanup(func() {
		cmn.GCO.Put(old)
		cmn.Rom.Set(&old.ClusterConfig)
	})

	cos.InitShortID(0)

	config := cmn.GCO.BeginUpdate()
	config.Auth.IntraCluster = new(cmn.IntraClusterConf)
	config.Auth.IntraCluster.RequestAuth = requestAuthConfigured
	cmn.GCO.CommitUpdate(config)
	cmn.Rom.Set(&config.ClusterConfig)

	p := &proxy{}

	pub, priv, err := cos.GenerateNodeKeyPair()
	if err != nil {
		t.Fatalf("failed to generate keys: %v", err)
	}
	p.htrun.nodeKeyPair = cos.NewNodeKeyPair(priv, pub)

	p.si = &meta.Snode{}
	p.si.Init(testProxyID, apc.Proxy, pub)
	p.si.PubNet.URL = "http://proxy:8080"
	p.si.ControlNet.URL = "http://proxy:8080"

	smap := newSmap()
	smap.Pmap = meta.NodeMap{p.si.ID(): p.si}
	smap.Primary = p.si
	smap.Version = 1

	p.owner.smap = newSmapOwner(config, false /*isTarget*/)
	p.owner.smap.put(smap) // populate the atomic.Pointer so get() works

	// mirror node startup order: svs.init -> keypair -> config-driven toggle
	p.htrun.svs.init()

	p.htrun.toggleSignVerify(cmn.Rom.SignVerifyEnabled())

	return p
}

func newTestSnode(t *testing.T) *meta.Snode {
	t.Helper()

	pub, _, err := cos.GenerateNodeKeyPair()
	if err != nil {
		t.Fatalf("failed to generate dst keys: %v", err)
	}

	si := &meta.Snode{}
	si.Init(testDstID, apc.Target, pub)

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

func makeRedirect(t *testing.T, requestAuthConfigured bool, method, path, rawQuery string, smapVer int64) (*proxy, *http.Request, *url.URL) {
	t.Helper()

	p := newTestProxy(t, requestAuthConfigured)
	dst := newTestSnode(t)
	req := newReq(method, path, rawQuery)

	out := p.redurl(req, dst, smapVer, cmn.NetIntraControl, "")
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

func requireNoSignVerifyParams(t *testing.T, q url.Values) {
	t.Helper()

	if q.Get(apc.QparamSmapVer) != "" || q.Get(apc.QparamNonce) != "" || q.Get(apc.QparamSig) != "" {
		t.Fatalf("plain redirect must not contain sign/verify params, got %v", q)
	}
}

func requireSignVerifyParams(t *testing.T, q url.Values) {
	t.Helper()

	smapVer := q.Get(apc.QparamSmapVer)
	nonce := q.Get(apc.QparamNonce)
	sig := q.Get(apc.QparamSig)
	if smapVer == "" || nonce == "" || sig == "" {
		t.Fatalf("missing sign/verify params: %v", q)
	}
	if len(sig) != sigLen() {
		t.Fatalf("expected signature len %d, got %d", sigLen(), len(sig))
	}
	if _, err := strconv.ParseInt(smapVer, svNumBase, 64); err != nil {
		t.Fatalf("%s=%q not valid base%d: %v", apc.QparamSmapVer, smapVer, svNumBase, err)
	}
	if _, err := strconv.ParseUint(nonce, svNumBase, 64); err != nil {
		t.Fatalf("%s=%q not valid base%d: %v", apc.QparamNonce, nonce, svNumBase, err)
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
			requireNoSignVerifyParams(t, q)
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
			requireSignVerifyParams(t, q)
		})
	}
}

func TestSignerVerifyRoundTrip(t *testing.T) {
	p, orig, u := makeRedirect(t, true /*signVerifyEnabled*/, http.MethodGet, "/v1/signed-verify", "q=ok", 888)

	if !cmn.Rom.SignVerifyEnabled() {
		t.Fatal("sign/verify must be enabled for verify test")
	}

	// 1. valid signed URL verifies
	if status, err := p.verify(u, orig); err != nil || status != 0 {
		t.Fatalf("verify failed for valid signed URL: status=%d, err=%v, url=%q", status, err, u.String())
	}

	// 2. tampered path => 401, and *only* because of the path: smap-ver and nonce
	// are seeded from the same (path-tampered) query, so r.URL.Path is the sole
	// differing input into svPayload
	uBad := *u
	uBad.Path = u.Path + "-tampered"
	if status, err := p.verify(&uBad, orig); err == nil || status != http.StatusUnauthorized {
		t.Fatalf("expected 401 for tampered path, got status=%d, err=%v", status, err)
	}
}

// S3 copy: the signed redirect binds the copy-source bucket (not the object name)
func TestSignerVerifyCopySrc(t *testing.T) {
	const (
		path    = "/s3/dst/obj"
		srcA    = "/A/own-object"
		smapVer = 999
	)
	sign := func(t *testing.T, copySrc string) (*proxy, *http.Request, *url.URL) {
		t.Helper()
		p := newTestProxy(t, true /*requestAuthConfigured*/)
		req := newReq(http.MethodPut, path, "")
		req.Header = http.Header{}
		var srcBck string
		if copySrc != "" {
			req.Header.Set(cos.S3HdrObjSrc, copySrc)
			srcBck, _, _, _ = s3.ParseCopyObjSrc(copySrc) // as in copyObjS3
		}
		out := p.redurl(req, newTestSnode(t), smapVer, cmn.NetIntraControl, "", srcBck)
		return p, req, parseRedirect(t, out)
	}
	// redirected request: same method and length, client-controlled copy source
	redirected := func(orig *http.Request, copySrc string) *http.Request {
		r := &http.Request{Method: orig.Method, ContentLength: orig.ContentLength, Header: http.Header{}}
		if copySrc != "" {
			r.Header.Set(cos.S3HdrObjSrc, copySrc)
		}
		return r
	}

	tests := []struct {
		name     string
		signed   string // copy source at the proxy
		verified string // copy source at the target
		ok       bool
	}{
		{"same copy source", srcA, srcA, true},
		{"other object, same bucket", srcA, "/A/other-object", true},
		{"escaped object, same bucket", srcA, "A/own%2Dobject", true},
		{"escaped bucket", srcA, "/%41/own-object", true},
		{"other bucket", srcA, "/SECRET/victim", false},
		{"copy source removed", srcA, "", false},
		{"plain PUT", "", "", true},
		{"copy source added", "", "/SECRET/victim", false},
		{"invalid copy source added", "", "SECRET//victim", true}, // not bound; the target rejects the copy (400)
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, orig, u := sign(t, tc.signed)
			status, err := p.verify(u, redirected(orig, tc.verified))
			switch {
			case tc.ok && (err != nil || status != 0):
				t.Fatalf("expected verified, got status=%d, err=%v", status, err)
			case !tc.ok && (err == nil || status != http.StatusUnauthorized):
				t.Fatalf("expected 401, got status=%d, err=%v", status, err)
			}
		})
	}
}

// native single-object copy: the signed redirect binds the destination (apc.QparamObjTo)
func TestSignerVerifyObjto(t *testing.T) {
	const (
		path    = "/v1/objects/src/obj"
		dstA    = "ais/@#/dst/obj"
		smapVer = 999
	)
	sign := func(t *testing.T, objto string) (*proxy, *http.Request, *url.URL) {
		t.Helper()
		p := newTestProxy(t, true /*requestAuthConfigured*/)
		var rawQuery string
		if objto != "" {
			rawQuery = url.Values{apc.QparamObjTo: []string{objto}}.Encode()
		}
		req := newReq(http.MethodPut, path, rawQuery)
		out := p.redurl(req, newTestSnode(t), smapVer, cmn.NetIntraData, "", objto)
		return p, req, parseRedirect(t, out)
	}
	// redirected request: client-controlled destination
	redirected := func(u *url.URL, objto string) *url.URL {
		v := *u
		q := v.Query()
		if objto == "" {
			q.Del(apc.QparamObjTo)
		} else {
			q.Set(apc.QparamObjTo, objto)
		}
		v.RawQuery = q.Encode()
		return &v
	}

	tests := []struct {
		name     string
		signed   string // destination at the proxy
		verified string // destination at the target
		ok       bool
	}{
		{"same destination", dstA, dstA, true},
		{"other destination", dstA, "ais/@#/SECRET/victim", false},
		{"destination removed", dstA, "", false},
		{"plain PUT", "", "", true},
		{"destination added", "", "ais/@#/SECRET/victim", false},
		{"destination appended (repeated qparam)", dstA, "+ais/@#/SECRET/victim", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, orig, u := sign(t, tc.signed)
			var v *url.URL
			if dup, ok := strings.CutPrefix(tc.verified, "+"); ok {
				// keep the signed value; the target acts on the last one (dpq)
				w := *u
				w.RawQuery += "&" + url.Values{apc.QparamObjTo: []string{dup}}.Encode()
				v = &w
			} else {
				v = redirected(u, tc.verified)
			}
			status, err := p.verify(v, orig)
			switch {
			case tc.ok && (err != nil || status != 0):
				t.Fatalf("expected verified, got status=%d, err=%v", status, err)
			case !tc.ok && (err == nil || status != http.StatusUnauthorized):
				t.Fatalf("expected 401, got status=%d, err=%v", status, err)
			}
		})
	}
}

// reconstruct the verifier from the request's own signed query params
// (pid, smap-ver, nonce, sig) -
// so the only variable across cases is the URL itself
func (p *proxy) verify(u *url.URL, orig *http.Request) (int, error) {
	r := &http.Request{
		Method:        orig.Method,
		URL:           u,
		Host:          u.Host,
		ContentLength: orig.ContentLength,
		Header:        orig.Header,
	}
	q := r.URL.Query()
	svgrp, err := svgrpFromQ(q)
	if err != nil {
		return 0, fmt.Errorf("parse sign/verify params: %w", err)
	}
	smap := p.owner.smap.get()
	pid := q.Get(apc.QparamPID)
	psi := smap.GetNode(pid)

	// copy binding: same as target._verifySigned
	dpq := dpqAlloc()
	defer dpqFree(dpq)
	if err := dpq.parse(r.URL.RawQuery); err != nil {
		return 0, err
	}
	dpq.isS3 = strings.HasPrefix(r.URL.Path, apc.URLPathS3.S+"/")
	var sv *svReq
	if xval := svBindCopy(r, dpq); xval != "" {
		sv = newVerifierExt(r, &p.htrun, svgrp, xval)
	} else {
		sv = newVerifier(r, &p.htrun, svgrp)
	}
	return sv.verify(pid, psi, smap)
}

//
// proxy authentication
//

type mockTokenParser struct {
	validateMap map[string]error
	claimsMap   map[string]*tok.AISClaims
}

var (
	validClaim   = &tok.AISClaims{RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour))}}
	expiredClaim = &tok.AISClaims{RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(-1 * time.Second))}}
)

// Validate tokens in the validateMap, return claims from claimsMap, otherwise fail
func (m *mockTokenParser) ValidateToken(_ context.Context, token string) (*tok.AISClaims, error) {
	if err, ok := m.validateMap[token]; ok {
		return nil, err
	}
	if claims, ok := m.claimsMap[token]; ok {
		return claims, nil
	}
	return nil, errors.New("invalid token")
}

func (*mockTokenParser) IsSecretCksumValid(_ string) bool {
	return true
}

func (*mockTokenParser) IsPublicKeyValid(_ string) (bool, error) {
	return true, nil
}

func newMockTokenParser() *mockTokenParser {
	return &mockTokenParser{
		validateMap: make(map[string]error),
		claimsMap:   make(map[string]*tok.AISClaims),
	}
}

func TestAuth_AccessRequiresToken(t *testing.T) {
	old := cmn.GCO.Get()
	t.Cleanup(func() {
		cmn.GCO.Put(old)
		cmn.Rom.Set(&old.ClusterConfig)
	})

	config := cmn.GCO.BeginUpdate()
	config.Auth.ClientAuthRequired = true
	cmn.GCO.CommitUpdate(config)
	cmn.Rom.Set(&config.ClusterConfig)

	p := &proxy{htrun: htrun{statsT: mock.NewStatsTracker()}}
	bck := meta.NewBck("bucket", apc.AWS, cmn.NsGlobal)
	req := &http.Request{Header: make(http.Header)}

	err := p.access(req, bck, apc.AceGET)
	tassert.Errorf(t, errors.Is(err, tok.ErrNoToken), "expected ErrNoToken, got %v", err)
	tassert.Errorf(t, aceErrToCode(err) == http.StatusUnauthorized, "expected HTTP 401, got %d", aceErrToCode(err))
}

func TestAuth_Manager_UpdateRevokedList_AddsRevokedTokens(t *testing.T) {
	mockParser := newMockTokenParser()
	// Token claims must be non-expired to be added to revoked list
	tokens := []string{"tok1", "tok2"}
	// Tell the mock parser to consider these tokens valid
	for _, token := range tokens {
		mockParser.claimsMap[token] = validClaim
	}
	am := &authManager{
		tokenMap:      newShardedTokenMap(2),
		revokedTokens: newRevokedTokensMap(),
		tokenParser:   mockParser,
	}
	revoked := &tokenList{Tokens: tokens, Version: 2}

	res := am.updateRevokedList(t.Context(), revoked)

	tassert.Fatal(t, res != nil, "Expect a non-nil result from revocation call")
	tassert.Error(t, len(res.Tokens) == 2, "Expected only 2 entries")
	tassert.Error(t, res.Version == revoked.Version, "Expected version to match")
	tassert.Error(t, slices.Contains(res.Tokens, "tok1"), "Expected to contain 'tok1'")
	tassert.Error(t, slices.Contains(res.Tokens, "tok2"), "Expected to contain 'tok2'")
}

func TestAuth_Manager_UpdateRevokedList_CleansExpiredTokens(t *testing.T) {
	am := &authManager{
		tokenMap:      newShardedTokenMap(2),
		revokedTokens: newRevokedTokensMap(),
		tokenParser: &mockTokenParser{
			validateMap: map[string]error{"expiredToken": tok.ErrTokenExpired},
		},
	}
	revoked := &tokenList{Tokens: []string{"expiredToken"}, Version: 1}
	allRevoked := am.updateRevokedList(t.Context(), revoked)
	tassert.Error(t, allRevoked == nil, "Expected allRevoked to be nil after cleanup of expired tokens")
}

// A list that is not newer leaves both the revoked list and the token cache untouched
func TestAuth_Manager_UpdateRevokedList_StaleVersion(t *testing.T) {
	mockParser := newMockTokenParser()
	mockParser.claimsMap["tok1"] = validClaim
	tkMap := newShardedTokenMap(2)
	tkMap.set("tok1", validClaim)
	am := &authManager{
		tokenMap:      tkMap,
		revokedTokens: newRevokedTokensMap(),
		tokenParser:   mockParser,
	}
	am.revokedTokens.version = 5

	res := am.updateRevokedList(t.Context(), &tokenList{Tokens: []string{"tok1"}, Version: 3})

	tassert.Error(t, res == nil, "expected a nil result for a stale list")
	tassert.Error(t, !am.revokedTokens.contains("tok1"), "expected nothing to be revoked")
	_, ok := am.tokenMap.getClaims("tok1")
	tassert.Error(t, ok, "expected the token cache to be left alone")
}

// A caller cannot choose the revoked-list version, which would let it reject later updates
func TestAuth_Manager_UpdateRevokedList_AssignsVersion(t *testing.T) {
	am := &authManager{
		tokenMap:      newShardedTokenMap(2),
		revokedTokens: newRevokedTokensMap(),
		tokenParser:   newMockTokenParser(),
	}
	version := am.revokedTokens.version

	res := am.updateRevokedList(t.Context(), &tokenList{Tokens: []string{"tok1"}})

	tassert.Fatal(t, res != nil, "Expect a non-nil result from revocation call")
	tassert.Errorf(t, res.Version == version+1, "expected version %d, got %d", version+1, res.Version)
	tassert.Error(t, am.revokedTokens.contains("tok1"), "expected 'tok1' to be revoked")
}

func TestAuth_Manager_RevokedTokenList(t *testing.T) {
	am := &authManager{
		tokenMap:      newShardedTokenMap(2),
		revokedTokens: newRevokedTokensMap(),
		tokenParser:   newMockTokenParser(),
	}
	am.revokedTokens.update(&tokenList{Tokens: []string{"tok1"}})
	list := am.revokedTokenList()
	tassert.Error(t, len(list.Tokens) == 1, "Expected only 1 entry after update")
	tassert.Error(t, list.Tokens[0] == "tok1", "Expected 'tok1' to be revoked after update")
}

func TestAuth_Manager_ValidateToken_Revoked(t *testing.T) {
	// Set up a token with valid claims but revoke it, should still fail
	token := "revoked"
	tkMap := newShardedTokenMap(2)
	tkMap.set(token, validClaim)
	am := &authManager{
		tokenMap:      tkMap,
		revokedTokens: newRevokedTokensMap(),
		tokenParser:   &mockTokenParser{},
	}
	am.updateRevokedList(t.Context(), &tokenList{Tokens: []string{token}})
	_, err := am.validateToken(t.Context(), token)
	tassert.Errorf(t, errors.Is(err, tok.ErrTokenRevoked), "expected ErrTokenRevoked, got %v", err)
}

func TestAuth_Manager_ValidateToken_CachedValid(t *testing.T) {
	token := "goodtoken"
	tkMap := newShardedTokenMap(2)
	tkMap.set(token, validClaim)
	am := &authManager{
		tokenMap:      tkMap,
		revokedTokens: newRevokedTokensMap(),
		tokenParser:   &mockTokenParser{},
	}
	_, err := am.validateToken(t.Context(), token)
	tassert.Errorf(t, err == nil, "Expected token to be valid, but got: %v", err)
}

func TestAuth_Manager_ValidateToken_CachedExpired(t *testing.T) {
	token := "expiredtoken"
	tkMap := newShardedTokenMap(2)
	tkMap.set(token, expiredClaim)
	am := &authManager{
		tokenMap:      tkMap,
		revokedTokens: newRevokedTokensMap(),
		tokenParser:   &mockTokenParser{},
	}
	_, err := am.validateToken(t.Context(), token)
	tassert.Error(t, err != nil, "Expected error when token is expired")
}

func TestAuth_RevokedTokensMap_CheckVersion(t *testing.T) {
	r := newRevokedTokensMap()
	r.version = 5
	r.update(&tokenList{Version: 6})
	tassert.Errorf(t, r.version == 6, "expected version updated to 6, got %d", r.version)

	r.version = 10
	r.update(&tokenList{Version: 8})
	tassert.Errorf(t, r.version == 10, "version should remain 10, received %d", r.version)
}

func TestAuth_RevokedTokensMap_Cleanup(t *testing.T) {
	// Currently only removing revoked tokens that have expired
	r := newRevokedTokensMap()
	r.update(&tokenList{Tokens: []string{"good", "expired", "bad"}})
	tassert.Fatalf(t, len(r.revokedTokens) == 3, "expected only the 3 new entries in the revoked list")
	parser := &mockTokenParser{
		validateMap: map[string]error{
			"expired": tok.ErrTokenExpired,
			"bad":     errors.New("parse error"),
			"good":    nil,
		},
	}
	result := r.cleanup(t.Context(), parser)
	tassert.Error(t, len(result.Tokens) == 2, "expected both valid and invalid tokens after cleanup")
	tassert.Error(t, !r.contains("expired"), "expired token should have been removed")
}

//
// node join secrets
//

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

//
// target access
//

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

//
// intra-cluster sender headers
//

// newHTRunWithSmap creates a minimal htrun with a valid smap containing one proxy node.
func newHTRunWithSmap() *htrun {
	h := &htrun{}
	primaryID := "p-primary"
	h.si = newSnode(primaryID, apc.Proxy)

	config := cmn.GCO.BeginUpdate()
	cmn.GCO.CommitUpdate(config)

	h.owner.smap = newSmapOwner(config, false /*isTarget*/)
	smap := newSmap()
	smap.addProxy(h.si)
	smap.Primary = h.si
	h.owner.smap.put(smap)
	h.svs.init()
	return h
}

func TestSetIntraHdrs_ClearsRelayedSenderHdrs(t *testing.T) {
	spoofed := func() http.Header {
		hdr := http.Header{}
		hdr.Set(apc.HdrSenderID, "p-spoofed")
		hdr.Set(apc.HdrSenderName, "p[spoofed]")
		hdr.Set(apc.HdrSenderIsPrimary, "true")
		hdr.Set(apc.HdrSenderSmapVer, "99999")
		hdr.Set(apc.HdrSenderNonce, "42")
		hdr.Set(apc.HdrSenderSig, "not-a-signature")
		return hdr
	}

	// self is NOT primary; signing disabled and grace expired (svState.init),
	// so signIntra is skipped - exactly the window where a relayed signature would survive
	h := newHTRunWithSmap()
	tassert.Fatalf(t, !h.svs.sign(), "expecting sign() == false")

	smap := h.owner.smap.get().clone()
	other := newSnode("p-other", apc.Proxy)
	smap.addProxy(other)
	smap.Primary = other
	smap.Version++
	h.owner.smap.put(smap)
	smap = h.owner.smap.get()

	req := &http.Request{Header: spoofed()}
	h.setIntraHdrs(req, smap, true /*peer present*/)

	for _, tc := range []struct{ hdr, expected string }{
		{apc.HdrSenderID, h.SID()},
		{apc.HdrSenderName, h.si.Name()},
		{apc.HdrSenderIsPrimary, ""},
		{apc.HdrSenderSmapVer, smap.vstr},
		{apc.HdrSenderNonce, ""},
		{apc.HdrSenderSig, ""},
	} {
		got := req.Header.Get(tc.hdr)
		tassert.Errorf(t, got == tc.expected, "%s: got %q, expected %q", tc.hdr, got, tc.expected)
	}

	// and the legitimate primary case is not broken by the above
	hp := newHTRunWithSmap()
	reqp := &http.Request{Header: spoofed()}
	hp.setIntraHdrs(reqp, hp.owner.smap.get(), true /*peer present*/)
	got := reqp.Header.Get(apc.HdrSenderIsPrimary)
	tassert.Errorf(t, got == "true", "%s: got %q, expected %q", apc.HdrSenderIsPrimary, got, "true")
}
