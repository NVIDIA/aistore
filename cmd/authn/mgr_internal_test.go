// Package main contains the independent authentication server for AIStore.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmd/authn/config"
	"github.com/NVIDIA/aistore/cmd/authn/signing"
	"github.com/NVIDIA/aistore/cmd/authn/tok"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func newMgrWithConf(t *testing.T, conf *authn.Config) *mgr {
	conf.Init()
	confPath := filepath.Join(t.TempDir(), "authn.json")
	err := jsp.SaveMeta(confPath, conf, nil)
	tassert.CheckFatal(t, err)

	cm := config.NewConfManager()
	cm.Init(confPath)

	signer := signing.NewHMACSigner(cm.GetSecret())
	driver := mock.NewDBDriver()

	testMgr, _, err := newMgr(cm, signer, driver)
	tassert.CheckFatal(t, err)
	return testMgr
}

func TestManagerTLSVerification(t *testing.T) {
	tests := []struct {
		name, authn, generic string
		expected             bool
	}{
		{name: "default"},
		{name: "authn", authn: "true", expected: true},
		{name: "generic-ignored", generic: "true"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(env.AisAuthAdminPassword, "adminpass")
			t.Setenv(env.AisAuthSkipVerifyCrt, tc.authn)
			t.Setenv(env.AisSkipVerifyCrt, tc.generic)
			m := newMgrWithConf(t, &authn.Config{})
			transport := m.clientTLS.Transport.(*http.Transport)
			tassert.Errorf(t, transport.TLSClientConfig.InsecureSkipVerify == tc.expected,
				"expected skip-verify %t", tc.expected)
		})
	}
}

func validateCommonClaims(t *testing.T, claims *tok.AISClaims, sub, iss string, start time.Time) {
	actualSub, err := claims.GetSubject()
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, sub == actualSub, "Expected subject %q, got %q", sub, actualSub)
	actualIss, err := claims.GetIssuer()
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, iss == actualIss, "Expected issuer %q, got %q", iss, actualIss)
	actualIAT, err := claims.GetIssuedAt()
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, actualIAT != nil && !actualIAT.Before(start), "IssuedAt should be set and >= test start")
	exp, err := claims.GetExpirationTime()
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, exp != nil, "Expected ExpiresAt to be set")
	tassert.Errorf(t, exp.After(actualIAT.Time), "ExpiresAt %v should be after IssuedAt %v", exp.UTC(), actualIAT.UTC())
}

// When HMAC secret is provided through config, updating secret should take effect for signing and validation
func TestHMACSecretUpdate(t *testing.T) {
	const (
		initialSecret = "initial-test-secret"
		updatedSecret = "updated-test-secret"
		adminPass     = "test-pass"
	)

	t.Setenv(env.AisAuthAdminPassword, adminPass)
	t.Setenv(env.AisAuthSecretKey, "")
	conf := &authn.Config{
		Server: authn.ServerConf{
			Secret: initialSecret,
			Expire: cos.Duration(time.Hour),
		},
	}

	testMgr := newMgrWithConf(t, conf)

	// Issue a token with the initial secret and validate it
	loginMsg := &authn.LoginMsg{}
	token1, _, err := testMgr.issueToken(adminUserID, adminPass, loginMsg)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, token1 != "", "expected non-empty token")
	_, err = testMgr.validateToken(t.Context(), token1)
	tassert.CheckFatal(t, err)

	// Update the HMAC secret via updateConf
	newSecret := updatedSecret
	err = testMgr.updateConf(&authn.ConfigToUpdate{
		Server: &authn.ServerConfToSet{Secret: &newSecret},
	})
	tassert.CheckFatal(t, err)

	// Old token signed with the initial secret must fail validation
	_, err = testMgr.validateToken(t.Context(), token1)
	tassert.Errorf(t, err != nil, "old token should fail validation after secret update")

	// New token signed with the updated secret must validate
	token2, _, err := testMgr.issueToken(adminUserID, adminPass, loginMsg)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, token2 != "", "expected non-empty token after secret update")
	_, err = testMgr.validateToken(t.Context(), token2)
	tassert.CheckFatal(t, err)
}

// A revoked token must fail request authorization while remaining
// signature-valid for the revoked-list bookkeeping
func TestTokenRevocation(t *testing.T) {
	const adminPass = "test-pass"

	t.Setenv(env.AisAuthAdminPassword, adminPass)
	conf := &authn.Config{
		Server: authn.ServerConf{
			Secret: "test-secret",
			Expire: cos.Duration(time.Hour),
		},
	}
	testMgr := newMgrWithConf(t, conf)

	token, _, err := testMgr.issueToken(adminUserID, adminPass, &authn.LoginMsg{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, token != "", "expected non-empty token")

	// Token must validate before revocation
	_, err = testMgr.validateToken(t.Context(), token)
	tassert.CheckFatal(t, err)

	_, err = testMgr.revokeToken(token)
	tassert.CheckFatal(t, err)

	// Revoked token must no longer authorize requests
	_, err = testMgr.validateToken(t.Context(), token)
	tassert.Fatalf(t, errors.Is(err, tok.ErrTokenRevoked), "expected revoked-token error, got %v", err)

	// Its signature must remain valid, so the revoked-list cleanup keeps it
	_, err = testMgr.validateTokenSignature(t.Context(), token)
	tassert.CheckFatal(t, err)
	revoked, _, err := testMgr.generateRevokedTokenList(t.Context())
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(revoked) == 1 && revoked[0] == token,
		"expected the revoked token to stay on the revoked list, got %v", revoked)
}

// An altered spelling of a revoked token must stay rejected. Setting the
// signature's unused padding bits keeps the signature bytes (so the token still
// authenticates) but changes the token string, which evades the exact-match
// revoked list. Strict Base64 decoding is what rejects the altered token.
func TestTokenRevocationPadBits(t *testing.T) {
	const adminPass = "test-pass"

	t.Setenv(env.AisAuthAdminPassword, adminPass)
	conf := &authn.Config{
		Server: authn.ServerConf{
			Secret: "test-secret",
			Expire: cos.Duration(time.Hour),
		},
	}
	testMgr := newMgrWithConf(t, conf)

	token, _, err := testMgr.issueToken(adminUserID, adminPass, &authn.LoginMsg{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, token != "", "expected non-empty token")

	_, err = testMgr.revokeToken(token)
	tassert.CheckFatal(t, err)

	altered := alterSignaturePadBits(t, token)
	tassert.Fatalf(t, altered != token, "expected an altered token string")

	// The exact-match revoked list does not catch the altered spelling.
	revoked, err := testMgr.isTokenRevoked(altered)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, !revoked, "altered token unexpectedly matched the revoked list")

	// Strict Base64 decoding rejects it.
	_, err = testMgr.validateToken(t.Context(), altered)
	tassert.Fatalf(t, errors.Is(err, tok.ErrInvalidToken), "expected altered token to be rejected, got %v", err)
}

// Set an unused Base64URL padding bit in the signature's final character to get a
// different token string that decodes to identical signature bytes. HS256's
// 32-byte signature encodes to a final character with 2 unused (zero) padding
// bits, so incrementing that character sets a padding bit without altering the
// signature.
func alterSignaturePadBits(t *testing.T, token string) string {
	t.Helper()
	parts := strings.Split(token, ".")
	tassert.Fatalf(t, len(parts) == 3 && parts[2] != "", "expected a JWT with a signature")
	sig := parts[2]

	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	idx := strings.IndexByte(alphabet, sig[len(sig)-1])
	tassert.Fatalf(t, idx >= 0 && idx&0b11 == 0, "expected zeroed Base64URL signature padding bits")

	parts[2] = sig[:len(sig)-1] + string(alphabet[idx+1])
	return strings.Join(parts, ".")
}

func TestBuildClaims(t *testing.T) {
	const (
		adminPass   = "admin-pass"
		externalURL = "https://auth.example.com"
		testUser    = "test-user"
	)
	t.Setenv(env.AisAuthAdminPassword, adminPass)
	conf := &authn.Config{
		Server: authn.ServerConf{
			Secret: "test-secret",
			Expire: cos.Duration(time.Hour),
		},
		Net: authn.NetConf{
			ExternalURL: externalURL,
		},
	}
	testMgr := newMgrWithConf(t, conf)
	cluster := "clu1"
	cluACLs := []*authn.CluACL{{ID: cluster, Access: apc.ClusterAccessRO}}
	bckACLs := []*authn.BckACL{{
		Bck:    cmn.Bck{Name: "bkt1", Provider: apc.AIS, Ns: cmn.Ns{UUID: cluster}},
		Access: apc.AccessRO,
	}}
	before := time.Now().UTC().Truncate(time.Second)
	t.Run("Admin", func(t *testing.T) {
		adminUser := &authn.User{ID: adminUserID, Roles: []*authn.Role{{Name: authn.AdminRole, IsAdmin: true}}}
		claims, err := testMgr.buildClaims(&authn.LoginMsg{}, adminUser, nil, nil)
		tassert.CheckFatal(t, err)
		tassert.Error(t, claims.IsAdmin, "Expected admin claims")
		validateCommonClaims(t, claims, adminUserID, externalURL, before)
		tassert.Errorf(t, len(claims.ClusterACLs) == 0, "Admin claims should have no cluster ACLs")
		tassert.Errorf(t, len(claims.BucketACLs) == 0, "Admin claims should have no bucket ACLs")
		tassert.Error(t, len(claims.Audience) == 0, "Expected audience to be empty when no clusters registered in DB")
	})
	t.Run("Standard", func(t *testing.T) {
		user := &authn.User{ID: testUser}
		claims, err := testMgr.buildClaims(&authn.LoginMsg{}, user, cluACLs, bckACLs)
		tassert.CheckFatal(t, err)
		tassert.Error(t, !claims.IsAdmin, "Expected non-admin claims")
		validateCommonClaims(t, claims, testUser, externalURL, before)
		tassert.Errorf(t, len(claims.ClusterACLs) == 1, "Expected 1 cluster ACL, got %d", len(claims.ClusterACLs))
		tassert.Errorf(t, len(claims.BucketACLs) == 1, "Expected 1 bucket ACL, got %d", len(claims.BucketACLs))
		tassert.Errorf(t, len(claims.Audience) == 1, "Expected a single audience to be set, got %d", len(claims.Audience))
		tassert.Errorf(t, claims.Audience[0] == cluster, "Expected audience to match cluster ACL, got %s", claims.Audience[0])
	})
}

func TestGetExp(t *testing.T) {
	const (
		adminPass  = "admin-pass"
		defaultExp = time.Hour
		maxAge     = 24 * time.Hour
	)
	t.Setenv(env.AisAuthAdminPassword, adminPass)
	conf := &authn.Config{
		Server: authn.ServerConf{
			Secret:      "test-secret",
			Expire:      cos.Duration(defaultExp),
			MaxTokenAge: cos.Duration(maxAge),
		},
	}
	testMgr := newMgrWithConf(t, conf)
	now := time.Unix(1710000000, 0).UTC()

	tests := []struct {
		name      string
		expiresIn *time.Duration
		expected  time.Time
		wantErr   bool
	}{
		{name: "Default", expected: now.Add(defaultExp)},
		{name: "MaxAge", expiresIn: apc.Ptr(0 * time.Second), expected: now.Add(maxAge)},
		{name: "Requested", expiresIn: apc.Ptr(2 * time.Hour), expected: now.Add(2 * time.Hour)},
		{name: "TooShort", expiresIn: apc.Ptr(time.Second), wantErr: true},
		{name: "TooLong", expiresIn: apc.Ptr(48 * time.Hour), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exp, err := testMgr.getExp(now, &authn.LoginMsg{ExpiresIn: tt.expiresIn})
			if tt.wantErr {
				tassert.Errorf(t, errors.Is(err, errInvalidRequestedExp), "expected invalid expiration error, got %v", err)
				tassert.Errorf(t, exp == nil, "expected nil expiration on error, got %v", exp)
				return
			}
			tassert.CheckFatal(t, err)
			tassert.Errorf(t, exp != nil, "expected expiration to be set")
			tassert.Errorf(t, exp.Time.Equal(tt.expected), "expected expiration %v, got %v", tt.expected, exp.Time)
		})
	}
}

func TestSelfToken(t *testing.T) {
	const (
		adminPass   = "admin-pass"
		externalURL = "https://auth.example.com"
		target      = "clu-target"
		registered  = "clu-registered"
	)
	t.Setenv(env.AisAuthAdminPassword, adminPass)
	conf := &authn.Config{
		Server: authn.ServerConf{Secret: "test-secret", Expire: cos.Duration(time.Hour)},
		Net:    authn.NetConf{ExternalURL: externalURL},
	}
	testMgr := newMgrWithConf(t, conf)

	// a cluster already in the DB must not widen the audience
	_, err := testMgr.db.Set(clustersCollection, registered, &authn.CluACL{ID: registered})
	tassert.CheckFatal(t, err)

	before := time.Now().UTC().Truncate(time.Second)
	token, err := testMgr.selfToken(target)
	tassert.CheckFatal(t, err)

	claims, err := testMgr.validateTokenSignature(t.Context(), token)
	tassert.CheckFatal(t, err)
	tassert.Error(t, claims.IsAdmin, "Expected admin claims")
	validateCommonClaims(t, claims, config.ServiceName, externalURL, before)
	tassert.Errorf(t, len(claims.Audience) == 1 && claims.Audience[0] == target,
		"Expected audience [%s], got %v", target, claims.Audience)
}

// Stands in for a cluster's /v1/tokens endpoint, recording the tokens AuthN presents
type mockCluster struct {
	*httptest.Server
	tokens []string
	status int
	mu     sync.Mutex
}

func newMockCluster(t *testing.T, status int) *mockCluster {
	mc := &mockCluster{status: status}
	mc.Server = httptest.NewServer(http.HandlerFunc(mc.handle))
	t.Cleanup(mc.Close)
	return mc
}

func (mc *mockCluster) handle(w http.ResponseWriter, r *http.Request) {
	token, ok := strings.CutPrefix(r.Header.Get(apc.HdrAuthorization), apc.AuthenticationTypeBearer+" ")
	if !ok || token == "" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	mc.mu.Lock()
	mc.tokens = append(mc.tokens, token)
	mc.mu.Unlock()
	w.WriteHeader(mc.status)
}

// Tokens presented so far, in request order
func (mc *mockCluster) presented() []string {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return slices.Clone(mc.tokens)
}

func validateHandshakeClaims(t *testing.T, m *mgr, token, cluID string) {
	claims, err := m.validateTokenSignature(t.Context(), token)
	tassert.CheckFatal(t, err)
	tassert.Error(t, claims.IsAdmin, "Expected admin claims")
	tassert.Errorf(t, len(claims.Audience) == 1 && claims.Audience[0] == cluID,
		"Expected audience [%s], got %v", cluID, claims.Audience)
}

func newHandshakeMgr(t *testing.T) *mgr {
	t.Setenv(env.AisAuthAdminPassword, "admin-pass")
	return newMgrWithConf(t, &authn.Config{
		Server: authn.ServerConf{Secret: "test-secret", Expire: cos.Duration(time.Hour)},
		Net:    authn.NetConf{ExternalURL: "https://auth.example.com"},
	})
}

// Registering a cluster must present an admin token that cluster can verify
func TestRegisterClusterHandshake(t *testing.T) {
	const (
		newID   = "clu-new"
		otherID = "clu-other"
	)
	newACL := func(mc *mockCluster) *authn.CluACL {
		return &authn.CluACL{ID: newID, Alias: "newclu", URLs: []string{mc.URL}}
	}

	// Audience is the cluster being registered, which is not yet in the DB
	t.Run("FirstCluster", func(t *testing.T) {
		testMgr, mc := newHandshakeMgr(t), newMockCluster(t, http.StatusOK)

		_, err := testMgr.registerCluster(t.Context(), newACL(mc))
		tassert.CheckFatal(t, err)

		presented := mc.presented()
		tassert.Fatalf(t, len(presented) == 1, "Expected a single request, got %d", len(presented))
		validateHandshakeClaims(t, testMgr, presented[0], newID)
		_, _, err = testMgr.getCluster(newID)
		tassert.CheckFatal(t, err)
	})

	// An already-registered cluster must not widen the audience
	t.Run("ExistingClusters", func(t *testing.T) {
		testMgr, mc := newHandshakeMgr(t), newMockCluster(t, http.StatusOK)
		_, err := testMgr.db.Set(clustersCollection, otherID, &authn.CluACL{ID: otherID})
		tassert.CheckFatal(t, err)

		_, err = testMgr.registerCluster(t.Context(), newACL(mc))
		tassert.CheckFatal(t, err)

		presented := mc.presented()
		tassert.Fatalf(t, len(presented) == 1, "Expected a single request, got %d", len(presented))
		validateHandshakeClaims(t, testMgr, presented[0], newID)
	})

	// A rejected handshake must leave no half-registered cluster behind
	t.Run("Rejected", func(t *testing.T) {
		testMgr, mc := newHandshakeMgr(t), newMockCluster(t, http.StatusUnauthorized)

		_, err := testMgr.registerCluster(t.Context(), newACL(mc))
		tassert.Fatal(t, err != nil, "Expected registration to fail")

		_, _, err = testMgr.getCluster(newID)
		tassert.Error(t, err != nil, "Expected no cluster to be stored")
	})

	// Updating a cluster re-runs the same handshake
	t.Run("Update", func(t *testing.T) {
		testMgr, mc := newHandshakeMgr(t), newMockCluster(t, http.StatusOK)
		_, err := testMgr.registerCluster(t.Context(), newACL(mc))
		tassert.CheckFatal(t, err)

		_, err = testMgr.updateCluster(t.Context(), newID, &authn.CluACL{ID: newID, Alias: "renamed"})
		tassert.CheckFatal(t, err)

		presented := mc.presented()
		tassert.Fatalf(t, len(presented) == 2, "Expected register and update requests, got %d", len(presented))
		validateHandshakeClaims(t, testMgr, presented[1], newID)
	})
}

func TestGetAud(t *testing.T) {
	clu := func(id string) *authn.CluACL {
		return &authn.CluACL{ID: id, Access: apc.ClusterAccessRO}
	}
	bck := func(uuid string) *authn.BckACL {
		return &authn.BckACL{Bck: cmn.Bck{Name: "b", Provider: apc.AIS, Ns: cmn.Ns{UUID: uuid}}, Access: apc.AccessRO}
	}

	tests := []struct {
		name    string
		bckACLs []*authn.BckACL
		cluACLs []*authn.CluACL
		expect  map[string]struct{}
	}{
		// No ACLs, so no Aud
		{name: "Empty", expect: map[string]struct{}{}},
		// Aud produced from cluster ACLs
		{name: "CluOnly", cluACLs: []*authn.CluACL{clu("c1"), clu("c2")},
			expect: map[string]struct{}{"c1": {}, "c2": {}}},
		// Aud produced from bucket ACLs
		{name: "BckOnly", bckACLs: []*authn.BckACL{bck("c3"), bck("c4")},
			expect: map[string]struct{}{"c3": {}, "c4": {}}},
		// Combined ACLs across both cluster and bucket should not have duplicates
		{name: "Dedup", cluACLs: []*authn.CluACL{clu("c1")}, bckACLs: []*authn.BckACL{bck("c1")},
			expect: map[string]struct{}{"c1": {}}},
		// ACLs with missing IDs don't get Aud entries
		{name: "SkipEmpty", cluACLs: []*authn.CluACL{clu(""), clu("c1")}, bckACLs: []*authn.BckACL{bck("")},
			expect: map[string]struct{}{"c1": {}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aud := getAud(tt.bckACLs, tt.cluACLs)
			tassert.Errorf(t, len(aud) == len(tt.expect), "Expected %d aud entries, got %d", len(tt.expect), len(aud))
			for _, id := range aud {
				_, ok := tt.expect[id]
				tassert.Errorf(t, ok, "Unexpected aud entry %q", id)
			}
		})
	}
}
