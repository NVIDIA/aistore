// Package main contains the independent authentication server for AIStore.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"path/filepath"
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
