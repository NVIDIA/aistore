// Package main contains the independent authentication server for AIStore.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmd/authn/config"
	"github.com/NVIDIA/aistore/cmd/authn/signing"
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
	_, err = testMgr.getParser().ValidateToken(t.Context(), token1)
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
