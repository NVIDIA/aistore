// Package integration_test.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/trand"
)

func authNBP() api.BaseParams {
	bp := tools.BaseAPIParams()
	bp.URL = os.Getenv(env.AisAuthURL)
	return bp
}

func registerCluster(t *testing.T, bp api.BaseParams, clu authn.CluACL) {
	err := authn.RegisterCluster(bp, clu)
	if err == nil {
		t.Cleanup(func() { authn.UnregisterCluster(bp, clu) })
		return
	}
	var herr *cmn.ErrHTTP
	if errors.As(err, &herr) && herr.Status == http.StatusConflict {
		return
	}
	tassert.CheckFatal(t, err)
}

func expectStatus(t *testing.T, err error, status int) {
	t.Helper()
	tassert.Fatalf(t, err != nil, "expected status %d", status)
	var herr *cmn.ErrHTTP
	tassert.Fatalf(t, errors.As(err, &herr), "expected ErrHTTP, got %v", err)
	tassert.Fatalf(t, herr.Status == status, "expected %d, got %d", status, herr.Status)
}

func TestAuth(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiresAuth: true})

	var (
		aisBP  = tools.BaseAPIParams()
		authBP = authNBP()
		bck    = cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
		uid    = "user-" + trand.String(6)
		pass   = trand.String(12)
		role   = "role-" + trand.String(6)
	)

	smap, err := api.GetClusterMap(aisBP)
	tassert.CheckFatal(t, err)
	registerCluster(t, authBP, authn.CluACL{ID: smap.UUID, URLs: []string{aisBP.URL}})

	tassert.CheckFatal(t, api.CreateBucket(aisBP, bck, nil))
	t.Cleanup(func() { api.DestroyBucket(aisBP, bck) })

	tassert.CheckFatal(t, authn.AddRole(authBP, &authn.Role{
		Name:        role,
		BucketACLs:  []*authn.BckACL{{Bck: cmn.Bck{Name: bck.Name, Provider: bck.Provider, Ns: cmn.Ns{UUID: smap.UUID}}, Access: apc.AceBckHEAD}},
		ClusterACLs: []*authn.CluACL{{ID: smap.UUID, Access: apc.AceShowCluster}},
	}))
	t.Cleanup(func() { authn.DeleteRole(authBP, role) })

	r, err := authn.GetRole(authBP, role)
	tassert.CheckFatal(t, err)
	tassert.CheckFatal(t, authn.AddUser(authBP, &authn.User{ID: uid, Password: pass, Roles: []*authn.Role{r}}))
	t.Cleanup(func() { authn.DeleteUser(authBP, uid) })

	tok, err := authn.LoginUser(authBP, uid, pass, nil)
	tassert.CheckFatal(t, err)
	userBP := aisBP
	userBP.Token = tok.Token

	t.Run("success", func(t *testing.T) {
		t.Run("bucket", func(t *testing.T) {
			_, err := api.HeadBucket(userBP, bck, true)
			tassert.CheckFatal(t, err)
		})
		t.Run("cluster", func(t *testing.T) {
			_, err := api.GetClusterMap(userBP)
			tassert.CheckFatal(t, err)
		})
	})

	t.Run("forbidden", func(t *testing.T) {
		t.Run("bucket", func(t *testing.T) {
			_, err := api.ListObjects(userBP, bck, nil, api.ListArgs{})
			expectStatus(t, err, http.StatusForbidden)
		})
		t.Run("cluster", func(t *testing.T) {
			err := api.DestroyBucket(userBP, bck)
			expectStatus(t, err, http.StatusForbidden)
		})
	})

	t.Run("unauthorized", func(t *testing.T) {
		t.Run("empty", func(t *testing.T) {
			bp := aisBP
			bp.Token = ""
			_, err := api.HeadBucket(bp, bck, true)
			expectStatus(t, err, http.StatusUnauthorized)
		})
		t.Run("invalid", func(t *testing.T) {
			bp := aisBP
			bp.Token = "invalid"
			_, err := api.HeadBucket(bp, bck, true)
			expectStatus(t, err, http.StatusUnauthorized)
		})
		t.Run("expired", func(t *testing.T) {
			exp := time.Nanosecond
			tok, err := authn.LoginUser(authBP, uid, pass, &exp)
			tassert.CheckFatal(t, err)
			time.Sleep(time.Millisecond)
			bp := aisBP
			bp.Token = tok.Token
			_, err = api.HeadBucket(bp, bck, true)
			expectStatus(t, err, http.StatusUnauthorized)
		})
		t.Run("revoked", func(t *testing.T) {
			tok, err := authn.LoginUser(authBP, uid, pass, nil)
			tassert.CheckFatal(t, err)
			tassert.CheckFatal(t, authn.RevokeToken(authBP, tok.Token))
			time.Sleep(time.Second)
			bp := aisBP
			bp.Token = tok.Token
			_, err = api.HeadBucket(bp, bck, true)
			expectStatus(t, err, http.StatusUnauthorized)
		})
	})
}
