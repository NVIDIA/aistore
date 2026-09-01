// Package integration_test.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"errors"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
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

// promoteTestDir returns a writable directory under the hard-coded promote root.
func promoteTestDir(t *testing.T) string {
	t.Helper()
	if err := os.MkdirAll(apc.PromoteRoot, 0o755); err != nil {
		t.Skipf("cannot create %s: %v", apc.PromoteRoot, err)
	}
	dir, err := os.MkdirTemp(apc.PromoteRoot, "prm-") //nolint:usetesting // t.TempDir is rooted at $TMPDIR; promote requires apc.PromoteRoot
	if err != nil {
		t.Skipf("cannot create under %s: %v", apc.PromoteRoot, err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func expectETLAccessDenied(t *testing.T, bp api.BaseParams, status int) {
	t.Helper()
	tests := []struct {
		name string
		run  func() error
	}{
		{"delete", func() error { return api.ETLDelete(bp, "missing-etl") }},
		{"get", func() error { _, err := api.ETLList(bp); return err }},
		{"post-start", func() error { return api.ETLStart(bp, "missing-etl") }},
		{"post-stop", func() error { return api.ETLStop(bp, "missing-etl") }},
		{"put", func() error { _, err := api.ETLInit(bp, nil); return err }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) { expectStatus(t, test.run(), status) })
	}
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
		t.Run("etl-admin", func(t *testing.T) {
			tools.CheckSkip(t, &tools.SkipTestArgs{RequiresETL: true})
			err := api.ETLStart(aisBP, "missing-etl")
			expectStatus(t, err, http.StatusNotFound)
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
		t.Run("etl", func(t *testing.T) {
			tools.CheckSkip(t, &tools.SkipTestArgs{RequiresETL: true})
			expectETLAccessDenied(t, userBP, http.StatusForbidden)
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
		t.Run("etl", func(t *testing.T) {
			tools.CheckSkip(t, &tools.SkipTestArgs{RequiresETL: true})
			bp := aisBP
			bp.Token = ""
			expectETLAccessDenied(t, bp, http.StatusUnauthorized)
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

			revokedBP := authBP
			revokedBP.Token = tok.Token
			_, err = authn.GetUser(revokedBP, uid)
			expectStatus(t, err, http.StatusUnauthorized)
		})
	})
}

// loginWithACLs creates a role/user with the given ACLs and returns AIS BaseParams for that user.
func loginWithACLs(t *testing.T, authBP, aisBP api.BaseParams, cluID string, bckACLs []*authn.BckACL, cluAccess apc.AccessAttrs) api.BaseParams {
	t.Helper()
	var (
		uid  = "user-" + trand.String(6)
		pass = trand.String(12)
		role = "role-" + trand.String(6)
	)
	tassert.CheckFatal(t, authn.AddRole(authBP, &authn.Role{
		Name:        role,
		BucketACLs:  bckACLs,
		ClusterACLs: []*authn.CluACL{{ID: cluID, Access: cluAccess}},
	}))
	t.Cleanup(func() { authn.DeleteRole(authBP, role) })

	r, err := authn.GetRole(authBP, role)
	tassert.CheckFatal(t, err)
	tassert.CheckFatal(t, authn.AddUser(authBP, &authn.User{ID: uid, Password: pass, Roles: []*authn.Role{r}}))
	t.Cleanup(func() { authn.DeleteUser(authBP, uid) })

	tok, err := authn.LoginUser(authBP, uid, pass, nil)
	tassert.CheckFatal(t, err)
	bp := aisBP
	bp.Token = tok.Token
	return bp
}

// promote is gated by AcePromote alone; source must be under the hard-coded promote root
func TestAuthPromoteRequiresPromotePerm(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiresAuth: true})

	var (
		aisBP  = tools.BaseAPIParams()
		authBP = authNBP()
		bck    = cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
		srcDir = promoteTestDir(t)
		srcFQN = filepath.Join(srcDir, trand.String(12))
	)
	tassert.CheckFatal(t, os.WriteFile(srcFQN, []byte("promote"), 0o600))

	smap, err := api.GetClusterMap(aisBP)
	tassert.CheckFatal(t, err)
	registerCluster(t, authBP, authn.CluACL{ID: smap.UUID, URLs: []string{aisBP.URL}})

	tassert.CheckFatal(t, api.CreateBucket(aisBP, bck, nil))
	t.Cleanup(func() { api.DestroyBucket(aisBP, bck) })

	bckACL := cmn.Bck{Name: bck.Name, Provider: bck.Provider, Ns: cmn.Ns{UUID: smap.UUID}}

	// AcePromote on the bucket grants promote
	t.Run("promote-perm", func(t *testing.T) {
		bp := loginWithACLs(t, authBP, aisBP, smap.UUID,
			[]*authn.BckACL{{Bck: bckACL, Access: apc.AccessRW | apc.AcePromote}},
			apc.ClusterAccessRW)
		_, err := api.Promote(bp, bck, &apc.PromoteArgs{SrcFQN: srcFQN, ObjName: "promoted-with-perm"})
		tassert.CheckFatal(t, err)
	})

	// no bucket ACL: AcePromote falls through to cluster AccessAll
	t.Run("fall-through", func(t *testing.T) {
		bp := loginWithACLs(t, authBP, aisBP, smap.UUID, nil, apc.AccessAll)
		_, err := api.Promote(bp, bck, &apc.PromoteArgs{SrcFQN: srcFQN, ObjName: "promoted-fall-through"})
		tassert.CheckFatal(t, err)
	})

	// cluster AcePromote, but a bucket ACL without AcePromote blocks fall-through
	t.Run("restricted-bucket", func(t *testing.T) {
		bp := loginWithACLs(t, authBP, aisBP, smap.UUID,
			[]*authn.BckACL{{Bck: bckACL, Access: apc.AccessRW}},
			apc.AcePromote)
		_, err := api.Promote(bp, bck, &apc.PromoteArgs{SrcFQN: srcFQN, ObjName: "promoted-restricted"})
		expectStatus(t, err, http.StatusForbidden)
	})

	t.Run("outside-root", func(t *testing.T) {
		outside := filepath.Join(t.TempDir(), "outside")
		tassert.CheckFatal(t, os.WriteFile(outside, []byte("x"), 0o600))
		_, err := api.Promote(aisBP, bck, &apc.PromoteArgs{SrcFQN: outside, ObjName: "promoted-outside"})
		expectStatus(t, err, http.StatusBadRequest)
	})

	t.Run("superuser", func(t *testing.T) {
		_, err := api.Promote(aisBP, bck, &apc.PromoteArgs{SrcFQN: srcFQN, ObjName: "promoted-superuser"})
		tassert.CheckFatal(t, err)
	})
}

// targets don't validate client tokens: when AuthN is on, the node-control API
// must not be readable on a target's public listener
func TestAuthDirectTargetDaemon(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiresAuth: true})

	smap, err := api.GetClusterMap(tools.BaseAPIParams())
	tassert.CheckFatal(t, err)
	tsi, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)

	bp := tools.BaseAPIParams(tsi.URL(cmn.NetPublic))
	bp.Token = ""

	for _, what := range []string{apc.WhatSmap, apc.WhatBMD, apc.WhatNodeConfig, apc.WhatSnode, apc.WhatLog} {
		t.Run(what, func(t *testing.T) {
			expectStatus(t, daeGetWhat(bp, what), http.StatusForbidden)
		})
	}
}

func daeGetWhat(bp api.BaseParams, what string) error {
	bp.Method = http.MethodGet
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathDae.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{what}}
	}
	return reqParams.DoRequest()
}
