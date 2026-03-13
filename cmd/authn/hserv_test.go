// Package main contains the independent authentication server for AIStore.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func newTestHserv(t *testing.T) (*hserv, string) {
	t.Helper()
	const adminPass = "admin-pass-for-test"
	t.Setenv(env.AisAuthAdminPassword, adminPass)

	conf := &authn.Config{
		Server: authn.ServerConf{
			Secret: "test-secret-key",
			Expire: cos.Duration(time.Hour),
		},
	}
	mgr := newMgrWithConf(t, conf)
	srv := newServer(mgr)
	srv.registerHandlers()

	loginMsg := &authn.LoginMsg{}
	token, _, err := mgr.issueToken(adminUserID, adminPass, loginMsg)
	tassert.CheckFatal(t, err)
	return srv, token
}

func TestHttpSrvDelete(t *testing.T) {
	srv, adminToken := newTestHserv(t)

	const (
		cluID    = "test-cluster-id"
		cluAlias = "test-cluster"
	)

	// Seed a cluster into the DB so we can delete it.
	clu := authn.CluACL{
		ID:    cluID,
		Alias: cluAlias,
		URLs:  []string{"http://localhost:8080"},
	}
	if _, err := srv.mgr.db.Set(clustersCollection, clu.ID, clu); err != nil {
		t.Fatal(err)
	}

	t.Run("no auth token", func(t *testing.T) {
		path := apc.URLPathClusters.Join(cluID)
		req := httptest.NewRequest(http.MethodDelete, path, http.NoBody)
		w := httptest.NewRecorder()
		srv.clusterHandler(w, req)
		tassert.Errorf(t, w.Code == http.StatusUnauthorized,
			"expected %d, got %d", http.StatusUnauthorized, w.Code)
	})

	t.Run("no cluster ID", func(t *testing.T) {
		path := apc.URLPathClusters.S
		req := httptest.NewRequest(http.MethodDelete, path, http.NoBody)
		req.Header.Set(apc.HdrAuthorization, apc.AuthenticationTypeBearer+" "+adminToken)
		w := httptest.NewRecorder()
		srv.clusterHandler(w, req)
		tassert.Errorf(t, w.Code == http.StatusBadRequest,
			"expected %d, got %d", http.StatusBadRequest, w.Code)
	})

	t.Run("nonexistent cluster", func(t *testing.T) {
		path := apc.URLPathClusters.Join("no-such-cluster")
		req := httptest.NewRequest(http.MethodDelete, path, http.NoBody)
		req.Header.Set(apc.HdrAuthorization, apc.AuthenticationTypeBearer+" "+adminToken)
		w := httptest.NewRecorder()
		srv.clusterHandler(w, req)
		tassert.Errorf(t, w.Code == http.StatusNotFound,
			"expected %d, got %d", http.StatusNotFound, w.Code)
	})

	t.Run("successful delete", func(t *testing.T) {
		path := apc.URLPathClusters.Join(cluID)
		req := httptest.NewRequest(http.MethodDelete, path, http.NoBody)
		req.Header.Set(apc.HdrAuthorization, apc.AuthenticationTypeBearer+" "+adminToken)
		w := httptest.NewRecorder()
		srv.clusterHandler(w, req)
		tassert.Errorf(t, w.Code == http.StatusOK,
			"expected %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())

		// Verify the cluster is actually gone.
		_, code, err := srv.mgr.getCluster(cluID)
		tassert.Errorf(t, err != nil, "cluster should not exist after delete")
		tassert.Errorf(t, code == http.StatusNotFound,
			"expected %d, got %d", http.StatusNotFound, code)
	})

	t.Run("delete already-deleted cluster", func(t *testing.T) {
		path := apc.URLPathClusters.Join(cluID)
		req := httptest.NewRequest(http.MethodDelete, path, http.NoBody)
		req.Header.Set(apc.HdrAuthorization, apc.AuthenticationTypeBearer+" "+adminToken)
		w := httptest.NewRecorder()
		srv.clusterHandler(w, req)
		tassert.Errorf(t, w.Code == http.StatusNotFound,
			"expected %d, got %d", http.StatusNotFound, w.Code)
	})
}
