// Package ais: internal unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools/tassert"

	jsoniter "github.com/json-iterator/go"
)

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
