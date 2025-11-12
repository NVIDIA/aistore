// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package tests_test

import (
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestConfigTestEnv(t *testing.T) {
	oldConfig := cmn.GCO.Get()
	defer func() {
		cmn.GCO.BeginUpdate()
		cmn.GCO.CommitUpdate(oldConfig)
	}()

	confPath := filepath.Join(thisFileDir(t), "configs", "config.json")
	localConfPath := filepath.Join(thisFileDir(t), "configs", "confignet.json")
	newConfig := cmn.Config{}
	err := cmn.LoadConfig(confPath, localConfPath, apc.Proxy, &newConfig)
	tassert.CheckFatal(t, err)
}

func TestConfigFSPaths(t *testing.T) {
	var (
		oldConfig     = cmn.GCO.Get()
		confPath      = filepath.Join(thisFileDir(t), "configs", "config.json")
		localConfPath = filepath.Join(thisFileDir(t), "configs", "configmpaths.json")
	)
	defer func() {
		cmn.GCO.BeginUpdate()
		cmn.GCO.CommitUpdate(oldConfig)
	}()

	var localConf cmn.LocalConfig
	_, err := jsp.LoadMeta(localConfPath, &localConf)
	tassert.CheckFatal(t, err)
	newConfig := cmn.Config{}
	err = cmn.LoadConfig(confPath, localConfPath, apc.Target, &newConfig)
	tassert.CheckFatal(t, err)

	mpaths := localConf.FSP.Paths
	tassert.Fatalf(t, len(newConfig.FSP.Paths) == len(mpaths), "mountpath count %v != %v", len(newConfig.FSP.Paths), len(mpaths))
	for p := range mpaths {
		tassert.Fatalf(t, newConfig.FSP.Paths.Contains(p), "%q not in config FSP", p)
	}
}

func thisFileDir(t *testing.T) string {
	_, filename, _, ok := runtime.Caller(1)
	tassert.Fatalf(t, ok, "Taking path of a file failed")
	return filepath.Dir(filename)
}

func TestValidateMpath(t *testing.T) {
	mpaths := []string{
		"tmp", // not absolute path
		"/",   // root
	}
	for _, mpath := range mpaths {
		_, err := cmn.ValidateMpath(mpath)
		if err == nil {
			t.Errorf("validation of invalid mountpath: %q succeeded", mpath)
		}
	}
}

func TestAuthConfValidateFailure(t *testing.T) {
	tests := []struct {
		auth cmn.AuthConf
		desc string
	}{
		{auth: cmn.AuthConf{Enabled: true, Signature: nil, OIDC: nil}, desc: "no provided validation config"},
		{auth: cmn.AuthConf{Enabled: true, Signature: &cmn.AuthSignatureConf{Key: "key", Method: "HS256"}, OIDC: &cmn.OIDCConf{}}, desc: "both configs set"},
		{auth: cmn.AuthConf{Enabled: true, Signature: &cmn.AuthSignatureConf{Key: "key"}, OIDC: nil}, desc: "missing method"},
		{auth: cmn.AuthConf{Enabled: true, Signature: &cmn.AuthSignatureConf{Key: "key", Method: "wrong"}, OIDC: nil}, desc: "invalid method"},
	}
	for _, tt := range tests {
		if err := tt.auth.Validate(); err == nil {
			t.Errorf("AuthConf.Validate() should have errored [%s] for %#v", tt.desc, tt.auth)
		}
	}
}

func TestAuthConfValidateSuccess(t *testing.T) {
	tests := []struct {
		auth cmn.AuthConf
		desc string
	}{
		{auth: cmn.AuthConf{Enabled: true, Signature: &cmn.AuthSignatureConf{Key: "key", Method: "HS256"}}, desc: "valid signature"},
		{auth: cmn.AuthConf{Enabled: false, Signature: nil, OIDC: nil}, desc: "not enabled"},
	}
	for _, tt := range tests {
		if err := tt.auth.Validate(); err != nil {
			t.Errorf("AuthConf.Validate() unexpected error [%s] for %#v: %v", tt.desc, tt.auth, err)
		}
	}
}

func TestAuthConfUnmarshalJSON(t *testing.T) {
	cases := []struct {
		input []byte
		want  cmn.AuthConf
		desc  string
	}{
		{
			input: []byte(`{"enabled":true,"signature":{"key":"mykey","method":"HS256"},"required_claims":{"aud":["aud"]},"oidc":null}`),
			want:  cmn.AuthConf{Enabled: true, Signature: &cmn.AuthSignatureConf{Key: "mykey", Method: "HS256"}, RequiredClaims: &cmn.RequiredClaimsConf{Aud: []string{"aud"}}, OIDC: nil},
			desc:  "current format with signature",
		},
		{
			input: []byte(`{"enabled":false,"signature":null,"oidc":null}`),
			want:  cmn.AuthConf{Enabled: false, Signature: nil, RequiredClaims: nil, OIDC: nil},
			desc:  "current format, disabled and nils",
		},
	}
	for _, tt := range cases {
		var got cmn.AuthConf
		err := got.UnmarshalJSON(tt.input)
		tassert.CheckFatal(t, err)
		tassert.Errorf(t, reflect.DeepEqual(got, tt.want), "UnmarshalJSON returned %+v, want %+v", got, tt.want)
	}
}

func TestAuthConfUnmarshalJSON_Legacyv4(t *testing.T) {
	input := []byte(`{"enabled":true,"secret":"legacykey"}`)
	want := cmn.AuthConf{
		Enabled:        true,
		Signature:      &cmn.AuthSignatureConf{Key: "legacykey", Method: "HS256"},
		RequiredClaims: nil,
		OIDC:           nil,
	}
	var got cmn.AuthConf
	err := got.UnmarshalJSON(input)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, reflect.DeepEqual(got, want), "UnmarshalJSON returned %+v, want %+v", got, want)
}

func TestAuthConfUnmarshalJSON_InvalidInput(t *testing.T) {
	input := []byte("{invalid json")
	var got cmn.AuthConf
	err := got.UnmarshalJSON(input)
	tassert.Fatal(t, err != nil, "UnmarshalJSON should return an error for invalid input")
}

func TestAuthSignatureConf_ValidMethods(t *testing.T) {
	conf := cmn.AuthSignatureConf{}
	got := conf.ValidMethods()
	wantAll := []string{"HMAC", "HS256", "HS384", "HS512", "RSA", "RS256", "RS384", "RS512"}
	for _, m := range wantAll {
		tassert.Errorf(t, strings.Contains(got, m), "ValidMethods missing %s", m)
	}
}

func TestAuthSignatureConf_IsHMAC(t *testing.T) {
	tests := []struct {
		method string
		want   bool
	}{
		{"HMAC", true},
		{"HS256", true},
		{"HS384", true},
		{"hs512", true},
		{"RSA", false},
		{"foobar", false},
	}
	for _, tt := range tests {
		conf := cmn.AuthSignatureConf{Method: tt.method}
		tassert.Errorf(t, conf.IsHMAC() == tt.want, "IsHMAC(%q) = %v, want %v", tt.method, conf.IsRSA(), tt.want)
	}
}

func TestAuthSignatureConf_IsRSA(t *testing.T) {
	tests := []struct {
		method string
		want   bool
	}{
		{"RSA", true},
		{"RS256", true},
		{"RS384", true},
		{"rs512", true},
		{"HMAC", false},
		{"foobar", false},
	}
	for _, tt := range tests {
		conf := cmn.AuthSignatureConf{Method: tt.method}
		tassert.Errorf(t, conf.IsRSA() == tt.want, "IsRSA(%q) = %v, want %v", tt.method, conf.IsRSA(), tt.want)
	}
}
