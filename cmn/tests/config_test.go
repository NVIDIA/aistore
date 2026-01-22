// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package tests_test

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/tools/tassert"
)

var (
	validIssUrls   = []string{"https://localhost:8080"}
	invalidIssUrls = []string{"invalid-scheme"}
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
		{auth: cmn.AuthConf{Enabled: true, Signature: &cmn.AuthSignatureConf{Key: "key"}, OIDC: nil}, desc: "missing method"},
		{auth: cmn.AuthConf{Enabled: true, Signature: &cmn.AuthSignatureConf{Key: "key", Method: "wrong"}, OIDC: nil}, desc: "invalid method"},
		{auth: cmn.AuthConf{Enabled: true, Signature: &cmn.AuthSignatureConf{Key: "key", Method: "HS256"}, OIDC: &cmn.OIDCConf{AllowedIssuers: validIssUrls}}, desc: "both configs set"},
		{auth: cmn.AuthConf{Enabled: true, Signature: nil, OIDC: &cmn.OIDCConf{AllowedIssuers: invalidIssUrls}}, desc: "invalid allowed issuer"},
		{auth: cmn.AuthConf{Enabled: true, Signature: nil, OIDC: &cmn.OIDCConf{AllowedIssuers: []string{}}}, desc: "missing allowed issuers"},
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
		{auth: cmn.AuthConf{Enabled: true, Signature: nil, OIDC: &cmn.OIDCConf{AllowedIssuers: validIssUrls}}, desc: "valid OIDC"},
		{auth: cmn.AuthConf{Enabled: false, Signature: nil, OIDC: nil}, desc: "not enabled"},
	}
	for _, tt := range tests {
		if err := tt.auth.Validate(); err != nil {
			t.Errorf("AuthConf.Validate() for case [%s] with %#v raised unexpected error: %v", tt.desc, tt.auth, err)
		}
	}
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

func TestGCOClone_NoAuthTracingAlias(t *testing.T) {
	config := cmn.GCO.BeginUpdate()
	config.Cksum.Type = cos.ChecksumOneXxh
	config.Space = cmn.SpaceConf{
		LowWM: 75, HighWM: 90, OOS: 95,
	}
	config.LRU = cmn.LRUConf{
		DontEvictTime: cos.Duration(time.Hour), CapacityUpdTime: cos.Duration(time.Minute), Enabled: true,
	}
	config.ClusterConfig.Auth.Signature = &cmn.AuthSignatureConf{Key: "k"}
	config.ClusterConfig.Tracing = &cmn.TracingConf{Enabled: true, ExporterEndpoint: "x"}
	cmn.GCO.CommitUpdate(config)

	c := cmn.GCO.Get()
	clone := cmn.GCO.Clone()

	if &clone.Auth == &c.Auth {
		t.Fatal("Auth alias")
	}
	if clone.Auth.Signature == c.Auth.Signature {
		t.Fatal("Auth.Signature alias")
	}
	if clone.Tracing == c.Tracing {
		t.Fatal("Tracing alias")
	}
}

// TestLocalNetConfigValidate_NoOverlap verifies that different hostnames pass validation
func TestLocalNetConfigValidate_NoOverlap(t *testing.T) {
	tests := []struct {
		name                 string
		hostname             string
		hostnameIntraControl string
		hostnameIntraData    string
	}{
		{
			name:                 "IP addresses",
			hostname:             "192.0.2.1",
			hostnameIntraControl: "198.51.100.1",
			hostnameIntraData:    "203.0.113.1",
		},
		{
			name:                 "podDNS format",
			hostname:             "target-0-hostname.example.com",
			hostnameIntraControl: "target-0.target-svc.ns1.svc.cluster.local",
			hostnameIntraData:    "target-0.target-svc.ns1.svc.cluster.local",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			contextConfig := &cmn.Config{}
			localNet := cmn.LocalNetConfig{
				Hostname:             tt.hostname,
				HostnameIntraControl: tt.hostnameIntraControl,
				HostnameIntraData:    tt.hostnameIntraData,
				Port:                 8080,
				PortIntraControl:     9080,
				PortIntraData:        10080,
			}
			err := localNet.Validate(contextConfig)
			tassert.CheckFatal(t, err)
		})
	}
}

// TestLocalNetConfigValidate_OverlappingHostDifferentPort verifies that same hostname
// with different ports produces a warning but no error (for hostNetwork deployments)
func TestLocalNetConfigValidate_OverlappingHostDifferentPort(t *testing.T) {
	tests := []struct {
		name     string
		hostname string // Used for all three: public, control, and data
	}{
		{
			name:     "IP address",
			hostname: "192.0.2.1",
		},
		{
			name:     "podDNS format (hostNetwork scenario)",
			hostname: "target-0.target-svc.ns.svc.cluster.local",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			contextConfig := &cmn.Config{}
			localNet := cmn.LocalNetConfig{
				Hostname:             tt.hostname,
				HostnameIntraControl: tt.hostname, // Same as public
				HostnameIntraData:    tt.hostname, // Same as public
				Port:                 8080,
				PortIntraControl:     9080,  // Different port
				PortIntraData:        10080, // Different port
			}
			err := localNet.Validate(contextConfig)
			// Should NOT return an error - just warns
			tassert.CheckFatal(t, err)
		})
	}
}

// TestLocalNetConfigValidate_OverlappingHostAndPort verifies that same hostname
// AND same port produces an error
func TestLocalNetConfigValidate_OverlappingHostAndPort(t *testing.T) {
	tests := []struct {
		name     string
		hostname string
	}{
		{
			name:     "IP address",
			hostname: "192.0.2.1",
		},
		{
			name:     "podDNS format",
			hostname: "target-0.target-svc.ns.svc.cluster.local",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			contextConfig := &cmn.Config{}
			localNet := cmn.LocalNetConfig{
				Hostname:             tt.hostname,
				HostnameIntraControl: tt.hostname,    // Same as public
				HostnameIntraData:    "198.51.100.1", // Different
				Port:                 8080,
				PortIntraControl:     8080, // Same port - should error!
				PortIntraData:        10080,
			}
			err := localNet.Validate(contextConfig)
			tassert.Fatalf(t, err != nil, "expected error when hostname and port overlap, got nil")
		})
	}
}

// TestLocalNetConfigValidate_OverlappingDataHostAndPort verifies that same hostname
// AND same port for data network produces an error
func TestLocalNetConfigValidate_OverlappingDataHostAndPort(t *testing.T) {
	tests := []struct {
		name     string
		hostname string
	}{
		{
			name:     "IP address",
			hostname: "192.0.2.1",
		},
		{
			name:     "podDNS format",
			hostname: "target-0.target-svc.ns.svc.cluster.local",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			contextConfig := &cmn.Config{}
			localNet := cmn.LocalNetConfig{
				Hostname:             tt.hostname,
				HostnameIntraControl: "198.51.100.1",
				HostnameIntraData:    tt.hostname, // Same as public
				Port:                 8080,
				PortIntraControl:     9080,
				PortIntraData:        8080, // Same port - should error!
			}
			err := localNet.Validate(contextConfig)
			tassert.Fatalf(t, err != nil, "expected error when data hostname and port overlap, got nil")
		})
	}
}
