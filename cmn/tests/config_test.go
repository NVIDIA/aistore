// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package tests_test

import (
	"crypto/tls"
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

	jsoniter "github.com/json-iterator/go"
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

func TestChunksConfValidate(t *testing.T) {
	tests := []struct {
		name          string
		in            cmn.ChunksConf
		wantErr       bool
		wantChunkSize cos.SizeIEC
		wantMaxMono   cos.SizeIEC
	}{
		{
			// the prod-cluster panic scenario: legacy seed file or untouched bucket
			// where AutoEnabled is off and ChunkSize was never set. The validator
			// must normalize ChunkSize to ChunkSizeDflt so the > MaxMonolithicSize
			// safety branch in putObject() can never see a zero.
			name:          "auto-disabled, zero chunk_size backfilled",
			in:            cmn.ChunksConf{ObjSizeLimit: 0, ChunkSize: 0, MaxMonolithicSize: 0},
			wantChunkSize: cmn.ChunkSizeDflt,
			wantMaxMono:   cmn.MaxMonolithicSize,
		},
		{
			name:          "auto-enabled, zero chunk_size backfilled",
			in:            cmn.ChunksConf{ObjSizeLimit: 64 * cos.MiB, ChunkSize: 0, MaxMonolithicSize: 0},
			wantChunkSize: cmn.ChunkSizeDflt,
			wantMaxMono:   cmn.MaxMonolithicSize,
		},
		{
			name:          "auto-disabled, explicit chunk_size preserved",
			in:            cmn.ChunksConf{ObjSizeLimit: 0, ChunkSize: 2 * cos.GiB, MaxMonolithicSize: 0},
			wantChunkSize: 2 * cos.GiB,
			wantMaxMono:   cmn.MaxMonolithicSize,
		},
		{
			// range check must apply even when auto-chunking is disabled
			name:    "auto-disabled, chunk_size below min rejected",
			in:      cmn.ChunksConf{ObjSizeLimit: 0, ChunkSize: cmn.ChunkSizeMin / 2, MaxMonolithicSize: 0},
			wantErr: true,
		},
		{
			name:    "auto-disabled, chunk_size above max rejected",
			in:      cmn.ChunksConf{ObjSizeLimit: 0, ChunkSize: cmn.ChunkSizeMax + 1, MaxMonolithicSize: 0},
			wantErr: true,
		},
		{
			name:    "auto-enabled, chunk_size above max rejected",
			in:      cmn.ChunksConf{ObjSizeLimit: 64 * cos.MiB, ChunkSize: cmn.ChunkSizeMax + 1, MaxMonolithicSize: 0},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.in
			err := c.Validate()
			if tt.wantErr {
				tassert.Fatalf(t, err != nil, "expected error, got nil; result=%+v", c)
				return
			}
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, c.ChunkSize == tt.wantChunkSize,
				"chunk_size: want %d, got %d", tt.wantChunkSize, c.ChunkSize)
			tassert.Fatalf(t, c.MaxMonolithicSize == tt.wantMaxMono,
				"max_monolithic_size: want %d, got %d", tt.wantMaxMono, c.MaxMonolithicSize)
		})
	}
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
		{auth: cmn.AuthConf{Enabled: true, Signature: nil, OIDC: &cmn.OIDCConf{AllowedIssuers: validIssUrls, JWKSCacheConf: &cmn.JWKSCacheConf{MinBackgroundRefresh: cos.Duration(time.Second)}}}, desc: "min_refresh_interval too small"},
		{auth: cmn.AuthConf{Enabled: true, Signature: nil, OIDC: &cmn.OIDCConf{AllowedIssuers: validIssUrls, JWKSCacheConf: &cmn.JWKSCacheConf{MinRotationRefresh: cos.Duration(500 * time.Millisecond)}}}, desc: "min_rotation_refresh too small"},
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
		{auth: cmn.AuthConf{Enabled: true, Signature: nil, OIDC: &cmn.OIDCConf{AllowedIssuers: validIssUrls, JWKSCacheConf: &cmn.JWKSCacheConf{MinBackgroundRefresh: cos.Duration(10 * time.Minute)}}}, desc: "valid OIDC with custom background refresh"},
		{auth: cmn.AuthConf{Enabled: true, Signature: nil, OIDC: &cmn.OIDCConf{AllowedIssuers: validIssUrls, JWKSCacheConf: &cmn.JWKSCacheConf{MinRotationRefresh: cos.Duration(5 * time.Second)}}}, desc: "valid OIDC with custom rotation refresh"},
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
	// v5.0
	if c.Net.HTTP.Pub != nil {
		tassert.Fatalf(t, clone.Net.HTTP.Pub != c.Net.HTTP.Pub, "cloned Pub aliases source")
	}
}

func TestHTTPConfValidateTLS(t *testing.T) {
	const crt = "crt.pem"

	tests := []struct {
		name    string
		http    cmn.HTTPConf
		wantErr bool
	}{
		{name: "pub fully unset", http: cmn.HTTPConf{}},
		{name: "pub auth knobs without certs", http: cmn.HTTPConf{Pub: &cmn.TLSConf{ClientAuthTLS: int(tls.RequireAndVerifyClientCert)}}, wantErr: true},
		{name: "pub CA without certs", http: cmn.HTTPConf{Pub: &cmn.TLSConf{ClientCA: "ca.pem"}}, wantErr: true},
		{name: "pub cert without key", http: cmn.HTTPConf{Pub: &cmn.TLSConf{Certificate: crt}}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.http.Validate()
			if tt.wantErr {
				tassert.Fatalf(t, err != nil, "expected error, got nil; http=%+v", tt.http)
				return
			}
			tassert.CheckFatal(t, err)
		})
	}
}

func TestTLSConfValidate(t *testing.T) {
	const (
		crt = "crt.pem"
		key = "key.pem"
		ca  = "ca.pem"
	)
	tests := []struct {
		name    string
		tls     cmn.TLSConf
		tag     string
		wantErr bool
	}{
		{name: "empty", tag: "net.http"},
		{name: "cert without key", tag: "net.http", tls: cmn.TLSConf{Certificate: crt}, wantErr: true},
		{name: "key without cert", tag: "net.http", tls: cmn.TLSConf{CertKey: key}, wantErr: true},

		// client_auth_tls <= RequireAnyClientCert(2): no verification, CA not required
		// (existing client_auth_tls=2 deployments with empty client_ca_tls must survive upgrade)
		{name: "request cert without CA", tag: "net.http", tls: cmn.TLSConf{Certificate: crt, CertKey: key, ClientAuthTLS: int(tls.RequestClientCert)}},
		{name: "require-any without CA", tag: "net.http.pub", tls: cmn.TLSConf{Certificate: crt, CertKey: key, ClientAuthTLS: int(tls.RequireAnyClientCert)}},

		// client_auth_tls >= VerifyClientCertIfGiven(3): verification, CA required
		{name: "verify-if-given without CA", tag: "net.http", tls: cmn.TLSConf{Certificate: crt, CertKey: key, ClientAuthTLS: int(tls.VerifyClientCertIfGiven)}, wantErr: true},
		{name: "require-and-verify without CA", tag: "net.http.pub", tls: cmn.TLSConf{Certificate: crt, CertKey: key, ClientAuthTLS: int(tls.RequireAndVerifyClientCert)}, wantErr: true},
		{name: "require-and-verify with CA", tag: "net.http.pub", tls: cmn.TLSConf{Certificate: crt, CertKey: key, ClientCA: ca, ClientAuthTLS: int(tls.RequireAndVerifyClientCert)}},

		{name: "client_auth_tls out of range (negative)", tag: "net.http", tls: cmn.TLSConf{Certificate: crt, CertKey: key, ClientAuthTLS: -1}, wantErr: true},
		{name: "client_auth_tls out of range (high)", tag: "net.http", tls: cmn.TLSConf{Certificate: crt, CertKey: key, ClientAuthTLS: int(tls.RequireAndVerifyClientCert) + 1}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.tls.Validate(tt.tag)
			if tt.wantErr {
				tassert.Fatalf(t, err != nil, "expected error, got nil; tls=%+v", tt.tls)
				return
			}
			tassert.CheckFatal(t, err)
		})
	}
}

func TestHTTPConfIterFieldsPubTLSPaths(t *testing.T) {
	http := cmn.HTTPConf{
		TLSConf: cmn.TLSConf{Certificate: "main.crt", CertKey: "main.key"},
		Pub:     &cmn.TLSConf{Certificate: "pub.crt", CertKey: "pub.key"},
	}
	want := map[string]any{
		"server_crt":     "main.crt",
		"server_key":     "main.key",
		"pub.server_crt": "pub.crt",
		"pub.server_key": "pub.key",
	}

	got := make(map[string]any)
	err := cmn.IterFields(http, func(tag string, field cmn.IterField) (error, bool) {
		got[tag] = field.Value()
		return nil, false
	})
	tassert.CheckFatal(t, err)
	for tag, wantVal := range want {
		tassert.Errorf(t, got[tag] == wantVal, "%s = %v, want %v", tag, got[tag], wantVal)
	}
}

func TestHTTPConfCopyPropsPubTLS(t *testing.T) {
	mainCrt, mainKey := "main.crt", "main.key"
	pubCrt, pubKey := "pub.crt", "pub.key"

	src := cmn.HTTPConfToSet{
		TLSConfToSet: &cmn.TLSConfToSet{
			Certificate: &mainCrt,
			CertKey:     &mainKey,
		},
		Pub: &cmn.TLSConfToSet{
			Certificate: &pubCrt,
			CertKey:     &pubKey,
		},
	}
	tests := []struct {
		name string
		pub  *cmn.TLSConf
	}{
		{name: "create pub"},
		{
			name: "update pub",
			pub:  &cmn.TLSConf{Certificate: "old-pub.crt", CertKey: "old-pub.key"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := cmn.HTTPConf{
				TLSConf: cmn.TLSConf{
					Certificate: "old.crt",
					CertKey:     "old.key",
				},
				Pub: tt.pub,
			}

			tassert.CheckFatal(t, cmn.CopyProps(src, &dst, apc.Cluster))
			tassert.Errorf(t, dst.Certificate == mainCrt, "main cert = %q, want %q", dst.Certificate, mainCrt)
			tassert.Errorf(t, dst.CertKey == mainKey, "main key = %q, want %q", dst.CertKey, mainKey)

			tassert.Fatalf(t, dst.Pub != nil, "expected pub TLS config to be allocated")
			tassert.Errorf(t, dst.Pub.Certificate == pubCrt, "pub cert = %q, want %q", dst.Pub.Certificate, pubCrt)
			tassert.Errorf(t, dst.Pub.CertKey == pubKey, "pub key = %q, want %q", dst.Pub.CertKey, pubKey)
		})
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

// expecting TLSConf inline
func TestHTTPConfJSONFlat(t *testing.T) {
	legacy := []byte(`{"server_crt":"c.pem","server_key":"k.pem","client_ca_tls":"ca.pem","client_auth_tls":4,"use_https":true}`)
	var c cmn.HTTPConf
	tassert.CheckFatal(t, jsoniter.Unmarshal(legacy, &c))
	tassert.Fatalf(t, c.Certificate == "c.pem" && c.CertKey == "k.pem" && c.ClientCA == "ca.pem" && c.ClientAuthTLS == 4 && c.UseHTTPS,
		"promotion broken: %+v", c)
	b, _ := jsoniter.Marshal(c)
	tassert.Fatalf(t, !strings.Contains(string(b), `"tls"`), "unexpected nesting: %s", b)
}
