// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"crypto/tls"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/xact"

	jsoniter "github.com/json-iterator/go"
)

var (
	validIssUrls   = []string{"https://localhost:8080"}
	invalidIssUrls = []string{"invalid-scheme"}
)

func TestConfig(t *testing.T) {
	var (
		highWM           = int32(80)
		lowWM            = int32(60)
		cleanupWM        = int32(55)
		updTime          = time.Second * 20
		dontEvictTime    = time.Hour * 2
		configRegression = map[string]string{
			"periodic.stats_time":   updTime.String(),
			"space.cleanupwm":       strconv.Itoa(int(cleanupWM)),
			"space.lowwm":           strconv.Itoa(int(lowWM)),
			"space.highwm":          strconv.Itoa(int(highWM)),
			"lru.enabled":           "true",
			"lru.capacity_upd_time": updTime.String(),
			"lru.dont_evict_time":   dontEvictTime.String(),
		}
		oconfig      = tools.GetClusterConfig(t)
		ospaceconfig = oconfig.Space
		olruconfig   = oconfig.LRU
		operiodic    = oconfig.Periodic
	)
	defer tools.SetClusterConfig(t, cos.StrKVs{
		"periodic.stats_time":   oconfig.Periodic.StatsTime.String(),
		"space.cleanupwm":       strconv.Itoa(int(oconfig.Space.CleanupWM)),
		"space.lowwm":           strconv.Itoa(int(oconfig.Space.LowWM)),
		"space.highwm":          strconv.Itoa(int(oconfig.Space.HighWM)),
		"lru.enabled":           strconv.FormatBool(oconfig.LRU.Enabled),
		"lru.capacity_upd_time": oconfig.LRU.CapacityUpdTime.String(),
		"lru.dont_evict_time":   oconfig.LRU.DontEvictTime.String(),
	})

	tools.SetClusterConfig(t, configRegression)

	nconfig := tools.GetClusterConfig(t)
	nlruconfig := nconfig.LRU
	nspaceconfig := nconfig.Space
	nperiodic := nconfig.Periodic

	if v, _ := time.ParseDuration(configRegression["periodic.stats_time"]); nperiodic.StatsTime != cos.Duration(v) {
		t.Errorf("StatsTime was not set properly: %v, should be: %v",
			nperiodic.StatsTime, configRegression["periodic.stats_time"])
	} else {
		o := operiodic.StatsTime
		tools.SetClusterConfig(t, cos.StrKVs{"periodic.stats_time": o.String()})
	}
	if v, _ := time.ParseDuration(configRegression["lru.dont_evict_time"]); nlruconfig.DontEvictTime != cos.Duration(v) {
		t.Errorf("DontEvictTime was not set properly: %v, should be: %v",
			nlruconfig.DontEvictTime, configRegression["lru.dont_evict_time"])
	} else {
		o := olruconfig.DontEvictTime
		tools.SetClusterConfig(t, cos.StrKVs{"lru.dont_evict_time": o.String()})
	}

	if v, _ := time.ParseDuration(configRegression["lru.capacity_upd_time"]); nlruconfig.CapacityUpdTime != cos.Duration(v) {
		t.Errorf("CapacityUpdTime was not set properly: %v, should be: %v",
			nlruconfig.CapacityUpdTime, configRegression["lru.capacity_upd_time"])
	} else {
		o := olruconfig.CapacityUpdTime
		tools.SetClusterConfig(t, cos.StrKVs{"lru.capacity_upd_time": o.String()})
	}
	if hw, err := strconv.Atoi(configRegression["space.highwm"]); err != nil {
		t.Fatalf("Error parsing HighWM: %v", err)
	} else if nspaceconfig.HighWM != int64(hw) {
		t.Errorf("HighWatermark was not set properly: %d, should be: %d",
			nspaceconfig.HighWM, hw)
	} else {
		oldhwmStr, err := cos.ConvertToString(ospaceconfig.HighWM)
		if err != nil {
			t.Fatalf("Error parsing HighWM: %v", err)
		}
		tools.SetClusterConfig(t, cos.StrKVs{"space.highwm": oldhwmStr})
	}
	if lw, err := strconv.Atoi(configRegression["space.lowwm"]); err != nil {
		t.Fatalf("Error parsing LowWM: %v", err)
	} else if nspaceconfig.LowWM != int64(lw) {
		t.Errorf("LowWatermark was not set properly: %d, should be: %d",
			nspaceconfig.LowWM, lw)
	} else {
		oldlwmStr, err := cos.ConvertToString(ospaceconfig.LowWM)
		if err != nil {
			t.Fatalf("Error parsing LowWM: %v", err)
		}
		tools.SetClusterConfig(t, cos.StrKVs{"space.lowwm": oldlwmStr})
	}
	if pt, err := cos.ParseBool(configRegression["lru.enabled"]); err != nil {
		t.Fatalf("Error parsing lru.enabled: %v", err)
	} else if nlruconfig.Enabled != pt {
		t.Errorf("lru.enabled was not set properly: %v, should be %v",
			nlruconfig.Enabled, pt)
	} else {
		tools.SetClusterConfig(t, cos.StrKVs{"lru.enabled": strconv.FormatBool(olruconfig.Enabled)})
	}
}

func TestConfigGet(t *testing.T) {
	smap := tools.GetClusterMap(t, tools.GetPrimaryURL())

	proxy, err := smap.GetRandProxy(false)
	tassert.CheckFatal(t, err)
	tools.GetDaemonConfig(t, proxy)

	target, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)
	tools.GetDaemonConfig(t, target)
}

func TestConfigSetGlobal(t *testing.T) {
	var (
		ecCondition bool
		smap        = tools.GetClusterMap(t, tools.GetPrimaryURL())
		config      = tools.GetClusterConfig(t)
		check       = func(snode *meta.Snode, c *cmn.Config) {
			tassert.Errorf(t, c.EC.Enabled == ecCondition,
				"%s expected 'ec.enabled' to be %v, got %v", snode, ecCondition, c.EC.Enabled)
		}
	)
	ecCondition = !config.EC.Enabled
	toUpdate := &cmn.ConfigToSet{EC: &cmn.ECConfToSet{
		Enabled: apc.Ptr(ecCondition),
	}}

	tools.SetClusterConfigUsingMsg(t, toUpdate)
	checkConfig(t, smap, check)

	// Reset config
	ecCondition = config.EC.Enabled
	tools.SetClusterConfig(t, cos.StrKVs{
		"ec.enabled": strconv.FormatBool(ecCondition),
	})
	checkConfig(t, smap, check)

	// wait for ec
	flt := xact.ArgsMsg{Kind: apc.ActECEncode}
	_, _ = api.WaitForXactionIC(baseParams, &flt)
}

func checkConfig(t *testing.T, smap *meta.Smap, check func(*meta.Snode, *cmn.Config)) {
	for _, node := range smap.Pmap {
		config := tools.GetDaemonConfig(t, node)
		check(node, config)
	}
	for _, node := range smap.Tmap {
		config := tools.GetDaemonConfig(t, node)
		check(node, config)
	}
}

func TestConfigFailOverrideClusterOnly(t *testing.T) {
	var (
		proxyURL   = tools.GetPrimaryURL()
		baseParams = tools.BaseAPIParams(proxyURL)
		smap       = tools.GetClusterMap(t, proxyURL)
		config     = tools.GetClusterConfig(t)
	)
	proxy, err := smap.GetRandProxy(false /*exclude primary*/)
	tassert.CheckFatal(t, err)

	// Try overriding cluster only config on a daemon
	err = api.SetDaemonConfig(baseParams, proxy.ID(), cos.StrKVs{"ec.enabled": strconv.FormatBool(!config.EC.Enabled)})
	tassert.Fatalf(t, err != nil, "expected error to occur when trying to override cluster only config")

	daemonConfig := tools.GetDaemonConfig(t, proxy)
	tassert.Errorf(t, daemonConfig.EC.Enabled == config.EC.Enabled,
		"expected 'ec.enabled' to be %v, got: %v", config.EC.Enabled, daemonConfig.EC.Enabled)

	// wait for ec
	flt := xact.ArgsMsg{Kind: apc.ActECEncode}
	_, _ = api.WaitForXactionIC(baseParams, &flt)
}

const errWMConfigNotExpected = "expected 'disk.disk_util_low_wm' to be %d, got: %d"

func TestConfigOverrideAndRestart(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal, MinProxies: 2})
	var (
		proxyURL      = tools.GetPrimaryURL()
		baseParams    = tools.BaseAPIParams(proxyURL)
		smap          = tools.GetClusterMap(t, proxyURL)
		config        = tools.GetClusterConfig(t)
		origProxyCnt  = smap.CountActivePs()
		origTargetCnt = smap.CountActiveTs()
	)
	proxy, err := smap.GetRandProxy(true /*exclude primary*/)
	tassert.CheckFatal(t, err)

	// Override cluster config on the selected proxy
	newLowWM := config.Disk.DiskUtilLowWM - 10
	err = api.SetDaemonConfig(baseParams, proxy.ID(),
		cos.StrKVs{"disk.disk_util_low_wm": strconv.FormatInt(newLowWM, 10)})
	tassert.CheckFatal(t, err)

	daemonConfig := tools.GetDaemonConfig(t, proxy)
	tassert.Errorf(t, daemonConfig.Disk.DiskUtilLowWM == newLowWM,
		errWMConfigNotExpected, newLowWM, daemonConfig.Disk.DiskUtilLowWM)

	// Restart and check that config persisted
	tlog.Logfln("Killing %s", proxy.StringEx())
	cmd, err := tools.KillNode(baseParams, proxy)
	tassert.CheckFatal(t, err)
	smap, err = tools.WaitForClusterState(proxyURL, "proxy removed", smap.Version, origProxyCnt-1, origTargetCnt)
	tassert.CheckFatal(t, err)

	err = tools.RestoreNode(cmd, false, apc.Proxy)
	tassert.CheckFatal(t, err)
	_, err = tools.WaitForClusterState(proxyURL, "proxy restored", smap.Version, origProxyCnt, origTargetCnt)
	tassert.CheckFatal(t, err)

	daemonConfig = tools.GetDaemonConfig(t, proxy)
	tassert.Fatalf(t, daemonConfig.Disk.DiskUtilLowWM == newLowWM,
		errWMConfigNotExpected, newLowWM, daemonConfig.Disk.DiskUtilLowWM)

	// Reset node config.
	err = api.SetDaemonConfig(baseParams, proxy.ID(),
		cos.StrKVs{"disk.disk_util_low_wm": strconv.FormatInt(config.Disk.DiskUtilLowWM, 10)})
	tassert.CheckFatal(t, err)
}

func TestConfigChunksValidate(t *testing.T) {
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

func TestConfigLsoValidate(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		var c cmn.LsoConf
		tassert.CheckFatal(t, c.Validate())

		tassert.Fatalf(t, c.Compression == apc.CompressNever, "expected compression=%q, got %q",
			apc.CompressNever, c.Compression)
		tassert.Fatalf(t, c.SbundleMult == 1, "expected bundle_multiplier=1, got %d", c.SbundleMult)
		tassert.Fatalf(t, c.Burst == 32, "expected burst_buffer=32, got %d", c.Burst)
		tassert.Fatalf(t, c.WalkBuffer == 128, "expected walk_buffer=128, got %d", c.WalkBuffer)
		tassert.Fatalf(t, c.IdleTime.D() == 20*time.Second, "expected idle_time=20s, got %v", c.IdleTime)
		tassert.Fatalf(t, c.QuiesceTime.D() == 5*time.Second, "expected quiescent=5s, got %v", c.QuiesceTime)
	})

	t.Run("partial", func(t *testing.T) {
		c := cmn.LsoConf{WalkBuffer: 256}
		tassert.CheckFatal(t, c.Validate())

		tassert.Fatalf(t, c.WalkBuffer == 256, "expected walk_buffer=256, got %d", c.WalkBuffer)
		tassert.Fatalf(t, c.SbundleMult == 1, "expected bundle_multiplier=1, got %d", c.SbundleMult)
		tassert.Fatalf(t, c.Burst == 32, "expected burst_buffer=32, got %d", c.Burst)
		tassert.Fatalf(t, c.IdleTime.D() == 20*time.Second, "expected idle_time=20s, got %v", c.IdleTime)
		tassert.Fatalf(t, c.QuiesceTime.D() == 5*time.Second, "expected quiescent=5s, got %v", c.QuiesceTime)
	})

	valid := []struct {
		name string
		conf cmn.LsoConf
	}{
		{
			name: "minimums",
			conf: cmn.LsoConf{
				XactConf:    cmn.XactConf{SbundleMult: 1, Burst: 32},
				WalkBuffer:  16,
				IdleTime:    cos.Duration(5 * time.Second),
				QuiesceTime: cos.Duration(2 * time.Second),
			},
		},
		{
			name: "maximums",
			conf: cmn.LsoConf{
				XactConf:    cmn.XactConf{SbundleMult: 16, Burst: 10_000},
				WalkBuffer:  4096,
				IdleTime:    cos.Duration(10 * time.Minute),
				QuiesceTime: cos.Duration(time.Minute),
			},
		},
	}
	for _, tt := range valid {
		t.Run(tt.name, func(t *testing.T) {
			tassert.CheckFatal(t, tt.conf.Validate())
		})
	}

	invalid := []struct {
		name string
		conf cmn.LsoConf
	}{
		{name: "walk buffer below minimum", conf: cmn.LsoConf{WalkBuffer: 15}},
		{name: "walk buffer above maximum", conf: cmn.LsoConf{WalkBuffer: 4097}},
		{name: "idle below minimum", conf: cmn.LsoConf{IdleTime: cos.Duration(4 * time.Second)}},
		{name: "idle above maximum", conf: cmn.LsoConf{IdleTime: cos.Duration(10*time.Minute + time.Second)}},
		{name: "quiescent below minimum", conf: cmn.LsoConf{QuiesceTime: cos.Duration(time.Second)}},
		{name: "quiescent above maximum", conf: cmn.LsoConf{QuiesceTime: cos.Duration(time.Minute + time.Second)}},
		{
			name: "quiescent equals idle",
			conf: cmn.LsoConf{
				IdleTime:    cos.Duration(5 * time.Second),
				QuiesceTime: cos.Duration(5 * time.Second),
			},
		},
		{
			name: "quiescent exceeds idle",
			conf: cmn.LsoConf{
				IdleTime:    cos.Duration(5 * time.Second),
				QuiesceTime: cos.Duration(6 * time.Second),
			},
		},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.conf.Validate(); err == nil {
				t.Fatalf("expected validation error; result=%+v", tt.conf)
			}
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

func TestConfigAuthValidateFailure(t *testing.T) {
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

func TestConfigAuthValidateSuccess(t *testing.T) {
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

func TestConfigAuthSignature_ValidMethods(t *testing.T) {
	conf := cmn.AuthSignatureConf{}
	got := conf.ValidMethods()
	wantAll := []string{"HMAC", "HS256", "HS384", "HS512", "RSA", "RS256", "RS384", "RS512"}
	for _, m := range wantAll {
		tassert.Errorf(t, strings.Contains(got, m), "ValidMethods missing %s", m)
	}
}

func TestConfigAuthSignature_IsHMAC(t *testing.T) {
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

func TestConfigAuthSignature_IsRSA(t *testing.T) {
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

func TestConfigClone_NoAuthTracingAlias(t *testing.T) {
	config := cmn.GCO.BeginUpdate()
	config.Cksum = &cmn.CksumConf{Type: cos.ChecksumOneXxh}
	config.Space = &cmn.SpaceConf{
		LowWM: 75, HighWM: 90, OOS: 95,
	}
	config.LRU = &cmn.LRUConf{
		DontEvictTime: cos.Duration(time.Hour), CapacityUpdTime: cos.Duration(time.Minute), Enabled: true,
	}
	config.ClusterConfig.Auth.Signature = &cmn.AuthSignatureConf{Key: "k"}
	config.ClusterConfig.Tracing = &cmn.TracingConf{Enabled: true, ExporterEndpoint: "x"}
	config.ClusterConfig.Lso = &cmn.LsoConf{WalkBuffer: 256}

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
	if clone.Lso == c.Lso {
		t.Fatal("Lso alias")
	}
	if clone.Cksum == c.Cksum {
		t.Fatal("Cksum alias")
	}
}

func TestConfigHTTPValidateTLS(t *testing.T) {
	const (
		crt = "crt.pem"
		key = "key.pem"
	)

	tests := []struct {
		name    string
		http    cmn.HTTPConf
		wantErr bool
	}{
		{name: "HTTPS without certs", http: cmn.HTTPConf{UseHTTPS: true}, wantErr: true},
		{name: "HTTPS with certs", http: cmn.HTTPConf{UseHTTPS: true, TLSConf: cmn.TLSConf{Certificate: crt, CertKey: key}}},
		{name: "HTTPS with crt only", http: cmn.HTTPConf{UseHTTPS: true, TLSConf: cmn.TLSConf{Certificate: crt}}, wantErr: true},
		{name: "HTTPS with key only", http: cmn.HTTPConf{UseHTTPS: true, TLSConf: cmn.TLSConf{CertKey: key}}, wantErr: true},
		{name: "not HTTPS (yet) with certs", http: cmn.HTTPConf{TLSConf: cmn.TLSConf{Certificate: crt, CertKey: key}}},
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

func TestConfigTLSValidate(t *testing.T) {
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

func TestConfigHTTPIterFieldsPubTLSPaths(t *testing.T) {
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

func TestConfigHTTPCopyPropsPubTLS(t *testing.T) {
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

func TestConfigTLSEnabled(t *testing.T) {
	tests := []struct {
		name string
		tls  *cmn.TLSConf
		want bool
	}{
		{name: "empty", tls: &cmn.TLSConf{}, want: false},
		{name: "cert only", tls: &cmn.TLSConf{Certificate: "crt.pem"}, want: false},
		{name: "key only", tls: &cmn.TLSConf{CertKey: "key.pem"}, want: false},
		{name: "cert and key", tls: &cmn.TLSConf{Certificate: "crt.pem", CertKey: "key.pem"}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tassert.Errorf(t, tt.tls.Enabled() == tt.want, "Enabled() = %v, want %v", tt.tls.Enabled(), tt.want)
		})
	}
}

// TestLocalNetConfigValidate_NoOverlap verifies that different hostnames pass validation
func TestConfigLocalNetValidate_NoOverlap(t *testing.T) {
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

// verifies that same hostname
// with different ports produces a warning but no error (for hostNetwork deployments)
func TestConfigLocalNetValidate_OverlappingHostDifferentPort(t *testing.T) {
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

// verifies that same hostname
// AND same port produces an error
func TestConfigLocalNetValidate_OverlappingHostAndPort(t *testing.T) {
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

// verifies that same hostname
// AND same port for data network produces an error
func TestConfigLocalNetValidate_OverlappingDataHostAndPort(t *testing.T) {
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
func TestConfigHTTPJSONFlat(t *testing.T) {
	legacy := []byte(`{"server_crt":"c.pem","server_key":"k.pem","client_ca_tls":"ca.pem","client_auth_tls":4,"use_https":true}`)
	var c cmn.HTTPConf
	tassert.CheckFatal(t, jsoniter.Unmarshal(legacy, &c))
	tassert.Fatalf(t, c.Certificate == "c.pem" && c.CertKey == "k.pem" && c.ClientCA == "ca.pem" && c.ClientAuthTLS == 4 && c.UseHTTPS,
		"promotion broken: %+v", c)
	b, _ := jsoniter.Marshal(c)
	tassert.Fatalf(t, !strings.Contains(string(b), `"tls"`), "unexpected nesting: %s", b)
}
