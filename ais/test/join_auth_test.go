// Package integration_test.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
)

type joinTestBody struct {
	SI    *meta.Snode        `json:"si"`
	Flags cos.NodeStateFlags `json:"flags"`
}

const invalidNodeJoinSecret = "invalid-node-join-secret"

// NOTE: mirrors ais.nodeJoinSignature (unexported). The canonicalization is pinned
// package-side by TestNodeJoinSignatureKAT; if the two ever diverge, the positive cases
// below start failing with 401.
func nodeJoinHeader(body []byte, secret string) http.Header {
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("ais-self-join-v1"))
	mac.Write([]byte{0})
	mac.Write([]byte(timestamp))
	mac.Write([]byte{0})
	mac.Write(body)
	return http.Header{
		apc.HdrJoinTime: []string{timestamp},
		apc.HdrJoinSig:  []string{base64.RawURLEncoding.EncodeToString(mac.Sum(nil))},
	}
}

func writeNodeJoinSecret(t *testing.T, secretPath, secret string) {
	t.Helper()
	tassert.CheckFatal(t, os.Chmod(secretPath, 0o600))
	tassert.CheckFatal(t, os.WriteFile(secretPath, []byte(secret+"\n"), 0o400))
	tassert.CheckFatal(t, os.Chmod(secretPath, 0o400))
}

func TestSelfJoinSecret(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal, MinProxies: 2})

	var (
		proxyURL   = tools.GetPrimaryURL()
		config     = tools.GetClusterConfig(t)
		secretPath = config.Auth.NodeJoinSecretPath()
		nodeDir    string
	)
	if secretPath == "" {
		t.Skip("node-join secret must be configured before cluster startup")
	}
	if !filepath.IsAbs(secretPath) {
		cwd, err := os.Getwd()
		tassert.CheckFatal(t, err)
		nodeDir = filepath.Clean(filepath.Join(cwd, "..", ".."))
		secretPath = filepath.Join(nodeDir, secretPath)
	}
	validSecret, err := cos.ReadOneLine(secretPath)
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		tools.SetClusterConfig(t, cos.StrKVs{"rebalance.enabled": strconv.FormatBool(config.Rebalance.Enabled)})
	})

	for _, enabled := range []bool{true, false} {
		t.Run("rebalance="+strconv.FormatBool(enabled), func(t *testing.T) {
			tools.SetClusterConfig(t, cos.StrKVs{"rebalance.enabled": strconv.FormatBool(enabled)})
			testNodeJoinRequests(t, proxyURL, validSecret, enabled)
			testProxySelfJoin(t, proxyURL, secretPath, validSecret, nodeDir)
		})
	}
}

func testNodeJoinRequests(t *testing.T, proxyURL, secret string, rebalanceEnabled bool) {
	var (
		smap = tools.GetClusterMap(t, proxyURL)
		bp   = tools.BaseAPIParams(proxyURL)
	)
	bp.Method = http.MethodPost
	node, err := smap.GetRandProxy(true /*excludePrimary*/)
	tassert.CheckFatal(t, err)

	// Compatibility: an existing node's keepalive succeeds with or without a signature.
	header := http.Header{apc.HdrNodeVersion: []string{cmn.VersionAIStore}}
	body := cos.MustMarshal(joinTestBody{SI: node})
	tassert.CheckFatal(t, (&api.ReqParams{BaseParams: bp, Path: apc.URLPathCluKalive.S, Body: body, Header: header}).DoRequest())
	signedHeader := nodeJoinHeader(body, secret)
	signedHeader.Set(apc.HdrNodeVersion, cmn.VersionAIStore)
	tassert.CheckFatal(t, (&api.ReqParams{BaseParams: bp, Path: apc.URLPathCluKalive.S, Body: body, Header: signedHeader}).DoRequest())
	unknown := node.Clone()
	unknown.DaeID = "unknown-keepalive"
	unknown.PubNet = meta.NetInfo{Hostname: "127.0.0.1", Port: "19080", URL: "http://127.0.0.1:19080"}
	unknown.ControlNet = meta.NetInfo{Hostname: "127.0.0.1", Port: "19081", URL: "http://127.0.0.1:19081"}
	unknown.DataNet = meta.NetInfo{Hostname: "127.0.0.1", Port: "19082", URL: "http://127.0.0.1:19082"}
	// Negative: unsigned admission keepalives are rejected.
	for _, test := range []struct {
		name              string
		body              joinTestBody
		requiresRebalance bool
	}{
		{name: "restarted", body: joinTestBody{SI: node, Flags: cos.NodeRestarted}, requiresRebalance: true},
		{name: "unknown", body: joinTestBody{SI: unknown}},
	} {
		if test.requiresRebalance && !rebalanceEnabled {
			continue
		}
		t.Run("unsigned keepalive "+test.name, func(t *testing.T) {
			body := cos.MustMarshal(test.body)
			err := (&api.ReqParams{BaseParams: bp, Path: apc.URLPathCluKalive.S, Body: body, Header: header}).DoRequest()
			herr := cmn.AsErrHTTP(err)
			tassert.Fatalf(t, herr != nil && herr.Status == http.StatusUnauthorized,
				"expected status %d, got %v", http.StatusUnauthorized, err)
		})
	}
	if rebalanceEnabled {
		// Positive: a signed restarted keepalive can re-admit the node.
		body = cos.MustMarshal(joinTestBody{SI: node, Flags: cos.NodeRestarted})
		header = nodeJoinHeader(body, secret)
		header.Set(apc.HdrNodeVersion, cmn.VersionAIStore)
		tassert.CheckFatal(t, (&api.ReqParams{BaseParams: bp, Path: apc.URLPathCluKalive.S, Body: body, Header: header}).DoRequest())
	}

	var (
		selfJoinBody      = cos.MustMarshal(joinTestBody{SI: node})
		unsignedHeader    = http.Header{apc.HdrNodeVersion: []string{cmn.VersionAIStore}}
		invalidHeader     = nodeJoinHeader(selfJoinBody, invalidNodeJoinSecret)
		invalidBody       = []byte("{")
		invalidBodyHeader = nodeJoinHeader(invalidBody, secret)
	)
	invalidHeader.Set(apc.HdrNodeVersion, cmn.VersionAIStore)
	invalidBodyHeader.Set(apc.HdrNodeVersion, cmn.VersionAIStore)

	// Negative: reject malformed or unauthenticated self-join requests.
	for _, test := range []struct {
		name   string
		body   []byte
		header http.Header
		status int
	}{
		{name: "unsigned", body: selfJoinBody, header: unsignedHeader, status: http.StatusUnauthorized},
		{name: "invalid secret", body: selfJoinBody, header: invalidHeader, status: http.StatusUnauthorized},
		{name: "invalid body", body: invalidBody, header: invalidBodyHeader, status: http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := (&api.ReqParams{BaseParams: bp, Path: apc.URLPathCluAutoReg.S, Body: test.body, Header: test.header}).DoRequest()
			herr := cmn.AsErrHTTP(err)
			tassert.Fatalf(t, herr != nil && herr.Status == test.status,
				"expected status %d, got %v", test.status, err)
		})
	}
}

func testProxySelfJoin(t *testing.T, proxyURL, secretPath, validSecret, nodeDir string) {
	smap := tools.GetClusterMap(t, proxyURL)
	proxy, err := smap.GetRandProxy(true /*excludePrimary*/)
	tassert.CheckFatal(t, err)

	// Remove one proxy from Smap.
	cmd, err := tools.KillNode(tools.BaseAPIParams(proxyURL), proxy)
	tassert.CheckFatal(t, err)
	if nodeDir != "" {
		cmd.Dir = nodeDir // preserve relative credential-path resolution on restart
	}
	smap, err = tools.WaitForClusterState(proxyURL, "proxy removed", smap.Version,
		smap.CountActivePs()-1, smap.CountActiveTs())
	tassert.CheckFatal(t, err)

	proxyRejoined := false
	// If the test stops early, restore the proxy with the valid secret.
	t.Cleanup(func() {
		if !proxyRejoined {
			_, _ = tools.KillNode(tools.BaseAPIParams(proxyURL), proxy)
			writeNodeJoinSecret(t, secretPath, validSecret)
			_ = tools.RestoreNode(cmd, false, "proxy")
			_, _ = tools.WaitForClusterState(proxyURL, "cleanup restore", smap.Version,
				smap.CountActivePs()+1, smap.CountActiveTs())
		}
	})

	// Negative: the proxy stays out of Smap after restarting with the invalid secret.
	writeNodeJoinSecret(t, secretPath, invalidNodeJoinSecret)
	tassert.CheckFatal(t, tools.RestoreNode(cmd, false, "proxy"))
	_, err = tools.WaitForClusterState(proxyURL, "proxy rejected", smap.Version,
		smap.CountActivePs()+1, smap.CountActiveTs())
	tassert.Fatalf(t, err == tools.ErrTimedOutStabilize, "expected rejected proxy to stay out of Smap, got %v", err)
	tassert.Fatalf(t, tools.GetClusterMap(t, proxyURL).GetNode(proxy.ID()) == nil,
		"proxy %s joined with an invalid secret", proxy.ID())

	tassert.CheckFatal(t, tools.WaitNodePubAddrNotInUse(proxy, time.Minute))

	// Positive: the same proxy rejoins after restarting with the valid secret.
	writeNodeJoinSecret(t, secretPath, validSecret)
	tassert.CheckFatal(t, tools.RestoreNode(cmd, false, "proxy"))
	_, err = tools.WaitForClusterState(proxyURL, "proxy rejoined", smap.Version,
		smap.CountActivePs()+1, smap.CountActiveTs())
	tassert.CheckFatal(t, err)
	proxyRejoined = true
}
