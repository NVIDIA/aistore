// Package integration_test.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/volume"
)

type joinTestBody struct {
	SI    *meta.Snode        `json:"si"`
	Flags cos.NodeStateFlags `json:"flags"`
}

const invalidNodeJoinSecret = "invalid-node-join-secret"

func TestPodDerivedRejoin(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal})

	config := tools.GetClusterConfig(t)
	config.Version, config.UUID = 0, ""
	config.Rebalance.Enabled = true
	config.Keepalive.Proxy.Interval = cos.Duration(45 * time.Second)
	config.Keepalive.Proxy.Factor = 2
	host, scheme := "127.0.0.1", "http"
	if config.Net.UseIPv6 {
		host = "::1"
	}
	if config.Net.HTTP.UseHTTPS {
		scheme = "https"
	}

	// reserve distinct ports for the private cluster and its replacements
	var listeners [10]net.Listener
	var ports [len(listeners)]int
	for i := range listeners {
		listener, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
		tassert.CheckFatal(t, err)
		t.Cleanup(func() { _ = listener.Close() })
		listeners[i] = listener
		ports[i] = listener.Addr().(*net.TCPAddr).Port
	}
	for _, listener := range listeners {
		tassert.CheckFatal(t, listener.Close())
	}
	primaryURL := cmn.ProtoEndpoint(scheme, net.JoinHostPort(host, strconv.Itoa(ports[0])))
	config.Proxy.PrimaryURL, config.Proxy.OriginalURL, config.Proxy.DiscoveryURL = primaryURL, primaryURL, primaryURL
	t.Setenv(env.AisPrimaryEP, primaryURL)
	t.Setenv(env.AisK8sNamespace, "ais-rejoin-test")
	t.Setenv("AIS_DAEMON_ID", "")

	start := func(role, pod string, public, control int, args ...string) func() {
		t.Helper()
		tlog.Logfln("Starting %s pod %s with fresh state (public port %d, control port %d)", role, pod, public, control)
		t.Setenv(env.AisK8sPod, pod)
		local := &cmn.LocalConfig{
			ConfigDir: t.TempDir(),
			LogDir:    t.TempDir(),
			TestFSP:   cmn.TestFSPConf{Root: t.TempDir(), Count: 1},
			HostNet: cmn.LocalNetConfig{
				Hostname: host, HostnameIntraControl: host, Port: public, PortIntraControl: control,
			},
		}
		tassert.CheckFatal(t, os.Mkdir(filepath.Join(local.TestFSP.Root, "mp1"), 0o755))
		cfg := *config
		pid := tools.DeployNode(t, &meta.Snode{DaeType: role}, &cfg, local, append(args, "-daemon_id_source=k8s-pod")...)
		stop := func() {
			if pid != 0 {
				tools.CleanupNode(t, pid)
				pid = 0
			}
		}
		t.Cleanup(stop)
		return stop
	}
	start(apc.Proxy, "ais-proxy-0", ports[0], ports[1], "-ntargets=2")
	start(apc.Target, "ais-target-0", ports[2], ports[3])
	stopTarget := start(apc.Target, "ais-target-1", ports[4], ports[5])
	bp := tools.BaseAPIParams(primaryURL)
	startup := tools.WaitRetryOpts{MaxRetries: 160, Interval: 250 * time.Millisecond}
	rejoin := tools.WaitRetryOpts{MaxRetries: 60, Interval: 250 * time.Millisecond}
	tlog.Logfln("Waiting for private cluster primary %s to be ready for rebalance", primaryURL)
	tassert.CheckFatal(t, tools.WaitForCondition(func() bool { return api.Health(bp, true) == nil }, startup))
	tlog.Logfln("Private cluster is ready for rebalance")

	checkRejoin := func(role, pod string, public, control int, stop, onPublished func()) {
		t.Helper()
		var old *meta.Snode
		tlog.Logfln("Waiting for %s pod %s to register and become ready", role, pod)
		tassert.CheckFatal(t, tools.WaitForCondition(func() bool {
			smap, err := api.GetClusterMap(bp)
			if err != nil {
				return false
			}
			nodes := smap.Tmap
			if role == apc.Proxy {
				nodes = smap.Pmap
			}
			for _, node := range nodes {
				if node.PubNet.Port == strconv.Itoa(public) && api.GetProxyReadiness(tools.BaseAPIParams(node.PubNet.URL)) == nil {
					old = node
					return true
				}
			}
			return false
		}, rejoin))

		tlog.Logfln("Killing %s (control %s)", old.StringEx(), old.ControlNet.URL)
		stop()
		tassert.CheckFatal(t, tools.WaitForCondition(func() bool {
			return api.Health(tools.BaseAPIParams(old.PubNet.URL)) != nil
		}, rejoin))
		current, err := api.GetClusterMap(bp)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, current.GetNode(old.ID()) != nil, "%s was removed before the replacement started", old.StringEx())
		tlog.Logfln("%s is down and its old registration is still in Smap v%d", old.StringEx(), current.Version)

		start(role, pod, public, control)
		tlog.Logfln("Waiting for %s to rejoin at control port %d", old.StringEx(), control)
		published := false
		err = tools.WaitForCondition(func() bool {
			smap, err := api.GetClusterMap(bp)
			if err != nil {
				return false
			}
			newNode := smap.GetNode(old.ID())
			tassert.Fatalf(t, newNode != nil, "%s was removed while rejoining", old.StringEx())
			if newNode.ControlNet.Port != strconv.Itoa(control) || newNode.SameIncarnation(old) {
				return false
			}
			if !published {
				tlog.Logfln("Smap v%d published %s with control %s and a new verifying key, waiting for readiness",
					smap.Version, newNode.StringEx(), newNode.ControlNet.URL)
				published = true
				if onPublished != nil {
					onPublished()
				}
			}
			return api.GetProxyReadiness(tools.BaseAPIParams(newNode.PubNet.URL)) == nil
		}, rejoin)
		tassert.Fatalf(t, err == nil, "%s did not rejoin at control port %d: %v", old.StringEx(), control, err)
		tlog.Logfln("Replacement %s is ready", old.StringEx())
	}
	// a same-id replacement with fresh state must not need early GFN from the other target
	smap, err := api.GetClusterMap(bp)
	tassert.CheckFatal(t, err)
	var survivor *meta.Snode
	for _, node := range smap.Tmap {
		if node.PubNet.Port == strconv.Itoa(ports[2]) {
			survivor = node
			break
		}
	}
	tassert.Fatalf(t, survivor != nil, "surviving target missing from Smap")
	tlog.Logfln("Pausing %s during the same-ID target rejoin", survivor.StringEx())
	resume, err := tools.PauseNode(survivor)
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		if resume != nil {
			tassert.CheckError(t, resume())
		}
	})
	checkRejoin(apc.Target, "ais-target-1", ports[4], ports[6], stopTarget, func() {
		tassert.CheckFatal(t, resume())
		resume = nil
		tlog.Logfln("Resumed %s", survivor.StringEx())
	})
	stopProxy := start(apc.Proxy, "ais-proxy-1", ports[7], ports[8])
	checkRejoin(apc.Proxy, "ais-proxy-1", ports[7], ports[9], stopProxy, nil)
}

// pod-derived target restarts (SIGKILL) with its original mountpaths, one of which now stores a bad VMD copy:
// - the target must derive the same ID, ignore the bad copy, and rejoin
// - the bad copy must be replaced
func TestPodDerivedRestartBadVMD(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal})

	config := tools.GetClusterConfig(t)
	config.Version, config.UUID = 0, ""
	host, scheme := "127.0.0.1", "http"
	if config.Net.UseIPv6 {
		host = "::1"
	}
	if config.Net.HTTP.UseHTTPS {
		scheme = "https"
	}
	// reserve distinct ports for the private cluster
	var listeners [4]net.Listener
	var ports [len(listeners)]int
	for i := range listeners {
		listener, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
		tassert.CheckFatal(t, err)
		t.Cleanup(func() { _ = listener.Close() })
		listeners[i] = listener
		ports[i] = listener.Addr().(*net.TCPAddr).Port
	}
	for _, listener := range listeners {
		tassert.CheckFatal(t, listener.Close())
	}
	primaryURL := cmn.ProtoEndpoint(scheme, net.JoinHostPort(host, strconv.Itoa(ports[0])))
	config.Proxy.PrimaryURL, config.Proxy.OriginalURL, config.Proxy.DiscoveryURL = primaryURL, primaryURL, primaryURL
	t.Setenv(env.AisPrimaryEP, primaryURL)
	t.Setenv(env.AisK8sNamespace, "ais-vmd-test")
	t.Setenv("AIS_DAEMON_ID", "")

	fspRoot := t.TempDir()
	mpaths := []string{filepath.Join(fspRoot, "mp1"), filepath.Join(fspRoot, "mp2")}
	for _, mpath := range mpaths {
		tassert.CheckFatal(t, os.Mkdir(mpath, 0o755))
	}
	start := func(role, pod string, public, control int, fsp cmn.TestFSPConf, args ...string) func() {
		t.Helper()
		tlog.Logfln("Starting %s pod %s (public port %d, control port %d, mountpaths %d)", role, pod, public, control, fsp.Count)
		t.Setenv(env.AisK8sPod, pod)
		local := &cmn.LocalConfig{
			ConfigDir: t.TempDir(),
			LogDir:    t.TempDir(),
			TestFSP:   fsp,
			HostNet: cmn.LocalNetConfig{
				Hostname: host, HostnameIntraControl: host, Port: public, PortIntraControl: control,
			},
		}
		cfg := *config
		pid := tools.DeployNode(t, &meta.Snode{DaeType: role}, &cfg, local, append(args, "-daemon_id_source=k8s-pod")...)
		stop := func() {
			if pid != 0 {
				tools.CleanupNode(t, pid)
				pid = 0
			}
		}
		t.Cleanup(stop)
		return stop
	}
	proxyRoot := t.TempDir()
	tassert.CheckFatal(t, os.Mkdir(filepath.Join(proxyRoot, "mp1"), 0o755))
	start(apc.Proxy, "ais-proxy-0", ports[0], ports[1], cmn.TestFSPConf{Root: proxyRoot, Count: 1}, "-ntargets=1")
	targetFSP := cmn.TestFSPConf{Root: fspRoot, Count: len(mpaths)}
	stop := start(apc.Target, "ais-target-0", ports[2], ports[3], targetFSP)

	var (
		bp      = tools.BaseAPIParams(primaryURL)
		startup = tools.WaitRetryOpts{MaxRetries: 160, Interval: 250 * time.Millisecond}
		target  *meta.Snode
	)
	ready := func(match func(*meta.Snode) bool) error {
		return tools.WaitForCondition(func() bool {
			smap, err := api.GetClusterMap(bp)
			if err != nil {
				return false
			}
			for _, node := range smap.Tmap {
				if match(node) && api.GetProxyReadiness(tools.BaseAPIParams(node.PubNet.URL)) == nil {
					target = node
					return true
				}
			}
			return false
		}, startup)
	}
	tlog.Logfln("Waiting for the target to join private cluster %s", primaryURL)
	tassert.CheckFatal(t, ready(func(*meta.Snode) bool { return true }))
	old := target
	tlog.Logfln("%s is ready", old.StringEx())

	stop()
	tassert.CheckFatal(t, tools.WaitForCondition(func() bool {
		return api.Health(tools.BaseAPIParams(old.PubNet.URL)) != nil
	}, startup))
	vmd, err := volume.LoadVMDTest(old.ID(), mpaths...)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, vmd != nil, "%s: no VMD on %v", old.StringEx(), mpaths)
	tlog.Logfln("%s is down, %s", old.StringEx(), vmd)

	bad := filepath.Join(mpaths[1], fname.Vmd)
	tassert.CheckFatal(t, cos.Stat(bad))
	tlog.Logfln("Writing a bad VMD copy: %s", bad)
	tassert.CheckFatal(t, os.WriteFile(bad, []byte("garbage"), 0o644))

	start(apc.Target, "ais-target-0", ports[2], ports[3], targetFSP)
	tlog.Logfln("Waiting for %s to restart and rejoin", old.StringEx())
	err = ready(func(node *meta.Snode) bool { return node.ID() == old.ID() && !node.SameIncarnation(old) })
	tassert.Fatalf(t, err == nil, "%s did not rejoin: %v", old.StringEx(), err)
	tlog.Logfln("%s rejoined", target.StringEx())

	after, err := volume.LoadVMDTest(old.ID(), mpaths...)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, after != nil && after.Version >= vmd.Version, "expected VMD version >= %d, got %v", vmd.Version, after)
	repaired, err := volume.LoadVMDTest(old.ID(), mpaths[1])
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, repaired != nil && repaired.Version == after.Version, "%s: expected repaired VMD v%d, got %v", bad, after.Version, repaired)
	tlog.Logfln("Bad copy replaced: %s", repaired)
}

func TestSlowKeepaliveDuplicateID(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 1})
	var (
		proxyURL = tools.GetPrimaryURL()
		smap     = tools.GetClusterMap(t, proxyURL)
		bp       = tools.BaseAPIParams(proxyURL)
	)
	nodes := smap.Tmap.ActiveNodes()
	if len(nodes) == 0 {
		t.Skip("requires an active target")
	}
	node := nodes[0]
	claimant := node.Clone()
	// The registered endpoint responds with its original identity, rejecting this claimant.
	claimant.ControlNet.Init("http", "127.0.0.1", "1")
	bp.Method = http.MethodPost
	rp := &api.ReqParams{
		BaseParams: bp,
		Path:       apc.URLPathCluKalive.S,
		Body:       cos.MustMarshal(joinTestBody{SI: claimant}),
		Header:     http.Header{apc.HdrNodeVersion: []string{cmn.VersionAIStore}},
	}
	for range 3 {
		err := rp.DoRequest()
		herr := cmn.AsErrHTTP(err)
		tassert.Fatalf(t, herr != nil && herr.Status == http.StatusConflict && herr.TypeCode == "ErrKaliveRejected",
			"expected explicit keepalive rejection, got %v", err)
		current := tools.GetClusterMap(t, proxyURL).GetNode(node.ID())
		tassert.Fatalf(t, current != nil && current.EqNetID(node) && current.SameIncarnation(node),
			"rejected keepalive changed the registered node: %v", current)
	}
}

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

// The primary must answer an unresolvable public hostname with a retriable code,
// distinct from ErrJoinRejected (which stops the joining node at once).
func TestSelfJoinUnresolvedHostname(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 1, NoNodeJoinSecret: true})

	var (
		proxyURL = tools.GetPrimaryURL()
		smap     = tools.GetClusterMap(t, proxyURL)
		bp       = tools.BaseAPIParams(proxyURL)
	)
	node := smap.Tmap.ActiveNodes()[0].Clone()
	node.PubNet = meta.NetInfo{Hostname: "unresolvable.invalid", Port: "19080", URL: "http://unresolvable.invalid:19080"}

	bp.Method = http.MethodPost
	rp := &api.ReqParams{
		BaseParams: bp,
		Path:       apc.URLPathCluAutoReg.S,
		Body:       cos.MustMarshal(joinTestBody{SI: node}),
		Header:     http.Header{apc.HdrNodeVersion: []string{cmn.VersionAIStore}},
	}
	err := rp.DoRequest()
	herr := cmn.AsErrHTTP(err)
	tassert.Fatalf(t, herr != nil && herr.Status == http.StatusBadRequest && herr.TypeCode == "ErrJoinUnresolved",
		"expected ErrJoinUnresolved, got %v", err)
}
