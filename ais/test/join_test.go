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
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
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
	"github.com/NVIDIA/aistore/xact"
)

type joinTestBody struct {
	SI    *meta.Snode        `json:"si"`
	Flags cos.NodeStateFlags `json:"flags"`
}

const invalidNodeJoinSecret = "invalid-node-join-secret"

// crash and quickly restart a target (before keepalive removes it from the cluster):
// - after a full rebalance: fast rejoin, no rebalance
// - after a bucket-scoped rebalance: regular self-join and full rebalance
func TestFastRejoin(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal, MinTargets: 2})
	if !tools.GetClusterConfig(t).Rebalance.Enabled {
		t.Skip("requires rebalance enabled")
	}
	var (
		m = &ioContext{
			t:         t,
			num:       500,
			fileSize:  cos.KiB,
			fixedSize: true,
			silent:    true,
		}
		bp = tools.BaseAPIParams(tools.GetPrimaryURL())
	)
	// wait out rebalances left by earlier tests
	xargs := xact.ArgsMsg{Kind: apc.ActRebalance, OnlyRunning: true, Timeout: tools.RebalanceTimeout}
	_, err := api.WaitForSnaps(bp, &xargs, xargs.NotRunning())
	tassert.CheckFatal(t, err)

	m.initAndSaveState(true /*cleanup*/)
	tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)
	m.puts()

	tsi, err := m.smap.GetRandTarget()
	tassert.CheckFatal(t, err)

	for _, scoped := range []bool{false, true} {
		// 1. the latest rebalance: full or bucket-scoped
		args := xact.ArgsMsg{Kind: apc.ActRebalance}
		if scoped {
			args.Bck = m.bck
		}
		xid, err := api.StartXaction(bp, &args, "")
		tassert.CheckFatal(t, err)
		tools.WaitForRebalanceByID(t, bp, xid)
		tlog.Logfln("scoped=%t: %s completed", scoped, xid)

		// 2. crash and restart
		smap := tools.GetClusterMap(t, bp.URL)
		cmd, err := tools.CrashNode(tsi)
		tassert.CheckFatal(t, err)
		started := time.Now()
		tassert.CheckFatal(t, tools.RestoreNode(cmd, false, "target"))
		smap, err = tools.WaitForClusterState(bp.URL, "target rejoined", smap.Version,
			m.originalProxyCount, m.originalTargetCount)
		tassert.CheckFatal(t, err)
		tassert.CheckFatal(t, tools.WaitNodeReady(tsi.URL(cmn.NetPublic)))
		tlog.Logfln("%s rejoined in %v", tsi.StringEx(), time.Since(started))

		next := smap.GetTarget(tsi.ID())
		tassert.Fatalf(t, next != nil && !next.SameIncarnation(tsi), "expected new incarnation of %s", tsi.StringEx())
		tsi = next

		// 3. check rebalance: none vs full
		primary := fastRejoinNsti(t, bp, smap.Primary)
		gen := xact.RebID2S(primary.RMD.Version)
		if scoped {
			tassert.Fatalf(t, xact.CompareRebIDs(gen, xid) > 0, "expected full rebalance after %s, got %s", xid, gen)
			tools.WaitForRebalanceByID(t, bp, gen)
		} else {
			tassert.Fatalf(t, gen == xid, "expected no rebalance after fast rejoin: %s => %s", xid, gen)
			snaps, err := api.QueryXactionSnaps(bp, &xact.ArgsMsg{Kind: apc.ActRebalance, DaemonID: tsi.ID()})
			if !cmn.IsStatusNotFound(err) {
				tassert.CheckFatal(t, err)
				tassert.Fatalf(t, len(snaps[tsi.ID()]) == 0, "%s: unexpected rebalance: %s", tsi.StringEx(), cos.MustMarshal(snaps))
			}
		}

		// 4. check the restarted target: RMD in sync, no "restarted" state
		nsti := fastRejoinNsti(t, bp, tsi)
		tassert.Fatalf(t, nsti.RMD.Version == primary.RMD.Version, "%s: RMD v%d, primary v%d",
			tsi.StringEx(), nsti.RMD.Version, primary.RMD.Version)
		tassert.Fatalf(t, !nsti.Flags.IsSet(cos.NodeRestarted), "%s: unexpected flags %s", tsi.StringEx(), nsti.Flags)

		// 5. all objects in place
		m.gets(nil, true /*withValidation*/)
		m.ensureNoGetErrors()
	}
}

// node state info, including metadata versions and state flags (see apc.QparamClusterInfo)
func fastRejoinNsti(t *testing.T, bp api.BaseParams, si *meta.Snode) (nsti cos.NodeStateInfo) {
	rp := api.ReqParams{BaseParams: bp, Path: apc.URLPathHealth.S, Query: url.Values{apc.QparamClusterInfo: []string{"true"}}}
	rp.BaseParams.URL, rp.BaseParams.Method = si.URL(cmn.NetPublic), http.MethodGet
	_, err := rp.DoReqAny(&nsti)
	tassert.CheckFatal(t, err)
	return nsti
}

func TestJoinBackendOnDemand(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MinProxies: 2})
	var (
		proxyURL = tools.GetPrimaryURL()
		bp       = tools.BaseAPIParams(proxyURL)
		smap     = tools.GetClusterMap(t, proxyURL)
		bck      = cmn.Bck{Name: "join-on-demand-" + cos.GenTie(), Provider: apc.AIS}
	)
	node, err := smap.GetRandProxy(true)
	tassert.CheckFatal(t, err)
	joinBP := bp
	joinBP.Method = http.MethodPost
	rp := &api.ReqParams{
		BaseParams: joinBP,
		Path:       apc.URLPathCluAutoReg.S,
		Body:       cos.MustMarshal(joinTestBody{SI: node}),
		Header:     http.Header{apc.HdrNodeVersion: []string{cmn.VersionAIStore}},
	}
	t.Cleanup(func() {
		rp.Header.Set(apc.HdrNodeVersion, cmn.VersionAIStore)
		tassert.CheckError(t, rp.DoRequest())
	})
	tools.CreateBucket(t, proxyURL, bck, nil, true)
	join := func(t *testing.T, version string, reject bool) {
		rp.Header.Set(apc.HdrNodeVersion, version)
		err := rp.DoRequest()
		if reject {
			herr := cmn.AsErrHTTP(err)
			tassert.Fatalf(t, herr != nil && herr.Status == http.StatusConflict,
				"expected incompatible join to return HTTP 409, got %v", err)
		} else {
			tassert.CheckFatal(t, err)
		}
		current := tools.GetClusterMap(t, proxyURL).GetNode(node.ID())
		tassert.Fatalf(t, current != nil && current.EqNetID(node) && current.SameIncarnation(node),
			"join changed the registered node: %v", current)
	}
	for _, tc := range []struct {
		name     string
		version  string
		onDemand bool
		enabled  bool
		reject   bool
	}{
		{name: "5.2-without-on-demand", version: "5.2"},
		{name: "5.2-with-bucket-configured", version: "5.2", enabled: true},
		{name: "5.2-with-on-demand", version: "5.2", onDemand: true, reject: true},
		{name: "5.3-with-on-demand", version: "5.3", onDemand: true},
		{name: "5.3-rc-with-on-demand", version: "5.3.rc1", onDemand: true},
		{name: "current-version-with-on-demand", version: cmn.VersionAIStore, onDemand: true},
		{name: "5.2-after-clearing-on-demand", version: "5.2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := api.SetBucketProps(bp, bck, &cmn.BpropsToSet{RateLimit: &cmn.RateLimitConfToSet{
				Backend: &cmn.AdaptiveToSet{RateLimitBaseToSet: cmn.RateLimitBaseToSet{Enabled: apc.Ptr(tc.enabled)},
					OnDemand: apc.Ptr(tc.onDemand)},
			}})
			tassert.CheckFatal(t, err)
			join(t, tc.version, tc.reject)
		})
	}

	// cluster defaults (with no bucket on-demand left from the cases above)
	config := tools.GetClusterConfig(t)
	t.Cleanup(func() {
		tools.SetClusterConfig(t, cos.StrKVs{
			"rate_limit.backend.enabled":   strconv.FormatBool(config.RateLimit.Backend.Enabled),
			"rate_limit.backend.on_demand": strconv.FormatBool(config.RateLimit.Backend.OnDemand),
		})
	})
	for _, tc := range []struct {
		name     string
		version  string
		onDemand bool
		reject   bool
	}{
		{name: "5.2-with-cluster-on-demand", version: "5.2", onDemand: true, reject: true},
		{name: "current-version-with-cluster-on-demand", version: cmn.VersionAIStore, onDemand: true},
		{name: "5.2-after-clearing-cluster-on-demand", version: "5.2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tools.SetClusterConfig(t, cos.StrKVs{
				"rate_limit.backend.enabled":   "false",
				"rate_limit.backend.on_demand": strconv.FormatBool(tc.onDemand),
			})
			join(t, tc.version, tc.reject)
		})
	}
}

func TestJoinBackendOnDemandRefreshedBMD(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal, MinTargets: 1})
	var (
		proxyURL = tools.GetPrimaryURL()
		bp       = tools.BaseAPIParams(proxyURL)
		config   = tools.GetClusterConfig(t)
	)
	t.Cleanup(func() {
		tools.SetClusterConfig(t, cos.StrKVs{"rebalance.enabled": strconv.FormatBool(config.Rebalance.Enabled)})
	})
	for _, tc := range []struct {
		name     string
		version  string
		onDemand bool
		reject   bool
	}{
		{name: "5.2-rejected-after-on-demand-is-set", version: "5.2", onDemand: true, reject: true},
		{name: "5.2-accepted-after-unrelated-bmd-update", version: "5.2"},
		{name: "5.3-accepted-after-on-demand-is-set", version: "5.3", onDemand: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tassert.CheckFatal(t, api.EnableRebalance(bp))
			bck := cmn.Bck{Name: "join-bmd-refresh-" + cos.GenTie(), Provider: apc.AIS}
			tools.CreateBucket(t, proxyURL, bck, nil, true)
			observer := newJoinBMDTarget(t, bp, "5.3", true)
			_, _, err := api.JoinCluster(bp, observer.si, 0)
			tassert.CheckFatal(t, err)
			joiner := newJoinBMDTarget(t, bp, tc.version, false)
			joined, updated, disabled := make(chan error, 1), make(chan error, 1), make(chan error, 1)
			go joiner.join(bp, joined)
			select {
			case <-observer.entered:
			case err := <-joined:
				t.Fatalf("join completed before early GFN: %v", err)
			case <-t.Context().Done():
				t.Fatal(t.Context().Err())
			}
			go disableJoinRebalance(bp, disabled)
			tassert.CheckFatal(t, tools.WaitForCondition(func() bool {
				return !tools.GetClusterConfig(t).Rebalance.Enabled
			}, tools.WaitRetryOpts{Interval: 10 * time.Millisecond, MaxRetries: 100}))
			go updateJoinBMD(bp, bck, tc.onDemand, updated)
			tassert.CheckFatal(t, tools.WaitForCondition(func() bool {
				bmd, err := api.GetBMD(bp)
				tassert.CheckFatal(t, err)
				props, exists := bmd.Get(meta.CloneBck(&bck))
				return exists && props.RateLimit.Backend.Enabled != tc.onDemand && props.RateLimit.Backend.OnDemand == tc.onDemand
			}, tools.WaitRetryOpts{Interval: 10 * time.Millisecond, MaxRetries: 100}))
			close(observer.resume)
			tassert.CheckFatal(t, <-disabled)
			tassert.CheckFatal(t, <-updated)
			err = <-joined
			if tc.reject {
				herr := cmn.AsErrHTTP(err)
				tassert.Errorf(t, herr != nil && herr.Status == http.StatusConflict,
					"expected refreshed BMD to reject join with HTTP 409, got %v", err)
			} else {
				tassert.CheckError(t, err)
			}
			smap := tools.GetClusterMap(t, proxyURL)
			tassert.Errorf(t, (smap.GetNode(joiner.si.ID()) == nil) == tc.reject,
				"unexpected membership after BMD refresh: %s", smap.StringEx())
		})
	}
}

type joinBMDTarget struct {
	t       *testing.T
	si      *meta.Snode
	version string
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func newJoinBMDTarget(t *testing.T, bp api.BaseParams, version string, pause bool) *joinBMDTarget {
	target := &joinBMDTarget{t: t, si: &meta.Snode{}, version: version}
	if pause {
		target.entered, target.resume = make(chan struct{}), make(chan struct{})
		target.si.Flags = meta.SnodeMaint
	}
	server := httptest.NewUnstartedServer(target)
	host, port, err := net.SplitHostPort(server.Listener.Addr().String())
	tassert.CheckFatal(t, err)
	target.si.Init("join-bmd-"+cos.GenDaemonID(), apc.Target, nil)
	target.si.PubNet.Init("http", host, port)
	target.si.ControlNet, target.si.DataNet = target.si.PubNet, target.si.PubNet
	server.Start()
	t.Cleanup(server.Close)
	t.Cleanup(func() {
		if tools.GetClusterMap(t, bp.URL).GetNode(target.si.ID()) != nil {
			tassert.CheckError(t, api.RemoveNodeUnsafe(bp, target.si.ID()))
		}
	})
	return target
}

func (target *joinBMDTarget) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(apc.HdrNodeVersion, target.version)
	switch {
	case r.Method == http.MethodGet && r.URL.Path == apc.URLPathDae.S:
		_, _ = w.Write(cos.MustMarshal(target.si))
	case r.Method == http.MethodPost && r.URL.Path == apc.URLPathMetasync.S && target.entered != nil:
		target.once.Do(target.pauseGFN)
	}
}

func (target *joinBMDTarget) pauseGFN() {
	close(target.entered)
	select {
	case <-target.resume:
	case <-target.t.Context().Done():
	}
}

func (target *joinBMDTarget) join(bp api.BaseParams, result chan<- error) {
	_, _, err := api.JoinCluster(bp, target.si, 0)
	result <- err
}

// either on-demand or, as an unrelated BMD update, bucket-configured
func updateJoinBMD(bp api.BaseParams, bck cmn.Bck, onDemand bool, result chan<- error) {
	_, err := api.SetBucketProps(bp, bck, &cmn.BpropsToSet{RateLimit: &cmn.RateLimitConfToSet{
		Backend: &cmn.AdaptiveToSet{RateLimitBaseToSet: cmn.RateLimitBaseToSet{Enabled: apc.Ptr(!onDemand)},
			OnDemand: apc.Ptr(onDemand)},
	}})
	result <- err
}

func disableJoinRebalance(bp api.BaseParams, result chan<- error) {
	result <- api.DisableRebalance(bp)
}

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

func TestPodDerivedRestartBadVMD(t *testing.T) {
	testPodDerivedRestart(t, false /*missing*/)
}

func TestPodDerivedRestartMissingMountpath(t *testing.T) {
	testPodDerivedRestart(t, true /*missing*/)
}

func testPodDerivedRestart(t *testing.T, missing bool) {
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
		if missing && role == apc.Target {
			// test_fspaths would recreate a missing path before identity selection
			local.TestFSP = cmn.TestFSPConf{}
			local.FSP.Paths = cos.StrKVs{mpaths[0]: "test", mpaths[1]: "test"}
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
	var args []string
	if missing {
		tassert.CheckFatal(t, os.Rename(mpaths[1], mpaths[1]+".lost"))
		args = []string{"-start_with_lost_mountpath"}
	} else {
		tassert.CheckFatal(t, cos.Stat(bad))
		tlog.Logfln("Writing a bad VMD copy: %s", bad)
		tassert.CheckFatal(t, os.WriteFile(bad, []byte("garbage"), 0o644))
	}

	start(apc.Target, "ais-target-0", ports[2], ports[3], targetFSP, args...)
	tlog.Logfln("Waiting for %s to restart and rejoin", old.StringEx())
	err = ready(func(node *meta.Snode) bool { return node.ID() == old.ID() && !node.SameIncarnation(old) })
	tassert.Fatalf(t, err == nil, "%s did not rejoin: %v", old.StringEx(), err)
	tlog.Logfln("%s rejoined", target.StringEx())

	if missing {
		_, err := os.Stat(mpaths[1])
		tassert.Fatalf(t, cos.IsNotExist(err), "missing path was recreated: %v", err)
		vmd, err := volume.LoadVMDTest(old.ID(), mpaths[0])
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, vmd != nil, "surviving mountpath lost its VMD")
		return
	}

	after, err := volume.LoadVMDTest(old.ID(), mpaths...)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, after != nil && after.Version >= vmd.Version, "expected VMD version >= %d, got %v", vmd.Version, after)
	repaired, err := volume.LoadVMDTest(old.ID(), mpaths[1])
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, repaired != nil && repaired.Version == after.Version, "%s: expected repaired VMD v%d, got %v", bad, after.Version, repaired)
	tlog.Logfln("Bad copy replaced: %s", repaired)
}

// upgrade: pod-derived ID source enabled on a target with persisted (generated) ID
// - persisted ID takes precedence; target rejoins with the same ID
func TestPodDerivedPersistedPrecedence(t *testing.T) {
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
	t.Setenv(env.AisK8sNamespace, "ais-upgrade-test")
	t.Setenv("AIS_DAEMON_ID", "")
	t.Setenv("AIS_DAEMON_ID_SOURCE", "")

	fspRoot := t.TempDir()
	tassert.CheckFatal(t, os.Mkdir(filepath.Join(fspRoot, "mp1"), 0o755))
	start := func(role, pod string, public, control int, fsp cmn.TestFSPConf, args ...string) func() {
		t.Helper()
		tlog.Logfln("Starting %s pod %s (public port %d, control port %d, args %v)", role, pod, public, control, args)
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
		pid := tools.DeployNode(t, &meta.Snode{DaeType: role}, &cfg, local, args...)
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

	// 1. pre-upgrade: no ID source - generated and persisted
	targetFSP := cmn.TestFSPConf{Root: fspRoot, Count: 1}
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
	tassert.CheckFatal(t, ready(func(*meta.Snode) bool { return true }))
	old := target
	tlog.Logfln("%s is ready (pre-upgrade)", old.StringEx())

	stop()
	tassert.CheckFatal(t, tools.WaitForCondition(func() bool {
		return api.Health(tools.BaseAPIParams(old.PubNet.URL)) != nil
	}, startup))

	// 2. post-upgrade: same mountpaths, pod-derived ID source enabled
	stop = start(apc.Target, "ais-target-0", ports[2], ports[3], targetFSP, "-daemon_id_source=k8s-pod")
	err := ready(func(node *meta.Snode) bool { return node.ID() == old.ID() && !node.SameIncarnation(old) })
	tassert.Fatalf(t, err == nil, "%s did not rejoin with its persisted ID: %v", old.StringEx(), err)
	tlog.Logfln("%s rejoined with persisted ID", target.StringEx())

	// 3. unused derivation settings (changed scope, missing pod metadata) must not block a subsequent restart
	old = target
	stop()
	tassert.CheckFatal(t, tools.WaitForCondition(func() bool {
		return api.Health(tools.BaseAPIParams(old.PubNet.URL)) != nil
	}, startup))
	t.Setenv(env.AisK8sNamespace, "")
	start(apc.Target, "ais-target-0", ports[2], ports[3], targetFSP, "-daemon_id_source=k8s-pod:changed")
	err = ready(func(node *meta.Snode) bool { return node.ID() == old.ID() && !node.SameIncarnation(old) })
	tassert.Fatalf(t, err == nil, "%s did not ignore unused derivation settings: %v", old.StringEx(), err)
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
			testNodeJoinRequests(t, proxyURL, validSecret)
			testProxySelfJoin(t, proxyURL, secretPath, validSecret, nodeDir)
		})
	}
}

func testNodeJoinRequests(t *testing.T, proxyURL, secret string) {
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
	// Negative: unsigned keepalive that would admit an absent node is rejected.
	t.Run("unsigned keepalive unknown", func(t *testing.T) {
		body := cos.MustMarshal(joinTestBody{SI: unknown})
		err := (&api.ReqParams{BaseParams: bp, Path: apc.URLPathCluKalive.S, Body: body, Header: header}).DoRequest()
		herr := cmn.AsErrHTTP(err)
		tassert.Fatalf(t, herr != nil && herr.Status == http.StatusUnauthorized,
			"expected status %d, got %v", http.StatusUnauthorized, err)
	})

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
