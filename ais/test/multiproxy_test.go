// Package integration_test.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"fmt"
	"math"
	"net/http"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/ais"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/reb"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/docker"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact"

	jsoniter "github.com/json-iterator/go"
)

const (
	localBucketDir  = "multipleproxy"
	defaultChanSize = 10
)

var (
	voteTests = []Test{
		{"PrimaryCrash", primaryCrashElectRestart},
		{"NodeCrashRestoreDifferentIP", nodeCrashRestoreDifferentIP},
		{"ProxyCrash", proxyCrash},
		{"PrimaryAndTargetCrash", primaryAndTargetCrash},
		{"PrimaryAndProxyCrash", primaryAndProxyCrash},
		{"CrashAndFastRestore", crashAndFastRestore},
		{"TargetRejoin", targetRejoin},
		{"JoinWhileVoteInProgress", joinWhileVoteInProgress},
		{"ConcurrentPutGetDel", concurrentPutGetDel},
		{"ProxyStress", proxyStress},
		{"PrimaryAndNextCrash", primaryAndNextCrash},
		{"DiscoveryAndOriginalPrimaryCrash", discoveryAndOrigPrimaryProxiesCrash},
		{"AddNodeDuplicateIP", addNodeDuplicateIP},
		{"AddNodeDuplicateDaemonID", addNodeDuplicateDaemonID},
	}

	icTests = []Test{
		{"ICMemberLeaveAndRejoin", icMemberLeaveAndRejoin},
		{"ICKillAndRestorePrimary", icKillAndRestorePrimary},
		{"ICSyncOwnTbl", icSyncOwnershipTable},
		{"ICSinglePrimaryRevamp", icSinglePrimaryRevamp},
		{"ICStressMonitorXactMultiICFail", icStressMonitorXactMultiICFail},
	}
)

func TestMultiProxy(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{
		Long:               true,
		RequiredDeployment: tools.ClusterTypeLocal,
		MinProxies:         3,
		MinTargets:         1,
	})

	defer tools.EnsureOrigClusterState(t)
	for _, test := range voteTests {
		t.Run(test.name, test.method)
		if t.Failed() {
			t.FailNow()
		}
	}
}

// primaryCrashElectRestart kills the current primary proxy, wait for the new primary proxy is up and verifies it,
// restores the original primary proxy as non primary
func primaryCrashElectRestart(t *testing.T) {
	proxyURL := tools.RandomProxyURL(t)
	killRestorePrimary(t, proxyURL, false, nil)
}

func killRestorePrimary(t *testing.T, proxyURL string, restoreAsPrimary bool,
	postKill func(smap *meta.Smap, newPrimary, oldPrimary *meta.Snode)) *meta.Smap {
	var (
		smap          = tools.GetClusterMap(t, proxyURL)
		proxyCount    = smap.CountActivePs()
		oldPrimary    = smap.Primary
		oldPrimaryURL = smap.Primary.URL(cmn.NetPublic)
		oldPrimaryID  = smap.Primary.ID()
	)

	tlog.Logfln("targets: %d, proxies: %d", smap.CountActiveTs(), proxyCount)
	newPrimaryID, newPrimaryURL, err := chooseNextProxy(smap)
	tassert.CheckFatal(t, err)
	newPrimary := smap.GetProxy(newPrimaryID)

	tlog.Logfln("New primary: %s --> %s", newPrimaryID, newPrimaryURL)
	tlog.Logfln("Killing primary: %s --> %s", oldPrimaryURL, oldPrimaryID)

	// cmd and args are the original command line of how the proxy is started
	cmd, err := tools.KillNode(baseParams, smap.Primary)
	tassert.CheckFatal(t, err)

	smap, err = tools.WaitForClusterState(newPrimaryURL, "new primary elected", smap.Version,
		smap.CountActivePs()-1, smap.CountActiveTs())
	tassert.CheckFatal(t, err)
	tlog.Logfln("New primary elected: %s", newPrimaryID)

	tassert.Errorf(t, smap.Primary.ID() == newPrimaryID, "Wrong primary proxy: %s, expecting: %s", smap.Primary.ID(), newPrimaryID)

	if postKill != nil {
		postKill(smap, newPrimary, oldPrimary)
	}

	// re-construct the command line to start the original proxy but add the current primary proxy to the args
	err = tools.RestoreNode(cmd, false, "proxy (prev primary)")
	tassert.CheckFatal(t, err)

	smap, err = tools.WaitForClusterState(newPrimaryURL, "restore", smap.Version, proxyCount, 0)
	tassert.CheckFatal(t, err)
	if _, ok := smap.Pmap[oldPrimaryID]; !ok {
		t.Fatal("Previous primary proxy did not rejoin the cluster")
	}
	checkSmaps(t, newPrimaryURL)

	if restoreAsPrimary {
		return setPrimaryTo(t, oldPrimaryURL, smap, "", oldPrimaryID)
	}
	return smap
}

func nodeCrashRestoreDifferentIP(t *testing.T) {
	for _, ty := range []string{apc.Proxy, apc.Target} {
		t.Run(ty, func(t *testing.T) {
			killRestoreDiffIP(t, ty)
		})
	}
}

func killRestoreDiffIP(t *testing.T, nodeType string) {
	// NOTE: This function requires local deployment as it changes node config
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal})

	var (
		proxyURL                      = tools.GetPrimaryURL()
		smap                          = tools.GetClusterMap(t, proxyURL)
		origProxyCnt, origTargetCount = smap.CountActivePs(), smap.CountActiveTs()
		portInc                       = 100
		node                          *meta.Snode
		err                           error
		pdc, tdc                      int
		restore                       bool
	)

	if nodeType == apc.Proxy {
		node, err = smap.GetRandProxy(true /*exclude primary*/)
		pdc = 1
	} else {
		node, err = smap.GetRandTarget()
		tdc = 1
	}
	tassert.CheckFatal(t, err)

killRestore:
	cfg := tools.GetDaemonConfig(t, node)
	tlog.Logfln("Killing %s", node.StringEx())
	cmd, err := tools.KillNode(baseParams, node)
	tassert.CheckFatal(t, err)

	smap, err = tools.WaitForClusterState(proxyURL, "cluster to stabilize", smap.Version, origProxyCnt-pdc, origTargetCount-tdc)
	tassert.CheckFatal(t, err)

	// Update local config ports.
	localConfPath := filepath.Join(cfg.ConfigDir, fname.PlainLocalConfig)
	localConf := &cmn.LocalConfig{}
	_, err = jsp.LoadMeta(localConfPath, localConf)
	tassert.CheckFatal(t, err)
	localConf.HostNet.Port = cfg.HostNet.Port + portInc
	localConf.HostNet.PortIntraControl = cfg.HostNet.PortIntraControl + portInc
	localConf.HostNet.PortIntraData = cfg.HostNet.PortIntraData + portInc
	err = jsp.SaveMeta(localConfPath, localConf, nil)
	tassert.CheckFatal(t, err)

	err = tools.RestoreNode(cmd, false, nodeType)
	tassert.CheckFatal(t, err)

	smap, err = tools.WaitForClusterState(proxyURL, "restore with changed config", smap.Version, origProxyCnt, 0)
	tassert.CheckFatal(t, err)

	// Health check with old public URL should fail
	err = api.Health(tools.BaseAPIParams(node.URL(cmn.NetPublic)))
	tassert.Errorf(t, err != nil, "health check with old IP information should fail %v", err)

	newNode := smap.GetNode(node.ID())
	err = tools.WaitNodeReady(newNode.URL(cmn.NetPublic))
	tassert.CheckError(t, err)
	if !restore {
		// Revert port changes
		restore = true
		node = newNode
		portInc = -portInc
		goto killRestore
	}

	if nodeType == apc.Target {
		tools.WaitForRebalAndResil(t, tools.BaseAPIParams(proxyURL))
	}

	_, err = tools.WaitForClusterState(proxyURL, "wait for original node counts", 0, origProxyCnt, origTargetCount)
	tassert.CheckFatal(t, err)
}

// primaryAndTargetCrash kills the primary p[roxy and one random target, verifies the next in
// line proxy becomes the new primary, restore the target and proxy, restore original primary.
func primaryAndTargetCrash(t *testing.T) {
	if docker.IsRunning() {
		t.Skip("Skipped because setting new primary URL in command line for docker is not supported")
	}

	proxyURL := tools.RandomProxyURL(t)
	smap := tools.GetClusterMap(t, proxyURL)
	tlog.Logfln("targets: %d, proxies: %d", smap.CountActiveTs(), smap.CountActivePs())

	newPrimaryID, newPrimaryURL, err := chooseNextProxy(smap)
	tassert.CheckFatal(t, err)

	oldPrimaryURL := smap.Primary.URL(cmn.NetPublic)
	tlog.Logfln("Killing proxy %s - %s", oldPrimaryURL, smap.Primary.ID())
	cmd, err := tools.KillNode(baseParams, smap.Primary)
	tassert.CheckFatal(t, err)

	// Select a random target
	var (
		targetURL       string
		targetID        string
		targetNode      *meta.Snode
		origTargetCount = smap.CountActiveTs()
		origProxyCount  = smap.CountActivePs()
	)

	targetNode, _ = smap.GetRandTarget()
	targetURL = targetNode.URL(cmn.NetPublic)
	targetID = targetNode.ID()

	tlog.Logfln("Killing target: %s - %s", targetURL, targetID)
	tcmd, err := tools.KillNode(baseParams, targetNode)
	tassert.CheckFatal(t, err)

	smap, err = tools.WaitForClusterState(newPrimaryURL, "new primary elected",
		smap.Version, origProxyCount-1, origTargetCount-1)
	tassert.CheckFatal(t, err)

	if smap.Primary.ID() != newPrimaryID {
		t.Fatalf("Wrong primary proxy: %s, expecting: %s", smap.Primary.ID(), newPrimaryID)
	}

	err = tools.RestoreNode(tcmd, false, "target")
	tassert.CheckFatal(t, err)

	err = tools.RestoreNode(cmd, false, "proxy (prev primary)")
	tassert.CheckFatal(t, err)

	_, err = tools.WaitForClusterState(newPrimaryURL, "restore proxy and target",
		smap.Version, origProxyCount, origTargetCount)
	tassert.CheckFatal(t, err)
	tools.WaitForRebalAndResil(t, tools.BaseAPIParams(newPrimaryURL))
}

// A very simple test to check if a primary proxy can detect non-primary one
// dies and then update and sync SMap
func proxyCrash(t *testing.T) {
	proxyURL := tools.RandomProxyURL(t)
	smap := tools.GetClusterMap(t, proxyURL)
	origProxyCount := smap.CountActivePs()
	origTargetCount := smap.CountActiveTs()
	tlog.Logfln("targets: %d, proxies: %d", origTargetCount, origProxyCount)

	primaryURL := smap.Primary.URL(cmn.NetPublic)
	tlog.Logfln("Primary: %s", smap.Primary.StringEx())

	secondNode, err := smap.GetRandProxy(true /*exclude primary*/)
	tassert.CheckFatal(t, err)

	tlog.Logfln("Killing non-primary %s", secondNode.StringEx())
	secondCmd, err := tools.KillNode(baseParams, secondNode)
	tassert.CheckFatal(t, err)

	smap, err = tools.WaitForClusterState(primaryURL, "proxy removed", smap.Version, origProxyCount-1, 0)
	tassert.CheckFatal(t, err)

	err = tools.RestoreNode(secondCmd, false, "proxy")
	tassert.CheckFatal(t, err)

	smap, err = tools.WaitForClusterState(primaryURL, "proxy restoreid", smap.Version, origProxyCount, origTargetCount)
	tassert.CheckFatal(t, err)

	if _, ok := smap.Pmap[secondNode.ID()]; !ok {
		t.Fatal("Non-primary proxy did not rejoin the cluster.")
	}
}

func addNodeDuplicateDaemonID(t *testing.T) {
	for _, ty := range []string{apc.Proxy, apc.Target} {
		t.Run(ty, func(t *testing.T) {
			_addNodeDuplicateDaemonID(t, ty)
		})
	}
}

// 1. Select a random proxy/target node based on `nodeType` param
// 2. Try deploying a new node using the same DaemonID as randomly chosen node
// 3. Wait for the newly deployed daemon to be terminated - failing to join cluster
// NOTE: Test assumes that the randomly chosen node is healthy (i.e. doesn't terminate or restart)
// TODO: add test for target that tries to join with duplicate DaemonID and contains user-data
func _addNodeDuplicateDaemonID(t *testing.T, nodeType string) {
	// NOTE: This function requires local deployment as it changes node config
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal})

	var (
		proxyURL = tools.GetPrimaryURL()
		smap     = tools.GetClusterMap(t, proxyURL)
		node     *meta.Snode
		err      error

		// node configs
		portInc = 100
	)

	if nodeType == apc.Proxy {
		node, err = smap.GetRandProxy(true)
	} else {
		node, err = smap.GetRandTarget()
	}
	tassert.CheckFatal(t, err)
	conf := tools.GetDaemonConfig(t, node)

	// Create local config for daemon.
	localConf := &cmn.LocalConfig{}
	localConf.ConfigDir = conf.ConfigDir
	localConf.HostNet.Port = conf.HostNet.Port + portInc
	localConf.HostNet.PortIntraControl = conf.HostNet.PortIntraControl + portInc
	localConf.HostNet.PortIntraData = conf.HostNet.PortIntraData + portInc

	// start with different config but same daemon ID
	pid := tools.DeployNode(t, node, conf, localConf)
	t.Cleanup(func() {
		tools.CleanupNode(t, pid)
	})

	err = tools.WaitForPID(pid)
	tassert.CheckFatal(t, err)
}

func addNodeDuplicateIP(t *testing.T) {
	for _, ty := range []string{apc.Proxy, apc.Target} {
		t.Run(ty, func(t *testing.T) {
			_addNodeDuplicateIP(t, ty)
		})
	}
}

// 1. Select a random proxy/target node based on `nodeType` param
// 2. Try deploying a new node using the same IP configuration as randomly chosen node
// 3. Wait for the newly deployed daemon to be terminated - failing to join cluster
// NOTE: Test assumes that the randomly chosen node is healthy (i.e. doesn't terminate or restart)
func _addNodeDuplicateIP(t *testing.T, nodeType string) {
	// NOTE: This function requires local deployment as it changes node config
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal})

	var (
		primaryURL = tools.GetPrimaryURL()
		smap       = tools.GetClusterMap(t, primaryURL)
		node       *meta.Snode
		err        error
	)

	if nodeType == apc.Proxy {
		node, err = smap.GetRandProxy(true)
	} else {
		node, err = smap.GetRandTarget()
	}
	tassert.CheckFatal(t, err)
	conf := tools.GetDaemonConfig(t, node)

	// Make sure that the `DaemonID` is different.
	node.DaeID = "testing_" + trand.String(10)

	pid := tools.DeployNode(t, node, conf, nil)
	t.Cleanup(func() {
		tools.CleanupNode(t, pid)
	})

	err = tools.WaitForPID(pid)
	tassert.CheckFatal(t, err)
}

// primaryAndProxyCrash kills primary proxy and one another proxy(not the next in line primary)
// and restore them afterwards
func primaryAndProxyCrash(t *testing.T) {
	var (
		randProxyURL                = tools.RandomProxyURL(t)
		smap                        = tools.GetClusterMap(t, randProxyURL)
		origProxyCount              = smap.CountActivePs()
		origTargetCount             = smap.CountActiveTs()
		oldPrimaryURL, oldPrimaryID = smap.Primary.URL(cmn.NetPublic), smap.Primary.ID()
		secondNode                  *meta.Snode
		secondID                    string
	)
	tlog.Logfln("targets: %d, proxies: %d", origTargetCount, origProxyCount)

	newPrimaryID, newPrimaryURL, err := chooseNextProxy(smap)
	tassert.CheckFatal(t, err)

	tlog.Logfln("Killing primary: %s - %s", oldPrimaryURL, oldPrimaryID)
	cmd, err := tools.KillNode(baseParams, smap.Primary)
	tassert.CheckFatal(t, err)

	// Do not choose the next primary in line, or the current primary proxy
	// This is because the system currently cannot recover if the next proxy in line is
	// also killed (TODO)
	for k, v := range smap.Pmap {
		if k != newPrimaryID && k != oldPrimaryID {
			secondNode = v
			secondID = secondNode.ID()
			break
		}
	}
	tassert.Errorf(t, secondID != "", "not enough proxies (%d)", origProxyCount)
	n := cos.NowRand().IntN(20)
	time.Sleep(time.Duration(n+1) * time.Second)

	tlog.Logfln("Killing non-primary: %s", secondNode.StringEx())
	secondCmd, err := tools.KillNode(baseParams, secondNode)
	tassert.CheckFatal(t, err)

	smap, err = tools.WaitForClusterState(newPrimaryURL, "elect new primary",
		smap.Version, origProxyCount-2, 0)
	tassert.CheckFatal(t, err)

	err = tools.RestoreNode(cmd, true, "previous primary "+oldPrimaryID)
	tassert.CheckFatal(t, err)

	smap, err = tools.WaitForClusterState(newPrimaryURL, "join back previous primary "+oldPrimaryID,
		smap.Version, origProxyCount-1, 0)
	tassert.CheckFatal(t, err)

	err = tools.RestoreNode(secondCmd, false, "proxy")
	tassert.CheckFatal(t, err)

	smap, err = tools.WaitForClusterState(newPrimaryURL, "join back non-primary "+secondID,
		smap.Version, origProxyCount, origTargetCount)
	tassert.CheckFatal(t, err)

	if smap.Primary.ID() != newPrimaryID {
		t.Fatalf("Wrong primary proxy: %s, expecting: %s", smap.Primary.ID(), newPrimaryID)
	}

	if _, ok := smap.Pmap[oldPrimaryID]; !ok {
		t.Fatalf("Previous primary proxy %s did not rejoin the cluster", oldPrimaryID)
	}

	if _, ok := smap.Pmap[secondID]; !ok {
		t.Fatalf("Second proxy %s did not rejoin the cluster", secondID)
	}
}

// targetRejoin kills a random selected target, wait for it to rejoin and verifies it
func targetRejoin(t *testing.T) {
	var (
		id              string
		randProxyURL    = tools.RandomProxyURL(t)
		smap            = tools.GetClusterMap(t, randProxyURL)
		origProxyCount  = smap.CountActivePs()
		origTargetCount = smap.CountActiveTs()
	)
	tlog.Logfln("targets: %d, proxies: %d", origTargetCount, origProxyCount)

	node, err := smap.GetRandTarget()
	if err != nil {
		tlog.Logfln("Warning: %v", err)
		tlog.Logln("Waiting for a while and retrying...")
		// retry once
		time.Sleep(25 * time.Second)
		randProxyURL = tools.RandomProxyURL(t)
		smap = tools.GetClusterMap(t, randProxyURL)
		node, err = smap.GetRandTarget()
		tassert.CheckFatal(t, err)
		origProxyCount = smap.CountActivePs()
		origTargetCount = smap.CountActiveTs()
	}
	id = node.ID()

	cmd, err := tools.KillNode(baseParams, node)
	tassert.CheckFatal(t, err)
	smap, err = tools.WaitForClusterState(randProxyURL, "target crashed", smap.Version, origProxyCount, origTargetCount-1)
	tassert.CheckFatal(t, err)

	if _, ok := smap.Tmap[id]; ok {
		t.Fatalf("Killed target was not removed from the Smap: %v", id)
	}

	err = tools.RestoreNode(cmd, false, "target")
	tassert.CheckFatal(t, err)

	smap, err = tools.WaitForClusterState(randProxyURL, "target rejoined", smap.Version, origProxyCount, origTargetCount)
	tassert.CheckFatal(t, err)

	if _, ok := smap.Tmap[id]; !ok {
		t.Fatalf("Restarted target %s did not rejoin the cluster", id)
	}
	tools.WaitForRebalAndResil(t, tools.BaseAPIParams(randProxyURL))
}

// crashAndFastRestore kills the primary and restores it before a new leader is elected
func crashAndFastRestore(t *testing.T) {
	var (
		randProxyURL    = tools.RandomProxyURL(t)
		smap            = tools.GetClusterMap(t, randProxyURL)
		origProxyCount  = smap.CountActivePs()
		origTargetCount = smap.CountActiveTs()
	)

	tlog.Logfln("targets: %d, proxies: %d", origTargetCount, origProxyCount)

	// Make sure proxyURL is not primary URL.
	_, randProxyURL, err := chooseNextProxy(smap)
	tassert.CheckFatal(t, err)
	oldPrimaryID := smap.Primary.ID()
	tlog.Logfln("The current primary %s, Smap version %d", oldPrimaryID, smap.Version)

	cmd, err := tools.KillNode(baseParams, smap.Primary)
	tassert.CheckFatal(t, err)

	// quick crash and recover
	time.Sleep(2 * time.Second)
	err = tools.RestoreNode(cmd, true, "proxy (primary)")
	tassert.CheckFatal(t, err)

	tlog.Logfln("The %s is currently restarting", oldPrimaryID)

	// NOTE:
	// using (version - 1) because the primary will restart with its old version,
	// there will be no version change for this restore, so force beginning version to 1 less
	// than the original version in order to use WaitForClusterState.
	time.Sleep(5 * time.Second)
	smap, err = tools.WaitForClusterState(randProxyURL, "restore", smap.Version-1, origProxyCount, origTargetCount)
	tassert.CheckFatal(t, err)

	if smap.Primary.ID() != oldPrimaryID {
		t.Fatalf("Wrong primary proxy: %s, expecting: %s", smap.Primary.ID(), oldPrimaryID)
	}
}

func joinWhileVoteInProgress(t *testing.T) {
	if docker.IsRunning() {
		t.Skipf("skipping %s (docker is not supported)", t.Name())
	}
	var (
		smap         = tools.GetClusterMap(t, proxyURL)
		oldTargetCnt = smap.CountActiveTs()
		oldProxyCnt  = smap.CountActivePs()
		stopch       = make(chan struct{})
		errCh        = make(chan error, 10)
		mocktgt      = &voteRetryMockTarget{
			voteInProgress: true,
			errCh:          errCh,
		}
	)
	tlog.Logfln("targets: %d, proxies: %d", oldTargetCnt, oldProxyCnt)

	proxy, err := smap.GetRandProxy(true /*exclude primary*/)
	tassert.CheckFatal(t, err)
	randProxyURL := proxy.URL(cmn.NetPublic)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go runMockTarget(t, randProxyURL, mocktgt, stopch, smap, wg)

	_, err = tools.WaitForClusterState(randProxyURL, "mock target joined", smap.Version, oldProxyCnt, oldTargetCnt+1)
	tassert.CheckFatal(t, err)

	smap = killRestorePrimary(t, randProxyURL, false, nil)
	//
	// FIXME: election is in progress if and only when xaction(apc.ActElection) is running -
	//        simulating the scenario via mocktgt.voteInProgress = true is incorrect
	//
	// if _, ok := smap.Pmap[oldPrimaryID]; ok {
	//	t.Fatalf("Previous primary proxy rejoined the cluster during a vote")
	// }
	mocktgt.voteInProgress = false
	// smap, err = tools.WaitForClusterState(newPrimaryURL, "synchronize new Smap",
	// smap.Version, testing.Verbose(), oldProxyCnt, oldTargetCnt+1)
	// tassert.CheckFatal(t, err)
	//
	// end of FIXME

	// time to kill the mock target, job well done
	var v struct{}
	stopch <- v
	close(stopch)
	select {
	case err := <-errCh:
		t.Errorf("Mock Target Error: %v", err)
	default:
	}

	wg.Wait()

	_, err = tools.WaitForClusterState(smap.Primary.URL(cmn.NetPublic),
		"cluster to stabilize", smap.Version, oldProxyCnt, oldTargetCnt)
	tassert.CheckFatal(t, err)
}

// concurrentPutGetDel does put/get/del sequence against all proxies concurrently
func concurrentPutGetDel(t *testing.T) {
	_ = tools.RandomProxyURL(t)
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		randProxyURL := tools.RandomProxyURL(t)
		smap := tools.GetClusterMap(t, randProxyURL)
		tlog.Logfln("targets: %d, proxies: %d", smap.CountActiveTs(), smap.CountActivePs())

		var (
			wg        = &sync.WaitGroup{}
			errCh     = make(chan error, smap.CountActivePs())
			cksumType = bck.Props.Cksum.Type
		)

		// cid = a goroutine ID to make filenames unique
		// otherwise it is easy to run into a trouble when 2 goroutines do:
		//   1PUT 2PUT 1DEL 2DEL
		// And the second goroutine fails with error "object does not exist"
		for _, v := range smap.Pmap {
			wg.Add(1)
			go func(url string) {
				defer wg.Done()
				errCh <- proxyPutGetDelete(100, url, bck.Clone(), cksumType)
			}(v.URL(cmn.NetPublic))
		}

		wg.Wait()
		close(errCh)

		for err := range errCh {
			tassert.CheckFatal(t, err)
		}
	})
}

// proxyPutGetDelete repeats put/get/del N times, all requests go to the same proxy
func proxyPutGetDelete(count int, proxyURL string, bck cmn.Bck, cksumType string) error {
	baseParams := tools.BaseAPIParams(proxyURL)
	for range count {
		reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: fileSize, CksumType: cksumType})
		if err != nil {
			return fmt.Errorf("error creating reader: %v", err)
		}
		fname := trand.String(20)
		keyname := fmt.Sprintf("%s/%s", localBucketDir, fname)
		putArgs := api.PutArgs{
			BaseParams: baseParams,
			Bck:        bck,
			ObjName:    keyname,
			Cksum:      reader.Cksum(),
			Reader:     reader,
		}
		if _, err = api.PutObject(&putArgs); err != nil {
			return fmt.Errorf("error executing put: %v", err)
		}
		if _, err = api.GetObject(baseParams, bck, keyname, nil); err != nil {
			return fmt.Errorf("error executing get: %v", err)
		}
		if err = tools.Del(proxyURL, bck, keyname, nil /* wg */, nil /* errCh */, true /* silent */); err != nil {
			return fmt.Errorf("error executing del: %v", err)
		}
	}

	return nil
}

// run PUT/GET/DELETE and retry pending deletes once per primary notification
// - request errors are expected while proxies are down
// - bucket cleanup removes any objects left when workers stop
func putGetDelWorker(proxyURL string, stopCh <-chan struct{}, proxyURLCh <-chan string, wg *sync.WaitGroup) {
	defer wg.Done()

	missedDeletes := make([]string, 0, 100)
	baseParams := tools.BaseAPIParams(proxyURL)

	bck := cmn.Bck{
		Name:     testBucketName,
		Provider: apc.AIS,
	}
	cksumType := bck.DefaultProps(initialClusterConfig).Cksum.Type
	for {
		select {
		case <-stopCh:
			return

		case url := <-proxyURLCh:
			proxyURL = url
			baseParams = tools.BaseAPIParams(url)
			n := 0
			for _, objName := range missedDeletes {
				select {
				case <-stopCh:
					return
				default:
				}
				if err := tools.Del(url, bck, objName, nil, nil, true); err != nil {
					missedDeletes[n] = objName
					n++
				}
			}
			clear(missedDeletes[n:])
			missedDeletes = missedDeletes[:n]

		default:
		}

		reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: fileSize, CksumType: cksumType})
		if err != nil {
			continue
		}

		fname := trand.String(20)
		objName := fmt.Sprintf("%s/%s", localBucketDir, fname)
		putArgs := api.PutArgs{
			BaseParams: baseParams,
			Bck:        bck,
			ObjName:    objName,
			Cksum:      reader.Cksum(),
			Reader:     reader,
		}
		_, err = api.PutObject(&putArgs)
		if err != nil {
			continue
		}
		_, _ = api.GetObject(baseParams, bck, objName, nil)

		err = tools.Del(proxyURL, bck, objName, nil, nil, true)
		if err != nil {
			missedDeletes = append(missedDeletes, objName)
		}
	}
}

// primaryKiller kills primary proxy, notifies all workers, and restores it.
func primaryKiller(t *testing.T, proxyURL string, stopch <-chan struct{}, proxyurlchs []chan string,
	wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-stopch:
			return

		default:
		}

		postKill := func(_ *meta.Smap, newPrimary, _ *meta.Snode) {
			// let the workers go to the dying primary for a little while longer to generate errored requests
			time.Sleep(time.Second)
			for _, ch := range proxyurlchs {
				select {
				case ch <- newPrimary.URL(cmn.NetPublic):
				case <-stopch:
					return // killRestorePrimary still restores the killed proxy
				}
			}
		}
		killRestorePrimary(t, proxyURL, false, postKill)
	}
}

// Tests if a node is able to restart when discovery and original primary provided in config are not available
// 1. Set primary as original primary from config
// 2. Kill discovery node provided in config, a random proxy and target
// 3. Try restoring the killed nodes one at a time
func discoveryAndOrigPrimaryProxiesCrash(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MinProxies: 4})
	var (
		config       = tools.GetClusterConfig(t)
		restoreCmd   = make([]tools.RestoreCmd, 0, 3)
		proxyURL     string
		pcnt, tcnt   int
		randomKilled bool
	)

	// Make sure primary is same config
	smap := primarySetToRand(t)
	origProxyCnt := smap.CountActivePs()
	origTargetCnt := smap.CountActiveTs()

	for _, si := range smap.Pmap {
		if smap.IsPrimary(si) {
			continue
		}
		if si.HasURL(config.Proxy.DiscoveryURL) {
			pcnt++
			tlog.Logfln("Kill #%d: %s", pcnt+tcnt, si.StringEx())
			cmd, err := tools.KillNode(baseParams, si)
			tassert.CheckFatal(t, err)
			restoreCmd = append(restoreCmd, cmd)
			continue
		}
		if randomKilled {
			// Set proxyURL - used to get latest smap
			proxyURL = si.URL(cmn.NetPublic)
			continue
		}

		// Kill a random non primary proxy
		pcnt++
		tlog.Logfln("Kill #%d: %s", pcnt+tcnt, si.StringEx())
		cmd, err := tools.KillNode(baseParams, si)
		tassert.CheckFatal(t, err)
		restoreCmd = append(restoreCmd, cmd)
		randomKilled = true
	}

	// Kill a random target
	target, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)
	tcnt++
	tlog.Logfln("Kill #%d: %s", pcnt+tcnt, target.StringEx())
	cmd, err := tools.KillNode(baseParams, target)
	tassert.CheckFatal(t, err)
	restoreCmd = append(restoreCmd, cmd)

	// Kill original primary
	pcnt++
	tlog.Logfln("Kill #%d: %s", pcnt+tcnt, smap.Primary.StringEx())
	cmd, err = tools.KillNode(baseParams, smap.Primary)
	tassert.CheckFatal(t, err)
	restoreCmd = append(restoreCmd, cmd)

	proxyCnt, targetCnt := origProxyCnt-pcnt, origTargetCnt-tcnt
	smap, err = tools.WaitForClusterState(proxyURL, "kill proxies and target", smap.Version, proxyCnt, targetCnt)
	tassert.CheckFatal(t, err)

	// Restore all killed nodes
	for _, cmd := range restoreCmd {
		if cmd.Node.IsProxy() {
			proxyCnt++
		}
		if cmd.Node.IsTarget() {
			targetCnt++
		}
		tools.RestoreNode(cmd, false, cmd.Node.Type())
		_, err = tools.WaitForClusterState(proxyURL, "restore "+cmd.Node.ID(), smap.Version,
			proxyCnt, targetCnt)
		tassert.CheckError(t, err)
	}

	tools.WaitForRebalAndResil(t, tools.BaseAPIParams(proxyURL))
}

// proxyStress starts a group of workers doing put/get/del in sequence against primary proxy,
// while the operations are on going, a separate go routine kills the primary proxy, notifies all
// workers about the proxy change, restart the killed proxy as a non-primary proxy.
// the process is repeated until a pre-defined time duration is reached.
func proxyStress(t *testing.T) {
	var (
		wg          sync.WaitGroup
		stopCh      = make(chan struct{})
		proxyURLChs = make([]chan string, workerCnt)
		bck         = cmn.Bck{
			Name:     testBucketName,
			Provider: apc.AIS,
		}
		proxyURL = tools.RandomProxyURL(t)
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	defer func() {
		err := tools.WaitNodeReady(proxyURL)
		tassert.CheckFatal(t, err)
	}()

	// start all workers
	rnd := cos.NowRand()
	for i := range workerCnt {
		proxyURLChs[i] = make(chan string, defaultChanSize)

		wg.Add(1)
		go putGetDelWorker(proxyURL, stopCh, proxyURLChs[i], &wg)

		// randomize the work
		n := rnd.IntN(999)
		time.Sleep(time.Duration(n+1) * time.Millisecond)
	}

	wg.Add(1)
	go primaryKiller(t, proxyURL, stopCh, proxyURLChs, &wg)

	time.Sleep(tools.MultiProxyTestTimeout)
	close(stopCh)
	wg.Wait()
}

// smap 	- current Smap
// directURL	- URL of the proxy that we send the request to (not necessarily the current primary)
// toID 	- DaemonID and URL of the proxy that must become the new primary
func setPrimaryTo(t *testing.T, proxyURL string, smap *meta.Smap, directURL, toID string) (newSmap *meta.Smap) {
	if directURL == "" {
		directURL = smap.Primary.URL(cmn.NetPublic)
	}

	baseParams := tools.BaseAPIParams(directURL)
	tlog.Logfln("Setting primary from %s to %s", smap.Primary.ID(), toID)
	err := api.SetPrimary(baseParams, toID, "" /*toURL*/, false /*force*/)
	tassert.CheckFatal(t, err)

	newSmap, err = tools.WaitForNewSmap(proxyURL, smap.Version)
	tassert.CheckFatal(t, err)
	if newSmap.Primary.ID() != toID {
		t.Fatalf("Expected primary=%s, got %s", toID, newSmap.Primary.ID())
	}
	checkSmaps(t, newSmap.Primary.URL(cmn.NetPublic))
	return
}

func chooseNextProxy(smap *meta.Smap) (string, string, error) {
	pi, err := smap.HrwProxy(smap.Primary.ID())
	if err != nil {
		return "", "", err
	}
	return pi.ID(), pi.URL(cmn.NetPublic), nil
}

// For each proxy: compare its Smap vs primary(*) and return an error if differs
func checkSmaps(t *testing.T, proxyURL string) {
	var (
		smap1      = tools.GetClusterMap(t, proxyURL)
		primary    = smap1.Primary // primary according to the `proxyURL`(*)
		smapDiffer bool
	)
	for _, psi := range smap1.Pmap {
		smap2 := tools.GetClusterMap(t, psi.URL(cmn.NetPublic))
		uuid, sameOrigin, sameVersion, eq := smap1.Compare(smap2)
		if sameOrigin && sameVersion && eq {
			continue
		}
		err := fmt.Errorf("(%s %s, primary=%s) != (%s %s, primary=%s): (uuid=%s, same-orig=%t, same-ver=%t)",
			proxyURL, smap1, primary, psi.URL(cmn.NetPublic), smap2, smap2.Primary, uuid, sameOrigin, sameVersion)
		t.Error(err)
		smapDiffer = true
	}
	if !smapDiffer {
		tlog.Logln("all Smap copies are identical: " + smap1.StringEx())
	}
}

func primarySetToRand(t *testing.T) *meta.Smap {
	var (
		proxyURL = tools.GetPrimaryURL()
		smap     = tools.GetClusterMap(t, proxyURL)
		currURL  = smap.Primary.URL(cmn.NetPublic)
	)
	if currURL != proxyURL {
		t.Fatalf("Err in the test itself: expecting currURL %s == proxyurl %s", currURL, proxyURL)
	}

	psi, err := smap.GetRandProxy(true /*exclude primary*/)
	tassert.CheckFatal(t, err)
	return setPrimaryTo(t, proxyURL, smap, "", psi.ID())
}

// primaryAndNextCrash kills the primary proxy and a proxy that should be selected
// after the current primary dies, verifies the second in line proxy becomes
// the new primary, restore all proxies
func primaryAndNextCrash(t *testing.T) {
	proxyURL := tools.RandomProxyURL(t)
	smap := tools.GetClusterMap(t, proxyURL)
	origProxyCount := smap.CountActivePs()

	if origProxyCount < 4 {
		t.Skip("The test requires at least 4 proxies, found only ", origProxyCount)
	}

	// get next primary
	firstPrimaryID, firstPrimaryURL, err := chooseNextProxy(smap)
	tassert.CheckFatal(t, err)
	// Cluster map is re-read to have a clone of original smap that the test
	// can modify in any way it needs. Because original smap got must be preserved
	smapNext := tools.GetClusterMap(t, proxyURL)
	// get next primary
	firstPrimary := smapNext.Pmap[firstPrimaryID]
	delete(smapNext.Pmap, firstPrimaryID)
	finalPrimaryID, finalPrimaryURL, err := chooseNextProxy(smapNext)
	tassert.CheckFatal(t, err)

	// kill the current primary
	oldPrimaryURL, oldPrimaryID := smap.Primary.URL(cmn.NetPublic), smap.Primary.ID()
	tlog.Logfln("Killing primary proxy: %s - %s", oldPrimaryURL, oldPrimaryID)
	cmdFirst, err := tools.KillNode(baseParams, smap.Primary)
	tassert.CheckFatal(t, err)

	// kill the next primary
	tlog.Logfln("Killing next to primary proxy: %s - %s", firstPrimaryID, firstPrimaryURL)
	cmdSecond, errSecond := tools.KillNode(baseParams, firstPrimary)
	// if kill fails it does not make sense to wait for the cluster is stable
	if errSecond == nil {
		// the cluster should vote, so the smap version should be increased at
		// least by 100, that is why +99
		smap, err = tools.WaitForClusterState(finalPrimaryURL, "new primary elected",
			smap.Version+99, origProxyCount-2, 0)
		tassert.CheckFatal(t, err)
	}

	tlog.Logfln("Checking current primary, %s", smap.StringEx())
	if smap.Primary.ID() != finalPrimaryID {
		t.Errorf("Expected primary %s but real primary is %s", finalPrimaryID, smap.Primary.ID())
	}

	// restore next and prev primaries in the reversed order
	err = tools.RestoreNode(cmdSecond, false, "proxy (next primary)")
	tassert.CheckFatal(t, err)
	smap, err = tools.WaitForClusterState(finalPrimaryURL, "restore next primary",
		smap.Version, origProxyCount-1, 0)
	tassert.CheckFatal(t, err)

	err = tools.RestoreNode(cmdFirst, false, "proxy (prev primary)")
	tassert.CheckFatal(t, err)
	_, err = tools.WaitForClusterState(finalPrimaryURL, "restore prev primary",
		smap.Version, origProxyCount, 0)
	tassert.CheckFatal(t, err)
}

func TestIC(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true, RequiredDeployment: tools.ClusterTypeLocal})

	proxyURL := tools.RandomProxyURL(t)
	smap := tools.GetClusterMap(t, proxyURL)
	if cnt := smap.CountActivePs(); cnt < 4 {
		t.Fatalf("Not enough proxies (%d) to run tests (must be at least 4)", cnt)
	}

	defer tools.EnsureOrigClusterState(t)
	for _, test := range icTests {
		t.Run(test.name, test.method)
		if t.Failed() {
			t.FailNow()
		}
	}
	time.Sleep(time.Second)
}

func killRandNonPrimaryIC(t testing.TB, smap *meta.Smap) (tools.RestoreCmd, *meta.Smap) {
	origProxyCount := smap.CountActivePs()
	primary := smap.Primary
	var killNode *meta.Snode
	for _, psi := range smap.Pmap {
		if smap.IsIC(psi) && !psi.EqNetID(primary) {
			killNode = psi
			break
		}
	}
	cmd, err := tools.KillNode(baseParams, killNode)
	tassert.CheckFatal(t, err)

	smap, err = tools.WaitForClusterState(primary.URL(cmn.NetPublic), "propagate new Smap",
		smap.Version, origProxyCount-1, 0)
	tassert.CheckFatal(t, err)
	return cmd, smap
}

func icFromSmap(smap *meta.Smap) cos.StrSet {
	lst := make(cos.StrSet, meta.DfltCountIC)
	for pid, psi := range smap.Pmap {
		if smap.IsIC(psi) {
			lst.Add(pid)
		}
	}
	return lst
}

func icMemberLeaveAndRejoin(t *testing.T) {
	smap := tools.GetClusterMap(t, proxyURL)
	primary := smap.Primary
	origProxyCount := smap.CountActivePs()
	tassert.Fatalf(t, smap.ICCount() == meta.DfltCountIC,
		"should have %d members in IC, has %d", meta.DfltCountIC, smap.ICCount())

	// Primary must be an IC member
	tassert.Fatalf(t, smap.IsIC(primary), "primary (%s) should be a IC member, (were: %s)", primary, smap.StrIC(primary))

	// killing an IC member, should add a new IC member
	// select IC member which is not primary and kill
	origIC := icFromSmap(smap)
	cmd, smap := killRandNonPrimaryIC(t, smap)
	delete(origIC, cmd.Node.ID())

	tassert.Errorf(t, !smap.IsIC(cmd.Node), "Killed daemon (%s) must be removed from IC", cmd.Node.ID())

	// should have remaining IC nodes
	for sid := range origIC {
		tassert.Errorf(t, smap.IsIC(smap.GetProxy(sid)), "Should not remove existing IC members (%s)", sid)
	}
	tassert.Errorf(t, smap.ICCount() == meta.DfltCountIC, "should have %d members in IC, has %d",
		meta.DfltCountIC, smap.ICCount())

	err := tools.RestoreNode(cmd, false, "proxy")
	tassert.CheckFatal(t, err)

	updatedICs := icFromSmap(smap)
	smap, err = tools.WaitForClusterState(primary.URL(cmn.NetPublic), "restore IC member",
		smap.Version, origProxyCount, 0)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, smap.GetProxy(cmd.Node.ID()) != nil, "restored proxy %s is missing from %s",
		cmd.Node.ID(), smap.StringEx())
	err = tools.WaitNodeReady(cmd.Node.URL(cmn.NetPublic))
	tassert.CheckFatal(t, err)

	// Adding a new node shouldn't change IC members.
	newIC := icFromSmap(smap)
	tassert.Errorf(t, reflect.DeepEqual(updatedICs, newIC), "shouldn't update existing IC members")
}

func icKillAndRestorePrimary(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
	var (
		proxyURL   = tools.RandomProxyURL(t)
		smap       = tools.GetClusterMap(t, proxyURL)
		oldIC      = icFromSmap(smap)
		oldPrimary = smap.Primary
	)

	icCheck := func(smap *meta.Smap, newPrimary, oldPrimary *meta.Snode) {
		// Old primary shouldn't be in IC.
		tassert.Errorf(t, !smap.IsIC(oldPrimary), "killed primary (%s) must be removed from IC", oldPrimary)

		// New primary should be part of IC.
		tassert.Errorf(t, smap.IsIC(newPrimary), "new primary (%s) must be part of IC", newPrimary)

		// Remaining IC member should be unchanged.
		for sid := range oldIC {
			if sid != oldPrimary.ID() {
				tassert.Errorf(t, smap.IsIC(smap.GetProxy(sid)), "should not remove existing IC members (%s)", sid)
			}
		}
	}

	smap = killRestorePrimary(t, proxyURL, true, icCheck)

	// When a node added as primary, it should add itself to IC.
	tassert.Fatalf(t, smap.IsIC(oldPrimary),
		"primary (%s) should be a IC member, (were: %s)", oldPrimary, smap.StrIC(oldPrimary))
	tassert.Errorf(t, smap.ICCount() == meta.DfltCountIC,
		"should have %d members in IC, has %d", meta.DfltCountIC, smap.ICCount())
}

func icSyncOwnershipTable(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		smap       = tools.GetClusterMap(t, proxyURL)
		primary    = smap.Primary

		src = cmn.Bck{
			Name:     testBucketName,
			Provider: apc.AIS,
		}

		dstBck = cmn.Bck{
			Name:     testBucketName + "_new",
			Provider: apc.AIS,
		}
	)

	tools.CreateBucket(t, proxyURL, src, nil, true /*cleanup*/)

	// Start any xaction and get ID.
	xid, err := api.CopyBucket(baseParams, src, dstBck, nil)
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		tools.DestroyBucket(t, proxyURL, dstBck)
	})

	// Killing an IC member, should add a new IC member.
	// Select IC member which is not primary and kill.
	origIC := icFromSmap(smap)
	cmd, smap := killRandNonPrimaryIC(t, smap)

	// Try getting xaction status from new IC member.
	updatedIC := icFromSmap(smap)
	newICMemID := getNewICMember(t, origIC, updatedIC)

	newICNode := smap.GetProxy(newICMemID)

	baseParams = tools.BaseAPIParams(newICNode.URL(cmn.NetPublic))
	xargs := xact.ArgsMsg{ID: xid, Kind: apc.ActCopyBck}
	_, err = api.GetOneXactionStatus(baseParams, &xargs)
	tassert.CheckError(t, err)

	err = tools.RestoreNode(cmd, false, "proxy")
	tassert.CheckFatal(t, err)

	smap, err = tools.WaitNodeAdded(baseParams, cmd.Node.ID())
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, !smap.IsIC(cmd.Node), "newly joined node shouldn't be in IC (%s)", cmd.Node)

	// Should sync ownership table when non-ic member become primary.
	smap = setPrimaryTo(t, primary.URL(cmn.NetPublic), smap, "", cmd.Node.ID())
	tassert.Fatalf(t, smap.IsIC(cmd.Node), "primary (%s) should be a IC member, (were: %s)", primary, smap.StrIC(primary))

	baseParams = tools.BaseAPIParams(cmd.Node.URL(cmn.NetPublic))
	_, err = api.GetOneXactionStatus(baseParams, &xargs)
	tassert.CheckError(t, err)
}

func icSinglePrimaryRevamp(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	var (
		proxyURL       = tools.RandomProxyURL(t)
		smap           = tools.GetClusterMap(t, proxyURL)
		origProxyCount = smap.CountActivePs()

		src = cmn.Bck{
			Name:     testBucketName,
			Provider: apc.AIS,
		}

		dstBck = cmn.Bck{
			Name:     testBucketName + "_new",
			Provider: apc.AIS,
		}
	)

	nodesToRestore := make([]tools.RestoreCmd, 0, origProxyCount-1)

	// Kill all nodes except primary.
	for i := origProxyCount; i > 1; i-- {
		var cmd tools.RestoreCmd
		cmd, smap = killRandNonPrimaryIC(t, smap)
		nodesToRestore = append(nodesToRestore, cmd)
	}

	proxyURL = smap.Primary.URL(cmn.NetPublic)
	baseParams = tools.BaseAPIParams(proxyURL)
	tools.CreateBucket(t, proxyURL, src, nil, true /*cleanup*/)

	// Start any xaction and get ID.
	xid, err := api.CopyBucket(baseParams, src, dstBck, nil)
	xargs := xact.ArgsMsg{ID: xid, Kind: apc.ActCopyBck}

	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		tools.DestroyBucket(t, proxyURL, dstBck)
	})

	// Restart all killed nodes and check for xaction status.
	for _, cmd := range nodesToRestore {
		err = tools.RestoreNode(cmd, false, "proxy")
		tassert.CheckError(t, err)

		smap, err = tools.WaitForClusterState(proxyURL,
			"restore node "+cmd.Node.ID(), smap.Version,
			smap.CountActivePs()+1, smap.CountTargets())
		tassert.CheckFatal(t, err)

		baseParams = tools.BaseAPIParams(cmd.Node.URL(cmn.NetPublic))
		_, err = api.GetOneXactionStatus(baseParams, &xargs)
		tassert.CheckError(t, err)
	}
}

func icStressMonitorXactMultiICFail(t *testing.T) {
	var (
		proxyURL = tools.GetPrimaryURL()
		smap     = tools.GetClusterMap(t, proxyURL)

		m = ioContext{
			t:        t,
			num:      1000,
			fileSize: 50 * cos.KiB,
		}
		numCopyXacts = 20
	)

	// 1. Populate a bucket required for copy xactions
	m.init(true /*cleanup*/)
	tools.CreateBucket(t, proxyURL, m.bck, nil, true /*cleanup*/)
	m.puts()

	// 2. Kill and restore random IC members in background
	stopCh := &cos.StopCh{}
	stopCh.Init()
	krWg := &sync.WaitGroup{}
	krWg.Add(1)
	go killRestoreIC(t, smap, stopCh, krWg)
	defer func() {
		// Stop the background kill and restore task
		stopCh.Close()
		krWg.Wait()
	}()

	// 3. Start multiple xactions and poll random proxy for status till xaction is complete
	wg := startCPBckAndWait(t, m.bck, numCopyXacts)
	wg.Wait()
}

func startCPBckAndWait(t testing.TB, srcBck cmn.Bck, count int) *sync.WaitGroup {
	var (
		proxyURL   = tools.GetPrimaryURL()
		baseParams = tools.BaseAPIParams(proxyURL)
		wg         = &sync.WaitGroup{}
	)
	for i := range count {
		wg.Add(1)
		go func(idx int) {
			dstBck := cmn.Bck{
				Name:     fmt.Sprintf("%s_dst_par_%d", testBucketName, idx),
				Provider: apc.AIS,
			}
			xid, err := api.CopyBucket(baseParams, srcBck, dstBck, nil)
			tassert.CheckError(t, err)
			defer func() {
				tools.DestroyBucket(t, proxyURL, dstBck)
				wg.Done()
			}()
			xargs := xact.ArgsMsg{ID: xid, Timeout: tools.RebalanceTimeout}
			_, err = api.WaitForXactionIC(baseParams, &xargs)
			tassert.CheckError(t, err)
		}(i)
	}
	return wg
}

// Continuously kill and restore IC nodes
func killRestoreIC(t *testing.T, smap *meta.Smap, stopCh *cos.StopCh, wg *sync.WaitGroup) {
	var (
		cmd      tools.RestoreCmd
		proxyURL = smap.Primary.URL(cmn.NetPublic)
	)
	defer wg.Done()

	for {
		cmd, smap = killRandNonPrimaryIC(t, smap)
		err := tools.RestoreNode(cmd, false, "proxy")
		tassert.CheckFatal(t, err)

		smap, err = tools.WaitForClusterState(proxyURL, "restore", smap.Version, 0, 0)
		tassert.CheckFatal(t, err)
		time.Sleep(2 * time.Second)

		select {
		case <-stopCh.Listen():
			return
		default:
			break
		}
	}
}

// misc

func getNewICMember(t testing.TB, oldMap, newMap cos.StrSet) (daeID string) {
	for sid := range newMap {
		if _, ok := oldMap[sid]; !ok {
			tassert.Errorf(t, daeID == "", "should change only one IC member")
			daeID = sid
		}
	}
	tassert.Fatalf(t, daeID != "", "should change at least one IC member")
	return
}

//
// mock target
//

const (
	mockTargetPort = "8079"
)

type targetMocker interface {
	filehdlr(w http.ResponseWriter, r *http.Request)
	daemonhdlr(w http.ResponseWriter, r *http.Request)
	votehdlr(w http.ResponseWriter, r *http.Request)
	healthdlr(w http.ResponseWriter, r *http.Request)
}

type MockRegRequest struct {
	SI *meta.Snode `json:"si"`
}

func runMockTarget(t *testing.T, proxyURL string, mocktgt targetMocker, stopch chan struct{}, smap *meta.Smap, wg *sync.WaitGroup) {
	defer wg.Done()
	mux := http.NewServeMux()

	mux.HandleFunc(apc.URLPathBuckets.S, mocktgt.filehdlr)
	mux.HandleFunc(apc.URLPathObjects.S, mocktgt.filehdlr)
	mux.HandleFunc(apc.URLPathDae.S, mocktgt.daemonhdlr)
	mux.HandleFunc(apc.URLPathVote.S, mocktgt.votehdlr)
	mux.HandleFunc(apc.URLPathHealth.S, mocktgt.healthdlr)

	target, _ := smap.GetRandTarget()
	ip := target.PubNet.Hostname

	s := &http.Server{
		Addr:              ip + ":" + mockTargetPort,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go s.ListenAndServe()

	err := registerMockTarget(proxyURL, smap)
	if err != nil {
		t.Errorf("failed to start http server for mock target: %v", err)
		return
	}
	tlog.Logfln("t[%s] is up", tools.MockDaemonID)

	<-stopch

	tlog.Logfln("started unsafe removal of t[%s]", tools.MockDaemonID)
	err = tools.RemoveNodeUnsafe(proxyURL, tools.MockDaemonID)
	if err != nil {
		tlog.Logfln("Error: failed to unsafely remove t[%s]: %v", tools.MockDaemonID, err)
	}
	s.Shutdown(t.Context())
}

func registerMockTarget(proxyURL string, smap *meta.Smap) error {
	var (
		jsonDaemonInfo []byte
		err            error
	)

	// borrow a random target's ip but using a different port to register the mock target
	for _, v := range smap.Tmap {
		v.DaeID = tools.MockDaemonID
		v.PubNet = meta.NetInfo{
			Hostname: v.PubNet.Hostname,
			Port:     mockTargetPort,
			URL:      "http://" + v.PubNet.Hostname + ":" + mockTargetPort,
		}
		v.ControlNet = v.PubNet
		v.DataNet = v.PubNet
		regReq := MockRegRequest{SI: v}
		jsonDaemonInfo, err = jsoniter.Marshal(regReq)
		if err != nil {
			return err
		}
		break
	}
	baseParams := tools.BaseAPIParams(proxyURL)
	baseParams.Method = http.MethodPost
	reqParams := &api.ReqParams{
		BaseParams: baseParams,
		Path:       apc.URLPathCluAutoReg.S,
		Body:       jsonDaemonInfo,
		Header: http.Header{
			cos.HdrContentType: []string{cos.ContentJSON},
			apc.HdrNodeVersion: []string{cmn.VersionAIStore},
		},
	}
	return reqParams.DoRequest()
}

type voteRetryMockTarget struct {
	voteInProgress bool
	errCh          chan error
}

type cluMetaRedux struct {
	Smap           *meta.Smap
	VoteInProgress bool `json:"voting"`
}

func newVoteMsg(inp bool) cluMetaRedux {
	return cluMetaRedux{VoteInProgress: inp, Smap: &meta.Smap{Version: 1}}
}

func (*voteRetryMockTarget) filehdlr(http.ResponseWriter, *http.Request) {
	// Ignore all file requests
}

func (p *voteRetryMockTarget) daemonhdlr(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		msg := newVoteMsg(p.voteInProgress) // treat all Get requests as requests for a VoteMsg
		jsbytes, err := jsoniter.Marshal(msg)
		if err == nil {
			_, err = w.Write(jsbytes)
		}
		if err != nil {
			p.errCh <- fmt.Errorf("error writing vote message: %v", err)
		}
	default:
	}
}

func (*voteRetryMockTarget) votehdlr(w http.ResponseWriter, _ *http.Request) {
	// Always vote yes.
	w.Write([]byte(ais.VoteYes))
}

func (p *voteRetryMockTarget) healthdlr(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	getRebStatus := cos.IsParseBool(query.Get(apc.QparamRebStatus))
	if getRebStatus {
		status := &reb.Status{}
		status.RebID = math.MaxInt64 // to abort t[MOCK] join triggered rebalance
		body := cos.MustMarshal(status)
		_, err := w.Write(body)
		if err != nil {
			p.errCh <- fmt.Errorf("error writing reb-status: %v", err)
		}
	}
}

//
// new tests added here --------------------------------------------
//

// Use case:
// - some starting nodes contact the former primary;
// - the rest of the cluster has already elected a new primary.
func TestStartupFormerPrimary(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{
		Long:               true,
		RequiredDeployment: tools.ClusterTypeLocal,
		MinProxies:         3,
		MinTargets:         2,
	})
	t.Setenv(env.AisPrimaryEP, "") // do not explicitly designate the returning proxy as primary
	defer tools.EnsureOrigClusterState(t)

	var (
		original      = tools.GetClusterMap(t, tools.GetPrimaryURL())
		former        = original.Primary
		formerURL     = former.URL(cmn.NetPublic)
		formerConfig  = tools.GetDaemonConfig(t, former)
		formerStarted bool
		waitOpts      = &tools.WaitRetryOpts{
			MaxRetries: int(formerConfig.Timeout.Startup.D() / (100 * time.Millisecond)),
			Interval:   100 * time.Millisecond,
		}
	)
	_, electedURL, err := chooseNextProxy(original)
	tassert.CheckFatal(t, err)

	formerCmd, err := tools.KillNode(baseParams, former)
	tassert.CheckFatal(t, err)
	formerArgs := slices.Clone(formerCmd.Args)
	defer func() {
		if !formerStarted {
			t.Setenv(env.AisPrimaryEP, electedURL)
			tassert.CheckError(t, tools.RestoreNode(formerCmd, false, "former primary (cleanup)"))
		}
	}()

	elected, err := tools.WaitForClusterState(electedURL, "elect a new primary", original.Version,
		original.CountActivePs()-1, original.CountActiveTs())
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, elected.Primary.ID() != former.ID(), "former primary was not replaced: %s", elected)
	electedURL = elected.Primary.URL(cmn.NetPublic)
	electedID := elected.Primary.ID()
	tlog.Logfln("Elected %s while former primary %s is offline", electedID, former.ID())

	// Restart one target with its post-election Smap, but direct its join to the former primary.
	// Keep the other targets and proxies running with the elected primary.
	target, err := elected.GetRandTarget()
	tassert.CheckFatal(t, err)
	targetCmd, err := tools.KillNode(tools.BaseAPIParams(electedURL), target)
	tassert.CheckFatal(t, err)
	targetStarted := false
	defer func() {
		t.Setenv(env.AisPrimaryEP, electedURL)
		formerCmd.Args = formerArgs
		if !targetStarted {
			tassert.CheckError(t, tools.RestoreNode(targetCmd, false, "target (cleanup)"))
		}
		tassert.CheckError(t, tools.WaitNodeReady(target.URL(cmn.NetPublic), waitOpts))
		// Changing startup roles does not yet hand off admitted nodes. Re-admit this
		// target explicitly during cleanup, including its newly generated verifying key.
		_, err := tools.JoinCluster(electedURL, target)
		tassert.CheckError(t, err)

		// former primary not in the elected Smap: either not restarted or still running as a
		// provisional primary (stale Smap, `-ntargets=1`) - restart it with the current Smap and original args
		smap, err := api.GetClusterMap(tools.BaseAPIParams(electedURL))
		tassert.CheckError(t, err)
		if err == nil && smap.GetProxy(former.ID()) == nil {
			tlog.Logfln("Former primary %s not in %s - restarting it as secondary", former.ID(), smap)
			if formerStarted {
				if _, err := tools.KillNode(tools.BaseAPIParams(formerURL), former); err != nil {
					tlog.Logfln("Former primary %s: %v", former.ID(), err)
				}
			}
			err = jsp.SaveMeta(filepath.Join(formerConfig.ConfigDir, fname.Smap), smap, nil)
			tassert.CheckError(t, err)
			err = tools.RestoreNode(formerCmd, false, "former primary (cleanup)")
			tassert.CheckError(t, err)
			formerStarted = err == nil
		}
		_, err = tools.WaitForClusterState(electedURL, "restore original membership", 0,
			original.CountActivePs(), original.CountActiveTs())
		tassert.CheckError(t, err)
		tools.WaitForRebalAndResil(t, tools.BaseAPIParams(electedURL))
	}()

	// Retain only self in the former primary's stale Smap so that its earlier health
	// and registration-time discovery cannot find the elected cluster. Leadership must
	// be recovered from the incoming registration, exercising regpoolMaxVer.
	stale := &meta.Smap{
		UUID: original.UUID, CreationTime: original.CreationTime, Version: original.Version,
		Primary: former, Pmap: meta.NodeMap{former.ID(): former}, Tmap: make(meta.NodeMap),
	}
	err = jsp.SaveMeta(filepath.Join(formerConfig.ConfigDir, fname.Smap), stale, nil)
	tassert.CheckFatal(t, err)
	formerCmd.Args = append(slices.Clone(formerArgs), "-ntargets=1")
	err = tools.RestoreNode(formerCmd, false, "former primary")
	tassert.CheckFatal(t, err)
	formerStarted = true
	tassert.CheckFatal(t, tools.WaitNodeReady(formerURL, waitOpts))
	boot := tools.GetClusterMap(t, formerURL)
	tassert.Fatalf(t, boot.Primary.ID() == former.ID() && boot.CountTargets() == 0,
		"expected provisional primary awaiting registrations, got %s", boot)

	t.Setenv(env.AisPrimaryEP, formerURL)
	err = tools.RestoreNode(targetCmd, false, "target joining former primary")
	tassert.CheckFatal(t, err)
	targetStarted = true
	t.Setenv(env.AisPrimaryEP, "")

	// First prove that the target actually registered with the provisional primary.
	deadline := time.Now().Add(formerConfig.Timeout.Startup.D())
	for {
		boot = tools.GetClusterMap(t, formerURL)
		if boot.GetTarget(target.ID()) != nil {
			tassert.Fatalf(t, boot.Primary.ID() == former.ID(),
				"target did not register before the former primary changed roles: %s", boot)
			break
		}
		tassert.Fatalf(t, time.Now().Before(deadline), "target never registered with former primary: %s", boot)
		time.Sleep(100 * time.Millisecond)
	}
	tlog.Logfln("%s admitted %s while %s remained primary of the running cluster", former.ID(), target.ID(), electedID)

	// The former primary must yield, rather than adopt the newer Smap and bump its
	// version to take over. Check both views so convergence on the wrong leader fails.
	deadline = time.Now().Add(formerConfig.Timeout.Startup.D() + tools.WaitClusterStartup)
	for {
		boot = tools.GetClusterMap(t, formerURL)
		current := tools.GetClusterMap(t, electedURL)
		tassert.Fatalf(t, current.Primary.ID() == electedID,
			"former primary hijacked the cluster: expected %s, got %s", electedID, current)
		tassert.Fatalf(t, boot.Primary.ID() != former.ID() || boot.Version <= elected.Version,
			"former primary took over the recovered Smap: %s (elected %s)", boot, elected)
		if boot.Primary.ID() == electedID && current.GetProxy(former.ID()) != nil {
			tassert.Fatalf(t, boot.UUID == original.UUID && current.UUID == original.UUID,
				"cluster UUID changed: %s vs %s", boot, current)
			break
		}
		tassert.Fatalf(t, time.Now().Before(deadline), "former primary did not rejoin as secondary: %s", boot)
		time.Sleep(100 * time.Millisecond)
	}
	_, err = tools.WaitForClusterState(electedURL, "former primary rejoined as secondary", elected.Version,
		original.CountActivePs(), 0, target.ID())
	tassert.CheckFatal(t, err)
	checkSmaps(t, electedURL)
}
