// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/devtools/docker"
	"github.com/NVIDIA/aistore/devtools/readers"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tlog"
	"github.com/NVIDIA/aistore/devtools/trand"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/OneOfOne/xxhash"
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
		{"MinorityTargetMapVersionMismatch", minorityTargetMapVersionMismatch},
		{"MajorityTargetMapVersionMismatch", majorityTargetMapVersionMismatch},
		{"ConcurrentPutGetDel", concurrentPutGetDel},
		{"ProxyStress", proxyStress},
		{"NetworkFailure", networkFailure},
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
		{"ICStressCachedXactions", icStressCachedXactions},
	}
)

func TestMultiProxy(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true, RequiredDeployment: tutils.ClusterTypeLocal})

	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)
	if cnt := smap.CountActiveProxies(); cnt < 3 {
		t.Fatalf("Not enough proxies (%d) to run tests (must be at least 3)", cnt)
	}
	if smap.CountActiveTargets() < 1 {
		t.Fatalf("No active targets to run tests (%s, num-t-active=0)", smap.StringEx())
	}

	defer tutils.EnsureOrigClusterState(t)
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
	proxyURL := tutils.RandomProxyURL(t)
	killRestorePrimary(t, proxyURL, false, nil)
}

func killRestorePrimary(t *testing.T, proxyURL string, restoreAsPrimary bool,
	postKill func(smap *cluster.Smap, newPrimary, oldPrimary *cluster.Snode)) *cluster.Smap {
	var (
		smap          = tutils.GetClusterMap(t, proxyURL)
		proxyCount    = smap.CountActiveProxies()
		oldPrimary    = smap.Primary
		oldPrimaryURL = smap.Primary.URL(cmn.NetPublic)
		oldPrimaryID  = smap.Primary.ID()
	)

	tlog.Logf("targets: %d, proxies: %d\n", smap.CountActiveTargets(), proxyCount)
	newPrimaryID, newPrimaryURL, err := chooseNextProxy(smap)
	tassert.CheckFatal(t, err)
	newPrimary := smap.GetProxy(newPrimaryID)

	tlog.Logf("New primary: %s --> %s\n", newPrimaryID, newPrimaryURL)
	tlog.Logf("Killing primary: %s --> %s\n", oldPrimaryURL, oldPrimaryID)

	// cmd and args are the original command line of how the proxy is started
	cmd, err := tutils.KillNode(smap.Primary)
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForClusterState(newPrimaryURL, "designate new primary", smap.Version,
		smap.CountActiveProxies()-1, smap.CountActiveTargets())
	tassert.CheckFatal(t, err)
	tlog.Logf("New primary elected: %s\n", newPrimaryID)

	tassert.Errorf(t, smap.Primary.ID() == newPrimaryID, "Wrong primary proxy: %s, expecting: %s", smap.Primary.ID(), newPrimaryID)

	if postKill != nil {
		postKill(smap, newPrimary, oldPrimary)
	}

	// re-construct the command line to start the original proxy but add the current primary proxy to the args
	err = tutils.RestoreNode(cmd, false, "proxy (prev primary)")
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForClusterState(newPrimaryURL, "restore", smap.Version, proxyCount, 0)
	tassert.CheckFatal(t, err)
	if _, ok := smap.Pmap[oldPrimaryID]; !ok {
		t.Fatalf("Previous primary proxy did not rejoin the cluster")
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
	tutils.CheckSkip(t, tutils.SkipTestArgs{RequiredDeployment: tutils.ClusterTypeLocal})

	var (
		proxyURL                      = tutils.GetPrimaryURL()
		smap                          = tutils.GetClusterMap(t, proxyURL)
		origProxyCnt, origTargetCount = smap.CountActiveProxies(), smap.CountActiveTargets()
		portInc                       = 100
		node                          *cluster.Snode
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
	cfg := tutils.GetDaemonConfig(t, node)
	tlog.Logf("Killing %s\n", node.StringEx())
	cmd, err := tutils.KillNode(node)
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForClusterState(proxyURL, "kill primary", smap.Version, origProxyCnt-pdc, origTargetCount-tdc)
	tassert.CheckFatal(t, err)

	// Update local config ports.
	localConfPath := filepath.Join(cfg.ConfigDir, fname.PlaintextInitialConfig)
	localConf := &cmn.LocalConfig{}
	_, err = jsp.LoadMeta(localConfPath, localConf)
	tassert.CheckFatal(t, err)
	localConf.HostNet.Port = cfg.HostNet.Port + portInc
	localConf.HostNet.PortIntraControl = cfg.HostNet.PortIntraControl + portInc
	localConf.HostNet.PortIntraData = cfg.HostNet.PortIntraData + portInc
	err = jsp.SaveMeta(localConfPath, localConf, nil)
	tassert.CheckFatal(t, err)

	err = tutils.RestoreNode(cmd, false, nodeType)
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForClusterState(proxyURL, "restore with changed config", smap.Version, origProxyCnt, 0)
	tassert.CheckFatal(t, err)

	// Health check with old public URL should fail
	err = api.Health(tutils.BaseAPIParams(node.URL(cmn.NetPublic)))
	tassert.Errorf(t, err != nil, "health check with old IP information should fail %v", err)

	newNode := smap.GetNode(node.ID())
	err = tutils.WaitNodeReady(newNode.URL(cmn.NetPublic))
	tassert.CheckError(t, err)
	if !restore {
		// Revert port changes
		restore = true
		node = newNode
		portInc = -portInc
		goto killRestore
	}

	if nodeType == apc.Target {
		tutils.WaitForRebalAndResil(t, tutils.BaseAPIParams(proxyURL))
	}
}

// primaryAndTargetCrash kills the primary p[roxy and one random target, verifies the next in
// line proxy becomes the new primary, restore the target and proxy, restore original primary.
func primaryAndTargetCrash(t *testing.T) {
	if docker.IsRunning() {
		t.Skip("Skipped because setting new primary URL in command line for docker is not supported")
	}

	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)
	tlog.Logf("targets: %d, proxies: %d\n", smap.CountActiveTargets(), smap.CountActiveProxies())

	newPrimaryID, newPrimaryURL, err := chooseNextProxy(smap)
	tassert.CheckFatal(t, err)

	oldPrimaryURL := smap.Primary.URL(cmn.NetPublic)
	tlog.Logf("Killing proxy %s - %s\n", oldPrimaryURL, smap.Primary.ID())
	cmd, err := tutils.KillNode(smap.Primary)
	tassert.CheckFatal(t, err)

	// Select a random target
	var (
		targetURL       string
		targetID        string
		targetNode      *cluster.Snode
		origTargetCount = smap.CountActiveTargets()
		origProxyCount  = smap.CountActiveProxies()
	)

	targetNode, _ = smap.GetRandTarget()
	targetURL = targetNode.URL(cmn.NetPublic)
	targetID = targetNode.ID()

	tlog.Logf("Killing target: %s - %s\n", targetURL, targetID)
	tcmd, err := tutils.KillNode(targetNode)
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForClusterState(newPrimaryURL, "designate new primary",
		smap.Version, origProxyCount-1, origTargetCount-1)
	tassert.CheckFatal(t, err)

	if smap.Primary.ID() != newPrimaryID {
		t.Fatalf("Wrong primary proxy: %s, expecting: %s", smap.Primary.ID(), newPrimaryID)
	}

	err = tutils.RestoreNode(tcmd, false, "target")
	tassert.CheckFatal(t, err)

	err = tutils.RestoreNode(cmd, false, "proxy (prev primary)")
	tassert.CheckFatal(t, err)

	_, err = tutils.WaitForClusterState(newPrimaryURL, "restore proxy and target",
		smap.Version, origProxyCount, origTargetCount)
	tassert.CheckFatal(t, err)
	tutils.WaitForRebalAndResil(t, tutils.BaseAPIParams(newPrimaryURL))
}

// A very simple test to check if a primary proxy can detect non-primary one
// dies and then update and sync SMap
func proxyCrash(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)
	tlog.Logf("targets: %d, proxies: %d\n", smap.CountActiveTargets(), smap.CountActiveProxies())

	primaryURL := smap.Primary.URL(cmn.NetPublic)
	tlog.Logf("Primary: %s\n", smap.Primary.StringEx())

	origProxyCount := smap.CountActiveProxies()
	secondNode, err := smap.GetRandProxy(true /*exclude primary*/)
	tassert.CheckFatal(t, err)

	tlog.Logf("Killing non-primary %s\n", secondNode.StringEx())
	secondCmd, err := tutils.KillNode(secondNode)
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForClusterState(primaryURL, "proxy removed", smap.Version, origProxyCount-1, 0)
	tassert.CheckFatal(t, err)

	err = tutils.RestoreNode(secondCmd, false, "proxy")
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForClusterState(primaryURL, "proxy restoreid", smap.Version, origProxyCount, 0)
	tassert.CheckFatal(t, err)

	if _, ok := smap.Pmap[secondNode.ID()]; !ok {
		t.Fatalf("Non-primary proxy did not rejoin the cluster.")
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
	tutils.CheckSkip(t, tutils.SkipTestArgs{RequiredDeployment: tutils.ClusterTypeLocal})

	var (
		proxyURL = tutils.GetPrimaryURL()
		smap     = tutils.GetClusterMap(t, proxyURL)
		node     *cluster.Snode
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
	conf := tutils.GetDaemonConfig(t, node)

	// Create local config for daemon.
	localConf := &cmn.LocalConfig{}
	localConf.ConfigDir = conf.ConfigDir
	localConf.HostNet.Port = conf.HostNet.Port + portInc
	localConf.HostNet.PortIntraControl = conf.HostNet.PortIntraControl + portInc
	localConf.HostNet.PortIntraData = conf.HostNet.PortIntraData + portInc

	// start with different config but same daemon ID
	pid := tutils.DeployNode(t, node, conf, localConf)
	t.Cleanup(func() {
		tutils.CleanupNode(t, pid)
	})

	err = tutils.WaitForNodeToTerminate(pid)
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
	tutils.CheckSkip(t, tutils.SkipTestArgs{RequiredDeployment: tutils.ClusterTypeLocal})

	var (
		proxyURL = tutils.GetPrimaryURL()
		smap     = tutils.GetClusterMap(t, proxyURL)
		node     *cluster.Snode
		err      error
	)

	if nodeType == apc.Proxy {
		node, err = smap.GetRandProxy(true)
	} else {
		node, err = smap.GetRandTarget()
	}
	tassert.CheckFatal(t, err)
	conf := tutils.GetDaemonConfig(t, node)

	// Make sure that the `DaemonID` is different.
	node.DaeID = "testing_" + trand.String(10)

	pid := tutils.DeployNode(t, node, conf, nil)
	t.Cleanup(func() {
		tutils.CleanupNode(t, pid)
	})

	err = tutils.WaitForNodeToTerminate(pid)
	tassert.CheckFatal(t, err)
}

// primaryAndProxyCrash kills primary proxy and one another proxy(not the next in line primary)
// and restore them afterwards
func primaryAndProxyCrash(t *testing.T) {
	var (
		proxyURL                    = tutils.RandomProxyURL(t)
		smap                        = tutils.GetClusterMap(t, proxyURL)
		origProxyCount              = smap.CountActiveProxies()
		oldPrimaryURL, oldPrimaryID = smap.Primary.URL(cmn.NetPublic), smap.Primary.ID()
		secondNode                  *cluster.Snode
		secondID                    string
	)
	tlog.Logf("targets: %d, proxies: %d\n", smap.CountActiveTargets(), smap.CountActiveProxies())

	newPrimaryID, newPrimaryURL, err := chooseNextProxy(smap)
	tassert.CheckFatal(t, err)

	tlog.Logf("Killing primary: %s - %s\n", oldPrimaryURL, oldPrimaryID)
	cmd, err := tutils.KillNode(smap.Primary)
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
	n := cos.NowRand().Intn(20)
	time.Sleep(time.Duration(n+1) * time.Second)

	tlog.Logf("Killing non-primary: %s\n", secondNode.StringEx())
	secondCmd, err := tutils.KillNode(secondNode)
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForClusterState(newPrimaryURL, "elect new primary",
		smap.Version, origProxyCount-2, 0)
	tassert.CheckFatal(t, err)

	err = tutils.RestoreNode(cmd, true, "previous primary "+oldPrimaryID)
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForClusterState(newPrimaryURL, "join back previous primary "+oldPrimaryID,
		smap.Version, origProxyCount-1, 0)
	tassert.CheckFatal(t, err)

	err = tutils.RestoreNode(secondCmd, false, "proxy")
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForClusterState(newPrimaryURL, "join back non-primary "+secondID,
		smap.Version, origProxyCount, 0)
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
		id       string
		node     *cluster.Snode
		proxyURL = tutils.RandomProxyURL(t)
	)

	smap := tutils.GetClusterMap(t, proxyURL)
	tlog.Logf("targets: %d, proxies: %d\n", smap.CountActiveTargets(), smap.CountActiveProxies())

	node, _ = smap.GetRandTarget()
	id = node.ID()

	cmd, err := tutils.KillNode(node)
	tassert.CheckFatal(t, err)
	smap, err = tutils.WaitForClusterState(proxyURL, "synchronize on 'target crashed'",
		smap.Version, smap.CountActiveProxies(), smap.CountActiveTargets()-1)
	tassert.CheckFatal(t, err)

	if _, ok := smap.Tmap[id]; ok {
		t.Fatalf("Killed target was not removed from the Smap: %v", id)
	}

	err = tutils.RestoreNode(cmd, false, "target")
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForClusterState(proxyURL, "synchronize on 'target rejoined'",
		smap.Version, smap.CountActiveProxies(), smap.CountActiveTargets()+1)
	tassert.CheckFatal(t, err)

	if _, ok := smap.Tmap[id]; !ok {
		t.Fatalf("Restarted target %s did not rejoin the cluster", id)
	}
	tutils.WaitForRebalAndResil(t, tutils.BaseAPIParams(proxyURL))
}

// crashAndFastRestore kills the primary and restores it before a new leader is elected
func crashAndFastRestore(t *testing.T) {
	var err error
	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)
	tlog.Logf("targets: %d, proxies: %d\n", smap.CountActiveTargets(), smap.CountActiveProxies())

	// Make sure proxyURL is not primary URL.
	_, proxyURL, err = chooseNextProxy(smap)
	tassert.CheckFatal(t, err)
	oldPrimaryID := smap.Primary.ID()
	tlog.Logf("The current primary %s, Smap version %d\n", oldPrimaryID, smap.Version)

	cmd, err := tutils.KillNode(smap.Primary)
	tassert.CheckFatal(t, err)

	// quick crash and recover
	time.Sleep(2 * time.Second)
	err = tutils.RestoreNode(cmd, true, "proxy (primary)")
	tassert.CheckFatal(t, err)

	tlog.Logf("The %s is currently restarting\n", oldPrimaryID)

	// NOTE: using (version - 1) because the primary will restart with its old version,
	//       there will be no version change for this restore, so force beginning version to 1 less
	//       than the original version in order to use WaitForClusterState.
	smap, err = tutils.WaitForClusterState(proxyURL, "restore", smap.Version-1, 0, 0)
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
		proxyURL     = tutils.RandomProxyURL(t)
		smap         = tutils.GetClusterMap(t, proxyURL)
		oldTargetCnt = smap.CountActiveTargets()
		oldProxyCnt  = smap.CountActiveProxies()
		stopch       = make(chan struct{})
		errCh        = make(chan error, 10)
		mocktgt      = &voteRetryMockTarget{
			voteInProgress: true,
			errCh:          errCh,
		}
	)
	tlog.Logf("targets: %d, proxies: %d\n", oldTargetCnt, oldProxyCnt)

	go runMockTarget(t, proxyURL, mocktgt, stopch, smap)

	_, err := tutils.WaitForClusterState(proxyURL, "synchronize on 'new mock target'",
		smap.Version, oldProxyCnt, oldTargetCnt+1)
	tassert.CheckFatal(t, err)

	smap = killRestorePrimary(t, proxyURL, false, nil)
	//
	// FIXME: election is in progress if and only when xaction(apc.ActElection) is running -
	//        simulating the scenario via mocktgt.voteInProgress = true is incorrect
	//
	// if _, ok := smap.Pmap[oldPrimaryID]; ok {
	//	t.Fatalf("Previous primary proxy rejoined the cluster during a vote")
	// }
	mocktgt.voteInProgress = false
	// smap, err = tutils.WaitForClusterState(newPrimaryURL, "synchronize new Smap",
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

	_, err = tutils.WaitForClusterState(smap.Primary.URL(cmn.NetPublic),
		"to kill mock target", smap.Version, oldProxyCnt, oldTargetCnt)
	tassert.CheckFatal(t, err)
}

func minorityTargetMapVersionMismatch(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)
	targetMapVersionMismatch(
		func(i int) int {
			return i/4 + 1
		}, t, proxyURL)
}

func majorityTargetMapVersionMismatch(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)
	targetMapVersionMismatch(
		func(i int) int {
			return i/2 + 1
		}, t, proxyURL)
}

// targetMapVersionMismatch updates map version of a few targets, kill the primary proxy
// wait for the new leader to come online
func targetMapVersionMismatch(getNum func(int) int, t *testing.T, proxyURL string) {
	smap := tutils.GetClusterMap(t, proxyURL)
	tlog.Logf("targets: %d, proxies: %d\n", smap.CountActiveTargets(), smap.CountActiveProxies())

	smap.Version++
	jsonMap, err := jsoniter.Marshal(smap)
	tassert.CheckFatal(t, err)

	n := getNum(smap.CountActiveTargets() + smap.CountActiveProxies() - 1)
	for _, v := range smap.Tmap {
		if n == 0 {
			break
		}
		baseParams := tutils.BaseAPIParams(v.URL(cmn.NetPublic))
		baseParams.Method = http.MethodPut
		reqParams := &api.ReqParams{
			BaseParams: baseParams,
			Path:       apc.URLPathDae.Join(apc.SyncSmap),
			Body:       jsonMap,
			Header:     http.Header{cos.HdrContentType: []string{cos.ContentJSON}},
		}
		err = reqParams.DoHTTPRequest()
		tassert.CheckFatal(t, err)
		n--
	}
	killRestorePrimary(t, proxyURL, false, nil)
}

// concurrentPutGetDel does put/get/del sequence against all proxies concurrently
func concurrentPutGetDel(t *testing.T) {
	_ = tutils.RandomProxyURL(t)
	runProviderTests(t, func(t *testing.T, bck *cluster.Bck) {
		proxyURL := tutils.RandomProxyURL(t)
		smap := tutils.GetClusterMap(t, proxyURL)
		tlog.Logf("targets: %d, proxies: %d\n", smap.CountActiveTargets(), smap.CountActiveProxies())

		var (
			wg        = &sync.WaitGroup{}
			errCh     = make(chan error, smap.CountActiveProxies())
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
	baseParams := tutils.BaseAPIParams(proxyURL)
	for i := 0; i < count; i++ {
		reader, err := readers.NewRandReader(fileSize, cksumType)
		if err != nil {
			return fmt.Errorf("error creating reader: %v", err)
		}
		fname := trand.String(20)
		keyname := fmt.Sprintf("%s/%s", localBucketDir, fname)
		putArgs := api.PutObjectArgs{
			BaseParams: baseParams,
			Bck:        bck,
			Object:     keyname,
			Cksum:      reader.Cksum(),
			Reader:     reader,
		}
		if err = api.PutObject(putArgs); err != nil {
			return fmt.Errorf("error executing put: %v", err)
		}
		if _, err = api.GetObject(baseParams, bck, keyname); err != nil {
			return fmt.Errorf("error executing get: %v", err)
		}
		if err = tutils.Del(proxyURL, bck, keyname, nil /* wg */, nil /* errCh */, true /* silent */); err != nil {
			return fmt.Errorf("error executing del: %v", err)
		}
	}

	return nil
}

// putGetDelWorker does put/get/del in sequence; if primary proxy change happens, it checks the failed delete
// channel and route the deletes to the new primary proxy
// stops when told to do so via the stop channel
func putGetDelWorker(proxyURL string, stopCh <-chan struct{}, proxyURLCh <-chan string, errCh chan error,
	wg *sync.WaitGroup) {
	defer wg.Done()

	missedDeleteCh := make(chan string, 100)
	baseParams := tutils.BaseAPIParams(proxyURL)

	bck := cmn.Bck{
		Name:     testBucketName,
		Provider: apc.ProviderAIS,
	}
	cksumType := bck.DefaultProps().Cksum.Type
loop:
	for {
		select {
		case <-stopCh:
			close(errCh)
			break loop

		case url := <-proxyURLCh:
			// send failed deletes to the new primary proxy
		deleteLoop:
			for {
				select {
				case objName := <-missedDeleteCh:
					err := tutils.Del(url, bck, objName, nil, errCh, true)
					if err != nil {
						missedDeleteCh <- objName
					}

				default:
					break deleteLoop
				}
			}

		default:
		}

		reader, err := readers.NewRandReader(fileSize, cksumType)
		if err != nil {
			errCh <- err
			continue
		}

		fname := trand.String(20)
		objName := fmt.Sprintf("%s/%s", localBucketDir, fname)
		putArgs := api.PutObjectArgs{
			BaseParams: baseParams,
			Bck:        bck,
			Object:     objName,
			Cksum:      reader.Cksum(),
			Reader:     reader,
		}
		err = api.PutObject(putArgs)
		if err != nil {
			errCh <- err
			continue
		}
		_, err = api.GetObject(baseParams, bck, objName)
		if err != nil {
			errCh <- err
		}

		err = tutils.Del(proxyURL, bck, objName, nil, errCh, true)
		if err != nil {
			missedDeleteCh <- objName
		}
	}

	// process left over not deleted objects
	close(missedDeleteCh)
	for n := range missedDeleteCh {
		tutils.Del(proxyURL, bck, n, nil, nil, true)
	}
}

// primaryKiller kills primary proxy, notifies all workers, and restores it.
func primaryKiller(t *testing.T, proxyURL string, stopch <-chan struct{}, proxyurlchs []chan string,
	errCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()

loop:
	for {
		select {
		case <-stopch:
			close(errCh)
			for _, ch := range proxyurlchs {
				close(ch)
			}

			break loop

		default:
		}

		postKill := func(smap *cluster.Smap, newPrimary, _ *cluster.Snode) {
			// let the workers go to the dying primary for a little while longer to generate errored requests
			time.Sleep(time.Second)
			for _, ch := range proxyurlchs {
				ch <- newPrimary.URL(cmn.NetPublic)
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
	var (
		config             = tutils.GetClusterConfig(t)
		restoreCmd         = make([]tutils.RestoreCmd, 0, 3)
		configDiscovery, _ = cos.ParseURL(config.Proxy.DiscoveryURL)
		proxyURL           string
		randomKilled       bool
	)

	// Make sure primary is same config
	smap := primarySetToOriginal(t)
	origProxyCnt := smap.CountActiveProxies()
	origTargetCnt := smap.CountActiveTargets()

	for _, si := range smap.Pmap {
		if smap.IsPrimary(si) {
			continue
		}
		publicURL, _ := cos.ParseURL(si.URL(cmn.NetPublic))
		if publicURL.Host == configDiscovery.Host || configDiscovery.Port() == publicURL.Port() {
			cmd, err := tutils.KillNode(si)
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
		cmd, err := tutils.KillNode(si)
		tassert.CheckFatal(t, err)
		restoreCmd = append(restoreCmd, cmd)
		randomKilled = true
	}

	// Kill a random target
	target, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)
	cmd, err := tutils.KillNode(target)
	tassert.CheckFatal(t, err)
	restoreCmd = append(restoreCmd, cmd)

	// Kill original primary
	cmd, err = tutils.KillNode(smap.Primary)
	tassert.CheckFatal(t, err)
	restoreCmd = append(restoreCmd, cmd)

	proxyCnt, targetCnt := origProxyCnt-3, origTargetCnt-1
	smap, err = tutils.WaitForClusterState(proxyURL, "kill proxies and target", smap.Version, proxyCnt, targetCnt)
	tassert.CheckFatal(t, err)

	// Restore all killed nodes
	for _, cmd := range restoreCmd {
		if cmd.Node.IsProxy() {
			proxyCnt++
		}
		if cmd.Node.IsTarget() {
			targetCnt++
		}
		tutils.RestoreNode(cmd, false, cmd.Node.Type())
		_, err = tutils.WaitForClusterState(proxyURL, "restore "+cmd.Node.ID(), smap.Version,
			proxyCnt, targetCnt)
		tassert.CheckError(t, err)
	}

	tutils.WaitForRebalAndResil(t, tutils.BaseAPIParams(proxyURL))
}

// proxyStress starts a group of workers doing put/get/del in sequence against primary proxy,
// while the operations are on going, a separate go routine kills the primary proxy, notifies all
// workers about the proxy change, restart the killed proxy as a non-primary proxy.
// the process is repeated until a pre-defined time duration is reached.
func proxyStress(t *testing.T) {
	var (
		wg          sync.WaitGroup
		errChs      = make([]chan error, workerCnt+1)
		stopChs     = make([]chan struct{}, workerCnt+1)
		proxyURLChs = make([]chan string, workerCnt)
		bck         = cmn.Bck{
			Name:     testBucketName,
			Provider: apc.ProviderAIS,
		}
		proxyURL = tutils.RandomProxyURL(t)
	)

	tutils.CreateBucketWithCleanup(t, proxyURL, bck, nil)
	defer func() {
		err := tutils.WaitNodeReady(proxyURL)
		tassert.CheckFatal(t, err)
	}()

	// start all workers
	for i := 0; i < workerCnt; i++ {
		errChs[i] = make(chan error, defaultChanSize)
		stopChs[i] = make(chan struct{}, defaultChanSize)
		proxyURLChs[i] = make(chan string, defaultChanSize)

		wg.Add(1)
		go putGetDelWorker(proxyURL, stopChs[i], proxyURLChs[i], errChs[i], &wg)

		// stagger the workers so they don't always do the same operation at the same time
		n := cos.NowRand().Intn(999)
		time.Sleep(time.Duration(n+1) * time.Millisecond)
	}

	errChs[workerCnt] = make(chan error, defaultChanSize)
	stopChs[workerCnt] = make(chan struct{}, defaultChanSize)
	wg.Add(1)
	go primaryKiller(t, proxyURL, stopChs[workerCnt], proxyURLChs, errChs[workerCnt], &wg)

	timer := time.After(multiProxyTestTimeout)
loop:
	for {
		for _, ch := range errChs {
			select {
			case <-timer:
				break loop
			case <-ch:
				// Read errors, throw away, this is needed to unblock the workers.
			default:
			}
		}
	}

	// stop all workers
	for _, stopCh := range stopChs {
		stopCh <- struct{}{}
		close(stopCh)
	}

	wg.Wait()
}

// smap 	- current Smap
// directURL	- URL of the proxy that we send the request to (not necessarily the current primary)
// toID, toURL 	- DaemonID and URL of the proxy that must become the new primary
func setPrimaryTo(t *testing.T, proxyURL string, smap *cluster.Smap, directURL, toID string) (newSmap *cluster.Smap) {
	if directURL == "" {
		directURL = smap.Primary.URL(cmn.NetPublic)
	}

	baseParams := tutils.BaseAPIParams(directURL)
	tlog.Logf("Setting primary from %s to %s\n", smap.Primary.ID(), toID)
	err := api.SetPrimaryProxy(baseParams, toID, false /*force*/)
	tassert.CheckFatal(t, err)

	newSmap, err = tutils.WaitForNewSmap(proxyURL, smap.Version)
	tassert.CheckFatal(t, err)
	if newSmap.Primary.ID() != toID {
		t.Fatalf("Expected primary=%s, got %s", toID, newSmap.Primary.ID())
	}
	checkSmaps(t, proxyURL)
	return
}

func chooseNextProxy(smap *cluster.Smap) (proxyid, proxyURL string, err error) {
	pid, err := hrwProxyTest(smap, smap.Primary.ID())
	pi := smap.Pmap[pid]
	if err != nil {
		return
	}

	return pi.ID(), pi.URL(cmn.NetPublic), nil
}

// For each proxy: compare its Smap vs primary(*) and return an error if differs
func checkSmaps(t *testing.T, proxyURL string) {
	var (
		smap1   = tutils.GetClusterMap(t, proxyURL)
		primary = smap1.Primary // primary according to the `proxyURL`(*)
	)
	for _, psi := range smap1.Pmap {
		smap2 := tutils.GetClusterMap(t, psi.URL(cmn.NetPublic))
		uuid, sameOrigin, sameVersion, eq := smap1.Compare(smap2)
		if eq {
			continue
		}
		err := fmt.Errorf("(%s %s, primary=%s) != (%s %s, primary=%s): (uuid=%s, same-orig=%t, same-ver=%t)",
			proxyURL, smap1, primary, psi.URL(cmn.NetPublic), smap2, smap2.Primary, uuid, sameOrigin, sameVersion)
		t.Error(err)
	}
}

// primarySetToOriginal reads original primary proxy from configuration and
// makes it a primary proxy again
// NOTE: This test cannot be run as separate test. It requires that original
// primary proxy was down and retuned back. So, the test should be executed
// after primaryCrashElectRestart test
func primarySetToOriginal(t *testing.T) *cluster.Smap {
	var (
		proxyURL              = tutils.GetPrimaryURL()
		smap                  = tutils.GetClusterMap(t, proxyURL)
		currID                = smap.Primary.ID()
		currURL               = smap.Primary.URL(cmn.NetPublic)
		byURL, byPort, origID string
	)
	if currURL != proxyURL {
		t.Fatalf("Err in the test itself: expecting currURL %s == proxyurl %s", currURL, proxyURL)
	}
	tlog.Logf("Setting primary proxy %s back to the original, Smap version %d\n", currID, smap.Version)

	config := tutils.GetClusterConfig(t)
	proxyconf := config.Proxy
	origURL := proxyconf.OriginalURL

	if origURL == "" {
		t.Fatal("Original primary proxy is not defined in configuration")
	}
	urlparts := strings.Split(origURL, ":")
	proxyPort := urlparts[len(urlparts)-1]

	for key, val := range smap.Pmap {
		if val.URL(cmn.NetPublic) == origURL {
			byURL = key
			break
		}

		keyparts := strings.Split(val.URL(cmn.NetPublic), ":")
		port := keyparts[len(keyparts)-1]
		if port == proxyPort {
			byPort = key
		}
	}
	if byPort == "" && byURL == "" {
		t.Fatalf("No original primary proxy: %v", proxyconf)
	}
	origID = byURL
	if origID == "" {
		origID = byPort
	}
	tlog.Logf("Found original primary ID: %s\n", origID)
	if currID == origID {
		tlog.Logf("Original %s == the current primary: nothing to do\n", origID)
		return smap
	}

	return setPrimaryTo(t, proxyURL, smap, "", origID)
}

// This is duplicated in the tests because the `idDigest` of `daemonInfo` is not
// exported. As a result of this, ais.HrwProxy will not return the correct
// proxy since the `idDigest` will be initialized to 0. To avoid this, we
// compute the checksum directly in this method.
func hrwProxyTest(smap *cluster.Smap, idToSkip string) (pi string, err error) {
	if smap.CountActiveProxies() == 0 {
		err = errors.New("AIStore cluster map is empty: no proxies")
		return
	}
	var (
		max     uint64
		skipped int
	)
	for id, snode := range smap.Pmap {
		if id == idToSkip {
			skipped++
			continue
		}
		if smap.NonElectable(snode) {
			skipped++
			continue
		}

		if smap.PresentInMaint(snode) {
			skipped++
			continue
		}

		cs := xxhash.ChecksumString64S(snode.ID(), cos.MLCG32)
		if cs > max {
			max = cs
			pi = id
		}
	}
	if pi == "" {
		err = fmt.Errorf("cannot HRW-select proxy: current count=%d, skipped=%d",
			smap.CountActiveProxies(), skipped)
	}
	return
}

func networkFailureTarget(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)
	proxyCount, targetCount := smap.CountActiveProxies(), smap.CountActiveTargets()

	tassert.Fatalf(t, targetCount > 0, "At least 1 target required")
	target, _ := smap.GetRandTarget()
	targetID := target.ID()

	tlog.Logf("Disconnecting target: %s\n", targetID)
	oldNetworks, err := docker.Disconnect(targetID)
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForClusterState(
		proxyURL,
		"target is down",
		smap.Version,
		proxyCount,
		targetCount-1,
	)
	tassert.CheckFatal(t, err)

	tlog.Logf("Connecting target %s to networks again\n", targetID)
	err = docker.Connect(targetID, oldNetworks)
	tassert.CheckFatal(t, err)

	_, err = tutils.WaitForClusterState(
		proxyURL,
		"to check cluster state",
		smap.Version,
		proxyCount,
		targetCount,
	)
	tassert.CheckFatal(t, err)
}

func networkFailureProxy(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)
	proxyCount, targetCount := smap.CountActiveProxies(), smap.CountActiveTargets()
	tassert.Fatalf(t, proxyCount > 1, "At least 2 proxy required (has: %d)", proxyCount)

	oldPrimaryID := smap.Primary.ID()
	proxyID, _, err := chooseNextProxy(smap)
	tassert.CheckFatal(t, err)

	tlog.Logf("Disconnecting proxy: %s\n", proxyID)
	oldNetworks, err := docker.Disconnect(proxyID)
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForClusterState(
		proxyURL,
		"proxy is down",
		smap.Version,
		proxyCount-1,
		targetCount,
	)
	tassert.CheckFatal(t, err)

	tlog.Logf("Connecting proxy %s to networks again\n", proxyID)
	err = docker.Connect(proxyID, oldNetworks)
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForClusterState(
		proxyURL,
		"to check cluster state",
		smap.Version,
		proxyCount,
		targetCount,
	)
	tassert.CheckFatal(t, err)

	if oldPrimaryID != smap.Primary.ID() {
		t.Fatalf("Primary proxy changed from %s to %s",
			oldPrimaryID, smap.Primary.ID())
	}
}

func networkFailurePrimary(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)
	if smap.CountActiveProxies() < 2 {
		t.Fatal("At least 2 proxy required")
	}

	proxyCount, targetCount := smap.CountActiveProxies(), smap.CountActiveTargets()
	oldPrimaryID, oldPrimaryURL := smap.Primary.ID(), smap.Primary.URL(cmn.NetPublic)
	newPrimaryID, newPrimaryURL, err := chooseNextProxy(smap)
	tassert.CheckFatal(t, err)

	// Disconnect primary
	tlog.Logf("Disconnecting primary %s from all networks\n", oldPrimaryID)
	oldNetworks, err := docker.Disconnect(oldPrimaryID)
	tassert.CheckFatal(t, err)

	// Check smap
	smap, err = tutils.WaitForClusterState(
		newPrimaryURL,
		"original primary gone",
		smap.Version,
		proxyCount-1,
		targetCount,
	)
	tassert.CheckFatal(t, err)

	if smap.Primary.ID() != newPrimaryID {
		t.Fatalf("wrong primary proxy: %s, expecting: %s after disconnecting",
			smap.Primary.ID(), newPrimaryID)
	}

	// Connect again
	tlog.Logf("Connecting primary %s to networks again\n", oldPrimaryID)
	err = docker.Connect(oldPrimaryID, oldNetworks)
	tassert.CheckFatal(t, err)

	// give a little time to original primary, so it picks up the network
	// connections and starts talking to neighbors
	_, err = tutils.WaitForClusterState(
		oldPrimaryID,
		"original primary is restored",
		smap.Version,
		proxyCount,
		targetCount,
	)
	tassert.CheckFatal(t, err)

	oldSmap := tutils.GetClusterMap(t, oldPrimaryURL)
	// the original primary still thinks that it is the primary, so its smap
	// should not change after the network is back
	if oldSmap.Primary.ID() != oldPrimaryID {
		tlog.Logf("Old primary changed its smap. Its current primary: %s (expected %s - self)\n",
			oldSmap.Primary.ID(), oldPrimaryID)
	}

	// Forcefully set new primary for the original one
	baseParams := tutils.BaseAPIParams(oldPrimaryURL)
	baseParams.Method = http.MethodPut
	reqParams := &api.ReqParams{
		BaseParams: baseParams,
		Path:       apc.URLPathDaeProxy.Join(newPrimaryID),
		Query: url.Values{
			apc.QparamForce:            {"true"},
			apc.QparamPrimaryCandidate: {newPrimaryURL},
		},
	}
	err = reqParams.DoHTTPRequest()
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForClusterState(
		newPrimaryURL,
		"original primary joined the new primary",
		smap.Version,
		proxyCount,
		targetCount,
	)
	tassert.CheckFatal(t, err)

	if smap.Primary.ID() != newPrimaryID {
		t.Fatalf("expected primary=%s, got %s after connecting again", newPrimaryID, smap.Primary.ID())
	}
}

func networkFailure(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{RequiredDeployment: tutils.ClusterTypeDocker})

	t.Run("Target network disconnect", networkFailureTarget)
	t.Run("Secondary proxy network disconnect", networkFailureProxy)
	t.Run("Primary proxy network disconnect", networkFailurePrimary)
}

// primaryAndNextCrash kills the primary proxy and a proxy that should be selected
// after the current primary dies, verifies the second in line proxy becomes
// the new primary, restore all proxies
func primaryAndNextCrash(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)
	origProxyCount := smap.CountActiveProxies()

	if origProxyCount < 4 {
		t.Skip("The test requires at least 4 proxies, found only ", origProxyCount)
	}

	// get next primary
	firstPrimaryID, firstPrimaryURL, err := chooseNextProxy(smap)
	tassert.CheckFatal(t, err)
	// Cluster map is re-read to have a clone of original smap that the test
	// can modify in any way it needs. Because original smap got must be preserved
	smapNext := tutils.GetClusterMap(t, proxyURL)
	// get next next primary
	firstPrimary := smapNext.Pmap[firstPrimaryID]
	delete(smapNext.Pmap, firstPrimaryID)
	finalPrimaryID, finalPrimaryURL, err := chooseNextProxy(smapNext)
	tassert.CheckFatal(t, err)

	// kill the current primary
	oldPrimaryURL, oldPrimaryID := smap.Primary.URL(cmn.NetPublic), smap.Primary.ID()
	tlog.Logf("Killing primary proxy: %s - %s\n", oldPrimaryURL, oldPrimaryID)
	cmdFirst, err := tutils.KillNode(smap.Primary)
	tassert.CheckFatal(t, err)

	// kill the next primary
	tlog.Logf("Killing next to primary proxy: %s - %s\n", firstPrimaryID, firstPrimaryURL)
	cmdSecond, errSecond := tutils.KillNode(firstPrimary)
	// if kill fails it does not make sense to wait for the cluster is stable
	if errSecond == nil {
		// the cluster should vote, so the smap version should be increased at
		// least by 100, that is why +99
		smap, err = tutils.WaitForClusterState(finalPrimaryURL, "designate new primary",
			smap.Version+99, origProxyCount-2, 0)
		tassert.CheckFatal(t, err)
	}

	tlog.Logf("Checking current primary, %s\n", smap.StringEx())
	if smap.Primary.ID() != finalPrimaryID {
		t.Errorf("Expected primary %s but real primary is %s", finalPrimaryID, smap.Primary.ID())
	}

	// restore next and prev primaries in the reversed order
	err = tutils.RestoreNode(cmdSecond, false, "proxy (next primary)")
	tassert.CheckFatal(t, err)
	smap, err = tutils.WaitForClusterState(finalPrimaryURL, "restore next primary",
		smap.Version, origProxyCount-1, 0)
	tassert.CheckFatal(t, err)

	err = tutils.RestoreNode(cmdFirst, false, "proxy (prev primary)")
	tassert.CheckFatal(t, err)
	_, err = tutils.WaitForClusterState(finalPrimaryURL, "restore prev primary",
		smap.Version, origProxyCount, 0)
	tassert.CheckFatal(t, err)
}

func TestIC(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true, RequiredDeployment: tutils.ClusterTypeLocal})

	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)
	if cnt := smap.CountActiveProxies(); cnt < 4 {
		t.Fatalf("Not enough proxies (%d) to run tests (must be at least 4)", cnt)
	}

	defer tutils.EnsureOrigClusterState(t)
	for _, test := range icTests {
		t.Run(test.name, test.method)
		if t.Failed() {
			t.FailNow()
		}
	}
	time.Sleep(time.Second)
}

func killRandNonPrimaryIC(t testing.TB, smap *cluster.Smap) (tutils.RestoreCmd, *cluster.Smap) {
	origProxyCount := smap.CountActiveProxies()
	primary := smap.Primary
	var killNode *cluster.Snode
	for _, psi := range smap.Pmap {
		if smap.IsIC(psi) && !psi.Equals(primary) {
			killNode = psi
			break
		}
	}
	cmd, err := tutils.KillNode(killNode)
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForClusterState(primary.URL(cmn.NetPublic), "propagate new Smap",
		smap.Version, origProxyCount-1, 0)
	tassert.CheckError(t, err)
	return cmd, smap
}

func icFromSmap(smap *cluster.Smap) cos.StringSet {
	lst := make(cos.StringSet, smap.DefaultICSize())
	for pid, psi := range smap.Pmap {
		if smap.IsIC(psi) {
			lst.Add(pid)
		}
	}
	return lst
}

func icMemberLeaveAndRejoin(t *testing.T) {
	smap := tutils.GetClusterMap(t, proxyURL)
	primary := smap.Primary
	tassert.Fatalf(t, smap.ICCount() == smap.DefaultICSize(),
		"should have %d members in IC, has %d", smap.DefaultICSize(), smap.ICCount())

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
	tassert.Errorf(t, smap.ICCount() == smap.DefaultICSize(), "should have %d members in IC, has %d",
		smap.DefaultICSize(), smap.ICCount())

	err := tutils.RestoreNode(cmd, false, "proxy")
	tassert.CheckFatal(t, err)

	updatedICs := icFromSmap(smap)
	smap, err = tutils.WaitNodeAdded(tutils.BaseAPIParams(primary.URL(cmn.NetPublic)), cmd.Node.ID())
	tassert.CheckFatal(t, err)

	// Adding a new node shouldn't change IC members.
	newIC := icFromSmap(smap)
	tassert.Errorf(t, reflect.DeepEqual(updatedICs, newIC), "shouldn't update existing IC members")
}

func icKillAndRestorePrimary(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	var (
		proxyURL   = tutils.RandomProxyURL(t)
		smap       = tutils.GetClusterMap(t, proxyURL)
		oldIC      = icFromSmap(smap)
		oldPrimary = smap.Primary
	)

	icCheck := func(smap *cluster.Smap, newPrimary, oldPrimary *cluster.Snode) {
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
	tassert.Errorf(t, smap.ICCount() == smap.DefaultICSize(),
		"should have %d members in IC, has %d", smap.DefaultICSize(), smap.ICCount())
}

func icSyncOwnershipTable(t *testing.T) {
	var (
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)
		smap       = tutils.GetClusterMap(t, proxyURL)
		primary    = smap.Primary

		src = cmn.Bck{
			Name:     testBucketName,
			Provider: apc.ProviderAIS,
		}

		dstBck = cmn.Bck{
			Name:     testBucketName + "_new",
			Provider: apc.ProviderAIS,
		}
	)

	tutils.CreateBucketWithCleanup(t, proxyURL, src, nil)

	// Start any xaction and get ID.
	xactID, err := api.CopyBucket(baseParams, src, dstBck, nil)
	tassert.CheckFatal(t, err)
	defer tutils.DestroyBucket(t, proxyURL, dstBck)

	// Killing an IC member, should add a new IC member.
	// Select IC member which is not primary and kill.
	origIC := icFromSmap(smap)
	cmd, smap := killRandNonPrimaryIC(t, smap)

	// Try getting xaction status from new IC member.
	updatedIC := icFromSmap(smap)
	newICMemID := getNewICMember(t, origIC, updatedIC)

	newICNode := smap.GetProxy(newICMemID)

	baseParams = tutils.BaseAPIParams(newICNode.URL(cmn.NetPublic))
	xactArgs := api.XactReqArgs{ID: xactID, Kind: apc.ActCopyBck}
	_, err = api.GetXactionStatus(baseParams, xactArgs)
	tassert.CheckError(t, err)

	err = tutils.RestoreNode(cmd, false, "proxy")
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitNodeAdded(baseParams, cmd.Node.ID())
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, !smap.IsIC(cmd.Node), "newly joined node shouldn't be in IC (%s)", cmd.Node)

	// Should sync ownership table when non-ic member become primary.
	smap = setPrimaryTo(t, primary.URL(cmn.NetPublic), smap, "", cmd.Node.ID())
	tassert.Fatalf(t, smap.IsIC(cmd.Node), "primary (%s) should be a IC member, (were: %s)", primary, smap.StrIC(primary))

	baseParams = tutils.BaseAPIParams(cmd.Node.URL(cmn.NetPublic))
	_, err = api.GetXactionStatus(baseParams, xactArgs)
	tassert.CheckError(t, err)
}

func icSinglePrimaryRevamp(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	var (
		proxyURL       = tutils.RandomProxyURL(t)
		smap           = tutils.GetClusterMap(t, proxyURL)
		origProxyCount = smap.CountActiveProxies()

		src = cmn.Bck{
			Name:     testBucketName,
			Provider: apc.ProviderAIS,
		}

		dstBck = cmn.Bck{
			Name:     testBucketName + "_new",
			Provider: apc.ProviderAIS,
		}
	)

	nodesToRestore := make([]tutils.RestoreCmd, 0, origProxyCount-1)

	// Kill all nodes except primary.
	for i := origProxyCount; i > 1; i-- {
		var cmd tutils.RestoreCmd
		cmd, smap = killRandNonPrimaryIC(t, smap)
		nodesToRestore = append(nodesToRestore, cmd)
	}

	proxyURL = smap.Primary.URL(cmn.NetPublic)
	baseParams = tutils.BaseAPIParams(proxyURL)
	tutils.CreateBucketWithCleanup(t, proxyURL, src, nil)

	// Start any xaction and get ID.
	xactID, err := api.CopyBucket(baseParams, src, dstBck, nil)
	xactArgs := api.XactReqArgs{ID: xactID, Kind: apc.ActCopyBck}

	tassert.CheckFatal(t, err)
	defer tutils.DestroyBucket(t, proxyURL, dstBck)

	// Restart all killed nodes and check for xaction status.
	for _, cmd := range nodesToRestore {
		err = tutils.RestoreNode(cmd, false, "proxy")
		tassert.CheckError(t, err)

		smap, err = tutils.WaitForClusterState(proxyURL,
			"restore node "+cmd.Node.ID(), smap.Version,
			smap.CountActiveProxies()+1, smap.CountTargets())
		tassert.CheckFatal(t, err)

		baseParams = tutils.BaseAPIParams(cmd.Node.URL(cmn.NetPublic))
		_, err = api.GetXactionStatus(baseParams, xactArgs)
		tassert.CheckError(t, err)
	}
}

func icStressMonitorXactMultiICFail(t *testing.T) {
	var (
		proxyURL = tutils.GetPrimaryURL()
		smap     = tutils.GetClusterMap(t, proxyURL)

		m = ioContext{
			t:        t,
			num:      1000,
			fileSize: 50 * cos.KiB,
		}
		numCopyXacts = 20
	)

	// 1. Populate a bucket required for copy xactions
	m.initWithCleanup()
	tutils.CreateBucketWithCleanup(t, proxyURL, m.bck, nil)
	m.puts()

	// 2. Kill and restore random IC members in background
	stopCh := cos.NewStopCh()
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

func icStressCachedXactions(t *testing.T) {
	// TODO -- FIXME: to stress test xactions need list-objects notifications (ref. #922)
	t.Skipf("skipping %s (currently, list-objects does not generate IC notifications)", t.Name())

	var (
		m = ioContext{
			t:        t,
			num:      5000,
			fileSize: cos.KiB,
		}

		proxyURL        = tutils.GetPrimaryURL()
		baseParams      = tutils.BaseAPIParams(proxyURL)
		smap            = tutils.GetClusterMap(t, proxyURL)
		numListObjXacts = 20 // number of list obj xactions to run in parallel
	)

	m.initWithCleanup()
	m.puts()

	// 2. Kill and restore random IC members in background
	stopCh := cos.NewStopCh()
	krWg := &sync.WaitGroup{}
	krWg.Add(1)
	go killRestoreIC(t, smap, stopCh, krWg)
	defer func() {
		// Stop the background kill and restore task
		stopCh.Close()
		krWg.Wait()
	}()

	// 3. Start multiple list obj range operation in background
	wg := startListObjRange(t, baseParams, m.bck, numListObjXacts, m.num, 500, 10)
	wg.Wait()
}

// Expects objects to be numbered as {%04d}; BaseParams of primary proxy
//
//nolint:unused // will be used when icStressCachedXaction test is enabled
func startListObjRange(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, numJobs, numObjs, rangeSize int,
	pageSize uint) *sync.WaitGroup {
	tassert.Fatalf(t, numObjs > rangeSize,
		"number of objects (%d) should be greater than range size (%d)", numObjs, rangeSize)
	wg := &sync.WaitGroup{}
	for i := 0; i < numJobs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Start list object xactions with a small lag
			time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
			var (
				after = fmt.Sprintf("%04d", rand.Intn(numObjs-1-rangeSize))
				msg   = &apc.ListObjsMsg{PageSize: pageSize, StartAfter: after}
			)

			resList, err := api.ListObjects(baseParams, bck, msg, uint(rangeSize))
			if err == nil {
				tassert.Errorf(t, len(resList.Entries) == rangeSize, "should list %d objects", rangeSize)
				return
			}
			if cmn.IsStatusBadGateway(err) {
				// TODO : handle cache owner getting killed
				return
			}
			tassert.Errorf(t, err == nil, "List objects %s failed, err = %v", bck, err)
		}()
	}
	return wg
}

func startCPBckAndWait(t testing.TB, srcBck cmn.Bck, count int) *sync.WaitGroup {
	var (
		proxyURL   = tutils.GetPrimaryURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		wg         = &sync.WaitGroup{}
	)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			dstBck := cmn.Bck{
				Name:     fmt.Sprintf("%s_dst_par_%d", testBucketName, idx),
				Provider: apc.ProviderAIS,
			}
			xactID, err := api.CopyBucket(baseParams, srcBck, dstBck, nil)
			tassert.CheckError(t, err)
			defer func() {
				tutils.DestroyBucket(t, proxyURL, dstBck)
				wg.Done()
			}()
			xargs := api.XactReqArgs{ID: xactID, Timeout: rebalanceTimeout}
			_, err = api.WaitForXactionIC(baseParams, xargs)
			tassert.CheckError(t, err)
		}(i)
	}
	return wg
}

// Continuously kill and restore IC nodes
func killRestoreIC(t *testing.T, smap *cluster.Smap, stopCh *cos.StopCh, wg *sync.WaitGroup) {
	var (
		cmd      tutils.RestoreCmd
		proxyURL = smap.Primary.URL(cmn.NetPublic)
	)
	defer wg.Done()

	for {
		cmd, smap = killRandNonPrimaryIC(t, smap)
		err := tutils.RestoreNode(cmd, false, "proxy")
		tassert.CheckFatal(t, err)

		smap, err = tutils.WaitForClusterState(proxyURL, "restore", smap.Version, 0, 0)
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

func getNewICMember(t testing.TB, oldMap, newMap cos.StringSet) (daeID string) {
	for sid := range newMap {
		if _, ok := oldMap[sid]; !ok {
			tassert.Errorf(t, daeID == "", "should change only one IC member")
			daeID = sid
		}
	}
	tassert.Fatalf(t, daeID != "", "should change at least one IC member")
	return
}
