// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/docker"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
)

// Intended for a deployment with multiple targets
// 1. Create ais bucket
// 2. Unregister target T
// 3. PUT large amount of objects into the ais bucket
// 4. GET the objects while simultaneously registering the target T
func TestGetAndReRegisterInParallel(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})
	var (
		m = ioContext{
			t:               t,
			num:             50000,
			numGetsEachFile: 3,
			fileSize:        10 * cos.KiB,
		}
		rebID string
	)

	m.initWithCleanupAndSaveState()
	m.expectTargets(2)

	// Step 1.
	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	// Step 2.
	target := m.startMaintenanceNoRebalance()

	// Step 3.
	m.puts()

	// Step 4.
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		// without defer, if gets crashes Done is not called resulting in test hangs
		defer wg.Done()
		m.gets()
	}()

	time.Sleep(time.Second * 3) // give gets some room to breathe
	go func() {
		// without defer, if reregister crashes Done is not called resulting in test hangs
		defer wg.Done()
		rebID = m.stopMaintenance(target)
	}()
	wg.Wait()

	m.ensureNoGetErrors()
	m.waitAndCheckCluState()
	if rebID != "" {
		tools.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID)
	}
}

// All of the above PLUS proxy failover/failback sequence in parallel:
// 1. Create an ais bucket
// 2. Unregister a target
// 3. Crash the primary proxy and PUT in parallel
// 4. Failback to the original primary proxy, register target, and GET in parallel
func TestProxyFailbackAndReRegisterInParallel(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true, MinTargets: 2, MinProxies: 3})
	m := ioContext{
		t:                   t,
		otherTasksToTrigger: 1,
		num:                 150000,
	}

	m.initWithCleanupAndSaveState()

	// Step 1.
	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	// Step 2.
	target := m.startMaintenanceNoRebalance()

	// Step 3.
	_, newPrimaryURL, err := chooseNextProxy(m.smap)
	tassert.CheckFatal(t, err)
	// use a new proxyURL because primaryCrashElectRestart has a side-effect:
	// it changes the primary proxy. Without the change tools.PutRandObjs is
	// failing while the current primary is restarting and rejoining
	m.proxyURL = newPrimaryURL

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		killRestorePrimary(t, m.proxyURL, false, nil)
	}()

	// delay PUTs to ensure they run during primary elections
	time.Sleep(5 * time.Second)
	m.puts()
	wg.Wait()

	// Step 4: (three tasks)
	wg.Add(3)
	go func() {
		defer wg.Done()
		m.stopMaintenance(target)
	}()
	go func() {
		defer wg.Done()
		m.gets()
	}()
	go func() {
		defer wg.Done()
		<-m.controlCh // <-- half GETs
		primarySetToOriginal(t)
	}()
	wg.Wait()

	xactArgs := api.XactReqArgs{Kind: apc.ActRebalance, OnlyRunning: true, Timeout: rebalanceTimeout}
	_, _ = api.WaitForXactionIC(baseParams, xactArgs)

	// Step 5.
	m.ensureNoGetErrors()
	m.waitAndCheckCluState()
}

// Similar to TestGetAndReRegisterInParallel, but instead of unregister, we kill the target
// 1. Kill registered target and wait for Smap to updated
// 2. Create ais bucket
// 3. PUT large amounts of objects into ais bucket
// 4. Get the objects while simultaneously registering the target
func TestGetAndRestoreInParallel(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true, RequiredDeployment: tools.ClusterTypeLocal})

	var (
		m = ioContext{
			t:               t,
			num:             20000,
			numGetsEachFile: 5,
			fileSize:        cos.KiB * 2,
		}
		targetNode *cluster.Snode
	)

	m.initWithCleanupAndSaveState()
	m.expectTargets(3)

	// Step 1
	// Select a random target
	targetNode, _ = m.smap.GetRandTarget()
	tlog.Logf("Killing %s\n", targetNode.StringEx())
	tcmd, err := tools.KillNode(targetNode)
	tassert.CheckFatal(t, err)

	proxyURL := tools.RandomProxyURL(t)
	m.smap, err = tools.WaitForClusterState(proxyURL, "target removed", m.smap.Version, m.originalProxyCount,
		m.originalTargetCount-1)
	tassert.CheckError(t, err)

	// Step 2
	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	// Step 3
	m.puts()

	// Step 4
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		time.Sleep(4 * time.Second)
		tools.RestoreNode(tcmd, false, "target")
	}()
	go func() {
		defer wg.Done()
		m.gets()
	}()
	wg.Wait()

	m.ensureNoGetErrors()
	m.waitAndCheckCluState()
	tools.WaitForRebalAndResil(m.t, tools.BaseAPIParams(m.proxyURL))
}

func TestUnregisterPreviouslyUnregisteredTarget(t *testing.T) {
	m := ioContext{t: t}
	m.initWithCleanupAndSaveState()
	m.expectTargets(1)
	target := m.startMaintenanceNoRebalance()

	// Decommission the same target again.
	args := &apc.ActValRmNode{DaemonID: target.ID(), SkipRebalance: true}
	_, err := api.StartMaintenance(tools.BaseAPIParams(m.proxyURL), args)
	tassert.Errorf(t, err != nil, "error expected")

	n := tools.GetClusterMap(t, m.proxyURL).CountActiveTs()
	if n != m.originalTargetCount-1 {
		t.Fatalf("expected %d targets after putting target in maintenance, got %d targets",
			m.originalTargetCount-1, n)
	}

	// Register target (bring cluster to normal state)
	rebID := m.stopMaintenance(target)
	m.waitAndCheckCluState()
	tools.WaitForRebalanceByID(m.t, m.originalTargetCount, tools.BaseAPIParams(m.proxyURL), rebID)
}

func TestRegisterAndUnregisterTargetAndPutInParallel(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	m := ioContext{
		t:   t,
		num: 10000,
	}

	m.initWithCleanupAndSaveState()
	m.expectTargets(3)

	targets := m.smap.Tmap.ActiveNodes()

	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	// Unregister target[0]
	args := &apc.ActValRmNode{DaemonID: targets[0].ID(), SkipRebalance: true}
	baseParams := tools.BaseAPIParams(m.proxyURL)
	_, err := api.StartMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)
	tools.WaitForClusterState(
		m.proxyURL,
		"put target in maintenance",
		m.smap.Version,
		m.originalProxyCount,
		m.originalTargetCount-1,
	)

	n := tools.GetClusterMap(t, m.proxyURL).CountActiveTs()
	if n != m.originalTargetCount-1 {
		t.Fatalf("expected %d targets after putting target in maintenance, got %d targets",
			m.originalTargetCount-1, n)
	}

	// Do puts in parallel
	wg := &sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		m.puts()
	}()

	// Register target 0 in parallel
	go func() {
		defer wg.Done()
		args := &apc.ActValRmNode{DaemonID: targets[0].ID()}
		tlog.Logf("Take %s out of maintenance\n", targets[0].StringEx())
		_, err = api.StopMaintenance(baseParams, args)
		tassert.CheckFatal(t, err)
	}()

	// Decommission target[1] in parallel
	go func() {
		defer wg.Done()
		args := &apc.ActValRmNode{DaemonID: targets[1].ID(), SkipRebalance: true}
		_, err = api.StartMaintenance(baseParams, args)
		tassert.CheckFatal(t, err)
	}()

	// Wait for everything to end
	wg.Wait()

	// Register target 1 to bring cluster to original state
	rebID := m.stopMaintenance(targets[1])

	// wait for rebalance to complete
	tools.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID, rebalanceTimeout)

	m.waitAndCheckCluState()
}

func TestAckRebalance(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	m := ioContext{
		t:             t,
		num:           30000,
		getErrIsFatal: true,
	}

	m.initWithCleanupAndSaveState()
	m.expectTargets(3)

	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	target := m.startMaintenanceNoRebalance()

	// Start putting files into bucket.
	m.puts()

	rebID := m.stopMaintenance(target)

	// Wait for everything to finish.
	baseParams := tools.BaseAPIParams(m.proxyURL)
	tools.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID, rebalanceTimeout)

	m.gets()

	m.ensureNoGetErrors()
	m.waitAndCheckCluState()
}

func TestStressRebalance(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	m := &ioContext{
		t: t,
	}

	m.initWithCleanupAndSaveState()
	m.expectTargets(4)

	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	for i := 1; i <= 3; i++ {
		tlog.Logf("Iteration #%d ======\n", i)
		testStressRebalance(t, m.bck)
	}
}

func testStressRebalance(t *testing.T, bck cmn.Bck) {
	m := &ioContext{
		t:             t,
		bck:           bck,
		num:           50000,
		getErrIsFatal: true,
	}

	m.initWithCleanupAndSaveState()

	tgts := m.smap.Tmap.ActiveNodes()
	i1 := rand.Intn(len(tgts))
	i2 := (i1 + 1) % len(tgts)
	target1, target2 := tgts[i1], tgts[i2]

	// Unregister targets.
	tlog.Logf("Killing %s and %s\n", target1.StringEx(), target2.StringEx())
	cmd1, err := tools.KillNode(target1)
	tassert.CheckFatal(t, err)
	time.Sleep(time.Second)
	cmd2, err := tools.KillNode(target2)
	tassert.CheckFatal(t, err)

	// Start putting objects into bucket
	m.puts()

	// Get objects and register targets in parallel
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.gets()
	}()

	// and join 2 targets in parallel
	time.Sleep(time.Second)
	err = tools.RestoreNode(cmd1, false, "the 1st target")
	tassert.CheckFatal(t, err)

	// random sleep between the first and the second join
	time.Sleep(time.Duration(rand.Intn(3)+1) * time.Second)

	err = tools.RestoreNode(cmd2, false, "the 2nd target")
	tassert.CheckFatal(t, err)

	_, err = tools.WaitForClusterState(
		m.proxyURL,
		"targets to join",
		m.smap.Version,
		m.originalProxyCount,
		m.originalTargetCount,
	)
	tassert.CheckFatal(m.t, err)

	// wait for the rebalance to finish
	baseParams := tools.BaseAPIParams(m.proxyURL)
	tools.WaitForRebalAndResil(t, baseParams, rebalanceTimeout)

	// wait for the reads to run out
	wg.Wait()

	m.ensureNoGetErrors()
	m.waitAndCheckCluState()
}

func TestRebalanceAfterUnregisterAndReregister(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})
	m := ioContext{
		t:   t,
		num: 10000,
	}
	m.initWithCleanupAndSaveState()
	m.expectTargets(3)

	targets := m.smap.Tmap.ActiveNodes()

	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	target0, target1 := targets[0], targets[1]
	args := &apc.ActValRmNode{DaemonID: target0.ID(), SkipRebalance: true}
	baseParams := tools.BaseAPIParams(m.proxyURL)
	_, err := api.StartMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)

	_, err = tools.WaitForClusterState(
		m.proxyURL,
		"put target in maintenance",
		m.smap.Version,
		m.originalProxyCount,
		m.originalTargetCount-1,
	)
	tassert.CheckFatal(m.t, err)

	// Put some files
	m.puts()

	// Register target 0 in parallel
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		tlog.Logf("Take %s out of maintenance\n", target0.StringEx())
		args := &apc.ActValRmNode{DaemonID: target0.ID()}
		_, err = api.StopMaintenance(baseParams, args)
		tassert.CheckFatal(t, err)
	}()

	// Unregister target 1 in parallel
	go func() {
		defer wg.Done()
		err = tools.RemoveNodeFromSmap(m.proxyURL, target1.ID())
		tassert.CheckFatal(t, err)
	}()

	// Wait for everything to end
	wg.Wait()

	// Register target 1 to bring cluster to original state
	sleep := time.Duration(rand.Intn(5))*time.Second + time.Millisecond
	time.Sleep(sleep)
	tlog.Logf("Join %s back\n", target1.StringEx())
	rebID, err := tools.JoinCluster(m.proxyURL, target1)
	tassert.CheckFatal(t, err)
	_, err = tools.WaitForClusterState(
		m.proxyURL,
		"targets to join",
		m.smap.Version,
		m.originalProxyCount,
		m.originalTargetCount,
	)
	tassert.CheckFatal(m.t, err)

	tlog.Logf("Wait for rebalance (%q?)...\n", rebID)
	time.Sleep(sleep)
	tools.WaitForRebalAndResil(t, baseParams, rebalanceTimeout)

	m.gets()

	m.ensureNoGetErrors()
	m.waitAndCheckCluState()
}

func TestPutDuringRebalance(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	m := ioContext{
		t:   t,
		num: 10000,
	}

	m.initWithCleanupAndSaveState()
	m.expectTargets(3)

	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	target := m.startMaintenanceNoRebalance()

	// Start putting files and register target in parallel.
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.puts()
	}()

	// Sleep some time to wait for PUT operations to begin.
	time.Sleep(3 * time.Second)

	rebID := m.stopMaintenance(target)

	// Wait for everything to finish.
	wg.Wait()
	baseParams := tools.BaseAPIParams(m.proxyURL)
	tools.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID, rebalanceTimeout)

	// Main check - try to read all objects.
	m.gets()

	m.checkObjectDistribution(t)
	m.waitAndCheckCluState()
}

func TestGetDuringLocalAndGlobalRebalance(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	var (
		m = ioContext{
			t:               t,
			num:             10000,
			numGetsEachFile: 3,
		}
		baseParams     = tools.BaseAPIParams()
		selectedTarget *cluster.Snode
		killTarget     *cluster.Snode
	)

	m.initWithCleanupAndSaveState()
	m.expectTargets(2)

	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	// Select a random target to disable one of its mountpaths,
	// and another random target to unregister.
	for _, target := range m.smap.Tmap {
		if selectedTarget == nil {
			selectedTarget = target
		} else {
			killTarget = target
			break
		}
	}
	mpList, err := api.GetMountpaths(baseParams, selectedTarget)
	tassert.CheckFatal(t, err)
	ensureNoDisabledMountpaths(t, selectedTarget, mpList)

	if len(mpList.Available) < 2 {
		t.Fatalf("Must have at least 2 mountpaths")
	}

	// Disable mountpaths temporarily
	mpath := mpList.Available[0]
	tlog.Logf("Disable mountpath at target %s\n", selectedTarget.ID())
	err = api.DisableMountpath(baseParams, selectedTarget, mpath, false /*dont-resil*/)
	tassert.CheckFatal(t, err)

	args := &apc.ActValRmNode{DaemonID: killTarget.ID(), SkipRebalance: true}
	_, err = api.StartMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)
	smap, err := tools.WaitForClusterState(
		m.proxyURL,
		"target removal",
		m.smap.Version,
		m.originalProxyCount,
		m.originalTargetCount-1,
	)
	tassert.CheckFatal(m.t, err)

	m.puts()

	// Start getting objects
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.gets()
	}()

	// Let's give gets some momentum
	time.Sleep(time.Second * 4)

	// register a new target
	args = &apc.ActValRmNode{DaemonID: killTarget.ID()}
	_, err = api.StopMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)

	// enable mountpath
	err = api.EnableMountpath(baseParams, selectedTarget, mpath)
	tassert.CheckFatal(t, err)

	// wait until GETs are done while 2 rebalance are running
	wg.Wait()

	// make sure that the cluster has all targets enabled
	_, err = tools.WaitForClusterState(
		m.proxyURL,
		"join target back",
		smap.Version,
		m.originalProxyCount,
		m.originalTargetCount,
	)
	tassert.CheckFatal(m.t, err)

	// wait for rebalance to complete
	baseParams = tools.BaseAPIParams(m.proxyURL)
	tools.WaitForRebalAndResil(t, baseParams, rebalanceTimeout) // TODO -- FIXME: revise

	m.ensureNoGetErrors()
	m.waitAndCheckCluState()
	m.ensureNumMountpaths(selectedTarget, mpList)
}

func TestGetDuringResilver(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	var (
		m = ioContext{
			t:   t,
			num: 20000,
		}
		baseParams = tools.BaseAPIParams()
	)

	m.initWithCleanupAndSaveState()
	m.expectTargets(1)

	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	target, _ := m.smap.GetRandTarget()
	mpList, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)
	ensureNoDisabledMountpaths(t, target, mpList)

	if len(mpList.Available) < 2 {
		t.Fatalf("Must have at least 2 mountpaths")
	}

	// select up to 2 mountpath
	mpaths := []string{mpList.Available[0]}
	if len(mpList.Available) > 2 {
		mpaths = append(mpaths, mpList.Available[1])
	}

	// Disable mountpaths temporarily
	for _, mp := range mpaths {
		err = api.DisableMountpath(baseParams, target, mp, false /*dont-resil*/)
		tassert.CheckFatal(t, err)
	}

	m.puts()

	// Start getting objects and enable mountpaths in parallel
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.getsUntilStop()
	}()

	for _, mp := range mpaths {
		time.Sleep(time.Second)
		err = api.EnableMountpath(baseParams, target, mp)
		tassert.CheckFatal(t, err)
	}
	m.stopGets()

	wg.Wait()
	time.Sleep(2 * time.Second)

	tlog.Logf("Wait for rebalance (when target %s that has previously lost all mountpaths joins back)\n", target.StringEx())
	args := api.XactReqArgs{Kind: apc.ActRebalance, Timeout: rebalanceTimeout}
	_, _ = api.WaitForXactionIC(baseParams, args)

	tools.WaitForResilvering(t, baseParams, nil)

	m.ensureNoGetErrors()
	m.ensureNumMountpaths(target, mpList)
}

func TestGetDuringRebalance(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	m := ioContext{
		t:   t,
		num: 30000,
	}

	m.initWithCleanupAndSaveState()
	m.expectTargets(3)

	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	target := m.startMaintenanceNoRebalance()

	m.puts()

	// Start getting objects and register target in parallel.
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.gets()
	}()

	rebID := m.stopMaintenance(target)

	// Wait for everything to finish.
	baseParams := tools.BaseAPIParams(m.proxyURL)
	tools.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID, rebalanceTimeout)
	wg.Wait()

	// Get objects once again to check if they are still accessible after rebalance.
	m.gets()

	m.ensureNoGetErrors()
	m.waitAndCheckCluState()
}

func TestRegisterTargetsAndCreateBucketsInParallel(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	const (
		unregisterTargetCount = 2
		newBucketCount        = 3
	)

	m := ioContext{
		t: t,
	}

	m.initWithCleanupAndSaveState()
	m.expectTargets(3)

	targets := m.smap.Tmap.ActiveNodes()
	baseParams := tools.BaseAPIParams(m.proxyURL)

	// Decommission targets
	for i := 0; i < unregisterTargetCount; i++ {
		args := &apc.ActValRmNode{DaemonID: targets[i].ID(), SkipRebalance: true}
		_, err := api.StartMaintenance(baseParams, args)
		tassert.CheckError(t, err)
	}
	tools.WaitForClusterState(
		m.proxyURL,
		"remove targets",
		m.smap.Version,
		m.originalProxyCount,
		m.originalTargetCount-unregisterTargetCount,
	)

	wg := &sync.WaitGroup{}
	wg.Add(unregisterTargetCount)
	for i := 0; i < unregisterTargetCount; i++ {
		go func(number int) {
			defer wg.Done()
			args := &apc.ActValRmNode{DaemonID: targets[number].ID()}
			_, err := api.StopMaintenance(baseParams, args)
			tassert.CheckError(t, err)
		}(i)
	}

	wg.Add(newBucketCount)
	for i := 0; i < newBucketCount; i++ {
		bck := m.bck
		bck.Name += strconv.Itoa(i)

		go func() {
			defer wg.Done()
			tools.CreateBucketWithCleanup(t, m.proxyURL, bck, nil)
		}()
	}
	wg.Wait()
	m.waitAndCheckCluState()
	tools.WaitForRebalAndResil(t, baseParams, rebalanceTimeout)
}

func TestMountpathDetachAll(t *testing.T) {
	if true {
		t.Skipf("skipping %s", t.Name()) // TODO -- FIXME: add back, here and elsewhere
	}
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true, MinTargets: 2})

	var (
		m = ioContext{
			t:               t,
			num:             5000,
			numGetsEachFile: 2,
		}
		baseParams = tools.BaseAPIParams()
	)

	m.initWithCleanupAndSaveState()
	m.expectTargets(2)

	target, _ := m.smap.GetRandTarget()
	tname := target.StringEx()
	origMountpaths, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)
	ensureNoDisabledMountpaths(t, target, origMountpaths)

	// Remove all mountpaths on the target
	for _, mpath := range origMountpaths.Available {
		err = api.DetachMountpath(baseParams, target, mpath, false /*dont-resil*/)
		tassert.CheckFatal(t, err)
	}

	time.Sleep(time.Second)
	tlog.Logf("Wait for rebalance (triggered by %s leaving the cluster after having lost all mountpaths)\n", tname)
	args := api.XactReqArgs{Kind: apc.ActRebalance, Timeout: rebalanceTimeout}
	_, _ = api.WaitForXactionIC(baseParams, args)

	// Check if mountpaths were actually removed
	mountpaths, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)

	if len(mountpaths.Available) != 0 {
		t.Fatalf("%s should not have any paths available: %d", tname, len(mountpaths.Available))
	}

	// Create ais bucket
	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	// Add target mountpath again
	for _, mpath := range origMountpaths.Available {
		err = api.AttachMountpath(baseParams, target, mpath, false /*force*/)
		tassert.CheckFatal(t, err)
	}

	time.Sleep(2 * time.Second)
	tlog.Logf("Wait for rebalance (when target %s that has previously lost all mountpaths joins back)\n", target.StringEx())
	args = api.XactReqArgs{Kind: apc.ActRebalance, Timeout: rebalanceTimeout}
	_, _ = api.WaitForXactionIC(baseParams, args)

	tools.WaitForResilvering(t, baseParams, target)

	// random read/write
	m.puts()
	m.gets()

	m.ensureNoGetErrors()
	m.ensureNumMountpaths(target, origMountpaths)
}

func TestResilverAfterAddingMountpath(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})
	var (
		m = ioContext{
			t:               t,
			num:             5000,
			numGetsEachFile: 2,
		}
		baseParams = tools.BaseAPIParams()
	)

	m.initWithCleanupAndSaveState()
	m.expectTargets(1)
	target, _ := m.smap.GetRandTarget()
	mpList, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)
	ensureNoDisabledMountpaths(t, target, mpList)

	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	if docker.IsRunning() {
		err := docker.CreateMpathDir(0, testMpath)
		tassert.CheckFatal(t, err)
	} else {
		err := cos.CreateDir(testMpath)
		tassert.CheckFatal(t, err)
	}

	defer func() {
		if !docker.IsRunning() {
			os.RemoveAll(testMpath)
		}
	}()

	m.puts()

	// Add new mountpath to target
	tlog.Logf("attach new %q at target %s\n", testMpath, target.StringEx())
	err = api.AttachMountpath(baseParams, target, testMpath, true /*force*/)
	tassert.CheckFatal(t, err)

	tools.WaitForResilvering(t, baseParams, target)

	m.gets()

	// Remove new mountpath from target
	tlog.Logf("detach %q from target %s\n", testMpath, target.StringEx())
	if docker.IsRunning() {
		if err := api.DetachMountpath(baseParams, target, testMpath, false /*dont-resil*/); err != nil {
			t.Error(err.Error())
		}
	} else {
		err = api.DetachMountpath(baseParams, target, testMpath, false /*dont-resil*/)
		tassert.CheckFatal(t, err)
	}

	m.ensureNoGetErrors()

	tools.WaitForResilvering(t, baseParams, target)
	m.ensureNumMountpaths(target, mpList)
}

func TestAttachDetachMountpathAllTargets(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})
	var (
		m = ioContext{
			t:               t,
			num:             10000,
			numGetsEachFile: 5,
		}
		baseParams = tools.BaseAPIParams()

		allMps = make(map[string]*apc.MountpathList)
	)

	m.initWithCleanupAndSaveState()
	m.expectTargets(1)

	targets := m.smap.Tmap.ActiveNodes()

	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	defer func() {
		if !docker.IsRunning() {
			os.RemoveAll(testMpath)
		}
	}()

	// PUT random objects
	m.puts()

	if docker.IsRunning() {
		err := docker.CreateMpathDir(0, testMpath)
		tassert.CheckFatal(t, err)
		for _, target := range targets {
			mpList, err := api.GetMountpaths(baseParams, target)
			tassert.CheckFatal(t, err)
			allMps[target.ID()] = mpList

			err = api.AttachMountpath(baseParams, target, testMpath, true /*force*/)
			tassert.CheckFatal(t, err)
		}
	} else {
		// Add new mountpath to all targets
		for idx, target := range targets {
			mpList, err := api.GetMountpaths(baseParams, target)
			tassert.CheckFatal(t, err)
			allMps[target.ID()] = mpList

			mountpath := filepath.Join(testMpath, fmt.Sprintf("%d", idx))
			cos.CreateDir(mountpath)
			err = api.AttachMountpath(baseParams, target, mountpath, true /*force*/)
			tassert.CheckFatal(t, err)
		}
	}

	tools.WaitForResilvering(t, baseParams, nil)

	// Read after rebalance
	m.gets()

	// Remove new mountpath from all targets
	if docker.IsRunning() {
		err := docker.RemoveMpathDir(0, testMpath)
		tassert.CheckFatal(t, err)
		for _, target := range targets {
			if err := api.DetachMountpath(baseParams, target, testMpath, false /*dont-resil*/); err != nil {
				t.Error(err.Error())
			}
		}
	} else {
		for idx, target := range targets {
			mountpath := filepath.Join(testMpath, fmt.Sprintf("%d", idx))
			os.RemoveAll(mountpath)
			if err := api.DetachMountpath(baseParams, target, mountpath, false /*dont-resil*/); err != nil {
				t.Error(err.Error())
			}
		}
	}

	tools.WaitForResilvering(t, baseParams, nil)

	m.ensureNoGetErrors()
	for _, target := range targets {
		m.ensureNumMountpaths(target, allMps[target.ID()])
	}
}

func TestMountpathDisableAll(t *testing.T) {
	var (
		m = ioContext{
			t:               t,
			num:             5000,
			numGetsEachFile: 2,
		}
		baseParams = tools.BaseAPIParams()
	)

	m.initWithCleanupAndSaveState()
	m.expectTargets(1)

	// Remove all mountpaths on the target
	target, _ := m.smap.GetRandTarget()
	tname := target.StringEx()
	origMountpaths, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)
	ensureNoDisabledMountpaths(t, target, origMountpaths)

	if len(origMountpaths.WaitingDD) != 0 || len(origMountpaths.Disabled) != 0 {
		tlog.Logf("Warning %s: orig mountpaths (avail=%d, dd=%d, disabled=%d)\n", tname,
			len(origMountpaths.Available), len(origMountpaths.WaitingDD), len(origMountpaths.Disabled))
		for _, mpath := range origMountpaths.Disabled {
			err = api.EnableMountpath(baseParams, target, mpath)
			tlog.Logf("Warning %s: late enable %q, err=%v\n", tname, mpath, err)
			time.Sleep(2 * time.Second)
		}
		origMountpaths, err = api.GetMountpaths(baseParams, target)
		tassert.CheckFatal(t, err)
	} else {
		tlog.Logf("%s: orig avail mountpaths=%d\n", tname, len(origMountpaths.Available))
	}
	disabled := make(cos.StrSet)
	defer func() {
		for mpath := range disabled {
			err := api.EnableMountpath(baseParams, target, mpath)
			tassert.CheckError(t, err)
		}
		if len(disabled) != 0 {
			tlog.Logf("Wait for rebalance (when target %s that has previously lost all mountpaths joins back)\n",
				tname)
			args := api.XactReqArgs{Kind: apc.ActRebalance, Timeout: rebalanceTimeout}
			_, _ = api.WaitForXactionIC(baseParams, args)

			tools.WaitForResilvering(t, baseParams, nil)
		}
	}()
	for _, mpath := range origMountpaths.Available {
		err := api.DisableMountpath(baseParams, target, mpath, true /*dont-resil*/)
		tassert.CheckFatal(t, err)
		disabled.Add(mpath)
	}

	time.Sleep(2 * time.Second)
	tlog.Logf("Wait for rebalance (triggered by %s leaving the cluster after having lost all mountpaths)\n", tname)
	args := api.XactReqArgs{Kind: apc.ActRebalance, Timeout: rebalanceTimeout}
	_, _ = api.WaitForXactionIC(baseParams, args)

	// Check if mountpaths were actually disabled
	time.Sleep(time.Second)
	mountpaths, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)

	if len(mountpaths.Available) != 0 {
		t.Fatalf("%s should not have any mountpaths left (%d)", tname, len(mountpaths.Available))
	}
	if len(mountpaths.Disabled)+len(mountpaths.WaitingDD) != len(origMountpaths.Available) {
		t.Fatalf("%s: not all mountpaths were disabled (%d, %d, %d)", tname,
			len(mountpaths.Disabled), len(mountpaths.WaitingDD), len(origMountpaths.Available))
	}

	// Create ais bucket
	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	// Re-enable target mountpaths
	for _, mpath := range origMountpaths.Available {
		err := api.EnableMountpath(baseParams, target, mpath)
		tassert.CheckFatal(t, err)
		disabled.Delete(mpath)
	}

	time.Sleep(2 * time.Second)
	tlog.Logf("Wait for rebalance (when target %s that has previously lost all mountpaths joins back)\n", target.StringEx())
	args = api.XactReqArgs{Kind: apc.ActRebalance, Timeout: rebalanceTimeout}
	_, _ = api.WaitForXactionIC(baseParams, args)

	tools.WaitForResilvering(t, baseParams, target)

	tlog.Logf("waiting for bucket %s to show up on all targets\n", m.bck)
	err = tools.WaitForBucket(m.proxyURL, cmn.QueryBcks(m.bck), true /*exists*/)
	tassert.CheckFatal(t, err)

	// Put and read random files
	m.puts()
	m.gets()

	m.ensureNoGetErrors()
	m.ensureNumMountpaths(target, origMountpaths)
}

func TestForwardCP(t *testing.T) {
	m := ioContext{
		t:               t,
		num:             10000,
		numGetsEachFile: 2,
		fileSize:        128,
	}

	// Step 1.
	m.initWithCleanupAndSaveState()
	m.expectProxies(2)

	// Step 2.
	origID, origURL := m.smap.Primary.ID(), m.smap.Primary.PubNet.URL
	nextProxyID, nextProxyURL, err := chooseNextProxy(m.smap)
	tassert.CheckFatal(t, err)

	t.Cleanup(func() {
		// Restore original primary.
		m.smap = tools.GetClusterMap(m.t, m.proxyURL)
		setPrimaryTo(t, m.proxyURL, m.smap, origURL, origID)

		time.Sleep(time.Second)
	})

	tools.CreateBucketWithCleanup(t, nextProxyURL, m.bck, nil)
	tlog.Logf("Created bucket %s via non-primary %s\n", m.bck, nextProxyID)

	// Step 3.
	m.puts()

	// Step 4. in parallel: run GETs and designate a new primary=nextProxyID
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		m.gets()
	}()
	go func() {
		defer wg.Done()

		setPrimaryTo(t, m.proxyURL, m.smap, nextProxyURL, nextProxyID)
		m.proxyURL = nextProxyURL
	}()
	wg.Wait()

	m.ensureNoGetErrors()

	// Step 5. destroy ais bucket via original primary which is not primary at this point
	tools.DestroyBucket(t, origURL, m.bck)
	tlog.Logf("Destroyed bucket %s via non-primary %s/%s\n", m.bck, origID, origURL)
}

func TestAtimeRebalance(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	m := ioContext{
		t:               t,
		num:             2000,
		numGetsEachFile: 2,
	}

	m.initWithCleanupAndSaveState()
	m.expectTargets(2)

	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	target := m.startMaintenanceNoRebalance()

	m.puts()

	// Get atime in a format that includes nanoseconds to properly check if it
	// was updated in atime cache (if it wasn't, then the returned atime would
	// be different from the original one, but the difference could be very small).
	msg := &apc.LsoMsg{TimeFormat: time.StampNano}
	msg.AddProps(apc.GetPropsAtime, apc.GetPropsStatus)
	baseParams := tools.BaseAPIParams(m.proxyURL)
	lst, err := api.ListObjects(baseParams, m.bck, msg, 0)
	tassert.CheckFatal(t, err)

	objNames := make(cos.StrKVs, 10)
	for _, entry := range lst.Entries {
		objNames[entry.Name] = entry.Atime
	}

	rebID := m.stopMaintenance(target)

	// make sure that the cluster has all targets enabled
	_, err = tools.WaitForClusterState(
		m.proxyURL,
		"join target back",
		m.smap.Version,
		m.originalProxyCount,
		m.originalTargetCount,
	)
	tassert.CheckFatal(t, err)

	tools.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID, rebalanceTimeout)

	msg = &apc.LsoMsg{TimeFormat: time.StampNano}
	msg.AddProps(apc.GetPropsAtime, apc.GetPropsStatus)
	lstReb, err := api.ListObjects(baseParams, m.bck, msg, 0)
	tassert.CheckFatal(t, err)

	itemCount, itemCountOk := len(lstReb.Entries), 0
	l := len(lst.Entries)
	if itemCount != l {
		t.Errorf("The number of objects mismatch: before %d, after %d", len(lst.Entries), itemCount)
	}
	for _, entry := range lstReb.Entries {
		atime, ok := objNames[entry.Name]
		if !ok {
			t.Errorf("Object %q not found", entry.Name)
			continue
		}
		if atime != entry.Atime {
			t.Errorf("Atime mismatched for %s: before %q, after %q", entry.Name, atime, entry.Atime)
		}
		if entry.IsStatusOK() {
			itemCountOk++
		}
	}
	if itemCountOk != l {
		t.Errorf("Wrong number of objects with status OK: %d (expecting %d)", itemCountOk, l)
	}
}

func TestAtimeLocalGet(t *testing.T) {
	var (
		bck = cmn.Bck{
			Name:     t.Name(),
			Provider: apc.AIS,
		}
		proxyURL      = tools.RandomProxyURL(t)
		baseParams    = tools.BaseAPIParams(proxyURL)
		objectName    = t.Name()
		objectContent = readers.NewBytesReader([]byte("file content"))
	)

	tools.CreateBucketWithCleanup(t, proxyURL, bck, nil)

	err := api.PutObject(api.PutObjectArgs{BaseParams: baseParams, Bck: bck, Object: objectName, Reader: objectContent})
	tassert.CheckFatal(t, err)

	putAtime, putAtimeFormatted := tools.GetObjectAtime(t, baseParams, bck, objectName, time.RFC3339Nano)

	// Get object so that atime is updated
	_, err = api.GetObject(baseParams, bck, objectName)
	tassert.CheckFatal(t, err)

	getAtime, getAtimeFormatted := tools.GetObjectAtime(t, baseParams, bck, objectName, time.RFC3339Nano)

	if !(getAtime.After(putAtime)) {
		t.Errorf("Expected PUT atime (%s) to be before GET atime (%s)", putAtimeFormatted, getAtimeFormatted)
	}
}

func TestAtimeColdGet(t *testing.T) {
	var (
		bck           = cliBck
		proxyURL      = tools.RandomProxyURL(t)
		baseParams    = tools.BaseAPIParams(proxyURL)
		objectName    = t.Name()
		objectContent = readers.NewBytesReader([]byte("dummy content"))
	)

	tools.CheckSkip(t, tools.SkipTestArgs{RemoteBck: true, Bck: bck})
	api.DeleteObject(baseParams, bck, objectName)
	defer api.DeleteObject(baseParams, bck, objectName)

	tools.PutObjectInRemoteBucketWithoutCachingLocally(t, bck, objectName, objectContent)

	timeAfterPut := time.Now()

	// Perform the COLD get
	_, err := api.GetObject(baseParams, bck, objectName)
	tassert.CheckFatal(t, err)

	getAtime, getAtimeFormatted := tools.GetObjectAtime(t, baseParams, bck, objectName, time.RFC3339Nano)
	tassert.Fatalf(t, !getAtime.IsZero(), "GET atime is zero")

	if !(getAtime.After(timeAfterPut)) {
		t.Errorf("Expected PUT atime (%s) to be before GET atime (%s)", timeAfterPut.Format(time.RFC3339Nano), getAtimeFormatted)
	}
}

func TestAtimePrefetch(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	var (
		bck        = cliBck
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		objectName = t.Name()
		numObjs    = 10
		objPath    = "atime/obj-"
		errCh      = make(chan error, numObjs)
		nameCh     = make(chan string, numObjs)
		objs       = make([]string, 0, numObjs)
	)

	tools.CheckSkip(t, tools.SkipTestArgs{RemoteBck: true, Bck: bck})
	api.DeleteObject(baseParams, bck, objectName)
	defer func() {
		for _, obj := range objs {
			api.DeleteObject(baseParams, bck, obj)
		}
	}()

	wg := &sync.WaitGroup{}
	for i := 0; i < numObjs; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			object := objPath + strconv.FormatUint(uint64(idx), 10)
			err := api.PutObject(api.PutObjectArgs{
				BaseParams: baseParams,
				Bck:        bck,
				Object:     object,
				Reader:     readers.NewBytesReader([]byte("dummy content")),
			})
			if err == nil {
				nameCh <- object
			} else {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	close(nameCh)
	tassert.SelectErr(t, errCh, "put", true)
	for obj := range nameCh {
		objs = append(objs, obj)
	}
	xactID, err := api.EvictList(baseParams, bck, objs)
	tassert.CheckFatal(t, err)
	args := api.XactReqArgs{ID: xactID, Timeout: rebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, args)
	tassert.CheckFatal(t, err)

	timeAfterPut := time.Now()

	xactID, err = api.PrefetchList(baseParams, bck, objs)
	tassert.CheckFatal(t, err)
	args = api.XactReqArgs{ID: xactID, Kind: apc.ActPrefetchObjects, Timeout: rebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, args)
	tassert.CheckFatal(t, err)

	timeFormat := time.RFC3339Nano
	msg := &apc.LsoMsg{Props: apc.GetPropsAtime, TimeFormat: timeFormat, Prefix: objPath}
	lst, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	if len(lst.Entries) != numObjs {
		t.Errorf("Number of objects mismatch: expected %d, found %d", numObjs, len(lst.Entries))
	}
	for _, entry := range lst.Entries {
		atime, err := time.Parse(timeFormat, entry.Atime)
		tassert.CheckFatal(t, err)
		if atime.After(timeAfterPut) {
			t.Errorf("Atime should not be updated after prefetch (got: atime after PUT: %s, atime after GET: %s).",
				timeAfterPut.Format(timeFormat), atime.Format(timeFormat))
		}
	}
}

func TestAtimeLocalPut(t *testing.T) {
	var (
		bck = cmn.Bck{
			Name:     t.Name(),
			Provider: apc.AIS,
		}
		proxyURL      = tools.RandomProxyURL(t)
		baseParams    = tools.BaseAPIParams(proxyURL)
		objectName    = t.Name()
		objectContent = readers.NewBytesReader([]byte("dummy content"))
	)

	tools.CreateBucketWithCleanup(t, proxyURL, bck, nil)

	timeBeforePut := time.Now()
	err := api.PutObject(api.PutObjectArgs{BaseParams: baseParams, Bck: bck, Object: objectName, Reader: objectContent})
	tassert.CheckFatal(t, err)

	putAtime, putAtimeFormatted := tools.GetObjectAtime(t, baseParams, bck, objectName, time.RFC3339Nano)

	if !(putAtime.After(timeBeforePut)) {
		t.Errorf("Expected atime after PUT (%s) to be after atime before PUT (%s)",
			putAtimeFormatted, timeBeforePut.Format(time.RFC3339Nano))
	}
}

// 1. Unregister target
// 2. Add bucket - unregistered target should miss the update
// 3. Reregister target
// 4. Put objects
// 5. Get objects - everything should succeed
func TestGetAndPutAfterReregisterWithMissedBucketUpdate(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	m := ioContext{
		t:               t,
		num:             10000,
		numGetsEachFile: 5,
	}

	m.initWithCleanupAndSaveState()
	m.expectTargets(2)

	target := m.startMaintenanceNoRebalance()

	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	rebID := m.stopMaintenance(target)

	m.puts()
	m.gets()

	m.ensureNoGetErrors()
	m.waitAndCheckCluState()
	baseParams := tools.BaseAPIParams(m.proxyURL)
	tools.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID)
}

// 1. Unregister target
// 2. Add bucket - unregistered target should miss the update
// 3. Put objects
// 4. Reregister target - rebalance kicks in
// 5. Get objects - everything should succeed
func TestGetAfterReregisterWithMissedBucketUpdate(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	m := ioContext{
		t:               t,
		num:             10000,
		fileSize:        1024,
		numGetsEachFile: 5,
	}

	// Initialize ioContext
	m.initWithCleanupAndSaveState()
	m.expectTargets(2)

	targets := m.smap.Tmap.ActiveNodes()

	// Unregister target[0]
	args := &apc.ActValRmNode{DaemonID: targets[0].ID(), SkipRebalance: true}
	_, err := api.StartMaintenance(tools.BaseAPIParams(m.proxyURL), args)
	tassert.CheckFatal(t, err)
	tools.WaitForClusterState(
		m.proxyURL,
		"remove target",
		m.smap.Version,
		m.originalProxyCount,
		m.originalTargetCount-1,
	)

	// Create ais bucket
	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	m.puts()

	// Reregister target 0
	rebID := m.stopMaintenance(targets[0])

	// Wait for rebalance and do gets
	baseParams := tools.BaseAPIParams(m.proxyURL)
	tools.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID)

	m.gets()

	m.ensureNoGetErrors()
	m.waitAndCheckCluState()
}

func TestRenewRebalance(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	var (
		m = ioContext{
			t:                   t,
			num:                 10000,
			numGetsEachFile:     5,
			otherTasksToTrigger: 1,
		}
		rebID string
	)

	m.initWithCleanupAndSaveState()
	m.expectTargets(2)

	// Step 1: Unregister a target
	target := m.startMaintenanceNoRebalance()

	// Step 2: Create an ais bucket
	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	// Step 3: PUT objects in the bucket
	m.puts()

	baseParams := tools.BaseAPIParams(m.proxyURL)

	// Step 4: Re-register target (triggers rebalance)
	m.stopMaintenance(target)
	xactArgs := api.XactReqArgs{Kind: apc.ActRebalance, Timeout: rebalanceStartTimeout}
	err := api.WaitForXactionNode(baseParams, xactArgs, xactSnapRunning)
	tassert.CheckError(t, err)
	tlog.Logf("automatic rebalance started\n")

	wg := &sync.WaitGroup{}
	wg.Add(2)
	// Step 5: GET objects from the buket
	go func() {
		defer wg.Done()
		m.gets()
	}()

	// Step 6:
	//   - Start new rebalance manually after some time
	//   - TODO: Verify that new rebalance xaction has started
	go func() {
		defer wg.Done()

		<-m.controlCh // wait for half the GETs to complete

		rebID, err = api.StartXaction(baseParams, api.XactReqArgs{Kind: apc.ActRebalance})
		tassert.CheckFatal(t, err)
		tlog.Logf("manually initiated rebalance\n")
	}()

	wg.Wait()
	args := api.XactReqArgs{ID: rebID, Kind: apc.ActRebalance, Timeout: rebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, args)
	tassert.CheckError(t, err)

	m.ensureNoGetErrors()
	m.waitAndCheckCluState()
}

func TestGetFromMirroredWithLostOneMountpath(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})
	var (
		copies = 2
		m      = ioContext{
			t:               t,
			num:             5000,
			numGetsEachFile: 4,
		}
		baseParams = tools.BaseAPIParams()
	)

	m.initWithCleanupAndSaveState()
	m.expectTargets(1)

	// Select one target at random
	target, _ := m.smap.GetRandTarget()
	mpList, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)
	ensureNoDisabledMountpaths(t, target, mpList)
	if len(mpList.Available) < copies {
		t.Fatalf("%s requires at least %d mountpaths per target", t.Name(), copies)
	}

	// Step 1: Create a local bucket
	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	// Step 2: Make the bucket redundant
	_, err = api.SetBucketProps(baseParams, m.bck, &cmn.BucketPropsToUpdate{
		Mirror: &cmn.MirrorConfToUpdate{
			Enabled: api.Bool(true),
			Copies:  api.Int64(int64(copies)),
		},
	})
	if err != nil {
		t.Fatalf("Failed to make the bucket redundant: %v", err)
	}

	// Step 3: PUT objects in the bucket
	m.puts()
	m.ensureNumCopies(baseParams, copies, false)

	// Step 4: Remove a mountpath
	mpath := mpList.Available[0]
	tlog.Logf("Remove mountpath %s on target %s\n", mpath, target.ID())
	err = api.DetachMountpath(baseParams, target, mpath, false /*dont-resil*/)
	tassert.CheckFatal(t, err)

	tools.WaitForResilvering(t, baseParams, target)

	// Step 5: GET objects from the bucket
	m.gets()

	m.ensureNumCopies(baseParams, copies, true /*greaterOk*/)

	// Step 6: Add previously removed mountpath
	tlog.Logf("Add mountpath %s on target %s\n", mpath, target.ID())
	err = api.AttachMountpath(baseParams, target, mpath, false /*force*/)
	tassert.CheckFatal(t, err)

	tools.WaitForResilvering(t, baseParams, target)

	m.ensureNumCopies(baseParams, copies, true)
	m.ensureNoGetErrors()
	m.ensureNumMountpaths(target, mpList)
}

func TestGetFromMirroredWithLostMountpathAllExceptOne(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})
	m := ioContext{
		t:               t,
		num:             10000,
		numGetsEachFile: 4,
	}
	m.initWithCleanupAndSaveState()
	baseParams := tools.BaseAPIParams(m.proxyURL)

	// Select a random target
	target, _ := m.smap.GetRandTarget()
	mpList, err := api.GetMountpaths(baseParams, target)
	mpathCount := len(mpList.Available)
	ensureNoDisabledMountpaths(t, target, mpList)
	tassert.CheckFatal(t, err)
	if mpathCount < 3 {
		t.Skipf("%s requires at least 3 mountpaths per target (%s has %d)", t.Name(), target.StringEx(), mpathCount)
	}

	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	// Make the bucket n-copy mirrored
	_, err = api.SetBucketProps(baseParams, m.bck, &cmn.BucketPropsToUpdate{
		Mirror: &cmn.MirrorConfToUpdate{
			Enabled: api.Bool(true),
			Copies:  api.Int64(int64(mpathCount)),
		},
	})
	if err != nil {
		t.Fatalf("Failed to make the bucket redundant: %v", err)
	}

	// PUT
	m.puts()
	m.ensureNumCopies(baseParams, mpathCount, false /*greaterOk*/)

	// Remove all mountpaths except one
	tlog.Logf("Remove all except one (%q) mountpath on target %s\n", mpList.Available[0], target.StringEx())
	for i, mpath := range mpList.Available[1:] {
		err = api.DetachMountpath(baseParams, target, mpath, false /*dont-resil*/)
		if err != nil {
			for j := 0; j < i; j++ {
				api.AttachMountpath(baseParams, target, mpList.Available[j+1], false /*force*/)
			}
			tassert.CheckFatal(t, err)
		}
		time.Sleep(time.Second)
	}

	tools.WaitForResilvering(t, baseParams, target)

	// Wait for async mirroring to finish
	flt := api.XactReqArgs{Kind: apc.ActPutCopies, Bck: m.bck}
	api.WaitForXactionIdle(baseParams, flt)
	time.Sleep(time.Second) // pending writes

	// GET
	m.gets()

	// Reattach previously removed mountpaths
	tlog.Logf("Reattach mountpaths at %s\n", target.StringEx())
	for _, mpath := range mpList.Available[1:] {
		err = api.AttachMountpath(baseParams, target, mpath, false /*force*/)
		tassert.CheckFatal(t, err)
		time.Sleep(time.Second)
	}

	tools.WaitForResilvering(t, baseParams, nil)

	m.ensureNumCopies(baseParams, mpathCount, true /*greaterOk*/)
	m.ensureNoGetErrors()
	m.ensureNumMountpaths(target, mpList)
}

// TODO: remove all except one mountpath, run short, reduce sleep, increase stress...
func TestGetNonRedundantWithDisabledMountpath(t *testing.T) {
	testNonRedundantMpathDD(t, apc.ActMountpathDisable)
}

func TestGetNonRedundantWithDetachedMountpath(t *testing.T) {
	testNonRedundantMpathDD(t, apc.ActMountpathDetach)
}

func testNonRedundantMpathDD(t *testing.T, action string) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})
	m := ioContext{
		t:               t,
		num:             1000,
		numGetsEachFile: 2,
	}
	m.initWithCleanupAndSaveState()
	baseParams := tools.BaseAPIParams(m.proxyURL)

	// Select a random target
	target, _ := m.smap.GetRandTarget()
	mpList, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)
	ensureNoDisabledMountpaths(t, target, mpList)

	mpathCount := len(mpList.Available)
	if mpathCount < 2 {
		t.Skipf("%s requires at least 2 mountpaths per target (%s has %d)", t.Name(), target.StringEx(), mpathCount)
	}

	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	// PUT
	m.puts()

	tlog.Logf("%s %q at target %s\n", action, mpList.Available[0], target.StringEx())
	if action == apc.ActMountpathDisable {
		err = api.DisableMountpath(baseParams, target, mpList.Available[0], false /*dont-resil*/)
	} else {
		err = api.DetachMountpath(baseParams, target, mpList.Available[0], false /*dont-resil*/)
	}
	tassert.CheckFatal(t, err)

	tools.WaitForResilvering(t, baseParams, target)

	// GET
	m.gets()

	// Add previously disabled or detached mountpath
	if action == apc.ActMountpathDisable {
		tlog.Logf("Re-enable %q at target %s\n", mpList.Available[0], target.StringEx())
		err = api.EnableMountpath(baseParams, target, mpList.Available[0])
	} else {
		tlog.Logf("Re-attach %q at target %s\n", mpList.Available[0], target.StringEx())
		err = api.AttachMountpath(baseParams, target, mpList.Available[0], false /*force*/)
	}
	tassert.CheckFatal(t, err)

	tools.WaitForResilvering(t, baseParams, target)

	m.ensureNoGetErrors()
	m.ensureNumMountpaths(target, mpList)
}

// 1. Start rebalance
// 2. Start changing the primary proxy
// 3. IC must survive and rebalance must finish
func TestICRebalance(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true, RequiredDeployment: tools.ClusterTypeLocal})

	var (
		m = ioContext{
			t:   t,
			num: 25000,
		}
		rebID string
	)

	m.initWithCleanupAndSaveState()
	m.expectTargets(3)
	m.expectProxies(3)
	psi, err := m.smap.GetRandProxy(true /*exclude primary*/)
	tassert.CheckFatal(t, err)
	m.proxyURL = psi.URL(cmn.NetPublic)
	icNode := tools.GetICProxy(t, m.smap, psi.ID())

	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	m.puts()

	baseParams := tools.BaseAPIParams(m.proxyURL)

	tlog.Logf("Manually initiated rebalance\n")
	rebID, err = api.StartXaction(baseParams, api.XactReqArgs{Kind: apc.ActRebalance})
	tassert.CheckFatal(t, err)

	xactArgs := api.XactReqArgs{Kind: apc.ActRebalance, Timeout: rebalanceStartTimeout}
	api.WaitForXactionNode(baseParams, xactArgs, xactSnapRunning)

	tlog.Logf("Killing %s\n", icNode.StringEx())
	// cmd and args are the original command line of how the proxy is started
	cmd, err := tools.KillNode(icNode)
	tassert.CheckFatal(t, err)

	proxyCnt := m.smap.CountActivePs()
	smap, err := tools.WaitForClusterState(m.proxyURL, "designate new primary", m.smap.Version, proxyCnt-1, 0)
	tassert.CheckError(t, err)

	// re-construct the command line to start the original proxy but add the current primary proxy to the args
	err = tools.RestoreNode(cmd, false, "proxy (prev primary)")
	tassert.CheckFatal(t, err)

	smap, err = tools.WaitForClusterState(m.proxyURL, "restore", smap.Version, proxyCnt, 0)
	tassert.CheckFatal(t, err)
	if _, ok := smap.Pmap[psi.ID()]; !ok {
		t.Fatalf("Previous primary proxy did not rejoin the cluster")
	}
	checkSmaps(t, m.proxyURL)

	tlog.Logf("Wait for rebalance: %s\n", rebID)
	args := api.XactReqArgs{ID: rebID, Kind: apc.ActRebalance, Timeout: rebalanceTimeout}
	_, _ = api.WaitForXactionIC(baseParams, args)

	m.waitAndCheckCluState()
}

// 1. Start decommissioning a target with rebalance
// 2. Start changing the primary proxy
// 3. IC must survive, rebalance must finish, and the target must be gone
func TestICDecommission(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true, RequiredDeployment: tools.ClusterTypeLocal})

	var (
		err error
		m   = ioContext{
			t:   t,
			num: 25000,
		}
	)

	m.initWithCleanupAndSaveState()
	m.expectTargets(3)
	m.expectProxies(3)
	psi, err := m.smap.GetRandProxy(true /*exclude primary*/)
	tassert.CheckFatal(t, err)
	m.proxyURL = psi.URL(cmn.NetPublic)
	tlog.Logf("Monitoring node: %s\n", psi.StringEx())
	icNode := tools.GetICProxy(t, m.smap, psi.ID())

	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	m.puts()

	baseParams := tools.BaseAPIParams(m.proxyURL)
	tsi, err := m.smap.GetRandTarget()
	tassert.CheckFatal(t, err)

	args := &apc.ActValRmNode{DaemonID: tsi.ID(), SkipRebalance: true}
	_, err = api.StartMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)

	defer func() {
		args := &apc.ActValRmNode{DaemonID: tsi.ID()}
		rebID, err := api.StopMaintenance(baseParams, args)
		tassert.CheckFatal(t, err)
		tools.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID)
		tassert.CheckFatal(t, err)
	}()

	tassert.CheckFatal(t, err)
	tlog.Logf("Killing %s\n", icNode.StringEx())

	// cmd and args are the original command line of how the proxy is started
	cmd, err := tools.KillNode(icNode)
	tassert.CheckFatal(t, err)

	proxyCnt := m.smap.CountActivePs()
	smap, err := tools.WaitForClusterState(m.proxyURL, "designate new primary", m.smap.Version, proxyCnt-1, 0)
	tassert.CheckError(t, err)

	// re-construct the command line to start the original proxy but add the current primary proxy to the args
	err = tools.RestoreNode(cmd, false, "proxy (prev primary)")
	tassert.CheckFatal(t, err)

	smap, err = tools.WaitForClusterState(m.proxyURL, "restore", smap.Version, proxyCnt, 0)
	tassert.CheckFatal(t, err)
	if _, ok := smap.Pmap[psi.ID()]; !ok {
		t.Fatalf("Previous primary proxy did not rejoin the cluster")
	}
	checkSmaps(t, m.proxyURL)

	_, err = tools.WaitForClusterState(m.proxyURL, "decommission target",
		m.smap.Version, m.smap.CountProxies(), m.smap.CountTargets()-1)
	tassert.CheckFatal(t, err)
}

func TestSingleResilver(t *testing.T) {
	m := ioContext{t: t}
	m.initWithCleanupAndSaveState()
	baseParams := tools.BaseAPIParams(m.proxyURL)

	// Select a random target
	target, _ := m.smap.GetRandTarget()

	// Start resilvering just on the target
	args := api.XactReqArgs{Kind: apc.ActResilver, DaemonID: target.ID()}
	id, err := api.StartXaction(baseParams, args)
	tassert.CheckFatal(t, err)

	// Wait for specific resilvering x[id]
	args = api.XactReqArgs{ID: id, Kind: apc.ActResilver, Timeout: rebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, args)
	tassert.CheckFatal(t, err)

	// Make sure other nodes were not resilvered
	args = api.XactReqArgs{ID: id}
	snaps, err := api.QueryXactionSnaps(baseParams, args)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(snaps) == 1, "expected only 1 resilver")
}
