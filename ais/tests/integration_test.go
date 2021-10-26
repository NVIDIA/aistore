// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/containers"
	"github.com/NVIDIA/aistore/devtools/readers"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tlog"
	"github.com/NVIDIA/aistore/devtools/tutils"
)

// Intended for a deployment with multiple targets
// 1. Create ais bucket
// 2. Unregister target T
// 3. PUT large amount of objects into the ais bucket
// 4. GET the objects while simultaneously registering the target T
func TestGetAndReRegisterInParallel(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	var (
		m = ioContext{
			t:               t,
			num:             50000,
			numGetsEachFile: 3,
			fileSize:        10 * cos.KiB,
		}
		rebID string
	)

	m.initAndSaveCluState()
	m.expectTargets(2)

	// Step 1.
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

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

	m.ensureNoErrors()
	m.waitAndCheckCluState()
	if rebID != "" {
		tutils.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID)
	}
}

// All of the above PLUS proxy failover/failback sequence in parallel:
// 1. Create an ais bucket
// 2. Unregister a target
// 3. Crash the primary proxy and PUT in parallel
// 4. Failback to the original primary proxy, register target, and GET in parallel
func TestProxyFailbackAndReRegisterInParallel(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	m := ioContext{
		t:                   t,
		otherTasksToTrigger: 1,
		num:                 150000,
	}

	m.initAndSaveCluState()
	m.expectTargets(2)
	m.expectProxies(3)

	// Step 1.
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

	// Step 2.
	target := m.startMaintenanceNoRebalance()
	defer tutils.WaitForRebalanceToComplete(t, baseParams, time.Minute)

	// Step 3.
	_, newPrimaryURL, err := chooseNextProxy(m.smap)
	tassert.CheckFatal(t, err)
	// use a new proxyURL because primaryCrashElectRestart has a side-effect:
	// it changes the primary proxy. Without the change tutils.PutRandObjs is
	// failing while the current primary is restarting and rejoining
	m.proxyURL = newPrimaryURL

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		killRestorePrimary(t, m.proxyURL, false, nil)
	}()

	// PUT phase is timed to ensure it doesn't finish before primaryCrashElectRestart() begins
	time.Sleep(5 * time.Second)
	m.puts()
	wg.Wait()

	// Step 4.
	wg.Add(3)
	go func() {
		defer wg.Done()
		m.stopMaintenance(target)
	}()

	go func() {
		defer wg.Done()
		<-m.controlCh
		primarySetToOriginal(t)
	}()

	go func() {
		defer wg.Done()
		m.gets()
	}()
	wg.Wait()

	m.ensureNoErrors()
	m.waitAndCheckCluState()
}

// Similar to TestGetAndReRegisterInParallel, but instead of unregister, we kill the target
// 1. Kill registered target and wait for Smap to updated
// 2. Create ais bucket
// 3. PUT large amounts of objects into ais bucket
// 4. Get the objects while simultaneously registering the target
func TestGetAndRestoreInParallel(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true, RequiredDeployment: tutils.ClusterTypeLocal})

	var (
		m = ioContext{
			t:               t,
			num:             20000,
			numGetsEachFile: 5,
			fileSize:        cos.KiB * 2,
		}
		targetURL  string
		targetID   string
		targetNode *cluster.Snode
	)

	m.initAndSaveCluState()
	m.expectTargets(3)

	// Step 1
	// Select a random target
	targetNode, _ = m.smap.GetRandTarget()
	targetURL = targetNode.PublicNet.DirectURL
	targetID = targetNode.ID()

	tlog.Logf("Killing target: %s - %s\n", targetURL, targetID)
	tcmd, err := tutils.KillNode(targetNode)
	tassert.CheckFatal(t, err)

	proxyURL := tutils.RandomProxyURL(t)
	m.smap, err = tutils.WaitForClusterState(proxyURL, "target removal", m.smap.Version, m.originalProxyCount,
		m.originalTargetCount-1)
	tassert.CheckError(t, err)

	// Step 2
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

	// Step 3
	m.puts()

	// Step 4
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		time.Sleep(4 * time.Second)
		tutils.RestoreNode(tcmd, false, "target")
	}()
	go func() {
		defer wg.Done()
		m.gets()
	}()
	wg.Wait()

	m.ensureNoErrors()
	m.waitAndCheckCluState()
	tutils.WaitForRebalanceToComplete(m.t, tutils.BaseAPIParams(m.proxyURL))
}

func TestUnregisterPreviouslyUnregisteredTarget(t *testing.T) {
	m := ioContext{t: t}
	m.initAndSaveCluState()
	m.expectTargets(1)
	target := m.startMaintenanceNoRebalance()

	// Decommission the same target again.
	args := &cmn.ActValRmNode{DaemonID: target.ID(), SkipRebalance: true}
	_, err := api.StartMaintenance(tutils.BaseAPIParams(m.proxyURL), args)
	tassert.Errorf(t, err != nil, "error expected")

	n := tutils.GetClusterMap(t, m.proxyURL).CountActiveTargets()
	if n != m.originalTargetCount-1 {
		t.Fatalf("expected %d targets after putting target in maintenance, got %d targets",
			m.originalTargetCount-1, n)
	}

	// Register target (bring cluster to normal state)
	rebID := m.stopMaintenance(target)
	m.waitAndCheckCluState()
	tutils.WaitForRebalanceByID(m.t, m.originalTargetCount, tutils.BaseAPIParams(m.proxyURL), rebID)
}

func TestRegisterAndUnregisterTargetAndPutInParallel(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	m := ioContext{
		t:   t,
		num: 10000,
	}

	m.initAndSaveCluState()
	m.expectTargets(3)

	targets := m.smap.Tmap.ActiveNodes()

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

	// Unregister target[0]
	args := &cmn.ActValRmNode{DaemonID: targets[0].ID(), SkipRebalance: true}
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	_, err := api.StartMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)
	tutils.WaitForClusterState(
		m.proxyURL,
		"put target in maintenance",
		m.smap.Version,
		m.originalProxyCount,
		m.originalTargetCount-1,
	)

	n := tutils.GetClusterMap(t, m.proxyURL).CountActiveTargets()
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
		args := &cmn.ActValRmNode{DaemonID: targets[0].ID()}
		tlog.Logf("Take %s out of maintenance\n", targets[0].StringEx())
		_, err = api.StopMaintenance(baseParams, args)
		tassert.CheckFatal(t, err)
	}()

	// Decommission target[1] in parallel
	go func() {
		defer wg.Done()
		args := &cmn.ActValRmNode{DaemonID: targets[1].ID(), SkipRebalance: true}
		_, err = api.StartMaintenance(baseParams, args)
		tassert.CheckFatal(t, err)
	}()

	// Wait for everything to end
	wg.Wait()

	// Register target 1 to bring cluster to original state
	rebID := m.stopMaintenance(targets[1])

	// wait for rebalance to complete
	tutils.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID, rebalanceTimeout)

	m.waitAndCheckCluState()
}

func TestAckRebalance(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	m := ioContext{
		t:             t,
		num:           30000,
		getErrIsFatal: true,
	}

	m.initAndSaveCluState()
	m.expectTargets(3)

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

	target := m.startMaintenanceNoRebalance()

	// Start putting files into bucket.
	m.puts()

	rebID := m.stopMaintenance(target)

	// Wait for everything to finish.
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	tutils.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID, rebalanceTimeout)

	m.gets()

	m.ensureNoErrors()
	m.waitAndCheckCluState()
}

func TestStressRebalance(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	m := &ioContext{
		t: t,
	}

	m.initAndSaveCluState()
	m.expectTargets(4)

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

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

	m.initAndSaveCluState()

	tgts := m.smap.Tmap.ActiveNodes()
	i1 := rand.Intn(len(tgts))
	i2 := (i1 + 1) % len(tgts)
	target1, target2 := tgts[i1], tgts[i2]

	// Unregister targets.
	tlog.Logf("Killing %s and %s\n", target1.StringEx(), target2.StringEx())
	cmd1, err := tutils.KillNode(target1)
	tassert.CheckFatal(t, err)
	time.Sleep(time.Second)
	cmd2, err := tutils.KillNode(target2)
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
	err = tutils.RestoreNode(cmd1, false, "the 1st target")
	tassert.CheckFatal(t, err)

	// random sleep between the first and the second join
	time.Sleep(time.Duration(rand.Intn(3)+1) * time.Second)

	err = tutils.RestoreNode(cmd2, false, "the 2nd target")
	tassert.CheckFatal(t, err)

	_, err = tutils.WaitForClusterState(
		m.proxyURL,
		"targets to join",
		m.smap.Version,
		m.originalProxyCount,
		m.originalTargetCount,
	)
	tassert.CheckFatal(m.t, err)

	// wait for the rebalance to finish
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	tutils.WaitForRebalanceToComplete(t, baseParams, rebalanceTimeout)

	// wait for the reads to run out
	wg.Wait()

	m.ensureNoErrors()
	m.waitAndCheckCluState()
}

func TestRebalanceAfterUnregisterAndReregister(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	m := ioContext{
		t:   t,
		num: 10000,
	}

	m.initAndSaveCluState()
	m.expectTargets(3)

	targets := m.smap.Tmap.ActiveNodes()

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

	target0, target1 := targets[0], targets[1]
	args := &cmn.ActValRmNode{DaemonID: target0.ID(), SkipRebalance: true}
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	_, err := api.StartMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)

	_, err = tutils.WaitForClusterState(
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
		args := &cmn.ActValRmNode{DaemonID: target0.ID()}
		_, err = api.StopMaintenance(baseParams, args)
		tassert.CheckFatal(t, err)
	}()

	// Unregister target 1 in parallel
	go func() {
		defer wg.Done()
		tlog.Logf("Remove %s from Smap\n", target1.StringEx())
		err = tutils.RemoveNodeFromSmap(m.proxyURL, target1.ID())
		tassert.CheckFatal(t, err)
	}()

	// Wait for everything to end
	wg.Wait()

	// Register target 1 to bring cluster to original state
	sleep := time.Duration(rand.Intn(5))*time.Second + time.Millisecond
	time.Sleep(sleep)
	tlog.Logf("Join target %s\n", target1.StringEx())
	rebID, err := tutils.JoinCluster(m.proxyURL, target1)
	tassert.CheckFatal(t, err)
	_, err = tutils.WaitForClusterState(
		m.proxyURL,
		"targets to join",
		m.smap.Version,
		m.originalProxyCount,
		m.originalTargetCount,
	)
	tassert.CheckFatal(m.t, err)

	tlog.Logf("Wait for rebalance...\n")
	tutils.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID, rebalanceTimeout)

	m.gets()

	m.ensureNoErrors()
	m.waitAndCheckCluState()
}

func TestPutDuringRebalance(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	m := ioContext{
		t:   t,
		num: 10000,
	}

	m.initAndSaveCluState()
	m.expectTargets(3)

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

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
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	tutils.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID, rebalanceTimeout)

	// Main check - try to read all objects.
	m.gets()

	m.checkObjectDistribution(t)
	m.waitAndCheckCluState()
}

func TestGetDuringLocalAndGlobalRebalance(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	var (
		m = ioContext{
			t:               t,
			num:             10000,
			numGetsEachFile: 3,
		}
		baseParams     = tutils.BaseAPIParams()
		selectedTarget *cluster.Snode
		killTarget     *cluster.Snode
	)

	m.initAndSaveCluState()
	m.expectTargets(2)

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

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

	if len(mpList.Available) < 2 {
		t.Fatalf("Must have at least 2 mountpaths")
	}

	// Disable mountpaths temporarily
	mpath := mpList.Available[0]
	tlog.Logf("Disable mountpath on target %s\n", selectedTarget.ID())
	err = api.DisableMountpath(baseParams, selectedTarget, mpath)
	tassert.CheckFatal(t, err)

	args := &cmn.ActValRmNode{DaemonID: killTarget.ID(), SkipRebalance: true}
	_, err = api.StartMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)
	smap, err := tutils.WaitForClusterState(
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
	args = &cmn.ActValRmNode{DaemonID: killTarget.ID()}
	_, err = api.StopMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)

	// enable mountpath
	err = api.EnableMountpath(baseParams, selectedTarget, mpath)
	tassert.CheckFatal(t, err)

	// wait until GETs are done while 2 rebalance are running
	wg.Wait()

	// make sure that the cluster has all targets enabled
	_, err = tutils.WaitForClusterState(
		m.proxyURL,
		"join target back",
		smap.Version,
		m.originalProxyCount,
		m.originalTargetCount,
	)
	tassert.CheckFatal(m.t, err)

	mpListAfter, err := api.GetMountpaths(baseParams, selectedTarget)
	tassert.CheckFatal(t, err)
	if len(mpList.Available) != len(mpListAfter.Available) {
		t.Fatalf("Some mountpaths failed to enable: the number before %d, after %d",
			len(mpList.Available), len(mpListAfter.Available))
	}

	// wait for rebalance to complete
	baseParams = tutils.BaseAPIParams(m.proxyURL)
	tutils.WaitForRebalanceToComplete(t, baseParams, rebalanceTimeout)

	m.ensureNoErrors()
	m.waitAndCheckCluState()
}

func TestGetDuringLocalRebalance(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	var (
		m = ioContext{
			t:   t,
			num: 20000,
		}
		baseParams = tutils.BaseAPIParams()
	)

	m.initAndSaveCluState()
	m.expectTargets(1)

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

	target, _ := m.smap.GetRandTarget()
	mpList, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)

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
		err = api.DisableMountpath(baseParams, target, mp)
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
		// sleep for a while before enabling another mountpath
		time.Sleep(50 * time.Millisecond)
		err = api.EnableMountpath(baseParams, target, mp)
		tassert.CheckFatal(t, err)
	}
	m.stopGets()

	wg.Wait()
	tutils.WaitForRebalanceToComplete(t, baseParams, rebalanceTimeout)

	mpListAfter, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)
	if len(mpList.Available) != len(mpListAfter.Available) {
		t.Fatalf("Some mountpaths failed to enable: the number before %d, after %d",
			len(mpList.Available), len(mpListAfter.Available))
	}

	m.ensureNoErrors()
}

func TestGetDuringRebalance(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	m := ioContext{
		t:   t,
		num: 30000,
	}

	m.initAndSaveCluState()
	m.expectTargets(3)

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

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
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	tutils.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID, rebalanceTimeout)
	wg.Wait()

	// Get objects once again to check if they are still accessible after rebalance.
	m.gets()

	m.ensureNoErrors()
	m.waitAndCheckCluState()
}

func TestRegisterTargetsAndCreateBucketsInParallel(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	const (
		unregisterTargetCount = 2
		newBucketCount        = 3
	)

	m := ioContext{
		t: t,
	}

	m.initAndSaveCluState()
	m.expectTargets(3)

	targets := m.smap.Tmap.ActiveNodes()
	baseParams := tutils.BaseAPIParams(m.proxyURL)

	// Decommission targets
	for i := 0; i < unregisterTargetCount; i++ {
		args := &cmn.ActValRmNode{DaemonID: targets[i].ID(), SkipRebalance: true}
		_, err := api.StartMaintenance(baseParams, args)
		tassert.CheckError(t, err)
	}
	tutils.WaitForClusterState(
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
			args := &cmn.ActValRmNode{DaemonID: targets[number].ID()}
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
			tutils.CreateFreshBucket(t, m.proxyURL, bck, nil)
		}()
	}
	wg.Wait()
	m.waitAndCheckCluState()
	tutils.WaitForRebalanceToComplete(t, baseParams, rebalanceTimeout)
}

func TestMountpathRemoveAndAdd(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true, MinTargets: 2})

	var (
		m = ioContext{
			t:               t,
			num:             5000,
			numGetsEachFile: 2,
		}
		baseParams = tutils.BaseAPIParams()
	)

	m.initAndSaveCluState()
	m.expectTargets(2)

	target, _ := m.smap.GetRandTarget()
	// Remove all mountpaths for one target
	oldMountpaths, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)

	for _, mpath := range oldMountpaths.Available {
		err = api.DetachMountpath(baseParams, target, mpath)
		tassert.CheckFatal(t, err)
	}

	// Check if mountpaths were actually removed
	mountpaths, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)

	if len(mountpaths.Available) != 0 {
		t.Fatalf("Target should not have any paths available")
	}

	// Create ais bucket
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

	// Add target mountpath again
	for _, mpath := range oldMountpaths.Available {
		err = api.AttachMountpath(baseParams, target, mpath)
		tassert.CheckFatal(t, err)
	}

	// Check if mountpaths were actually added
	mountpaths, err = api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)

	if len(mountpaths.Available) != len(oldMountpaths.Available) {
		t.Fatalf("Target should have old mountpath available restored")
	}

	// Put and read random files
	m.puts()
	m.gets()
	m.ensureNoErrors()
}

func TestLocalRebalanceAfterAddingMountpath(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	const newMountpath = "/tmp/ais/mountpath"

	var (
		m = ioContext{
			t:               t,
			num:             5000,
			numGetsEachFile: 2,
		}
		baseParams = tutils.BaseAPIParams()
	)

	m.initAndSaveCluState()
	m.expectTargets(1)
	target, _ := m.smap.GetRandTarget()

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

	if containers.DockerRunning() {
		err := containers.DockerCreateMpathDir(0, newMountpath)
		tassert.CheckFatal(t, err)
	} else {
		err := cos.CreateDir(newMountpath)
		tassert.CheckFatal(t, err)
	}

	defer func() {
		if !containers.DockerRunning() {
			os.RemoveAll(newMountpath)
		}
	}()

	m.puts()

	// Add new mountpath to target
	err := api.AttachMountpath(baseParams, target, newMountpath)
	tassert.CheckFatal(t, err)

	tutils.WaitForRebalanceToComplete(t, tutils.BaseAPIParams(m.proxyURL), rebalanceTimeout)

	m.gets()

	// Remove new mountpath from target
	if containers.DockerRunning() {
		if err := api.DetachMountpath(baseParams, target, newMountpath); err != nil {
			t.Error(err.Error())
		}
	} else {
		err = api.DetachMountpath(baseParams, target, newMountpath)
		tassert.CheckFatal(t, err)
	}

	m.ensureNoErrors()
}

func TestLocalAndGlobalRebalanceAfterAddingMountpath(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	const (
		newMountpath = "/tmp/ais/mountpath"
	)

	var (
		m = ioContext{
			t:               t,
			num:             10000,
			numGetsEachFile: 5,
		}
		baseParams = tutils.BaseAPIParams()
	)

	m.initAndSaveCluState()
	m.expectTargets(1)

	targets := m.smap.Tmap.ActiveNodes()

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

	defer func() {
		if !containers.DockerRunning() {
			os.RemoveAll(newMountpath)
		}
	}()

	// PUT random objects
	m.puts()

	if containers.DockerRunning() {
		err := containers.DockerCreateMpathDir(0, newMountpath)
		tassert.CheckFatal(t, err)
		for _, target := range targets {
			err = api.AttachMountpath(baseParams, target, newMountpath)
			tassert.CheckFatal(t, err)
		}
	} else {
		// Add new mountpath to all targets
		for idx, target := range targets {
			mountpath := filepath.Join(newMountpath, fmt.Sprintf("%d", idx))
			cos.CreateDir(mountpath)
			err := api.AttachMountpath(baseParams, target, mountpath)
			tassert.CheckFatal(t, err)
		}
	}

	tutils.WaitForRebalanceToComplete(t, tutils.BaseAPIParams(m.proxyURL), rebalanceTimeout)

	// Read after rebalance
	m.gets()

	// Remove new mountpath from all targets
	if containers.DockerRunning() {
		err := containers.DockerRemoveMpathDir(0, newMountpath)
		tassert.CheckFatal(t, err)
		for _, target := range targets {
			if err := api.DetachMountpath(baseParams, target, newMountpath); err != nil {
				t.Error(err.Error())
			}
		}
	} else {
		for idx, target := range targets {
			mountpath := filepath.Join(newMountpath, fmt.Sprintf("%d", idx))
			os.RemoveAll(mountpath)
			if err := api.DetachMountpath(baseParams, target, mountpath); err != nil {
				t.Error(err.Error())
			}
		}
	}

	m.ensureNoErrors()
}

func TestMountpathDisableAndEnable(t *testing.T) {
	var (
		m = ioContext{
			t:               t,
			num:             5000,
			numGetsEachFile: 2,
		}
		baseParams = tutils.BaseAPIParams()
	)

	m.initAndSaveCluState()
	m.expectTargets(1)

	target, _ := m.smap.GetRandTarget()
	// Remove all mountpaths for one target
	oldMountpaths, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)

	disabled := make(cos.StringSet)
	defer func() {
		for mpath := range disabled {
			err := api.EnableMountpath(baseParams, target, mpath)
			tassert.CheckError(t, err)
		}
		if len(disabled) != 0 {
			tutils.WaitForRebalanceToComplete(t, baseParams)
		}
	}()
	for _, mpath := range oldMountpaths.Available {
		err := api.DisableMountpath(baseParams, target, mpath)
		tassert.CheckFatal(t, err)
		disabled.Add(mpath)
	}

	// Check if mountpaths were actually disabled
	mountpaths, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)

	if len(mountpaths.Available) != 0 {
		t.Fatalf("Target should not have any paths available")
	}

	if len(mountpaths.Disabled) != len(oldMountpaths.Available) {
		t.Fatalf("Not all mountpaths were added to disabled paths")
	}

	// Create ais bucket
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

	// Add target mountpath again
	for _, mpath := range oldMountpaths.Available {
		err := api.EnableMountpath(baseParams, target, mpath)
		tassert.CheckFatal(t, err)
		disabled.Delete(mpath)
	}

	// Check if mountpaths were actually enabled
	mountpaths, err = api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)

	if len(mountpaths.Available) != len(oldMountpaths.Available) {
		t.Fatalf("Target should have old mountpath available restored")
	}

	if len(mountpaths.Disabled) != 0 {
		t.Fatalf("Not all disabled mountpaths were enabled")
	}

	tlog.Logf("waiting for ais bucket %s to appear on all targets\n", m.bck)
	err = tutils.WaitForBucket(m.proxyURL, cmn.QueryBcks(m.bck), true /*exists*/)
	tassert.CheckFatal(t, err)

	// Put and read random files
	m.puts()
	m.gets()
	m.ensureNoErrors()
	tutils.WaitForRebalanceToComplete(t, baseParams)
}

func TestForwardCP(t *testing.T) {
	m := ioContext{
		t:               t,
		num:             10000,
		numGetsEachFile: 2,
		fileSize:        128,
	}

	// Step 1.
	m.initAndSaveCluState()
	m.expectProxies(2)

	// Step 2.
	origID, origURL := m.smap.Primary.ID(), m.smap.Primary.PublicNet.DirectURL
	nextProxyID, nextProxyURL, err := chooseNextProxy(m.smap)
	tassert.CheckFatal(t, err)

	t.Cleanup(func() {
		// Restore original primary.
		setPrimaryTo(t, m.proxyURL, m.smap, origURL, origID)
	})

	tutils.CreateFreshBucket(t, nextProxyURL, m.bck, nil)
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

	m.ensureNoErrors()

	// Step 5. destroy ais bucket via original primary which is not primary at this point
	tutils.DestroyBucket(t, origURL, m.bck)
	tlog.Logf("Destroyed bucket %s via non-primary %s/%s\n", m.bck, origID, origURL)
}

func TestAtimeRebalance(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	m := ioContext{
		t:               t,
		num:             2000,
		numGetsEachFile: 2,
	}

	m.initAndSaveCluState()
	m.expectTargets(2)

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

	target := m.startMaintenanceNoRebalance()

	m.puts()

	// Get atime in a format that includes nanoseconds to properly check if it
	// was updated in atime cache (if it wasn't, then the returned atime would
	// be different from the original one, but the difference could be very small).
	msg := &cmn.SelectMsg{TimeFormat: time.StampNano}
	msg.AddProps(cmn.GetPropsAtime, cmn.GetPropsStatus)
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	bucketList, err := api.ListObjects(baseParams, m.bck, msg, 0)
	tassert.CheckFatal(t, err)

	objNames := make(cos.SimpleKVs, 10)
	for _, entry := range bucketList.Entries {
		objNames[entry.Name] = entry.Atime
	}

	rebID := m.stopMaintenance(target)

	// make sure that the cluster has all targets enabled
	_, err = tutils.WaitForClusterState(
		m.proxyURL,
		"join target back",
		m.smap.Version,
		m.originalProxyCount,
		m.originalTargetCount,
	)
	tassert.CheckFatal(t, err)

	tutils.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID, rebalanceTimeout)

	msg = &cmn.SelectMsg{TimeFormat: time.StampNano}
	msg.AddProps(cmn.GetPropsAtime, cmn.GetPropsStatus)
	bucketListReb, err := api.ListObjects(baseParams, m.bck, msg, 0)
	tassert.CheckFatal(t, err)

	itemCount, itemCountOk := len(bucketListReb.Entries), 0
	l := len(bucketList.Entries)
	if itemCount != l {
		t.Errorf("The number of objects mismatch: before %d, after %d", len(bucketList.Entries), itemCount)
	}
	for _, entry := range bucketListReb.Entries {
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
			Provider: cmn.ProviderAIS,
		}
		proxyURL      = tutils.RandomProxyURL(t)
		baseParams    = tutils.BaseAPIParams(proxyURL)
		objectName    = t.Name()
		objectContent = readers.NewBytesReader([]byte("file content"))
	)

	tutils.CreateFreshBucket(t, proxyURL, bck, nil)

	err := api.PutObject(api.PutObjectArgs{BaseParams: baseParams, Bck: bck, Object: objectName, Reader: objectContent})
	tassert.CheckFatal(t, err)

	timeAfterPut := tutils.GetObjectAtime(t, baseParams, bck, objectName, time.RFC3339Nano)

	// Get object so that atime is updated
	_, err = api.GetObject(baseParams, bck, objectName)
	tassert.CheckFatal(t, err)

	timeAfterGet := tutils.GetObjectAtime(t, baseParams, bck, objectName, time.RFC3339Nano)

	if !(timeAfterGet.After(timeAfterPut)) {
		t.Errorf("Expected PUT atime (%s) to be before subsequent GET atime (%s).",
			timeAfterGet.Format(time.RFC3339Nano), timeAfterPut.Format(time.RFC3339Nano))
	}
}

func TestAtimeColdGet(t *testing.T) {
	var (
		bck           = cliBck
		proxyURL      = tutils.RandomProxyURL(t)
		baseParams    = tutils.BaseAPIParams(proxyURL)
		objectName    = t.Name()
		objectContent = readers.NewBytesReader([]byte("dummy content"))
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{RemoteBck: true, Bck: bck})
	api.DeleteObject(baseParams, bck, objectName)
	defer api.DeleteObject(baseParams, bck, objectName)

	tutils.PutObjectInRemoteBucketWithoutCachingLocally(t, bck, objectName, objectContent)

	timeAfterPut := time.Now()

	// Perform the COLD get
	_, err := api.GetObject(baseParams, bck, objectName)
	tassert.CheckFatal(t, err)

	timeAfterGet := tutils.GetObjectAtime(t, baseParams, bck, objectName, time.RFC3339Nano)

	if !(timeAfterGet.After(timeAfterPut)) {
		t.Errorf("Expected PUT atime (%s) to be before subsequent GET atime (%s).",
			timeAfterGet.Format(time.RFC3339Nano), timeAfterPut.Format(time.RFC3339Nano))
	}
}

func TestAtimePrefetch(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	var (
		bck        = cliBck
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)
		objectName = t.Name()
		numObjs    = 10
		objPath    = "atime/obj-"
		errCh      = make(chan error, numObjs)
		nameCh     = make(chan string, numObjs)
		objs       = make([]string, 0, numObjs)
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{RemoteBck: true, Bck: bck})
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
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)

	timeAfterPut := time.Now()

	xactID, err = api.PrefetchList(baseParams, bck, objs)
	tassert.CheckFatal(t, err)
	args = api.XactReqArgs{ID: xactID, Kind: cmn.ActPrefetchObjects, Timeout: rebalanceTimeout}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)

	timeFormat := time.RFC3339Nano
	msg := &cmn.SelectMsg{Props: cmn.GetPropsAtime, TimeFormat: timeFormat, Prefix: objPath}
	bucketList, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	if len(bucketList.Entries) != numObjs {
		t.Errorf("Number of objects mismatch: expected %d, found %d", numObjs, len(bucketList.Entries))
	}
	for _, entry := range bucketList.Entries {
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
			Provider: cmn.ProviderAIS,
		}
		proxyURL      = tutils.RandomProxyURL(t)
		baseParams    = tutils.BaseAPIParams(proxyURL)
		objectName    = t.Name()
		objectContent = readers.NewBytesReader([]byte("dummy content"))
	)

	tutils.CreateFreshBucket(t, proxyURL, bck, nil)

	timeBeforePut := time.Now()
	err := api.PutObject(api.PutObjectArgs{BaseParams: baseParams, Bck: bck, Object: objectName, Reader: objectContent})
	tassert.CheckFatal(t, err)

	timeAfterPut := tutils.GetObjectAtime(t, baseParams, bck, objectName, time.RFC3339Nano)

	if !(timeAfterPut.After(timeBeforePut)) {
		t.Errorf("Expected atime after PUT (%s) to be after atime before PUT (%s).",
			timeAfterPut.Format(time.RFC3339Nano), timeBeforePut.Format(time.RFC3339Nano))
	}
}

// 1. Unregister target
// 2. Add bucket - unregistered target should miss the update
// 3. Reregister target
// 4. Put objects
// 5. Get objects - everything should succeed
func TestGetAndPutAfterReregisterWithMissedBucketUpdate(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	m := ioContext{
		t:               t,
		num:             10000,
		numGetsEachFile: 5,
	}

	m.initAndSaveCluState()
	m.expectTargets(2)

	target := m.startMaintenanceNoRebalance()

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

	rebID := m.stopMaintenance(target)

	m.puts()
	m.gets()

	m.ensureNoErrors()
	m.waitAndCheckCluState()
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	tutils.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID)
}

// 1. Unregister target
// 2. Add bucket - unregistered target should miss the update
// 3. Put objects
// 4. Reregister target - rebalance kicks in
// 5. Get objects - everything should succeed
func TestGetAfterReregisterWithMissedBucketUpdate(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	m := ioContext{
		t:               t,
		num:             10000,
		fileSize:        1024,
		numGetsEachFile: 5,
	}

	// Initialize ioContext
	m.initAndSaveCluState()
	m.expectTargets(2)

	targets := m.smap.Tmap.ActiveNodes()

	// Unregister target[0]
	args := &cmn.ActValRmNode{DaemonID: targets[0].ID(), SkipRebalance: true}
	_, err := api.StartMaintenance(tutils.BaseAPIParams(m.proxyURL), args)
	tassert.CheckFatal(t, err)
	tutils.WaitForClusterState(
		m.proxyURL,
		"remove target",
		m.smap.Version,
		m.originalProxyCount,
		m.originalTargetCount-1,
	)

	// Create ais bucket
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

	m.puts()

	// Reregister target 0
	rebID := m.stopMaintenance(targets[0])

	// Wait for rebalance and do gets
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	tutils.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID)

	m.gets()

	m.ensureNoErrors()
	m.waitAndCheckCluState()
}

func TestRenewRebalance(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	var (
		m = ioContext{
			t:                   t,
			num:                 10000,
			numGetsEachFile:     5,
			otherTasksToTrigger: 1,
		}
		rebID string
	)

	m.initAndSaveCluState()
	m.expectTargets(2)

	// Step 1: Unregister a target
	target := m.startMaintenanceNoRebalance()

	// Step 2: Create an ais bucket
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

	// Step 3: PUT objects in the bucket
	m.puts()

	baseParams := tutils.BaseAPIParams(m.proxyURL)

	// Step 4: Re-register target (triggers rebalance)
	m.stopMaintenance(target)
	xactArgs := api.XactReqArgs{Kind: cmn.ActRebalance, Timeout: rebalanceStartTimeout}
	err := tutils.WaitForRebalanceToStart(baseParams, xactArgs)
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

		rebID, err = api.StartXaction(baseParams, api.XactReqArgs{Kind: cmn.ActRebalance})
		tassert.CheckFatal(t, err)
		tlog.Logf("manually initiated rebalance\n")
	}()

	wg.Wait()
	args := api.XactReqArgs{ID: rebID, Kind: cmn.ActRebalance, Timeout: rebalanceTimeout}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckError(t, err)

	m.ensureNoErrors()
	m.waitAndCheckCluState()
}

func TestGetFromMirroredBucketWithLostMountpath(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	var (
		copies = 2
		m      = ioContext{
			t:               t,
			num:             5000,
			numGetsEachFile: 4,
		}
		baseParams = tutils.BaseAPIParams()
	)

	m.initAndSaveCluState()
	m.expectTargets(1)

	// Select one target at random
	target, _ := m.smap.GetRandTarget()
	mpList, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)
	if len(mpList.Available) < copies {
		t.Fatalf("%s requires at least %d mountpaths per target", t.Name(), copies)
	}

	// Step 1: Create a local bucket
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

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
	m.ensureNumCopies(copies)

	// Step 4: Remove a mountpath (simulates disk loss)
	mpath := mpList.Available[0]
	tlog.Logf("Remove mountpath %s on target %s\n", mpath, target.ID())
	err = api.DetachMountpath(baseParams, target, mpath)
	tassert.CheckFatal(t, err)

	// Step 5: GET objects from the bucket
	m.gets()

	m.ensureNumCopies(copies)

	// Step 6: Add previously removed mountpath
	tlog.Logf("Add mountpath %s on target %s\n", mpath, target.ID())
	err = api.AttachMountpath(baseParams, target, mpath)
	tassert.CheckFatal(t, err)

	tutils.WaitForRebalanceToComplete(t, baseParams, rebalanceTimeout)

	m.ensureNumCopies(copies)
	m.ensureNoErrors()
}

func TestGetFromMirroredBucketWithLostAllMountpath(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	m := ioContext{
		t:               t,
		num:             10000,
		numGetsEachFile: 4,
	}
	m.initAndSaveCluState()
	baseParams := tutils.BaseAPIParams(m.proxyURL)

	// Select one target at random
	target, _ := m.smap.GetRandTarget()
	mpList, err := api.GetMountpaths(baseParams, target)
	mpathCount := len(mpList.Available)
	tassert.CheckFatal(t, err)
	if mpathCount < 3 {
		t.Fatalf("%s requires at least 3 mountpaths per target", t.Name())
	}

	// Step 1: Create a local bucket
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

	// Step 2: Make the bucket redundant
	_, err = api.SetBucketProps(baseParams, m.bck, &cmn.BucketPropsToUpdate{
		Mirror: &cmn.MirrorConfToUpdate{
			Enabled: api.Bool(true),
			Copies:  api.Int64(int64(mpathCount)),
		},
	})
	if err != nil {
		t.Fatalf("Failed to make the bucket redundant: %v", err)
	}

	// Step 3: PUT objects in the bucket
	m.puts()
	m.ensureNumCopies(mpathCount)

	// Step 4: Remove almost all mountpaths
	tlog.Logf("Remove mountpaths on target %s\n", target.ID())
	for _, mpath := range mpList.Available[1:] {
		err = api.DetachMountpath(baseParams, target, mpath)
		tassert.CheckFatal(t, err)
	}

	// Step 5: GET objects from the bucket
	m.gets()

	// Step 6: Add previously removed mountpath
	tlog.Logf("Add mountpaths on target %s\n", target.ID())
	for _, mpath := range mpList.Available[1:] {
		err = api.AttachMountpath(baseParams, target, mpath)
		tassert.CheckFatal(t, err)
	}

	tutils.WaitForRebalanceToComplete(t, baseParams, rebalanceTimeout)

	m.ensureNumCopies(mpathCount)
	m.ensureNoErrors()
}

// 1. Start rebalance
// 2. Start changing the primary proxy
// 3. IC must survive and rebalance must finish
func TestICRebalance(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true, RequiredDeployment: tutils.ClusterTypeLocal})

	var (
		m = ioContext{
			t:   t,
			num: 25000,
		}
		rebID string
	)

	m.initAndSaveCluState()
	m.expectTargets(3)
	m.expectProxies(3)
	psi, err := m.smap.GetRandProxy(true /*exclude primary*/)
	tassert.CheckFatal(t, err)
	m.proxyURL = psi.URL(cmn.NetworkPublic)
	icNode := tutils.GetICProxy(t, m.smap, psi.ID())

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

	m.puts()

	baseParams := tutils.BaseAPIParams(m.proxyURL)

	tlog.Logf("Manually initiated rebalance\n")
	rebID, err = api.StartXaction(baseParams, api.XactReqArgs{Kind: cmn.ActRebalance})
	tassert.CheckFatal(t, err)

	xactArgs := api.XactReqArgs{Kind: cmn.ActRebalance, Timeout: rebalanceStartTimeout}
	tutils.WaitForRebalanceToStart(baseParams, xactArgs)

	tlog.Logf("Killing %s\n", icNode)
	// cmd and args are the original command line of how the proxy is started
	cmd, err := tutils.KillNode(icNode)
	tassert.CheckFatal(t, err)

	proxyCnt := m.smap.CountActiveProxies()
	smap, err := tutils.WaitForClusterState(m.proxyURL, "designate new primary", m.smap.Version, proxyCnt-1, 0)
	tassert.CheckError(t, err)

	// re-construct the command line to start the original proxy but add the current primary proxy to the args
	err = tutils.RestoreNode(cmd, false, "proxy (prev primary)")
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForClusterState(m.proxyURL, "restore", smap.Version, proxyCnt, 0)
	tassert.CheckFatal(t, err)
	if _, ok := smap.Pmap[psi.ID()]; !ok {
		t.Fatalf("Previous primary proxy did not rejoin the cluster")
	}
	checkSmaps(t, m.proxyURL)

	tlog.Logf("Wait for rebalance: %s\n", rebID)
	args := api.XactReqArgs{ID: rebID, Kind: cmn.ActRebalance, Timeout: rebalanceTimeout}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckError(t, err)

	m.waitAndCheckCluState()
}

// 1. Start decommissioning a target with rebalance
// 2. Start changing the primary proxy
// 3. IC must survive, rebalance must finish, and the target must be gone
func TestICDecommission(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true, RequiredDeployment: tutils.ClusterTypeLocal})

	var (
		err error
		m   = ioContext{
			t:   t,
			num: 25000,
		}
	)

	m.initAndSaveCluState()
	m.expectTargets(3)
	m.expectProxies(3)
	psi, err := m.smap.GetRandProxy(true /*exclude primary*/)
	tassert.CheckFatal(t, err)
	m.proxyURL = psi.URL(cmn.NetworkPublic)
	tlog.Logf("Monitoring node: %s\n", psi)
	icNode := tutils.GetICProxy(t, m.smap, psi.ID())

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

	m.puts()

	baseParams := tutils.BaseAPIParams(m.proxyURL)
	tsi, err := m.smap.GetRandTarget()
	tassert.CheckFatal(t, err)

	args := &cmn.ActValRmNode{DaemonID: tsi.ID(), SkipRebalance: true}
	_, err = api.StartMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)

	defer func() {
		args := &cmn.ActValRmNode{DaemonID: tsi.ID()}
		rebID, err := api.StopMaintenance(baseParams, args)
		tassert.CheckFatal(t, err)
		tutils.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID)
		tassert.CheckFatal(t, err)
	}()

	tassert.CheckFatal(t, err)
	tlog.Logf("Killing %s\n", icNode)

	// cmd and args are the original command line of how the proxy is started
	cmd, err := tutils.KillNode(icNode)
	tassert.CheckFatal(t, err)

	proxyCnt := m.smap.CountActiveProxies()
	smap, err := tutils.WaitForClusterState(m.proxyURL, "designate new primary", m.smap.Version, proxyCnt-1, 0)
	tassert.CheckError(t, err)

	// re-construct the command line to start the original proxy but add the current primary proxy to the args
	err = tutils.RestoreNode(cmd, false, "proxy (prev primary)")
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForClusterState(m.proxyURL, "restore", smap.Version, proxyCnt, 0)
	tassert.CheckFatal(t, err)
	if _, ok := smap.Pmap[psi.ID()]; !ok {
		t.Fatalf("Previous primary proxy did not rejoin the cluster")
	}
	checkSmaps(t, m.proxyURL)

	_, err = tutils.WaitForClusterState(m.proxyURL, "decommission target",
		m.smap.Version, m.smap.CountProxies(), m.smap.CountTargets()-1)
	tassert.CheckFatal(t, err)
}

func TestSingleResilver(t *testing.T) {
	m := ioContext{t: t}
	m.initAndSaveCluState()
	baseParams := tutils.BaseAPIParams(m.proxyURL)

	// Select a random target
	target, _ := m.smap.GetRandTarget()

	// Start resilvering just on the target
	args := api.XactReqArgs{Kind: cmn.ActResilver, Node: target.DaemonID}
	id, err := api.StartXaction(baseParams, args)
	tassert.CheckFatal(t, err)

	// Wait for resilver
	args = api.XactReqArgs{ID: id, Kind: cmn.ActResilver, Timeout: rebalanceTimeout}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)

	// Make sure other nodes were not resilvered
	args = api.XactReqArgs{ID: id}
	xactStats, err := api.QueryXactionStats(baseParams, args)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(xactStats) == 1, "expected only 1 resilver")
}
