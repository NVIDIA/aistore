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
	"github.com/NVIDIA/aistore/containers"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/readers"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

// Intended for a deployment with multiple targets
// 1. Create ais bucket
// 2. Unregister target T
// 3. PUT large amount of objects into the ais bucket
// 4. GET the objects while simultaneously registering the target T
func TestGetAndReRegisterInParallel(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	m := ioContext{
		t:               t,
		num:             50000,
		numGetsEachFile: 3,
		fileSize:        10 * cmn.KiB,
	}

	m.saveClusterState()
	m.expectTargets(2)

	// Step 1.
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	// Step 2.
	target := m.unregisterTarget()
	defer tutils.WaitForRebalanceToComplete(t, baseParams)

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
		m.reregisterTarget(target)
	}()
	wg.Wait()

	m.ensureNoErrors()
	m.assertClusterState()
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

	m.saveClusterState()
	m.expectTargets(2)
	m.expectProxies(3)

	// Step 1.
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	// Step 2.
	target := m.unregisterTarget()
	defer tutils.WaitForRebalanceToComplete(t, baseParams, time.Minute)

	// Step 3.
	_, newPrimaryURL, err := chooseNextProxy(m.smap)
	// use a new proxyURL because primaryCrashElectRestart has a side-effect:
	// it changes the primary proxy. Without the change tutils.PutRandObjs is
	// failing while the current primary is restarting and rejoining
	m.proxyURL = newPrimaryURL
	tassert.CheckFatal(t, err)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		primaryCrashElectRestart(t)
	}()

	// PUT phase is timed to ensure it doesn't finish before primaryCrashElectRestart() begins
	time.Sleep(5 * time.Second)
	m.puts()
	wg.Wait()

	// Step 4.
	wg.Add(3)
	go func() {
		defer wg.Done()
		m.reregisterTarget(target)
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
	m.assertClusterState()
}

// Similar to TestGetAndReRegisterInParallel, but instead of unregister, we kill the target
// 1. Kill registered target and wait for Smap to updated
// 2. Create ais bucket
// 3. PUT large amounts of objects into ais bucket
// 4. Get the objects while simultaneously registering the target
func TestGetAndRestoreInParallel(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	var (
		m = ioContext{
			t:               t,
			num:             20000,
			numGetsEachFile: 5,
			fileSize:        cmn.KiB * 2,
		}
		targetURL  string
		targetID   string
		targetNode *cluster.Snode
	)

	m.saveClusterState()
	m.expectTargets(3)

	// Step 1
	// Select a random target
	for _, v := range m.smap.Tmap {
		targetURL = v.PublicNet.DirectURL
		targetID = v.ID()
		targetNode = v
		break
	}
	tutils.Logf("Killing target: %s - %s\n", targetURL, targetID)
	tcmd, err := kill(targetNode)
	tassert.CheckFatal(t, err)

	proxyURL := tutils.RandomProxyURL(t)
	m.smap, err = tutils.WaitForPrimaryProxy(proxyURL, "to update smap", m.smap.Version,
		testing.Verbose(), m.originalProxyCount, m.originalTargetCount-1)
	tassert.CheckError(t, err)

	// Step 2
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	// Step 3
	m.puts()

	// Step 4
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		time.Sleep(4 * time.Second)
		restore(tcmd, false, "target")
	}()
	go func() {
		defer wg.Done()
		m.gets()
	}()
	wg.Wait()

	m.ensureNoErrors()
	m.assertClusterState()
	tutils.WaitForRebalanceToComplete(m.t, tutils.BaseAPIParams(m.proxyURL))
}

func TestUnregisterPreviouslyUnregisteredTarget(t *testing.T) {
	m := ioContext{
		t: t,
	}

	m.saveClusterState()
	m.expectTargets(2)

	target := m.unregisterTarget()

	// Unregister same target again.
	err := tutils.UnregisterNode(m.proxyURL, target.ID())
	tutils.CheckErrIsNotFound(t, err)

	n := tutils.GetClusterMap(t, m.proxyURL).CountTargets()
	if n != m.originalTargetCount-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets", m.originalTargetCount-1, n)
	}

	// Register target (bring cluster to normal state)
	m.reregisterTarget(target)
	m.assertClusterState()
	tutils.WaitForRebalanceToComplete(m.t, tutils.BaseAPIParams(m.proxyURL))
}

func TestRegisterAndUnregisterTargetAndPutInParallel(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	m := ioContext{
		t:   t,
		num: 10000,
	}

	m.saveClusterState()
	m.expectTargets(3)

	targets := tutils.ExtractTargetNodes(m.smap)

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	// Unregister target 0
	tutils.Logf("Unregister target %s\n", targets[0].ID())
	err := tutils.UnregisterNode(m.proxyURL, targets[0].ID())
	tassert.CheckFatal(t, err)
	n := tutils.GetClusterMap(t, m.proxyURL).CountTargets()
	if n != m.originalTargetCount-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets",
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
		tutils.Logf("Register target %s\n", targets[0].ID())
		err = tutils.JoinCluster(m.proxyURL, targets[0], m.smap)
		tassert.CheckFatal(t, err)
	}()

	// Unregister target 1 in parallel
	go func() {
		defer wg.Done()
		tutils.Logf("Unregister target %s\n", targets[1].ID())
		err = tutils.UnregisterNode(m.proxyURL, targets[1].ID())
		tassert.CheckFatal(t, err)
	}()

	// Wait for everything to end
	wg.Wait()

	// Register target 1 to bring cluster to original state
	m.reregisterTarget(targets[1])

	// wait for rebalance to complete
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	tutils.WaitForRebalanceToComplete(t, baseParams, rebalanceTimeout)

	m.assertClusterState()
}

func TestAckRebalance(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	m := ioContext{
		t:             t,
		num:           30000,
		getErrIsFatal: true,
	}

	m.saveClusterState()
	m.expectTargets(3)

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	target := m.unregisterTarget()

	// Start putting files into bucket.
	m.puts()

	m.reregisterTarget(target)

	// Wait for everything to finish.
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	tutils.WaitForRebalanceToComplete(t, baseParams, rebalanceTimeout)

	m.gets()

	m.ensureNoErrors()
	m.assertClusterState()
}

func TestStressRebalance(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	m := &ioContext{
		t: t,
	}

	m.saveClusterState()
	m.expectTargets(4)

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	for i := 1; i <= 3; i++ {
		tutils.Logf("Iteration #%d ======\n", i)
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

	m.saveClusterState()

	tgts := tutils.ExtractTargetNodes(m.smap)
	i1 := rand.Intn(len(tgts))
	i2 := (i1 + 1) % len(tgts)
	target1, target2 := tgts[i1], tgts[i2]

	// Unregister targets.
	tutils.Logf("Unregister targets: %s and %s\n", target1.URL(cmn.NetworkPublic), target2.URL(cmn.NetworkPublic))
	err := tutils.RemoveNodeFromSmap(m.proxyURL, target1.ID())
	tassert.CheckFatal(t, err)
	time.Sleep(time.Second)
	err = tutils.RemoveNodeFromSmap(m.proxyURL, target2.ID())
	tassert.CheckFatal(t, err)

	_, err = tutils.WaitForPrimaryProxy(
		m.proxyURL,
		"to targets are removed",
		m.smap.Version, testing.Verbose(),
		m.originalProxyCount,
		m.originalTargetCount-2,
	)
	tassert.CheckFatal(m.t, err)

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
	tutils.Logf("Register 1st target %s\n", target1.URL(cmn.NetworkPublic))
	err = tutils.JoinCluster(m.proxyURL, target1, m.smap)
	tassert.CheckFatal(t, err)

	// random sleep between the first and the second join
	time.Sleep(time.Duration(rand.Intn(3)+1) * time.Second)

	tutils.Logf("Register 2nd target %s\n", target2.URL(cmn.NetworkPublic))
	err = tutils.JoinCluster(m.proxyURL, target2, m.smap)
	tassert.CheckFatal(t, err)

	_, err = tutils.WaitForPrimaryProxy(
		m.proxyURL,
		"to targets are registered",
		m.smap.Version, testing.Verbose(),
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
	m.assertClusterState()
}

func TestRebalanceAfterUnregisterAndReregister(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	m := ioContext{
		t:   t,
		num: 10000,
	}

	m.saveClusterState()
	m.expectTargets(3)

	targets := tutils.ExtractTargetNodes(m.smap)

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	// Unregister target
	target0, target1 := targets[0], targets[1]
	tutils.Logf("Unregister target %s\n", target0.URL(cmn.NetworkPublic))
	err := tutils.UnregisterNode(m.proxyURL, target0.ID())
	tassert.CheckFatal(t, err)

	_, err = tutils.WaitForPrimaryProxy(
		m.proxyURL,
		"to target is removed",
		m.smap.Version, testing.Verbose(),
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
		tutils.Logf("Register target %s\n", target0.URL(cmn.NetworkPublic))
		err = tutils.JoinCluster(m.proxyURL, target0, m.smap)
		tassert.CheckFatal(t, err)
	}()

	// Unregister target 1 in parallel
	go func() {
		defer wg.Done()
		tutils.Logf("Unregister target %s\n", target1.URL(cmn.NetworkPublic))
		err = tutils.RemoveNodeFromSmap(m.proxyURL, target1.ID())
		tassert.CheckFatal(t, err)
	}()

	// Wait for everything to end
	wg.Wait()

	// Register target 1 to bring cluster to original state
	sleep := time.Duration(rand.Intn(5))*time.Second + time.Millisecond
	time.Sleep(sleep)
	tutils.Logf("Register target %s\n", target1.URL(cmn.NetworkPublic))
	err = tutils.JoinCluster(m.proxyURL, target1, m.smap)
	tassert.CheckFatal(t, err)
	_, err = tutils.WaitForPrimaryProxy(
		m.proxyURL,
		"to targets are registered",
		m.smap.Version, testing.Verbose(),
		m.originalProxyCount,
		m.originalTargetCount,
	)
	tassert.CheckFatal(m.t, err)

	tutils.Logf("Wait for rebalance...\n")
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	tutils.WaitForRebalanceToComplete(t, baseParams, rebalanceTimeout)

	m.gets()

	m.ensureNoErrors()
	m.assertClusterState()
}

func TestPutDuringRebalance(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	m := ioContext{
		t:   t,
		num: 10000,
	}

	m.saveClusterState()
	m.expectTargets(3)

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	target := m.unregisterTarget()

	// Start putting files and register target in parallel.
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.puts()
	}()

	// Sleep some time to wait for PUT operations to begin.
	time.Sleep(3 * time.Second)

	m.reregisterTarget(target)

	// Wait for everything to finish.
	wg.Wait()
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	tutils.WaitForRebalanceToComplete(t, baseParams, rebalanceTimeout)

	// Main check - try to read all objects.
	m.gets()

	m.checkObjectDistribution(t)
	m.assertClusterState()
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

	m.saveClusterState()
	m.expectTargets(2)

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

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
	tutils.Logf("Disable mountpath on target %s\n", selectedTarget.ID())
	err = api.DisableMountpath(baseParams, selectedTarget.ID(), mpath)
	tassert.CheckFatal(t, err)

	// Unregister another target
	tutils.Logf("Unregister target %s\n", killTarget.URL(cmn.NetworkPublic))
	err = tutils.UnregisterNode(m.proxyURL, killTarget.ID())
	tassert.CheckFatal(t, err)
	smap, err := tutils.WaitForPrimaryProxy(
		m.proxyURL,
		"target is gone",
		m.smap.Version, testing.Verbose(),
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
	err = tutils.JoinCluster(m.proxyURL, killTarget, m.smap)
	tassert.CheckFatal(t, err)

	// enable mountpath
	err = api.EnableMountpath(baseParams, selectedTarget, mpath)
	tassert.CheckFatal(t, err)

	// wait until GETs are done while 2 rebalance are running
	wg.Wait()

	// make sure that the cluster has all targets enabled
	_, err = tutils.WaitForPrimaryProxy(
		m.proxyURL,
		"to join target back",
		smap.Version, testing.Verbose(),
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
	m.assertClusterState()
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

	m.saveClusterState()
	m.expectTargets(1)

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	target := tutils.ExtractTargetNodes(m.smap)[0]
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
		err = api.DisableMountpath(baseParams, target.ID(), mp)
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

	m.saveClusterState()
	m.expectTargets(3)

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	target := m.unregisterTarget()

	m.puts()

	// Start getting objects and register target in parallel.
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.gets()
	}()

	m.reregisterTarget(target)

	// Wait for everything to finish.
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	tutils.WaitForRebalanceToComplete(t, baseParams, rebalanceTimeout)
	wg.Wait()

	// Get objects once again to check if they are still accessible after rebalance.
	m.gets()

	m.ensureNoErrors()
	m.assertClusterState()
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

	m.saveClusterState()
	m.expectTargets(3)

	targets := tutils.ExtractTargetNodes(m.smap)

	// Unregister targets
	for i := 0; i < unregisterTargetCount; i++ {
		err := tutils.UnregisterNode(m.proxyURL, targets[i].ID())
		tassert.CheckError(t, err)
		n := tutils.GetClusterMap(t, m.proxyURL).CountTargets()
		if n != m.originalTargetCount-(i+1) {
			t.Errorf("%d targets expected after unregister, actually %d targets",
				m.originalTargetCount-(i+1), n)
		}
		tutils.Logf("Unregistered target %s: the cluster now has %d targets\n",
			targets[i].URL(cmn.NetworkPublic), n)
	}

	wg := &sync.WaitGroup{}
	wg.Add(unregisterTargetCount)
	for i := 0; i < unregisterTargetCount; i++ {
		go func(number int) {
			defer wg.Done()

			err := tutils.JoinCluster(m.proxyURL, targets[number], m.smap)
			tassert.CheckError(t, err)
		}(i)
	}

	wg.Add(newBucketCount)
	for i := 0; i < newBucketCount; i++ {
		bck := m.bck
		bck.Name += strconv.Itoa(i)

		go func() {
			defer wg.Done()
			tutils.CreateFreshBucket(t, m.proxyURL, bck)
		}()

		defer tutils.DestroyBucket(t, m.proxyURL, bck)
	}
	wg.Wait()
	m.assertClusterState()
}

func TestAddAndRemoveMountpath(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	var (
		m = ioContext{
			t:               t,
			num:             5000,
			numGetsEachFile: 2,
		}
		baseParams = tutils.BaseAPIParams()
	)

	m.saveClusterState()
	m.expectTargets(2)

	target := tutils.ExtractTargetNodes(m.smap)[0]
	// Remove all mountpaths for one target
	oldMountpaths, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)

	for _, mpath := range oldMountpaths.Available {
		err = api.RemoveMountpath(baseParams, target.ID(), mpath)
		tassert.CheckFatal(t, err)
	}

	// Check if mountpaths were actually removed
	mountpaths, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)

	if len(mountpaths.Available) != 0 {
		t.Fatalf("Target should not have any paths available")
	}

	// Create ais bucket
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	// Add target mountpath again
	for _, mpath := range oldMountpaths.Available {
		err = api.AddMountpath(baseParams, target, mpath)
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

	m.saveClusterState()
	m.expectTargets(1)
	target := tutils.ExtractTargetNodes(m.smap)[0]

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)

	if containers.DockerRunning() {
		err := containers.DockerCreateMpathDir(0, newMountpath)
		tassert.CheckFatal(t, err)
	} else {
		err := cmn.CreateDir(newMountpath)
		tassert.CheckFatal(t, err)
	}

	defer func() {
		if !containers.DockerRunning() {
			os.RemoveAll(newMountpath)
		}
		tutils.DestroyBucket(t, m.proxyURL, m.bck)
	}()

	m.puts()

	// Add new mountpath to target
	err := api.AddMountpath(baseParams, target, newMountpath)
	tassert.CheckFatal(t, err)

	tutils.WaitForRebalanceToComplete(t, tutils.BaseAPIParams(m.proxyURL), rebalanceTimeout)

	m.gets()

	// Remove new mountpath from target
	if containers.DockerRunning() {
		if err := api.RemoveMountpath(baseParams, target.ID(), newMountpath); err != nil {
			t.Error(err.Error())
		}
	} else {
		err = api.RemoveMountpath(baseParams, target.ID(), newMountpath)
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

	m.saveClusterState()
	m.expectTargets(1)

	targets := tutils.ExtractTargetNodes(m.smap)

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)

	defer func() {
		if !containers.DockerRunning() {
			os.RemoveAll(newMountpath)
		}
		tutils.DestroyBucket(t, m.proxyURL, m.bck)
	}()

	// PUT random objects
	m.puts()

	if containers.DockerRunning() {
		err := containers.DockerCreateMpathDir(0, newMountpath)
		tassert.CheckFatal(t, err)
		for _, target := range targets {
			err = api.AddMountpath(baseParams, target, newMountpath)
			tassert.CheckFatal(t, err)
		}
	} else {
		// Add new mountpath to all targets
		for idx, target := range targets {
			mountpath := filepath.Join(newMountpath, fmt.Sprintf("%d", idx))
			cmn.CreateDir(mountpath)
			err := api.AddMountpath(baseParams, target, mountpath)
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
			if err := api.RemoveMountpath(baseParams, target.ID(), newMountpath); err != nil {
				t.Error(err.Error())
			}
		}
	} else {
		for idx, target := range targets {
			mountpath := filepath.Join(newMountpath, fmt.Sprintf("%d", idx))
			os.RemoveAll(mountpath)
			if err := api.RemoveMountpath(baseParams, target.ID(), mountpath); err != nil {
				t.Error(err.Error())
			}
		}
	}

	m.ensureNoErrors()
}

func TestDisableAndEnableMountpath(t *testing.T) {
	var (
		m = ioContext{
			t:               t,
			num:             5000,
			numGetsEachFile: 2,
		}
		baseParams = tutils.BaseAPIParams()
	)

	m.saveClusterState()
	m.expectTargets(1)

	target := tutils.ExtractTargetNodes(m.smap)[0]
	// Remove all mountpaths for one target
	oldMountpaths, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)

	for _, mpath := range oldMountpaths.Available {
		err := api.DisableMountpath(baseParams, target.ID(), mpath)
		tassert.CheckFatal(t, err)
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
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	// Add target mountpath again
	for _, mpath := range oldMountpaths.Available {
		err := api.EnableMountpath(baseParams, target, mpath)
		tassert.CheckFatal(t, err)
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

	tutils.Logf("waiting for ais bucket %s to appear on all targets\n", m.bck)
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
	m.saveClusterState()
	m.expectProxies(2)

	// Step 2.
	origID, origURL := m.smap.Primary.ID(), m.smap.Primary.PublicNet.DirectURL
	nextProxyID, nextProxyURL, _ := chooseNextProxy(m.smap)

	tutils.DestroyBucket(t, m.proxyURL, m.bck)

	tutils.CreateFreshBucket(t, nextProxyURL, m.bck)
	tutils.Logf("Created bucket %s via non-primary %s\n", m.bck, nextProxyID)

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
	tutils.Logf("Destroyed bucket %s via non-primary %s/%s\n", m.bck, origID, origURL)
}

func TestAtimeRebalance(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	m := ioContext{
		t:               t,
		num:             2000,
		numGetsEachFile: 2,
	}

	m.saveClusterState()
	m.expectTargets(2)

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	target := m.unregisterTarget()

	m.puts()

	// Get atime in a format that includes nanoseconds to properly check if it
	// was updated in atime cache (if it wasn't, then the returned atime would
	// be different from the original one, but the difference could be very small).
	msg := &cmn.SelectMsg{TimeFormat: time.StampNano}
	msg.AddProps(cmn.GetPropsAtime, cmn.GetPropsStatus)
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	bucketList, err := api.ListObjects(baseParams, m.bck, msg, 0)
	tassert.CheckFatal(t, err)

	objNames := make(cmn.SimpleKVs, 10)
	for _, entry := range bucketList.Entries {
		objNames[entry.Name] = entry.Atime
	}

	m.reregisterTarget(target)

	// make sure that the cluster has all targets enabled
	_, err = tutils.WaitForPrimaryProxy(
		m.proxyURL,
		"to join target back",
		m.smap.Version, testing.Verbose(),
		m.originalProxyCount,
		m.originalTargetCount,
	)
	tassert.CheckFatal(t, err)

	tutils.WaitForRebalanceToComplete(t, baseParams, rebalanceTimeout)

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

	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

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

	tutils.CheckSkip(t, tutils.SkipTestArgs{Cloud: true, Bck: bck})
	api.DeleteObject(baseParams, bck, objectName)
	defer api.DeleteObject(baseParams, bck, objectName)

	tutils.PutObjectInCloudBucketWithoutCachingLocally(t, proxyURL, bck, objectName, objectContent)

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
		bck           = cliBck
		proxyURL      = tutils.RandomProxyURL(t)
		baseParams    = tutils.BaseAPIParams(proxyURL)
		objectName    = t.Name()
		objectContent = readers.NewBytesReader([]byte("dummy content"))
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{Cloud: true, Bck: bck})
	api.DeleteObject(baseParams, bck, objectName)
	defer api.DeleteObject(baseParams, bck, objectName)

	tutils.PutObjectInCloudBucketWithoutCachingLocally(t, proxyURL, bck, objectName, objectContent)
	time.Sleep(25 * time.Millisecond)
	timeAfterPut := time.Now()

	xactID, err := api.PrefetchList(baseParams, bck, []string{objectName})
	tassert.CheckFatal(t, err)
	args := api.XactReqArgs{ID: xactID, Kind: cmn.ActPrefetch, Timeout: rebalanceTimeout}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)

	atime := tutils.GetObjectAtime(t, baseParams, bck, objectName, time.RFC3339Nano)

	if atime.After(timeAfterPut) {
		t.Errorf("Atime should not be updated after prefetch (got: atime after PUT: %s, atime after GET: %s).",
			timeAfterPut.Format(time.RFC3339Nano), atime.Format(time.RFC3339Nano))
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

	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

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

	m.saveClusterState()
	m.expectTargets(2)

	target := m.unregisterTarget()

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	m.reregisterTarget(target)

	m.puts()
	m.gets()

	m.ensureNoErrors()
	m.assertClusterState()
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	tutils.WaitForRebalanceToComplete(t, baseParams)
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
	m.saveClusterState()
	m.expectTargets(2)

	targets := tutils.ExtractTargetNodes(m.smap)

	// Unregister target 0
	err := tutils.UnregisterNode(m.proxyURL, targets[0].ID())
	tassert.CheckFatal(t, err)
	n := tutils.GetClusterMap(t, m.proxyURL).CountTargets()
	if n != m.originalTargetCount-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets", m.originalTargetCount-1, n)
	}

	// Create ais bucket
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	m.puts()

	// Reregister target 0
	m.reregisterTarget(targets[0])

	// Wait for rebalance and do gets
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	tutils.WaitForRebalanceToComplete(t, baseParams)

	m.gets()

	m.ensureNoErrors()
	m.assertClusterState()
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

	m.saveClusterState()
	m.expectTargets(2)

	// Step 1: Unregister a target
	target := m.unregisterTarget()

	// Step 2: Create an ais bucket
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	// Step 3: PUT objects in the bucket
	m.puts()

	baseParams := tutils.BaseAPIParams(m.proxyURL)

	// Step 4: Re-register target (triggers rebalance)
	m.reregisterTarget(target)
	xactArgs := api.XactReqArgs{Kind: cmn.ActRebalance, Timeout: rebalanceStartTimeout}
	err := api.WaitForXactionToStart(baseParams, xactArgs)
	tassert.CheckError(t, err)
	tutils.Logf("automatic rebalance started\n")

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
		tutils.Logf("manually initiated rebalance\n")
	}()

	wg.Wait()
	args := api.XactReqArgs{ID: rebID, Kind: cmn.ActRebalance, Timeout: rebalanceTimeout}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckError(t, err)

	m.ensureNoErrors()
	m.assertClusterState()
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

	m.saveClusterState()
	m.expectTargets(1)

	// Select one target at random
	target := tutils.ExtractTargetNodes(m.smap)[0]
	mpList, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)
	if len(mpList.Available) < copies {
		t.Fatalf("%s requires at least %d mountpaths per target", t.Name(), copies)
	}

	// Step 1: Create a local bucket
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	// Step 2: Make the bucket redundant
	_, err = api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
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
	tutils.Logf("Remove mountpath %s on target %s\n", mpath, target.ID())
	err = api.RemoveMountpath(baseParams, target.ID(), mpath)
	tassert.CheckFatal(t, err)

	// Step 5: GET objects from the bucket
	m.gets()

	m.ensureNumCopies(copies)

	// Step 6: Add previously removed mountpath
	tutils.Logf("Add mountpath %s on target %s\n", mpath, target.ID())
	err = api.AddMountpath(baseParams, target, mpath)
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
	m.saveClusterState()
	baseParams := tutils.BaseAPIParams(m.proxyURL)

	// Select one target at random
	target := tutils.ExtractTargetNodes(m.smap)[0]
	mpList, err := api.GetMountpaths(baseParams, target)
	mpathCount := len(mpList.Available)
	tassert.CheckFatal(t, err)
	if mpathCount < 3 {
		t.Fatalf("%s requires at least 3 mountpaths per target", t.Name())
	}

	// Step 1: Create a local bucket
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	// Step 2: Make the bucket redundant
	_, err = api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
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
	tutils.Logf("Remove mountpaths on target %s\n", target.ID())
	for _, mpath := range mpList.Available[1:] {
		err = api.RemoveMountpath(baseParams, target.ID(), mpath)
		tassert.CheckFatal(t, err)
	}

	// Step 5: GET objects from the bucket
	m.gets()

	// Step 6: Add previously removed mountpath
	tutils.Logf("Add mountpaths on target %s\n", target.ID())
	for _, mpath := range mpList.Available[1:] {
		err = api.AddMountpath(baseParams, target, mpath)
		tassert.CheckFatal(t, err)
	}

	tutils.WaitForRebalanceToComplete(t, baseParams, rebalanceTimeout)

	m.ensureNumCopies(mpathCount)
	m.ensureNoErrors()
}
