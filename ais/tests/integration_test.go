// Package ais_test contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais_test

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/tutils/tassert"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tutils"
)

const rebalanceObjectDistributionTestCoef = 0.3

type repFile struct {
	repetitions int
	filename    string
}

type metadata struct {
	t                   *testing.T
	smap                cluster.Smap
	semaphore           chan struct{}
	controlCh           chan struct{}
	repFilenameCh       chan repFile
	wg                  *sync.WaitGroup
	bucket              string
	otherTasksToTrigger int
	originalTargetCount int
	originalProxyCount  int
	num                 int
	numGetsEachFile     int
	fileSize            uint64
	numGetErrs          atomic.Uint64
	getsCompleted       atomic.Uint64
	proxyURL            string
}

func (m *metadata) saveClusterState() {
	m.init()
	m.smap = getClusterMap(m.t, m.proxyURL)
	m.originalTargetCount = len(m.smap.Tmap)
	m.originalProxyCount = len(m.smap.Pmap)
	tutils.Logf("Number of targets %d, number of proxies %d\n", m.originalTargetCount, m.originalProxyCount)
}

func (m *metadata) init() {
	m.proxyURL = getPrimaryURL(m.t, proxyURLReadOnly)
	if m.fileSize == 0 {
		m.fileSize = cmn.KiB
	}
	if m.num > 0 {
		m.repFilenameCh = make(chan repFile, m.num)
	}
	if m.otherTasksToTrigger > 0 {
		m.controlCh = make(chan struct{}, m.otherTasksToTrigger)
	}
	m.semaphore = make(chan struct{}, 10) // 10 concurrent GET requests at a time
	m.wg = &sync.WaitGroup{}
	if m.bucket == "" {
		m.bucket = m.t.Name() + "Bucket"
	}
}

func (m *metadata) assertClusterState() {
	smap, err := tutils.WaitForPrimaryProxy(
		m.proxyURL,
		"to check cluster state",
		m.smap.Version, testing.Verbose(),
		m.originalProxyCount,
		m.originalTargetCount,
	)
	tassert.CheckFatal(m.t, err)

	proxyCount := len(smap.Pmap)
	targetCount := len(smap.Tmap)
	if targetCount != m.originalTargetCount ||
		proxyCount != m.originalProxyCount {
		m.t.Errorf(
			"cluster state is not preserved. targets (before: %d, now: %d); proxies: (before: %d, now: %d)",
			targetCount, m.originalTargetCount,
			proxyCount, m.originalProxyCount,
		)
	}
}

func (m *metadata) checkObjectDistribution(t *testing.T) {
	var (
		requiredCount     = int64(rebalanceObjectDistributionTestCoef * (float64(m.num) / float64(m.originalTargetCount)))
		targetObjectCount = make(map[string]int64)
	)
	tutils.Logf("Checking if each target has a required number of object in bucket %s...\n", m.bucket)
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	bucketList, err := api.ListBucket(baseParams, m.bucket, &cmn.SelectMsg{Props: cmn.GetTargetURL}, 0)
	tassert.CheckFatal(t, err)
	for _, obj := range bucketList.Entries {
		targetObjectCount[obj.TargetURL]++
	}
	if len(targetObjectCount) != m.originalTargetCount {
		t.Fatalf("Rebalance error, %d/%d targets received no objects from bucket %s\n",
			m.originalTargetCount-len(targetObjectCount), m.originalTargetCount, m.bucket)
	}
	for targetURL, objCount := range targetObjectCount {
		if objCount < requiredCount {
			t.Fatalf("Rebalance error, target %s didn't receive required number of objects\n", targetURL)
		}
	}
}

func (m *metadata) puts(dontFail ...bool) int {
	sgl := tutils.Mem2.NewSGL(int64(m.fileSize))
	defer sgl.Free()

	// With the current design, there exists a brief period of time
	// during which GET errors can occur - see the timeline comment below
	filenameCh := make(chan string, m.num)
	errCh := make(chan error, m.num)

	tutils.Logf("PUT %d objects into bucket %s...\n", m.num, m.bucket)
	start := time.Now()
	tutils.PutRandObjs(m.proxyURL, m.bucket, SmokeDir, readerType, SmokeStr, m.fileSize, m.num, errCh, filenameCh, sgl)
	if len(dontFail) == 0 {
		selectErr(errCh, "put", m.t, false)
	}
	close(filenameCh)
	close(errCh)
	tutils.Logf("PUT time: %v\n", time.Since(start))
	for f := range filenameCh {
		m.repFilenameCh <- repFile{repetitions: m.numGetsEachFile, filename: f}
	}
	return len(errCh)
}

func (m *metadata) gets() {
	for i := 0; i < 10; i++ {
		m.semaphore <- struct{}{}
	}
	if m.numGetsEachFile == 1 {
		tutils.Logf("GET each of the %d objects from bucket %s...\n", m.num, m.bucket)
	} else {
		tutils.Logf("GET each of the %d objects %d times from bucket %s...\n", m.num, m.numGetsEachFile, m.bucket)
	}
	baseParams := tutils.DefaultBaseAPIParams(m.t)
	for i := 0; i < m.num*m.numGetsEachFile; i++ {
		go func() {
			<-m.semaphore
			defer func() {
				m.semaphore <- struct{}{}
				m.wg.Done()
			}()
			repFile := <-m.repFilenameCh
			if repFile.repetitions > 0 {
				repFile.repetitions--
				m.repFilenameCh <- repFile
			}
			_, err := api.GetObject(baseParams, m.bucket, path.Join(SmokeStr, repFile.filename))
			if err != nil {
				m.numGetErrs.Inc()
			}
			g := m.getsCompleted.Inc()
			if g%5000 == 0 {
				tutils.Logf(" %d/%d GET requests completed...\n", g, m.num*m.numGetsEachFile)
			}

			// Tell other tasks they can begin to do work in parallel
			if int(g) == m.num*m.numGetsEachFile/2 {
				for i := 0; i < m.otherTasksToTrigger; i++ {
					m.controlCh <- struct{}{}
				}
			}
		}()
	}
}

func (m *metadata) ensureNoErrors() {
	if m.numGetErrs.Load() > 0 {
		m.t.Fatalf("Number of get errors is non-zero: %d\n", m.numGetErrs.Load())
	}
}

func (m *metadata) reregisterTarget(target *cluster.Snode) {
	const (
		timeout    = time.Second * 10
		interval   = time.Millisecond * 10
		iterations = int(timeout / interval)
	)

	// T1
	tutils.Logf("Re-registering target %s...\n", target.DaemonID)
	smap := getClusterMap(m.t, m.proxyURL)
	err := tutils.RegisterTarget(m.proxyURL, target, smap)
	tassert.CheckFatal(m.t, err)
	baseParams := tutils.BaseAPIParams(target.URL(cmn.NetworkPublic))
	for i := 0; i < iterations; i++ {
		time.Sleep(interval)
		if _, ok := smap.Tmap[target.DaemonID]; !ok {
			// T2
			smap = getClusterMap(m.t, m.proxyURL)
			if _, ok := smap.Tmap[target.DaemonID]; ok {
				tutils.Logf("T2: re-registered target %s\n", target.DaemonID)
			}
		} else {
			baseParams.URL = m.proxyURL
			proxyLBNames, err := api.GetBucketNames(baseParams, cmn.LocalBs)
			tassert.CheckFatal(m.t, err)

			baseParams.URL = target.URL(cmn.NetworkPublic)
			targetLBNames, err := api.GetBucketNames(baseParams, cmn.LocalBs)
			tassert.CheckFatal(m.t, err)
			// T3
			if reflect.DeepEqual(proxyLBNames.Local, targetLBNames.Local) {
				tutils.Logf("T3: re-registered target %s got updated with the new bucket-metadata\n", target.DaemonID)
				return
			}
		}
	}

	m.t.Fatalf("failed to reregister target %s. Either is not in the smap or did not receive bucket metadata", target.DaemonID)
}

// Intended for a deployment with multiple targets
// 1. Unregister target T
// 2. Create local bucket
// 3. PUT large amount of objects into the local bucket
// 4. GET the objects while simultaneously re-registering the target T
func TestGetAndReRegisterInParallel(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		m = metadata{
			t:               t,
			num:             50000,
			numGetsEachFile: 3,
			fileSize:        cmn.KiB * 10,
		}
	)

	// Step 1.
	m.saveClusterState()
	if m.originalTargetCount < 2 {
		t.Fatalf("Must have 2 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Step 2.
	tutils.CreateFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer tutils.DestroyLocalBucket(t, m.proxyURL, m.bucket)

	target := tutils.ExtractTargetNodes(m.smap)[0]
	err := tutils.UnregisterTarget(m.proxyURL, target.DaemonID)
	tassert.CheckFatal(t, err)

	n := len(getClusterMap(t, m.proxyURL).Tmap)
	if n != m.originalTargetCount-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets", m.originalTargetCount-1, n)
	}
	tutils.Logf("Unregistered target %s: the cluster now has %d targets\n", target.URL(cmn.NetworkPublic), n)

	// Step 3.
	m.puts()

	// Step 4.
	m.wg.Add(m.num*m.numGetsEachFile + 2)
	go func() {
		m.gets()
		m.wg.Done()
	}()

	time.Sleep(time.Second * 3) // give gets some room to breathe
	go func() {
		m.reregisterTarget(target)
		m.wg.Done()
	}()

	m.wg.Wait()

	m.ensureNoErrors()
	m.assertClusterState()
}

// All of the above PLUS proxy failover/failback sequence in parallel
// Namely:
// 1. Unregister a target
// 2. Create a local bucket
// 3. Crash the primary proxy and PUT in parallel
// 4. Failback to the original primary proxy, re-register target, and GET in parallel
func TestProxyFailbackAndReRegisterInParallel(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		m = metadata{
			t:                   t,
			otherTasksToTrigger: 1,
			num:                 150000,
			numGetsEachFile:     1,
		}
	)

	// Step 1.
	m.saveClusterState()
	if m.originalTargetCount < 2 {
		t.Fatalf("Must have 2 or more targets in the cluster, have only %d", m.originalTargetCount)
	}
	if m.originalProxyCount < 3 {
		t.Fatalf("Must have 3 or more proxies/gateways in the cluster, have only %d", m.originalProxyCount)
	}

	// Step 2.
	tutils.CreateFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer tutils.DestroyLocalBucket(t, m.proxyURL, m.bucket)

	target := tutils.ExtractTargetNodes(m.smap)[0]
	err := tutils.UnregisterTarget(m.proxyURL, target.DaemonID)
	tassert.CheckFatal(t, err)
	n := len(getClusterMap(t, m.proxyURL).Tmap)
	if n != m.originalTargetCount-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets", m.originalTargetCount-1, n)
	}
	tutils.Logf("Unregistered target %s: the cluster now has %d targets\n", target.URL(cmn.NetworkPublic), n)

	// Step 3.
	_, newPrimaryURL, err := chooseNextProxy(&m.smap)
	// use a new proxyURL because primaryCrashElectRestart has a side-effect:
	// it changes the primary proxy. Without the change tutils.PutRandObjs is
	// failing while the current primary is restarting and rejoining
	m.proxyURL = newPrimaryURL
	tassert.CheckFatal(t, err)

	m.wg.Add(1)
	go func() {
		primaryCrashElectRestart(t)
		m.wg.Done()
	}()

	// PUT phase is timed to ensure it doesn't finish before primaryCrashElectRestart() begins
	time.Sleep(5 * time.Second)
	m.puts()
	m.wg.Wait()

	// Step 4.

	// m.num*m.numGetsEachFile is for `gets` and +2 is for goroutines
	// below (one for reregisterTarget and second for primarySetToOriginal)
	m.wg.Add(m.num*m.numGetsEachFile + 2)

	go func() {
		m.reregisterTarget(target)
		m.wg.Done()
	}()

	go func() {
		<-m.controlCh
		primarySetToOriginal(t)
		m.wg.Done()
	}()

	m.gets()

	m.wg.Wait()
	m.ensureNoErrors()
	m.assertClusterState()
}

// Similar to TestGetAndReRegisterInParallel, but instead of unregister, we kill the target
// 1. Kill registered target and wait for Smap to updated
// 2. Create local bucket
// 3. PUT large amounts of objects into local bucket
// 4. Get the objects while simultaneously re-registering the target
func TestGetAndRestoreInParallel(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		m = metadata{
			t:               t,
			num:             20000,
			numGetsEachFile: 5,
			fileSize:        cmn.KiB * 2,
		}
		targetURL  string
		targetPort string
		targetID   string
	)

	m.saveClusterState()
	if m.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Step 1
	// Select a random target
	for _, v := range m.smap.Tmap {
		targetURL = v.PublicNet.DirectURL
		targetPort = v.PublicNet.DaemonPort
		targetID = v.DaemonID
		break
	}
	tutils.Logf("Killing target: %s - %s\n", targetURL, targetID)
	tcmd, targs, err := kill(targetID, targetPort)
	tassert.CheckFatal(t, err)

	primaryProxy := getPrimaryURL(m.t, proxyURLReadOnly)
	m.smap, err = tutils.WaitForPrimaryProxy(primaryProxy, "to update smap", m.smap.Version, testing.Verbose(), m.originalProxyCount, m.originalTargetCount-1)
	tassert.CheckError(t, err)

	// Step 2
	tutils.CreateFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer tutils.DestroyLocalBucket(t, m.proxyURL, m.bucket)

	// Step 3
	m.puts()

	// Step 4
	m.wg.Add(m.num*m.numGetsEachFile + 1)
	go func() {
		time.Sleep(4 * time.Second)
		restore(tcmd, targs, false, "target")
		m.wg.Done()
	}()

	m.gets()

	m.wg.Wait()
	m.ensureNoErrors()
	m.assertClusterState()
}

func TestUnregisterPreviouslyUnregisteredTarget(t *testing.T) {
	var (
		m = metadata{
			t: t,
		}
	)

	// Initialize metadata
	m.saveClusterState()
	if m.originalTargetCount < 2 {
		t.Fatalf("Must have 2 or more targets in the cluster, have only %d", m.originalTargetCount)
	}
	tutils.Logf("Num targets %d, num proxies %d\n", m.originalTargetCount, m.originalProxyCount)

	target := tutils.ExtractTargetNodes(m.smap)[0]
	// Unregister target
	err := tutils.UnregisterTarget(m.proxyURL, target.DaemonID)
	tassert.CheckFatal(t, err)
	n := len(getClusterMap(t, m.proxyURL).Tmap)
	if n != m.originalTargetCount-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets", m.originalTargetCount-1, n)
	}
	tutils.Logf("Unregistered target %s: the cluster now has %d targets\n", target.URL(cmn.NetworkPublic), n)

	// Unregister same target again
	err = tutils.UnregisterTarget(m.proxyURL, target.DaemonID)
	if err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatal("Unregistering the same target twice must return error 404")
	}
	n = len(getClusterMap(t, m.proxyURL).Tmap)
	if n != m.originalTargetCount-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets", m.originalTargetCount-1, n)
	}
	tutils.Logf("Unregistered target %s: the cluster now has %d targets\n", target.URL(cmn.NetworkPublic), n)

	// Register target (bring cluster to normal state)
	m.reregisterTarget(target)
	m.assertClusterState()
}

func TestRegisterAndUnregisterTargetAndPutInParallel(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		m = metadata{
			t:   t,
			num: 10000,
		}
	)

	// Initialize metadata
	m.saveClusterState()
	if m.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
	}
	targets := tutils.ExtractTargetNodes(m.smap)

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer tutils.DestroyLocalBucket(t, m.proxyURL, m.bucket)

	// Unregister target 0
	err := tutils.UnregisterTarget(m.proxyURL, targets[0].DaemonID)
	tassert.CheckFatal(t, err)
	n := len(getClusterMap(t, m.proxyURL).Tmap)
	if n != m.originalTargetCount-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets", m.originalTargetCount-1, n)
	}

	// Do puts in parallel
	m.wg.Add(1)
	go func() {
		m.puts()
		m.wg.Done()
	}()

	// Register target 0 in parallel
	m.wg.Add(1)
	go func() {
		tutils.Logf("Registering target: %s\n", targets[0].URL(cmn.NetworkPublic))
		err = tutils.RegisterTarget(m.proxyURL, targets[0], m.smap)
		tassert.CheckFatal(t, err)
		m.wg.Done()
		tutils.Logf("Registered target %s again\n", targets[0].URL(cmn.NetworkPublic))
	}()

	// Unregister target 1 in parallel
	m.wg.Add(1)
	go func() {
		tutils.Logf("Unregistering target: %s\n", targets[1].URL(cmn.NetworkPublic))
		err = tutils.UnregisterTarget(m.proxyURL, targets[1].DaemonID)
		tassert.CheckFatal(t, err)
		m.wg.Done()
		tutils.Logf("Unregistered target %s\n", targets[1].URL(cmn.NetworkPublic))
	}()

	// Wait for everything to end
	m.wg.Wait()

	// Register target 1 to bring cluster to original state
	m.reregisterTarget(targets[1])
	m.assertClusterState()
}

func TestRebalanceAfterUnregisterAndReregister(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		m = metadata{
			t:               t,
			num:             10000,
			numGetsEachFile: 1,
		}
	)

	// Initialize metadata
	m.saveClusterState()
	if m.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
	}
	targets := tutils.ExtractTargetNodes(m.smap)

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer tutils.DestroyLocalBucket(t, m.proxyURL, m.bucket)

	// Unregister target 0
	err := tutils.UnregisterTarget(m.proxyURL, targets[0].DaemonID)
	tassert.CheckFatal(t, err)
	n := len(getClusterMap(t, m.proxyURL).Tmap)
	if n != m.originalTargetCount-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets", m.originalTargetCount-1, n)
	}

	// Put some files
	m.puts()

	// Register target 0 in parallel
	m.wg.Add(1)
	go func() {
		tutils.Logf("trying to register target: %s\n", targets[0].URL(cmn.NetworkPublic))
		err = tutils.RegisterTarget(m.proxyURL, targets[0], m.smap)
		tassert.CheckFatal(t, err)
		m.wg.Done()
		tutils.Logf("registered target %s again\n", targets[0].URL(cmn.NetworkPublic))
	}()

	// Unregister target 1 in parallel
	m.wg.Add(1)
	go func() {
		tutils.Logf("trying to unregister target: %s\n", targets[1].URL(cmn.NetworkPublic))
		err = tutils.UnregisterTarget(m.proxyURL, targets[1].DaemonID)
		tassert.CheckFatal(t, err)
		m.wg.Done()
		tutils.Logf("unregistered target %s\n", targets[1].URL(cmn.NetworkPublic))
	}()

	// Wait for everything to end
	m.wg.Wait()

	// Register target 1 to bring cluster to original state
	m.reregisterTarget(targets[1])
	tutils.Logln("reregistering complete")

	baseParams := tutils.BaseAPIParams(m.proxyURL)
	waitForRebalanceToComplete(t, baseParams, rebalanceTimeout)

	m.wg.Add(m.num * m.numGetsEachFile)
	m.gets()
	m.wg.Wait()

	m.ensureNoErrors()
	m.assertClusterState()
}

func TestPutDuringRebalance(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		m = metadata{
			t:               t,
			num:             10000,
			numGetsEachFile: 1,
		}
	)

	// Init. metadata
	m.saveClusterState()
	if m.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
	}
	target := tutils.ExtractTargetNodes(m.smap)[0]

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer tutils.DestroyLocalBucket(t, m.proxyURL, m.bucket)

	// Unregister a target
	tutils.Logf("Trying to unregister target: %s\n", target.URL(cmn.NetworkPublic))
	err := tutils.UnregisterTarget(m.proxyURL, target.DaemonID)
	tassert.CheckFatal(t, err)
	n := len(getClusterMap(t, m.proxyURL).Tmap)
	if n != m.originalTargetCount-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets", m.originalTargetCount-1, n)
	}

	// Start putting files and register target in parallel
	m.wg.Add(1)
	go func() {
		// sleep some time to wait for PUT operations to begin
		time.Sleep(3 * time.Second)
		tutils.Logf("Trying to register target: %s\n", target.URL(cmn.NetworkPublic))
		err = tutils.RegisterTarget(m.proxyURL, target, m.smap)
		tassert.CheckFatal(t, err)
		m.wg.Done()
		tutils.Logf("Target %s is registered again.\n", target.URL(cmn.NetworkPublic))
	}()

	m.puts()

	// Wait for everything to finish
	m.wg.Wait()
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	waitForRebalanceToComplete(t, baseParams, rebalanceTimeout)

	// main check - try to read all objects
	m.wg.Add(m.num * m.numGetsEachFile)
	m.gets()
	m.wg.Wait()

	m.checkObjectDistribution(t)
	m.assertClusterState()
}

func TestGetDuringLocalAndGlobalRebalance(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		m = metadata{
			t:               t,
			num:             10000,
			numGetsEachFile: 10,
		}
		targetURL  string
		killTarget *cluster.Snode
	)

	// Init. metadata
	m.saveClusterState()
	if m.originalTargetCount < 2 {
		t.Fatalf("Must have at least 2 target in the cluster")
	}

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer tutils.DestroyLocalBucket(t, m.proxyURL, m.bucket)

	// select a random target to disable one of its mountpaths,
	// and another random target to unregister
	for _, target := range m.smap.Tmap {
		if targetURL == "" {
			targetURL = target.PublicNet.DirectURL
		} else {
			killTarget = target
			break
		}
	}
	baseParams := tutils.BaseAPIParams(targetURL)
	mpList, err := api.GetMountpaths(baseParams)
	tassert.CheckFatal(t, err)

	if len(mpList.Available) < 2 {
		t.Fatalf("Must have at least 2 mountpaths")
	}

	// Disable mountpaths temporarily
	mpath := mpList.Available[0]
	err = api.DisableMountpath(baseParams, mpath)
	tassert.CheckFatal(t, err)

	// Unregister a target
	tutils.Logf("Trying to unregister target: %s\n", killTarget.URL(cmn.NetworkPublic))
	err = tutils.UnregisterTarget(m.proxyURL, killTarget.DaemonID)
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
	m.wg.Add(m.num * m.numGetsEachFile)
	go func() {
		m.gets()
	}()

	// Let's give gets some momentum
	time.Sleep(time.Second * 4)

	// register a new target
	err = tutils.RegisterTarget(m.proxyURL, killTarget, m.smap)
	tassert.CheckFatal(t, err)

	// enable mountpath
	err = api.EnableMountpath(baseParams, mpath)
	tassert.CheckFatal(t, err)

	// wait until GETs are done while 2 rebalance are running
	m.wg.Wait()

	// make sure that the cluster has all targets enabled
	_, err = tutils.WaitForPrimaryProxy(
		m.proxyURL,
		"to join target back",
		smap.Version, testing.Verbose(),
		m.originalProxyCount,
		m.originalTargetCount,
	)
	tassert.CheckFatal(m.t, err)

	mpListAfter, err := api.GetMountpaths(baseParams)
	tassert.CheckFatal(t, err)
	if len(mpList.Available) != len(mpListAfter.Available) {
		t.Fatalf("Some mountpaths failed to enable: the number before %d, after %d",
			len(mpList.Available), len(mpListAfter.Available))
	}

	m.ensureNoErrors()
	m.assertClusterState()
}

func TestGetDuringLocalRebalance(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		m = metadata{
			t:               t,
			num:             20000,
			numGetsEachFile: 1,
		}
	)

	// Init. metadata
	m.saveClusterState()
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have at least 1 target in the cluster")
	}

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer tutils.DestroyLocalBucket(t, m.proxyURL, m.bucket)

	targets := tutils.ExtractTargetNodes(m.smap)
	baseParams := tutils.BaseAPIParams(targets[0].URL(cmn.NetworkPublic))
	mpList, err := api.GetMountpaths(baseParams)
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
		err = api.DisableMountpath(baseParams, mp)
		tassert.CheckFatal(t, err)
	}

	m.puts()

	// Start getting objects and enable mountpaths in parallel
	m.wg.Add(m.num * m.numGetsEachFile)
	m.gets()

	for _, mp := range mpaths {
		// sleep for a while before enabling another mountpath
		time.Sleep(50 * time.Millisecond)
		err = api.EnableMountpath(baseParams, mp)
		tassert.CheckFatal(t, err)
	}

	m.wg.Wait()

	mpListAfter, err := api.GetMountpaths(baseParams)
	tassert.CheckFatal(t, err)
	if len(mpList.Available) != len(mpListAfter.Available) {
		t.Fatalf("Some mountpaths failed to enable: the number before %d, after %d",
			len(mpList.Available), len(mpListAfter.Available))
	}

	m.ensureNoErrors()
}

func TestGetDuringRebalance(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		md = metadata{
			t:               t,
			num:             10000,
			numGetsEachFile: 1,
		}
		mdAfterRebalance = metadata{
			t:               t,
			num:             10000,
			numGetsEachFile: 1,
		}
	)

	// Init. metadata
	md.saveClusterState()
	mdAfterRebalance.saveClusterState()

	if md.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", md.originalTargetCount)
	}
	target := tutils.ExtractTargetNodes(md.smap)[0]

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, md.proxyURL, md.bucket)
	defer tutils.DestroyLocalBucket(t, md.proxyURL, md.bucket)

	// Unregister a target
	err := tutils.UnregisterTarget(md.proxyURL, target.DaemonID)
	tassert.CheckFatal(t, err)
	n := len(getClusterMap(t, md.proxyURL).Tmap)
	if n != md.originalTargetCount-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets", md.originalTargetCount-1, n)
	}

	// Start putting files into bucket
	md.puts()
	mdAfterRebalance.puts()

	// Start getting objects and register target in parallel
	md.wg.Add(md.num * md.numGetsEachFile)
	md.gets()

	tutils.Logf("Trying to register target: %s\n", target.URL(cmn.NetworkPublic))
	err = tutils.RegisterTarget(md.proxyURL, target, md.smap)
	tassert.CheckFatal(t, err)

	// wait for everything to finish
	baseParams := tutils.BaseAPIParams(md.proxyURL)
	waitForRebalanceToComplete(t, baseParams, rebalanceTimeout)
	md.wg.Wait()

	// read files once again
	mdAfterRebalance.wg.Add(mdAfterRebalance.num * mdAfterRebalance.numGetsEachFile)
	mdAfterRebalance.gets()
	mdAfterRebalance.wg.Wait()

	mdAfterRebalance.ensureNoErrors()
	md.assertClusterState()
}

func TestRegisterTargetsAndCreateLocalBucketsInParallel(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	const (
		unregisterTargetCount = 2
		newLocalBucketCount   = 3
	)

	var (
		m = metadata{
			t:  t,
			wg: &sync.WaitGroup{},
		}
	)

	// Initialize metadata
	m.saveClusterState()
	if m.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
	}
	tutils.Logf("Num targets %d\n", m.originalTargetCount)
	targets := tutils.ExtractTargetNodes(m.smap)

	// Unregister targets
	for i := 0; i < unregisterTargetCount; i++ {
		err := tutils.UnregisterTarget(m.proxyURL, targets[i].DaemonID)
		tassert.CheckError(t, err)
		n := len(getClusterMap(t, m.proxyURL).Tmap)
		if n != m.originalTargetCount-(i+1) {
			t.Errorf("%d targets expected after unregister, actually %d targets", m.originalTargetCount-(i+1), n)
		}
		tutils.Logf("Unregistered target %s: the cluster now has %d targets\n", targets[i].URL(cmn.NetworkPublic), n)
	}

	m.wg.Add(unregisterTargetCount)
	for i := 0; i < unregisterTargetCount; i++ {
		go func(number int) {
			err := tutils.RegisterTarget(m.proxyURL, targets[number], m.smap)
			tassert.CheckError(t, err)
			m.wg.Done()
		}(i)
	}

	m.wg.Add(newLocalBucketCount)
	for i := 0; i < newLocalBucketCount; i++ {
		go func(number int) {
			tutils.CreateFreshLocalBucket(t, m.proxyURL, m.bucket+strconv.Itoa(number))
			m.wg.Done()
		}(i)

		defer tutils.DestroyLocalBucket(t, m.proxyURL, m.bucket+strconv.Itoa(i))
	}
	m.wg.Wait()
	m.assertClusterState()
}

func TestRenameEmptyLocalBucket(t *testing.T) {
	const (
		newTestLocalBucketName = TestLocalBucketName + "_new"
	)
	var (
		m = metadata{
			t:  t,
			wg: &sync.WaitGroup{},
		}
	)

	// Initialize metadata
	m.saveClusterState()
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, m.proxyURL, m.bucket)
	tutils.DestroyLocalBucket(t, m.proxyURL, newTestLocalBucketName)

	// Rename it
	err := api.RenameLocalBucket(tutils.DefaultBaseAPIParams(t), m.bucket, newTestLocalBucketName)
	tassert.CheckFatal(t, err)

	// Destroy renamed local bucket
	tutils.DestroyLocalBucket(t, m.proxyURL, newTestLocalBucketName)
}

func TestRenameNonEmptyLocalBucket(t *testing.T) {
	const (
		newTestLocalBucketName = TestLocalBucketName + "_new"
	)

	var (
		m = metadata{
			t:               t,
			num:             1000,
			numGetsEachFile: 2,
		}
	)

	// Initialize metadata
	m.saveClusterState()
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, m.proxyURL, m.bucket)
	tutils.DestroyLocalBucket(t, m.proxyURL, newTestLocalBucketName)

	// Put some files
	m.puts()

	// Rename it
	oldLocalBucketName := m.bucket
	m.bucket = newTestLocalBucketName
	err := api.RenameLocalBucket(tutils.DefaultBaseAPIParams(t), oldLocalBucketName, m.bucket)
	tassert.CheckFatal(t, err)

	// Gets on renamed local bucket
	m.wg.Add(m.num * m.numGetsEachFile)
	m.gets()
	m.wg.Wait()
	m.ensureNoErrors()

	// Destroy renamed local bucket
	tutils.DestroyLocalBucket(t, m.proxyURL, m.bucket)
}

func TestDirectoryExistenceWhenModifyingBucket(t *testing.T) {
	const (
		newTestLocalBucketName = TestLocalBucketName + "_new"
	)
	var (
		m = metadata{
			t:  t,
			wg: &sync.WaitGroup{},
		}
	)

	// Initialize metadata
	m.saveClusterState()
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	localBucketDir := ""
	fsWalkFunc := func(path string, info os.FileInfo, err error) error {
		if localBucketDir != "" {
			return filepath.SkipDir
		}
		if strings.HasSuffix(path, "/local") && strings.Contains(path, fs.ObjectType) {
			localBucketDir = path
			return filepath.SkipDir
		}
		return nil
	}
	filepath.Walk(rootDir, fsWalkFunc)
	tutils.Logf("Found local bucket's directory: %s\n", localBucketDir)
	bucketFQN := filepath.Join(localBucketDir, m.bucket)
	newBucketFQN := filepath.Join(localBucketDir, newTestLocalBucketName)

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, m.proxyURL, m.bucket)
	tutils.DestroyLocalBucket(t, m.proxyURL, newTestLocalBucketName)

	if _, err := os.Stat(bucketFQN); os.IsNotExist(err) {
		t.Fatalf("local bucket folder was not created")
	}

	// Rename local bucket
	err := api.RenameLocalBucket(tutils.DefaultBaseAPIParams(m.t), m.bucket, newTestLocalBucketName)
	tassert.CheckFatal(t, err)
	if _, err := os.Stat(bucketFQN); !os.IsNotExist(err) {
		t.Fatalf("local bucket folder was not deleted")
	}

	if _, err := os.Stat(newBucketFQN); os.IsNotExist(err) {
		t.Fatalf("new local bucket folder was not created")
	}

	// Destroy renamed local bucket
	tutils.DestroyLocalBucket(t, m.proxyURL, newTestLocalBucketName)
	if _, err := os.Stat(newBucketFQN); !os.IsNotExist(err) {
		t.Fatalf("new local bucket folder was not deleted")
	}
}

func TestAddAndRemoveMountpath(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		m = metadata{
			t:               t,
			num:             5000,
			numGetsEachFile: 2,
		}
	)

	// Initialize metadata
	m.saveClusterState()
	if m.originalTargetCount < 2 {
		t.Fatalf("Must have 2 or more targets in the cluster, have only %d", m.originalTargetCount)
	}
	target := tutils.ExtractTargetNodes(m.smap)[0]
	baseParams := tutils.BaseAPIParams(target.URL(cmn.NetworkPublic))
	// Remove all mountpaths for one target
	oldMountpaths, err := api.GetMountpaths(baseParams)
	tassert.CheckFatal(t, err)

	for _, mpath := range oldMountpaths.Available {
		err = api.RemoveMountpath(baseParams, mpath)
		tassert.CheckFatal(t, err)
	}

	// Check if mountpaths were actually removed
	mountpaths, err := api.GetMountpaths(baseParams)
	tassert.CheckFatal(t, err)

	if len(mountpaths.Available) != 0 {
		t.Fatalf("Target should not have any paths available")
	}

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer tutils.DestroyLocalBucket(t, m.proxyURL, m.bucket)

	// Add target mountpath again
	for _, mpath := range oldMountpaths.Available {
		err = api.AddMountpath(baseParams, mpath)
		tassert.CheckFatal(t, err)
	}

	// Check if mountpaths were actually added
	mountpaths, err = api.GetMountpaths(baseParams)
	tassert.CheckFatal(t, err)

	if len(mountpaths.Available) != len(oldMountpaths.Available) {
		t.Fatalf("Target should have old mountpath available restored")
	}

	// Put and read random files
	m.puts()

	m.wg.Add(m.num * m.numGetsEachFile)
	m.gets()
	m.wg.Wait()
	m.ensureNoErrors()
}

func TestLocalRebalanceAfterAddingMountpath(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	const newMountpath = "/tmp/ais/mountpath"

	var (
		m = metadata{
			t:               t,
			num:             5000,
			numGetsEachFile: 2,
		}
	)

	// Initialize metadata
	m.saveClusterState()
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}
	target := tutils.ExtractTargetNodes(m.smap)[0]

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, m.proxyURL, m.bucket)
	err := cmn.CreateDir(newMountpath)
	tassert.CheckFatal(t, err)

	if tutils.DockerRunning() {
		err := tutils.DockerCreateMpathDir(0, newMountpath)
		tassert.CheckFatal(t, err)
	} else {
		err := cmn.CreateDir(newMountpath)
		tassert.CheckFatal(t, err)
	}

	defer func() {
		if !tutils.DockerRunning() {
			os.RemoveAll(newMountpath)
		}
		tutils.DestroyLocalBucket(t, m.proxyURL, m.bucket)
	}()

	// Put random files
	m.puts()

	// Add new mountpath to target
	baseParams := tutils.BaseAPIParams(target.URL(cmn.NetworkPublic))
	err = api.AddMountpath(baseParams, newMountpath)
	tassert.CheckFatal(t, err)

	waitForRebalanceToComplete(t, tutils.BaseAPIParams(m.proxyURL), rebalanceTimeout)

	// Read files after rebalance
	m.wg.Add(m.num * m.numGetsEachFile)
	m.gets()
	m.wg.Wait()

	// Remove new mountpath from target
	if tutils.DockerRunning() {
		baseParams := tutils.BaseAPIParams(target.URL(cmn.NetworkPublic))
		if err := api.RemoveMountpath(baseParams, newMountpath); err != nil {
			t.Error(err.Error())
		}
	} else {
		err = api.RemoveMountpath(baseParams, newMountpath)
		tassert.CheckFatal(t, err)
	}

	m.ensureNoErrors()
}

func TestGlobalAndLocalRebalanceAfterAddingMountpath(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	const (
		newMountpath = "/tmp/ais/mountpath"
	)

	var (
		m = metadata{
			t:               t,
			num:             10000,
			numGetsEachFile: 5,
		}
	)

	// Initialize metadata
	m.saveClusterState()
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}
	targets := tutils.ExtractTargetNodes(m.smap)

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, m.proxyURL, m.bucket)

	defer func() {
		if !tutils.DockerRunning() {
			os.RemoveAll(newMountpath)
		}
		tutils.DestroyLocalBucket(t, m.proxyURL, m.bucket)
	}()

	// Put random files
	m.puts()

	if tutils.DockerRunning() {
		err := tutils.DockerCreateMpathDir(0, newMountpath)
		tassert.CheckFatal(t, err)
		for _, target := range targets {
			baseParams := tutils.BaseAPIParams(target.URL(cmn.NetworkPublic))
			err = api.AddMountpath(baseParams, newMountpath)
			tassert.CheckFatal(t, err)
		}
	} else {
		// Add new mountpath to all targets
		for idx, target := range targets {
			mountpath := filepath.Join(newMountpath, fmt.Sprintf("%d", idx))
			cmn.CreateDir(mountpath)
			baseParams := tutils.BaseAPIParams(target.URL(cmn.NetworkPublic))
			err := api.AddMountpath(baseParams, mountpath)
			tassert.CheckFatal(t, err)
		}
	}

	waitForRebalanceToComplete(t, tutils.BaseAPIParams(m.proxyURL), rebalanceTimeout)

	// Read after rebalance
	m.wg.Add(m.num * m.numGetsEachFile)
	m.gets()
	m.wg.Wait()

	// Remove new mountpath from all targets
	if tutils.DockerRunning() {
		err := tutils.DockerRemoveMpathDir(0, newMountpath)
		tassert.CheckFatal(t, err)
		for _, target := range targets {
			baseParams := tutils.BaseAPIParams(target.URL(cmn.NetworkPublic))
			if err := api.RemoveMountpath(baseParams, newMountpath); err != nil {
				t.Error(err.Error())
			}
		}
	} else {
		for idx, target := range targets {
			mountpath := filepath.Join(newMountpath, fmt.Sprintf("%d", idx))
			os.RemoveAll(mountpath)
			baseParams := tutils.BaseAPIParams(target.URL(cmn.NetworkPublic))
			if err := api.RemoveMountpath(baseParams, mountpath); err != nil {
				t.Error(err.Error())
			}
		}
	}

	m.ensureNoErrors()
}

func TestDisableAndEnableMountpath(t *testing.T) {
	var (
		m = metadata{
			t:               t,
			num:             5000,
			numGetsEachFile: 2,
		}
	)

	// Initialize metadata
	m.saveClusterState()
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}
	target := tutils.ExtractTargetNodes(m.smap)[0]
	baseParams := tutils.BaseAPIParams(target.URL(cmn.NetworkPublic))
	// Remove all mountpaths for one target
	oldMountpaths, err := api.GetMountpaths(baseParams)
	tassert.CheckFatal(t, err)

	for _, mpath := range oldMountpaths.Available {
		err := api.DisableMountpath(baseParams, mpath)
		tassert.CheckFatal(t, err)
	}

	// Check if mountpaths were actually disabled
	mountpaths, err := api.GetMountpaths(baseParams)
	tassert.CheckFatal(t, err)

	if len(mountpaths.Available) != 0 {
		t.Fatalf("Target should not have any paths available")
	}

	if len(mountpaths.Disabled) != len(oldMountpaths.Available) {
		t.Fatalf("Not all mountpaths were added to disabled paths")
	}

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer tutils.DestroyLocalBucket(t, m.proxyURL, m.bucket)

	// Add target mountpath again
	for _, mpath := range oldMountpaths.Available {
		err := api.EnableMountpath(baseParams, mpath)
		tassert.CheckFatal(t, err)
	}

	// Check if mountpaths were actually enabled
	mountpaths, err = api.GetMountpaths(baseParams)
	tassert.CheckFatal(t, err)

	if len(mountpaths.Available) != len(oldMountpaths.Available) {
		t.Fatalf("Target should have old mountpath available restored")
	}

	if len(mountpaths.Disabled) != 0 {
		t.Fatalf("Not all disabled mountpaths were enabled")
	}

	tutils.Logf("Waiting for local bucket %s appears on all targets\n", m.bucket)
	err = tutils.WaitForLocalBucket(m.proxyURL, m.bucket, true /*exists*/)
	tassert.CheckFatal(t, err)

	// Put and read random files
	m.puts()

	m.wg.Add(m.num * m.numGetsEachFile)
	m.gets()
	m.wg.Wait()
	m.ensureNoErrors()
}

func TestForwardCP(t *testing.T) {
	var (
		m = metadata{
			t:               t,
			num:             10000,
			numGetsEachFile: 2,
			fileSize:        128,
		}
	)

	// Step 1.
	m.saveClusterState()
	if m.originalProxyCount < 2 {
		t.Fatalf("Must have 2 or more proxies in the cluster, have only %d", m.originalProxyCount)
	}

	// Step 2.
	origID, origURL := m.smap.ProxySI.DaemonID, m.smap.ProxySI.PublicNet.DirectURL
	nextProxyID, nextProxyURL, _ := chooseNextProxy(&m.smap)

	tutils.DestroyLocalBucket(t, m.proxyURL, m.bucket)

	tutils.CreateFreshLocalBucket(t, nextProxyURL, m.bucket)
	tutils.Logf("Created bucket %s via non-primary %s\n", m.bucket, nextProxyID)

	// Step 3.
	m.puts()

	// Step 4. in parallel: run GETs and designate a new primary=nextProxyID
	m.wg.Add(m.num*m.numGetsEachFile + 1)
	m.gets()

	go func() {
		setPrimaryTo(t, m.proxyURL, m.smap, nextProxyURL, nextProxyID)
		m.proxyURL = nextProxyURL
		m.wg.Done()
	}()

	m.wg.Wait()
	m.ensureNoErrors()

	// Step 5. destroy local bucket via original primary which is not primary at this point
	tutils.DestroyLocalBucket(t, origURL, m.bucket)
	tutils.Logf("Destroyed bucket %s via non-primary %s/%s\n", m.bucket, origID, origURL)
}

func TestAtimeRebalance(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		m = metadata{
			t:               t,
			num:             2000,
			numGetsEachFile: 2,
		}
		bucketProps = defaultBucketProps()
	)

	// Initialize metadata
	m.saveClusterState()
	if m.originalTargetCount < 2 {
		t.Fatalf("Must have 2 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer tutils.DestroyLocalBucket(t, m.proxyURL, m.bucket)

	// Enable bucket level LRU properties
	bucketProps.LRU.Enabled = true
	err := api.SetBucketPropsMsg(tutils.DefaultBaseAPIParams(t), m.bucket, bucketProps)
	tassert.CheckFatal(t, err)

	target := tutils.ExtractTargetNodes(m.smap)[0]

	// Unregister a target
	err = tutils.UnregisterTarget(m.proxyURL, target.DaemonID)
	tassert.CheckFatal(t, err)
	n := len(getClusterMap(t, m.proxyURL).Tmap)
	if n != m.originalTargetCount-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets", m.originalTargetCount-1, n)
	}
	tutils.Logf("Unregistered target %s: the cluster now has %d targets\n", target.URL(cmn.NetworkPublic), n)

	// Put random files
	m.puts()

	// Get atime in a format that includes nanoseconds to properly check if it was updated in atime cache (if it wasn't,
	// then the returned atime would be different from the original one, but the difference could be very small).
	msg := &cmn.SelectMsg{Props: cmn.GetPropsAtime + ", " + cmn.GetPropsStatus, TimeFormat: time.StampNano}
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	bucketList, err := api.ListBucket(baseParams, m.bucket, msg, 0)
	tassert.CheckFatal(t, err)

	objNames := make(cmn.SimpleKVs, 10)
	for _, entry := range bucketList.Entries {
		objNames[entry.Name] = entry.Atime
	}

	// re-register target
	err = tutils.RegisterTarget(m.proxyURL, target, m.smap)
	tassert.CheckFatal(t, err)

	// make sure that the cluster has all targets enabled
	_, err = tutils.WaitForPrimaryProxy(
		m.proxyURL,
		"to join target back",
		m.smap.Version, testing.Verbose(),
		m.originalProxyCount,
		m.originalTargetCount,
	)
	tassert.CheckFatal(t, err)

	waitForRebalanceToComplete(t, baseParams, rebalanceTimeout)

	msg = &cmn.SelectMsg{Props: cmn.GetPropsAtime + ", " + cmn.GetPropsStatus, TimeFormat: time.StampNano}
	bucketListReb, err := api.ListBucket(baseParams, m.bucket, msg, 0)
	tassert.CheckFatal(t, err)

	itemCount := 0
	for _, entry := range bucketListReb.Entries {
		if entry.Status != cmn.ObjStatusOK {
			continue
		}
		itemCount++
		atime, ok := objNames[entry.Name]
		if !ok {
			t.Errorf("Object %q not found", entry.Name)
			continue
		}
		if atime != entry.Atime {
			t.Errorf("Atime mismatched for %s: before %q, after %q", entry.Name, atime, entry.Atime)
		}
	}
	if itemCount != len(bucketList.Entries) {
		t.Errorf("The number of objects mismatch: before %d, after %d",
			len(bucketList.Entries), itemCount)
	}
}

func TestAtimeLocalGet(t *testing.T) {
	var (
		proxyURL      = getPrimaryURL(t, proxyURLReadOnly)
		baseParams    = tutils.DefaultBaseAPIParams(t)
		bucket        = t.Name()
		objectName    = t.Name()
		objectContent = tutils.NewBytesReader([]byte("file content"))
	)

	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	err := api.PutObject(api.PutObjectArgs{BaseParams: baseParams, Bucket: bucket, Object: objectName, Reader: objectContent})
	tassert.CheckFatal(t, err)

	timeAfterPut := tutils.GetObjectAtime(t, baseParams, objectName, bucket, time.RFC3339Nano)

	// Get object so that atime is updated
	_, err = api.GetObject(baseParams, bucket, objectName)
	tassert.CheckFatal(t, err)

	timeAfterGet := tutils.GetObjectAtime(t, baseParams, objectName, bucket, time.RFC3339Nano)

	if !(timeAfterGet.After(timeAfterPut)) {
		t.Errorf("Expected PUT atime (%s) to be before subsequent GET atime (%s).", timeAfterGet.Format(time.RFC3339Nano), timeAfterPut.Format(time.RFC3339Nano))
	}
}

func TestAtimeColdGet(t *testing.T) {
	var (
		proxyURL      = getPrimaryURL(t, proxyURLReadOnly)
		baseParams    = tutils.DefaultBaseAPIParams(t)
		bucket        = clibucket
		objectName    = t.Name()
		objectContent = tutils.NewBytesReader([]byte("file content"))
	)

	if !isCloudBucket(t, proxyURL, bucket) {
		t.Skip("test requires a cloud bucket")
	}
	tutils.CleanCloudBucket(t, proxyURL, bucket, objectName)
	defer tutils.CleanCloudBucket(t, proxyURL, bucket, objectName)

	tutils.PutObjectInCloudBucketWithoutCachingLocally(t, objectName, bucket, proxyURL, objectContent)

	timeAfterPut := time.Now()

	// Perform the COLD get
	_, err := api.GetObject(baseParams, bucket, objectName)
	tassert.CheckFatal(t, err)

	timeAfterGet := tutils.GetObjectAtime(t, baseParams, objectName, bucket, time.RFC3339Nano)

	if !(timeAfterGet.After(timeAfterPut)) {
		t.Errorf("Expected PUT atime (%s) to be before subsequent GET atime (%s).", timeAfterGet.Format(time.RFC3339Nano), timeAfterPut.Format(time.RFC3339Nano))
	}
}

func TestAtimePrefetch(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		proxyURL      = getPrimaryURL(t, proxyURLReadOnly)
		baseParams    = tutils.DefaultBaseAPIParams(t)
		bucket        = clibucket
		objectName    = t.Name()
		objectContent = tutils.NewBytesReader([]byte("file content"))
	)

	if !isCloudBucket(t, proxyURL, bucket) {
		t.Skip("test requires a cloud bucket")
	}
	tutils.CleanCloudBucket(t, proxyURL, bucket, objectName)
	defer tutils.CleanCloudBucket(t, proxyURL, bucket, objectName)

	tutils.PutObjectInCloudBucketWithoutCachingLocally(t, objectName, bucket, proxyURL, objectContent)

	timeAfterPut := time.Now()

	err := api.PrefetchList(baseParams, bucket, cmn.CloudBs, []string{objectName}, true, 0)
	tassert.CheckFatal(t, err)

	timeAfterGet := tutils.GetObjectAtime(t, baseParams, objectName, bucket, time.RFC3339Nano)

	if !(timeAfterGet.Before(timeAfterPut)) {
		t.Errorf("Atime should not be updated after prefetch (got: atime after PUT: %s, atime after GET: %s).",
			timeAfterPut.Format(time.RFC3339Nano), timeAfterGet.Format(time.RFC3339Nano))
	}
}

func TestAtimeLocalPut(t *testing.T) {
	var (
		proxyURL      = getPrimaryURL(t, proxyURLReadOnly)
		baseParams    = tutils.DefaultBaseAPIParams(t)
		bucket        = t.Name()
		objectName    = t.Name()
		objectContent = tutils.NewBytesReader([]byte("file content"))
	)

	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	timeBeforePut := time.Now()
	err := api.PutObject(api.PutObjectArgs{BaseParams: baseParams, Bucket: bucket, Object: objectName, Reader: objectContent})
	tassert.CheckFatal(t, err)

	timeAfterPut := tutils.GetObjectAtime(t, baseParams, objectName, bucket, time.RFC3339Nano)

	if !(timeAfterPut.After(timeBeforePut)) {
		t.Errorf("Expected atime after PUT (%s) to be after atime before PUT (%s).", timeAfterPut.Format(time.RFC3339Nano), timeBeforePut.Format(time.RFC3339Nano))
	}
}

// 1. Unregister target
// 2. Add bucket - unregistered target should miss the update
// 3. Reregister target
// 4. Put objects
// 5. Get objects - everything should succeed
func TestGetAndPutAfterReregisterWithMissedBucketUpdate(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		m = metadata{
			t:               t,
			num:             10000,
			numGetsEachFile: 5,
		}
	)

	// Initialize metadata
	m.saveClusterState()
	if m.originalTargetCount < 2 {
		t.Fatalf("Must have 2 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Unregister target 0
	targets := tutils.ExtractTargetNodes(m.smap)
	err := tutils.UnregisterTarget(m.proxyURL, targets[0].DaemonID)
	tassert.CheckFatal(t, err)
	n := len(getClusterMap(t, m.proxyURL).Tmap)
	if n != m.originalTargetCount-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets", m.originalTargetCount-1, n)
	}

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer tutils.DestroyLocalBucket(t, m.proxyURL, m.bucket)

	// Reregister target 0
	m.reregisterTarget(targets[0])
	tutils.Logln("reregistering complete")

	// Do puts
	m.puts()

	// Do gets
	m.wg.Add(m.num * m.numGetsEachFile)
	m.gets()
	m.wg.Wait()

	m.ensureNoErrors()
	m.assertClusterState()
}

// 1. Unregister target
// 2. Add bucket - unregistered target should miss the update
// 3. Put objects
// 4. Reregister target - rebalance kicks in
// 5. Get objects - everything should succeed
func TestGetAfterReregisterWithMissedBucketUpdate(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		m = metadata{
			t:               t,
			num:             10000,
			fileSize:        1024,
			numGetsEachFile: 5,
		}
	)

	// Initialize metadata
	m.saveClusterState()
	if m.originalTargetCount < 2 {
		t.Fatalf("Must have 2 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	targets := tutils.ExtractTargetNodes(m.smap)

	// Unregister target 0
	err := tutils.UnregisterTarget(m.proxyURL, targets[0].DaemonID)
	tassert.CheckFatal(t, err)
	n := len(getClusterMap(t, m.proxyURL).Tmap)
	if n != m.originalTargetCount-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets", m.originalTargetCount-1, n)
	}

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer tutils.DestroyLocalBucket(t, m.proxyURL, m.bucket)

	m.puts()

	// Reregister target 0
	m.reregisterTarget(targets[0])
	tutils.Logln("reregistering complete")

	// Wait for rebalance and do gets
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	waitForRebalanceToComplete(t, baseParams)

	m.wg.Add(m.num * m.numGetsEachFile)
	m.gets()
	m.wg.Wait()

	m.ensureNoErrors()
	m.assertClusterState()
}
