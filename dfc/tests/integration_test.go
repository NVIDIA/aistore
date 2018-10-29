/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

package dfc_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/common"
	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/memsys"
	"github.com/NVIDIA/dfcpub/pkg/client"
	"github.com/NVIDIA/dfcpub/tutils"
)

const rebalanceObjectDistributionTestCoef = 0.3

const skipping = "skipping test in short mode."

type repFile struct {
	repetitions int
	filename    string
}

type targetInfo struct {
	sid       string
	directURL string
}

type metadata struct {
	t                   *testing.T
	smap                cluster.Smap
	delay               time.Duration
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
	numGetErrsBefore    uint64
	numGetErrsAfter     uint64
	getsCompleted       uint64
	reregistered        uint64
	proxyURL            string
}

// Intended for a deployment with multiple targets
// 1. Unregister target T
// 2. Create local bucket
// 3. PUT large amount of objects into the local bucket
// 4. GET the objects while simultaneously re-registering the target T
func TestGetAndReRegisterInParallel(t *testing.T) {
	const (
		num       = 20000
		filesize  = 1024
		seed      = int64(111)
		maxErrPct = 5
	)

	var (
		err error
		m   = metadata{
			t:               t,
			delay:           18 * time.Second,
			num:             num,
			numGetsEachFile: 5,
			repFilenameCh:   make(chan repFile, num),
			semaphore:       make(chan struct{}, 10), // 10 concurrent GET requests at a time
			wg:              &sync.WaitGroup{},
			bucket:          TestLocalBucketName,
		}
		// With the current design, there exists a brief period of time
		// during which GET errors can occur - see the timeline comment below
		filenameCh = make(chan string, m.num)
		errCh      = make(chan error, m.num)
		sgl        *memsys.SGL
	)
	if testing.Short() {
		t.Skip(skipping)
	}
	// Step 1.
	saveClusterState(&m)
	if m.originalTargetCount < 2 {
		t.Fatalf("Must have 2 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	targets := extractTargetsInfo(m.smap)
	err = client.UnregisterTarget(m.proxyURL, targets[0].sid)
	tutils.CheckFatal(err, t)

	n := len(getClusterMap(t, m.proxyURL).Tmap)
	if n != m.originalTargetCount-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets", m.originalTargetCount-1, n)
	}
	tutils.Logf("Unregistered target %s: the cluster now has %d targets\n", targets[0].directURL, n)

	// Step 2.
	createFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer destroyLocalBucket(t, m.proxyURL, m.bucket)

	if usingSG {
		sgl = client.Mem2.NewSGL(filesize)
		defer sgl.Free()
	}

	// Step 3.
	tutils.Logf("PUT %d objects into bucket %s...\n", num, m.bucket)
	start := time.Now()
	putRandObjs(m.proxyURL, seed, filesize, num, m.bucket, errCh, filenameCh, SmokeDir, SmokeStr, true, sgl)
	selectErr(errCh, "put", t, false)
	close(filenameCh)
	close(errCh)
	tutils.Logf("PUT time: %v\n", time.Since(start))
	for f := range filenameCh {
		m.repFilenameCh <- repFile{repetitions: m.numGetsEachFile, filename: f}
	}

	// Step 4.
	m.wg.Add(num*m.numGetsEachFile + 1)
	go func() {
		doReregisterTarget(targets[0].sid, targets[0].directURL, &m)
		m.wg.Done()
	}()
	doGetsInParallel(&m)

	m.wg.Wait()
	// ===================================================================
	// the timeline (denoted as well in the doReregisterTarget() function) looks as follows:
	// 	- T1: client executes ReRegister
	// 	- T2: the cluster map gets updated
	// 	- T3: re-registered target gets the updated local bucket map
	// all the while GETs are running, and the "before" and "after" counters are almost
	// exactly "separated" by the time T3 ("almost" because of the Sleep in doGetsInParallel())
	// ===================================================================
	resultsBeforeAfter(&m, num, maxErrPct)
	assertClusterState(&m)
}

// All of the above PLUS proxy failover/failback sequence in parallel
// Namely:
// 1. Unregister a target
// 2. Create a local bucket
// 3. Crash the primary proxy and PUT in parallel
// 4. Failback to the original primary proxy, re-register target, and GET in parallel
func TestProxyFailbackAndReRegisterInParallel(t *testing.T) {
	const (
		num                 = 20000
		otherTasksToTrigger = 1
		filesize            = 1024
		seed                = int64(111)
		maxErrPct           = 5
	)

	var (
		err error
		m   = metadata{
			t:                   t,
			delay:               15 * time.Second,
			otherTasksToTrigger: otherTasksToTrigger,
			num:                 num,
			numGetsEachFile:     5,
			repFilenameCh:       make(chan repFile, num),
			semaphore:           make(chan struct{}, 10), // 10 concurrent GET requests at a time
			controlCh:           make(chan struct{}, otherTasksToTrigger),
			wg:                  &sync.WaitGroup{},
			bucket:              TestLocalBucketName,
		}
		// Currently, a small percentage of GET errors can be reasonably expected as a result of this test.
		// With the current design of dfc, there is exists a brief period in which the cluster map is synced to
		// all nodes in the cluster during re-registering. During this period, errors can occur.
		filenameCh = make(chan string, m.num)
		errCh      = make(chan error, m.num)
		sgl        *memsys.SGL
	)
	if testing.Short() {
		t.Skip(skipping)
	}

	// Step 1.
	saveClusterState(&m)
	if m.originalTargetCount < 2 {
		t.Fatalf("Must have 2 or more targets in the cluster, have only %d", m.originalTargetCount)
	}
	if m.originalProxyCount < 3 {
		t.Fatalf("Must have 3 or more proxies/gateways in the cluster, have only %d", m.originalProxyCount)
	}

	targets := extractTargetsInfo(m.smap)
	err = client.UnregisterTarget(m.proxyURL, targets[0].sid)
	tutils.CheckFatal(err, t)
	n := len(getClusterMap(t, m.proxyURL).Tmap)
	if n != m.originalTargetCount-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets", m.originalTargetCount-1, n)
	}
	tutils.Logf("Unregistered target %s: the cluster now has %d targets\n", targets[0].directURL, n)

	// Step 2.
	createFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer destroyLocalBucket(t, m.proxyURL, m.bucket)

	if usingSG {
		sgl = client.Mem2.NewSGL(filesize)
		defer sgl.Free()
	}

	// Step 3.
	_, newPrimaryURL, err := chooseNextProxy(&m.smap)
	// use a new proxuURL because primaryCrashElectRestart has a side-effect:
	// it changes the primary proxy. Without the change putRandObjs is
	// failing while the current primary is restarting and rejoining
	m.proxyURL = newPrimaryURL
	tutils.CheckFatal(err, t)

	m.wg.Add(1)
	go func() {
		primaryCrashElectRestart(t)
		m.wg.Done()
	}()

	// PUT phase is timed to ensure it doesn't finish before primaryCrashElectRestart() begins
	time.Sleep(5 * time.Second)
	tutils.Logf("PUT %d objects into bucket %s...\n", num, m.bucket)
	putRandObjs(m.proxyURL, seed, filesize, num, m.bucket, errCh, filenameCh, SmokeDir, SmokeStr, true, sgl)
	selectErr(errCh, "put", t, false)
	close(filenameCh)
	close(errCh)

	for f := range filenameCh {
		m.repFilenameCh <- repFile{repetitions: m.numGetsEachFile, filename: f}
	}
	m.wg.Wait()

	// Step 4.

	// m.num*m.numGetsEachFile is for doGetsInParallel and +2 is for goroutines
	// below (one for doReregisterTarget and second for primarySetToOriginal)
	m.wg.Add(m.num*m.numGetsEachFile + 2)

	go func() {
		doReregisterTarget(targets[0].sid, targets[0].directURL, &m)
		m.wg.Done()
	}()

	go func() {
		<-m.controlCh
		primarySetToOriginal(t)
		m.wg.Done()
	}()

	doGetsInParallel(&m)

	m.wg.Wait()
	resultsBeforeAfter(&m, num, maxErrPct)
	assertClusterState(&m)
}

func TestUnregisterPreviouslyUnregisteredTarget(t *testing.T) {
	var (
		err error
		m   = metadata{
			t:      t,
			wg:     &sync.WaitGroup{},
			bucket: TestLocalBucketName,
		}
	)
	if testing.Short() {
		t.Skip(skipping)
	}

	// Initialize metadata
	saveClusterState(&m)
	if m.originalTargetCount < 2 {
		t.Fatalf("Must have 2 or more targets in the cluster, have only %d", m.originalTargetCount)
	}
	tutils.Logf("Num targets %d, num proxies %d\n", m.originalTargetCount, m.originalProxyCount)

	targets := extractTargetsInfo(m.smap)
	// Unregister target
	err = client.UnregisterTarget(m.proxyURL, targets[0].sid)
	tutils.CheckFatal(err, t)
	n := len(getClusterMap(t, m.proxyURL).Tmap)
	if n != m.originalTargetCount-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets", m.originalTargetCount-1, n)
	}
	tutils.Logf("Unregistered target %s: the cluster now has %d targets\n", targets[0].directURL, n)

	// Unregister same target again
	err = client.UnregisterTarget(m.proxyURL, targets[0].sid)
	if err == nil || !strings.Contains(err.Error(), "Not Found") {
		t.Fatal("Unregistering the same target twice must return error 404")
	}
	n = len(getClusterMap(t, m.proxyURL).Tmap)
	if n != m.originalTargetCount-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets", m.originalTargetCount-1, n)
	}
	tutils.Logf("Unregistered target %s: the cluster now has %d targets\n", targets[0].directURL, n)

	// Register target (bring cluster to normal state)
	doReregisterTarget(targets[0].sid, targets[0].directURL, &m)
	assertClusterState(&m)
}

func TestRegisterAndUnregisterTargetAndPutInParallel(t *testing.T) {
	const (
		num       = 10000
		filesize  = 1024
		seed      = int64(111)
		maxErrPct = 5
	)

	var (
		err error
		m   = metadata{
			t:             t,
			delay:         0 * time.Second,
			num:           num,
			repFilenameCh: make(chan repFile, num),
			semaphore:     make(chan struct{}, 10), // 10 concurrent GET requests at a time
			wg:            &sync.WaitGroup{},
			bucket:        TestLocalBucketName,
		}
		// Currently, a small percentage of GET errors can be reasonably expected as a result of this test.
		// With the current design of dfc, there is exists a brief period in which the cluster map is synced to
		// all nodes in the cluster during re-registering. During this period, errors can occur.
		filenameCh = make(chan string, m.num)
		errCh      = make(chan error, m.num)
		sgl        *memsys.SGL
	)
	if testing.Short() {
		t.Skip(skipping)
	}

	// Initialize metadata
	saveClusterState(&m)
	if m.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
	}
	targets := extractTargetsInfo(m.smap)

	// Create local bucket
	m.bucket = TestLocalBucketName
	createFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer destroyLocalBucket(t, m.proxyURL, m.bucket)

	if usingSG {
		sgl = client.Mem2.NewSGL(filesize)
		defer sgl.Free()
	}

	// Unregister target 0
	err = client.UnregisterTarget(m.proxyURL, targets[0].sid)
	tutils.CheckFatal(err, t)
	n := len(getClusterMap(t, m.proxyURL).Tmap)
	if n != m.originalTargetCount-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets", m.originalTargetCount-1, n)
	}

	// Do puts in parallel
	m.wg.Add(1)
	go func() {
		// Put some files
		tutils.Logf("PUT %d files into bucket %s...\n", num, m.bucket)
		putRandObjs(m.proxyURL, seed, filesize, num, m.bucket, errCh, filenameCh, SmokeDir, SmokeStr, true, sgl)
		selectErr(errCh, "put", t, false)
		close(filenameCh)
		close(errCh)

		m.wg.Done()
		tutils.Logln("PUT done.")
	}()

	// Register target 0 in parallel
	m.wg.Add(1)
	go func() {
		tutils.Logf("Registering target: %s\n", targets[0].directURL)
		err = client.RegisterTarget(targets[0].sid, targets[0].directURL, m.smap)
		tutils.CheckFatal(err, t)
		m.wg.Done()
		tutils.Logf("Registered target %s again\n", targets[0].directURL)
	}()

	// Unregister target 1 in parallel
	m.wg.Add(1)
	go func() {
		tutils.Logf("Unregistering target: %s\n", targets[1].directURL)
		err = client.UnregisterTarget(m.proxyURL, targets[1].sid)
		tutils.CheckFatal(err, t)
		m.wg.Done()
		tutils.Logf("Unregistered target %s\n", targets[1].directURL)
	}()

	// Wait for everything to end
	m.wg.Wait()

	// Register target 1 to bring cluster to original state
	doReregisterTarget(targets[1].sid, targets[1].directURL, &m)
	assertClusterState(&m)
}

func TestRebalanceAfterUnregisterAndReregister(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	const (
		num       = 10000
		filesize  = 1024
		seed      = int64(111)
		maxErrPct = 0
	)

	var (
		err error
		m   = metadata{
			t:               t,
			delay:           0 * time.Second,
			num:             num,
			numGetsEachFile: 1,
			repFilenameCh:   make(chan repFile, num),
			semaphore:       make(chan struct{}, 10), // 10 concurrent GET requests at a time
			wg:              &sync.WaitGroup{},
			bucket:          TestLocalBucketName,
		}
		filenameCh = make(chan string, m.num)
		errCh      = make(chan error, m.num)
		sgl        *memsys.SGL
	)

	// Initialize metadata
	saveClusterState(&m)
	if m.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
	}
	targets := extractTargetsInfo(m.smap)

	// Create local bucket
	createFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer destroyLocalBucket(t, m.proxyURL, m.bucket)

	if usingSG {
		sgl = client.Mem2.NewSGL(filesize)
		defer sgl.Free()
	}

	// Unregister target 0
	err = client.UnregisterTarget(m.proxyURL, targets[0].sid)
	tutils.CheckFatal(err, t)
	n := len(getClusterMap(t, m.proxyURL).Tmap)
	if n != m.originalTargetCount-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets", m.originalTargetCount-1, n)
	}

	// Put some files
	tutils.Logf("PUT %d objects into bucket %s...\n", num, m.bucket)
	putRandObjs(m.proxyURL, seed, filesize, num, m.bucket, errCh, filenameCh, SmokeDir, SmokeStr, true, sgl)
	selectErr(errCh, "put", t, false)
	close(filenameCh)
	close(errCh)

	for f := range filenameCh {
		m.repFilenameCh <- repFile{repetitions: m.numGetsEachFile, filename: f}
	}

	// Register target 0 in parallel
	m.wg.Add(1)
	go func() {
		tutils.Logf("trying to register target: %s\n", targets[0].directURL)
		err = client.RegisterTarget(targets[0].sid, targets[0].directURL, m.smap)
		tutils.CheckFatal(err, t)
		m.wg.Done()
		tutils.Logf("registered target %s again\n", targets[0].directURL)
	}()

	// Unregister target 1 in parallel
	m.wg.Add(1)
	go func() {
		tutils.Logf("trying to unregister target: %s\n", targets[1].directURL)
		err = client.UnregisterTarget(m.proxyURL, targets[1].sid)
		tutils.CheckFatal(err, t)
		m.wg.Done()
		tutils.Logf("unregistered target %s\n", targets[1].directURL)
	}()

	// Wait for everything to end
	m.wg.Wait()

	// Register target 1 to bring cluster to original state
	doReregisterTarget(targets[1].sid, targets[1].directURL, &m)
	tutils.Logln("reregistering complete")

	waitForRebalanceToComplete(t, m.proxyURL)

	m.wg.Add(num * m.numGetsEachFile)
	doGetsInParallel(&m)
	m.wg.Wait()

	resultsBeforeAfter(&m, num, maxErrPct)
	assertClusterState(&m)
}

func TestPutDuringRebalance(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	const (
		num       = 10000
		filesize  = 1024
		seed      = int64(111)
		maxErrPct = 0
	)

	var (
		err error
		m   = metadata{
			t:               t,
			delay:           0 * time.Second,
			num:             num,
			numGetsEachFile: 1,
			repFilenameCh:   make(chan repFile, num),
			semaphore:       make(chan struct{}, 10), // 10 concurrent GET requests at a time
			wg:              &sync.WaitGroup{},
			bucket:          TestLocalBucketName,
		}
		filenameCh = make(chan string, m.num)
		errCh      = make(chan error, m.num)
		sgl        *memsys.SGL
	)

	// Init. metadata
	saveClusterState(&m)
	if m.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
	}
	targets := extractTargetsInfo(m.smap)

	// Create local bucket
	createFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer destroyLocalBucket(t, m.proxyURL, m.bucket)

	if usingSG {
		sgl = client.Mem2.NewSGL(filesize)
		defer sgl.Free()
	}

	// Unregister a target
	tutils.Logf("Trying to unregister target: %s\n", targets[0].directURL)
	err = client.UnregisterTarget(m.proxyURL, targets[0].sid)
	tutils.CheckFatal(err, t)
	n := len(getClusterMap(t, m.proxyURL).Tmap)
	if n != m.originalTargetCount-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets", m.originalTargetCount-1, n)
	}

	// Start putting files and register target in parallel
	m.wg.Add(1)
	go func() {
		// sleep some time to wait for PUT operations to begin
		time.Sleep(3 * time.Second)
		tutils.Logf("Trying to register target: %s\n", targets[0].directURL)
		err = client.RegisterTarget(targets[0].sid, targets[0].directURL, m.smap)
		tutils.CheckFatal(err, t)
		m.wg.Done()
		tutils.Logf("Target %s is registered again.\n", targets[0].directURL)
	}()

	tutils.Logf("PUT %d objects into bucket %s...\n", num, m.bucket)
	putRandObjs(m.proxyURL, seed, filesize, num, m.bucket, errCh, filenameCh, SmokeDir, SmokeStr, true, sgl)
	selectErr(errCh, "put", t, false)
	close(filenameCh)
	close(errCh)

	for f := range filenameCh {
		m.repFilenameCh <- repFile{repetitions: m.numGetsEachFile, filename: f}
	}
	tutils.Logf("PUT done.\n")

	// Wait for everything to finish
	m.wg.Wait()
	waitForRebalanceToComplete(t, m.proxyURL)

	// main check - try to read all objects
	m.wg.Add(num * m.numGetsEachFile)
	doGetsInParallel(&m)
	m.wg.Wait()

	checkObjectDistribution(t, &m)

	assertClusterState(&m)
}

func TestGetDuringLocalAndGlobalRebalance(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	const (
		num       = 20000
		filesize  = 1024
		seed      = int64(112)
		maxErrPct = 0
	)

	var (
		md = metadata{
			t:               t,
			delay:           0 * time.Second,
			num:             num,
			numGetsEachFile: 10,
			repFilenameCh:   make(chan repFile, num),
			semaphore:       make(chan struct{}, 10), // 10 concurrent GET requests at a time
			wg:              &sync.WaitGroup{},
			bucket:          TestLocalBucketName,
		}
		err        error
		filenameCh = make(chan string, md.num)
		errCh      = make(chan error, md.num)
		sgl        *memsys.SGL
	)

	// Init. metadata
	saveClusterState(&md)
	if md.originalTargetCount < 2 {
		t.Fatalf("Must have at least 2 target in the cluster")
	}

	// Create local bucket
	createFreshLocalBucket(t, md.proxyURL, md.bucket)
	defer destroyLocalBucket(t, md.proxyURL, md.bucket)

	if usingSG {
		sgl = client.Mem2.NewSGL(filesize)
		defer sgl.Free()
	}

	var (
		targetURL     string
		killTargetURL string
		killTargetSI  string
	)

	// select a random target to disable one of its mountpaths,
	// and another random target to unregister
	for _, tinfo := range md.smap.Tmap {
		if targetURL == "" {
			targetURL = tinfo.PublicNet.DirectURL
		} else {
			killTargetURL = tinfo.PublicNet.DirectURL
			killTargetSI = tinfo.DaemonID
			break
		}
	}

	mpList, err := client.TargetMountpaths(targetURL)
	tutils.CheckFatal(err, t)

	if len(mpList.Available) < 2 {
		t.Fatalf("Must have at least 2 mountpaths")
	}

	// Disable mountpaths temporarily
	mpath := mpList.Available[0]
	err = client.DisableTargetMountpath(targetURL, mpath)

	// Unregister a target
	tutils.Logf("Trying to unregister target: %s\n", killTargetURL)
	err = client.UnregisterTarget(md.proxyURL, killTargetSI)
	tutils.CheckFatal(err, t)
	smap, err := waitForPrimaryProxy(
		md.proxyURL,
		"target is gone",
		md.smap.Version, testing.Verbose(),
		md.originalProxyCount,
		md.originalTargetCount-1,
	)
	tutils.CheckFatal(err, md.t)

	tutils.Logf("PUT %d objects into bucket %s...\n", num, md.bucket)
	putRandObjs(md.proxyURL, seed, filesize, num, md.bucket, errCh, filenameCh, SmokeDir, SmokeStr, true, sgl)
	selectErr(errCh, "put", t, false)
	close(filenameCh)
	close(errCh)
	for f := range filenameCh {
		md.repFilenameCh <- repFile{repetitions: md.numGetsEachFile, filename: f}
	}
	tutils.Logf("PUT done.\n")

	// Start getting objects
	md.wg.Add(num * md.numGetsEachFile)
	go func() {
		doGetsInParallel(&md)
	}()

	// register a new target
	err = client.RegisterTarget(killTargetSI, killTargetURL, md.smap)
	tutils.CheckFatal(err, t)

	// enable mountpath
	err = client.EnableTargetMountpath(targetURL, mpath)
	tutils.CheckFatal(err, t)

	// wait until GETs are done while 2 rebalance are running
	md.wg.Wait()

	// make sure that the cluster has all targets enabled
	_, err = waitForPrimaryProxy(
		md.proxyURL,
		"to join target back",
		smap.Version, testing.Verbose(),
		md.originalProxyCount,
		md.originalTargetCount,
	)
	tutils.CheckFatal(err, md.t)

	resultsBeforeAfter(&md, num, maxErrPct)

	mpListAfter, err := client.TargetMountpaths(targetURL)
	tutils.CheckFatal(err, t)
	if len(mpList.Available) != len(mpListAfter.Available) {
		t.Fatalf("Some mountpaths failed to enable: the number before %d, after %d",
			len(mpList.Available), len(mpListAfter.Available))
	}
}

func TestGetDuringLocalRebalance(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	const (
		num       = 20000
		filesize  = 1024
		seed      = int64(112)
		maxErrPct = 0
	)

	var (
		md = metadata{
			t:               t,
			delay:           0 * time.Second,
			num:             num,
			numGetsEachFile: 1,
			repFilenameCh:   make(chan repFile, num),
			semaphore:       make(chan struct{}, 10), // 10 concurrent GET requests at a time
			wg:              &sync.WaitGroup{},
			bucket:          TestLocalBucketName,
		}
		err        error
		filenameCh = make(chan string, md.num)
		errCh      = make(chan error, md.num)
		sgl        *memsys.SGL
	)

	// Init. metadata
	saveClusterState(&md)
	if md.originalTargetCount < 1 {
		t.Fatalf("Must have at least 1 target in the cluster")
	}

	// Create local bucket
	createFreshLocalBucket(t, md.proxyURL, md.bucket)
	defer destroyLocalBucket(t, md.proxyURL, md.bucket)

	if usingSG {
		sgl = client.Mem2.NewSGL(filesize)
		defer sgl.Free()
	}

	var targetURL string
	for _, tinfo := range md.smap.Tmap {
		targetURL = tinfo.PublicNet.DirectURL
		break
	}

	mpList, err := client.TargetMountpaths(targetURL)
	tutils.CheckFatal(err, t)

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
		err = client.DisableTargetMountpath(targetURL, mp)
		tutils.CheckFatal(err, t)
	}

	tutils.Logf("PUT %d objects into bucket %s...\n", num, md.bucket)
	putRandObjs(md.proxyURL, seed, filesize, num, md.bucket, errCh, filenameCh, SmokeDir, SmokeStr, true, sgl)
	selectErr(errCh, "put", t, false)
	close(filenameCh)
	close(errCh)
	for f := range filenameCh {
		md.repFilenameCh <- repFile{repetitions: md.numGetsEachFile, filename: f}
	}
	tutils.Logf("PUT done.\n")

	// Start getting objects and enable mountpaths in parallel
	md.wg.Add(num * md.numGetsEachFile)
	doGetsInParallel(&md)

	for _, mp := range mpaths {
		// sleep for a while before enabling another mountpath
		time.Sleep(50 * time.Millisecond)
		err = client.EnableTargetMountpath(targetURL, mp)
		tutils.CheckFatal(err, t)
	}

	md.wg.Wait()
	resultsBeforeAfter(&md, num, maxErrPct)

	mpListAfter, err := client.TargetMountpaths(targetURL)
	tutils.CheckFatal(err, t)
	if len(mpList.Available) != len(mpListAfter.Available) {
		t.Fatalf("Some mountpaths failed to enable: the number before %d, after %d",
			len(mpList.Available), len(mpListAfter.Available))
	}
}

func TestGetDuringRebalance(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	const (
		num       = 10000
		filesize  = 1024
		seed      = int64(111)
		maxErrPct = 0
	)

	var (
		md = metadata{
			t:               t,
			delay:           0 * time.Second,
			num:             num,
			numGetsEachFile: 1,
			repFilenameCh:   make(chan repFile, num),
			semaphore:       make(chan struct{}, 10), // 10 concurrent GET requests at a time
			wg:              &sync.WaitGroup{},
			bucket:          TestLocalBucketName,
		}
		mdAfterRebalance = metadata{
			t:               t,
			delay:           0 * time.Second,
			num:             num,
			numGetsEachFile: 1,
			repFilenameCh:   make(chan repFile, num),
			semaphore:       make(chan struct{}, 10),
			wg:              &sync.WaitGroup{},
			bucket:          TestLocalBucketName,
		}
		err        error
		filenameCh = make(chan string, md.num)
		errCh      = make(chan error, md.num)
		sgl        *memsys.SGL
	)

	// Init. metadata
	saveClusterState(&md)
	mdAfterRebalance.proxyURL = md.proxyURL

	if md.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", md.originalTargetCount)
	}
	targets := extractTargetsInfo(md.smap)

	// Create local bucket
	createFreshLocalBucket(t, md.proxyURL, md.bucket)
	defer destroyLocalBucket(t, md.proxyURL, md.bucket)

	if usingSG {
		sgl = client.Mem2.NewSGL(filesize)
		defer sgl.Free()
	}

	// Unregister a target
	err = client.UnregisterTarget(md.proxyURL, targets[0].sid)
	tutils.CheckFatal(err, t)
	n := len(getClusterMap(t, md.proxyURL).Tmap)
	if n != md.originalTargetCount-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets", md.originalTargetCount-1, n)
	}

	// Start putting files into bucket
	tutils.Logf("PUT %d objects into bucket %s...\n", num, md.bucket)
	putRandObjs(md.proxyURL, seed, filesize, num, md.bucket, errCh, filenameCh, SmokeDir, SmokeStr, true, sgl)
	selectErr(errCh, "put", t, false)
	close(filenameCh)
	close(errCh)
	for f := range filenameCh {
		md.repFilenameCh <- repFile{repetitions: md.numGetsEachFile, filename: f}
		mdAfterRebalance.repFilenameCh <- repFile{repetitions: mdAfterRebalance.numGetsEachFile, filename: f}
	}
	tutils.Logf("PUT done.\n")

	// Start getting objects and register target in parallel
	md.wg.Add(num * md.numGetsEachFile)
	doGetsInParallel(&md)

	tutils.Logf("Trying to register target: %s\n", targets[0].directURL)
	err = client.RegisterTarget(targets[0].sid, targets[0].directURL, md.smap)
	tutils.CheckFatal(err, t)

	// wait for everything to finish
	waitForRebalanceToComplete(t, md.proxyURL)
	md.wg.Wait()

	// read files once again
	mdAfterRebalance.wg.Add(num * mdAfterRebalance.numGetsEachFile)
	doGetsInParallel(&mdAfterRebalance)
	mdAfterRebalance.wg.Wait()
	resultsBeforeAfter(&mdAfterRebalance, num, maxErrPct)

	assertClusterState(&md)
}

func TestRegisterTargetsAndCreateLocalBucketsInParallel(t *testing.T) {
	const (
		unregisterTargetCount = 2
		newLocalBucketCount   = 3
	)
	var (
		err error
		m   = metadata{
			t:      t,
			wg:     &sync.WaitGroup{},
			bucket: TestLocalBucketName,
		}
	)
	if testing.Short() {
		t.Skip(skipping)
	}

	// Initialize metadata
	saveClusterState(&m)
	if m.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
	}
	tutils.Logf("Num targets %d\n", m.originalTargetCount)
	targets := extractTargetsInfo(m.smap)

	// Unregister targets
	for i := 0; i < unregisterTargetCount; i++ {
		err = client.UnregisterTarget(m.proxyURL, targets[i].sid)
		tutils.CheckFatal(err, t)
		n := len(getClusterMap(t, m.proxyURL).Tmap)
		if n != m.originalTargetCount-(i+1) {
			t.Fatalf("%d targets expected after unregister, actually %d targets", m.originalTargetCount-(i+1), n)
		}
		tutils.Logf("Unregistered target %s: the cluster now has %d targets\n", targets[i].directURL, n)
	}

	m.wg.Add(unregisterTargetCount)
	for i := 0; i < unregisterTargetCount; i++ {
		go func(number int) {
			err = client.RegisterTarget(targets[number].sid, targets[number].directURL, m.smap)
			tutils.CheckFatal(err, t)
			m.wg.Done()
		}(i)
	}

	m.wg.Add(newLocalBucketCount)
	for i := 0; i < newLocalBucketCount; i++ {
		go func(number int) {
			createFreshLocalBucket(t, m.proxyURL, m.bucket+strconv.Itoa(number))
			m.wg.Done()
		}(i)

		defer destroyLocalBucket(t, m.proxyURL, m.bucket+strconv.Itoa(i))
	}
	m.wg.Wait()
	assertClusterState(&m)
}

func TestRenameEmptyLocalBucket(t *testing.T) {
	const (
		newTestLocalBucketName = TestLocalBucketName + "_new"
	)
	var (
		err error
		m   = metadata{
			t:      t,
			wg:     &sync.WaitGroup{},
			bucket: TestLocalBucketName,
		}
	)

	// Initialize metadata
	saveClusterState(&m)
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create local bucket
	createFreshLocalBucket(t, m.proxyURL, m.bucket)
	destroyLocalBucket(t, m.proxyURL, newTestLocalBucketName)

	// Rename it
	err = client.RenameLocalBucket(m.proxyURL, m.bucket, newTestLocalBucketName)
	tutils.CheckFatal(err, t)

	// Destroy renamed local bucket
	destroyLocalBucket(t, m.proxyURL, newTestLocalBucketName)
}

func TestRenameNonEmptyLocalBucket(t *testing.T) {
	const (
		newTestLocalBucketName = TestLocalBucketName + "_new"
		num                    = 1000
		filesize               = 1024
		seed                   = int64(111)
		maxErrPct              = 0
	)

	var (
		err error
		m   = metadata{
			t:               t,
			delay:           0 * time.Second,
			num:             num,
			numGetsEachFile: 2,
			repFilenameCh:   make(chan repFile, num),
			semaphore:       make(chan struct{}, 10), // 10 concurrent GET requests at a time
			wg:              &sync.WaitGroup{},
			bucket:          TestLocalBucketName,
		}
		filenameCh = make(chan string, m.num)
		errCh      = make(chan error, m.num)
		sgl        *memsys.SGL
	)

	// Initialize metadata
	saveClusterState(&m)
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create local bucket
	createFreshLocalBucket(t, m.proxyURL, m.bucket)
	destroyLocalBucket(t, m.proxyURL, newTestLocalBucketName)

	if usingSG {
		sgl = client.Mem2.NewSGL(filesize)
		defer sgl.Free()
	}

	// Put some files
	tutils.Logf("PUT %d objects into bucket %s...\n", num, m.bucket)
	putRandObjs(m.proxyURL, seed, filesize, num, m.bucket, errCh, filenameCh, SmokeDir, SmokeStr, true, sgl)
	selectErr(errCh, "put", t, false)
	close(filenameCh)
	close(errCh)
	for f := range filenameCh {
		m.repFilenameCh <- repFile{repetitions: m.numGetsEachFile, filename: f}
	}

	tutils.Logln("PUT done.")

	// Rename it
	oldLocalBucketName := m.bucket
	m.bucket = newTestLocalBucketName
	err = client.RenameLocalBucket(m.proxyURL, oldLocalBucketName, m.bucket)
	tutils.CheckFatal(err, t)

	// Gets on renamed local bucket
	m.wg.Add(num * m.numGetsEachFile)
	doGetsInParallel(&m)
	m.wg.Wait()
	resultsBeforeAfter(&m, num, maxErrPct)

	// Destroy renamed local bucket
	destroyLocalBucket(t, m.proxyURL, m.bucket)
}

func TestDirectoryExistenceWhenModifyingBucket(t *testing.T) {
	const (
		newTestLocalBucketName = TestLocalBucketName + "_new"
	)
	var (
		err error
		m   = metadata{
			t:      t,
			wg:     &sync.WaitGroup{},
			bucket: TestLocalBucketName,
		}
	)

	// Initialize metadata
	saveClusterState(&m)
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	localBucketDir := ""
	fsWalkFunc := func(path string, info os.FileInfo, err error) error {
		if localBucketDir != "" {
			return filepath.SkipDir
		}
		if strings.HasSuffix(path, "/local") {
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
	createFreshLocalBucket(t, m.proxyURL, m.bucket)
	destroyLocalBucket(t, m.proxyURL, newTestLocalBucketName)

	if _, err := os.Stat(bucketFQN); os.IsNotExist(err) {
		t.Fatalf("local bucket folder was not created")
	}

	// Rename local bucket
	err = client.RenameLocalBucket(m.proxyURL, m.bucket, newTestLocalBucketName)
	tutils.CheckFatal(err, t)
	if _, err := os.Stat(bucketFQN); !os.IsNotExist(err) {
		t.Fatalf("local bucket folder was not deleted")
	}

	if _, err := os.Stat(newBucketFQN); os.IsNotExist(err) {
		t.Fatalf("new local bucket folder was not created")
	}

	// Destroy renamed local bucket
	destroyLocalBucket(t, m.proxyURL, newTestLocalBucketName)
	if _, err := os.Stat(newBucketFQN); !os.IsNotExist(err) {
		t.Fatalf("new local bucket folder was not deleted")
	}
}

func TestAddAndRemoveMountpath(t *testing.T) {
	const (
		num       = 5000
		filesize  = 1024
		seed      = int64(111)
		maxErrPct = 0
	)

	var (
		err error
		m   = metadata{
			t:               t,
			delay:           0 * time.Second,
			num:             num,
			numGetsEachFile: 2,
			repFilenameCh:   make(chan repFile, num),
			semaphore:       make(chan struct{}, 10), // 10 concurrent GET requests at a time
			wg:              &sync.WaitGroup{},
			bucket:          TestLocalBucketName,
		}
		filenameCh = make(chan string, m.num)
		errCh      = make(chan error, m.num)
		sgl        *memsys.SGL
	)

	if testing.Short() {
		t.Skip(skipping)
	}

	if usingSG {
		sgl = client.Mem2.NewSGL(filesize)
		defer sgl.Free()
	}

	// Initialize metadata
	saveClusterState(&m)
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}
	targets := extractTargetsInfo(m.smap)

	// Remove all mountpaths for one target
	oldMountpaths, err := client.TargetMountpaths(targets[0].directURL)
	tutils.CheckFatal(err, t)

	for _, mpath := range oldMountpaths.Available {
		err = client.RemoveTargetMountpath(targets[0].directURL, mpath)
		tutils.CheckFatal(err, t)
	}

	// Check if mountpaths were actually removed
	mountpaths, err := client.TargetMountpaths(targets[0].directURL)
	tutils.CheckFatal(err, t)

	if len(mountpaths.Available) != 0 {
		t.Fatalf("Target should not have any paths available")
	}

	// Create local bucket
	createFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer destroyLocalBucket(t, m.proxyURL, m.bucket)

	// Add target mountpath again
	for _, mpath := range oldMountpaths.Available {
		err = client.AddTargetMountpath(targets[0].directURL, mpath)
		tutils.CheckFatal(err, t)
	}

	// Check if mountpaths were actually added
	mountpaths, err = client.TargetMountpaths(targets[0].directURL)
	tutils.CheckFatal(err, t)

	if len(mountpaths.Available) != len(oldMountpaths.Available) {
		t.Fatalf("Target should have old mountpath available restored")
	}

	// Put and read random files
	putRandObjs(m.proxyURL, seed, filesize, num, m.bucket, errCh, filenameCh, SmokeDir, SmokeStr, true, sgl)
	selectErr(errCh, "put", t, false)
	close(filenameCh)
	close(errCh)
	for f := range filenameCh {
		m.repFilenameCh <- repFile{repetitions: m.numGetsEachFile, filename: f}
	}

	m.wg.Add(m.num * m.numGetsEachFile)
	doGetsInParallel(&m)
	m.wg.Wait()
	resultsBeforeAfter(&m, num, maxErrPct)
}

func TestLocalRebalanceAfterAddingMountpath(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	const (
		num          = 5000
		filesize     = 1024
		seed         = int64(111)
		maxErrPct    = 0
		newMountpath = "/tmp/dfc"
	)

	var (
		err error
		m   = metadata{
			t:               t,
			delay:           0 * time.Second,
			num:             num,
			numGetsEachFile: 2,
			repFilenameCh:   make(chan repFile, num),
			semaphore:       make(chan struct{}, 10), // 10 concurrent GET requests at a time
			wg:              &sync.WaitGroup{},
			bucket:          TestLocalBucketName,
		}
		filenameCh = make(chan string, m.num)
		errCh      = make(chan error, m.num)
		sgl        *memsys.SGL
	)

	if usingSG {
		sgl = client.Mem2.NewSGL(filesize)
		defer sgl.Free()
	}

	// Initialize metadata
	saveClusterState(&m)
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}
	targets := extractTargetsInfo(m.smap)

	// Create local bucket
	createFreshLocalBucket(t, m.proxyURL, m.bucket)

	defer func() {
		os.RemoveAll(newMountpath)
		destroyLocalBucket(t, m.proxyURL, m.bucket)
	}()

	// Put random files
	putRandObjs(m.proxyURL, seed, filesize, num, m.bucket, errCh, filenameCh, SmokeDir, SmokeStr, true, sgl)
	selectErr(errCh, "put", t, false)
	close(filenameCh)
	close(errCh)
	for f := range filenameCh {
		m.repFilenameCh <- repFile{repetitions: m.numGetsEachFile, filename: f}
	}

	// Add new mountpath to target
	err = client.AddTargetMountpath(targets[0].directURL, newMountpath)
	tutils.CheckFatal(err, t)

	waitForRebalanceToComplete(t, m.proxyURL)

	// Read files after rebalance
	m.wg.Add(m.num * m.numGetsEachFile)
	doGetsInParallel(&m)
	m.wg.Wait()

	// Remove new mountpath from target
	err = client.RemoveTargetMountpath(targets[0].directURL, newMountpath)
	tutils.CheckFatal(err, t)

	resultsBeforeAfter(&m, num, maxErrPct)
}

func TestGlobalAndLocalRebalanceAfterAddingMountpath(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	const (
		num          = 10000
		filesize     = 1024
		seed         = int64(111)
		maxErrPct    = 0
		newMountpath = "/tmp/dfc/mountpath"
	)

	var (
		err error
		m   = metadata{
			t:               t,
			delay:           0 * time.Second,
			num:             num,
			numGetsEachFile: 5,
			repFilenameCh:   make(chan repFile, num),
			semaphore:       make(chan struct{}, 10), // 10 concurrent GET requests at a time
			wg:              &sync.WaitGroup{},
			bucket:          TestLocalBucketName,
		}
		filenameCh = make(chan string, m.num)
		errCh      = make(chan error, m.num)
		sgl        *memsys.SGL
	)

	if usingSG {
		sgl = client.Mem2.NewSGL(filesize)
		defer sgl.Free()
	}

	// Initialize metadata
	saveClusterState(&m)
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}
	targets := extractTargetsInfo(m.smap)

	// Create local bucket
	createFreshLocalBucket(t, m.proxyURL, m.bucket)

	defer func() {
		os.RemoveAll(newMountpath)
		destroyLocalBucket(t, m.proxyURL, m.bucket)
	}()

	// Put random files
	putRandObjs(m.proxyURL, seed, filesize, num, m.bucket, errCh, filenameCh, SmokeDir, SmokeStr, true, sgl)
	selectErr(errCh, "put", t, false)
	close(filenameCh)
	close(errCh)
	for f := range filenameCh {
		m.repFilenameCh <- repFile{repetitions: m.numGetsEachFile, filename: f}
	}

	// Add new mountpath to all targets
	for idx, target := range targets {
		mountpath := filepath.Join(newMountpath, fmt.Sprintf("%d", idx))
		common.CreateDir(mountpath)
		err = client.AddTargetMountpath(target.directURL, mountpath)
		tutils.CheckFatal(err, t)
	}

	waitForRebalanceToComplete(t, m.proxyURL)

	// Read after rebalance
	m.wg.Add(m.num * m.numGetsEachFile)
	doGetsInParallel(&m)
	m.wg.Wait()

	// Remove new mountpath from all targets
	for idx, target := range targets {
		mountpath := filepath.Join(newMountpath, fmt.Sprintf("%d", idx))
		os.RemoveAll(mountpath)
		err = client.RemoveTargetMountpath(target.directURL, mountpath)
		tutils.CheckFatal(err, t)
	}

	resultsBeforeAfter(&m, num, maxErrPct)
}

func TestDisableAndEnableMountpath(t *testing.T) {
	const (
		num       = 5000
		filesize  = 1024
		seed      = int64(111)
		maxErrPct = 0
	)

	var (
		err error
		m   = metadata{
			t:               t,
			delay:           0 * time.Second,
			num:             num,
			numGetsEachFile: 2,
			repFilenameCh:   make(chan repFile, num),
			semaphore:       make(chan struct{}, 10), // 10 concurrent GET requests at a time
			wg:              &sync.WaitGroup{},
			bucket:          TestLocalBucketName,
		}
		filenameCh = make(chan string, m.num)
		errCh      = make(chan error, m.num)
		sgl        *memsys.SGL
	)

	if testing.Short() {
		t.Skip(skipping)
	}

	if usingSG {
		sgl = client.Mem2.NewSGL(filesize)
		defer sgl.Free()
	}

	// Initialize metadata
	saveClusterState(&m)
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}
	targets := extractTargetsInfo(m.smap)

	// Remove all mountpaths for one target
	oldMountpaths, err := client.TargetMountpaths(targets[0].directURL)
	tutils.CheckFatal(err, t)

	for _, mpath := range oldMountpaths.Available {
		err = client.DisableTargetMountpath(targets[0].directURL, mpath)
		tutils.CheckFatal(err, t)
	}

	// Check if mountpaths were actually disabled
	mountpaths, err := client.TargetMountpaths(targets[0].directURL)
	tutils.CheckFatal(err, t)

	if len(mountpaths.Available) != 0 {
		t.Fatalf("Target should not have any paths available")
	}

	if len(mountpaths.Disabled) != len(oldMountpaths.Available) {
		t.Fatalf("Not all mountpaths were added to disabled paths")
	}

	// Create local bucket
	createFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer destroyLocalBucket(t, m.proxyURL, m.bucket)

	// Add target mountpath again
	for _, mpath := range oldMountpaths.Available {
		err = client.EnableTargetMountpath(targets[0].directURL, mpath)
		tutils.CheckFatal(err, t)
	}

	// Check if mountpaths were actually enabled
	mountpaths, err = client.TargetMountpaths(targets[0].directURL)
	tutils.CheckFatal(err, t)

	if len(mountpaths.Available) != len(oldMountpaths.Available) {
		t.Fatalf("Target should have old mountpath available restored")
	}

	if len(mountpaths.Disabled) != 0 {
		t.Fatalf("Not all disabled mountpaths were enabled")
	}

	// Put and read random files
	putRandObjs(m.proxyURL, seed, filesize, num, m.bucket, errCh, filenameCh, SmokeDir, SmokeStr, true, sgl)
	selectErr(errCh, "put", t, false)
	close(filenameCh)
	close(errCh)
	for f := range filenameCh {
		m.repFilenameCh <- repFile{repetitions: m.numGetsEachFile, filename: f}
	}

	m.wg.Add(m.num * m.numGetsEachFile)
	doGetsInParallel(&m)
	m.wg.Wait()
	resultsBeforeAfter(&m, num, maxErrPct)
}

// see above - the T1/2/3 timeline and details
func resultsBeforeAfter(m *metadata, num, maxErrPct int) {
	tutils.Logf("Errors before and after time=T3 (re-registered target gets the updated local bucket map): %d and %d, respectively\n",
		m.numGetErrsBefore, m.numGetErrsAfter)
	pctBefore := int(m.numGetErrsBefore) * 100 / (num * m.numGetsEachFile)
	pctAfter := int(m.numGetErrsAfter) * 100 / (num * m.numGetsEachFile)
	if pctBefore > maxErrPct || pctAfter > maxErrPct {
		m.t.Fatalf("Error rates before %d%% or after %d%% T3 exceed the max %d%%\n", pctBefore, pctAfter, maxErrPct)
	}
}

func doGetsInParallel(m *metadata) {
	for i := 0; i < 10; i++ {
		m.semaphore <- struct{}{}
	}
	if m.numGetsEachFile == 1 {
		tutils.Logf("GET each of the %d objects from bucket %s...\n", m.num, m.bucket)
	} else {
		tutils.Logf("GET each of the %d objects %d times from bucket %s...\n", m.num, m.numGetsEachFile, m.bucket)
	}

	// GET is timed so a large portion of requests will happen both before and after the target is re-registered
	time.Sleep(m.delay)
	for i := 0; i < m.num*m.numGetsEachFile; i++ {
		go func() {
			<-m.semaphore
			defer func() {
				m.semaphore <- struct{}{}
				m.wg.Done()
			}()

			repFile := <-m.repFilenameCh
			if repFile.repetitions > 0 {
				repFile.repetitions -= 1
				m.repFilenameCh <- repFile
			}
			_, _, err := client.Get(m.proxyURL, m.bucket, SmokeStr+"/"+repFile.filename, nil, nil, false, false)
			if err != nil {
				r := atomic.LoadUint64(&(m.reregistered))
				if r == 1 {
					atomic.AddUint64(&(m.numGetErrsAfter), 1)
				} else {
					atomic.AddUint64(&(m.numGetErrsBefore), 1)
				}
			}
			g := atomic.AddUint64(&(m.getsCompleted), 1)
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

func extractTargetsInfo(smap cluster.Smap) []targetInfo {
	var targets []targetInfo
	for sid, daemon := range smap.Tmap {
		targets = append(targets, targetInfo{sid: sid, directURL: daemon.PublicNet.DirectURL})
	}
	return targets
}

func doReregisterTarget(target, targetDirectURL string, m *metadata) {
	const (
		timeout    = time.Second * 10
		interval   = time.Millisecond * 10
		iterations = int(timeout / interval)
	)

	// T1
	tutils.Logf("Re-registering target %s...\n", target)
	smap := getClusterMap(m.t, m.proxyURL)
	err := client.RegisterTarget(target, targetDirectURL, smap)
	tutils.CheckFatal(err, m.t)

	for i := 0; i < iterations; i++ {
		time.Sleep(interval)
		if _, ok := smap.Tmap[target]; !ok {
			// T2
			smap = getClusterMap(m.t, m.proxyURL)
			if _, ok := smap.Tmap[target]; ok {
				tutils.Logf("T2: re-registered target %s\n", target)
			}
		} else {
			lbNames, err := client.GetLocalBucketNames(targetDirectURL)
			tutils.CheckFatal(err, m.t)
			// T3
			if common.StringInSlice(m.bucket, lbNames.Local) {
				s := atomic.CompareAndSwapUint64(&m.reregistered, 0, 1)
				if !s {
					m.t.Errorf("reregistered should have swapped from 0 to 1. Actual reregistered = %d\n", m.reregistered)
				}
				tutils.Logf("T3: re-registered target %s got updated with the new bucket-metadata\n", target)
				break
			}
		}
	}
}

func saveClusterState(m *metadata) {
	var err error
	m.proxyURL = getPrimaryURL(m.t, proxyURLRO)
	m.smap, err = client.GetClusterMap(m.proxyURL)
	tutils.CheckFatal(err, m.t)
	m.originalTargetCount = len(m.smap.Tmap)
	m.originalProxyCount = len(m.smap.Pmap)
	tutils.Logf("Number of targets %d, number of proxies %d\n", m.originalTargetCount, m.originalProxyCount)
}

func assertClusterState(m *metadata) {
	smap, err := waitForPrimaryProxy(
		m.proxyURL,
		"to check cluster state",
		m.smap.Version, testing.Verbose(),
		m.originalProxyCount,
		m.originalTargetCount,
	)
	tutils.CheckFatal(err, m.t)

	proxyCount := len(smap.Pmap)
	targetCount := len(smap.Tmap)
	if targetCount != m.originalTargetCount ||
		proxyCount != m.originalProxyCount {
		m.t.Errorf(
			"cluster state is not preserverd. targets (before: %d, now: %d); proxies: (before: %d, now: %d)",
			targetCount, m.originalTargetCount,
			proxyCount, m.originalProxyCount,
		)
	}
}

func createFreshLocalBucket(t *testing.T, proxyURL, bucketFQN string) {
	destroyLocalBucket(t, proxyURL, bucketFQN)
	err := client.CreateLocalBucket(proxyURL, bucketFQN)
	tutils.CheckFatal(err, t)
}

func destroyLocalBucket(t *testing.T, proxyURL, bucket string) {
	exists, err := client.DoesLocalBucketExist(proxyURL, bucket)
	tutils.CheckFatal(err, t)
	if exists {
		err = client.DestroyLocalBucket(proxyURL, bucket)
		tutils.CheckFatal(err, t)
	}
}

func checkObjectDistribution(t *testing.T, m *metadata) {
	var (
		requiredCount     = int64(rebalanceObjectDistributionTestCoef * (float64(m.num) / float64(m.originalTargetCount)))
		targetObjectCount = make(map[string]int64)
	)
	tutils.Logf("Checking if each target has a required number of object in bucket %s...\n", m.bucket)
	bucketList, err := client.ListBucket(m.proxyURL, m.bucket, &api.GetMsg{GetProps: api.GetTargetURL}, 0)
	tutils.CheckFatal(err, t)
	for _, obj := range bucketList.Entries {
		targetObjectCount[obj.TargetURL] += 1
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

func TestForwardCP(t *testing.T) {
	const (
		num      = 10000
		filesize = 128
		seed     = int64(555)
	)
	var (
		random = rand.New(rand.NewSource(seed))
		m      = metadata{
			t:               t,
			num:             num,
			numGetsEachFile: 2,
			repFilenameCh:   make(chan repFile, num),
			semaphore:       make(chan struct{}, 10), // 10 concurrent GET requests at a time
			wg:              &sync.WaitGroup{},
			bucket:          tutils.FastRandomFilename(random, 13),
		}
		filenameCh = make(chan string, m.num)
		errCh      = make(chan error, m.num)
		sgl        *memsys.SGL
	)

	// Step 1.
	saveClusterState(&m)
	if m.originalProxyCount < 2 {
		t.Fatalf("Must have 2 or more proxies in the cluster, have only %d", m.originalProxyCount)
	}

	// Step 2.
	origID, origURL := m.smap.ProxySI.DaemonID, m.smap.ProxySI.PublicNet.DirectURL
	nextProxyID, nextProxyURL, _ := chooseNextProxy(&m.smap)

	destroyLocalBucket(t, m.proxyURL, m.bucket)

	createFreshLocalBucket(t, nextProxyURL, m.bucket)
	tutils.Logf("Created bucket %s via non-primary %s\n", m.bucket, nextProxyID)

	if usingSG {
		sgl = client.Mem2.NewSGL(filesize)
		defer sgl.Free()
	}

	// Step 3.
	tutils.Logf("PUT %d objects into bucket %s...\n", num, m.bucket)
	putRandObjs(m.proxyURL, seed, filesize, num, m.bucket, errCh, filenameCh, SmokeDir, SmokeStr, true, sgl)
	selectErr(errCh, "put", t, false)
	close(filenameCh)
	close(errCh)
	for f := range filenameCh {
		m.repFilenameCh <- repFile{repetitions: m.numGetsEachFile, filename: f}
	}

	// Step 4. in parallel: run GETs and designate a new primary=nextProxyID
	m.wg.Add(num*m.numGetsEachFile + 1)
	doGetsInParallel(&m)

	go func() {
		setPrimaryTo(t, m.proxyURL, m.smap, nextProxyURL, nextProxyID, nextProxyURL)
		m.proxyURL = nextProxyURL
		m.wg.Done()
	}()

	m.wg.Wait()
	if m.numGetErrsBefore+m.numGetErrsAfter > 0 {
		t.Fatalf("Unexpected: GET errors before %d and after %d", m.numGetErrsBefore, m.numGetErrsAfter)
	}

	// Step 5. destroy local bucket via original primary which is not primary at this point
	destroyLocalBucket(t, origURL, m.bucket)
	tutils.Logf("Destroyed bucket %s via non-primary %s/%s\n", m.bucket, origID, origURL)
}

func TestAtimeRebalance(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	const (
		num      = 50
		filesize = 1024
		seed     = int64(141)
	)

	var (
		err error
		m   = metadata{
			t:               t,
			delay:           0 * time.Second,
			num:             num,
			numGetsEachFile: 2,
			repFilenameCh:   make(chan repFile, num),
			semaphore:       make(chan struct{}, 10), // 10 concurrent GET requests at a time
			wg:              &sync.WaitGroup{},
			bucket:          TestLocalBucketName,
		}
		filenameCh  = make(chan string, m.num)
		errCh       = make(chan error, m.num)
		sgl         *memsys.SGL
		bucketProps dfc.BucketProps
	)

	if usingSG {
		sgl = client.Mem2.NewSGL(filesize)
		defer sgl.Free()
	}

	// Initialize metadata
	saveClusterState(&m)
	if m.originalTargetCount < 2 {
		t.Fatalf("Must have 2 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create local bucket
	createFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer destroyLocalBucket(t, m.proxyURL, m.bucket)

	// Enable bucket level LRU properties
	bucketProps.LRUProps.LRUEnabled = true
	err = client.SetBucketProps(m.proxyURL, m.bucket, bucketProps)
	tutils.CheckFatal(err, t)

	target := extractTargetsInfo(m.smap)[0]

	// Unregister a target
	tutils.Logf("Trying to unregister target: %s\n", target.directURL)
	err = client.UnregisterTarget(m.proxyURL, target.sid)
	tutils.CheckFatal(err, t)
	smap, err := waitForPrimaryProxy(
		m.proxyURL,
		"target is gone",
		m.smap.Version, testing.Verbose(),
		m.originalProxyCount,
		m.originalTargetCount-1,
	)
	tutils.CheckFatal(err, t)

	// Put random files
	putRandObjs(m.proxyURL, seed, filesize, num, m.bucket, errCh, filenameCh, SmokeDir, SmokeStr, true, sgl)
	selectErr(errCh, "put", t, false)
	close(filenameCh)
	close(errCh)
	objNames := make(map[string]string, 0)
	msg := &api.GetMsg{GetProps: api.GetPropsAtime + ", " + api.GetPropsStatus}
	bucketList, err := client.ListBucket(m.proxyURL, m.bucket, msg, 0)
	tutils.CheckFatal(err, t)

	for _, entry := range bucketList.Entries {
		objNames[entry.Name] = entry.Atime
	}

	// register a new target
	err = client.RegisterTarget(target.sid, target.directURL, m.smap)
	tutils.CheckFatal(err, t)

	// make sure that the cluster has all targets enabled
	_, err = waitForPrimaryProxy(
		m.proxyURL,
		"to join target back",
		smap.Version, testing.Verbose(),
		m.originalProxyCount,
		m.originalTargetCount,
	)
	tutils.CheckFatal(err, t)

	waitForRebalanceToComplete(t, m.proxyURL)

	bucketListReb, err := client.ListBucket(m.proxyURL, m.bucket, msg, 0)
	tutils.CheckFatal(err, t)

	itemCount := 0
	for _, entry := range bucketListReb.Entries {
		if entry.Status != api.ObjStatusOK {
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
