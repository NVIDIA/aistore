/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

package dfc_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"runtime/debug"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
)

type repFile struct {
	repetitions int
	filename    string
}

type metadata struct {
	t                   *testing.T
	smap                dfc.Smap
	delay               time.Duration
	semaphore           chan struct{}
	controlCh           chan struct{}
	repFilenameCh       chan repFile
	wg                  *sync.WaitGroup
	bucket              string
	targetDirectURL     string
	sid                 string
	otherTasksToTrigger int
	origNumTargets      int
	num                 int
	numGetsEachFile     int
	numGetErrsBefore    uint64
	numGetErrsAfter     uint64
	getsCompleted       uint64
	reregistered        uint64
}

// Intended for a deployment with multiple targets
// 1. Unregister target T
// 2. Create local bucket
// 3. PUT large amount of objects into the local bucket
// 4. GET the objects while simultaneously re-registering the target T
func TestGetAndReRegisterInParallel(t *testing.T) {
	const (
		num      = 20000
		filesize = uint64(1024)
		seed     = int64(111)
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
		// Currently, a small percentage of GET errors can be reasonably expected as a result of this test.
		// With the current design of dfc, there is exists a brief period in which the cluster map is synced to
		// all nodes in the cluster during re-registering. During this period, errors can occur.
		filenameCh    = make(chan string, m.num)
		errch         = make(chan error, m.num)
		sgl           *dfc.SGLIO
		maxNumGetErrs = uint64(m.num * m.numGetsEachFile / 10) // 10 % of GET requests
	)
	// Step 1.
	m.smap, err = client.GetClusterMap(proxyurl)
	checkFatal(err, t)

	m.origNumTargets = len(m.smap.Tmap)
	if m.origNumTargets < 2 {
		t.Fatalf("Must have 2 or more targets in the cluster, have only %d", m.origNumTargets)
	}
	for m.sid = range m.smap.Tmap {
		break
	}
	m.targetDirectURL = m.smap.Tmap[m.sid].DirectURL
	err = client.UnregisterTarget(proxyurl, m.sid)
	checkFatal(err, t)

	n := len(getClusterMap(t).Tmap)
	if n != m.origNumTargets-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets", m.origNumTargets-1, n)
	}
	tlogf("Unregistered target %s: the cluster now has %d targets\n", m.sid, n)

	// Step 2.
	err = client.CreateLocalBucket(proxyurl, m.bucket)
	checkFatal(err, t)

	defer func() {
		err = client.DestroyLocalBucket(proxyurl, m.bucket)
		checkFatal(err, t)
	}()

	if usingSG {
		sgl = dfc.NewSGLIO(filesize)
		defer sgl.Free()
	}

	// Step 3.
	tlogf("Putting %d files into bucket %s...\n", num, m.bucket)
	putRandomFiles(0, seed, filesize, num, m.bucket, t, nil, errch, filenameCh, SmokeDir, SmokeStr, "", true, sgl)
	selectErr(errch, "put", t, false)
	close(filenameCh)

	for f := range filenameCh {
		m.repFilenameCh <- repFile{repetitions: m.numGetsEachFile, filename: f}
	}

	// Step 4.
	m.wg.Add(num*m.numGetsEachFile + 1)
	go doReregisterTarget(&m)
	doGetsInParallel(&m)

	m.wg.Wait()
	tlogf("%d GET errors before target's local bucket map was updated (expected)\n", m.numGetErrsBefore)
	if m.numGetErrsAfter > maxNumGetErrs {
		t.Fatalf("Found %d GET errors (should be no more than %d/%d) after target's local bucket map was updated",
			m.numGetErrsAfter, maxNumGetErrs, num*m.numGetsEachFile)
	} else {
		tlogf("Found %d GET errors (should be no more than %d/%d) after target's local bucket map was updated",
			m.numGetErrsAfter, maxNumGetErrs, num*m.numGetsEachFile)
	}
}

// 1. Unregister a target
// 2. Create a local bucket
// 3. Crash the primary proxy and PUT in parallel
// 4. Failback to the original primary proxy, re-register target, and GET in parallel
func TestProxyFailbackAndGetAndReRegisterInParallel(t *testing.T) {
	const (
		num                 = 20000
		otherTasksToTrigger = 1
		filesize            = uint64(1024)
		seed                = int64(111)
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
			controlCh:           make(chan struct{}, otherTasksToTrigger-1),
			wg:                  &sync.WaitGroup{},
			bucket:              TestLocalBucketName,
		}
		// Currently, a small percentage of GET errors can be reasonably expected as a result of this test.
		// With the current design of dfc, there is exists a brief period in which the cluster map is synced to
		// all nodes in the cluster during re-registering. During this period, errors can occur.
		filenameCh    = make(chan string, m.num)
		errch         = make(chan error, m.num)
		sgl           *dfc.SGLIO
		maxNumGetErrs = uint64(m.num * m.numGetsEachFile / 10) // 10 % of GET requests
	)

	// Step 1.
	m.smap, err = client.GetClusterMap(proxyurl)
	checkFatal(err, t)
	m.origNumTargets = len(m.smap.Tmap)
	if m.origNumTargets < 2 {
		t.Fatalf("Must have 2 or more targets in the cluster, have only %d", m.origNumTargets)
	}
	for m.sid = range m.smap.Tmap {
		break
	}
	m.targetDirectURL = m.smap.Tmap[m.sid].DirectURL

	err = client.UnregisterTarget(proxyurl, m.sid)
	checkFatal(err, t)
	n := len(getClusterMap(t).Tmap)
	if n != m.origNumTargets-1 {
		t.Fatalf("%d targets expected after unregister, actually %d targets", m.origNumTargets-1, n)
	}
	tlogf("Unregistered target %s: the cluster now has %d targets\n", m.sid, n)

	// Step 2.
	m.bucket = TestLocalBucketName
	err = client.CreateLocalBucket(proxyurl, m.bucket)
	checkFatal(err, t)

	defer func() {
		err = client.DestroyLocalBucket(proxyurl, m.bucket)
		checkFatal(err, t)
	}()

	if usingSG {
		sgl = dfc.NewSGLIO(filesize)
		defer sgl.Free()
	}

	// Step 3.
	m.wg.Add(1)
	go func() {
		primaryCrash(t)
		m.wg.Done()
	}()

	// PUT phase is timed to ensure it doesn't finish before primaryCrash() begins
	time.Sleep(5 * time.Second)
	tlogf("Putting %d files into bucket %s...\n", num, m.bucket)
	putRandomFiles(0, seed, filesize, num, m.bucket, t, nil, errch, filenameCh, SmokeDir, SmokeStr, "", true, sgl)
	selectErr(errch, "put", t, false)
	close(filenameCh)

	for f := range filenameCh {
		m.repFilenameCh <- repFile{repetitions: m.numGetsEachFile, filename: f}
	}
	m.wg.Wait()

	// Step 4.
	m.wg.Add(m.num*m.numGetsEachFile + 2)
	go doReregisterTarget(&m)
	go func() {
		<-m.controlCh
		primarySetToOriginal(t)
		m.wg.Done()
	}()

	doGetsInParallel(&m)

	m.wg.Wait()
	tlogf("%d GET errors before target's local bucket map was updated (expected)\n", m.numGetErrsBefore)
	if m.numGetErrsAfter > maxNumGetErrs {
		t.Fatalf("Found %d GET errors (should be no more than %d/%d) after target's local bucket map was updated",
			m.numGetErrsAfter, maxNumGetErrs, num*m.numGetsEachFile)
	} else {
		tlogf("Found %d GET errors (should be no more than %d/%d) after target's local bucket map was updated",
			m.numGetErrsAfter, maxNumGetErrs, num*m.numGetsEachFile)
	}
}

func doGetsInParallel(m *metadata) {
	for i := 0; i < 10; i++ {
		m.semaphore <- struct{}{}
	}
	tlogf("Getting each of the %d files %d times from bucket %s...\n", m.num, m.numGetsEachFile, m.bucket)

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
			_, _, err := client.Get(proxyurl, m.bucket, SmokeStr+"/"+repFile.filename, nil, nil, false, false)
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
				tlogf(" %d/%d GET requests completed...\n", g, m.num*m.numGetsEachFile)
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

func doReregisterTarget(m *metadata) {
	defer m.wg.Done()
	err := client.RegisterTarget(m.sid, m.smap)
	checkFatal(err, m.t)
PollLoop:
	for i := 0; i < 25; i++ {
		time.Sleep(time.Second)
		m.smap = getClusterMap(m.t)

		if len(m.smap.Tmap) == m.origNumTargets {
			lbNames, err := client.GetLocalBucketNames(m.targetDirectURL)
			checkFatal(err, m.t)
			for _, b := range lbNames.Local {
				if b == m.bucket {
					break PollLoop
				}
			}
		}
	}

	if len(m.smap.Tmap) != m.origNumTargets {
		m.t.Fatalf("Re-registration timed out: target %s, num targets now: %d, orig num targets: %d\n",
			m.sid, len(m.smap.Tmap), m.origNumTargets)
	}
	tlogf("Re-registered target %s: the cluster is now back to %d targets\n", m.sid, m.origNumTargets)
	s := atomic.CompareAndSwapUint64(&m.reregistered, 0, 1)
	if !s {
		m.t.Errorf("reregistered should have swapped from 0 to 1. Actual reregistered = %d\n", m.reregistered)
	}
}

func checkFatal(err error, t *testing.T) {
	if err != nil {
		debug.PrintStack()
		t.Fatal(err)
	}
}
