// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	"math/rand"
	"net/url"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/readers"
	"github.com/NVIDIA/aistore/tutils/tassert"
	jsoniter "github.com/json-iterator/go"
)

const rebalanceObjectDistributionTestCoef = 0.3

const (
	baseDir                 = "/tmp/ais"
	ColdValidStr            = "coldmd5"
	ChksumValidStr          = "chksum"
	ColdMD5str              = "coldmd5"
	EvictCBStr              = "evictCB"
	ChecksumWarmValidateStr = "checksumWarmValidate"
	RangeGetStr             = "rangeGet"
	DeleteStr               = "delete"
	SmokeStr                = "smoke"
	largeFileSize           = 4 * cmn.MiB
	copyBucketTimeout       = 3 * time.Minute
	rebalanceTimeout        = 5 * time.Minute
	rebalanceStartTimeout   = 10 * time.Second
)

var (
	numops                 int
	numfiles               int
	numworkers             int
	match                  = ".*"
	pagesize               int64
	fnlen                  int
	prefix                 string
	abortonerr             = false
	prefetchRange          = "{0..200}"
	skipdel                bool
	baseseed               = int64(1062984096)
	multiProxyTestDuration time.Duration
	clichecksum            string
	cycles                 int

	clibucket string
)

// nolint:maligned // no performance critical code
type ioContext struct {
	t                   *testing.T
	smap                *cluster.Smap
	controlCh           chan struct{}
	stopCh              chan struct{}
	objNames            []string
	bck                 cmn.Bck
	fileSize            uint64
	fixedSize           bool
	numGetErrs          atomic.Uint64
	proxyURL            string
	otherTasksToTrigger int
	originalTargetCount int
	originalProxyCount  int
	num                 int
	numGetsEachFile     int
	getErrIsFatal       bool
	silent              bool

	// internal
	wg   *sync.WaitGroup
	sema *cmn.DynSemaphore
}

func (m *ioContext) saveClusterState() {
	m.init()
	m.smap = tutils.GetClusterMap(m.t, m.proxyURL)
	m.originalTargetCount = len(m.smap.Tmap)
	m.originalProxyCount = len(m.smap.Pmap)
	tutils.Logf("targets: %d, proxies: %d\n", m.originalTargetCount, m.originalProxyCount)
}

func (m *ioContext) init() {
	m.proxyURL = tutils.GetPrimaryURL()
	if m.fileSize == 0 {
		m.fileSize = cmn.KiB
	}
	if m.num > 0 {
		m.objNames = make([]string, 0, m.num)
	}
	if m.otherTasksToTrigger > 0 {
		m.controlCh = make(chan struct{}, m.otherTasksToTrigger)
	}
	m.wg = &sync.WaitGroup{}
	m.sema = cmn.NewDynSemaphore(numworkers)
	if m.bck.Name == "" {
		m.bck.Name = tutils.GenRandomString(15)
	}
	if m.bck.Provider == "" {
		m.bck.Provider = cmn.ProviderAIS
	}
	if m.numGetsEachFile == 0 {
		m.numGetsEachFile = 1
	}
	m.stopCh = make(chan struct{})
}

func (m *ioContext) assertClusterState() {
	smap, err := tutils.WaitForPrimaryProxy(
		m.proxyURL,
		"to check cluster state",
		m.smap.Version, testing.Verbose(),
		m.originalProxyCount,
		m.originalTargetCount,
	)
	tassert.CheckFatal(m.t, err)

	proxyCount := smap.CountProxies()
	targetCount := smap.CountTargets()
	if targetCount != m.originalTargetCount ||
		proxyCount != m.originalProxyCount {
		m.t.Errorf(
			"cluster state is not preserved. targets (before: %d, now: %d); proxies: (before: %d, now: %d)",
			targetCount, m.originalTargetCount,
			proxyCount, m.originalProxyCount,
		)
	}
}

func (m *ioContext) checkObjectDistribution(t *testing.T) {
	var (
		requiredCount     = int64(rebalanceObjectDistributionTestCoef * (float64(m.num) / float64(m.originalTargetCount)))
		targetObjectCount = make(map[string]int64)
	)
	tutils.Logf("Checking if each target has a required number of object in bucket %s...\n", m.bck)
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	bucketList, err := api.ListObjects(baseParams, m.bck, &cmn.SelectMsg{Props: cmn.GetTargetURL}, 0)
	tassert.CheckFatal(t, err)
	for _, obj := range bucketList.Entries {
		targetObjectCount[obj.TargetURL]++
	}
	if len(targetObjectCount) != m.originalTargetCount {
		t.Fatalf("Rebalance error, %d/%d targets received no objects from bucket %s\n",
			m.originalTargetCount-len(targetObjectCount), m.originalTargetCount, m.bck)
	}
	for targetURL, objCount := range targetObjectCount {
		if objCount < requiredCount {
			t.Fatalf("Rebalance error, target %s didn't receive required number of objects\n", targetURL)
		}
	}
}

func (m *ioContext) puts(dontFail ...bool) int {
	filenameCh := make(chan string, m.num)
	errCh := make(chan error, m.num)

	if !m.silent {
		tutils.Logf("PUT %d objects into bucket %s...\n", m.num, m.bck)
	}
	tutils.PutRandObjs(m.proxyURL, m.bck, SmokeStr, m.fileSize, m.num, errCh, filenameCh, m.fixedSize)
	if len(dontFail) == 0 {
		tassert.SelectErr(m.t, errCh, "put", false)
	}
	close(filenameCh)
	close(errCh)
	m.objNames = m.objNames[:0]
	for f := range filenameCh {
		m.objNames = append(m.objNames, path.Join(SmokeStr, f))
	}
	return len(errCh)
}

func (m *ioContext) cloudPuts(evict bool) {
	var (
		baseParams = tutils.DefaultBaseAPIParams(m.t)
		msg        = &cmn.SelectMsg{}
	)

	if !m.silent {
		tutils.Logf("cloud PUT %d objects into bucket %s...\n", m.num, m.bck)
	}

	objList, err := api.ListObjects(baseParams, m.bck, msg, 0)
	tassert.CheckFatal(m.t, err)

	leftToFill := m.num - len(objList.Entries)
	if leftToFill <= 0 {
		tutils.Logf("cloud PUT %d (%d) objects already in bucket %s...\n", m.num, len(objList.Entries), m.bck)
		m.num = len(objList.Entries)
		return
	}

	// Not enough objects in cloud bucket, need to create more.
	var (
		errCh = make(chan error, leftToFill)
		wg    = &sync.WaitGroup{}
	)
	m.objNames = m.objNames[:0]
	for i := 0; i < leftToFill; i++ {
		r, err := readers.NewRandReader(int64(m.fileSize), true /*withHash*/)
		tassert.CheckFatal(m.t, err)
		objName := fmt.Sprintf("%s%s%d", "copy/cloud_", cmn.RandString(4), i)
		wg.Add(1)
		go tutils.PutAsync(wg, m.proxyURL, m.bck, objName, r, errCh)
		m.objNames = append(m.objNames, objName)
	}
	wg.Wait()
	tassert.SelectErr(m.t, errCh, "put", true)
	tutils.Logln("cloud PUT done")

	objList, err = api.ListObjects(baseParams, m.bck, msg, 0)
	tassert.CheckFatal(m.t, err)
	if len(objList.Entries) != m.num {
		m.t.Fatalf("list_objects err: %d != %d", len(objList.Entries), m.num)
	}

	tutils.Logf("cloud bucket %s: %d cached objects\n", m.bck, m.num)

	if evict {
		tutils.Logln("evicting cloud bucket...")
		err := api.EvictCloudBucket(baseParams, m.bck)
		tassert.CheckFatal(m.t, err)
	}
}

func (m *ioContext) cloudPrefetch(prefetchCnt int) {
	var (
		baseParams = tutils.DefaultBaseAPIParams(m.t)
		msg        = &cmn.SelectMsg{}
	)

	objList, err := api.ListObjects(baseParams, m.bck, msg, 0)
	tassert.CheckFatal(m.t, err)

	tutils.Logf("cloud PREFETCH %d objects...\n", prefetchCnt)

	wg := &sync.WaitGroup{}
	for idx, obj := range objList.Entries {
		if idx >= prefetchCnt {
			break
		}

		wg.Add(1)
		go func(obj *cmn.BucketEntry) {
			_, err := api.GetObject(baseParams, m.bck, obj.Name)
			tassert.CheckError(m.t, err)
			wg.Done()
		}(obj)
	}
	wg.Wait()
}

func (m *ioContext) cloudDelete() {
	var (
		baseParams = tutils.DefaultBaseAPIParams(m.t)
		msg        = &cmn.SelectMsg{}
		sema       = make(chan struct{}, 40)
	)

	cmn.Assert(m.bck.Provider == cmn.AnyCloud)
	objList, err := api.ListObjects(baseParams, m.bck, msg, 0)
	tassert.CheckFatal(m.t, err)

	tutils.Logln("deleting cloud objects...")

	wg := &sync.WaitGroup{}
	for _, obj := range objList.Entries {
		wg.Add(1)
		go func(obj *cmn.BucketEntry) {
			sema <- struct{}{}
			defer func() {
				<-sema
			}()
			err := api.DeleteObject(baseParams, m.bck, obj.Name)
			tassert.CheckError(m.t, err)
			wg.Done()
		}(obj)
	}
	wg.Wait()
}

func (m *ioContext) get(baseParams api.BaseParams, idx, totalGets int) {
	m.sema.Acquire()
	defer func() {
		m.sema.Release()
		m.wg.Done()
	}()

	objName := m.objNames[idx%len(m.objNames)]
	_, err := api.GetObject(baseParams, m.bck, objName)
	if err != nil {
		if m.getErrIsFatal {
			m.t.Error(err)
		}
		m.numGetErrs.Inc()
	}
	if m.getErrIsFatal && m.numGetErrs.Load() > 0 {
		return
	}
	if idx > 0 && idx%5000 == 0 && !m.silent {
		if totalGets > 0 {
			tutils.Logf(" %d/%d GET requests completed...\n", idx, totalGets)
		} else {
			tutils.Logf(" %d GET requests completed...\n", idx)
		}
	}

	// Tell other tasks they can begin to do work in parallel
	if totalGets > 0 && idx == totalGets/2 { // only for `m.gets()`
		for i := 0; i < m.otherTasksToTrigger; i++ {
			m.controlCh <- struct{}{}
		}
	}
}

func (m *ioContext) gets() {
	var (
		baseParams = tutils.DefaultBaseAPIParams(m.t)
		totalGets  = m.num * m.numGetsEachFile
	)

	if !m.silent {
		if m.numGetsEachFile == 1 {
			tutils.Logf("GET each of the %d objects from bucket %s...\n", m.num, m.bck)
		} else {
			tutils.Logf("GET each of the %d objects %d times from bucket %s...\n", m.num, m.numGetsEachFile, m.bck)
		}
	}

	m.wg.Add(totalGets)
	for i := 0; i < totalGets; i++ {
		go m.get(baseParams, i, totalGets)
	}
	m.wg.Wait()
}

func (m *ioContext) getsUntilStop() {
	var (
		idx        = 0
		baseParams = tutils.DefaultBaseAPIParams(m.t)
	)
	for {
		select {
		case <-m.stopCh:
			m.wg.Wait()
			return
		default:
			m.wg.Add(1)
			go m.get(baseParams, idx, 0)
			idx++
			if idx%5000 == 0 {
				time.Sleep(500 * time.Millisecond) // prevents generating too many GET requests
			}
		}
	}
}

func (m *ioContext) stopGets() {
	m.stopCh <- struct{}{}
}

func (m *ioContext) ensureNumCopies(expectedCopies int) {
	var (
		total      int
		baseParams = tutils.DefaultBaseAPIParams(m.t)
	)

	time.Sleep(3 * time.Second)
	xactArgs := api.XactReqArgs{Kind: cmn.ActMakeNCopies, Bck: m.bck, Timeout: rebalanceTimeout}
	err := api.WaitForXaction(baseParams, xactArgs)
	tassert.CheckFatal(m.t, err)

	// List Bucket - primarily for the copies
	query := make(url.Values)
	msg := &cmn.SelectMsg{Cached: true}
	msg.AddProps(cmn.GetPropsCopies, cmn.GetPropsAtime, cmn.GetPropsStatus)
	objectList, err := api.ListObjects(baseParams, m.bck, msg, 0, query)
	tassert.CheckFatal(m.t, err)

	copiesToNumObjects := make(map[int]int)
	for _, entry := range objectList.Entries {
		if entry.Atime == "" {
			m.t.Errorf("%s/%s: access time is empty", m.bck, entry.Name)
		}
		total++
		copiesToNumObjects[int(entry.Copies)]++
	}
	tutils.Logf("objects (total, copies) = (%d, %v)\n", total, copiesToNumObjects)
	if total != m.num {
		m.t.Fatalf("list_objects: expecting %d objects, got %d", m.num, total)
	}

	if len(copiesToNumObjects) != 1 {
		s, _ := jsoniter.MarshalIndent(copiesToNumObjects, "", " ")
		m.t.Fatalf("some objects do not have expected number of copies: %s", s)
	}

	for copies := range copiesToNumObjects {
		if copies != expectedCopies {
			m.t.Fatalf("Expecting %d objects all to have %d replicas, got: %d", total, expectedCopies, copies)
		}
	}
}

func (m *ioContext) ensureNoErrors() {
	if m.numGetErrs.Load() > 0 {
		m.t.Fatalf("Number of get errors is non-zero: %d\n", m.numGetErrs.Load())
	}
}

func (m *ioContext) reregisterTarget(target *cluster.Snode) {
	const (
		timeout    = time.Second * 10
		interval   = time.Millisecond * 10
		iterations = int(timeout / interval)
	)

	// T1
	tutils.Logf("Registering target %s...\n", target.ID())
	smap := tutils.GetClusterMap(m.t, m.proxyURL)
	err := tutils.RegisterNode(m.proxyURL, target, smap)
	tassert.CheckFatal(m.t, err)
	baseParams := tutils.BaseAPIParams(target.URL(cmn.NetworkPublic))
	for i := 0; i < iterations; i++ {
		time.Sleep(interval)
		if _, ok := smap.Tmap[target.ID()]; !ok {
			// T2
			smap = tutils.GetClusterMap(m.t, m.proxyURL)
			if _, ok := smap.Tmap[target.ID()]; ok {
				tutils.Logf("T2: registered target %s\n", target.ID())
			}
		} else {
			query := cmn.QueryBcks(m.bck)
			baseParams.URL = m.proxyURL
			proxyBcks, err := api.ListBuckets(baseParams, query)
			tassert.CheckFatal(m.t, err)

			baseParams.URL = target.URL(cmn.NetworkPublic)
			targetBcks, err := api.ListBuckets(baseParams, query)
			tassert.CheckFatal(m.t, err)
			// T3
			if proxyBcks.Equal(targetBcks) {
				tutils.Logf("T3: registered target %s got updated with the new BMD\n", target.ID())
				return
			}
		}
	}

	m.t.Fatalf("failed to register target %s: not in the Smap or did not receive BMD", target.ID())
}

func (m *ioContext) setRandBucketProps() {
	baseParams := tutils.DefaultBaseAPIParams(m.t)

	// Set some weird bucket props to see if they were changed or not.
	props := cmn.BucketPropsToUpdate{
		LRU: &cmn.LRUConfToUpdate{
			LowWM:  api.Int64(int64(rand.Intn(35) + 1)),
			HighWM: api.Int64(int64(rand.Intn(15) + 40)),
			OOS:    api.Int64(int64(rand.Intn(30) + 60)),
		},
	}
	err := api.SetBucketProps(baseParams, m.bck, props)
	tassert.CheckFatal(m.t, err)
}

func runProviderTests(t *testing.T, f func(*testing.T, cmn.Bck)) {
	tests := []struct {
		name     string
		bck      cmn.Bck
		skipArgs tutils.SkipTestArgs
	}{
		{
			name: "local",
			bck:  cmn.Bck{Name: cmn.RandString(10), Provider: cmn.ProviderAIS},
		},
		{
			name: "cloud",
			bck:  cmn.Bck{Name: clibucket, Provider: cmn.AnyCloud},
			skipArgs: tutils.SkipTestArgs{
				Long:  true,
				Cloud: true,
			},
		},
		{
			name: "remote",
			bck:  cmn.Bck{Name: cmn.RandString(10), Provider: cmn.ProviderAIS, Ns: cmn.Ns{UUID: tutils.RemoteCluster.UUID}},
			skipArgs: tutils.SkipTestArgs{
				RequiresRemote: true,
			},
		},
	}
	for _, test := range tests { // nolint:gocritic // no performance critical code
		t.Run(test.name, func(t *testing.T) {
			test.skipArgs.Bck = test.bck
			tutils.CheckSkip(t, test.skipArgs)

			if test.bck.IsAIS() || test.bck.IsRemoteAIS() {
				baseParams := tutils.DefaultBaseAPIParams(t)
				err := api.CreateBucket(baseParams, test.bck)
				tassert.CheckFatal(t, err)
				defer func() {
					api.DestroyBucket(baseParams, test.bck)
				}()
			}

			f(t, test.bck)
		})
	}
}
