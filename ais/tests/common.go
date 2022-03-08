// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/readers"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tlog"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/fs"
	jsoniter "github.com/json-iterator/go"
)

const rebalanceObjectDistributionTestCoef = 0.3

const (
	prefixDir             = "filter"
	largeFileSize         = 4 * cos.MiB
	copyBucketTimeout     = 3 * time.Minute
	rebalanceTimeout      = 5 * time.Minute
	rebalanceStartTimeout = 10 * time.Second
	multiProxyTestTimeout = 3 * time.Minute

	workerCnt = 10
)

const testMpath = "/tmp/ais/mountpath"

var (
	cliBck         cmn.Bck
	errObjectFound = errors.New("found") // to interrupt fs.Walk when object found
	fsOnce         sync.Once
)

type ioContext struct {
	t                   *testing.T
	smap                *cluster.Smap
	controlCh           chan struct{}
	stopCh              chan struct{}
	objNames            []string
	bck                 cmn.Bck
	fileSize            uint64
	proxyURL            string
	prefix              string
	otherTasksToTrigger int
	originalTargetCount int
	originalProxyCount  int
	num                 int
	numGetsEachFile     int
	getErrIsFatal       bool
	silent              bool
	fixedSize           bool
	ordered             bool // true - object names make sequence, false - names are random

	numGetErrs atomic.Uint64
	numPutErrs int
}

func (m *ioContext) initWithCleanupAndSaveState() {
	m.initWithCleanup()
	m.saveCluState(m.proxyURL)
}

func (m *ioContext) saveCluState(proxyURL string) {
	m.smap = tutils.GetClusterMap(m.t, proxyURL)
	m.originalTargetCount = m.smap.CountActiveTargets()
	m.originalProxyCount = m.smap.CountActiveProxies()
	tlog.Logf("targets: %d, proxies: %d\n", m.originalTargetCount, m.originalProxyCount)
}

func (m *ioContext) waitAndCheckCluState() {
	time.Sleep(2 * time.Second)
	smap, err := tutils.WaitForClusterState(
		m.proxyURL,
		"wait for cluster state",
		m.smap.Version,
		m.originalProxyCount,
		m.originalTargetCount,
	)
	tassert.CheckFatal(m.t, err)
	m.checkCluState(smap)
}

func (m *ioContext) checkCluState(smap *cluster.Smap) {
	proxyCount := smap.CountActiveProxies()
	targetCount := smap.CountActiveTargets()
	if targetCount != m.originalTargetCount ||
		proxyCount != m.originalProxyCount {
		m.t.Errorf(
			"cluster state is not preserved. targets (before: %d, now: %d); proxies: (before: %d, now: %d)",
			targetCount, m.originalTargetCount,
			proxyCount, m.originalProxyCount,
		)
	}
}

func (m *ioContext) initWithCleanup() {
	m.proxyURL = tutils.RandomProxyURL()
	if m.proxyURL == "" {
		// if random selection failed, use RO url
		m.proxyURL = tutils.GetPrimaryURL()
	}
	if m.fileSize == 0 {
		m.fileSize = cos.KiB
	}
	if m.num > 0 {
		m.objNames = make([]string, 0, m.num)
	}
	if m.otherTasksToTrigger > 0 {
		m.controlCh = make(chan struct{}, m.otherTasksToTrigger)
	}
	if m.bck.Name == "" {
		m.bck.Name = cos.RandString(15)
	}
	if m.bck.Provider == "" {
		m.bck.Provider = apc.ProviderAIS
	}
	if m.numGetsEachFile == 0 {
		m.numGetsEachFile = 1
	}
	m.stopCh = make(chan struct{})

	if m.bck.IsRemote() {
		// Remove unnecessary local objects.
		tutils.EvictRemoteBucket(m.t, m.proxyURL, m.bck)
	}

	// cleanup m.bck upon exit from the test
	m.t.Cleanup(m._cleanup)
}

func (m *ioContext) _cleanup() {
	m.del()
	if m.bck.IsRemote() {
		// Ensure all local objects are removed.
		tutils.EvictRemoteBucket(m.t, m.proxyURL, m.bck)
	}
}

func (m *ioContext) expectTargets(n int) {
	if m.originalTargetCount < n {
		m.t.Skipf("Must have %d or more targets in the cluster, have only %d", n, m.originalTargetCount)
	}
}

func (m *ioContext) expectProxies(n int) {
	if m.originalProxyCount < n {
		m.t.Skipf("Must have %d or more proxies in the cluster, have only %d", n, m.originalProxyCount)
	}
}

func (m *ioContext) checkObjectDistribution(t *testing.T) {
	var (
		requiredCount     = int64(rebalanceObjectDistributionTestCoef * (float64(m.num) / float64(m.originalTargetCount)))
		targetObjectCount = make(map[string]int64)
	)
	tlog.Logf("Checking if each target has a required number of object in bucket %s...\n", m.bck)
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	bucketList, err := api.ListObjects(baseParams, m.bck, &apc.ListObjsMsg{Props: apc.GetTargetURL}, 0)
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

func (m *ioContext) puts(ignoreErrs ...bool) {
	if !m.bck.IsAIS() {
		m.remotePuts(false /*evict*/)
		return
	}
	baseParams := tutils.BaseAPIParams(m.proxyURL)
	p, err := api.HeadBucket(baseParams, m.bck)
	tassert.CheckFatal(m.t, err)

	var ignoreErr bool
	if len(ignoreErrs) > 0 {
		ignoreErr = ignoreErrs[0]
	}
	if !m.silent {
		tlog.Logf("PUT %d objects => %s\n", m.num, m.bck)
	}
	m.objNames, m.numPutErrs, err = tutils.PutRandObjs(tutils.PutObjectsArgs{
		ProxyURL:  m.proxyURL,
		Bck:       m.bck,
		ObjPath:   m.prefix,
		ObjCnt:    m.num,
		ObjSize:   m.fileSize,
		FixedSize: m.fixedSize,
		CksumType: p.Cksum.Type,
		WorkerCnt: 0, // TODO: Should we set something custom?
		IgnoreErr: ignoreErr,
		Ordered:   m.ordered,
	})
	tassert.CheckFatal(m.t, err)
}

// remotePuts by default empties remote bucket and puts new `m.num` objects
// into the bucket. If `override` parameter is set then the existing objects
// are updated with new ones (new version and checksum).
func (m *ioContext) remotePuts(evict bool, overrides ...bool) {
	var override bool
	if len(overrides) > 0 {
		override = overrides[0]
	}

	if !override {
		// Cleanup the remote bucket.
		m.del()
		m.objNames = m.objNames[:0]
	}

	m._remoteFill(m.num, evict, override)
}

// remoteRefill calculates number of missing objects and refills the bucket.
// It is expected that the number of missing objects is positive meaning that
// some of the objects were removed before calling remoteRefill.
func (m *ioContext) remoteRefill() {
	var (
		baseParams = tutils.BaseAPIParams()
		msg        = &apc.ListObjsMsg{Prefix: m.prefix, Props: apc.GetPropsName}
	)

	objList, err := api.ListObjects(baseParams, m.bck, msg, 0)
	tassert.CheckFatal(m.t, err)

	m.objNames = m.objNames[:0]
	for _, obj := range objList.Entries {
		m.objNames = append(m.objNames, obj.Name)
	}

	leftToFill := m.num - len(objList.Entries)
	cos.Assert(leftToFill > 0)

	m._remoteFill(leftToFill, false /*evict*/, false /*override*/)
}

func (m *ioContext) _remoteFill(objCnt int, evict, override bool) {
	var (
		baseParams = tutils.BaseAPIParams()
		errCh      = make(chan error, objCnt)
		wg         = cos.NewLimitedWaitGroup(20)
	)
	if !m.silent {
		tlog.Logf("remote PUT %d objects => %s\n", objCnt, m.bck)
	}
	p, err := api.HeadBucket(baseParams, m.bck)
	tassert.CheckFatal(m.t, err)

	for i := 0; i < objCnt; i++ {
		r, err := readers.NewRandReader(int64(m.fileSize), p.Cksum.Type)
		tassert.CheckFatal(m.t, err)

		var objName string
		if override {
			objName = m.objNames[i]
		} else if m.ordered {
			objName = fmt.Sprintf("%s%d", m.prefix, i)
		} else {
			objName = fmt.Sprintf("%s%s-%d", m.prefix, cos.RandString(8), i)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			tutils.Put(m.proxyURL, m.bck, objName, r, errCh)
		}()
		if !override {
			m.objNames = append(m.objNames, objName)
		}
	}
	wg.Wait()
	tassert.SelectErr(m.t, errCh, "put", true)
	tlog.Logf("remote bucket %s: %d cached objects\n", m.bck, m.num)

	if evict {
		m.evict()
	}
}

func (m *ioContext) evict() {
	var (
		baseParams = tutils.BaseAPIParams()
		msg        = &apc.ListObjsMsg{Prefix: m.prefix, Props: apc.GetPropsName}
	)

	objList, err := api.ListObjects(baseParams, m.bck, msg, 0)
	tassert.CheckFatal(m.t, err)
	if len(objList.Entries) != m.num {
		m.t.Fatalf("list_objects err: %d != %d", len(objList.Entries), m.num)
	}

	tlog.Logf("evicting remote bucket %s...\n", m.bck)
	err = api.EvictRemoteBucket(baseParams, m.bck, false)
	tassert.CheckFatal(m.t, err)
}

func (m *ioContext) remotePrefetch(prefetchCnt int) {
	var (
		baseParams = tutils.BaseAPIParams()
		msg        = &apc.ListObjsMsg{Prefix: m.prefix, Props: apc.GetPropsName}
	)

	objList, err := api.ListObjects(baseParams, m.bck, msg, 0)
	tassert.CheckFatal(m.t, err)

	tlog.Logf("remote PREFETCH %d objects...\n", prefetchCnt)

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

// NOTE: is also called via deferred t.Cleanup => _cleanup() => del()
func (m *ioContext) del(cnt ...int) {
	const maxErrCount = 100
	var (
		httpErr    *cmn.ErrHTTP
		baseParams = tutils.BaseAPIParams()
		lsmsg      = &apc.ListObjsMsg{Prefix: m.prefix, Props: apc.GetPropsName}
	)

	exists, err := api.DoesBucketExist(baseParams, cmn.QueryBcks(m.bck))
	tassert.CheckFatal(m.t, err)
	if !exists {
		return
	}
	// TODO -- FIXME: set dont-lookup-remote-bucket = true (the last arg)
	objList, err := api.ListObjectsWithOpts(baseParams, m.bck, lsmsg, 0, nil, false)
	if err != nil {
		if errors.As(err, &httpErr) && httpErr.Status == http.StatusNotFound {
			return
		}
		emsg := err.Error()
		// ignore client timeout awaiting headers
		if strings.Contains(emsg, "awaiting") && strings.Contains(emsg, "headers") {
			return
		}
	}
	tassert.CheckFatal(m.t, err)

	toRemove := objList.Entries
	if len(cnt) > 0 {
		toRemove = toRemove[:cnt[0]]
	}

	if len(toRemove) == 0 {
		return
	}

	tlog.Logf("deleting %d objects...\n", len(toRemove))
	var (
		errCnt atomic.Int64
		wg     = cos.NewLimitedWaitGroup(16)
	)
	for _, obj := range toRemove {
		if errCnt.Load() > maxErrCount {
			tassert.CheckFatal(m.t, errors.New("too many errors"))
			break
		}
		wg.Add(1)
		go func(obj *cmn.BucketEntry) {
			defer wg.Done()
			err := api.DeleteObject(baseParams, m.bck, obj.Name)
			if err != nil {
				if cmn.IsErrObjNought(err) {
					err = nil
				} else if strings.Contains(err.Error(), "server closed idle connection") {
					// see (unexported) http.exportErrServerClosedIdle in the Go source
					err = nil
				} else if cos.IsErrConnectionNotAvail(err) {
					errCnt.Add(maxErrCount / 10)
				} else {
					errCnt.Inc()
				}
			}
			tassert.CheckError(m.t, err)
		}(obj)
	}
	wg.Wait()
}

func (m *ioContext) get(baseParams api.BaseParams, idx, totalGets int, validate bool) {
	var (
		err     error
		objName = m.objNames[idx%len(m.objNames)]
	)
	if validate {
		_, err = api.GetObjectWithValidation(baseParams, m.bck, objName)
	} else {
		_, err = api.GetObject(baseParams, m.bck, objName)
	}
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
			tlog.Logf(" %d/%d GET requests completed...\n", idx, totalGets)
		} else {
			tlog.Logf(" %d GET requests completed...\n", idx)
		}
	}

	// Tell other tasks they can begin to do work in parallel
	if totalGets > 0 && idx == totalGets/2 { // only for `m.gets()`
		for i := 0; i < m.otherTasksToTrigger; i++ {
			m.controlCh <- struct{}{}
		}
	}
}

func (m *ioContext) gets(withValidation ...bool) {
	var (
		baseParams = tutils.BaseAPIParams()
		totalGets  = m.num * m.numGetsEachFile
		wg         = cos.NewLimitedWaitGroup(50)
		validate   bool
	)

	if len(withValidation) > 0 {
		validate = withValidation[0]
	}

	if !m.silent {
		if m.numGetsEachFile == 1 {
			tlog.Logf("GET %d objects from %s\n", m.num, m.bck)
		} else {
			tlog.Logf("GET %d objects %d times from %s\n", m.num, m.numGetsEachFile, m.bck)
		}
	}

	for i := 0; i < totalGets; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			m.get(baseParams, idx, totalGets, validate)
		}(i)
	}
	wg.Wait()
}

func (m *ioContext) getsUntilStop() {
	var (
		idx        = 0
		baseParams = tutils.BaseAPIParams()
		wg         = cos.NewLimitedWaitGroup(40)
	)
	for {
		select {
		case <-m.stopCh:
			wg.Wait()
			return
		default:
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				m.get(baseParams, idx, 0, false /*validate*/)
			}(idx)
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

func (m *ioContext) ensureNumCopies(expectedCopies int, greaterOk bool) {
	m.t.Helper()
	var (
		baseParams = tutils.BaseAPIParams()
		total      int
	)
	time.Sleep(time.Second)
	xactArgs := api.XactReqArgs{Kind: apc.ActMakeNCopies, Bck: m.bck, Timeout: rebalanceTimeout}
	_, err := api.WaitForXactionIC(baseParams, xactArgs)
	tassert.CheckFatal(m.t, err)

	// List Bucket - primarily for the copies
	msg := &apc.ListObjsMsg{Flags: apc.LsPresent, Prefix: m.prefix}
	msg.AddProps(apc.GetPropsCopies, apc.GetPropsAtime, apc.GetPropsStatus)
	objectList, err := api.ListObjects(baseParams, m.bck, msg, 0)
	tassert.CheckFatal(m.t, err)

	copiesToNumObjects := make(map[int]int)
	for _, entry := range objectList.Entries {
		if entry.Atime == "" {
			m.t.Errorf("%s/%s: access time is empty", m.bck, entry.Name)
		}
		total++
		if greaterOk && int(entry.Copies) > expectedCopies {
			copiesToNumObjects[expectedCopies]++
		} else {
			copiesToNumObjects[int(entry.Copies)]++
		}
	}
	tlog.Logf("objects (total, copies) = (%d, %v)\n", total, copiesToNumObjects)
	if total != m.num {
		m.t.Errorf("list_objects: expecting %d objects, got %d", m.num, total)
	}

	if len(copiesToNumObjects) != 1 {
		s, _ := jsoniter.MarshalIndent(copiesToNumObjects, "", " ")
		m.t.Errorf("some objects do not have expected number of copies: %s", s)
	}

	for copies := range copiesToNumObjects {
		if copies != expectedCopies {
			m.t.Errorf("Expecting %d objects all to have %d replicas, got: %d", total, expectedCopies, copies)
		}
	}
}

func (m *ioContext) ensureNoGetErrors() {
	m.t.Helper()
	if m.numGetErrs.Load() > 0 {
		m.t.Fatalf("Number of get errors is non-zero: %d\n", m.numGetErrs.Load())
	}
}

func (m *ioContext) ensureNumMountpaths(target *cluster.Snode, mpList *apc.MountpathList) {
	ensureNumMountpaths(m.t, target, mpList)
}

func ensureNumMountpaths(t *testing.T, target *cluster.Snode, mpList *apc.MountpathList) {
	t.Helper()
	tname := target.StringEx()
	baseParams := tutils.BaseAPIParams()
	mpl, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)
	for i := 0; i < 6; i++ {
		if len(mpl.Available) == len(mpList.Available) &&
			len(mpl.Disabled) == len(mpList.Disabled) &&
			len(mpl.WaitingDD) == len(mpList.WaitingDD) {
			break
		}
		time.Sleep(time.Second)
	}
	if len(mpl.Available) != len(mpList.Available) {
		t.Errorf("%s ended up with %d mountpaths (dd=%v, disabled=%v), expecting: %d",
			tname, len(mpl.Available), mpl.WaitingDD, mpl.Disabled, len(mpList.Available))
	} else if len(mpl.Disabled) != len(mpList.Disabled) || len(mpl.WaitingDD) != len(mpList.WaitingDD) {
		t.Errorf("%s ended up with (dd=%v, disabled=%v) mountpaths, expecting (%v and %v), respectively",
			tname, mpl.WaitingDD, mpl.Disabled, mpList.WaitingDD, mpList.Disabled)
	}
}

func ensureNoDisabledMountpaths(t *testing.T, target *cluster.Snode, mpList *apc.MountpathList) {
	t.Helper()
	for i := 0; i < 6; i++ {
		if len(mpList.WaitingDD) == 0 && len(mpList.Disabled) == 0 {
			break
		}
		time.Sleep(time.Second)
	}
	if len(mpList.WaitingDD) != 0 || len(mpList.Disabled) != 0 {
		t.Fatalf("%s: disabled mountpaths at the start of the %q (avail=%d, dd=%v, disabled=%v)\n",
			target.StringEx(), t.Name(), len(mpList.Available), mpList.WaitingDD, mpList.Disabled)
	}
}

func (m *ioContext) startMaintenanceNoRebalance() *cluster.Snode {
	target, _ := m.smap.GetRandTarget()
	tlog.Logf("Put %s in maintenance\n", target.StringEx())
	args := &apc.ActValRmNode{DaemonID: target.ID(), SkipRebalance: true}
	_, err := api.StartMaintenance(tutils.BaseAPIParams(m.proxyURL), args)
	tassert.CheckFatal(m.t, err)
	m.smap, err = tutils.WaitForClusterState(
		m.proxyURL,
		"put target in maintenance",
		m.smap.Version,
		m.smap.CountActiveProxies(),
		m.smap.CountActiveTargets()-1,
	)
	tassert.CheckFatal(m.t, err)
	return target
}

func (m *ioContext) stopMaintenance(target *cluster.Snode) (rebID string) {
	const (
		timeout    = time.Second * 10
		interval   = time.Millisecond * 10
		iterations = int(timeout / interval)
	)
	var err error
	tlog.Logf("Take %s out of maintenance...\n", target.StringEx())
	args := &apc.ActValRmNode{DaemonID: target.ID()}
	rebID, err = api.StopMaintenance(tutils.BaseAPIParams(m.proxyURL), args)
	tassert.CheckFatal(m.t, err)
	baseParams := tutils.BaseAPIParams(target.URL(cmn.NetPublic))
	smap := tutils.GetClusterMap(m.t, m.proxyURL)
	for i := 0; i < iterations; i++ {
		time.Sleep(interval)
		if _, ok := smap.Tmap[target.ID()]; !ok {
			smap = tutils.GetClusterMap(m.t, m.proxyURL)
		} else {
			query := cmn.QueryBcks(m.bck)
			baseParams.URL = m.proxyURL
			proxyBcks, err := api.ListBuckets(baseParams, query)
			tassert.CheckFatal(m.t, err)

			baseParams.URL = target.URL(cmn.NetPublic)
			targetBcks, err := api.ListBuckets(baseParams, query)
			tassert.CheckFatal(m.t, err)
			if proxyBcks.Equal(targetBcks) {
				tlog.Logf("%s got updated with the current BMD\n", target.StringEx())
				return
			}
		}
	}
	m.t.Fatalf("failed to bring %s out of maintenance: not in the %s and/or did not get updated BMD",
		target.StringEx(), smap.StringEx())
	return
}

func (m *ioContext) setRandBucketProps() {
	baseParams := tutils.BaseAPIParams()

	// Set some weird bucket props to see if they were changed or not.
	props := &cmn.BucketPropsToUpdate{
		LRU: &cmn.LRUConfToUpdate{
			LowWM:  api.Int64(int64(rand.Intn(35) + 1)),
			HighWM: api.Int64(int64(rand.Intn(15) + 40)),
			OOS:    api.Int64(int64(rand.Intn(30) + 60)),
		},
	}
	_, err := api.SetBucketProps(baseParams, m.bck, props)
	tassert.CheckFatal(m.t, err)
}

func runProviderTests(t *testing.T, f func(*testing.T, *cluster.Bck)) {
	tests := []struct {
		name       string
		bck        cmn.Bck
		backendBck cmn.Bck
		skipArgs   tutils.SkipTestArgs
		props      *cmn.BucketPropsToUpdate
	}{
		{
			name: "local",
			bck:  cmn.Bck{Name: cos.RandString(10), Provider: apc.ProviderAIS},
		},
		{
			name: "remote",
			bck:  cliBck,
			skipArgs: tutils.SkipTestArgs{
				Long:      true,
				RemoteBck: true,
			},
		},
		{
			name: "remote_ais",
			bck: cmn.Bck{
				Name:     cos.RandString(10),
				Provider: apc.ProviderAIS, Ns: cmn.Ns{UUID: tutils.RemoteCluster.UUID},
			},
			skipArgs: tutils.SkipTestArgs{
				RequiresRemoteCluster: true,
				Long:                  true,
			},
		},
		{
			name:       "backend",
			bck:        cmn.Bck{Name: cos.RandString(10), Provider: apc.ProviderAIS},
			backendBck: cliBck,
			skipArgs: tutils.SkipTestArgs{
				Long:      true,
				RemoteBck: true,
			},
		},
		{
			name: "local_3_copies",
			bck:  cmn.Bck{Name: cos.RandString(10), Provider: apc.ProviderAIS},
			props: &cmn.BucketPropsToUpdate{
				Mirror: &cmn.MirrorConfToUpdate{
					Enabled: api.Bool(true),
					Copies:  api.Int64(3),
				},
			},
			skipArgs: tutils.SkipTestArgs{Long: true},
		},
		{
			name: "local_ec_2_2",
			bck:  cmn.Bck{Name: cos.RandString(10), Provider: apc.ProviderAIS},
			props: &cmn.BucketPropsToUpdate{
				EC: &cmn.ECConfToUpdate{
					DataSlices:   api.Int(2),
					ParitySlices: api.Int(2),
					ObjSizeLimit: api.Int64(0),
				},
			},
			skipArgs: tutils.SkipTestArgs{Long: true},
		},
	}
	for _, test := range tests { // nolint:gocritic // no performance critical code
		t.Run(test.name, func(t *testing.T) {
			if test.backendBck.IsEmpty() {
				test.skipArgs.Bck = test.bck
			} else {
				test.skipArgs.Bck = test.backendBck
				if !test.backendBck.IsCloud() {
					t.Skipf("backend bucket must be a Cloud bucket (have %q)", test.backendBck)
				}
			}
			tutils.CheckSkip(t, test.skipArgs)

			baseParams := tutils.BaseAPIParams()

			if test.props != nil && test.props.Mirror != nil {
				skip := tutils.SkipTestArgs{
					MinMountpaths: int(*test.props.Mirror.Copies),
				}
				tutils.CheckSkip(t, skip)
			}
			if test.props != nil && test.props.EC != nil {
				skip := tutils.SkipTestArgs{
					MinTargets: *test.props.EC.DataSlices + *test.props.EC.ParitySlices + 1,
				}
				tutils.CheckSkip(t, skip)
			}

			if test.bck.IsAIS() || test.bck.IsRemoteAIS() {
				err := api.CreateBucket(baseParams, test.bck, test.props)
				tassert.CheckFatal(t, err)

				if !test.backendBck.IsEmpty() {
					tutils.SetBackendBck(t, baseParams, test.bck, test.backendBck)
				}
				defer api.DestroyBucket(baseParams, test.bck)
			}

			p, err := api.HeadBucket(baseParams, test.bck)
			tassert.CheckFatal(t, err)

			bck := cluster.CloneBck(&test.bck)
			bck.Props = p

			f(t, bck)
		})
	}
}

func numberOfFilesWithPrefix(fileNames []string, namePrefix string) int {
	numFiles := 0
	for _, fileName := range fileNames {
		if strings.HasPrefix(fileName, namePrefix) {
			numFiles++
		}
	}
	return numFiles
}

func prefixCreateFiles(t *testing.T, proxyURL string, bck cmn.Bck, cksumType string) []string {
	const (
		objCnt   = 100
		fileSize = cos.KiB
	)

	// Create specific files to test corner cases.
	var (
		extraNames = []string{"dir/obj01", "dir/obj02", "dir/obj03", "dir1/dir2/obj04", "dir1/dir2/obj05"}
		fileNames  = make([]string, 0, objCnt)
		wg         = &sync.WaitGroup{}
		errCh      = make(chan error, objCnt+len(extraNames))
	)

	for i := 0; i < objCnt; i++ {
		fileName := cos.RandString(20)
		keyName := fmt.Sprintf("%s/%s", prefixDir, fileName)

		// NOTE: Since this test is to test prefix fetch, the reader type is ignored, always use rand reader.
		r, err := readers.NewRandReader(fileSize, cksumType)
		if err != nil {
			t.Fatal(err)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			tutils.Put(proxyURL, bck, keyName, r, errCh)
		}()
		fileNames = append(fileNames, fileName)
	}

	for _, fName := range extraNames {
		keyName := fmt.Sprintf("%s/%s", prefixDir, fName)
		// NOTE: Since this test is to test prefix fetch, the reader type is ignored, always use rand reader.
		r, err := readers.NewRandReader(fileSize, cksumType)
		if err != nil {
			t.Fatal(err)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			tutils.Put(proxyURL, bck, keyName, r, errCh)
		}()
		fileNames = append(fileNames, fName)
	}

	wg.Wait()
	tassert.SelectErr(t, errCh, "put", false)
	return fileNames
}

func prefixLookupDefault(t *testing.T, proxyURL string, bck cmn.Bck, fileNames []string) {
	tlog.Logf("Looking up for files in alphabetic order\n")

	var (
		letters    = "abcdefghijklmnopqrstuvwxyz"
		baseParams = tutils.BaseAPIParams(proxyURL)
	)
	for i := 0; i < len(letters); i++ {
		key := letters[i : i+1]
		lookFor := fmt.Sprintf("%s/%s", prefixDir, key)
		msg := &apc.ListObjsMsg{Prefix: lookFor}
		objList, err := api.ListObjects(baseParams, bck, msg, 0)
		if err != nil {
			t.Errorf("List files with prefix failed, err = %v", err)
			return
		}

		numFiles := len(objList.Entries)
		realNumFiles := numberOfFilesWithPrefix(fileNames, key)

		if numFiles == realNumFiles {
			if numFiles != 0 {
				tlog.Logf("Found %v files starting with %q\n", numFiles, key)
			}
		} else {
			t.Errorf("Expected number of files with prefix %q is %v but found %v files", key, realNumFiles, numFiles)
			tlog.Logf("Objects returned:\n")
			for id, oo := range objList.Entries {
				tlog.Logf("    %d[%d]. %s\n", i, id, oo.Name)
			}
		}
	}
}

func prefixLookupCornerCases(t *testing.T, proxyURL string, bck cmn.Bck, objNames []string) {
	tlog.Logf("Testing corner cases\n")

	tests := []struct {
		title  string
		prefix string
	}{
		{"Entire list (dir)", "dir"},
		{"dir/", "dir/"},
		{"dir1", "dir1"},
		{"dir1/", "dir1/"},
	}
	baseParams := tutils.BaseAPIParams(proxyURL)
	for idx, test := range tests {
		p := fmt.Sprintf("%s/%s", prefixDir, test.prefix)

		objCount := 0
		for _, objName := range objNames {
			fullObjName := fmt.Sprintf("%s/%s", prefixDir, objName)
			if strings.HasPrefix(fullObjName, p) {
				objCount++
			}
		}

		tlog.Logf("%d. Prefix: %s [%s]\n", idx, test.title, p)
		msg := &apc.ListObjsMsg{Prefix: p}
		objList, err := api.ListObjects(baseParams, bck, msg, 0)
		if err != nil {
			t.Errorf("List files with prefix failed, err = %v", err)
			return
		}

		if len(objList.Entries) != objCount {
			t.Errorf("Expected number of objects with prefix %q is %d but found %d",
				test.prefix, objCount, len(objList.Entries))
			tlog.Logf("Objects returned:\n")
			for id, oo := range objList.Entries {
				tlog.Logf("    %d[%d]. %s\n", idx, id, oo.Name)
			}
		}
	}
}

func prefixLookup(t *testing.T, proxyURL string, bck cmn.Bck, fileNames []string) {
	prefixLookupDefault(t, proxyURL, bck, fileNames)
	prefixLookupCornerCases(t, proxyURL, bck, fileNames)
}

func prefixCleanup(t *testing.T, proxyURL string, bck cmn.Bck, fileNames []string) {
	var (
		wg    = cos.NewLimitedWaitGroup(40)
		errCh = make(chan error, len(fileNames))
	)

	for _, fileName := range fileNames {
		keyName := fmt.Sprintf("%s/%s", prefixDir, fileName)
		wg.Add(1)
		go func() {
			defer wg.Done()
			tutils.Del(proxyURL, bck, keyName, nil, errCh, true)
		}()
	}
	wg.Wait()

	select {
	case e := <-errCh:
		tlog.Logf("Failed to DEL: %s\n", e)
		t.Fail()
	default:
	}
}

func initFS() {
	proxyURL := tutils.GetPrimaryURL()
	primary, err := tutils.GetPrimaryProxy(proxyURL)
	if err != nil {
		tlog.Logf("ERROR: %v", err)
	}
	baseParams := tutils.BaseAPIParams(proxyURL)
	cfg, err := api.GetDaemonConfig(baseParams, primary)
	if err != nil {
		tlog.Logf("ERROR: %v", err)
	}

	config := cmn.GCO.BeginUpdate()
	config.TestFSP.Count = 1
	config.Backend = cfg.Backend
	cmn.GCO.CommitUpdate(config)

	_ = fs.CSM.Reg(fs.ObjectType, &fs.ObjectContentResolver{})
	_ = fs.CSM.Reg(fs.WorkfileType, &fs.WorkfileContentResolver{})
	_ = fs.CSM.Reg(fs.ECSliceType, &fs.ECSliceContentResolver{})
	_ = fs.CSM.Reg(fs.ECMetaType, &fs.ECMetaContentResolver{})
}

func initMountpaths(t *testing.T, proxyURL string) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{RequiredDeployment: tutils.ClusterTypeLocal})
	fsOnce.Do(initFS)
	baseParams := tutils.BaseAPIParams(proxyURL)
	fs.TestNew(nil)
	fs.TestDisableValidation()
	smap := tutils.GetClusterMap(t, proxyURL)
	for _, target := range smap.Tmap {
		mpathList, err := api.GetMountpaths(baseParams, target)
		tassert.CheckFatal(t, err)
		ensureNoDisabledMountpaths(t, target, mpathList)

		for _, mpath := range mpathList.Available {
			fs.Add(mpath, target.ID())
		}
	}
}

func findObjOnDisk(bck cmn.Bck, objName string) (fqn string) {
	fsWalkFunc := func(path string, de fs.DirEntry) error {
		if fqn != "" {
			return filepath.SkipDir
		}
		if de.IsDir() {
			return nil
		}

		ct, err := cluster.NewCTFromFQN(path, nil)
		if err != nil {
			return nil
		}
		if ct.ObjectName() == objName {
			fqn = path
			return errObjectFound
		}
		return nil
	}

	fs.WalkBck(&fs.WalkBckOpts{
		WalkOpts: fs.WalkOpts{
			Bck:      bck,
			CTs:      []string{fs.ObjectType},
			Callback: fsWalkFunc,
			Sorted:   true, // false is unsupported and asserts
		},
	})
	return fqn
}

func detectNewBucket(oldList, newList cmn.Bcks) (cmn.Bck, error) {
	for _, nbck := range newList {
		found := false
		for _, obck := range oldList {
			if obck.Name == nbck.Name {
				found = true
				break
			}
		}
		if !found {
			return nbck, nil
		}
	}
	return cmn.Bck{}, fmt.Errorf("new bucket is not found (old: %v, new: %v)", oldList, newList)
}

// xaction is running
func xactSnapRunning(snaps api.NodesXactMultiSnap) bool {
	tid, _ := snaps.Running()
	return tid != ""
}

// finished = did start in the past (use check above to confirm) and currently not running
func xactSnapNotRunning(snaps api.NodesXactMultiSnap) bool {
	return !xactSnapRunning(snaps)
}
