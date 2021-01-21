// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	"math/rand"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/devtools/tutils/readers"
	"github.com/NVIDIA/aistore/devtools/tutils/tassert"
	jsoniter "github.com/json-iterator/go"
)

const rebalanceObjectDistributionTestCoef = 0.3

const (
	prefixDir             = "filter"
	largeFileSize         = 4 * cmn.MiB
	copyBucketTimeout     = 3 * time.Minute
	rebalanceTimeout      = 5 * time.Minute
	rebalanceStartTimeout = 10 * time.Second
	multiProxyTestTimeout = 3 * time.Minute

	workerCnt = 10
)

var cliBck cmn.Bck

type ioContext struct {
	t                   *testing.T
	smap                *cluster.Smap
	controlCh           chan struct{}
	stopCh              chan struct{}
	objNames            []string
	bck                 cmn.Bck
	fileSize            uint64
	numGetErrs          atomic.Uint64
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
}

func (m *ioContext) saveClusterState() {
	m.init()
	m.smap = tutils.GetClusterMap(m.t, m.proxyURL)
	m.originalTargetCount = len(m.smap.Tmap)
	m.originalProxyCount = len(m.smap.Pmap)
	tutils.Logf("targets: %d, proxies: %d\n", m.originalTargetCount, m.originalProxyCount)
}

func (m *ioContext) init() {
	m.proxyURL = tutils.RandomProxyURL()
	if m.proxyURL == "" {
		// if random selection failed, use RO url
		m.proxyURL = tutils.GetPrimaryURL()
	}
	if m.fileSize == 0 {
		m.fileSize = cmn.KiB
	}
	if m.num > 0 {
		m.objNames = make([]string, 0, m.num)
	}
	if m.otherTasksToTrigger > 0 {
		m.controlCh = make(chan struct{}, m.otherTasksToTrigger)
	}
	if m.bck.Name == "" {
		m.bck.Name = cmn.RandString(15)
	}
	if m.bck.Provider == "" {
		m.bck.Provider = cmn.ProviderAIS
	}
	if m.numGetsEachFile == 0 {
		m.numGetsEachFile = 1
	}
	m.stopCh = make(chan struct{})

	if m.bck.IsRemote() {
		// Remove unnecessary local objects.
		tutils.EvictRemoteBucket(m.t, m.proxyURL, m.bck)
	}
	m.t.Cleanup(m._cleanup)
}

func (m *ioContext) _cleanup() {
	m.del()
	if m.bck.IsRemote() {
		// Ensure all local objects are removed.
		tutils.EvictRemoteBucket(m.t, m.proxyURL, m.bck)
	}
}

func (m *ioContext) assertClusterState() {
	smap, err := tutils.WaitForClusterState(
		m.proxyURL,
		"to check cluster state",
		m.smap.Version,
		m.originalProxyCount,
		m.originalTargetCount,
	)
	tassert.CheckFatal(m.t, err)

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

func (m *ioContext) expectTargets(n int) {
	if m.originalTargetCount < n {
		m.t.Fatalf("Must have %d or more targets in the cluster, have only %d", n, m.originalTargetCount)
	}
}

func (m *ioContext) expectProxies(n int) {
	if m.originalProxyCount < n {
		m.t.Fatalf("Must have %d or more proxies in the cluster, have only %d", n, m.originalProxyCount)
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
	if !m.bck.IsAIS() {
		m.remotePuts(false /*evict*/)
		return m.num
	}

	var (
		objPrefix  = "some_prefix"
		filenameCh = make(chan string, m.num)
		errCh      = make(chan error, m.num)
		baseParams = tutils.BaseAPIParams(m.proxyURL)
	)

	p, err := api.HeadBucket(baseParams, m.bck)
	tassert.CheckFatal(m.t, err)

	if !m.silent {
		tutils.Logf("PUT %d objects into bucket %s...\n", m.num, m.bck)
	}
	tutils.PutRandObjs(m.proxyURL, m.bck, objPrefix, m.fileSize, m.num, errCh, filenameCh, p.Cksum.Type, m.fixedSize)
	if len(dontFail) == 0 {
		tassert.SelectErr(m.t, errCh, "put", false)
	}
	close(filenameCh)
	close(errCh)
	m.objNames = m.objNames[:0]
	for f := range filenameCh {
		m.objNames = append(m.objNames, path.Join(objPrefix, f))
	}
	return len(errCh)
}

// remotePuts by default cleanups the remote bucket and puts fresh `m.num` objects
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
		msg        = &cmn.SelectMsg{Prefix: m.prefix, Props: cmn.GetPropsName}
	)

	objList, err := api.ListObjects(baseParams, m.bck, msg, 0)
	tassert.CheckFatal(m.t, err)

	m.objNames = m.objNames[:0]
	for _, obj := range objList.Entries {
		m.objNames = append(m.objNames, obj.Name)
	}

	leftToFill := m.num - len(objList.Entries)
	cmn.Assert(leftToFill > 0)

	m._remoteFill(leftToFill, false /*evict*/, false /*override*/)
}

func (m *ioContext) _remoteFill(objCnt int, evict, override bool) {
	var (
		baseParams = tutils.BaseAPIParams()
		errCh      = make(chan error, objCnt)
		wg         = cmn.NewLimitedWaitGroup(20)
		msg        = &cmn.SelectMsg{Prefix: m.prefix, Props: cmn.GetPropsName}
	)

	if !m.silent {
		tutils.Logf("remote PUT %d objects into bucket %s...\n", objCnt, m.bck)
	}

	p, err := api.HeadBucket(baseParams, m.bck)
	tassert.CheckFatal(m.t, err)

	objPrefix := m.prefix
	if objPrefix == "" {
		objPrefix = "copy"
	}
	objPrefix += "/remote_"
	for i := 0; i < objCnt; i++ {
		r, err := readers.NewRandReader(int64(m.fileSize), p.Cksum.Type)
		tassert.CheckFatal(m.t, err)

		var objName string
		if override {
			objName = m.objNames[i]
		} else {
			objName = fmt.Sprintf("%s%s%d", objPrefix, cmn.RandString(8), i)
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
	tutils.Logln("remote PUT done")

	objList, err := api.ListObjects(baseParams, m.bck, msg, 0)
	tassert.CheckFatal(m.t, err)
	if len(objList.Entries) != m.num {
		m.t.Fatalf("list_objects err: %d != %d", len(objList.Entries), m.num)
	}

	tutils.Logf("remote bucket %s: %d cached objects\n", m.bck, m.num)

	if evict {
		// TODO: This should be just single `EvictRemoteBucket`.
		if m.bck.IsCloud() {
			tutils.Logf("evicting cloud bucket %s...\n", m.bck)
			err := api.EvictRemoteBucket(baseParams, m.bck)
			tassert.CheckFatal(m.t, err)
		} else if m.bck.IsHDFS() {
			objNames := make([]string, 0, len(objList.Entries))
			for _, obj := range objList.Entries {
				objNames = append(objNames, obj.Name)
			}
			tutils.Logf("evicting HDFS bucket %s...\n", m.bck)
			xactID, err := api.EvictList(baseParams, m.bck, objNames)
			tassert.CheckFatal(m.t, err)
			_, err = api.WaitForXaction(baseParams, api.XactReqArgs{ID: xactID})
			tassert.CheckFatal(m.t, err)
		}
	}
}

func (m *ioContext) remotePrefetch(prefetchCnt int) {
	var (
		baseParams = tutils.BaseAPIParams()
		msg        = &cmn.SelectMsg{Prefix: m.prefix, Props: cmn.GetPropsName}
	)

	objList, err := api.ListObjects(baseParams, m.bck, msg, 0)
	tassert.CheckFatal(m.t, err)

	tutils.Logf("remote PREFETCH %d objects...\n", prefetchCnt)

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

func (m *ioContext) del(cnt ...int) {
	var (
		baseParams = tutils.BaseAPIParams()
		msg        = &cmn.SelectMsg{Prefix: m.prefix, Props: cmn.GetPropsName}
	)

	exists, err := api.DoesBucketExist(baseParams, cmn.QueryBcks(m.bck))
	tassert.CheckFatal(m.t, err)
	if !exists {
		return
	}

	objList, err := api.ListObjects(baseParams, m.bck, msg, 0)
	tassert.CheckFatal(m.t, err)

	toRemove := objList.Entries
	if len(cnt) > 0 {
		toRemove = toRemove[:cnt[0]]
	}

	if len(toRemove) == 0 {
		return
	}

	tutils.Logf("deleting %d objects...\n", len(toRemove))

	wg := cmn.NewLimitedWaitGroup(40)
	for _, obj := range toRemove {
		wg.Add(1)
		go func(obj *cmn.BucketEntry) {
			defer wg.Done()
			err := api.DeleteObject(baseParams, m.bck, obj.Name)
			tassert.CheckError(m.t, err)
		}(obj)
	}
	wg.Wait()
}

func (m *ioContext) get(baseParams api.BaseParams, idx, totalGets int) {
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
		baseParams = tutils.BaseAPIParams()
		totalGets  = m.num * m.numGetsEachFile
		wg         = cmn.NewLimitedWaitGroup(50)
	)

	if !m.silent {
		if m.numGetsEachFile == 1 {
			tutils.Logf("GET each of the %d objects from bucket %s...\n", m.num, m.bck)
		} else {
			tutils.Logf("GET each of the %d objects %d times from bucket %s...\n", m.num, m.numGetsEachFile, m.bck)
		}
	}

	for i := 0; i < totalGets; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			m.get(baseParams, idx, totalGets)
		}(i)
	}
	wg.Wait()
}

func (m *ioContext) getsUntilStop() {
	var (
		idx        = 0
		baseParams = tutils.BaseAPIParams()
		wg         = cmn.NewLimitedWaitGroup(40)
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
				m.get(baseParams, idx, 0)
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

func (m *ioContext) ensureNumCopies(expectedCopies int) {
	var (
		baseParams = tutils.BaseAPIParams()
		total      int
	)
	time.Sleep(time.Second)
	xactArgs := api.XactReqArgs{Kind: cmn.ActMakeNCopies, Bck: m.bck, Timeout: rebalanceTimeout}
	_, err := api.WaitForXaction(baseParams, xactArgs)
	tassert.CheckFatal(m.t, err)

	// List Bucket - primarily for the copies
	msg := &cmn.SelectMsg{Flags: cmn.SelectCached, Prefix: m.prefix}
	msg.AddProps(cmn.GetPropsCopies, cmn.GetPropsAtime, cmn.GetPropsStatus)
	objectList, err := api.ListObjects(baseParams, m.bck, msg, 0)
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

func (m *ioContext) unregisterTarget(forceUnreg ...bool) *cluster.Snode {
	var force bool
	if len(forceUnreg) != 0 {
		force = forceUnreg[0]
	}
	target, _ := m.smap.GetRandTarget()
	tutils.Logf("Unregister target: %s\n", target.URL(cmn.NetworkPublic))
	args := &cmn.ActValDecommision{
		DaemonID:      target.ID(),
		Force:         force,
		SkipRebalance: true,
	}
	err := tutils.UnregisterNode(m.proxyURL, args)
	tassert.CheckFatal(m.t, err)
	m.smap, err = tutils.WaitForClusterState(
		m.proxyURL,
		"target removed from the cluster",
		m.smap.Version,
		m.smap.CountActiveProxies(),
		m.smap.CountActiveTargets()-1,
	)
	tassert.CheckFatal(m.t, err)
	return target
}

func (m *ioContext) reregisterTarget(target *cluster.Snode) (rebID string) {
	const (
		timeout    = time.Second * 10
		interval   = time.Millisecond * 10
		iterations = int(timeout / interval)
	)

	var err error
	// T1
	tutils.Logf("Registering target %s...\n", target.ID())
	rebID, err = tutils.JoinCluster(m.proxyURL, target)
	tassert.CheckFatal(m.t, err)
	baseParams := tutils.BaseAPIParams(target.URL(cmn.NetworkPublic))
	smap := tutils.GetClusterMap(m.t, m.proxyURL)
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
			bck:  cmn.Bck{Name: cmn.RandString(10), Provider: cmn.ProviderAIS},
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
				Name:     cmn.RandString(10),
				Provider: cmn.ProviderAIS, Ns: cmn.Ns{UUID: tutils.RemoteCluster.UUID},
			},
			skipArgs: tutils.SkipTestArgs{
				RequiresRemoteCluster: true,
			},
		},
		{
			name:       "backend",
			bck:        cmn.Bck{Name: cmn.RandString(10), Provider: cmn.ProviderAIS},
			backendBck: cliBck,
			skipArgs: tutils.SkipTestArgs{
				Long:      true,
				RemoteBck: true,
			},
		},
		{
			name: "local_3_copies",
			bck:  cmn.Bck{Name: cmn.RandString(10), Provider: cmn.ProviderAIS},
			props: &cmn.BucketPropsToUpdate{
				Mirror: &cmn.MirrorConfToUpdate{
					Enabled: api.Bool(true),
					Copies:  api.Int64(3),
				},
			},
		},
		{
			name: "local_ec_2_2",
			bck:  cmn.Bck{Name: cmn.RandString(10), Provider: cmn.ProviderAIS},
			props: &cmn.BucketPropsToUpdate{
				EC: &cmn.ECConfToUpdate{
					DataSlices:   api.Int(2),
					ParitySlices: api.Int(2),
					ObjSizeLimit: api.Int64(0),
				},
			},
		},
	}
	for _, test := range tests { // nolint:gocritic // no performance critical code
		t.Run(test.name, func(t *testing.T) {
			if test.backendBck.IsEmpty() {
				test.skipArgs.Bck = test.bck
			} else {
				test.skipArgs.Bck = test.backendBck
				if !test.backendBck.IsCloud() {
					t.Skip("backend bucket is required to be a cloud bucket")
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

				defer func() {
					api.DestroyBucket(baseParams, test.bck)
				}()
			}

			p, err := api.HeadBucket(baseParams, test.bck)
			tassert.CheckFatal(t, err)

			bck := cluster.NewBckEmbed(test.bck)
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
		fileSize = cmn.KiB
	)

	// Create specific files to test corner cases.
	var (
		extraNames = []string{"dir/obj01", "dir/obj02", "dir/obj03", "dir1/dir2/obj04", "dir1/dir2/obj05"}
		fileNames  = make([]string, 0, objCnt)
		wg         = &sync.WaitGroup{}
		errCh      = make(chan error, objCnt+len(extraNames))
	)

	for i := 0; i < objCnt; i++ {
		fileName := cmn.RandString(20)
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
	tutils.Logf("Looking up for files in alphabetic order\n")

	var (
		letters    = "abcdefghijklmnopqrstuvwxyz"
		baseParams = tutils.BaseAPIParams(proxyURL)
	)
	for i := 0; i < len(letters); i++ {
		key := letters[i : i+1]
		lookFor := fmt.Sprintf("%s/%s", prefixDir, key)
		msg := &cmn.SelectMsg{Prefix: lookFor}
		objList, err := api.ListObjects(baseParams, bck, msg, 0)
		if err != nil {
			t.Errorf("List files with prefix failed, err = %v", err)
			return
		}

		numFiles := len(objList.Entries)
		realNumFiles := numberOfFilesWithPrefix(fileNames, key)

		if numFiles == realNumFiles {
			if numFiles != 0 {
				tutils.Logf("Found %v files starting with %q\n", numFiles, key)
			}
		} else {
			t.Errorf("Expected number of files with prefix %q is %v but found %v files", key, realNumFiles, numFiles)
			tutils.Logf("Objects returned:\n")
			for id, oo := range objList.Entries {
				tutils.Logf("    %d[%d]. %s\n", i, id, oo.Name)
			}
		}
	}
}

func prefixLookupCornerCases(t *testing.T, proxyURL string, bck cmn.Bck, objNames []string) {
	tutils.Logf("Testing corner cases\n")

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

		tutils.Logf("%d. Prefix: %s [%s]\n", idx, test.title, p)
		msg := &cmn.SelectMsg{Prefix: p}
		objList, err := api.ListObjects(baseParams, bck, msg, 0)
		if err != nil {
			t.Errorf("List files with prefix failed, err = %v", err)
			return
		}

		if len(objList.Entries) != objCount {
			t.Errorf("Expected number of objects with prefix %q is %d but found %d",
				test.prefix, objCount, len(objList.Entries))
			tutils.Logf("Objects returned:\n")
			for id, oo := range objList.Entries {
				tutils.Logf("    %d[%d]. %s\n", idx, id, oo.Name)
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
		wg    = cmn.NewLimitedWaitGroup(40)
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
		tutils.Logf("Failed to DEL: %s\n", e)
		t.Fail()
	default:
	}
}
