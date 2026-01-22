// Package integration_test.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	iofs "io/fs"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact"
)

const (
	rootDir = "/tmp/ais"
)

const (
	rebalanceObjectDistributionTestCoef = 0.3
)

const (
	prefixDir        = "filter"
	largeFileSize    = 4 * cos.MiB
	cloudMinPartSize = 5 * cos.MiB // AWS and GCP minimum part size for multipart uploads

	workerCnt = 10
)

const testMpath = "/tmp/ais/mountpath"

var (
	cliBck             cmn.Bck
	cliIOCtxChunksConf *ioCtxChunksConf
	errObjectFound     = errors.New("found") // to interrupt fs.Walk when object found
	_onceInit          sync.Once
)

type (
	ioContext struct {
		t         *testing.T
		smap      *meta.Smap
		controlCh chan struct{}
		stopCh    chan struct{}
		objNames  []string
		bck       cmn.Bck

		// Chunks configuration
		chunksConf *ioCtxChunksConf

		// File size configuration
		fileSize      uint64
		fixedSize     bool
		fileSizeRange [2]uint64 // [min, max] size range; take precedence over `fileSize` and `fixedSize`

		proxyURL            string
		prefix              string
		otherTasksToTrigger int
		originalTargetCount int
		originalProxyCount  int
		num                 int
		numGetsEachFile     int
		nameLen             int
		objIdx              int // Used in `m.nextObjName`
		numPutErrs          int

		numGetErrs atomic.Uint64

		getErrIsFatal       bool
		silent              bool
		deleteRemoteBckObjs bool
		skipRemoteEvict     bool // skip initial evict/delete for remote buckets (useful when multiple ioContexts share a bucket)
		ordered             bool // true - object names make sequence, false - names are random
		skipVC              bool // skip loading existing object's metadata (see also: apc.QparamSkipVC and api.PutArgs.SkipVC)
	}
	ioCtxChunksConf struct {
		numChunks int // desired number of chunks
		multipart bool
	}
)

func (m *ioContext) initAndSaveState(cleanup bool) {
	m.init(cleanup)
	m.saveCluState(m.proxyURL)
}

func (m *ioContext) saveCluState(proxyURL string) {
	m.smap = tools.GetClusterMap(m.t, proxyURL)
	m.originalTargetCount = m.smap.CountActiveTs()
	m.originalProxyCount = m.smap.CountActivePs()
	tlog.Logfln("targets: %d, proxies: %d", m.originalTargetCount, m.originalProxyCount)
}

func (m *ioContext) waitAndCheckCluState() {
	smap, err := tools.WaitForClusterState(
		m.proxyURL,
		"cluster state",
		m.smap.Version,
		m.originalProxyCount,
		m.originalTargetCount,
	)
	tassert.CheckFatal(m.t, err)
	m.checkCluState(smap)
}

func (m *ioContext) checkCluState(smap *meta.Smap) {
	m.t.Helper()
	proxyCount := smap.CountActivePs()
	targetCount := smap.CountActiveTs()
	if targetCount != m.originalTargetCount ||
		proxyCount != m.originalProxyCount {
		m.t.Errorf(
			"cluster state is not preserved. targets (before: %d, now: %d); proxies: (before: %d, now: %d)",
			targetCount, m.originalTargetCount,
			proxyCount, m.originalProxyCount,
		)
	}
}

func (m *ioContext) init(cleanup bool) {
	m.proxyURL = tools.RandomProxyURL()
	if m.proxyURL == "" {
		// if random selection failed, use RO url
		m.proxyURL = tools.GetPrimaryURL()
	}
	if m.fileSize == 0 && m.fileSizeRange[0] == 0 && m.fileSizeRange[1] == 0 {
		m.fileSizeRange[0], m.fileSizeRange[1] = cos.KiB, 4*cos.KiB
	}
	if m.num > 0 {
		m.objNames = make([]string, 0, m.num)
	}
	if m.otherTasksToTrigger > 0 {
		m.controlCh = make(chan struct{}, m.otherTasksToTrigger)
	}
	if m.bck.Name == "" {
		m.bck.Name = trand.String(15)
		m.bck.Ns = genBucketNs()
	}
	if m.bck.Provider == "" {
		m.bck.Provider = apc.AIS
	}
	if m.numGetsEachFile == 0 {
		m.numGetsEachFile = 1
	}
	m.stopCh = make(chan struct{})

	// NOTE: randomize skipVC (may need to assign explicitly in the future)
	m.skipVC = mono.NanoTime()&1 == 0

	if m.bck.IsRemote() && !m.skipRemoteEvict {
		if m.deleteRemoteBckObjs {
			m.del(-1 /*delete all*/, 0 /* lsmsg.Flags */)
		} else {
			tools.EvictRemoteBucket(m.t, m.proxyURL, m.bck, true /*keepMD*/)
		}
	}
	if cleanup {
		// cleanup upon exit from this (m.t) test
		m.t.Cleanup(func() {
			m.del()
			if m.bck.IsRemote() {
				tools.EvictRemoteBucket(m.t, m.proxyURL, m.bck, true /*keepMD*/)
			}
		})
	}
	// If no chunks configuration is provided, use the default from the environment variable
	if m.chunksConf == nil {
		m.chunksConf = cliIOCtxChunksConf
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
	m.t.Helper()
	var (
		requiredCount     = int64(rebalanceObjectDistributionTestCoef * (float64(m.num) / float64(m.originalTargetCount)))
		targetObjectCount = make(map[string]int64)
	)
	tlog.Logfln("Checking if each target has a required number of object in bucket %s...", m.bck.String())
	baseParams := tools.BaseAPIParams(m.proxyURL)
	lst, err := api.ListObjects(baseParams, m.bck, &apc.LsoMsg{Props: apc.GetPropsLocation}, api.ListArgs{})
	tassert.CheckFatal(t, err)
	for _, obj := range lst.Entries {
		tname, _ := core.ParseObjLoc(obj.Location)
		tid := meta.N2ID(tname)
		targetObjectCount[tid]++
	}
	if len(targetObjectCount) != m.originalTargetCount {
		t.Fatalf("Rebalance error, %d/%d targets received no objects from bucket %s\n",
			m.originalTargetCount-len(targetObjectCount), m.originalTargetCount, m.bck.String())
	}
	for targetURL, objCount := range targetObjectCount {
		if objCount < requiredCount {
			t.Fatalf("Rebalance error, target %s didn't receive required number of objects\n", targetURL)
		}
	}
}

func (m *ioContext) sizesToString() (s string) {
	siz0, siz1 := int64(m.fileSizeRange[0]), int64(m.fileSizeRange[1])
	switch {
	case siz0 >= 0 && siz1 > 0:
		s = fmt.Sprintf(" (size %s - %s)", cos.IEC(siz0, 0), cos.IEC(siz1, 0))
		debug.Assert(siz1 >= siz0, s)
	case m.fixedSize:
		s = fmt.Sprintf(" (size %d)", m.fileSize)
	case m.fileSize > 0:
		s = fmt.Sprintf(" (approx. size %d)", m.fileSize)
	}
	if m.chunksConf != nil && m.chunksConf.multipart {
		s += fmt.Sprintf(" (%d chunks)", m.chunksConf.numChunks)
	}
	return s
}

func (m *ioContext) puts(ignoreErrs ...bool) {
	m.t.Helper()
	if !m.bck.IsAIS() {
		m.remotePuts(false /*evict*/)
		return
	}
	baseParams := tools.BaseAPIParams(m.proxyURL)
	p, err := api.HeadBucket(baseParams, m.bck, false /* don't add */)
	tassert.CheckFatal(m.t, err)

	if !m.silent {
		s := m.sizesToString()
		tlog.Logfln("PUT %d objects%s => %s", m.num, s, m.bck.Cname(m.prefix))
	}
	putArgs := tools.PutObjectsArgs{
		ProxyURL:     m.proxyURL,
		Bck:          m.bck,
		ObjPath:      m.prefix,
		ObjCnt:       m.num,
		ObjNameLn:    m.nameLen,
		ObjSizeRange: m.fileSizeRange, // take precedence over `ObjSize` and `FixedSize`
		ObjSize:      m.fileSize,
		FixedSize:    m.fixedSize,
		CksumType:    p.Cksum.Type,
		WorkerCnt:    0, // TODO: Should we set something custom?
		IgnoreErr:    len(ignoreErrs) > 0 && ignoreErrs[0],
		Ordered:      m.ordered,
		SkipVC:       m.skipVC,
	}
	if m.chunksConf != nil && m.chunksConf.multipart {
		putArgs.MultipartNumChunks = m.chunksConf.numChunks
		m.objNames, m.numPutErrs, err = tools.PutRandObjs(putArgs)
		tassert.CheckFatal(m.t, err)

		// verify objects are chunked
		ls, err := api.ListObjects(baseParams, m.bck, &apc.LsoMsg{Prefix: m.prefix, Props: apc.GetPropsChunked}, api.ListArgs{})
		tassert.CheckFatal(m.t, err)
		if len(ls.Entries) != m.num {
			tlog.Logfln("warning: expected %d objects, got %d", m.num, len(ls.Entries))
		}
	} else {
		m.objNames, m.numPutErrs, err = tools.PutRandObjs(putArgs)
	}
	tassert.CheckFatal(m.t, err)
}

// adjustFileSizeRange adjusts the file size range to avoid cloud backend [EntityTooSmall] errors
// for AWS and GCP backends which require minimum 5MiB per part in multipart uploads
func (m *ioContext) adjustFileSizeRange() {
	m.t.Helper()

	if m.chunksConf == nil || !m.chunksConf.multipart {
		return
	}

	// Check if backend is AWS or GCP (both have 5MiB minimum part size)
	provider := m.bck.Provider
	if m.bck.Backend() != nil {
		provider = m.bck.Backend().Provider
	}
	if provider != apc.AWS && provider != apc.GCP {
		return
	}

	minTotalSize := cloudMinPartSize * uint64(m.chunksConf.numChunks)

	if m.fileSizeRange[0] >= minTotalSize && m.fileSizeRange[1] >= minTotalSize {
		return
	}

	m.fileSizeRange = [2]uint64{minTotalSize, minTotalSize * 2}
	tlog.Logfln("%s backend detected, increase file size range to %s - %s to avoid [EntityTooSmall] errors",
		provider, cos.IEC(int64(m.fileSizeRange[0]), 0), cos.IEC(int64(m.fileSizeRange[1]), 0))
}

// update updates the object with a new random reader and returns the reader and the size; reader is used to validate the object after the update
func (m *ioContext) update(objName, cksumType string) (readers.Reader, uint64) {
	m.adjustFileSizeRange()
	var (
		size      = tools.GetRandSize(m.fileSizeRange, m.fileSize, m.fixedSize)
		errCh     = make(chan error, 1)
		numChunks int
	)
	if m.chunksConf != nil && m.chunksConf.multipart {
		numChunks = m.chunksConf.numChunks
	}
	r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: int64(size), CksumType: cksumType})
	tassert.CheckFatal(m.t, err)
	tools.Put(m.proxyURL, m.bck, objName, r, size, numChunks, errCh)
	tassert.SelectErr(m.t, errCh, "put", true)

	return r, size
}

func (m *ioContext) updateAndValidate(baseParams api.BaseParams, idx int, cksumType string) {
	if idx < 0 || idx >= len(m.objNames) {
		m.t.Fatalf("index out of range: %d, len(objNames): %d", idx, len(m.objNames))
	}

	r, size := m.update(m.objNames[idx], cksumType)

	// GET and validate the object
	w := bytes.NewBuffer(nil)
	result, s, err := api.GetObjectReader(baseParams, m.bck, m.objNames[idx], &api.GetArgs{Writer: w})
	tassert.CheckFatal(m.t, err)

	// Compare retrieved content with original data
	br, err := r.Open()
	tassert.CheckFatal(m.t, err)
	tassert.Fatalf(m.t, s == int64(size), "object %s size mismatch: expected %d, got %d", m.objNames[idx], size, s)
	tassert.Fatalf(m.t, tools.ReaderEqual(br, result), "object %s content mismatch", m.objNames[idx])
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

	m.adjustFileSizeRange()

	m._remoteFill(m.num, evict, override)
}

// remoteRefill calculates number of missing objects and refills the bucket.
// It is expected that the number of missing objects is positive meaning that
// some of the objects were removed before calling remoteRefill.
func (m *ioContext) remoteRefill() {
	var (
		baseParams = tools.BaseAPIParams()
		msg        = &apc.LsoMsg{Prefix: m.prefix, Props: apc.GetPropsName}
	)

	lst, err := api.ListObjects(baseParams, m.bck, msg, api.ListArgs{})
	tassert.CheckFatal(m.t, err)

	m.objNames = m.objNames[:0]
	for _, obj := range lst.Entries {
		m.objNames = append(m.objNames, obj.Name)
	}

	leftToFill := m.num - len(lst.Entries)
	tassert.Fatalf(m.t, leftToFill > 0, "leftToFill %d", leftToFill)

	m._remoteFill(leftToFill, false /*evict*/, false /*override*/)
}

func (m *ioContext) _remoteFill(objCnt int, evict, override bool) {
	var (
		baseParams = tools.BaseAPIParams()
		errCh      = make(chan error, objCnt)
		wg         = cos.NewLimitedWaitGroup(20, 0)
	)
	if !m.silent {
		s := m.sizesToString()
		tlog.Logfln("remote PUT %d objects%s => %s", objCnt, s, m.bck.Cname(m.prefix))
	}
	p, err := api.HeadBucket(baseParams, m.bck, false /* don't add */)
	tassert.CheckFatal(m.t, err)

	for i := range objCnt {
		var (
			objName   string
			numChunks int
			size      = tools.GetRandSize(m.fileSizeRange, m.fileSize, m.fixedSize)
		)
		switch {
		case override:
			objName = m.objNames[i]
		case m.ordered:
			objName = fmt.Sprintf("%s%d", m.prefix, i)
		default:
			objName = fmt.Sprintf("%s%s-%d", m.prefix, trand.String(8), i)
		}

		if m.chunksConf != nil && m.chunksConf.multipart {
			numChunks = m.chunksConf.numChunks
		}

		wg.Add(1)
		go func(size uint64, objName string, cksumType string) {
			defer wg.Done()

			r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: int64(size), CksumType: cksumType})
			tassert.CheckFatal(m.t, err)
			tools.Put(m.proxyURL, m.bck, objName, r, size, numChunks, errCh)
		}(size, objName, p.Cksum.Type)

		if !override {
			m.objNames = append(m.objNames, objName)
		}
	}
	wg.Wait()
	tassert.SelectErr(m.t, errCh, "put", true)
	tlog.Logfln("remote bucket %s: %d cached objects", m.bck.String(), m.num)

	if evict {
		m.evict()
	}
}

func (m *ioContext) evict() {
	var (
		baseParams = tools.BaseAPIParams()
		msg        = &apc.LsoMsg{Prefix: m.prefix, Props: apc.GetPropsName}
	)

	lst, err := api.ListObjects(baseParams, m.bck, msg, api.ListArgs{})
	tassert.CheckFatal(m.t, err)
	if len(lst.Entries) != m.num {
		m.t.Fatalf("list_objects err: %d != %d", len(lst.Entries), m.num)
	}

	tlog.Logfln("evicting remote bucket %s...", m.bck.String())
	err = api.EvictRemoteBucket(baseParams, m.bck, false)
	tassert.CheckFatal(m.t, err)
}

// TODO: optionally, filter by content type as well
// NOTE: assuming 'RequiredDeployment == tools.ClusterTypeLocal'
func (m *ioContext) backdateLocalObjs(age time.Duration) {
	var (
		sep     = string(filepath.Separator)
		old     = time.Now().Add(-age)
		touched int
	)
	err := filepath.WalkDir(rootDir, func(path string, de iofs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if de.IsDir() {
			return nil
		}
		if strings.HasPrefix(de.Name(), m.prefix) && strings.Contains(path, sep+m.bck.Name+sep) {
			touched++
			return os.Chtimes(path, old, old)
		}
		return nil
	})
	tassert.CheckFatal(m.t, err)

	if touched != len(m.objNames) {
		tlog.Logfln("Warning: touched %d != %d objnames", touched, len(m.objNames))
	}
}

func (m *ioContext) remotePrefetch(prefetchCnt int) {
	var (
		baseParams = tools.BaseAPIParams()
		msg        = &apc.LsoMsg{Prefix: m.prefix, Props: apc.GetPropsName}
	)

	lst, err := api.ListObjects(baseParams, m.bck, msg, api.ListArgs{})
	tassert.CheckFatal(m.t, err)

	tlog.Logfln("remote PREFETCH %d objects...", prefetchCnt)

	wg := &sync.WaitGroup{}
	for idx, obj := range lst.Entries {
		if idx >= prefetchCnt {
			break
		}

		wg.Add(1)
		go func(obj *cmn.LsoEnt) {
			_, err := api.GetObject(baseParams, m.bck, obj.Name, nil)
			tassert.CheckError(m.t, err)
			wg.Done()
		}(obj)
	}
	wg.Wait()
}

func isContextDeadline(err error) bool {
	if err == nil {
		return false
	}
	return err == context.DeadlineExceeded || strings.Contains(err.Error(), context.DeadlineExceeded.Error())
}

// bucket cleanup
// is called in a variety of ways including (post-test) t.Cleanup => _cleanup()
// and (pre-test) via deleteRemoteBckObjs

const (
	maxDelObjErrCount = 10
	sleepDelObj       = 2 * time.Second
	maxSleepDelObj    = 10 * time.Second
	limitDelObjGor    = 8
)

func (m *ioContext) del(opts ...int) {
	var (
		herr        *cmn.ErrHTTP
		toRemoveCnt = -1 // remove all or opts[0]
		baseParams  = tools.BaseAPIParams()
	)
	// checks, params
	exists, err := api.QueryBuckets(baseParams, cmn.QueryBcks(m.bck), apc.FltExists)
	if isContextDeadline(err) {
		if m.bck.IsRemote() {
			time.Sleep(time.Second)
			tlog.Logfln("Warning: 2nd attempt to query buckets %q", cmn.QueryBcks(m.bck))
			exists, err = api.QueryBuckets(baseParams, cmn.QueryBcks(m.bck), apc.FltExists)
			if isContextDeadline(err) {
				tlog.Logfln("Error: failing to query buckets %q: %v - proceeding anyway...", cmn.QueryBcks(m.bck), err)
				exists, err = false, nil
			}
		}
	}
	tassert.CheckFatal(m.t, err)
	if !exists {
		return
	}

	// list
	lsmsg := &apc.LsoMsg{
		Prefix: m.prefix,
		Props:  apc.GetPropsName,
		Flags:  apc.LsBckPresent, // don't lookup unless overridden by the variadic (below)
	}
	if len(opts) > 0 {
		toRemoveCnt = opts[0]
		if len(opts) > 1 {
			lsmsg.Flags = uint64(opts[1]) // do HEAD(remote-bucket)
		}
	}
	if toRemoveCnt < 0 && m.prefix == "" {
		lsmsg.Prefix = "" // all means all (when prefix is already empty)
	}
	lst, err := api.ListObjects(baseParams, m.bck, lsmsg, api.ListArgs{})
	if err != nil {
		if errors.As(err, &herr) && herr.Status == http.StatusNotFound {
			return
		}
		emsg := err.Error()
		// ignore client timeout awaiting headers
		if strings.Contains(emsg, "awaiting") && strings.Contains(emsg, "headers") {
			return
		}
	}
	tassert.CheckFatal(m.t, err)

	// delete
	toRemove := lst.Entries
	if toRemoveCnt >= 0 {
		n := min(toRemoveCnt, len(toRemove))
		toRemove = toRemove[:n]
	}
	l := len(toRemove)
	if l == 0 {
		return
	}
	tlog.Logfln("deleting %d object%s from %s", l, cos.Plural(l), m.bck.Cname(lsmsg.Prefix))
	var (
		errCnt atomic.Int64
		sleep  = atomic.NewInt64(int64(sleepDelObj))
		wg     = cos.NewLimitedWaitGroup(min(limitDelObjGor, sys.MaxParallelism()), l)
	)
	for _, obj := range toRemove {
		if errCnt.Load() > maxDelObjErrCount {
			tassert.CheckFatal(m.t, errors.New("too many errors"))
			break
		}
		wg.Add(1)
		go func(obj *cmn.LsoEnt) {
			m._delOne(baseParams, obj, &errCnt, sleep)
			wg.Done()
		}(obj)
	}
	wg.Wait()
}

// TODO: rid of strings.Contains - use cmn.AsErrHTTP
func (m *ioContext) _delOne(baseParams api.BaseParams, obj *cmn.LsoEnt, errCnt, sleep *atomic.Int64) {
	err := api.DeleteObject(baseParams, m.bck, obj.Name)
	if err == nil {
		return
	}

	var (
		e = strings.ToLower(err.Error())
		d = time.Duration(sleep.Load())
		b bool
	)
	switch {
	case cmn.IsErrObjNought(err):
		return
	case strings.Contains(e, "server closed idle connection"):
		return // see (unexported) http.exportErrServerClosedIdle in the Go source
	case cos.IsErrConnectionNotAvail(err):
		errCnt.Add(max(2, maxDelObjErrCount/10-1))

	// retry (cloud): transient network or well-known HTTP flaps
	case m.bck.IsCloud():
		// respectively:
		// - timeout / reset / refused / aborted / pipe / url.Timeout
		// - 429 Too Many Requests
		// - 502 Bad Gateway
		// - 503 Service Unavailable
		// - AWS InternalError "Please try again"
		// - GCS / LB 504 wording
		retriable := cos.IsErrRetriableConn(err) ||
			strings.Contains(e, "too many requests") ||
			cmn.IsStatusBadGateway(err) || strings.Contains(e, "bad gateway") ||
			cmn.IsStatusServiceUnavailable(err) || strings.Contains(e, "service unavailable") ||
			strings.Contains(e, "try again") ||
			(apc.ToScheme(m.bck.Provider) == apc.GSScheme && strings.Contains(e, "gateway") && strings.Contains(e, "timeout"))
		if retriable {
			time.Sleep(d)
			err = api.DeleteObject(baseParams, m.bck, obj.Name)
			b = true
		}
	}

	if err == nil || cmn.IsErrObjNought(err) {
		return
	}
	errCnt.Inc()
	if b {
		time.Sleep(d)
		a := sleep.Load()
		a += a >> 1
		sleep.Store(min(int64(maxSleepDelObj), a))
	}
	if m.bck.IsCloud() && errCnt.Load() < 5 {
		tlog.Logfln("Warning: failed to cleanup %s: %v", m.bck.Cname(""), err)
	}
	tassert.CheckError(m.t, err)
}

func (m *ioContext) get(baseParams api.BaseParams, idx, totalGets int, getArgs *api.GetArgs, validate bool) {
	var (
		err     error
		objName = m.objNames[idx%len(m.objNames)]
	)
	if validate {
		_, err = api.GetObjectWithValidation(baseParams, m.bck, objName, getArgs)
	} else {
		_, err = api.GetObject(baseParams, m.bck, objName, getArgs)
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
			tlog.Logfln(" %d/%d GET requests completed...", idx, totalGets)
		} else {
			tlog.Logfln(" %d GET requests completed...", idx)
		}
	}

	// Tell other tasks they can begin to do work in parallel
	if totalGets > 0 && idx == totalGets/2 { // only for `m.gets(nil, false)`
		for range m.otherTasksToTrigger {
			m.controlCh <- struct{}{}
		}
	}
}

func (m *ioContext) gets(getArgs *api.GetArgs, withValidation bool) {
	var (
		bp        = tools.BaseAPIParams()
		totalGets = m.num * m.numGetsEachFile
	)
	if !m.silent {
		if m.numGetsEachFile == 1 {
			tlog.Logfln("GET %d objects from %s", m.num, m.bck.String())
		} else {
			tlog.Logfln("GET %d objects %d times from %s", m.num, m.numGetsEachFile, m.bck.String())
		}
	}
	wg := cos.NewLimitedWaitGroup(20, 0)
	for i := range totalGets {
		wg.Add(1)
		go func(idx int) {
			m.get(bp, idx, totalGets, getArgs, withValidation)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func (m *ioContext) getsUntilStop() {
	var (
		idx        = 0
		baseParams = tools.BaseAPIParams()
		wg         = cos.NewLimitedWaitGroup(20, 0)
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
				m.get(baseParams, idx, 0, nil /*api.GetArgs*/, false /*validate*/)
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

func (m *ioContext) nextObjName() string {
	if m.objIdx >= len(m.objNames) {
		m.t.Fatal("not enough objects to get next object name")
		return ""
	}
	objName := m.objNames[m.objIdx]
	m.objIdx++
	return objName
}

func (m *ioContext) startMaintenanceNoRebalance() *meta.Snode {
	target, _ := m.smap.GetRandTarget()
	tlog.Logfln("Put %s in maintenance", target.StringEx())
	args := &apc.ActValRmNode{DaemonID: target.ID(), SkipRebalance: true}
	_, err := api.StartMaintenance(tools.BaseAPIParams(m.proxyURL), args)
	tassert.CheckFatal(m.t, err)
	m.smap, err = tools.WaitForClusterState(
		m.proxyURL,
		"put target in maintenance",
		m.smap.Version,
		m.smap.CountActivePs(),
		m.smap.CountActiveTs()-1,
	)
	tassert.CheckFatal(m.t, err)
	return target
}

func (m *ioContext) stopMaintenance(target *meta.Snode) string {
	tlog.Logfln("Take %s out of maintenance mode...", target.StringEx())
	bp := tools.BaseAPIParams(m.proxyURL)
	rebID, err := api.StopMaintenance(bp, &apc.ActValRmNode{DaemonID: target.ID()})
	tassert.CheckFatal(m.t, err)
	if rebID == "" {
		return ""
	}
	tassert.Fatalf(m.t, xact.IsValidRebID(rebID), "invalid reb ID %q", rebID)

	xargs := xact.ArgsMsg{ID: rebID, Kind: apc.ActRebalance, Timeout: tools.RebalanceStartTimeout}
	api.WaitForSnaps(bp, &xargs, xargs.Started())

	return rebID
}

func (m *ioContext) setNonDefaultBucketProps() {
	baseParams := tools.BaseAPIParams()
	copies := int64(2)
	props := &cmn.BpropsToSet{
		Mirror: &cmn.MirrorConfToSet{
			Enabled: apc.Ptr(copies > 0),
			Copies:  apc.Ptr[int64](copies),
		},
		Cksum: &cmn.CksumConfToSet{
			Type:            apc.Ptr(cos.ChecksumSHA512),
			EnableReadRange: apc.Ptr(true),
			ValidateWarmGet: apc.Ptr(true),
			ValidateColdGet: apc.Ptr(false),
		},
		Extra: &cmn.ExtraToSet{
			AWS: &cmn.ExtraPropsAWSToSet{CloudRegion: apc.Ptr("us-notheast")},
		},
	}
	_, err := api.SetBucketProps(baseParams, m.bck, props)
	tassert.CheckFatal(m.t, err)
}

func runProviderTests(t *testing.T, f func(*testing.T, *meta.Bck)) {
	tests := []struct {
		name       string
		bck        cmn.Bck
		backendBck cmn.Bck
		skipArgs   tools.SkipTestArgs
		props      *cmn.BpropsToSet
	}{
		{
			name: "local",
			bck:  cmn.Bck{Name: trand.String(10), Provider: apc.AIS, Ns: genBucketNs()},
		},
		{
			name: "remote",
			bck:  cmn.Bck{Name: cliBck.Name, Provider: cliBck.Provider, Ns: genBucketNs()},
			skipArgs: tools.SkipTestArgs{
				Long:      true,
				RemoteBck: true,
			},
		},
		{
			name: "remote_ais",
			bck: cmn.Bck{
				Name:     trand.String(10),
				Provider: apc.AIS, Ns: cmn.Ns{UUID: tools.RemoteCluster.UUID},
			},
			skipArgs: tools.SkipTestArgs{
				RequiresRemoteCluster: true,
				Long:                  true,
			},
		},
		{
			name:       "backend",
			bck:        cmn.Bck{Name: trand.String(10), Provider: apc.AIS},
			backendBck: cliBck,
			skipArgs: tools.SkipTestArgs{
				Long:      true,
				RemoteBck: true,
			},
		},
		{
			name: "local_3_copies",
			bck:  cmn.Bck{Name: trand.String(10), Provider: apc.AIS, Ns: genBucketNs()},
			props: &cmn.BpropsToSet{
				Mirror: &cmn.MirrorConfToSet{
					Enabled: apc.Ptr(true),
					Copies:  apc.Ptr[int64](3),
				},
			},
			skipArgs: tools.SkipTestArgs{Long: true},
		},
		{
			name: "local_ec_2_2",
			bck:  cmn.Bck{Name: trand.String(10), Provider: apc.AIS, Ns: genBucketNs()},
			props: &cmn.BpropsToSet{
				EC: &cmn.ECConfToSet{
					DataSlices:   apc.Ptr(2),
					ParitySlices: apc.Ptr(2),
					ObjSizeLimit: apc.Ptr[int64](0),
				},
			},
			skipArgs: tools.SkipTestArgs{Long: true},
		},
	}
	for i := range tests {
		test := tests[i]
		t.Run(test.name, func(t *testing.T) {
			if test.backendBck.IsEmpty() {
				test.skipArgs.Bck = test.bck
			} else {
				test.skipArgs.Bck = test.backendBck
				if !test.backendBck.IsCloud() {
					t.Skipf("backend bucket must be a Cloud bucket (have %q)", test.backendBck.String())
				}
			}
			tools.CheckSkip(t, &test.skipArgs)

			baseParams := tools.BaseAPIParams()

			if test.props != nil && test.props.Mirror != nil {
				skip := tools.SkipTestArgs{
					MinMountpaths: int(*test.props.Mirror.Copies),
				}
				tools.CheckSkip(t, &skip)
			}
			if test.props != nil && test.props.EC != nil {
				skip := tools.SkipTestArgs{
					MinTargets: *test.props.EC.DataSlices + *test.props.EC.ParitySlices + 1,
				}
				tools.CheckSkip(t, &skip)
			}

			if test.bck.IsAIS() || test.bck.IsRemoteAIS() {
				err := api.CreateBucket(baseParams, test.bck, test.props)
				tassert.CheckFatal(t, err)

				if !test.backendBck.IsEmpty() {
					tools.SetBackendBck(t, baseParams, test.bck, test.backendBck)
				}
				t.Cleanup(func() {
					api.DestroyBucket(baseParams, test.bck)
				})
			}

			p, err := api.HeadBucket(baseParams, test.bck, false /* don't add */)
			tassert.CheckFatal(t, err)

			bck := meta.CloneBck(&test.bck)
			bck.Props = p

			f(t, bck)
		})
	}
}

func initOnce() {
	proxyURL := tools.GetPrimaryURL()
	primary, err := tools.GetPrimaryProxy(proxyURL)
	if err != nil {
		tlog.Logf("Error: %v", err)
	}
	baseParams := tools.BaseAPIParams(proxyURL)
	cfg, err := api.GetDaemonConfig(baseParams, primary)
	if err != nil {
		tlog.Logf("Error: %v", err)
	}

	config := cmn.GCO.BeginUpdate()
	config.TestFSP.Count = 1
	config.Backend = cfg.Backend
	cmn.GCO.CommitUpdate(config)
}

func initMountpaths(t *testing.T, proxyURL string, disabledOk ...bool) {
	t.Helper()

	isLocal, err := tools.IsClusterLocal()
	tassert.CheckFatal(t, err)
	if !isLocal {
		tlog.Logfln("initMountpaths: skipping for non-local deployment")
		return
	}

	fs.TestNew(nil)
	_onceInit.Do(initOnce)

	dok := len(disabledOk) > 0 && disabledOk[0]

	baseParams := tools.BaseAPIParams(proxyURL)
	smap := tools.GetClusterMap(t, proxyURL)
	for _, target := range smap.Tmap {
		mpathList, err := api.GetMountpaths(baseParams, target)
		tassert.CheckFatal(t, err)
		if !dok {
			ensureNoDisabledMountpaths(t, target, mpathList)
		}
		for _, mpath := range mpathList.Available {
			fs.Add(mpath, target.ID())
		}
	}
}

// NOTE:
// - do not fs.Walk if the bucket's content is chunked; instead GET to temp and return the latter
// - this workaround won't work those tests that corrupt bits
func (m *ioContext) findObjOnDisk(bck cmn.Bck, objName string) (fqn string) {
	//
	// TODO -- FIXME: this is _not_ the only condition indicating a chunked content
	//
	if m.chunksConf != nil && m.chunksConf.multipart {
		return getObjToTemp(m.t, m.proxyURL, bck, objName)
	}

	fsWalkFunc := func(path string, de fs.DirEntry) error {
		if fqn != "" {
			return filepath.SkipDir
		}
		if de.IsDir() {
			return nil
		}

		ct, err := core.NewCTFromFQN(path, nil)
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
			CTs:      []string{fs.ObjCT},
			Callback: fsWalkFunc,
			Sorted:   true, // false is unsupported and asserts
		},
	})
	return fqn
}

func (m *ioContext) findObjChunksOnDisk(bck cmn.Bck, objName string) (fqn []string) {
	if m.chunksConf != nil {
		fqn = make([]string, 0, m.chunksConf.numChunks)
	} else {
		fqn = make([]string, 0, 4)
	}
	fsWalkFunc := func(path string, de fs.DirEntry) error {
		if de.IsDir() {
			return nil
		}

		ct, err := core.NewCTFromFQN(path, nil)
		if err != nil {
			return nil
		}
		if strings.HasPrefix(ct.ObjectName(), objName) { // chunk files have the same prefix as the object
			fqn = append(fqn, path)
		}
		return nil
	}
	fs.WalkBck(&fs.WalkBckOpts{
		WalkOpts: fs.WalkOpts{
			Bck:      bck,
			CTs:      []string{fs.ChunkCT},
			Callback: fsWalkFunc,
			Sorted:   true, // false is unsupported and asserts
		},
	})
	return fqn
}

// validateChunksOnDisk validates the number of chunks on disk for an object.
// - expectedChunks: expected number of total chunks:
//   - > 0: exactly this many chunks expected (including main object, i.e., len(chunks)+1 == expected)
//   - = 0: no chunk files expected (len(chunks) == 0)
//   - < 0: at least |expectedChunks| chunk files expected (len(chunks) >= |expected|)
func (m *ioContext) validateChunksOnDisk(bck cmn.Bck, objName string, expectedChunks int) {
	m.t.Helper()

	isLocal, err := tools.IsClusterLocal()
	tassert.CheckFatal(m.t, err)
	if !isLocal {
		tlog.Logfln("validateChunksOnDisk: skipping for non-local deployment")
		return
	}

	chunks := m.findObjChunksOnDisk(bck, objName)
	switch {
	case expectedChunks > 0:
		// NOTE: findObjChunksOnDisk() helper returns len(chunks) that does NOT include chunk #1
		gotTotal := len(chunks) + 1

		// NOTE: check for >= rather than equality (benign)
		tassert.Fatalf(m.t, gotTotal >= expectedChunks,
			"object %s: expected at least %d total chunks, found %d (chunk files: %d)",
			objName, expectedChunks, gotTotal, len(chunks))

		// NOTE: re-enable when time permits
		// if gotTotal > expectedChunks {
		// 	tlog.Logf("INFO: object %s has extra chunks: expected=%d got=%d (benign)\n",
		// 		objName, expectedChunks, gotTotal)
		// }

	case expectedChunks == 0:
		// Expect no chunk files
		tassert.Fatalf(m.t, len(chunks) == 0,
			"object %s: expected 0 chunk files, found %d",
			objName, len(chunks))
	default:
		// expectedChunks < 0: expect at least |expectedChunks| chunk files
		minExpected := -expectedChunks
		tassert.Fatalf(m.t, len(chunks) >= minExpected,
			"object %s: expected at least %d chunk files, found %d",
			objName, minExpected, len(chunks))
	}
}

func getObjToTemp(t *testing.T, proxyURL string, bck cmn.Bck, objName string) string {
	t.Helper()
	dir := t.TempDir() // is auto-removed by stdlib
	tmp := filepath.Join(dir, objName)

	err := cos.CreateDir(filepath.Dir(tmp))
	tassert.CheckFatal(t, err)

	f, err := os.Create(tmp)
	tassert.CheckFatal(t, err)
	defer f.Close()

	bp := tools.BaseAPIParams(proxyURL)
	_, err = api.GetObject(bp, bck, objName, &api.GetArgs{Writer: f})
	tassert.CheckFatal(t, err)
	return tmp
}

func corruptSingleBitInFile(m *ioContext, objName string, eced bool) {
	m.t.Helper()

	var (
		fqn string
		b   = []byte{0}
	)

	switch {
	case eced:
		fqn = m.findObjOnDisk(m.bck, objName)
	case m.chunksConf != nil && m.chunksConf.multipart:
		fqns := m.findObjChunksOnDisk(m.bck, objName)
		tassert.Fatalf(m.t, len(fqns) > 0, "no chunks found for %s", objName)
		fqn = fqns[rand.IntN(len(fqns))]
	default:
		fqn = m.findObjOnDisk(m.bck, objName)
	}

	fi, err := os.Stat(fqn)

	tassert.CheckFatal(m.t, err)
	off := rand.Int64N(fi.Size())
	file, err := os.OpenFile(fqn, os.O_RDWR, cos.PermRWR)
	tassert.CheckFatal(m.t, err)

	_, err = file.Seek(off, 0)
	tassert.CheckFatal(m.t, err)

	_, err = file.Read(b)
	tassert.CheckFatal(m.t, err)

	bit := rand.IntN(8)
	b[0] ^= 1 << bit
	_, err = file.Seek(off, 0)
	tassert.CheckFatal(m.t, err)

	_, err = file.Write(b)
	tassert.CheckFatal(m.t, err)

	file.Close()
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

// randomize buckets' namespaces
func genBucketNs() cmn.Ns {
	s := os.Getenv(env.TestRandNs)
	if s == "" {
		return cmn.NsGlobal
	}
	gen, err := strconv.ParseBool(s)
	debug.AssertNoErr(err)
	if !gen {
		return cmn.NsGlobal
	}
	return cmn.Ns{Name: cos.GenTie()}
}

// TODO -- FIXME:
// - use isErrNotFound() across the board
// - unify; remove all strings.Contains("not found") and similar
func isErrNotFound(err error) bool {
	if err == nil {
		return false
	}
	herr := cmn.AsErrHTTP(err)
	return herr != nil && herr.Status == http.StatusNotFound
}
