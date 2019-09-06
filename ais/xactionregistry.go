// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/housekeep/hk"
	"github.com/NVIDIA/aistore/housekeep/lru"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/objwalk"
	"github.com/NVIDIA/aistore/stats"
)

const (
	// how often cleanup is called
	cleanupInterval = 10 * time.Minute

	// how long xaction had to finish to be considered to be removed
	entryOldAge = 1 * time.Hour

	// watermarks for entries size
	entriesSizeHW = 300
)

type (
	taskState struct {
		Result interface{} `json:"res"`
		Err    error       `json:"error"`
	}

	xactRebBase struct {
		cmn.XactBase
		confirmCh chan struct{}
		bucket    string
		runnerCnt int
	}
	xactGlobalReb struct {
		xactRebBase
		cmn.NonmountpathXact
		smapVersion int64 // Smap version on which this rebalance has started
	}
	xactLocalReb struct {
		xactRebBase
		cmn.MountpathXact
	}
	xactPrefetch struct {
		cmn.XactBase
		cmn.NonmountpathXact
		r *stats.Trunner
	}
	xactEvictDelete struct {
		cmn.XactBase
		cmn.NonmountpathXact
	}
	xactElection struct {
		cmn.XactBase
		cmn.NonmountpathXact
		proxyrunner *proxyrunner
		vr          *VoteRecord
	}
	xactBckListTask struct {
		cmn.XactBase
		ctx    context.Context
		res    atomic.Pointer
		t      *targetrunner
		bck    *cluster.Bck
		msg    *cmn.SelectMsg
		cached bool
	}
	xactBckSummaryTask struct {
		cmn.XactBase
		ctx    context.Context
		t      *targetrunner
		bck    *cluster.Bck
		msg    *cmn.SelectMsg
		cached bool
		res    atomic.Pointer
	}
	xactionEntry interface {
		Start(id int64) error // supposed to start an xaction, will be called when entry is stored to into registry
		Kind() string
		Get() cmn.Xact
		Stats(xact cmn.Xact) stats.XactStats
		IsGlobal() bool
		IsTask() bool
	}
	xactionsRegistry struct {
		sync.RWMutex
		globalXacts   map[string]xactionGlobalEntry
		taskXacts     sync.Map // map[string]xactionEntry
		bucketXacts   sync.Map // map[string]*bucketXactions
		byID          sync.Map // map[int64]xactionEntry
		byIDSize      atomic.Int64
		byIDTaskCount atomic.Int64
	}
)

//
// global descriptions
//

func (xact *xactRebBase) Description() string        { return "base for rebalance xactions" }
func (xact *xactGlobalReb) Description() string      { return "cluster-wide global rebalancing" }
func (xact *xactBckListTask) Description() string    { return "asynchronous bucket list task" }
func (xact *xactBckSummaryTask) Description() string { return "asynchronous bucket summary task" }
func (xact *xactEvictDelete) Description() string    { return "evict or delete objects" }
func (xact *xactElection) Description() string       { return "elect new primary proxy/gateway" }
func (xact *xactLocalReb) Description() string {
	return "resilver local storage upon mountpath-change events"
}
func (xact *xactPrefetch) Description() string {
	return "prefetch a list or a range of objects from Cloud bucket"
}

//
// misc methods
//
func (xact *xactPrefetch) ObjectsCnt() int64 {
	v := xact.r.Core.Tracker[stats.PrefetchCount]
	v.RLock()
	defer v.RUnlock()
	return xact.r.Core.Tracker[stats.PrefetchCount].Value
}
func (xact *xactPrefetch) BytesCnt() int64 {
	v := xact.r.Core.Tracker[stats.PrefetchCount]
	v.RLock()
	defer v.RUnlock()
	return xact.r.Core.Tracker[stats.PrefetchCount].Value
}

func (xact *xactRebBase) String() string {
	s := xact.XactBase.String()
	if xact.bucket != "" {
		s += ", bucket " + xact.bucket
	}
	return s
}
func (xact *xactGlobalReb) String() string {
	return fmt.Sprintf("%s(%d), Smap v%d", xact.xactRebBase.String(), xact.runnerCnt, xact.smapVersion)
}
func (xact *xactLocalReb) String() string {
	return fmt.Sprintf("%s(%d)", xact.xactRebBase.String(), xact.runnerCnt)
}

func (xact *xactGlobalReb) abortedAfter(d time.Duration) (aborted bool) {
	sleep := time.Second / 2
	steps := (d + sleep/2) / sleep
	for i := 0; i < int(steps); i++ {
		time.Sleep(sleep)
		if xact.Aborted() {
			return true
		}
	}
	return
}

//
// xactionsRegistry
//

func newXactions() *xactionsRegistry {
	xar := &xactionsRegistry{
		globalXacts: make(map[string]xactionGlobalEntry),
	}
	hk.Housekeeper.Register("xactions", xar.cleanUpFinished)
	return xar
}

func (r *xactionsRegistry) GetTaskXact(id int64) xactionEntry {
	val, loaded := r.byID.Load(id)

	if loaded {
		return val.(xactionEntry)
	}
	return nil
}

func (r *xactionsRegistry) GetL(kind string) xactionGlobalEntry {
	r.RLock()
	res := r.globalXacts[kind]
	r.RUnlock()
	return res
}

// xactionsRegistry - private methods

func (r *xactionsRegistry) abortAllBuckets(removeFromRegistry bool, buckets ...string) {
	wg := &sync.WaitGroup{}
	for _, b := range buckets {
		wg.Add(1)
		go func(b string) {
			defer wg.Done()
			val, ok := r.bucketXacts.Load(b)
			if !ok {
				// nothing to abort
				return
			}
			bXacts := val.(*bucketXactions)
			bXacts.AbortAll()
			if removeFromRegistry {
				r.bucketXacts.Delete(b)
			}
		}(b)
	}

	wg.Wait()
}

//nolint:unused
func (r *xactionsRegistry) abortAllGlobal() bool {
	sleep := false
	wg := &sync.WaitGroup{}

	for _, entry := range r.globalXacts {
		if !entry.Get().Finished() {
			wg.Add(1)
			sleep = true
			go func() {
				entry.Get().Abort()
				wg.Done()
			}()
		}
	}

	wg.Wait()
	return sleep
}

// AbortAll waits until abort of all xactions is finished
// Every abort is done asynchronously
func (r *xactionsRegistry) abortAll() bool {
	sleep := false
	wg := &sync.WaitGroup{}

	r.byID.Range(func(_, val interface{}) bool {
		entry := val.(xactionEntry)
		if !entry.Get().Finished() {
			sleep = true
			wg.Add(1)
			go func() {
				entry.Get().Abort()
				wg.Done()
			}()
		}

		return true
	})

	wg.Wait()
	return sleep
}

func (r *xactionsRegistry) isRebalancing(kind string) (aborted, running bool) {
	cmn.Assert(kind == cmn.ActGlobalReb || kind == cmn.ActLocalReb)
	pmarker := persistentMarker(kind)
	_, err := os.Stat(pmarker)
	if err == nil {
		aborted = true
	}
	entry := r.GetL(kind)
	if entry == nil {
		return
	}
	xact := entry.Get()
	running = !xact.Finished()
	if running {
		aborted = false
	}
	return
}

func (r *xactionsRegistry) uniqueID() int64 {
	n, err := cmn.GenUUID64()
	cmn.AssertNoErr(err)
	return n
}

func (r *xactionsRegistry) stopMountpathXactions() {
	r.byID.Range(func(_, value interface{}) bool {
		entry := value.(xactionEntry)

		if entry.Get().IsMountpathXact() {
			if !entry.Get().Finished() {
				entry.Get().Abort()
			}
		}

		return true
	})
}

func (r *xactionsRegistry) abortBucketXact(kind, bucket string) {
	val, ok := r.bucketXacts.Load(bucket)

	if !ok {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("Can't abort nonexistent xaction for bucket %s", bucket)
		}
		return
	}
	bucketsXacts := val.(*bucketXactions)
	entry := bucketsXacts.GetL(kind)
	if entry == nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("Can't abort nonexistent xaction for bucket %s", bucket)
		}
		return
	}

	if !entry.Get().Finished() {
		entry.Get().Abort()
	}
}

func (r *xactionsRegistry) globalXactRunning(kind string) bool {
	entry := r.GetL(kind)
	if entry == nil {
		return false
	}
	return !entry.Get().Finished()
}

func (r *xactionsRegistry) globalXactStats(kind string, onlyRecent bool) (map[int64]stats.XactStats, error) {
	if _, ok := cmn.ValidXact(kind); !ok {
		return nil, errors.New("unknown xaction " + kind)
	}

	entry := r.GetL(kind)
	if entry == nil {
		return nil, cmn.NewXactionNotFoundError(kind)
	}

	if onlyRecent {
		xact := entry.Get()
		return map[int64]stats.XactStats{xact.ID(): entry.Stats(xact)}, nil
	}

	return r.matchingXactsStats(func(xact cmn.Xact) bool {
		return xact.Kind() == kind || xact.ID() == entry.Get().ID()
	}), nil
}

func (r *xactionsRegistry) abortGlobalXact(kind string) (aborted bool) {
	entry := r.GetL(kind)
	if entry == nil {
		return
	}
	if !entry.Get().Finished() {
		entry.Get().Abort()
		aborted = true
	}
	return
}

// Returns stats of xaction with given 'kind' on a given bucket
func (r *xactionsRegistry) bucketSingleXactStats(kind, bucket string, onlyRecent bool) (map[int64]stats.XactStats, error) {
	if onlyRecent {
		bucketXats, ok := r.getBucketsXacts(bucket)
		if !ok {
			return nil, cmn.NewXactionNotFoundError("<any>, bucket=" + bucket)
		}

		entry := bucketXats.GetL(kind)
		if entry == nil {
			return nil, cmn.NewXactionNotFoundError(kind + ", bucket=" + bucket)
		}
		xact := entry.Get()
		return map[int64]stats.XactStats{xact.ID(): entry.Stats(xact)}, nil
	}

	return r.matchingXactsStats(func(xact cmn.Xact) bool {
		return xact.Bucket() == bucket && xact.Kind() == kind
	}), nil
}

// Returns stats of all present xactions
func (r *xactionsRegistry) allXactsStats(onlyRecent bool) map[int64]stats.XactStats {
	if !onlyRecent {
		return r.matchingXactsStats(func(_ cmn.Xact) bool { return true })
	}

	matching := make(map[int64]stats.XactStats)

	// add these xactions which are the most recent ones, even if they are finished
	r.RLock()
	for _, entry := range r.globalXacts {
		xact := entry.Get()
		matching[xact.ID()] = entry.Stats(xact)
	}
	r.RUnlock()

	r.bucketXacts.Range(func(_, val interface{}) bool {
		bckXactions := val.(*bucketXactions)

		for _, stat := range bckXactions.Stats() {
			matching[stat.ID()] = stat
		}
		return true
	})

	r.taskXacts.Range(func(_, val interface{}) bool {
		taskXact := val.(*bckListTaskEntry)

		stat := &stats.BaseXactStats{}
		stat.FillFromXact(taskXact.xact, "")
		matching[taskXact.xact.ID()] = stat
		return true
	})

	return matching
}

func (r *xactionsRegistry) matchingXactsStats(xactMatches func(xact cmn.Xact) bool) map[int64]stats.XactStats {
	sts := make(map[int64]stats.XactStats, 20)

	r.byID.Range(func(_, value interface{}) bool {
		entry := value.(xactionEntry)
		if !xactMatches(entry.Get()) {
			return true
		}
		xact := entry.Get()
		sts[xact.ID()] = entry.Stats(xact)
		return true
	})

	return sts
}

func (r *xactionsRegistry) getNonBucketSpecificStats(kind string, onlyRecent bool) (map[int64]stats.XactStats, error) {
	// no bucket and no kind - request for all xactions
	if kind == "" {
		return r.allXactsStats(onlyRecent), nil
	}

	global, err := cmn.XactKind.IsGlobalKind(kind)

	if err != nil {
		return nil, err
	}

	if global {
		return r.globalXactStats(kind, onlyRecent)
	}

	return nil, fmt.Errorf("xaction %s is not a global xaction", kind)
}

// Returns stats of all xactions of a given bucket
func (r *xactionsRegistry) bucketAllXactsStats(bucket string, onlyRecent bool) map[int64]stats.XactStats {
	bucketsXacts, ok := r.getBucketsXacts(bucket)

	// bucketsXacts is not present, bucket might have never existed
	// or has been removed, return empty result
	if !ok {
		if onlyRecent {
			return map[int64]stats.XactStats{}
		}
		return r.matchingXactsStats(func(xact cmn.Xact) bool {
			return xact.Bucket() == bucket
		})
	}

	return bucketsXacts.Stats()
}

func (r *xactionsRegistry) getStats(kind, bucket string, onlyRecent bool) (map[int64]stats.XactStats, error) {
	if bucket == "" {
		// no bucket - either all xactions or a global xaction
		return r.getNonBucketSpecificStats(kind, onlyRecent)
	}

	// both bucket and kind present - request for specific bucket's xaction
	if kind != "" {
		return r.bucketSingleXactStats(kind, bucket, onlyRecent)
	}

	// bucket present and no kind - request for all available bucket's xactions
	return r.bucketAllXactsStats(bucket, onlyRecent), nil
}

func (r *xactionsRegistry) doAbort(kind, bucket string) {
	// no bucket and no kind - request for all available xactions
	if bucket == "" && kind == "" {
		r.abortAll()
	}
	// bucket present and no kind - request for all available bucket's xactions
	if bucket != "" && kind == "" {
		r.abortAllBuckets(false, bucket)
	}
	// both bucket and kind present - request for specific bucket's xaction
	if bucket != "" && kind != "" {
		r.abortBucketXact(kind, bucket)
	}
	// no bucket, but kind present - request for specific global xaction
	if bucket == "" && kind != "" {
		r.abortGlobalXact(kind)
	}
}

func (r *xactionsRegistry) getBucketsXacts(bucket string) (xactions *bucketXactions, ok bool) {
	val, ok := r.bucketXacts.Load(bucket)
	if !ok {
		return nil, false
	}
	return val.(*bucketXactions), true
}

func (r *xactionsRegistry) bucketsXacts(bucket string) *bucketXactions {
	// NOTE: Load and then LoadOrStore saves us creating new object with
	// newBucketXactions every time this function is called, putting additional,
	// unnecessary stress on GC
	val, loaded := r.bucketXacts.Load(bucket)

	if loaded {
		return val.(*bucketXactions)
	}

	val, _ = r.bucketXacts.LoadOrStore(bucket, newBucketXactions(r, bucket))
	return val.(*bucketXactions)
}

func (r *xactionsRegistry) removeFinishedByID(id int64) error {
	item, ok := r.byID.Load(id)
	if !ok {
		return nil
	}

	xact := item.(xactionEntry)
	if !xact.Get().Finished() {
		return fmt.Errorf("xaction %s(%d, %T) is running - duplicate ID?", xact.Kind(), id, xact.Get())
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("Found finished xaction %d with id %d. Deleting", xact.Get(), id)
	}
	r.byID.Delete(id)
	r.byIDSize.Dec()
	return nil
}

func (r *xactionsRegistry) storeByID(id int64, entry xactionEntry) {
	r.byID.Store(id, entry)

	// Increase after cleanup to not force trigger it. If it was just added, for
	// sure it didn't yet finish.
	if entry.IsTask() {
		r.byIDTaskCount.Inc()
	}
}

// FIXME: cleanup might not remove the most old entries for each kind
// creating 'holes' in xactions history. Fix should probably use heap
// or change in structure of byID
// cleanup is made when size of r.byID is bigger then entriesSizeHW
// but not more often than cleanupInterval
func (r *xactionsRegistry) cleanUpFinished() time.Duration {
	startTime := time.Now()
	if r.byIDTaskCount.Load() == 0 {
		if r.byIDSize.Inc() <= entriesSizeHW {
			return cleanupInterval
		}
	}
	anyTaskDeleted := false
	r.byID.Range(func(k, v interface{}) bool {
		entry := v.(xactionEntry)
		if !entry.Get().Finished() {
			return true
		}

		eID := entry.Get().ID()
		eKind := entry.Get().Kind()

		// if IsTask == true the task must be cleaned up always - no extra
		// checks besides it is finished at least entryOldAge ago.
		//
		// We need to check if the entry is not the most recent entry for
		// given kind. If it is we want to keep it anyway.
		if entry.IsGlobal() {
			currentEntry := r.GetL(eKind)
			if currentEntry != nil && currentEntry.Get().ID() == eID {
				return true
			}
		} else if !entry.IsTask() {
			bXact, _ := r.getBucketsXacts(entry.Get().Bucket())
			if bXact != nil {
				currentEntry := bXact.GetL(eKind)
				if currentEntry != nil && currentEntry.Get().ID() == eID {
					return true
				}
			}
		}

		if entry.Get().EndTime().Add(entryOldAge).Before(startTime) {
			// xaction has finished more than entryOldAge ago
			r.byID.Delete(k)
			r.byIDSize.Dec()
			if entry.IsTask() {
				anyTaskDeleted = true
			}
			return true
		}
		return true
	})

	// free all memory taken by cleaned up tasks
	// Tasks like ListBucket ones may take up huge amount of memory, so they
	// must be cleaned up as soon as possible
	if anyTaskDeleted {
		cmn.FreeMemToOS(time.Second)
	}
	return cleanupInterval
}

//
// renew methods
//

func (r *xactionsRegistry) renewGlobalXaction(e xactionGlobalEntry) (ee xactionGlobalEntry, keep bool, err error) {
	r.RLock()
	previousEntry := r.globalXacts[e.Kind()]

	if previousEntry != nil && !previousEntry.Get().Finished() {
		if e.preRenewHook(previousEntry) {
			r.RUnlock()
			ee, keep = previousEntry, true
			return
		}
	}
	r.RUnlock()

	r.Lock()
	defer r.Unlock()
	previousEntry = r.globalXacts[e.Kind()]
	if previousEntry != nil && !previousEntry.Get().Finished() {
		if e.preRenewHook(previousEntry) {
			ee, keep = previousEntry, true
			return
		}
	}
	if err = e.Start(r.uniqueID()); err != nil {
		return
	}
	r.globalXacts[e.Kind()] = e
	r.storeByID(e.Get().ID(), e)

	if previousEntry != nil && !previousEntry.Get().Finished() {
		e.postRenewHook(previousEntry)
	}
	ee = e
	return
}

func (r *xactionsRegistry) renewPrefetch(tr *stats.Trunner) *xactPrefetch {
	e := &prefetchEntry{r: tr}
	ee, keep, _ := r.renewGlobalXaction(e)
	entry := ee.(*prefetchEntry)
	if keep { // previous prefetch is still running
		return nil
	}
	return entry.xact
}

func (r *xactionsRegistry) renewLRU() *lru.Xaction {
	e := &lruEntry{}
	ee, keep, _ := r.renewGlobalXaction(e)
	entry := ee.(*lruEntry)
	if keep { // previous LRU is still running
		return nil
	}
	return entry.xact
}

func (r *xactionsRegistry) renewGlobalReb(smapVersion, globRebID int64, runnerCnt int) *xactGlobalReb {
	e := &globalRebEntry{smapVersion: smapVersion, globRebID: globRebID, runnerCnt: runnerCnt}
	ee, keep, _ := r.renewGlobalXaction(e)
	entry := ee.(*globalRebEntry)
	if keep { // previous global rebalance is still running
		return nil
	}
	return entry.xact
}

func (r *xactionsRegistry) renewLocalReb(runnerCnt int) *xactLocalReb {
	e := &localRebEntry{runnerCnt: runnerCnt}
	ee, keep, _ := r.renewGlobalXaction(e)
	entry := ee.(*localRebEntry)
	if keep {
		// previous local rebalance is still running
		return nil
	}
	return entry.xact
}

func (r *xactionsRegistry) renewElection(p *proxyrunner, vr *VoteRecord) *xactElection {
	e := &electionEntry{p: p, vr: vr}
	ee, keep, _ := r.renewGlobalXaction(e)
	entry := ee.(*electionEntry)
	if keep { // previous election is still running
		return nil
	}
	return entry.xact
}

func (r *xactionsRegistry) renewDownloader(t *targetrunner) (*downloader.Downloader, error) {
	e := &downloaderEntry{t: t}
	ee, _, err := r.renewGlobalXaction(e)
	if err != nil {
		return nil, err
	}
	entry := ee.(*downloaderEntry)
	return entry.xact, nil
}

func (r *xactionsRegistry) renewEvictDelete(evict bool) *xactEvictDelete {
	e := &evictDeleteEntry{evict: evict}
	ee, _, _ := r.renewGlobalXaction(e)
	entry := ee.(*evictDeleteEntry)
	return entry.xact
}

func (r *xactionsRegistry) renewBckListXact(ctx context.Context, t *targetrunner, bck *cluster.Bck, msg *cmn.SelectMsg, cached bool) (*xactBckListTask, error) {
	id := msg.TaskID
	if err := r.removeFinishedByID(id); err != nil {
		return nil, err
	}
	e := &bckListTaskEntry{
		ctx:    ctx,
		t:      t,
		id:     id,
		bck:    bck,
		msg:    msg,
		cached: cached,
	}
	if err := e.Start(id); err != nil {
		return nil, err
	}
	r.taskXacts.Store(e.Kind(), e)
	r.storeByID(e.Get().ID(), e)
	return e.xact, nil
}

func (r *xactionsRegistry) renewBckSummaryXact(ctx context.Context, t *targetrunner, bck *cluster.Bck, msg *cmn.SelectMsg, cached bool) (*xactBckSummaryTask, error) {
	id := msg.TaskID
	if err := r.removeFinishedByID(id); err != nil {
		return nil, err
	}
	e := &bckSummaryTaskEntry{
		id:     id,
		ctx:    ctx,
		t:      t,
		bck:    bck,
		msg:    msg,
		cached: cached,
	}
	if err := e.Start(id); err != nil {
		return nil, err
	}
	r.taskXacts.Store(e.Kind(), e)
	r.storeByID(e.Get().ID(), e)
	return e.xact, nil
}

//
// xactBckListTask
//

func (r *xactBckListTask) IsGlobal() bool        { return false }
func (r *xactBckListTask) IsTask() bool          { return true }
func (r *xactBckListTask) IsMountpathXact() bool { return false }

func (r *xactBckListTask) Run() {
	walk := objwalk.NewWalk(r.ctx, r.t, r.bck, r.msg)
	if r.bck.IsAIS() {
		r.UpdateResult(walk.LocalObjPage())
	} else {
		r.UpdateResult(walk.CloudObjPage(r.cached))
	}
}

func (r *xactBckListTask) UpdateResult(result interface{}, err error) {
	res := &taskState{Err: err}
	if err == nil {
		res.Result = result
	}
	r.res.Store(unsafe.Pointer(res))
	r.EndTime(time.Now())
}

func (r *xactBckListTask) Result() (interface{}, error) {
	ts := (*taskState)(r.res.Load())
	if ts == nil {
		return nil, errors.New("no result to load")
	}
	return ts.Result, ts.Err
}

//
// xactBckSummaryTask
//

func (r *xactBckSummaryTask) IsGlobal() bool        { return false }
func (r *xactBckSummaryTask) IsTask() bool          { return true }
func (r *xactBckSummaryTask) IsMountpathXact() bool { return false }

func (r *xactBckSummaryTask) Run() {
	var (
		buckets []*cluster.Bck
		bmd     = r.t.bmdowner.get()
	)

	if r.bck.Name != "" {
		if err := r.bck.Init(r.t.bmdowner); err != nil {
			r.UpdateResult(nil, err)
			return
		}
		buckets = append(buckets, &cluster.Bck{Name: r.bck.Name, Provider: r.bck.Provider})
	} else {
		if r.bck.Provider == "" || cmn.IsProviderAIS(r.bck.Provider) {
			for name := range bmd.LBmap {
				buckets = append(buckets, &cluster.Bck{Name: name, Provider: cmn.AIS})
			}
		}
		if r.bck.Provider == "" || cmn.IsProviderCloud(r.bck.Provider) {
			for name := range bmd.CBmap {
				buckets = append(buckets, &cluster.Bck{Name: name, Provider: cmn.Cloud})
			}
		}
	}

	var (
		wg        = &sync.WaitGroup{}
		errCh     = make(chan error, len(buckets))
		summaries = make(cmn.BucketsSummaries)
	)
	wg.Add(len(buckets))

	// TODO: we should have general way to get size of the disk
	blocks, _, blockSize, err := ios.GetFSStats("/")
	if err != nil {
		r.UpdateResult(nil, err)
		return
	}
	totalDisksSize := blocks * uint64(blockSize)

	for _, bck := range buckets {
		go func(bck *cluster.Bck) {
			defer wg.Done()

			var (
				msg = &cmn.SelectMsg{Fast: false}
			)

			summary := cmn.BucketSummary{
				Name:     bck.Name,
				Provider: bck.Provider,
			}

			for {
				walk := objwalk.NewWalk(context.Background(), r.t, bck, msg)
				list, err := walk.LocalObjPage()
				if err != nil {
					errCh <- err
					return
				}

				for _, v := range list.Entries {
					summary.Size += uint64(v.Size)
					summary.ObjCount++
				}

				if list.PageMarker == "" {
					break
				}

				list.Entries = nil
				msg.PageMarker = list.PageMarker
			}

			summary.UsedPct = summary.Size * 100 / totalDisksSize
			summaries[bck.Name] = summary
		}(bck)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		r.UpdateResult(nil, err)
	}

	r.UpdateResult(summaries, nil)
}

func (r *xactBckSummaryTask) UpdateResult(result interface{}, err error) {
	res := &taskState{Err: err}
	if err == nil {
		res.Result = result
	}
	r.res.Store(unsafe.Pointer(res))
	r.EndTime(time.Now())
}

func (r *xactBckSummaryTask) Result() (interface{}, error) {
	ts := (*taskState)(r.res.Load())
	if ts == nil {
		return nil, errors.New("no result to load")
	}
	return ts.Result, ts.Err
}
