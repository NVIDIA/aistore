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
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/housekeep/housekeeper"
	"github.com/NVIDIA/aistore/housekeep/lru"
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
	xactRebBase struct {
		cmn.XactBase
		runnerCnt int
		confirmCh chan struct{}
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

	taskState struct {
		Err    error       `json:"error"`
		Result interface{} `json:"res"`
	}
	xactBckListTask struct {
		cmn.XactBase
		res     atomic.Pointer
		t       *targetrunner
		bucket  string
		msg     *cmn.SelectMsg
		ctx     context.Context
		isLocal bool
		cached  bool
	}

	xactionEntry interface {
		Start(id int64) error // supposed to start an xaction, will be called when entry is stored to into registry
		Kind() string
		Get() cmn.Xact
		Stats() stats.XactStats
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

func (xact *xactRebBase) Description() string   { return "base for rebalance xactions" }
func (xact *xactGlobalReb) Description() string { return "responsible for cluster-wide rebalancing" }
func (xact *xactGlobalReb) String() string {
	return fmt.Sprintf("%s, Smap v%d", xact.xactRebBase.String(), xact.smapVersion)
}
func (xact *xactLocalReb) Description() string {
	return "responsible for the mountpath-added and mountpath-enabled events that are handled locally within (and by) each storage target"
}
func (xact *xactEvictDelete) Description() string {
	return "responsible for evicting or deleting objects"
}
func (xact *xactElection) Description() string {
	return "responsible for election of a new primary proxy in case of failure of previous one"
}
func (xact *xactBckListTask) Description() string {
	return "asynchronous bucket list task"
}

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
func (xact *xactPrefetch) Description() string {
	return "responsible for prefetching a flexibly-defined list or range of objects from any given Cloud bucket"
}

func newXactions() *xactionsRegistry {
	xar := &xactionsRegistry{
		globalXacts: make(map[string]xactionGlobalEntry),
	}
	housekeeper.Housekeeper.Register(xar.cleanUpFinished)
	return xar
}

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
	running = !entry.Get().Finished()
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
		return map[int64]stats.XactStats{entry.Get().ID(): entry.Stats()}, nil
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

		return map[int64]stats.XactStats{entry.Get().ID(): entry.Stats()}, nil
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
	for _, stat := range r.globalXacts {
		matching[stat.Get().ID()] = stat.Stats()
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
		stat.FromXact(taskXact.xact, "")
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

		sts[entry.Get().ID()] = entry.Stats()
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

// GLOBAL RENEW METHODS

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

func (r *xactionsRegistry) Update(e xactionGlobalEntry) (stored bool, err error) {
	r.Lock()
	defer r.Unlock()

	previousEntry := r.globalXacts[e.Kind()]
	if previousEntry != nil && e.EndRenewOnPrevious(previousEntry) {
		e.ActOnPrevious(previousEntry)
		return false, nil
	}

	if err := e.Start(r.uniqueID()); err != nil {
		return false, err
	}

	r.globalXacts[e.Kind()] = e
	r.storeByID(e.Get().ID(), e)
	e.CleanupPrevious(previousEntry)
	return true, nil
}

func (r *xactionsRegistry) RenewGlobalXact(e xactionGlobalEntry) (stored bool, err error) {
	r.RLock()
	previousEntry := r.globalXacts[e.Kind()]

	if previousEntry != nil && e.EndRenewOnPrevious(previousEntry) {
		e.ActOnPrevious(previousEntry)
		r.RUnlock()
		return false, nil
	}

	r.RUnlock()
	return r.Update(e)
}

func (r *xactionsRegistry) renewPrefetch(tr *stats.Trunner) *xactPrefetch {
	e := &prefetchEntry{r: tr}
	stored, _ := r.RenewGlobalXact(e)
	entry := r.GetL(e.Kind()).(*prefetchEntry)

	if !stored && !entry.xact.Finished() {
		// previous prefetch is still running
		return nil
	}

	return entry.xact
}

func (r *xactionsRegistry) renewLRU() *lru.Xaction {
	e := &lruEntry{}
	stored, _ := r.RenewGlobalXact(e)
	entry := r.GetL(e.Kind()).(*lruEntry)

	if !stored && !entry.xact.Finished() {
		// previous LRU is still running
		return nil
	}

	return entry.xact
}

func (r *xactionsRegistry) renewGlobalReb(smapVersion int64, runnerCnt int) *xactGlobalReb {
	e := &globalRebEntry{smapVersion: smapVersion, runnerCnt: runnerCnt}
	stored, _ := r.RenewGlobalXact(e)
	entry := r.GetL(e.Kind()).(*globalRebEntry)

	if !stored {
		// previous global rebalance is still running
		return nil
	}
	return entry.xact
}

func (r *xactionsRegistry) renewLocalReb(runnerCnt int) *xactLocalReb {
	e := &localRebEntry{runnerCnt: runnerCnt}
	stored, _ := r.RenewGlobalXact(e)
	entry := r.GetL(e.Kind()).(*localRebEntry)

	if !stored {
		// previous local rebalance is still running
		return nil
	}
	return entry.xact
}

func (r *xactionsRegistry) renewElection(p *proxyrunner, vr *VoteRecord) *xactElection {
	e := &electionEntry{p: p, vr: vr}
	stored, _ := r.RenewGlobalXact(e)
	entry := r.GetL(e.Kind()).(*electionEntry)

	if !stored && !entry.xact.Finished() {
		// previous election is still running
		return nil
	}

	return entry.xact
}

func (r *xactionsRegistry) renewDownloader(t *targetrunner) (*downloader.Downloader, error) {
	e := &downloaderEntry{t: t}
	_, err := r.RenewGlobalXact(e)

	if err != nil {
		return nil, err
	}

	entry := r.GetL(e.Kind()).(*downloaderEntry)
	return entry.xact, nil
}

func (r *xactionsRegistry) renewEvictDelete(evict bool) *xactEvictDelete {
	e := &evictDeleteEntry{evict: evict}
	_, _ = r.RenewGlobalXact(e)
	entry := r.GetL(e.Kind()).(*evictDeleteEntry)
	return entry.xact
}

func (r *xactionsRegistry) renewBckListXact(ctx context.Context, t *targetrunner, bucket string, isLocal bool, msg *cmn.SelectMsg, cached bool) *xactBckListTask {
	id := msg.TaskID
	e := &bckListTaskEntry{
		id:      id,
		t:       t,
		bucket:  bucket,
		isLocal: isLocal,
		msg:     msg,
		ctx:     ctx,
		cached:  cached,
	}
	// TODO: duplicated ID - what to do? Just replace old one?
	_, ok := r.byID.Load(e.id)
	cmn.Assert(!ok)

	if err := e.Start(id); err != nil {
		return nil
	}

	r.taskXacts.Store(e.Kind(), e)
	r.storeByID(e.Get().ID(), e)
	return e.xact
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

func (r *xactBckListTask) IsGlobal() bool        { return false }
func (r *xactBckListTask) IsTask() bool          { return true }
func (r *xactBckListTask) IsMountpathXact() bool { return false }
func (r *xactBckListTask) UpdateResult(result interface{}, err error) {
	res := &taskState{Err: err}
	if err == nil {
		res.Result = result
	}
	r.res.Store(unsafe.Pointer(res))
	r.EndTime(time.Now())
}
func (r *xactBckListTask) Run() {
	walk := objwalk.NewWalk(r.ctx, r.bucket, r.isLocal, r.msg, r.t)
	if r.isLocal {
		r.UpdateResult(walk.LocalObjPage())
	} else {
		r.UpdateResult(walk.CloudObjPage(r.cached))
	}
}
