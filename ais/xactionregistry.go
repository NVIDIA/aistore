// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/lru"

	"github.com/NVIDIA/aistore/downloader"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
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
		smapVersion int64 // smap version on which this rebalance has started
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

	xactionEntry interface {
		Start(id int64) error // supposed to start an xaction, will be called when entry is stored to into registry
		Kind() string
		Get() cmn.Xact
		Stats() stats.XactStats
		IsGlobal() bool
	}
)

func (xact *xactRebBase) Description() string { return "base for rebalance xactions" }
func (xact *xactGlobalReb) Description() string {
	return "responsible for cluster-wide balance on the cluster-grow and cluster-shrink events"
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

// how often cleanup is called
const cleanupInterval = 10 * time.Minute

// how long xaction had to finish to be considered to be removed
const entryOldAge = 30 * time.Second

// watermarks for entries size
const entriesSizeHW = 200
const entriesSizeLW = 100

type xactionsRegistry struct {
	sync.RWMutex
	nextid      atomic.Int64
	globalXacts map[string]xactionGlobalEntry
	bucketXacts sync.Map // map[string]*bucketXactions
	byID        sync.Map // map[int64]xactionEntry
	byIDSize    *atomic.Int64
	lastCleanup *atomic.Int64
}

func newXactions() *xactionsRegistry {
	return &xactionsRegistry{
		globalXacts: make(map[string]xactionGlobalEntry),
		lastCleanup: atomic.NewInt64(time.Now().UnixNano()),
		byIDSize:    atomic.NewInt64(0),
	}
}

func (r *xactionsRegistry) abortAllBuckets(removeFromRegistry bool, buckets ...string) {
	wg := &sync.WaitGroup{}
	for _, b := range buckets {
		wg.Add(1)
		go func(b string) {
			defer wg.Done()
			val, ok := r.bucketXacts.Load(b)

			if !ok {
				glog.Warningf("Can't abort nonexistent xactions for bucket %s", b)
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

func (r *xactionsRegistry) rebStatus(global bool) (aborted, running bool) {
	rebType := localRebType
	xactKind := cmn.ActLocalReb
	if global {
		rebType = globalRebType
		xactKind = cmn.ActGlobalReb
	}

	pmarker := persistentMarker(rebType)
	_, err := os.Stat(pmarker)
	if err == nil {
		aborted = true
	}

	entry := r.GetL(xactKind)
	if entry == nil {
		return
	}

	running = !entry.Get().Finished()
	return
}

func (r *xactionsRegistry) uniqueID() int64 {
	return r.nextid.Inc()
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

func (r *xactionsRegistry) globalXactStats(kind string) ([]stats.XactStats, error) {
	if _, ok := cmn.ValidXact(kind); !ok {
		return nil, errors.New("unrecognized xaction " + kind)
	}

	entry := r.GetL(kind)
	if entry == nil {
		return nil, cmn.NewXactionNotFoundError(kind + " has not started yet")
	}

	return []stats.XactStats{entry.Stats()}, nil
}

func (r *xactionsRegistry) abortGlobalXact(kind string) {
	entry := r.GetL(kind)
	if entry == nil {
		return
	}

	if !entry.Get().Finished() {
		entry.Get().Abort()
	}
}

// Returns stats of xaction with given 'kind' on a given bucket
func (r *xactionsRegistry) bucketSingleXactStats(kind, bucket string) ([]stats.XactStats, error) {
	bucketXats, ok := r.getBucketsXacts(bucket)
	if !ok {
		return nil, cmn.NewXactionNotFoundError("xactions for bucket " + bucket + " don't exist; bucket might have been removed or not created")
	}

	entry := bucketXats.GetL(kind)
	if entry == nil {
		return nil, cmn.NewXactionNotFoundError(kind + " not found for bucket " + bucket + "; xaction might have not been started")
	}

	return []stats.XactStats{entry.Stats()}, nil
}

// Returns stats of all present xactions
func (r *xactionsRegistry) allXactsStats() []stats.XactStats {
	sts := make([]stats.XactStats, 0, 20)

	r.byID.Range(func(_, value interface{}) bool {
		entry := value.(xactionEntry)
		sts = append(sts, entry.Stats())
		return true
	})

	return sts
}

func (r *xactionsRegistry) getNonBucketSpecificStats(kind string) ([]stats.XactStats, error) {
	// no bucket and no kind - request for all xactions
	if kind == "" {
		return r.allXactsStats(), nil
	}

	global, err := cmn.XactKind.IsGlobalKind(kind)

	if err != nil {
		return nil, err
	}

	if global {
		return r.globalXactStats(kind)
	}

	return nil, fmt.Errorf("xaction %s is not a global xaction", kind)
}

// Returns stats of all xactions of a given bucket
func (r *xactionsRegistry) bucketAllXactsStats(bucket string) ([]stats.XactStats, error) {
	bucketsXacts, ok := r.getBucketsXacts(bucket)

	if !ok {
		return nil, fmt.Errorf("xactions for %s bucket not found", bucket)
	}

	return bucketsXacts.Stats(), nil
}

func (r *xactionsRegistry) getStats(kind, bucket string) ([]stats.XactStats, error) {
	if bucket == "" {
		// no bucket - either all xactions or a global xaction
		return r.getNonBucketSpecificStats(kind)
	}

	// both bucket and kind present - request for specific bucket's xaction
	if kind != "" {
		return r.bucketSingleXactStats(kind, bucket)
	}

	// bucket present and no kind - request for all available bucket's xactions
	return r.bucketAllXactsStats(bucket)
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

func (r *xactionsRegistry) renewGlobalReb(smapVersion int64, runnerCnt int) *xactRebBase {
	e := &globalRebEntry{smapVersion: smapVersion, runnerCnt: runnerCnt}
	stored, _ := r.RenewGlobalXact(e)
	entry := r.GetL(e.Kind()).(*globalRebEntry)

	if !stored {
		// previous global rebalance is still running
		return nil
	}

	return &entry.xact.xactRebBase
}

func (r *xactionsRegistry) renewLocalReb(runnerCnt int) *xactRebBase {
	e := &localRebEntry{runnerCnt: runnerCnt}
	stored, _ := r.RenewGlobalXact(e)
	entry := r.GetL(e.Kind()).(*localRebEntry)

	if !stored {
		// previous local rebalance is still running
		return nil
	}

	return &entry.xact.xactRebBase
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

func (r *xactionsRegistry) storeByID(id int64, entry xactionEntry) {
	r.byID.Store(id, entry)
	r.byIDSize.Inc()
	r.cleanUpFinished()
}

// FIXME: cleanup might not remove the most old entries for each kind
// creating 'holes' in xactions history. Fix should probably use heap
// or change in structure of byID
// cleanup is made when size of r.byID is bigger then entriesSizeHW
// but not more often than cleanupInterval
func (r *xactionsRegistry) cleanUpFinished() {
	if r.byIDSize.Load() < entriesSizeHW {
		return
	}

	last := r.lastCleanup.Load()
	startTime := time.Now()
	// last cleanup was earlier than cleanupInterval
	if time.Unix(0, last).Add(cleanupInterval).After(startTime) {
		return
	}

	if !r.lastCleanup.CAS(last, startTime.UnixNano()) {
		return
	}

	go func(startTime time.Time) {
		r.byID.Range(func(k, v interface{}) bool {
			entry := v.(xactionEntry)
			if !entry.Get().Finished() {
				return true
			}

			if entry.IsGlobal() {
				currentEntry := r.GetL(entry.Get().Kind())
				if currentEntry.Get().ID() == entry.Get().ID() {
					return true
				}
			} else {
				bXact, _ := r.getBucketsXacts(entry.Get().Bucket())
				currentEntry := bXact.GetL(entry.Get().Kind())
				if currentEntry.Get().ID() == entry.Get().ID() {
					return true
				}
			}

			if entry.Get().EndTime().Add(entryOldAge).Before(startTime) {
				// xaction has finished more then cleanupInterval ago
				r.byID.Delete(k)
				r.byIDSize.Dec()
				// stop loop if we reached LW
				return r.byIDSize.Load() < entriesSizeLW
			}
			return true
		})
	}(startTime)
}
