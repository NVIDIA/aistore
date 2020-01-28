// Package xaction provides core functionality for the AIStore extended actions.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/fs"
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

	RebBase struct {
		cmn.XactBase
		confirmCh chan struct{}
		runnerCnt int
	}
	GlobalReb struct {
		RebBase
		cmn.NonmountpathXact
		SmapVersion int64 // Smap version on which this rebalance has started
	}
	LocalReb struct {
		cmn.MountpathXact
		RebBase
	}
	prefetch struct {
		cmn.XactBase
		cmn.NonmountpathXact
		r *stats.Trunner
	}
	evictDelete struct {
		cmn.NonmountpathXact
		cmn.XactBase
	}
	Election struct {
		cmn.NonmountpathXact
		cmn.XactBase
	}
	bckListTask struct {
		cmn.XactBase
		ctx context.Context
		res atomic.Pointer
		t   cluster.Target
		bck *cluster.Bck
		msg *cmn.SelectMsg
	}
	bckSummaryTask struct {
		cmn.XactBase
		ctx context.Context
		t   cluster.Target
		bck *cluster.Bck
		msg *cmn.SelectMsg
		res atomic.Pointer
	}
	entry interface {
		Start(id int64) error // supposed to start an xaction, will be called when entry is stored to into registry
		Kind() string
		Get() cmn.Xact
		Stats(xact cmn.Xact) stats.XactStats
		IsGlobal() bool
		IsTask() bool
	}
	registry struct {
		sync.RWMutex
		globalXacts   map[string]globalEntry
		taskXacts     sync.Map // map[string]xactionEntry
		bucketXacts   sync.Map // map[string]*bucketXactions; FIXME: bucket name (key) cannot be considered unique
		byID          sync.Map // map[int64]xactionEntry
		byIDSize      atomic.Int64
		byIDTaskCount atomic.Int64
	}
)

var Registry *registry

//
// global descriptions
//

func (xact *RebBase) NotifyDone()                { xact.confirmCh <- struct{}{} }
func (xact *RebBase) Description() string        { return "base for rebalance xactions" }
func (xact *GlobalReb) Description() string      { return "cluster-wide global rebalancing" }
func (xact *bckListTask) Description() string    { return "asynchronous bucket list task" }
func (xact *bckSummaryTask) Description() string { return "asynchronous bucket summary task" }
func (xact *evictDelete) Description() string    { return "evict or delete objects" }
func (xact *Election) Description() string       { return "elect new primary proxy/gateway" }
func (xact *LocalReb) Description() string {
	return "resilver local storage upon mountpath-change events"
}
func (xact *prefetch) Description() string {
	return "prefetch a list or a range of objects from Cloud bucket"
}

//
// misc methods
//
func (xact *prefetch) ObjectsCnt() int64 {
	v := xact.r.Core.Tracker[stats.PrefetchCount]
	v.RLock()
	defer v.RUnlock()
	return xact.r.Core.Tracker[stats.PrefetchCount].Value
}
func (xact *prefetch) BytesCnt() int64 {
	v := xact.r.Core.Tracker[stats.PrefetchCount]
	v.RLock()
	defer v.RUnlock()
	return xact.r.Core.Tracker[stats.PrefetchCount].Value
}

func (xact *RebBase) String() string {
	s := xact.XactBase.String()
	if xact.Bck().Name != "" {
		s += ", bucket " + xact.Bck().String()
	}
	return s
}
func (xact *GlobalReb) String() string {
	return fmt.Sprintf("%s(%d), Smap v%d", xact.RebBase.String(), xact.runnerCnt, xact.SmapVersion)
}
func (xact *LocalReb) String() string {
	return fmt.Sprintf("%s(%d)", xact.RebBase.String(), xact.runnerCnt)
}

func (xact *GlobalReb) AbortedAfter(d time.Duration) (aborted bool) {
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
// registry
//

func newXactions() *registry {
	xar := &registry{
		globalXacts: make(map[string]globalEntry),
	}
	hk.Housekeeper.Register("xactions", xar.cleanUpFinished)
	return xar
}

func (r *registry) GetTaskXact(id int64) entry {
	val, loaded := r.byID.Load(id)

	if loaded {
		return val.(entry)
	}
	return nil
}

func (r *registry) GetL(kind string) globalEntry {
	r.RLock()
	res := r.globalXacts[kind]
	r.RUnlock()
	return res
}

// registry - private methods

func (r *registry) AbortAllBuckets(removeFromRegistry bool, bcks ...*cluster.Bck) {
	wg := &sync.WaitGroup{}
	for _, bck := range bcks {
		wg.Add(1)
		go func(bck *cluster.Bck) {
			defer wg.Done()
			bckUname := bck.MakeUname("")
			val, ok := r.bucketXacts.Load(bckUname)
			if !ok {
				// nothing to abort
				return
			}
			bXacts := val.(*bucketXactions)
			bXacts.AbortAll()
			if removeFromRegistry {
				r.bucketXacts.Delete(bckUname)
			}
		}(bck)
	}

	wg.Wait()
}

// nolint:unused // implemented but currently no use case
func (r *registry) AbortAllGlobal() bool {
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
func (r *registry) AbortAll() bool {
	sleep := false
	wg := &sync.WaitGroup{}

	r.byID.Range(func(_, val interface{}) bool {
		entry := val.(entry)
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

func (r *registry) IsRebalancing(kind string) (aborted, running bool) {
	cmn.Assert(kind == cmn.ActGlobalReb || kind == cmn.ActLocalReb)
	pmarker := cmn.PersistentMarker(kind)
	if err := fs.Access(pmarker); err == nil {
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

func (r *registry) uniqueID() int64 {
	n, err := cmn.GenUUID64()
	cmn.AssertNoErr(err)
	return n
}

func (r *registry) StopMountpathXactions() {
	r.byID.Range(func(_, value interface{}) bool {
		entry := value.(entry)

		if entry.Get().IsMountpathXact() {
			if !entry.Get().Finished() {
				entry.Get().Abort()
			}
		}

		return true
	})
}

func (r *registry) AbortBucketXact(kind string, bck *cluster.Bck) {
	bckUname := bck.MakeUname("")
	val, ok := r.bucketXacts.Load(bckUname)
	if !ok {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("Can't abort nonexistent xaction for bucket %s", bck)
		}
		return
	}
	bucketsXacts := val.(*bucketXactions)
	entry := bucketsXacts.GetL(kind)
	if entry == nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("Can't abort nonexistent xaction for bucket %s", bck)
		}
		return
	}
	if !entry.Get().Finished() {
		entry.Get().Abort()
	}
}

func (r *registry) GlobalXactRunning(kind string) bool {
	entry := r.GetL(kind)
	if entry == nil {
		return false
	}
	return !entry.Get().Finished()
}

func (r *registry) globalXactStats(kind string, onlyRecent bool) (map[int64]stats.XactStats, error) {
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

func (r *registry) AbortGlobalXact(kind string) (aborted bool) {
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
func (r *registry) bucketSingleXactStats(kind string, bck *cluster.Bck,
	onlyRecent bool) (map[int64]stats.XactStats, error) {
	if onlyRecent {
		bucketXats, ok := r.getBucketsXacts(bck)
		if !ok {
			return nil, cmn.NewXactionNotFoundError("<any>, bucket=" + bck.Name)
		}
		entry := bucketXats.GetL(kind)
		if entry == nil {
			return nil, cmn.NewXactionNotFoundError(kind + ", bucket=" + bck.Name)
		}
		xact := entry.Get()
		return map[int64]stats.XactStats{xact.ID(): entry.Stats(xact)}, nil
	}
	return r.matchingXactsStats(func(xact cmn.Xact) bool {
		return xact.Bck().Equal(bck.Bck) && xact.Kind() == kind
	}), nil
}

// Returns stats of all present xactions
func (r *registry) allXactsStats(onlyRecent bool) map[int64]stats.XactStats {
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
		stat.FillFromXact(taskXact.xact)
		matching[taskXact.xact.ID()] = stat
		return true
	})

	return matching
}

func (r *registry) matchingXactsStats(xactMatches func(xact cmn.Xact) bool) map[int64]stats.XactStats {
	sts := make(map[int64]stats.XactStats, 20)

	r.byID.Range(func(_, value interface{}) bool {
		entry := value.(entry)
		if !xactMatches(entry.Get()) {
			return true
		}
		xact := entry.Get()
		sts[xact.ID()] = entry.Stats(xact)
		return true
	})

	return sts
}

func (r *registry) getNonBucketSpecificStats(kind string, onlyRecent bool) (map[int64]stats.XactStats, error) {
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

	return nil, fmt.Errorf("xaction %s is not global", kind)
}

// Returns stats of all xactions of a given bucket
func (r *registry) bucketAllXactsStats(bck *cluster.Bck, onlyRecent bool) map[int64]stats.XactStats {
	bucketsXacts, ok := r.getBucketsXacts(bck)

	// bucketsXacts is not present, bucket might have never existed
	// or has been removed, return empty result
	if !ok {
		if onlyRecent {
			return map[int64]stats.XactStats{}
		}
		return r.matchingXactsStats(func(xact cmn.Xact) bool {
			return xact.Bck().Equal(bck.Bck)
		})
	}

	return bucketsXacts.Stats()
}

func (r *registry) GetStats(kind string, bck *cluster.Bck, onlyRecent bool) (map[int64]stats.XactStats, error) {
	if bck == nil {
		// no bucket - either all xactions or a global xaction
		return r.getNonBucketSpecificStats(kind, onlyRecent)
	}
	if !bck.HasProvider() {
		return nil, fmt.Errorf("xaction %q: unknown provider for bucket %s", kind, bck.Name)
	}
	// both bucket and kind present - request for specific bucket's xaction
	if kind != "" {
		return r.bucketSingleXactStats(kind, bck, onlyRecent)
	}
	// bucket present and no kind - request for all available bucket's xactions
	return r.bucketAllXactsStats(bck, onlyRecent), nil
}

func (r *registry) DoAbort(kind string, bck *cluster.Bck) {
	// no bucket and no kind - request for all available xactions
	if bck == nil && kind == "" {
		r.AbortAll()
	}
	// bucket present and no kind - request for all available bucket's xactions
	if bck != nil && kind == "" {
		cmn.Assert(bck.HasProvider()) // TODO -- FIXME: remove
		r.AbortAllBuckets(false, bck)
	}
	// both bucket and kind present - request for specific bucket's xaction
	if bck != nil && kind != "" {
		cmn.Assert(bck.HasProvider()) // TODO -- FIXME: remove
		r.AbortBucketXact(kind, bck)
	}
	// no bucket, but kind present - request for specific global xaction
	if bck == nil && kind != "" {
		r.AbortGlobalXact(kind)
	}
}

func (r *registry) getBucketsXacts(bck *cluster.Bck) (xactions *bucketXactions, ok bool) {
	bckUname := bck.MakeUname("")
	val, ok := r.bucketXacts.Load(bckUname)
	if !ok {
		return nil, false
	}
	return val.(*bucketXactions), true
}

func (r *registry) BucketsXacts(bck *cluster.Bck) *bucketXactions {
	if !bck.HasProvider() {
		cmn.AssertMsg(false, "bucket '"+bck.Name+"' must have provider")
	}
	bckUname := bck.MakeUname("")

	// try loading first to avoid creating newBucketXactions()
	val, loaded := r.bucketXacts.Load(bckUname)
	if loaded {
		return val.(*bucketXactions)
	}
	val, _ = r.bucketXacts.LoadOrStore(bckUname, newBucketXactions(r))
	return val.(*bucketXactions)
}

func (r *registry) removeFinishedByID(id int64) error {
	item, ok := r.byID.Load(id)
	if !ok {
		return nil
	}

	xact := item.(entry)
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

func (r *registry) storeByID(id int64, entry entry) {
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
func (r *registry) cleanUpFinished() time.Duration {
	startTime := time.Now()
	if r.byIDTaskCount.Load() == 0 {
		if r.byIDSize.Inc() <= entriesSizeHW {
			return cleanupInterval
		}
	}
	anyTaskDeleted := false
	r.byID.Range(func(k, v interface{}) bool {
		var (
			entry = v.(entry)
			xact  = entry.Get()
			eID   = xact.ID()
			eKind = xact.Kind()
		)
		if !xact.Finished() {
			return true
		}

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
			bck := cluster.NewBckEmbed(xact.Bck())
			cmn.Assert(bck.HasProvider())
			bXact, _ := r.getBucketsXacts(bck)
			if bXact != nil {
				currentEntry := bXact.GetL(eKind)
				if currentEntry != nil && currentEntry.Get().ID() == eID {
					return true
				}
			}
		}

		if xact.EndTime().Add(entryOldAge).Before(startTime) {
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

func (r *registry) renewGlobalXaction(e globalEntry) (ee globalEntry, keep bool, err error) {
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
	running := previousEntry != nil && !previousEntry.Get().Finished()
	if running {
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

	if running {
		e.postRenewHook(previousEntry)
	}
	ee = e
	return
}

func (r *registry) RenewPrefetch(tr *stats.Trunner) *prefetch {
	e := &prefetchEntry{r: tr}
	ee, keep, _ := r.renewGlobalXaction(e)
	entry := ee.(*prefetchEntry)
	if keep { // previous prefetch is still running
		return nil
	}
	return entry.xact
}

func (r *registry) RenewLRU() *lru.Xaction {
	e := &lruEntry{}
	ee, keep, _ := r.renewGlobalXaction(e)
	entry := ee.(*lruEntry)
	if keep { // previous LRU is still running
		return nil
	}
	return entry.xact
}

func (r *registry) RenewGlobalReb(smapVersion, globRebID int64, runnerCnt int, statRunner *stats.Trunner) *GlobalReb {
	e := &globalRebEntry{smapVersion: smapVersion, globRebID: globRebID, runnerCnt: runnerCnt, statRunner: statRunner}
	ee, keep, _ := r.renewGlobalXaction(e)
	entry := ee.(*globalRebEntry)
	if keep { // previous global rebalance is still running
		return nil
	}
	return entry.xact
}

func (r *registry) RenewLocalReb(runnerCnt int) *LocalReb {
	e := &localRebEntry{runnerCnt: runnerCnt}
	ee, keep, _ := r.renewGlobalXaction(e)
	entry := ee.(*localRebEntry)
	if keep {
		// previous local rebalance is still running
		return nil
	}
	return entry.xact
}

func (r *registry) RenewElection() *Election {
	e := &electionEntry{}
	ee, keep, _ := r.renewGlobalXaction(e)
	entry := ee.(*electionEntry)
	if keep { // previous election is still running
		return nil
	}
	return entry.xact
}

func (r *registry) RenewDownloader(t cluster.Target, statsT stats.Tracker) (*downloader.Downloader, error) {
	e := &downloaderEntry{t: t, statsT: statsT}
	ee, _, err := r.renewGlobalXaction(e)
	if err != nil {
		return nil, err
	}
	entry := ee.(*downloaderEntry)
	return entry.xact, nil
}

func (r *registry) RenewEvictDelete(evict bool) *evictDelete {
	e := &evictDeleteEntry{evict: evict}
	ee, _, _ := r.renewGlobalXaction(e)
	entry := ee.(*evictDeleteEntry)
	return entry.xact
}

func (r *registry) RenewBckListXact(ctx context.Context, t cluster.Target, bck *cluster.Bck,
	msg *cmn.SelectMsg) (*bckListTask, error) {
	id := msg.TaskID
	if err := r.removeFinishedByID(id); err != nil {
		return nil, err
	}
	e := &bckListTaskEntry{
		ctx: ctx,
		t:   t,
		id:  id,
		bck: bck,
		msg: msg,
	}
	if err := e.Start(id); err != nil {
		return nil, err
	}
	r.taskXacts.Store(e.Kind(), e)
	r.storeByID(e.Get().ID(), e)
	return e.xact, nil
}

func (r *registry) RenewBckSummaryXact(ctx context.Context, t cluster.Target, bck *cluster.Bck,
	msg *cmn.SelectMsg) (*bckSummaryTask, error) {
	id := msg.TaskID
	if err := r.removeFinishedByID(id); err != nil {
		return nil, err
	}
	e := &bckSummaryTaskEntry{
		id:  id,
		ctx: ctx,
		t:   t,
		bck: bck,
		msg: msg,
	}
	if err := e.Start(id); err != nil {
		return nil, err
	}
	r.taskXacts.Store(e.Kind(), e)
	r.storeByID(e.Get().ID(), e)
	return e.xact, nil
}

//
// bckListTask
//

func (r *bckListTask) IsGlobal() bool        { return false }
func (r *bckListTask) IsTask() bool          { return true }
func (r *bckListTask) IsMountpathXact() bool { return false }

func (r *bckListTask) Run() {
	walk := objwalk.NewWalk(r.ctx, r.t, r.bck, r.msg)
	if r.bck.IsAIS() || r.msg.Cached {
		r.UpdateResult(walk.LocalObjPage())
	} else {
		r.UpdateResult(walk.CloudObjPage())
	}
}

func (r *bckListTask) UpdateResult(result interface{}, err error) {
	res := &taskState{Err: err}
	if err == nil {
		res.Result = result
	}
	r.res.Store(unsafe.Pointer(res))
	r.EndTime(time.Now())
}

func (r *bckListTask) Result() (interface{}, error) {
	ts := (*taskState)(r.res.Load())
	if ts == nil {
		return nil, errors.New("no result to load")
	}
	return ts.Result, ts.Err
}

//
// bckSummaryTask
//

func (r *bckSummaryTask) IsGlobal() bool        { return false }
func (r *bckSummaryTask) IsTask() bool          { return true }
func (r *bckSummaryTask) IsMountpathXact() bool { return false }

func (r *bckSummaryTask) Run() {
	var (
		buckets []*cluster.Bck
		bmd     = r.t.GetBowner().Get()
		cfg     = cmn.GCO.Get()
	)
	if r.bck.Name != "" {
		buckets = append(buckets, r.bck)
	} else {
		if r.bck.Provider == "" || cmn.IsProviderAIS(r.bck.Provider) {
			provider := cmn.ProviderAIS
			bmd.Range(&provider, nil, func(bck *cluster.Bck) bool {
				buckets = append(buckets, bck)
				return false
			})
		}
		if r.bck.Provider == "" || cmn.IsProviderCloud(r.bck.Provider, true /*acceptAnon*/) {
			provider := cfg.Cloud.Provider
			bmd.Range(&provider, nil, func(bck *cluster.Bck) bool {
				buckets = append(buckets, bck)
				return false
			})
		}
	}

	var (
		wg                = &sync.WaitGroup{}
		availablePaths, _ = fs.Mountpaths.Get()
		errCh             = make(chan error, len(buckets))
		summaries         = make(cmn.BucketsSummaries)
	)
	wg.Add(len(buckets))

	totalDisksSize, err := fs.GetTotalDisksSize()
	if err != nil {
		r.UpdateResult(nil, err)
		return
	}

	si, err := cluster.HrwTargetTask(uint64(r.msg.TaskID), r.t.GetSowner().Get())
	if err != nil {
		r.UpdateResult(nil, err)
		return
	}

	// When we are target which should not list CB we should only list cached objects.
	shouldListCB := si.ID() == r.t.Snode().ID() && !r.msg.Cached
	if !shouldListCB {
		r.msg.Cached = true
	}

	for _, bck := range buckets {
		go func(bck *cluster.Bck) {
			defer wg.Done()

			if err := bck.Init(r.t.GetBowner()); err != nil {
				errCh <- err
				return
			}

			summary := cmn.BucketSummary{
				Bck:            bck.Bck,
				TotalDisksSize: totalDisksSize,
			}

			if r.msg.Fast && (bck.IsAIS() || r.msg.Cached) {
				for _, mpathInfo := range availablePaths {
					path := mpathInfo.MakePath(fs.ObjectType, bck.Bck)
					size, err := ios.GetDirSize(path)
					if err != nil {
						errCh <- err
						return
					}
					fileCount, err := ios.GetFileCount(path)
					if err != nil {
						errCh <- err
						return
					}

					if bck.Props.Mirror.Enabled {
						copies := int(bck.Props.Mirror.Copies)
						size /= uint64(copies)
						fileCount = fileCount/copies + fileCount%copies
					}
					summary.Size += size
					summary.ObjCount += uint64(fileCount)
				}
			} else { // slow path
				var (
					list *cmn.BucketList
					err  error
				)

				for {
					walk := objwalk.NewWalk(context.Background(), r.t, bck, r.msg)
					if bck.IsAIS() {
						list, err = walk.LocalObjPage()
					} else {
						list, err = walk.CloudObjPage()
					}
					if err != nil {
						errCh <- err
						return
					}

					for _, v := range list.Entries {
						summary.Size += uint64(v.Size)

						// We should not include object count for cloud buckets
						// as other target will do that for us. We just need to
						// report the size on the disk.
						if bck.IsAIS() || (!bck.IsAIS() && (shouldListCB || r.msg.Cached)) {
							summary.ObjCount++
						}
					}

					if list.PageMarker == "" {
						break
					}

					list.Entries = nil
					r.msg.PageMarker = list.PageMarker
				}
			}

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

func (r *bckSummaryTask) UpdateResult(result interface{}, err error) {
	res := &taskState{Err: err}
	if err == nil {
		res.Result = result
	}
	r.res.Store(unsafe.Pointer(res))
	r.EndTime(time.Now())
}

func (r *bckSummaryTask) Result() (interface{}, error) {
	ts := (*taskState)(r.res.Load())
	if ts == nil {
		return nil, errors.New("no result to load")
	}
	return ts.Result, ts.Err
}

func init() {
	Registry = newXactions()
}
