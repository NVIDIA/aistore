// Package xaction provides core functionality for the AIStore extended actions.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/housekeep/hk"
	"github.com/NVIDIA/aistore/housekeep/lru"
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
		wg *sync.WaitGroup
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
		msg *cmn.SelectMsg
	}
	bckSummaryTask struct {
		cmn.XactBase
		ctx context.Context
		t   cluster.Target
		msg *cmn.SelectMsg
		res atomic.Pointer
	}
	baseEntry interface {
		Start(id string, bck cmn.Bck) error // supposed to start an xaction, will be called when entry is stored to into registry
		Kind() string
		Get() cmn.Xact
		Stats(xact cmn.Xact) stats.XactStats
	}
	registry struct {
		sync.RWMutex

		// Helper map for keeping bucket related xactions
		// All xactions are stored in `byID`.
		bucketXacts sync.Map // bckUname (string) => *bucketXactions

		// Helper map for keeping latest entry for each kind.
		// All xactions are stored in `byID`.
		latest sync.Map // kind (string) => entry (baseEntry|bucketEntry|globalEntry)

		byID          sync.Map // id (int64) => baseEntry
		byIDSize      atomic.Int64
		byIDTaskCount atomic.Int64
	}
)

var Registry *registry

func init() {
	Registry = newRegistry()
}

//
// global descriptions
//

func (xact *RebBase) MarkDone()                  { xact.wg.Done() }
func (xact *RebBase) WaitForFinish()             { xact.wg.Wait() }
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
	return fmt.Sprintf("%s, Smap v%d", xact.RebBase.String(), xact.SmapVersion)
}
func (xact *LocalReb) String() string {
	return xact.RebBase.String()
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

func newRegistry() *registry {
	xar := &registry{}
	hk.Housekeeper.Register("xactions", xar.cleanUpFinished)
	return xar
}

func (r *registry) GetTaskXact(id string) (baseEntry, bool) {
	cmn.Assert(id != "")
	if e, ok := r.byID.Load(id); ok {
		return e.(baseEntry), true
	}
	return nil, false
}

func (r *registry) GetLatest(kind string) (baseEntry, bool) {
	cmn.Assert(kind != "")
	if e, ok := r.latest.Load(kind); ok {
		return e.(baseEntry), true
	}
	return nil, false
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

// AbortAll waits until abort of all xactions is finished
// Every abort is done asynchronously
func (r *registry) AbortAll(ty ...string) bool {
	sleep := false
	wg := &sync.WaitGroup{}

	r.byID.Range(func(_, e interface{}) bool {
		entry := e.(baseEntry)
		if !entry.Get().Finished() {
			if len(ty) > 0 && ty[0] != cmn.XactType[entry.Kind()] {
				return true
			}

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

func (r *registry) uniqueID() string {
	return cmn.GenUserID()
}

func (r *registry) AbortAllMountpathsXactions() {
	r.byID.Range(func(_, e interface{}) bool {
		entry := e.(baseEntry)
		if !entry.Get().Finished() {
			if entry.Get().IsMountpathXact() {
				entry.Get().Abort()
			}
		}
		return true
	})
}

func (r *registry) GlobalXactRunning(kind string) bool {
	e, ok := r.latest.Load(kind)
	if !ok {
		return false
	}
	return !e.(baseEntry).Get().Finished()
}

func (r *registry) abortByKind(kind string, bck *cluster.Bck) (aborted bool) {
	switch cmn.XactType[kind] {
	case cmn.XactTypeGlobal, cmn.XactTypeTask:
		e, ok := r.latest.Load(kind)
		if !ok {
			return false
		}
		entry := e.(baseEntry)
		if !entry.Get().Finished() {
			entry.Get().Abort()
			aborted = true
		}
	case cmn.XactTypeBck:
		cmn.Assert(bck != nil)
		bucketsXacts, ok := r.getBucketsXacts(bck)
		if !ok {
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("cannot abort nonexistent xaction for bucket %s", bck)
			}
			return
		}
		entry := bucketsXacts.GetL(kind)
		if entry == nil {
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("cannot abort nonexistent xaction for bucket %s", bck)
			}
			return
		}
		if !entry.Get().Finished() {
			entry.Get().Abort()
			aborted = true
		}
	default:
		cmn.Assert(false)
	}
	return
}

func (r *registry) matchingXactsStats(match func(xact cmn.Xact) bool) map[string]stats.XactStats {
	sts := make(map[string]stats.XactStats, 20)
	r.byID.Range(func(_, value interface{}) bool {
		entry := value.(baseEntry)
		if !match(entry.Get()) {
			return true
		}
		xact := entry.Get()
		sts[xact.ID()] = entry.Stats(xact)
		return true
	})
	return sts
}

func (r *registry) GetStats(kind string, bck *cluster.Bck, onlyRecent bool) (map[string]stats.XactStats, error) {
	if bck == nil && kind == "" {
		if !onlyRecent {
			return r.matchingXactsStats(func(_ cmn.Xact) bool { return true }), nil
		}

		// Add these xactions which are the most recent ones, even if they are finished
		matching := make(map[string]stats.XactStats, 10)
		r.latest.Range(func(_, e interface{}) bool {
			entry := e.(baseEntry)
			switch cmn.XactType[entry.Kind()] {
			case cmn.XactTypeGlobal, cmn.XactTypeTask:
				xact := entry.Get()
				matching[xact.ID()] = entry.Stats(xact)
			case cmn.XactTypeBck:
				bckXactions := e.(*bucketXactions)
				for _, stat := range bckXactions.Stats() {
					matching[stat.ID()] = stat
				}
			default:
				cmn.Assert(false)
			}
			return true
		})
		return matching, nil
	} else if bck == nil && kind != "" {
		if !cmn.IsValidXaction(kind) {
			return nil, cmn.NewXactionNotFoundError(kind)
		} else if cmn.XactType[kind] == cmn.XactTypeBck {
			return nil, fmt.Errorf("bucket xaction %q requires bucket", kind)
		}

		if onlyRecent {
			e, ok := r.latest.Load(kind)
			if !ok {
				return map[string]stats.XactStats{}, nil
			}
			entry := e.(baseEntry)
			xact := entry.Get()
			return map[string]stats.XactStats{xact.ID(): entry.Stats(xact)}, nil
		}
		return r.matchingXactsStats(func(xact cmn.Xact) bool {
			return xact.Kind() == kind
		}), nil
	} else if bck != nil && kind == "" {
		if !bck.HasProvider() {
			return nil, fmt.Errorf("xaction %q: unknown provider for bucket %s", kind, bck.Name)
		}
		bucketsXacts, ok := r.getBucketsXacts(bck)
		if !ok {
			// bucketsXacts is not present, bucket might have never existed
			// or has been removed, return empty result
			if onlyRecent {
				return map[string]stats.XactStats{}, nil
			}
			return r.matchingXactsStats(func(xact cmn.Xact) bool {
				return xact.Bck().Equal(bck.Bck)
			}), nil
		}
		return bucketsXacts.Stats(), nil
	} else if bck != nil && kind != "" {
		if !bck.HasProvider() {
			return nil, fmt.Errorf("xaction %q: unknown provider for bucket %s", kind, bck)
		} else if !cmn.IsValidXaction(kind) {
			return nil, cmn.NewXactionNotFoundError(kind)
		}

		switch cmn.XactType[kind] {
		case cmn.XactTypeGlobal, cmn.XactTypeTask:
			break
		case cmn.XactTypeBck:
			if onlyRecent {
				bucketXacts, ok := r.getBucketsXacts(bck)
				if !ok {
					return nil, cmn.NewXactionNotFoundError(kind + ", bucket=" + bck.Bck.String())
				}
				entry := bucketXacts.GetL(kind)
				if entry == nil {
					return nil, cmn.NewXactionNotFoundError(kind + ", bucket=" + bck.Bck.String())
				}
				xact := entry.Get()
				return map[string]stats.XactStats{xact.ID(): entry.Stats(xact)}, nil
			}
		}
		return r.matchingXactsStats(func(xact cmn.Xact) bool {
			return xact.Kind() == kind && xact.Bck().Equal(bck.Bck)
		}), nil
	}

	cmn.Assert(false)
	return nil, nil
}

func (r *registry) DoAbort(kind string, bck *cluster.Bck) (aborted bool) {
	if kind == "" {
		if bck == nil {
			// No bucket and no kind - request for all available xactions.
			r.AbortAll()
		} else {
			// Bucket present and no kind - request for all available bucket's xactions.
			r.AbortAllBuckets(false, bck)
		}
		aborted = true
	} else {
		r.abortByKind(kind, bck)
	}
	return
}

func (r *registry) getBucketsXacts(bck *cluster.Bck) (xactions *bucketXactions, ok bool) {
	bckUname := bck.MakeUname("")
	if e, ok := r.bucketXacts.Load(bckUname); ok {
		return e.(*bucketXactions), true
	}
	return nil, false
}

func (r *registry) BucketsXacts(bck *cluster.Bck) *bucketXactions {
	if !bck.HasProvider() {
		cmn.AssertMsg(false, "bucket '"+bck.Name+"' must have provider")
	}
	bckUname := bck.MakeUname("")

	// try loading first to avoid creating newBucketXactions()
	e, loaded := r.bucketXacts.Load(bckUname)
	if loaded {
		return e.(*bucketXactions)
	}
	e, _ = r.bucketXacts.LoadOrStore(bckUname, newBucketXactions(r, bck.Bck))
	return e.(*bucketXactions)
}

func (r *registry) removeFinishedByID(id string) error {
	item, ok := r.byID.Load(id)
	if !ok {
		return nil
	}

	xact := item.(baseEntry)
	if !xact.Get().Finished() {
		return fmt.Errorf("xaction %s(%s, %T) is running - duplicate ID?", xact.Kind(), id, xact.Get())
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("cleanup: removing xaction %s (ID %s)", xact.Get(), id)
	}
	r.byID.Delete(id)
	r.byIDSize.Dec()
	return nil
}

func (r *registry) storeEntry(entry baseEntry) {
	r.latest.Store(entry.Kind(), entry)
	r.byID.Store(entry.Get().ID(), entry)

	// Increase after cleanup to not force trigger it. If it was just added, for
	// sure it didn't yet finish.
	if cmn.XactType[entry.Kind()] == cmn.XactTypeTask {
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
	r.byID.Range(func(k, e interface{}) bool {
		var (
			entry = e.(baseEntry)
			xact  = entry.Get()
			eID   = xact.ID()
		)

		if !xact.Finished() {
			return true
		}

		// if entry is type of task the task must be cleaned up always - no extra
		// checks besides it is finished at least entryOldAge ago.
		//
		// We need to check if the entry is not the most recent entry for
		// given kind. If it is we want to keep it anyway.
		switch cmn.XactType[entry.Kind()] {
		case cmn.XactTypeGlobal:
			e, ok := r.latest.Load(xact.Kind())
			if ok && e.(baseEntry).Get().ID() == eID {
				return true
			}
		case cmn.XactTypeBck:
			bck := cluster.NewBckEmbed(xact.Bck())
			cmn.Assert(bck.HasProvider())
			bXact, _ := r.getBucketsXacts(bck)
			if bXact != nil {
				currentEntry := bXact.GetL(xact.Kind())
				if currentEntry != nil && currentEntry.Get().ID() == eID {
					return true
				}
			}
		}

		if xact.EndTime().Add(entryOldAge).Before(startTime) {
			// xaction has finished more than entryOldAge ago
			r.byID.Delete(k)
			r.byIDSize.Dec()
			if cmn.XactType[entry.Kind()] == cmn.XactTypeTask {
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

func (r *registry) renewGlobalXaction(gEntry globalEntry) (newGEntry globalEntry, keep bool, err error) {
	if e, ok := r.latest.Load(gEntry.Kind()); ok {
		prevEntry := e.(globalEntry)
		if !prevEntry.Get().Finished() {
			gEntry.preRenewHook(prevEntry)
			newGEntry, keep = prevEntry, true
			return
		}
	}

	r.Lock()
	defer r.Unlock()
	var (
		running   = false
		prevEntry globalEntry
	)
	if e, ok := r.latest.Load(gEntry.Kind()); ok {
		prevEntry = e.(globalEntry)
		if !prevEntry.Get().Finished() {
			running = true
			if gEntry.preRenewHook(prevEntry) {
				newGEntry, keep = prevEntry, true
				return
			}
		}
	}
	if err = gEntry.Start(r.uniqueID(), cmn.Bck{}); err != nil {
		return
	}
	r.storeEntry(gEntry)
	if running {
		gEntry.postRenewHook(prevEntry)
	}
	newGEntry = gEntry
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

func (r *registry) RenewGlobalReb(smapVersion, globRebID int64, statRunner *stats.Trunner) *GlobalReb {
	e := &globalRebEntry{smapVersion: smapVersion, globRebID: globRebID, statRunner: statRunner}
	ee, keep, _ := r.renewGlobalXaction(e)
	entry := ee.(*globalRebEntry)
	if keep { // previous global rebalance is still running
		return nil
	}
	return entry.xact
}

func (r *registry) RenewLocalReb() *LocalReb {
	e := &localRebEntry{}
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
		msg: msg,
	}
	if err := e.Start(id, bck.Bck); err != nil {
		return nil, err
	}
	r.storeEntry(e)
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
		msg: msg,
	}
	if err := e.Start(id, bck.Bck); err != nil {
		return nil, err
	}
	r.storeEntry(e)
	return e.xact, nil
}
