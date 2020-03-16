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
	Rebalance struct {
		RebBase
		cmn.NonmountpathXact
		SmapVersion int64 // Smap version on which this rebalance has started
	}
	Resilver struct {
		cmn.MountpathXact
		RebBase
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

		// Latest keeps recent `baseEntries` in double nested map.
		// At first level we keep the entries by kind and in the second level
		// we keep them by buckets.
		//
		// NOTE: first level is static and never changes so it does not require
		//  locking. Second level is bucket level so we need to use locking
		//  and we use `sync.Map` - it should be super fast though since
		//  the keys do not change frequently (this is when `sync.Map` excels).
		latest map[string]*sync.Map // kind => bck => (baseEntry|globalEntry|bucketEntry)

		byID          sync.Map // id (int64) => baseEntry
		byIDSize      atomic.Int64
		byIDTaskCount atomic.Int64
	}
)

var Registry *registry

func init() {
	Registry = newRegistry()
}

func (xact *RebBase) MarkDone()      { xact.wg.Done() }
func (xact *RebBase) WaitForFinish() { xact.wg.Wait() }

//
// misc methods
//

func (xact *RebBase) String() string {
	s := xact.XactBase.String()
	if xact.Bck().Name != "" {
		s += ", bucket " + xact.Bck().String()
	}
	return s
}
func (xact *Rebalance) String() string {
	return fmt.Sprintf("%s, Smap v%d", xact.RebBase.String(), xact.SmapVersion)
}
func (xact *Resilver) String() string {
	return xact.RebBase.String()
}

func (xact *Rebalance) AbortedAfter(d time.Duration) (aborted bool) {
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
	xar := &registry{
		latest: make(map[string]*sync.Map),
	}
	for kind := range cmn.XactType {
		xar.latest[kind] = &sync.Map{}
	}
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

func (r *registry) GetLatest(kind string, bckDefault ...*cluster.Bck) (baseEntry, bool) {
	cmn.AssertMsg(cmn.IsValidXaction(kind), kind)
	latest := r.latest[kind]
	if cmn.IsXactTypeBck(kind) {
		bck := bckDefault[0]
		if e, exists := latest.Load(bck.MakeUname("")); exists {
			return e.(baseEntry), true
		}
		return nil, false
	}

	e, exists := latest.Load("")
	if !exists {
		return nil, false
	}
	entry := e.(baseEntry)
	if len(bckDefault) == 0 || bckDefault[0] == nil {
		return entry, true
	}
	return entry, entry.Get().Bck().Equal(bckDefault[0].Bck)
}

// registry - private methods

func (r *registry) uniqueID() string {
	return cmn.GenUserID()
}

// AbortAllBuckets aborts all xactions that run with any of the provided bcks.
// It not only stops the "bucket xactions" but possibly "task xactions" which
// are running on given bucket.
func (r *registry) AbortAllBuckets(bcks ...*cluster.Bck) {
	wg := &sync.WaitGroup{}
	for _, bck := range bcks {
		wg.Add(1)
		go func(bck *cluster.Bck) {
			defer wg.Done()

			r.byID.Range(func(_, e interface{}) bool {
				xact := e.(baseEntry).Get()
				if !xact.Finished() && bck.Bck.Equal(xact.Bck()) {
					wg.Add(1)
					go func(xact cmn.Xact) {
						xact.Abort()
						wg.Done()
					}(xact)
				}
				return true
			})
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
		xact := e.(baseEntry).Get()
		if !xact.Finished() {
			if len(ty) > 0 && ty[0] != cmn.XactType[xact.Kind()] {
				return true
			}

			sleep = true
			wg.Add(1)
			go func() {
				xact.Abort()
				wg.Done()
			}()
		}
		return true
	})

	wg.Wait()
	return sleep
}

func (r *registry) AbortAllMountpathsXactions() {
	r.byID.Range(func(_, e interface{}) bool {
		xact := e.(baseEntry).Get()
		if !xact.Finished() && xact.IsMountpathXact() {
			xact.Abort()
		}
		return true
	})
}

func (r *registry) GlobalXactRunning(kind string) bool {
	entry, ok := r.GetLatest(kind)
	if !ok {
		return false
	}
	return !entry.Get().Finished()
}

func (r *registry) abortLatest(kind string, bck *cluster.Bck) (aborted bool) {
	entry, exists := r.GetLatest(kind, bck)
	if !exists {
		return false
	}
	if !entry.Get().Finished() {
		entry.Get().Abort()
		aborted = true
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
		for _, latest := range r.latest {
			latest.Range(func(_, e interface{}) bool {
				entry := e.(baseEntry)
				xact := entry.Get()
				matching[xact.ID()] = entry.Stats(xact)
				return true
			})
		}
		return matching, nil
	} else if bck == nil && kind != "" {
		if !cmn.IsValidXaction(kind) {
			return nil, cmn.NewXactionNotFoundError(kind)
		} else if cmn.IsXactTypeBck(kind) {
			return nil, fmt.Errorf("bucket xaction %q requires bucket", kind)
		}

		if onlyRecent {
			entry, exists := r.GetLatest(kind)
			if !exists {
				return map[string]stats.XactStats{}, nil
			}
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

		if onlyRecent {
			matching := make(map[string]stats.XactStats, 10)
			for kind := range r.latest {
				entry, exists := r.GetLatest(kind, bck)
				if exists {
					xact := entry.Get()
					matching[xact.ID()] = entry.Stats(xact)
				}
			}
			return matching, nil
		}
		return r.matchingXactsStats(func(xact cmn.Xact) bool {
			return cmn.IsXactTypeBck(xact.Kind()) && xact.Bck().Equal(bck.Bck)
		}), nil
	} else if bck != nil && kind != "" {
		if !bck.HasProvider() {
			return nil, fmt.Errorf("xaction %q: unknown provider for bucket %s", kind, bck)
		} else if !cmn.IsValidXaction(kind) {
			return nil, cmn.NewXactionNotFoundError(kind)
		}

		if onlyRecent {
			matching := make(map[string]stats.XactStats, 1)
			entry, exists := r.GetLatest(kind, bck)
			if exists {
				xact := entry.Get()
				matching[xact.ID()] = entry.Stats(xact)
			}
			return matching, nil
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
			r.AbortAllBuckets(bck)
		}
		aborted = true
	} else {
		r.abortLatest(kind, bck)
	}
	return
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
	if !cmn.IsXactTypeBck(entry.Kind()) {
		r.latest[entry.Kind()].Store("", entry)
	} else {
		bck := cluster.NewBckEmbed(entry.Get().Bck())
		cmn.Assert(bck.HasProvider())
		r.latest[entry.Kind()].Store(bck.MakeUname(""), entry)
	}
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
			entry, exists := r.GetLatest(entry.Kind())
			if exists && entry.Get().ID() == eID {
				return true
			}
		case cmn.XactTypeBck:
			bck := cluster.NewBckEmbed(xact.Bck())
			cmn.Assert(bck.HasProvider())
			entry, exists := r.GetLatest(entry.Kind(), bck)
			if exists && entry.Get().ID() == eID {
				return true
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

func (r *registry) renewBucketXaction(entry bucketEntry, bck *cluster.Bck) (bucketEntry, error) {
	r.RLock()
	if e, exists := r.GetLatest(entry.Kind(), bck); exists {
		prevEntry := e.(bucketEntry)
		if !prevEntry.Get().Finished() {
			if keep, err := entry.preRenewHook(prevEntry); keep || err != nil {
				r.RUnlock()
				return prevEntry, err
			}
		}
	}
	r.RUnlock()

	r.Lock()
	defer r.Unlock()
	var (
		running   = false
		prevEntry bucketEntry
	)
	if e, exists := r.GetLatest(entry.Kind(), bck); exists {
		prevEntry = e.(bucketEntry)
		if !prevEntry.Get().Finished() {
			running = true
			if keep, err := entry.preRenewHook(prevEntry); keep || err != nil {
				return prevEntry, err
			}
		}
	}

	if err := entry.Start(r.uniqueID(), bck.Bck); err != nil {
		return nil, err
	}
	r.storeEntry(entry)
	if running {
		entry.postRenewHook(prevEntry)
	}
	return entry, nil
}

func (r *registry) renewGlobalXaction(entry globalEntry) (globalEntry, bool, error) {
	r.RLock()
	if e, exists := r.GetLatest(entry.Kind()); exists {
		prevEntry := e.(globalEntry)
		if !prevEntry.Get().Finished() {
			if entry.preRenewHook(prevEntry) {
				r.RUnlock()
				return prevEntry, true, nil
			}
		}
	}
	r.RUnlock()

	r.Lock()
	defer r.Unlock()
	var (
		running   = false
		prevEntry globalEntry
	)
	if e, exists := r.GetLatest(entry.Kind()); exists {
		prevEntry = e.(globalEntry)
		if !prevEntry.Get().Finished() {
			running = true
			if entry.preRenewHook(prevEntry) {
				return prevEntry, true, nil
			}
		}
	}
	if err := entry.Start(r.uniqueID(), cmn.Bck{}); err != nil {
		return nil, false, err
	}
	r.storeEntry(entry)
	if running {
		entry.postRenewHook(prevEntry)
	}
	return entry, false, nil
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

func (r *registry) RenewRebalance(smapVersion, id int64, statRunner *stats.Trunner) *Rebalance {
	e := &rebalanceEntry{smapVersion: smapVersion, id: id, statRunner: statRunner}
	ee, keep, _ := r.renewGlobalXaction(e)
	entry := ee.(*rebalanceEntry)
	if keep { // previous global rebalance is still running
		return nil
	}
	return entry.xact
}

func (r *registry) RenewResilver() *Resilver {
	e := &resilverEntry{}
	ee, keep, _ := r.renewGlobalXaction(e)
	entry := ee.(*resilverEntry)
	cmn.Assert(!keep) // resilver must be always preempted
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
