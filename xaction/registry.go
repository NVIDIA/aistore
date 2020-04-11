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

	// Initial capacity of entries array in registry.
	registryEntriesCap = 1000
)

type (
	// If any of the section is set (mountpaths, bcks, all) the other all ignored.
	abortArgs struct {
		// Abort all xactions matching any of the buckets.
		bcks []*cluster.Bck

		// Abort all mountpath xactions.
		mountpaths bool

		// Abort all xactions. `ty` can be set so only
		// xactions matching type `ty` will be aborted.
		all bool
		ty  string
	}

	taskState struct {
		Result interface{} `json:"res"`
		Err    error       `json:"error"`
	}
	RebBase struct {
		cmn.XactBase
		wg *sync.WaitGroup
	}
	Rebalance struct {
		cmn.NonmountpathXact
		RebBase
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
		Start(bck cmn.Bck) error // starts an xaction, will be called when entry is stored into registry
		Kind() string
		Get() cmn.Xact
		Stats(xact cmn.Xact) stats.XactStats
	}
	XactQuery struct {
		ID         string
		Kind       string
		Bck        *cluster.Bck
		OnlyRecent bool
		Finished   bool // only finished xactions (FIXME: only works when `ID` is set)
	}
	registryEntries struct {
		mtx       sync.RWMutex
		entries   []baseEntry
		taskCount atomic.Int64
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

		// All entries in the registry. The entries are periodically cleaned up
		// to make sure that we don't keep old entries forever.
		entries *registryEntries
	}
)

// Registry is a global registry (see above) that keeps track of all running xactions
// In addition, the registry retains already finished xactions subject to lazy cleanup via `hk`
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
	return fmt.Sprintf("%s, %s", xact.RebBase.String(), xact.ID())
}
func (xact *Resilver) String() string {
	return xact.RebBase.String()
}

func (xact *Rebalance) AbortedAfter(dur time.Duration) (aborted bool) {
	sleep := cmn.MinDuration(dur, 500*time.Millisecond)
	for elapsed := time.Duration(0); elapsed < dur; elapsed += sleep {
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

func newRegistryEntries() *registryEntries {
	return &registryEntries{
		entries: make([]baseEntry, 0, registryEntriesCap),
	}
}

func (e *registryEntries) find(id string) baseEntry {
	e.mtx.RLock()
	defer e.mtx.RUnlock()
	for _, entry := range e.entries {
		if entry.Get().ID().Compare(id) == 0 {
			return entry
		}
	}
	return nil
}

func (e *registryEntries) forEach(exclusive bool, matcher func(entry baseEntry) bool) {
	if exclusive {
		e.mtx.Lock()
		defer e.mtx.Unlock()
	} else {
		e.mtx.RLock()
		defer e.mtx.RUnlock()
	}
	for _, entry := range e.entries {
		if !matcher(entry) {
			return
		}
	}
}

func (e *registryEntries) removeUnlocked(id string) {
	for idx, entry := range e.entries {
		if entry.Get().ID().String() == id {
			e.entries[idx] = e.entries[len(e.entries)-1]
			e.entries = e.entries[:len(e.entries)-1]

			if cmn.XactsMeta[entry.Kind()].Type == cmn.XactTypeTask {
				e.taskCount.Dec()
			}
			return
		}
	}
}

func (e *registryEntries) remove(id string) {
	e.mtx.Lock()
	e.removeUnlocked(id)
	e.mtx.Unlock()
}

func (e *registryEntries) insert(entry baseEntry) {
	e.mtx.Lock()
	e.entries = append(e.entries, entry)
	e.mtx.Unlock()

	// Increase after cleanup to not force trigger it. If it was just added, for
	// sure it didn't yet finish.
	if cmn.XactsMeta[entry.Kind()].Type == cmn.XactTypeTask {
		e.taskCount.Inc()
	}
}

func (e *registryEntries) len() int {
	e.mtx.RLock()
	defer e.mtx.RUnlock()
	return len(e.entries)
}

func newRegistry() *registry {
	xar := &registry{
		latest:  make(map[string]*sync.Map),
		entries: newRegistryEntries(),
	}
	for kind := range cmn.XactsMeta {
		xar.latest[kind] = &sync.Map{}
	}
	hk.Housekeeper.Register("xactions", xar.cleanUpFinished)
	return xar
}

func (r *registry) GetXact(id string) (baseEntry, bool) {
	cmn.Assert(id != "")
	entry := r.entries.find(id)
	return entry, entry != nil
}

func (r *registry) GetLatest(query XactQuery) (baseEntry, bool) {
	if query.ID != "" {
		entry := r.entries.find(query.ID)
		return entry, entry != nil
	}

	cmn.AssertMsg(cmn.IsValidXaction(query.Kind), query.Kind)
	latest := r.latest[query.Kind]
	if cmn.IsXactTypeBck(query.Kind) {
		if e, exists := latest.Load(query.Bck.MakeUname("")); exists {
			return e.(baseEntry), true
		}
		return nil, false
	}

	e, exists := latest.Load("")
	if !exists {
		return nil, false
	}
	entry := e.(baseEntry)
	if query.Bck == nil {
		return entry, true
	}
	return entry, entry.Get().Bck().Equal(query.Bck.Bck)
}

// AbortAllBuckets aborts all xactions that run with any of the provided bcks.
// It not only stops the "bucket xactions" but possibly "task xactions" which
// are running on given bucket.
func (r *registry) AbortAllBuckets(bcks ...*cluster.Bck) {
	r.abort(abortArgs{bcks: bcks})
}

// AbortAll waits until abort of all xactions is finished
// Every abort is done asynchronously
func (r *registry) AbortAll(tys ...string) {
	var ty string
	if len(tys) > 0 {
		ty = tys[0]
	}
	r.abort(abortArgs{all: true, ty: ty})
}

func (r *registry) AbortAllMountpathsXactions() {
	r.abort(abortArgs{mountpaths: true})
}

func (r *registry) abort(args abortArgs) {
	wg := &sync.WaitGroup{}
	r.entries.forEach(false /*exclusive*/, func(entry baseEntry) bool {
		xact := entry.Get()
		if xact.Finished() {
			return true
		}

		abort := false
		if args.mountpaths {
			if xact.IsMountpathXact() {
				abort = true
			}
		} else if len(args.bcks) > 0 {
			for _, bck := range args.bcks {
				if bck.Bck.Equal(xact.Bck()) {
					abort = true
					break
				}
			}
		} else if args.all {
			abort = true
			if args.ty != "" && args.ty != cmn.XactsMeta[xact.Kind()].Type {
				abort = false
			}
		}

		if abort {
			wg.Add(1)
			go func() {
				xact.Abort()
				wg.Done()
			}()
		}
		return true
	})
	wg.Wait()
}

func (r *registry) IsXactRunning(query XactQuery) bool {
	entry, ok := r.GetLatest(query)
	if !ok {
		return false
	}
	return !entry.Get().Finished()
}

func (r *registry) abortLatest(kind string, bck *cluster.Bck) (aborted bool) {
	entry, exists := r.GetLatest(XactQuery{Kind: kind, Bck: bck})
	if !exists {
		return false
	}
	if !entry.Get().Finished() {
		entry.Get().Abort()
		aborted = true
	}
	return
}

func (r *registry) matchingXactsStats(match func(xact cmn.Xact) bool) []stats.XactStats {
	sts := make([]stats.XactStats, 0, 20)
	r.entries.forEach(false /*exclusive*/, func(entry baseEntry) bool {
		if !match(entry.Get()) {
			return true
		}
		sts = append(sts, entry.Stats(entry.Get()))
		return true
	})
	return sts
}

func (r *registry) GetStats(query XactQuery) ([]stats.XactStats, error) {
	if query.ID != "" {
		if !query.Finished {
			return r.matchingXactsStats(func(xact cmn.Xact) bool {
				return xact.ID().Compare(query.ID) == 0
			}), nil
		}
		return r.matchingXactsStats(func(xact cmn.Xact) bool {
			if xact.Kind() == cmn.ActRebalance {
				// Any rebalance after a given ID that finished and was not aborted
				return xact.ID().Compare(query.ID) >= 0 && xact.Finished() && !xact.Aborted()
			}
			return xact.ID().Compare(query.ID) == 0 && xact.Finished() && !xact.Aborted()
		}), nil
	} else if query.Bck == nil && query.Kind == "" {
		if !query.OnlyRecent {
			return r.matchingXactsStats(func(_ cmn.Xact) bool { return true }), nil
		}

		// Add the most recent xactions (both currently running and already finished)
		matching := make([]stats.XactStats, 0, 10)
		for _, latest := range r.latest {
			latest.Range(func(_, e interface{}) bool {
				entry := e.(baseEntry)
				matching = append(matching, entry.Stats(entry.Get()))
				return true
			})
		}
		return matching, nil
	} else if query.Bck == nil && query.Kind != "" {
		if !cmn.IsValidXaction(query.Kind) {
			return nil, cmn.NewXactionNotFoundError(query.Kind)
		} else if cmn.IsXactTypeBck(query.Kind) {
			return nil, fmt.Errorf("bucket xaction %q requires a bucket", query.Kind)
		}

		if query.OnlyRecent {
			entry, exists := r.GetLatest(XactQuery{Kind: query.Kind})
			if !exists {
				return []stats.XactStats{}, nil
			}
			xact := entry.Get()
			return []stats.XactStats{entry.Stats(xact)}, nil
		}
		return r.matchingXactsStats(func(xact cmn.Xact) bool {
			return xact.Kind() == query.Kind
		}), nil
	} else if query.Bck != nil && query.Kind == "" {
		if !query.Bck.HasProvider() {
			return nil, fmt.Errorf("xaction %q: unknown provider for bucket %s", query.Kind, query.Bck.Name)
		}

		if query.OnlyRecent {
			matching := make([]stats.XactStats, 0, 10)
			for kind := range r.latest {
				entry, exists := r.GetLatest(XactQuery{Kind: kind, Bck: query.Bck})
				if exists {
					matching = append(matching, entry.Stats(entry.Get()))
				}
			}
			return matching, nil
		}
		return r.matchingXactsStats(func(xact cmn.Xact) bool {
			return cmn.IsXactTypeBck(xact.Kind()) && xact.Bck().Equal(query.Bck.Bck)
		}), nil
	} else if query.Bck != nil && query.Kind != "" {
		if !query.Bck.HasProvider() {
			return nil, fmt.Errorf("xaction %q: unknown provider for bucket %s", query.Kind, query.Bck)
		} else if !cmn.IsValidXaction(query.Kind) {
			return nil, cmn.NewXactionNotFoundError(query.Kind)
		}

		if query.OnlyRecent {
			matching := make([]stats.XactStats, 0, 1)
			entry, exists := r.GetLatest(XactQuery{Kind: query.Kind, Bck: query.Bck})
			if exists {
				matching = append(matching, entry.Stats(entry.Get()))
			}
			return matching, nil
		}
		return r.matchingXactsStats(func(xact cmn.Xact) bool {
			return xact.Kind() == query.Kind && xact.Bck().Equal(query.Bck.Bck)
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
	entry := r.entries.find(id)
	if entry == nil {
		return nil
	}

	xact := entry.(baseEntry)
	if !xact.Get().Finished() {
		return fmt.Errorf("xaction %s(%s, %T) is running - duplicate ID?", xact.Kind(), id, xact.Get())
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("cleanup: removing xaction %s (ID %s)", xact.Get(), id)
	}
	r.entries.remove(id)
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
	r.entries.insert(entry)
}

// FIXME: cleanup might not remove the most old entries for each kind
// creating 'holes' in xactions history. Fix should probably use heap
// or change in structure of byID
// cleanup is made when size of r.byID is bigger then entriesSizeHW
// but not more often than cleanupInterval
func (r *registry) cleanUpFinished() time.Duration {
	startTime := time.Now()
	if r.entries.taskCount.Load() == 0 {
		if r.entries.len() <= entriesSizeHW {
			return cleanupInterval
		}
	}
	anyTaskDeleted := false
	r.entries.forEach(true /*exclusive*/, func(entry baseEntry) bool {
		var (
			xact = entry.Get()
			eID  = xact.ID()
		)

		if !xact.Finished() {
			return true
		}

		// if entry is type of task the task must be cleaned up always - no extra
		// checks besides it is finished at least entryOldAge ago.
		//
		// We need to check if the entry is not the most recent entry for
		// given kind. If it is we want to keep it anyway.
		switch cmn.XactsMeta[entry.Kind()].Type {
		case cmn.XactTypeGlobal:
			entry, exists := r.GetLatest(XactQuery{Kind: entry.Kind()})
			if exists && entry.Get().ID() == eID {
				return true
			}
		case cmn.XactTypeBck:
			bck := cluster.NewBckEmbed(xact.Bck())
			cmn.Assert(bck.HasProvider())
			entry, exists := r.GetLatest(XactQuery{Kind: entry.Kind(), Bck: bck})
			if exists && entry.Get().ID() == eID {
				return true
			}
		}

		if xact.EndTime().Add(entryOldAge).Before(startTime) {
			// xaction has finished more than entryOldAge ago
			r.entries.removeUnlocked(eID.String())
			if cmn.XactsMeta[entry.Kind()].Type == cmn.XactTypeTask {
				anyTaskDeleted = true
			}
			return true
		}
		return true
	})

	// free all memory taken by cleaned up tasks
	// Tasks like ListObjects ones may take up huge amount of memory, so they
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
	if e, exists := r.GetLatest(XactQuery{Kind: entry.Kind(), Bck: bck}); exists {
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
	if e, exists := r.GetLatest(XactQuery{Kind: entry.Kind(), Bck: bck}); exists {
		prevEntry = e.(bucketEntry)
		if !prevEntry.Get().Finished() {
			running = true
			if keep, err := entry.preRenewHook(prevEntry); keep || err != nil {
				return prevEntry, err
			}
		}
	}

	if err := entry.Start(bck.Bck); err != nil {
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
	if e, exists := r.GetLatest(XactQuery{Kind: entry.Kind()}); exists {
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
	if e, exists := r.GetLatest(XactQuery{Kind: entry.Kind()}); exists {
		prevEntry = e.(globalEntry)
		if !prevEntry.Get().Finished() {
			running = true
			if entry.preRenewHook(prevEntry) {
				return prevEntry, true, nil
			}
		}
	}

	if err := entry.Start(cmn.Bck{}); err != nil {
		return nil, false, err
	}
	r.storeEntry(entry)
	if running {
		entry.postRenewHook(prevEntry)
	}
	return entry, false, nil
}

func (r *registry) RenewLRU(id string) *lru.Xaction {
	e := &lruEntry{id: id}
	ee, keep, _ := r.renewGlobalXaction(e)
	entry := ee.(*lruEntry)
	if keep { // previous LRU is still running
		return nil
	}
	return entry.xact
}

func (r *registry) RenewRebalance(id int64, statRunner *stats.Trunner) *Rebalance {
	e := &rebalanceEntry{id: rebID(id), statRunner: statRunner}
	ee, keep, _ := r.renewGlobalXaction(e)
	entry := ee.(*rebalanceEntry)
	if keep { // previous global rebalance is still running
		return nil
	}
	return entry.xact
}

func (r *registry) RenewResilver(id string) *Resilver {
	e := &resilverEntry{id: id}
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
	if err := e.Start(bck.Bck); err != nil {
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
	if err := e.Start(bck.Bck); err != nil {
		return nil, err
	}
	r.storeEntry(e)
	return e.xact, nil
}
