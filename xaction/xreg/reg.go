// Package registry provides core functionality for the AIStore extended actions xreg.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/xaction"
)

const (
	// cleanup old (entries.entries)
	delOldInterval = 10 * time.Minute

	// separately, cleanup inactive (entries.active)
	delInactiveInterval = time.Minute

	// the interval to keep for history
	entryOldAge = 1 * time.Hour

	// initial capacity of registry entries
	entriesCap = 256

	// the number of entries to trigger (housekeeping) cleanup
	delOldThreshold = 300
)

type (
	BaseEntry interface {
		Start(bck cmn.Bck) error // starts an xaction, will be called when entry is stored into registry
		Kind() string
		Get() cluster.Xact
	}
	XactFilter struct {
		ID          string
		Kind        string
		Bck         *cluster.Bck
		OnlyRunning *bool
	}
	// Represents result of renewing given xaction.
	RenewRes struct {
		Entry BaseEntry // Depending on situation can be new or old entry.
		Err   error     // Error that occurred during renewal.
		IsNew bool      // true: a new entry has been created; false: old one returned
	}
	// Selects subset of xactions to abort.
	abortArgs struct {
		// run on `bcks` buckets
		bcks []*cluster.Bck
		// one of { XactTypeGlobal, XactTypeBck, XactTypeTask } enum
		ty string
		// mountpath xactions - see xaction.XactsDtor
		mountpaths bool
		// all or matching `ty` above, if defined
		all bool
	}
	taskState struct {
		Result interface{} `json:"res"`
		Err    error       `json:"error"`
	}
	entries struct {
		mtx     sync.RWMutex
		active  []BaseEntry // running entries - finished entries are gradually removed
		entries []BaseEntry
	}
	registry struct {
		mtx sync.RWMutex // lock for transactions
		// All entries in the xreg. The entries are periodically cleaned up
		// to make sure that we don't keep old entries forever.
		inactive    atomic.Int64
		entries     *entries
		bckXacts    map[string]BckFactory
		globalXacts map[string]GlobalFactory
	}
)

// default global registry that keeps track of all running xactions
// In addition, the registry retains already finished xactions subject to lazy cleanup via `hk`.
var defaultReg *registry

func init() {
	defaultReg = newRegistry()
	xaction.IncInactive = defaultReg.incInactive
}

func newRegistry() *registry {
	xar := &registry{
		entries:     newRegistryEntries(),
		bckXacts:    make(map[string]BckFactory, 10),
		globalXacts: make(map[string]GlobalFactory, 10),
	}
	hk.Reg("xactions-old", xar.hkDelOld)
	hk.Reg("xactions-inactive", xar.hkDelInactive)
	return xar
}

func Reset() { defaultReg = newRegistry() } // tests only

//////////////////////
// xaction registry //
//////////////////////

func GetXact(uuid string) (xact cluster.Xact) { return defaultReg.getXact(uuid) }

func (r *registry) getXact(uuid string) (xact cluster.Xact) {
	r.entries.forEach(func(entry BaseEntry) bool {
		x := entry.Get()
		if x != nil && x.ID().Compare(uuid) == 0 {
			xact = x
			return false
		}
		return true
	})
	return
}

func GetXactRunning(kind string) (xact cluster.Xact) { return defaultReg.GetXactRunning(kind) }

func (r *registry) GetXactRunning(kind string) (xact cluster.Xact) {
	onl := false
	entry := r.entries.find(XactFilter{Kind: kind, OnlyRunning: &onl})
	if entry != nil {
		xact = entry.Get()
	}
	return
}

func GetRunning(flt XactFilter) BaseEntry { return defaultReg.getRunning(flt) }

func (r *registry) getRunning(flt XactFilter) BaseEntry {
	onl := true
	flt.OnlyRunning = &onl
	entry := r.entries.find(flt)
	return entry
}

func GetLatest(flt XactFilter) BaseEntry { return defaultReg.getLatest(flt) }

func (r *registry) getLatest(flt XactFilter) BaseEntry {
	entry := r.entries.find(flt)
	return entry
}

func CheckBucketsBusy() (cause BaseEntry) {
	// These xactions have cluster-wide consequences: in general moving objects between targets.
	busyXacts := []string{cmn.ActMoveBck, cmn.ActCopyBck, cmn.ActETLBck, cmn.ActECEncode}
	for _, kind := range busyXacts {
		if entry := GetRunning(XactFilter{Kind: kind}); entry != nil {
			return cause
		}
	}

	return nil
}

// AbortAllBuckets aborts all xactions that run with any of the provided bcks.
// It not only stops the "bucket xactions" but possibly "task xactions" which
// are running on given bucket.

func AbortAllBuckets(bcks ...*cluster.Bck) { defaultReg.abortAllBuckets(bcks...) }

func (r *registry) abortAllBuckets(bcks ...*cluster.Bck) {
	r.abort(abortArgs{bcks: bcks})
}

// AbortAll waits until abort of all xactions is finished
// Every abort is done asynchronously
func AbortAll(tys ...string) { defaultReg.abortAll(tys...) }

func (r *registry) abortAll(tys ...string) {
	var ty string
	if len(tys) > 0 {
		ty = tys[0]
	}
	r.abort(abortArgs{all: true, ty: ty})
}

func AbortAllMountpathsXactions() { defaultReg.abortAllMountpathsXactions() }

func (r *registry) abortAllMountpathsXactions() { r.abort(abortArgs{mountpaths: true}) }

func DoAbort(kind string, bck *cluster.Bck) (aborted bool) { return defaultReg.doAbort(kind, bck) }

func (r *registry) doAbort(kind string, bck *cluster.Bck) (aborted bool) {
	if kind != "" {
		entry := r.getRunning(XactFilter{Kind: kind, Bck: bck})
		if entry == nil {
			return false
		}
		entry.Get().Abort()
		return true
	}
	if bck == nil {
		// No bucket and no kind - request for all available xactions.
		r.abortAll()
	} else {
		// Bucket present and no kind - request for all available bucket's xactions.
		r.abortAllBuckets(bck)
	}
	aborted = true
	return
}

func DoAbortByID(uuid string) (aborted bool) { return defaultReg.doAbortByID(uuid) }

func (r *registry) doAbortByID(uuid string) (aborted bool) {
	entry := r.getXact(uuid)
	if entry == nil || entry.Finished() {
		return false
	}
	entry.Abort()
	return true
}

func GetStats(flt XactFilter) ([]cluster.XactStats, error) { return defaultReg.getStats(flt) }

func (r *registry) getStats(flt XactFilter) ([]cluster.XactStats, error) {
	if flt.ID != "" {
		if flt.OnlyRunning == nil || (flt.OnlyRunning != nil && *flt.OnlyRunning) {
			return r.matchXactsStatsByID(flt.ID)
		}
		return r.matchingXactsStats(func(xact cluster.Xact) bool {
			if xact.Kind() == cmn.ActRebalance {
				// Any rebalance after a given ID that finished and was not aborted
				return xact.ID().Compare(flt.ID) >= 0 && xact.Finished() && !xact.Aborted()
			}
			return xact.ID().Compare(flt.ID) == 0 && xact.Finished() && !xact.Aborted()
		}), nil
	}
	if flt.Bck != nil || flt.Kind != "" {
		// Error checks
		if flt.Kind != "" && !xaction.IsValid(flt.Kind) {
			return nil, cmn.NewXactionNotFoundError(flt.Kind)
		}
		if flt.Bck != nil && !flt.Bck.HasProvider() {
			return nil, fmt.Errorf("xaction %q: unknown provider for bucket %s", flt.Kind, flt.Bck.Name)
		}

		// TODO: investigate how does the following fare against genericMatcher
		if flt.OnlyRunning != nil && *flt.OnlyRunning {
			matching := make([]cluster.XactStats, 0, 10)
			if flt.Kind == "" {
				for kind := range xaction.XactsDtor {
					entry := r.getRunning(XactFilter{Kind: kind, Bck: flt.Bck})
					if entry != nil {
						matching = append(matching, entry.Get().Stats())
					}
				}
			} else {
				entry := r.getRunning(XactFilter{Kind: flt.Kind, Bck: flt.Bck})
				if entry != nil {
					matching = append(matching, entry.Get().Stats())
				}
			}
			return matching, nil
		}
		return r.matchingXactsStats(flt.genericMatcher), nil
	}
	return r.matchingXactsStats(flt.genericMatcher), nil
}

func (r *registry) abort(args abortArgs) {
	r.entries.forEach(func(entry BaseEntry) bool {
		xact := entry.Get()
		if xact.Finished() {
			return true
		}
		abort := false
		if args.mountpaths {
			debug.AssertMsg(args.ty == "", args.ty)
			if xaction.IsMountpath(xact.Kind()) {
				abort = true
			}
		} else if len(args.bcks) > 0 {
			debug.AssertMsg(args.ty == "", args.ty)
			for _, bck := range args.bcks {
				if bck.Bck.Equal(xact.Bck()) {
					abort = true
					break
				}
			}
		} else if args.all {
			abort = args.ty == "" || args.ty == xaction.XactsDtor[xact.Kind()].Type
		}
		if abort {
			xact.Abort()
		}
		return true
	})
}

func (r *registry) matchingXactsStats(match func(xact cluster.Xact) bool) []cluster.XactStats {
	matchingEntries := make([]BaseEntry, 0, 20)
	r.entries.forEach(func(entry BaseEntry) bool {
		if !match(entry.Get()) {
			return true
		}
		matchingEntries = append(matchingEntries, entry)
		return true
	})
	// TODO: we cannot do this inside `forEach` because - nested locks
	sts := make([]cluster.XactStats, 0, len(matchingEntries))
	for _, entry := range matchingEntries {
		sts = append(sts, entry.Get().Stats())
	}
	return sts
}

func (r *registry) matchXactsStatsByID(xactID string) ([]cluster.XactStats, error) {
	matchedStat := r.matchingXactsStats(func(xact cluster.Xact) bool {
		return xact.ID().Compare(xactID) == 0
	})
	if len(matchedStat) == 0 {
		return nil, cmn.NewXactionNotFoundError("ID=" + xactID)
	}
	return matchedStat, nil
}

func (r *registry) incInactive() { r.inactive.Inc() }

func (r *registry) hkDelInactive() time.Duration {
	if r.inactive.Swap(0) == 0 {
		return delInactiveInterval
	}
	r.entries.mtx.Lock()
	r.entries.delInactive()
	r.entries.mtx.Unlock()
	return delInactiveInterval
}

func (r *registry) add(entry BaseEntry) { r.entries.add(entry) }

// FIXME: the logic here must be revisited and revised
func (r *registry) hkDelOld() time.Duration {
	var toRemove []string
	if r.entries.len() < delOldThreshold {
		return delOldInterval
	}
	now := time.Now()
	r.entries.forEach(func(entry BaseEntry) (ret bool) {
		var (
			xact = entry.Get()
			eID  = xact.ID()
			onl  bool
		)
		ret, onl = true, true
		if !xact.Finished() {
			return
		}
		// extra check if the entry is not the most recent one for
		// a given kind (if it is keep it anyway)
		flt := XactFilter{Kind: entry.Kind(), OnlyRunning: &onl}
		if xaction.XactsDtor[entry.Kind()].Type == xaction.XactTypeBck {
			flt.Bck = cluster.NewBckEmbed(xact.Bck())
		}
		if r.entries.findUnlocked(flt) == nil {
			return
		}
		if xact.EndTime().Add(entryOldAge).Before(now) {
			toRemove = append(toRemove, eID.String())
		}
		return
	})
	if len(toRemove) > 0 {
		r.entries.mtx.Lock()
		for _, id := range toRemove {
			err := r.entries.del(id)
			debug.AssertNoErr(err)
		}
		r.entries.mtx.Unlock()
	}
	return delOldInterval
}

func (r *registry) renewBckXact(entry BucketEntry, bck *cluster.Bck, uuids ...string) (res RenewRes) {
	var uuid string
	if len(uuids) != 0 {
		uuid = uuids[0]
	}
	r.mtx.RLock()
	if e := r.getRunning(XactFilter{ID: uuid, Kind: entry.Kind(), Bck: bck}); e != nil {
		prevEntry := e.(BucketEntry)
		if keep, err := entry.PreRenewHook(prevEntry); keep || err != nil {
			r.mtx.RUnlock()
			return RenewRes{Entry: prevEntry, Err: err, IsNew: false}
		}
	}
	r.mtx.RUnlock()

	r.mtx.Lock()
	defer r.mtx.Unlock()
	var (
		running   = false
		prevEntry BucketEntry
	)
	if e := r.getRunning(XactFilter{ID: uuid, Kind: entry.Kind(), Bck: bck}); e != nil {
		prevEntry = e.(BucketEntry)
		running = true
		if keep, err := entry.PreRenewHook(prevEntry); keep || err != nil {
			return RenewRes{Entry: prevEntry, Err: err, IsNew: false}
		}
	}

	if err := entry.Start(bck.Bck); err != nil {
		return RenewRes{Entry: nil, Err: err, IsNew: true}
	}
	r.add(entry)
	if running {
		entry.PostRenewHook(prevEntry)
	}
	return RenewRes{Entry: entry, Err: nil, IsNew: true}
}

func (r *registry) renewGlobalXaction(entry GlobalEntry) RenewRes {
	r.mtx.RLock()
	if e := r.getRunning(XactFilter{Kind: entry.Kind()}); e != nil {
		prevEntry := e.(GlobalEntry)
		if entry.PreRenewHook(prevEntry) {
			r.mtx.RUnlock()
			return RenewRes{Entry: prevEntry, Err: nil, IsNew: false}
		}
	}
	r.mtx.RUnlock()

	r.mtx.Lock()
	defer r.mtx.Unlock()
	var (
		running   = false
		prevEntry GlobalEntry
	)
	if e := r.getRunning(XactFilter{Kind: entry.Kind()}); e != nil {
		prevEntry = e.(GlobalEntry)
		running = true
		if entry.PreRenewHook(prevEntry) {
			return RenewRes{Entry: prevEntry, Err: nil, IsNew: false}
		}
	}

	if err := entry.Start(cmn.Bck{}); err != nil {
		return RenewRes{Entry: nil, Err: err, IsNew: true}
	}
	r.add(entry)
	if running {
		entry.PostRenewHook(prevEntry)
	}
	return RenewRes{Entry: entry, Err: nil, IsNew: true}
}

//////////////////////
// registry entries //
//////////////////////

func newRegistryEntries() *entries {
	return &entries{
		entries: make([]BaseEntry, 0, entriesCap),
	}
}

func (e *entries) findUnlocked(flt XactFilter) BaseEntry {
	if flt.OnlyRunning == nil {
		// walk in reverse as there is a greater chance
		// the one we are looking for is at the end
		for idx := len(e.entries) - 1; idx >= 0; idx-- {
			entry := e.entries[idx]
			if matchEntry(entry, flt) {
				return entry
			}
		}
		return nil
	}
	debug.AssertMsg(flt.Kind == "" || xaction.IsValid(flt.Kind), flt.Kind)
	for _, entry := range e.active {
		if !entry.Get().Finished() && matchEntry(entry, flt) {
			return entry
		}
	}
	return nil
}

func (e *entries) find(flt XactFilter) (entry BaseEntry) {
	e.mtx.RLock()
	entry = e.findUnlocked(flt)
	e.mtx.RUnlock()
	return
}

func (e *entries) forEach(matcher func(entry BaseEntry) bool) {
	e.mtx.RLock()
	defer e.mtx.RUnlock()
	for _, entry := range e.entries {
		if !matcher(entry) {
			return
		}
	}
}

// NOTE: is called under lock
func (e *entries) del(id string) error {
	for idx, entry := range e.entries {
		xact := entry.Get()
		if xact.ID().String() == id {
			if !xact.Finished() {
				return fmt.Errorf("cannot remove %s - is running", xact)
			}
			nlen := len(e.entries) - 1
			e.entries[idx] = e.entries[nlen]
			e.entries = e.entries[:nlen]
			break
		}
	}
	for idx, entry := range e.active {
		xact := entry.Get()
		if xact.ID().String() == id {
			debug.Assert(xact.Finished())
			nlen := len(e.active) - 1
			e.active[idx] = e.active[nlen]
			e.active = e.active[:nlen]
			break
		}
	}
	return nil
}

func (e *entries) delInactive() {
	l := len(e.active)
	for i := 0; i < l; i++ {
		entry := e.active[i]
		if !entry.Get().Finished() {
			continue
		}
		copy(e.active[i:], e.active[i+1:])
		i--
		l--
		e.active = e.active[:l]
	}
}

func (e *entries) add(entry BaseEntry) {
	e.mtx.Lock()
	e.active = append(e.active, entry)
	e.entries = append(e.entries, entry)
	e.mtx.Unlock()
}

func (e *entries) len() (l int) {
	e.mtx.RLock()
	l = len(e.entries)
	e.mtx.RUnlock()
	return
}

////////////////
// XactFilter //
////////////////

func (rxf XactFilter) genericMatcher(xact cluster.Xact) bool {
	condition := true
	if rxf.OnlyRunning != nil {
		condition = condition && !xact.Finished() == *rxf.OnlyRunning
	}
	if rxf.Kind != "" {
		condition = condition && xact.Kind() == rxf.Kind
	}
	if rxf.Bck != nil {
		condition = condition && xact.Bck().Equal(rxf.Bck.Bck)
	}
	return condition
}

func matchEntry(entry BaseEntry, flt XactFilter) (matches bool) {
	if flt.ID != "" {
		return entry.Get().ID().Compare(flt.ID) == 0
	}
	if entry.Kind() == flt.Kind {
		if flt.Bck == nil || flt.Bck.IsEmpty() {
			return true
		}
		if !flt.Bck.IsEmpty() && entry.Get().Bck().Equal(flt.Bck.Bck) {
			return true
		}
	}
	return false
}
