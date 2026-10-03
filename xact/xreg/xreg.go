// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/xact"
)

const (
	initialCap         = 256  // initial capacity ('all')
	initialCapActive   = 128  // ditto ('active')
	initialCapROActive = 192  // ditto ('roActive')
	keepOldThreshold   = 1024 // keep at least so many in history (Brief kinds excluded; see hkDelOld)

	waitPrevAborted = 2 * time.Second
	waitLimitedCoex = 5 * time.Second

	waitTerminalCleanup = cos.PollSleepLong // registry housekeeping when TOCTOU
)

type WPR int

const (
	WprAbort = iota + 1
	WprUse
	WprKeepAndStartNew
)

type (
	Renewable interface {
		New(args Args, bck *meta.Bck) Renewable
		Kind() string
		Get() core.Xact
		WhenPrevIsRunning(prevEntry Renewable) (action WPR, err error)
		Bucket() *meta.Bck
		UUID() string

		// NOTE:
		// Start constructs and initializes the xaction while holding the registry-wide renewMtx.
		// It must not wait, sleep, perform blocking channel operations, or call xact.GoRunW.
		// The renewal caller starts a newly constructed xaction after Renew returns.
		//
		// A successful renewal that creates a new entry returns before its Run goroutine is
		// started. The renewal caller must start that entry (e.g., via xact.GoRunW). Existing entries
		// returned by renewal must not be started again (see rns.IsNew).

		Start() error
	}
	// used in constructions
	Args struct {
		Custom any // Additional arguments that are specific for a given xact.
		UUID   string
		// proxy's ptime (QparamUnixTime), shared across all targets, see GenBEID
		PTime uint64
	}
	RenewBase struct {
		Bck *meta.Bck
		Args
	}
	// simplified non-JSON QueryMsg (internal AIS use)
	Flt struct {
		Bck         *meta.Bck
		OnlyRunning *bool
		ID          string
		Kind        string
		Buckets     []*meta.Bck
	}
)

// private
type (
	// Represents result of renewing given xact.
	RenewRes struct {
		Entry Renewable // Depending on situation can be new or old entry.
		Err   error     // Error that occurred during renewal.
		UUID  string    // "" if a new entry has been created, ID of the existing xaction otherwise
	}
	// Selects subset of xactions to abort.
	abortArgs struct {
		err    error
		kind   string      // criteria: all of a kind
		bcks   []*meta.Bck // buckets to apply
		scope  []int       // { ScopeG, ScopeB, ... } enum
		newreb bool        // this abort is triggered by a new rebalance (ie., select via AbortByReb() rather than a kind/bck/scope)
	}

	entries struct {
		active   []Renewable // accepting work; stopping and finished entries are lazily pruned by hkPruneActive
		roActive []Renewable // read-only copy; reused by the single `periodic` caller (see getAllRunning)
		all      []Renewable // running and finished, in chronological order (superset of `active`)
		mtx      sync.RWMutex
	}
	// xaction factories (by kind) and entries; finished entries are lazily
	// removed by the housekeeper (see hkPruneActive and hkDelOld)
	registry struct {
		bckXacts    map[string]Renewable
		nonbckXacts map[string]Renewable
		entries     entries
		finDelta    atomic.Int64
		renewMtx    sync.RWMutex
	}
)

// default global registry: running xactions and the history of finished ones
var dreg *registry

//////////////////////
// xaction registry //
//////////////////////

func Init() {
	dreg = newRegistry()
	xact.Init(dreg.incFinished)
}

func TestReset() { dreg = newRegistry() } // tests only

func newRegistry() (r *registry) {
	return &registry{
		entries: entries{
			all:      make([]Renewable, 0, initialCap),
			active:   make([]Renewable, 0, initialCapActive),
			roActive: make([]Renewable, 0, initialCapROActive),
		},
		bckXacts:    make(map[string]Renewable, 32),
		nonbckXacts: make(map[string]Renewable, 32),
	}
}

// Registry retention:
// - `all` contains every registered entry; `active` is a lazily pruned subset.
// - IsDone includes stopping: hkPruneActive removes these entries from `active`,
//   but history removal requires a nonzero EndTime.
// - Finished `Brief` entries become eligible after hk.OldAgeXshort (hk.OldAgeXshortV
//   at V(4, ModXs)) and are removed by hkPruneActive, independently of keepOldThreshold.
// - Other finished entries become eligible after hk.OldAgeX; hkDelOld removes
//   the oldest eligible entries while preserving keepOldThreshold non-brief
//   entries, including running and stopping ones. This is not a hard cap.
// - `finDelta` also re-arms brief-history aging while stopping or not-yet-aged
//   entries remain, so cleanup continues without further finish notifications
//   and uses the current verbosity on each pass.

func RegWithHK() {
	hk.Reg("x-old"+hk.NameSuffix, dreg.hkDelOld, 0)
	hk.Reg("x-prune-active"+hk.NameSuffix, dreg.hkPruneActive, 0)
}

func GetXact(uuid string) (core.Xact, error) { return dreg.getXact(uuid) }

func (r *registry) getXact(uuid string) (xctn core.Xact, _ error) {
	if err := xact.CheckValidUUID(uuid); err != nil {
		return nil, err
	}
	e := &r.entries
	e.mtx.RLock()
outer:
	for _, entries := range [][]Renewable{e.active, e.all} { // `all` is a superset: check fewer active first
		for i := len(entries) - 1; i >= 0; i-- {
			x := entries[i].Get()
			if x != nil && x.ID() == uuid {
				xctn = x
				break outer
			}
		}
	}
	e.mtx.RUnlock()
	return xctn, nil
}

func GetActiveXact(uuid string) (xctn core.Xact) {
	e := &dreg.entries
	e.mtx.RLock()
	xctn = e.getActiveXact(uuid)
	e.mtx.RUnlock()
	return
}

func (e *entries) getActiveXact(uuid string) core.Xact {
	for i := len(e.active) - 1; i >= 0; i-- {
		if x := e.active[i].Get(); x.ID() == uuid {
			return x
		}
	}
	return nil
}

func GetAllRunning(inout *core.AllRunningInOut, periodic bool) {
	dreg.entries.getAllRunning(inout, periodic)
}

func (e *entries) getAllRunning(inout *core.AllRunningInOut, periodic bool) {
	var (
		roActive []Renewable
		l        int
	)
	e.mtx.RLock()
	l = len(e.active)
	if l == 0 {
		e.mtx.RUnlock()
		return
	}
	if periodic && cap(e.roActive) >= l { // reuse existing
		roActive = e.roActive
		roActive = roActive[:l]
	} else {
		// (private here - `e.roActive` is written under the write lock only: see `_add` and `shrinkAll`)
		roActive = make([]Renewable, l)
	}
	copy(roActive, e.active)
	e.mtx.RUnlock()

	for _, entry := range roActive {
		var (
			xctn = entry.Get()
			k    = xctn.Kind()
		)
		if inout.Kind != "" && inout.Kind != k {
			continue
		}
		if !xctn.IsRunning() {
			continue
		}
		var (
			xqn    = xctn.Cname() // e.g. "make-n-copies[fGhuvvn7t]"
			isIdle bool
		)
		if inout.Idle != nil {
			if _, ok := xctn.(xact.Demand); ok {
				isIdle = xctn.IsIdle()
			}
		}
		if isIdle {
			inout.Idle = append(inout.Idle, xqn)
		} else {
			inout.Running = append(inout.Running, xqn)
		}
	}

	sort.Strings(inout.Running)
	sort.Strings(inout.Idle)
}

func GetRunning(flt *Flt) Renewable { return dreg.getRunning(flt) }

func (r *registry) getRunning(flt *Flt) (entry Renewable) {
	e := &r.entries
	e.mtx.RLock()
	entry = e.findRunning(flt)
	e.mtx.RUnlock()
	return
}

// NOTE: relies on the find() to walk in the newer --> older order
func GetLatest(flt *Flt) Renewable {
	entry := dreg.entries.find(flt)
	return entry
}

// AbortAllBuckets aborts all xactions that run with any of the provided bcks.
// It not only stops the "bucket xactions" but possibly "task xactions" which
// are running on given bucket.

func AbortAllBuckets(err error, bcks ...*meta.Bck) {
	dreg.abort(&abortArgs{bcks: bcks, err: err})
}

// AbortAll waits until abort of all xactions is finished
// Every abort is done asynchronously
func AbortAll(err error, scope ...int) {
	dreg.abort(&abortArgs{scope: scope, err: err})
}

func AbortKind(err error, kind string) {
	dreg.abort(&abortArgs{kind: kind, err: err})
}

func AbortByNewReb(err error) { dreg.abort(&abortArgs{err: err, newreb: true}) }

func DoAbort(flt *Flt, err error) {
	switch {
	case flt.ID != "":
		xctn, errV := dreg.getXact(flt.ID)
		if xctn == nil || errV != nil {
			return
		}
		debug.Func(func() {
			debug.Assertf(flt.Kind == "" || xctn.Kind() == flt.Kind, "wrong xaction kind: %s vs %q", xctn.Cname(), flt.Kind)
		})
		xctn.Abort(err)
	case flt.Kind != "" && flt.Bck != nil:
		dreg.abort(&abortArgs{kind: flt.Kind, bcks: []*meta.Bck{flt.Bck}, err: err})
	case flt.Kind != "":
		debug.AssertFunc(func() bool { return xact.IsValidKind(flt.Kind) }, flt.Kind)
		AbortKind(err, flt.Kind)
	case flt.Bck != nil:
		AbortAllBuckets(err, flt.Bck)
	default:
		AbortAll(err)
	}
}

func GetSnap(flt *Flt) ([]*core.Snap, error) {
	var onl bool
	if flt.OnlyRunning != nil {
		onl = *flt.OnlyRunning
	}
	if flt.ID != "" {
		xctn, err := dreg.getXact(flt.ID)
		if err != nil {
			return nil, err
		}
		if xctn != nil {
			if onl && xctn.IsDone() {
				return nil, cmn.NewErrXactNotFoundError("[only-running vs " + xctn.String() + "]")
			}
			if flt.Kind != "" && xctn.Kind() != flt.Kind {
				return nil, cmn.NewErrXactNotFoundError("[kind=" + flt.Kind + " vs " + xctn.String() + "]")
			}
			return []*core.Snap{xctn.Snap()}, nil
		}
		if onl || flt.Kind != apc.ActRebalance {
			return nil, cmn.NewErrXactNotFoundError("ID=" + flt.ID)
		}
		// not running rebalance: include all finished (but not aborted) ones
		// with ID at or _after_ the specified
		return dreg.matchingXactsStats(func(xctn core.Xact) bool {
			cmp := xact.CompareRebIDs(xctn.ID(), flt.ID)
			return cmp >= 0 && xctn.IsDone() && !xctn.IsAborted()
		}), nil
	}
	if flt.Bck != nil || flt.Kind != "" {
		// Error checks
		if flt.Kind != "" && !xact.IsValidKind(flt.Kind) {
			return nil, cmn.NewErrXactNotFoundError(flt.Kind)
		}
		if flt.Bck != nil && !flt.Bck.HasProvider() {
			return nil, fmt.Errorf("xaction %q: unknown provider for bucket %s", flt.Kind, flt.Bck.Name)
		}

		if onl {
			var matching []*core.Snap

			dreg.entries.mtx.RLock() // ----------
			matching = make([]*core.Snap, 0, min(len(dreg.entries.active), 8))
			if flt.Kind == "" {
				for kind := range xact.Table {
					entry := dreg.entries.findRunning(&Flt{Kind: kind, Bck: flt.Bck})
					if entry != nil {
						matching = append(matching, entry.Get().Snap())
					}
				}
			} else {
				for _, entry := range dreg.entries.active {
					if xctn := entry.Get(); flt.Matches(xctn) {
						matching = append(matching, xctn.Snap())
					}
				}
			}
			dreg.entries.mtx.RUnlock() // ----------

			return matching, nil
		}
		return dreg.matchingXactsStats(flt.Matches), nil
	}
	return dreg.matchingXactsStats(flt.Matches), nil
}

func (r *registry) abort(args *abortArgs) {
	e := &r.entries
	e.mtx.RLock()
	active := slices.Clone(e.active)
	e.mtx.RUnlock()

	for _, entry := range active {
		args.do(entry)
	}
}

func (args *abortArgs) do(entry Renewable) {
	xctn := entry.Get()
	if xctn.IsDone() {
		return
	}

	var abort bool
	switch {
	case args.newreb:
		debug.Assertf(args.scope == nil && args.kind == "", "scope %v, kind %q", args.scope, args.kind)
		_, dtor, err := xact.GetDescriptor(xctn.Kind())
		debug.AssertNoErr(err)
		if dtor.AbortByReb {
			abort = true
		}
	case len(args.bcks) > 0:
		debug.Assertf(args.scope == nil, "scope %v", args.scope)
		for _, bck := range args.bcks {
			if xctn.Bck() != nil && bck.Equal(xctn.Bck(), true /*same BID*/, true /*same backend*/) {
				abort = true
				break
			}
		}
		if abort && args.kind != "" {
			abort = args.kind == xctn.Kind()
		}
	case args.kind != "":
		debug.Assertf(args.scope == nil && len(args.bcks) == 0, "scope %v, bcks %v", args.scope, args.bcks)
		abort = args.kind == xctn.Kind()
	default:
		abort = args.scope == nil || xact.IsSameScope(xctn.Kind(), args.scope...)
	}

	if abort {
		xctn.Abort(args.err)
	}
}

func (r *registry) matchingXactsStats(match func(xctn core.Xact) bool) []*core.Snap {
	e := &r.entries

	e.mtx.RLock()
	matching := make([]Renewable, 0, 32)
	for _, entry := range e.all {
		if xctn := entry.Get(); xctn != nil && match(xctn) {
			matching = append(matching, entry)
		}
	}
	e.mtx.RUnlock()

	// NOTE: Snap() takes locks of its own - must be called with the registry unlocked
	sts := make([]*core.Snap, 0, len(matching))
	for _, entry := range matching {
		if xctn := entry.Get(); xctn != nil {
			sts = append(sts, xctn.Snap())
		}
	}
	return sts
}

func (r *registry) incFinished() { r.finDelta.Inc() }

// prune stopping and finished entries from `active`; age out Brief history
func (r *registry) hkPruneActive(now int64) time.Duration {
	if r.finDelta.Swap(0) == 0 {
		return hk.Jitter(hk.Prune2mIval, now)
	}
	// the same knob that un-quiets logs extends brief history
	age := hk.OldAgeXshort
	if cmn.Rom.V(4, cos.ModXs) {
		age = hk.OldAgeXshortV
	}
	var (
		e        = &r.entries
		toRemove []Renewable
		pending  bool
		tnow     = time.Now() // need calendar time
	)
	e.mtx.RLock()
	for _, entry := range e.all {
		xctn := entry.Get()
		if !xctn.IsDone() || !xact.Table[xctn.Kind()].Brief {
			continue
		}
		// (zero EndTime: stopping, not yet finished)
		if et := xctn.EndTime(); !et.IsZero() && tnow.Sub(et) >= age {
			toRemove = append(toRemove, entry)
		} else {
			pending = true
		}
	}
	e.mtx.RUnlock()

	e.mtx.Lock()
	e.del(toRemove)
	e.active = slices.DeleteFunc(e.active, func(entry Renewable) bool { return entry.Get().IsDone() })
	e.mtx.Unlock()

	if pending {
		r.finDelta.Inc() // re-arm: not aged yet
	}
	return hk.Jitter(hk.Prune2mIval, now)
}

// age out history at hk.OldAgeX while keeping at least `keepOldThreshold` entries
// (Brief kinds excluded - see hkPruneActive)
func (r *registry) hkDelOld(int64) time.Duration {
	var (
		toRemove    []Renewable
		numKeepMore int
		now         = time.Now() // need calendar time
	)

	r.entries.mtx.RLock()
	l := len(r.entries.all)
	for i := range l {
		if !xact.Table[r.entries.all[i].Kind()].Brief {
			numKeepMore++
		}
	}

	// older to newer
	if numKeepMore > keepOldThreshold {
		var cnt int
		for i := range l {
			entry := r.entries.all[i]
			xctn := entry.Get()
			if xact.Table[xctn.Kind()].Brief {
				continue
			}
			if endTime := xctn.EndTime(); !endTime.IsZero() {
				if since := now.Sub(endTime); since >= hk.OldAgeX {
					toRemove = append(toRemove, entry)
					cnt++
					if numKeepMore-cnt <= keepOldThreshold {
						break
					}
				}
			}
		}
	}
	shrink := r.entries.shrinkable()
	r.entries.mtx.RUnlock()

	// adaptive HK cadence based on finished-registry backlog
	var (
		d       = hk.DelOldIval
		ll      = len(toRemove)
		remains = numKeepMore - ll
	)
	switch {
	case remains > keepOldThreshold<<1:
		d = max(d>>2, hk.OldAgeXshort)
	case remains > keepOldThreshold:
		d = max(d>>1, hk.OldAgeXshort)
	}

	if ll == 0 && !shrink {
		return d
	}

	// cleanup
	r.entries.mtx.Lock()
	r.entries.del(toRemove)
	r.entries.shrinkAll()
	r.entries.mtx.Unlock()

	if ll == 0 {
		return d // shrink-only pass
	}
	return hk.Jitter(d, now.UnixNano())
}

func (r *registry) renewByID(entry Renewable, bck *meta.Bck) (rns RenewRes) {
	flt := Flt{ID: entry.UUID(), Kind: entry.Kind(), Bck: bck}
	rns = r._renewFlt(entry, &flt)
	rns.beingRenewed()
	return
}

func (r *registry) renew(entry Renewable, bck *meta.Bck, buckets ...*meta.Bck) (rns RenewRes) {
	flt := Flt{Kind: entry.Kind(), Bck: bck, Buckets: buckets}
	rns = r._renewFlt(entry, &flt)
	rns.beingRenewed()
	return
}

//////////////////////
// registry entries //
//////////////////////

// NOTE: the caller must take rlock
func (e *entries) findRunning(flt *Flt) Renewable {
	onl := true
	flt.OnlyRunning = &onl
	for _, entry := range e.active {
		if flt.Matches(entry.Get()) {
			return entry
		}
	}
	return nil
}

// internal use, special case: Flt{Kind: kind}; NOTE: the caller must take rlock
func (e *entries) findRunningKind(kind string) Renewable {
	for _, entry := range e.active {
		if entry.Kind() != kind {
			continue
		}
		xctn := entry.Get()
		if xctn.IsRunning() {
			return entry
		}
	}
	return nil
}

func (e *entries) find(flt *Flt) (entry Renewable) {
	e.mtx.RLock()
	entry = e.findUnlocked(flt)
	e.mtx.RUnlock()
	return
}

func (e *entries) findUnlocked(flt *Flt) Renewable {
	if flt.OnlyRunning != nil && *flt.OnlyRunning {
		return e.findRunning(flt)
	}
	// walk in reverse as there is a greater chance
	// the one we are looking for is at the end
	for idx := len(e.all) - 1; idx >= 0; idx-- {
		entry := e.all[idx]
		if flt.Matches(entry.Get()) {
			return entry
		}
	}
	return nil
}

// remove the specified entries from `all` and `active`; called under lock
func (e *entries) del(toRemove []Renewable) {
	if len(toRemove) == 0 {
		return
	}

	tmp := make(map[Renewable]struct{}, len(toRemove))
	for _, entry := range toRemove {
		if debug.ON() {
			xdel := entry.Get()
			debug.Assert(xdel.IsDone(), "expected ", xdel.String(), " finished or aborted: ", xdel.IsAborted())
		}
		tmp[entry] = struct{}{}
	}

	// (`slices.DeleteFunc` is order-preserving and zeroes the vacated tail)
	matches := func(entry Renewable) bool { _, ok := tmp[entry]; return ok }
	e.all = slices.DeleteFunc(e.all, matches)
	e.active = slices.DeleteFunc(e.active, matches)
}

// shrink only when the excess is large, so that steady-state churn does not re-allocate
func (e *entries) shrinkAll() {
	e.all = _shrink(e.all, initialCap)

	e.active = _shrink(e.active, initialCapActive)

	// cached scratch: size against current active, discard stale contents
	l := len(e.active)
	if _shrinkable(cap(e.roActive), l, initialCapROActive) {
		e.roActive = cos.ResetSliceCap(e.roActive[:0], max(initialCapROActive, l+l>>1))
	}
}

// `active` and Brief history are drained by hkPruneActive and may leave nothing for hkDelOld
// to remove - ask separately whether there's capacity to hand back; called under rlock
func (e *entries) shrinkable() bool {
	return _shrinkable(cap(e.all), len(e.all), initialCap) ||
		_shrinkable(cap(e.active), len(e.active), initialCapActive) ||
		_shrinkable(cap(e.roActive), len(e.active), initialCapROActive)
}

func _shrink(s []Renewable, dflt int) []Renewable {
	if !_shrinkable(cap(s), len(s), dflt) {
		return s // nothing to reclaim, or still mostly in use
	}
	// leave 50% headroom
	return cos.ResetSliceCap(s, max(dflt, len(s)+len(s)>>1))
}

func _shrinkable(c, l, dflt int) bool { return c > dflt && c > l<<1 }

// called under lock
func (e *entries) _add(entry Renewable) {
	e.active = append(e.active, entry)
	e.all = append(e.all, entry)

	// grow
	if cap(e.roActive) < len(e.active) {
		e.roActive = make([]Renewable, 0, len(e.active)+len(e.active)>>1)
	}
}

// LimitedCoexistence checks whether a given xaction that is about to start can, in fact, "coexist"
// with those that are currently running. It's a piece of logic designed to centralize all decision-making
// of that sort. Further comments below.

func LimitedCoexistence(tsi *meta.Snode, bck *meta.Bck, action string, otherBck ...*meta.Bck) (err error) {
	if cmn.Rom.Features().IsSet(feat.IgnoreLimitedCoexistence) {
		return
	}
	const sleep = time.Second
	for i := time.Duration(0); i <= waitLimitedCoex; i += sleep {
		if err = dreg.limco(tsi, bck, action, otherBck...); err == nil {
			break
		}
		time.Sleep(sleep)
	}
	return
}

//   - assorted admin-requested actions, in turn, trigger global rebalance
//     e.g.: if copy-bucket or ETL is currently running we cannot start
//     transitioning storage targets to maintenance
//   - all supported xactions define "limited coexistence" via their respecive
//     descriptors in xact.Table
func (r *registry) limco(tsi *meta.Snode, bck *meta.Bck, action string, otherBck ...*meta.Bck) error {
	var (
		nd    *xact.Descriptor // the one that wants to run
		admin bool             // admin-requested action that'd generate a conflict
	)
	switch action {
	case apc.ActStartMaintenance, apc.ActStopMaintenance, apc.ActShutdownNode, apc.ActDecommissionNode:
		nd = &xact.Descriptor{}
		admin = true
	default:
		d, ok := xact.Table[action]
		if !ok {
			return nil
		}
		nd = &d
	}

	var locked bool // rlock/runlock only once
	for kind, d := range xact.Table {
		// rebalance-vs-rebalance and resilver-vs-resilver sort it out between themselves
		// (by preempting)
		conflict := (d.ConflictRebRes && admin) ||
			(d.Rebalance && nd.ConflictRebRes) || (d.Resilver && nd.ConflictRebRes)
		if !conflict {
			continue
		}

		// potential conflict becomes very real if the 'kind' is actually running
		if !locked {
			r.entries.mtx.RLock()
			locked = true
			defer r.entries.mtx.RUnlock()
		}
		entry := r.entries.findRunningKind(kind)
		if entry == nil {
			continue
		}

		// conflict confirmed
		var b string
		if bck != nil {
			b = bck.String()
		}
		return cmn.NewErrLimitedCoexistence(tsi.String(), entry.Get().String(), action, b)
	}

	// finally, bucket rename (apc.ActMoveBck) is a special case -
	// incompatible with any ConflictRebRes type operation _on the same_ bucket
	if action != apc.ActMoveBck {
		return nil
	}
	bck1, bck2 := bck, otherBck[0]
	for _, entry := range r.entries.active {
		xctn := entry.Get()
		if !xctn.IsRunning() {
			continue
		}
		d, ok := xact.Table[xctn.Kind()]
		debug.Func(func() { debug.Assert(ok, xctn.Kind()) })
		if !d.ConflictRebRes {
			continue
		}
		from, to := xctn.FromTo()
		if _eqAny(bck1, bck2, from, to) {
			detail := bck1.String() + " => " + bck2.String()
			return cmn.NewErrLimitedCoexistence(tsi.String(), entry.Get().String(), action, detail)
		}
	}
	return nil
}

func _eqAny(bck1, bck2, from, to *meta.Bck) (eq bool) {
	if from != nil {
		if bck1.Equal(from, false /*same BID*/, true) || bck2.Equal(from, false, true) {
			return true
		}
	}
	if to != nil {
		eq = bck1.Equal(to, false /*same BID*/, true) || bck2.Equal(to, false, true)
	}
	return
}

///////////////
// RenewBase //
///////////////

func (r *RenewBase) Bucket() *meta.Bck { return r.Bck }
func (r *RenewBase) UUID() string      { return r.Args.UUID }

func (r *RenewBase) Str(kind string) string {
	prefix := kind
	if r.Bck != nil {
		prefix += "@" + r.Bck.String()
	}
	return fmt.Sprintf("%s, ID=%q", prefix, r.UUID())
}

//////////////
// RenewRes //
//////////////

// note:
// for a newly constructed instance x-registry always returns RenewRes{Entry: entry}
// with empty rns.UUID
// in other words, IsRunning() can only be true when we are using xprev
func (rns *RenewRes) IsRunning() bool {
	if rns.UUID == "" {
		return false
	}
	return rns.Entry.Get().IsRunning()
}

// IsNew reports whether the candidate's Start succeeded and renewal registered
// it as a new entry. Only this outcome may be passed to Run. Otherwise, the
// candidate was not registered and will never run (Entry may reference xprev).
func (rns *RenewRes) IsNew() bool {
	return rns.Err == nil && rns.Entry != nil && rns.UUID == ""
}

// make sure existing on-demand is active to prevent it from (idle) expiration
// (see demand.go hkcb())
func (rns *RenewRes) beingRenewed() {
	if rns.Err != nil || !rns.IsRunning() {
		return
	}
	xctn := rns.Entry.Get()
	if xdmnd, ok := xctn.(xact.Demand); ok {
		xdmnd.IncPending()
		xdmnd.DecPending()
	}
}

/////////
// Flt //
/////////

func (flt *Flt) String() string {
	msg := xact.QueryMsg{OnlyRunning: flt.OnlyRunning, ID: flt.ID, Kind: flt.Kind}
	if flt.Bck != nil {
		msg.Bck = flt.Bck.Clone()
	}
	return msg.String()
}

func (flt *Flt) Matches(xctn core.Xact) (yes bool) {
	kind := xctn.Kind()
	if debug.ON() {
		debug.Assert(xact.IsValidKind(kind), xctn.String())
	}
	// running?
	if flt.OnlyRunning != nil {
		onl := *flt.OnlyRunning
		if onl != xctn.IsRunning() {
			return false
		}
	}
	// same ID?
	if flt.ID != "" {
		if debug.ON() {
			debug.Assert(xact.IsValidUUID(flt.ID), flt.ID)
		}
		if yes = xctn.ID() == flt.ID; yes {
			if debug.ON() {
				debug.Assert(flt.Kind == "" || kind == flt.Kind, xctn.String()+" vs same ID "+flt.String())
			}
		}
		return yes
	}
	// kind?
	if flt.Kind != "" {
		if debug.ON() {
			debug.Assert(xact.IsValidKind(flt.Kind), flt.Kind)
		}
		if kind != flt.Kind {
			return false
		}
	}
	// bucket?
	// (when the filter carries no bucket return early)
	if flt.Bck == nil {
		debug.Assert(len(flt.Buckets) == 0)
		return true // the filter's not filtering out
	}
	if xact.Table[kind].Scope != xact.ScopeB {
		return true // non single-bucket x
	}
	if len(flt.Buckets) > 0 {
		debug.Assert(len(flt.Buckets) == 2)
		from, to := xctn.FromTo()
		if from != nil { // XactArch special case
			debug.Assert(to != nil)
			return from.Equal(flt.Buckets[0], false /*same BID*/, false) && to.Equal(flt.Buckets[1], false, false)
		}
	}

	return xctn.Bck().Equal(flt.Bck, true, true)
}
