// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/xact"
)

/* TODO shard by xact.Kind as follows:
	kind struct {
		bckXacts    map[string]Renewable
		nonbckXacts map[string]Renewable
		entries     ...
		finDelta    atomic.Int64
		renewMtx    sync.RWMutex
	}
        // registry must be statically allocated with all (statically) declared xaction kinds
	// (see api.go)
        registry map[string]kind
*/

const (
	initialCap       = 256  // initial capacity
	keepOldThreshold = 1024 // keep so many

	waitPrevAborted     = 2 * time.Second
	waitTerminalCleanup = 500 * time.Millisecond // registry housekeeping when TOCTOU
	waitLimitedCoex     = 3 * time.Second
)

type WPR int

const (
	WprAbort = iota + 1
	WprUse
	WprKeepAndStartNew
)

type (
	Renewable interface {
		New(args Args, bck *meta.Bck) Renewable // new xaction stub that can be `Start`-ed.
		Start() error                           // starts an xaction, will be called when entry is stored into registry
		Kind() string
		Get() core.Xact
		WhenPrevIsRunning(prevEntry Renewable) (action WPR, err error)
		Bucket() *meta.Bck
		UUID() string
	}
	// used in constructions
	Args struct {
		Custom any // Additional arguments that are specific for a given xact.
		UUID   string
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
		newreb bool        // (rebalance is starting) vs (dtor.AbortRebRes)
	}

	entries struct {
		active   []Renewable // running entries - finished entries are gradually removed
		roActive []Renewable // read-only copy
		all      []Renewable
		mtx      sync.RWMutex
	}
	// All entries in the registry. The entries are periodically cleaned up
	// to make sure that we don't keep old entries forever.
	registry struct {
		bckXacts    map[string]Renewable
		nonbckXacts map[string]Renewable
		entries     entries
		finDelta    atomic.Int64
		renewMtx    sync.RWMutex
	}
)

// default global registry that keeps track of all running xactions
// In addition, the registry retains already finished xactions subject to lazy cleanup via `hk`.
var dreg *registry

//////////////////////
// xaction registry //
//////////////////////

func Init() {
	dreg = newRegistry()
	xact.IncFinished = dreg.incFinished
}

func TestReset() { dreg = newRegistry() } // tests only

func newRegistry() (r *registry) {
	return &registry{
		entries: entries{
			all:      make([]Renewable, 0, initialCap),
			active:   make([]Renewable, 0, 128),
			roActive: make([]Renewable, 0, 192),
		},
		bckXacts:    make(map[string]Renewable, 32),
		nonbckXacts: make(map[string]Renewable, 32),
	}
}

// register w/housekeeper periodic registry cleanups
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
	for _, entries := range [][]Renewable{e.active, e.all} { // tradeoff: fewer active, higher priority
		for _, entry := range entries {
			x := entry.Get()
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
	for _, entry := range e.active {
		if x := entry.Get(); x.ID() == uuid {
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
	} else { // allocate
		roActive = make([]Renewable, l)
		e.roActive = roActive // reuse later
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
				isIdle = xctn.Snap().IsIdle()
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
		debug.Assertf(flt.Kind == "" || xctn.Kind() == flt.Kind, "wrong xaction kind: %s vs %q", xctn.Cname(), flt.Kind)
		xctn.Abort(err)
	case flt.Kind != "" && flt.Bck != nil:
		dreg.abort(&abortArgs{kind: flt.Kind, bcks: []*meta.Bck{flt.Bck}, err: err})
	case flt.Kind != "":
		debug.Assert(xact.IsValidKind(flt.Kind), flt.Kind)
		AbortKind(err, flt.Kind)
	case flt.Bck != nil:
		AbortAllBuckets(err, flt.Bck)
	default:
		AbortAll(err)
	}
}

func GetSnap(flt *Flt) ([]*core.Snap, error) {
	var onlyRunning bool
	if flt.OnlyRunning != nil {
		onlyRunning = *flt.OnlyRunning
	}
	if flt.ID != "" {
		xctn, err := dreg.getXact(flt.ID)
		if err != nil {
			return nil, err
		}
		if xctn != nil {
			if onlyRunning && xctn.IsFinished() {
				return nil, cmn.NewErrXactNotFoundError("[only-running vs " + xctn.String() + "]")
			}
			if flt.Kind != "" && xctn.Kind() != flt.Kind {
				return nil, cmn.NewErrXactNotFoundError("[kind=" + flt.Kind + " vs " + xctn.String() + "]")
			}
			return []*core.Snap{xctn.Snap()}, nil
		}
		if onlyRunning || flt.Kind != apc.ActRebalance {
			return nil, cmn.NewErrXactNotFoundError("ID=" + flt.ID)
		}
		// not running rebalance: include all finished (but not aborted) ones
		// with ID at or _after_ the specified
		return dreg.matchingXactsStats(func(xctn core.Xact) bool {
			cmp := xact.CompareRebIDs(xctn.ID(), flt.ID)
			return cmp >= 0 && xctn.IsFinished() && !xctn.IsAborted()
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

		if onlyRunning {
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
	r.entries.forEach(args.do)
}

func (args *abortArgs) do(entry Renewable) bool {
	xctn := entry.Get()
	if xctn.IsFinished() {
		return true
	}

	var abort bool
	switch {
	case args.newreb:
		debug.Assertf(args.scope == nil && args.kind == "", "scope %v, kind %q", args.scope, args.kind)
		_, dtor, err := xact.GetDescriptor(xctn.Kind())
		debug.AssertNoErr(err)
		if dtor.AbortRebRes {
			abort = true
		}
	case len(args.bcks) > 0:
		debug.Assertf(args.scope == nil, "scope %v", args.scope)
		for _, bck := range args.bcks {
			if xctn.Bck() != nil && bck.Equal(xctn.Bck(), true /*sameID*/, true /*same backend*/) {
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
	return true
}

func (r *registry) matchingXactsStats(match func(xctn core.Xact) bool) []*core.Snap {
	matchingEntries := make([]Renewable, 0, 20)
	r.entries.forEach(func(entry Renewable) bool {
		if !match(entry.Get()) {
			return true
		}
		matchingEntries = append(matchingEntries, entry)
		return true
	})
	// TODO: we cannot do this inside `forEach` because - nested locks
	sts := make([]*core.Snap, 0, len(matchingEntries))
	for _, entry := range matchingEntries {
		sts = append(sts, entry.Get().Snap())
	}
	return sts
}

func (r *registry) incFinished() { r.finDelta.Inc() }

func (r *registry) hkPruneActive(now int64) time.Duration {
	if r.finDelta.Swap(0) == 0 {
		return hk.Jitter(hk.Prune2mIval, now)
	}
	e := &r.entries
	e.mtx.Lock()
	l := len(e.active)
	for i := 0; i < l; i++ {
		entry := e.active[i]
		if !entry.Get().IsFinished() {
			continue
		}
		copy(e.active[i:], e.active[i+1:])
		i--
		l--
		e.active = e.active[:l]
	}
	e.mtx.Unlock()
	return hk.Jitter(hk.Prune2mIval, now)
}

func (r *registry) hkDelOld(int64) time.Duration {
	var (
		toRemove    []string
		numKeepMore int
		now         = time.Now() // need calendar time
	)

	r.entries.mtx.RLock()
	l := len(r.entries.all)

	// first, cleanup (x-lso, x-moss): walk older to newer while counting the other kinds
	for i := range l {
		xctn := r.entries.all[i].Get()
		if !xact.Table[xctn.Kind()].LogLess {
			numKeepMore++
			continue
		}
		if xctn.IsFinished() {
			if sinceFin := now.Sub(xctn.EndTime()); sinceFin >= hk.OldAgeXshort {
				toRemove = append(toRemove, xctn.ID())
			}
		}
	}

	// all the rest: older to newer, while keeping at least `keepOldThreshold`
	if numKeepMore > keepOldThreshold {
		var cnt int
		for i := range l {
			xctn := r.entries.all[i].Get()
			if xact.Table[xctn.Kind()].LogLess {
				continue
			}
			if xctn.IsFinished() {
				if sinceFin := now.Sub(xctn.EndTime()); sinceFin >= hk.OldAgeX {
					toRemove = append(toRemove, xctn.ID())
					cnt++
					if numKeepMore-cnt <= keepOldThreshold {
						break
					}
				}
			}
		}
	}
	r.entries.mtx.RUnlock()

	// adaptive HK cadence based on finished-registry backlog
	var (
		d       = hk.DelOldIval
		ll      = len(toRemove)
		remains = l - ll
	)
	switch {
	case remains > keepOldThreshold<<1:
		d = max(d>>2, hk.OldAgeXshort)
	case remains > keepOldThreshold:
		d = max(d>>1, hk.OldAgeXshort)
	}

	if ll == 0 {
		return d
	}

	// cleanup
	r.entries.mtx.Lock()
	for _, id := range toRemove {
		r.entries.del(id)
	}
	r.entries.mtx.Unlock()

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

func (e *entries) forEach(matcher func(entry Renewable) bool) {
	e.mtx.RLock()
	defer e.mtx.RUnlock()
	for _, entry := range e.all {
		if !matcher(entry) {
			return
		}
	}
}

// NOTE: is called under lock
func (e *entries) del(id string) {
	for idx, entry := range e.all {
		xctn := entry.Get()
		if xctn.ID() == id {
			debug.Assert(xctn.IsFinished(), xctn.String(), " aborted: ", xctn.IsAborted())
			nlen := len(e.all) - 1
			e.all[idx] = e.all[nlen]
			e.all = e.all[:nlen]
			break
		}
	}
	for idx, entry := range e.active {
		xctn := entry.Get()
		if xctn.ID() == id {
			if !xctn.IsFinished() {
				nlog.Errorln("Warning: premature HK call to del-old", xctn.String())
				break
			}
			nlen := len(e.active) - 1
			e.active[idx] = e.active[nlen]
			e.active = e.active[:nlen]
			break
		}
	}
}

// is called under lock
// history control for LogLess kinds (x-lso, x-moss)
// – keep up to 1 024 finished records
// – anything beyond is silently dropped
func (e *entries) _add(entry Renewable) {
	e.active = append(e.active, entry)

	if l := len(e.all); xact.Table[entry.Kind()].LogLess && l >= keepOldThreshold {
		if n := skipXregHst.Inc(); n%skipXregHstCnt == 1 {
			nlog.Warningln("num entries in xreg history:", l, "exceeds the cap:", keepOldThreshold,
				"- not adding:", xact.Cname(entry.Kind(), entry.UUID()))
		}
		return
	}
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
		debug.Assert(ok, xctn.Kind())
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
		if bck1.Equal(from, false, true) || bck2.Equal(from, false, true) {
			return true
		}
	}
	if to != nil {
		eq = bck1.Equal(to, false, true) || bck2.Equal(to, false, true)
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

func (rns *RenewRes) IsRunning() bool {
	if rns.UUID == "" {
		return false
	}
	return rns.Entry.Get().IsRunning()
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
	debug.Assert(xact.IsValidKind(xctn.Kind()), xctn.String())
	// running?
	if flt.OnlyRunning != nil {
		if *flt.OnlyRunning != xctn.IsRunning() {
			return false
		}
	}
	// same ID?
	if flt.ID != "" {
		debug.Assert(cos.IsValidUUID(flt.ID) || xact.IsValidRebID(flt.ID), flt.ID)
		if yes = xctn.ID() == flt.ID; yes {
			debug.Assert(xctn.Kind() == flt.Kind, xctn.String()+" vs same ID "+flt.String())
		}
		return yes
	}
	// kind?
	if flt.Kind != "" {
		debug.Assert(xact.IsValidKind(flt.Kind), flt.Kind)
		if xctn.Kind() != flt.Kind {
			return false
		}
	}
	// bucket?
	if xact.Table[xctn.Kind()].Scope != xact.ScopeB {
		return true // non single-bucket x
	}
	if flt.Bck == nil {
		debug.Assert(len(flt.Buckets) == 0)
		return true // the filter's not filtering out
	}
	if len(flt.Buckets) > 0 {
		debug.Assert(len(flt.Buckets) == 2)
		from, to := xctn.FromTo()
		if from != nil { // XactArch special case
			debug.Assert(to != nil)
			return from.Equal(flt.Buckets[0], false, false) && to.Equal(flt.Buckets[1], false, false)
		}
	}

	return xctn.Bck().Equal(flt.Bck, true, true)
}
