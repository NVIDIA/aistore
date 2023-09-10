// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/xact"
)

// TODO: some of these constants must be configurable or derived from the config
const (
	initialCap       = 256 // initial capacity
	keepOldThreshold = 256 // keep so many

	waitPrevAborted = 2 * time.Second
	waitLimitedCoex = 5 * time.Second
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
		Get() cluster.Xact
		WhenPrevIsRunning(prevEntry Renewable) (action WPR, err error)
		Bucket() *meta.Bck
		UUID() string
	}
	// used in constructions
	Args struct {
		T      cluster.Target
		Custom any // Additional arguments that are specific for a given xact.
		UUID   string
	}
	RenewBase struct {
		Args
		Bck *meta.Bck
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
		bcks       []*meta.Bck // run on a slice of buckets
		scope      []int       // one of { ScopeG, ScopeB, ... } enum
		kind       string      // all of a kind
		err        error       // original cause (or reason), e.g. cmn.ErrUserAbort
		mountpaths bool        // mountpath xactions - see xact.Table
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
		renewMtx    sync.RWMutex // TODO: revisit to optimiz out
		entries     entries
		bckXacts    map[string]Renewable
		nonbckXacts map[string]Renewable
		finDelta    atomic.Int64
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
			active:   make([]Renewable, 0, 32),
			roActive: make([]Renewable, 0, 64),
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

func GetXact(uuid string) (cluster.Xact, error) { return dreg.getXact(uuid) }

func (r *registry) getXact(uuid string) (xctn cluster.Xact, err error) {
	if !xact.IsValidUUID(uuid) {
		err = fmt.Errorf("invalid UUID %q", uuid)
		return
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
	return
}

func GetAllRunning(inout *cluster.AllRunningInOut, periodic bool) {
	dreg.entries.getAllRunning(inout, periodic)
}

func (e *entries) getAllRunning(inout *cluster.AllRunningInOut, periodic bool) {
	var roActive []Renewable
	if periodic {
		roActive = e.roActive
		roActive = roActive[:len(e.active)]
	} else {
		roActive = make([]Renewable, len(e.active))
	}
	e.mtx.RLock()
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
		if !xctn.Running() {
			continue
		}
		var (
			xqn    = k + xact.LeftID + xctn.ID() + xact.RightID // e.g. "make-n-copies[fGhuvvn7t]"
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

func GetRunning(flt Flt) Renewable { return dreg.getRunning(flt) }

func (r *registry) getRunning(flt Flt) (entry Renewable) {
	e := &r.entries
	e.mtx.RLock()
	entry = e.findRunning(flt)
	e.mtx.RUnlock()
	return
}

// NOTE: relies on the find() to walk in the newer --> older order
func GetLatest(flt Flt) Renewable {
	entry := dreg.entries.find(flt)
	return entry
}

// AbortAllBuckets aborts all xactions that run with any of the provided bcks.
// It not only stops the "bucket xactions" but possibly "task xactions" which
// are running on given bucket.

func AbortAllBuckets(err error, bcks ...*meta.Bck) {
	dreg.abort(abortArgs{bcks: bcks, err: err})
}

// AbortAll waits until abort of all xactions is finished
// Every abort is done asynchronously
func AbortAll(err error, scope ...int) {
	dreg.abort(abortArgs{scope: scope, err: err})
}

func AbortKind(err error, kind string) {
	dreg.abort(abortArgs{kind: kind, err: err})
}

func AbortAllMountpathsXactions() { dreg.abort(abortArgs{mountpaths: true}) }

func DoAbort(flt Flt, err error) (bool /*aborted*/, error) {
	if flt.ID != "" {
		xctn, err := dreg.getXact(flt.ID)
		if xctn == nil || err != nil {
			return false, err
		}
		debug.Assertf(flt.Kind == "" || xctn.Kind() == flt.Kind,
			"UUID must uniquely identify kind: %s vs %+v", xctn, flt)
		return xctn.Abort(err), nil
	}

	if flt.Kind != "" {
		debug.Assert(xact.IsValidKind(flt.Kind), flt.Kind)
		entry := dreg.getRunning(flt)
		if entry == nil {
			return false, nil
		}
		return entry.Get().Abort(err), nil
	}
	if flt.Bck == nil {
		// No bucket and no kind - request for all available xactions.
		AbortAll(err)
	} else {
		// Bucket present and no kind - request for all available bucket's xactions.
		AbortAllBuckets(err, flt.Bck)
	}
	return true, nil
}

func GetSnap(flt Flt) ([]*cluster.Snap, error) {
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
			if onlyRunning && xctn.Finished() {
				return nil, cmn.NewErrXactNotFoundError("[only-running vs " + xctn.String() + "]")
			}
			if flt.Kind != "" && xctn.Kind() != flt.Kind {
				return nil, cmn.NewErrXactNotFoundError("[kind=" + flt.Kind + " vs " + xctn.String() + "]")
			}
			return []*cluster.Snap{xctn.Snap()}, nil
		}
		if onlyRunning || flt.Kind != apc.ActRebalance {
			return nil, cmn.NewErrXactNotFoundError("ID=" + flt.ID)
		}
		// not running rebalance: include all finished (but not aborted) ones
		// with ID at ot _after_ the specified
		return dreg.matchingXactsStats(func(xctn cluster.Xact) bool {
			cmp := xact.CompareRebIDs(xctn.ID(), flt.ID)
			return cmp >= 0 && xctn.Finished() && !xctn.IsAborted()
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
			matching := make([]*cluster.Snap, 0, 10)
			if flt.Kind == "" {
				dreg.entries.mtx.RLock()
				for kind := range xact.Table {
					entry := dreg.entries.findRunning(Flt{Kind: kind, Bck: flt.Bck})
					if entry != nil {
						matching = append(matching, entry.Get().Snap())
					}
				}
				dreg.entries.mtx.RUnlock()
			} else {
				entry := dreg.getRunning(Flt{Kind: flt.Kind, Bck: flt.Bck})
				if entry != nil {
					matching = append(matching, entry.Get().Snap())
				}
			}
			return matching, nil
		}
		return dreg.matchingXactsStats(flt.Matches), nil
	}
	return dreg.matchingXactsStats(flt.Matches), nil
}

func (r *registry) abort(args abortArgs) {
	r.entries.forEach(func(entry Renewable) bool {
		xctn := entry.Get()
		if xctn.Finished() {
			return true
		}

		var abort bool
		switch {
		case args.mountpaths:
			debug.Assertf(args.scope == nil && args.kind == "", "scope %v, kind %q", args.scope, args.kind)
			if xact.IsMountpath(xctn.Kind()) {
				abort = true
			}
		case len(args.bcks) > 0:
			debug.Assertf(args.scope == nil && args.kind == "", "scope %v, kind %q", args.scope, args.kind)
			for _, bck := range args.bcks {
				if xctn.Bck() != nil && bck.Equal(xctn.Bck(), true /*sameID*/, true /*same backend*/) {
					abort = true
					break
				}
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
	})
}

func (r *registry) matchingXactsStats(match func(xctn cluster.Xact) bool) []*cluster.Snap {
	matchingEntries := make([]Renewable, 0, 20)
	r.entries.forEach(func(entry Renewable) bool {
		if !match(entry.Get()) {
			return true
		}
		matchingEntries = append(matchingEntries, entry)
		return true
	})
	// TODO: we cannot do this inside `forEach` because - nested locks
	sts := make([]*cluster.Snap, 0, len(matchingEntries))
	for _, entry := range matchingEntries {
		sts = append(sts, entry.Get().Snap())
	}
	return sts
}

func (r *registry) incFinished() { r.finDelta.Inc() }

func (r *registry) hkPruneActive() time.Duration {
	if r.finDelta.Swap(0) == 0 {
		return hk.PruneActiveIval
	}
	e := &r.entries
	e.mtx.Lock()
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
	e.mtx.Unlock()
	return hk.PruneActiveIval
}

func (r *registry) hkDelOld() time.Duration {
	var (
		toRemove  []string
		numNonLso int
		now       = time.Now()
	)

	r.entries.mtx.RLock()
	l := len(r.entries.all)
	// first, cleanup list-objects: walk older to newer while counting non-lso
	for i := 0; i < l; i++ {
		xctn := r.entries.all[i].Get()
		if xctn.Kind() != apc.ActList {
			numNonLso++
			continue
		}
		if xctn.Finished() {
			if sinceFin := now.Sub(xctn.EndTime()); sinceFin >= hk.OldAgeLso {
				toRemove = append(toRemove, xctn.ID())
			}
		}
	}
	// all the rest: older to newer, while keeping at least `keepOldThreshold`
	if numNonLso > keepOldThreshold {
		var cnt int
		for i := 0; i < l; i++ {
			xctn := r.entries.all[i].Get()
			if xctn.Kind() == apc.ActList {
				continue
			}
			if xctn.Finished() {
				if sinceFin := now.Sub(xctn.EndTime()); sinceFin >= hk.OldAgeX {
					toRemove = append(toRemove, xctn.ID())
					cnt++
					if numNonLso-cnt <= keepOldThreshold {
						break
					}
				}
			}
		}
	}
	r.entries.mtx.RUnlock()

	if len(toRemove) == 0 {
		return hk.DelOldIval
	}

	// cleanup
	r.entries.mtx.Lock()
	for _, id := range toRemove {
		r.entries.del(id)
	}
	r.entries.mtx.Unlock()
	return hk.DelOldIval
}

func (r *registry) renewByID(entry Renewable, bck *meta.Bck) (rns RenewRes) {
	flt := Flt{ID: entry.UUID(), Kind: entry.Kind(), Bck: bck}
	rns = r._renewFlt(entry, flt)
	rns.beingRenewed()
	return
}

func (r *registry) renew(entry Renewable, bck *meta.Bck, buckets ...*meta.Bck) (rns RenewRes) {
	flt := Flt{Kind: entry.Kind(), Bck: bck, Buckets: buckets}
	rns = r._renewFlt(entry, flt)
	rns.beingRenewed()
	return
}

func (r *registry) _renewFlt(entry Renewable, flt Flt) (rns RenewRes) {
	bck := flt.Bck
	// first, try to reuse under rlock
	r.renewMtx.RLock()
	if prevEntry := r.getRunning(flt); prevEntry != nil {
		xprev := prevEntry.Get()
		if usePrev(xprev, entry, bck) {
			r.renewMtx.RUnlock()
			return RenewRes{Entry: prevEntry, UUID: xprev.ID()}
		}
		if wpr, err := entry.WhenPrevIsRunning(prevEntry); wpr == WprUse || err != nil {
			r.renewMtx.RUnlock()
			if cmn.IsErrXactUsePrev(err) {
				if wpr != WprUse {
					nlog.Errorf("%v - not starting a new one of the same kind", err)
				}
				err = nil
			}
			xctn := prevEntry.Get()
			return RenewRes{Entry: prevEntry, Err: err, UUID: xctn.ID()}
		}
	}
	r.renewMtx.RUnlock()

	// second
	r.renewMtx.Lock()
	rns = r.renewLocked(entry, flt, bck)
	r.renewMtx.Unlock()
	return
}

// reusing current (aka "previous") xaction: default policies
func usePrev(xprev cluster.Xact, nentry Renewable, bck *meta.Bck) (use bool) {
	pkind, nkind := xprev.Kind(), nentry.Kind()
	debug.Assertf(pkind == nkind && pkind != "", "%s != %s", pkind, nkind)
	pdtor, ndtor := xact.Table[pkind], xact.Table[nkind]
	debug.Assert(pdtor.Scope == ndtor.Scope)
	// same ID
	if xprev.ID() != "" && xprev.ID() == nentry.UUID() {
		use = true
		return
	}
	// on-demand
	if _, ok := xprev.(xact.Demand); ok {
		if pdtor.Scope != xact.ScopeB {
			use = true
			return
		}
		debug.Assert(!bck.IsEmpty())
		use = bck.Equal(xprev.Bck(), true, true)
		return
	}
	// otherwise, consult with the impl via WhenPrevIsRunning()
	return
}

func (r *registry) renewLocked(entry Renewable, flt Flt, bck *meta.Bck) (rns RenewRes) {
	var (
		xprev cluster.Xact
		wpr   WPR
		err   error
	)
	if prevEntry := r.getRunning(flt); prevEntry != nil {
		xprev = prevEntry.Get()
		if usePrev(xprev, entry, bck) {
			return RenewRes{Entry: prevEntry, UUID: xprev.ID()}
		}
		wpr, err = entry.WhenPrevIsRunning(prevEntry)
		if wpr == WprUse || err != nil {
			return RenewRes{Entry: prevEntry, Err: err, UUID: xprev.ID()}
		}
		debug.Assert(wpr == WprAbort || wpr == WprKeepAndStartNew)
		if wpr == WprAbort {
			xprev.Abort(cmn.ErrXactRenewAbort)
			time.Sleep(waitPrevAborted)
		}
	}
	if err = entry.Start(); err != nil {
		return RenewRes{Err: err}
	}
	r.entries.add(entry)
	return RenewRes{Entry: entry}
}

//////////////////////
// registry entries //
//////////////////////

// NOTE: the caller must take rlock
func (e *entries) findRunning(flt Flt) Renewable {
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
		xctn := entry.Get()
		if xctn.Kind() == kind && xctn.Running() {
			return entry
		}
	}
	return nil
}

func (e *entries) find(flt Flt) (entry Renewable) {
	e.mtx.RLock()
	entry = e.findUnlocked(flt)
	e.mtx.RUnlock()
	return
}

func (e *entries) findUnlocked(flt Flt) Renewable {
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
			debug.Assert(xctn.Finished(), xctn.String())
			nlen := len(e.all) - 1
			e.all[idx] = e.all[nlen]
			e.all = e.all[:nlen]
			break
		}
	}
	for idx, entry := range e.active {
		xctn := entry.Get()
		if xctn.ID() == id {
			debug.Assert(xctn.Finished(), xctn.String())
			nlen := len(e.active) - 1
			e.active[idx] = e.active[nlen]
			e.active = e.active[:nlen]
			break
		}
	}
}

func (e *entries) add(entry Renewable) {
	e.mtx.Lock()
	e.active = append(e.active, entry)
	e.all = append(e.all, entry)
	e.mtx.Unlock()

	// grow
	if cap(e.roActive) < len(e.active) {
		e.roActive = make([]Renewable, 0, len(e.active)+len(e.active)>>1)
	}
}

//
// LimitedCoexistence checks whether a given xaction that is about to start can, in fact, "coexist"
// with those that are currently running. It's a piece of logic designed to centralize all decision-making
// of that sort. Further comments below.
//

func LimitedCoexistence(tsi *meta.Snode, bck *meta.Bck, action string, otherBck ...*meta.Bck) (err error) {
	const sleep = time.Second
	for i := time.Duration(0); i < waitLimitedCoex; i += sleep {
		if err = dreg.limco(tsi, bck, action, otherBck...); err == nil {
			break
		}
		if action == apc.ActMoveBck {
			return
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
		admin bool             // admin-requested action that'd generate protential conflict
	)
	switch action {
	case apc.ActStartMaintenance, apc.ActShutdownNode:
		nd = &xact.Descriptor{}
		admin = true
	default:
		d, ok := xact.Table[action]
		if !ok {
			return nil
		}
		nd = &d
	}
	var locked bool
	for kind, d := range xact.Table {
		// note that rebalance-vs-rebalance and resilver-vs-resilver sort it out between themselves
		conflict := (d.MassiveBck && admin) ||
			(d.Rebalance && nd.MassiveBck) || (d.Resilver && nd.MassiveBck)
		if !conflict {
			continue
		}

		// the potential conflict becomes very real if the 'kind' is actually running
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
	// incompatible with any MassiveBck type operation _on the same_ bucket
	if action != apc.ActMoveBck {
		return nil
	}
	bck1, bck2 := bck, otherBck[0]
	for _, entry := range r.entries.active {
		xctn := entry.Get()
		if !xctn.Running() {
			continue
		}
		d, ok := xact.Table[xctn.Kind()]
		debug.Assert(ok, xctn.Kind())
		if !d.MassiveBck {
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
	return rns.Entry.Get().Running()
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
	msg := xact.QueryMsg{OnlyRunning: flt.OnlyRunning, Bck: flt.Bck.Clone(), ID: flt.ID, Kind: flt.Kind}
	return msg.String()
}

func (flt Flt) Matches(xctn cluster.Xact) (yes bool) {
	debug.Assert(xact.IsValidKind(xctn.Kind()), xctn.String())
	// running?
	if flt.OnlyRunning != nil {
		if *flt.OnlyRunning != xctn.Running() {
			return false
		}
	}
	// same ID?
	if flt.ID != "" {
		debug.Assert(cos.IsValidUUID(flt.ID) || xact.IsValidRebID(flt.ID), flt.ID)
		if yes = xctn.ID() == flt.ID; yes {
			debug.Assert(xctn.Kind() == flt.Kind, xctn.String()+" vs same ID "+flt.String())
		}
		return
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
