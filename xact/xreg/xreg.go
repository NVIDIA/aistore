// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/xact"
)

// TODO: some of these constants must be configurable or derived from the config
const (
	delOldIval      = 10 * time.Minute // cleanup entries.entries
	pruneActiveIval = 2 * time.Minute  // prune entries.active
	oldAgeIval      = 1 * time.Hour    // the interval to keep for history

	initialCap      = 256 // initial capacity of registry entries
	delOldThreshold = 300 // the number of entries to trigger (housekeeping) cleanup

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
		New(args Args, bck *cluster.Bck) Renewable // new xaction stub that can be `Start`-ed.
		Start() error                              // starts an xaction, will be called when entry is stored into registry
		Kind() string
		Get() cluster.Xact
		WhenPrevIsRunning(prevEntry Renewable) (action WPR, err error)
		Bucket() *cluster.Bck
		UUID() string
	}
	// used in constructions
	Args struct {
		T      cluster.Target
		UUID   string
		Custom interface{} // Additional arguments that are specific for a given xact.
	}
	RenewBase struct {
		Args
		Bck *cluster.Bck
	}

	XactFilter struct {
		ID          string
		Kind        string
		Bck         *cluster.Bck
		OnlyRunning *bool
	}

	// Represents result of renewing given xact.
	RenewRes struct {
		Entry Renewable // Depending on situation can be new or old entry.
		Err   error     // Error that occurred during renewal.
		UUID  string    // "" if a new entry has been created, ID of the existing xaction otherwise
	}
	// Selects subset of xactions to abort.
	abortArgs struct {
		bcks       []*cluster.Bck // run on a slice of buckets
		ty         string         // one of { ScopeG, ScopeBck, ... } enum
		err        error          // original cause (or reason), e.g. cmn.ErrUserAbort
		mountpaths bool           // mountpath xactions - see xact.Table
		all        bool           // all or matching `ty` above, if defined
	}

	entries struct {
		mtx    sync.RWMutex
		active []Renewable // running entries - finished entries are gradually removed
		all    []Renewable
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

///////////////
// RenewBase //
///////////////

func (r *RenewBase) Bucket() *cluster.Bck { return r.Bck }
func (r *RenewBase) UUID() string         { return r.Args.UUID }

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

//////////////////////
// xaction registry //
//////////////////////

// default global registry that keeps track of all running xactions
// In addition, the registry retains already finished xactions subject to lazy cleanup via `hk`.
var dreg *registry

func Init() {
	dreg = newRegistry()
	xact.IncFinished = dreg.incFinished
}

func TestReset() { dreg = newRegistry() } // tests only

func newRegistry() (r *registry) {
	return &registry{
		entries: entries{
			all:    make([]Renewable, 0, initialCap),
			active: make([]Renewable, 0, 32),
		},
		bckXacts:    make(map[string]Renewable, 32),
		nonbckXacts: make(map[string]Renewable, 32),
	}
}

// register w/housekeeper periodic registry cleanups
func RegWithHK() {
	hk.Reg("x-old", dreg.hkDelOld, 0 /*time.Duration*/)
	hk.Reg("x-prune-active", dreg.hkPruneActive, 0 /*time.Duration*/)
}

func GetXact(uuid string) (xctn cluster.Xact) { return dreg.getXact(uuid) }

func (r *registry) getXact(uuid string) (xctn cluster.Xact) {
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

func GetRunning(flt XactFilter) Renewable { return dreg.getRunning(flt) }

func (r *registry) getRunning(flt XactFilter) Renewable {
	onl := true
	flt.OnlyRunning = &onl
	return r.entries.find(flt)
}

// NOTE: relies on the find() to walk in the newer --> older order
func GetLatest(flt XactFilter) Renewable {
	entry := dreg.entries.find(flt)
	return entry
}

// AbortAllBuckets aborts all xactions that run with any of the provided bcks.
// It not only stops the "bucket xactions" but possibly "task xactions" which
// are running on given bucket.

func AbortAllBuckets(err error, bcks ...*cluster.Bck) {
	dreg.abort(abortArgs{bcks: bcks, err: err})
}

// AbortAll waits until abort of all xactions is finished
// Every abort is done asynchronously
func AbortAll(err error, tys ...string) {
	var ty string
	if len(tys) > 0 {
		ty = tys[0]
	}
	dreg.abort(abortArgs{ty: ty, err: err, all: true})
}

func AbortAllMountpathsXactions() { dreg.abort(abortArgs{mountpaths: true}) }

func DoAbort(kind string, bck *cluster.Bck, err error) (aborted bool) {
	if kind != "" {
		entry := dreg.getRunning(XactFilter{Kind: kind, Bck: bck})
		if entry == nil {
			return
		}
		return entry.Get().Abort(err)
	}
	if bck == nil {
		// No bucket and no kind - request for all available xactions.
		AbortAll(err)
	} else {
		// Bucket present and no kind - request for all available bucket's xactions.
		AbortAllBuckets(err, bck)
	}
	aborted = true
	return
}

func DoAbortByID(uuid string, err error) (aborted bool) {
	xctn := dreg.getXact(uuid)
	if xctn == nil {
		return
	}
	return xctn.Abort(err)
}

func GetSnap(flt XactFilter) ([]cluster.XactSnap, error) {
	var onlyRunning bool
	if flt.OnlyRunning != nil {
		onlyRunning = *flt.OnlyRunning
	}
	if flt.ID != "" {
		xctn := dreg.getXact(flt.ID)
		if xctn != nil {
			if onlyRunning && xctn.Finished() {
				return nil, cmn.NewErrXactNotFoundError("[only-running vs " + xctn.String() + "]")
			}
			if flt.Kind != "" && xctn.Kind() != flt.Kind {
				return nil, cmn.NewErrXactNotFoundError("[kind=" + flt.Kind + " vs " + xctn.String() + "]")
			}
			return []cluster.XactSnap{xctn.Snap()}, nil
		}
		if onlyRunning || flt.Kind != cmn.ActRebalance {
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
			matching := make([]cluster.XactSnap, 0, 10)
			if flt.Kind == "" {
				for kind := range xact.Table {
					entry := dreg.getRunning(XactFilter{Kind: kind, Bck: flt.Bck})
					if entry != nil {
						matching = append(matching, entry.Get().Snap())
					}
				}
			} else {
				entry := dreg.getRunning(XactFilter{Kind: flt.Kind, Bck: flt.Bck})
				if entry != nil {
					matching = append(matching, entry.Get().Snap())
				}
			}
			return matching, nil
		}
		return dreg.matchingXactsStats(flt.matches), nil
	}
	return dreg.matchingXactsStats(flt.matches), nil
}

func (r *registry) abort(args abortArgs) {
	r.entries.forEach(func(entry Renewable) bool {
		xctn := entry.Get()
		if xctn.Finished() {
			return true
		}
		abort := false
		if args.mountpaths {
			debug.AssertMsg(args.ty == "", args.ty)
			if xact.IsMountpath(xctn.Kind()) {
				abort = true
			}
		} else if len(args.bcks) > 0 {
			debug.AssertMsg(args.ty == "", args.ty)
			for _, bck := range args.bcks {
				if xctn.Bck() != nil && bck.Bck.Equal(xctn.Bck().Bck) {
					abort = true
					break
				}
			}
		} else if args.all {
			abort = args.ty == "" || args.ty == xact.Table[xctn.Kind()].Scope
		}
		if abort {
			xctn.Abort(args.err)
		}
		return true
	})
}

func (r *registry) matchingXactsStats(match func(xctn cluster.Xact) bool) []cluster.XactSnap {
	matchingEntries := make([]Renewable, 0, 20)
	r.entries.forEach(func(entry Renewable) bool {
		if !match(entry.Get()) {
			return true
		}
		matchingEntries = append(matchingEntries, entry)
		return true
	})
	// TODO: we cannot do this inside `forEach` because - nested locks
	sts := make([]cluster.XactSnap, 0, len(matchingEntries))
	for _, entry := range matchingEntries {
		sts = append(sts, entry.Get().Snap())
	}
	return sts
}

func (r *registry) incFinished() { r.finDelta.Inc() }

func (r *registry) hkPruneActive() time.Duration {
	if r.finDelta.Swap(0) == 0 {
		return pruneActiveIval
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
	return pruneActiveIval
}

func (r *registry) hkDelOld() time.Duration {
	var (
		toRemove []string
		now      time.Time
		cnt      int
	)
	r.entries.mtx.RLock()
	l := len(r.entries.all)
	for i := 0; i < l; i++ { // older (start-time wise) -> newer
		xctn := r.entries.all[i].Get()
		if !xctn.Finished() {
			continue
		}
		if cnt == 0 {
			now = time.Now()
		}
		if xctn.EndTime().Add(oldAgeIval).Before(now) {
			toRemove = append(toRemove, xctn.ID())
			cnt++
			if l-cnt < delOldThreshold {
				break
			}
		}
	}
	r.entries.mtx.RUnlock()
	if cnt == 0 {
		return delOldIval
	}
	r.entries.mtx.Lock()
	for _, id := range toRemove {
		r.entries.del(id)
	}
	r.entries.mtx.Unlock()
	return delOldIval
}

func (r *registry) renewByID(entry Renewable, bck *cluster.Bck) (rns RenewRes) {
	flt := XactFilter{ID: entry.UUID(), Kind: entry.Kind(), Bck: bck}
	rns = r._renewFlt(entry, flt)
	rns.beingRenewed()
	return
}

func (r *registry) renew(entry Renewable, bck *cluster.Bck) (rns RenewRes) {
	flt := XactFilter{Kind: entry.Kind(), Bck: bck}
	rns = r._renewFlt(entry, flt)
	rns.beingRenewed()
	return
}

func (r *registry) _renewFlt(entry Renewable, flt XactFilter) (rns RenewRes) {
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
			if cmn.IsErrUsePrevXaction(err) {
				if wpr != WprUse {
					glog.Errorf("%v - not starting a new one of the same kind", err)
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
func usePrev(xprev cluster.Xact, nentry Renewable, bck *cluster.Bck) (use bool) {
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
		if pdtor.Scope != xact.ScopeBck {
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

func (r *registry) renewLocked(entry Renewable, flt XactFilter, bck *cluster.Bck) (rns RenewRes) {
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
		return RenewRes{Entry: nil, Err: err, UUID: ""}
	}
	r.entries.add(entry)
	return RenewRes{Entry: entry, Err: nil, UUID: ""}
}

//////////////////////
// registry entries //
//////////////////////

func (e *entries) find(flt XactFilter) (entry Renewable) {
	e.mtx.RLock()
	entry = e.findUnlocked(flt)
	e.mtx.RUnlock()
	return
}

func (e *entries) findUnlocked(flt XactFilter) Renewable {
	var onlyRunning bool
	if flt.OnlyRunning != nil {
		onlyRunning = *flt.OnlyRunning
	}
	debug.AssertMsg(flt.Kind == "" || xact.IsValidKind(flt.Kind), flt.Kind)
	if !onlyRunning {
		// walk in reverse as there is a greater chance
		// the one we are looking for is at the end
		for idx := len(e.all) - 1; idx >= 0; idx-- {
			entry := e.all[idx]
			if flt.matches(entry.Get()) {
				return entry
			}
		}
		return nil
	}
	// only running
	for _, entry := range e.active {
		if flt.matches(entry.Get()) {
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
			debug.AssertMsg(xctn.Finished(), xctn.String())
			nlen := len(e.all) - 1
			e.all[idx] = e.all[nlen]
			e.all = e.all[:nlen]
			break
		}
	}
	for idx, entry := range e.active {
		xctn := entry.Get()
		if xctn.ID() == id {
			debug.AssertMsg(xctn.Finished(), xctn.String())
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
}

//
// LimitedCoexistence checks whether a given xaction that is about to start can, in fact, "coexist"
// with those that are currently running. It's a piece of logic designed to centralize all decision-making
// of that sort. Further comments below.
//

func LimitedCoexistence(tsi *cluster.Snode, bck *cluster.Bck, action string, otherBck ...*cluster.Bck) (err error) {
	const sleep = time.Second
	for i := time.Duration(0); i < waitLimitedCoex; i += sleep {
		if err = dreg.limco(tsi, bck, action, otherBck...); err == nil {
			break
		}
		if action == cmn.ActMoveBck {
			return
		}
		time.Sleep(sleep)
	}
	return
}

// - assorted admin-requested actions, in turn, trigger global rebalance
//    e.g.: if copy-bucket or ETL is currently running we cannot start
//          transitioning storage targets to maintenance
// - all supported xactions define "limited coexistence" via their respecive
//   descriptors in xact.Table
func (r *registry) limco(tsi *cluster.Snode, bck *cluster.Bck, action string, otherBck ...*cluster.Bck) error {
	var (
		nd          *xact.Descriptor // the one that wants to run
		adminReqAct bool             // admin-requested action that'd generate protential conflict
	)
	debug.Assert(tsi.Type() == cmn.Target) // TODO: extend to proxies
	switch {
	case action == cmn.ActStartMaintenance, action == cmn.ActShutdownNode:
		nd = &xact.Descriptor{}
		adminReqAct = true
	default:
		if d, ok := xact.Table[action]; ok {
			nd = &d
		} else {
			return nil
		}
	}

	for kind, d := range xact.Table {
		// note that rebalance-vs-rebalance and resilver-vs-resilver sort it out between themselves
		conflict := (d.MassiveBck && adminReqAct) ||
			(d.Rebalance && nd.MassiveBck) || (d.Resilver && nd.MassiveBck)
		if !conflict {
			continue
		}

		// the potential conflict becomes very real if the 'kind' is actually running
		entry := r.getRunning(XactFilter{Kind: kind})
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

	// finally, bucket rename (cmn.ActMoveBck) is a special case -
	// incompatible with any MassiveBck type operation _on the same_ bucket
	if action != cmn.ActMoveBck {
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

func _eqAny(bck1, bck2, from, to *cluster.Bck) (eq bool) {
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

////////////////
// XactFilter //
////////////////

func (flt *XactFilter) String() string {
	s := fmt.Sprintf("ID=%q, kind=%q", flt.ID, flt.Kind)
	if flt.Bck != nil {
		s = fmt.Sprintf("%s, %s", s, flt.Bck)
	}
	if flt.OnlyRunning != nil {
		s = fmt.Sprintf("%s, only-running=%t", s, *flt.OnlyRunning)
	}
	return s
}

func (flt XactFilter) matches(xctn cluster.Xact) (yes bool) {
	debug.AssertMsg(xact.IsValidKind(xctn.Kind()), xctn.String())
	// running?
	if flt.OnlyRunning != nil {
		if *flt.OnlyRunning != xctn.Running() {
			return false
		}
	}
	// same ID?
	if flt.ID != "" {
		if yes = xctn.ID() == flt.ID; yes {
			debug.AssertMsg(xctn.Kind() == flt.Kind, xctn.String()+" vs same ID "+flt.String())
		}
		return
	}
	// kind?
	if flt.Kind != "" {
		debug.AssertMsg(xact.IsValidKind(flt.Kind), flt.Kind)
		if xctn.Kind() != flt.Kind {
			return false
		}
	}
	// bucket?
	if xact.Table[xctn.Kind()].Scope != xact.ScopeBck {
		return true // non-bucket x
	}
	if flt.Bck == nil {
		return true // the filter's not filtering out
	}

	if xctn.Bck() == nil {
		return false // NOTE: ambiguity - cannot really compare
	}
	return xctn.Bck().Equal(flt.Bck, true, true)
}
