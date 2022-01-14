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
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/xact"
)

const (
	delOldInterval      = 10 * time.Minute // cleanup old (entries.entries)
	delInactiveInterval = time.Minute      // separately, cleanup inactive (entries.active)
	entryOldAge         = 1 * time.Hour    // the interval to keep for history
	entriesCap          = 256              // initial capacity of registry entries
	delOldThreshold     = 300              // the number of entries to trigger (housekeeping) cleanup
	waitAbortDone       = 2 * time.Second
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
		mtx     sync.RWMutex
		active  []Renewable // running entries - finished entries are gradually removed
		entries []Renewable
	}
	// All entries in the registry. The entries are periodically cleaned up
	// to make sure that we don't keep old entries forever.
	registry struct {
		sync.RWMutex
		inactive    atomic.Int64
		entries     *entries
		bckXacts    map[string]Renewable
		nonbckXacts map[string]Renewable
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

func (rns *RenewRes) IsRunning() bool { return rns.UUID != "" }

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
var defaultReg *registry

func Init() {
	defaultReg = newRegistry()
	xact.IncInactive = defaultReg.incInactive
}

func TestReset() { defaultReg = newRegistry() } // tests only

func newRegistry() *registry {
	return &registry{
		entries:     newRegistryEntries(),
		bckXacts:    make(map[string]Renewable, 32),
		nonbckXacts: make(map[string]Renewable, 32),
	}
}

// register w/housekeeper periodic registry cleanups
func RegWithHK() {
	hk.Reg("xactions-old", defaultReg.hkDelOld, 0 /*time.Duration*/)
	hk.Reg("xactions-inactive", defaultReg.hkDelInactive, 0 /*time.Duration*/)
}

func GetXact(uuid string) (xctn cluster.Xact) { return defaultReg.getXact(uuid) }

func (r *registry) getXact(uuid string) (xctn cluster.Xact) {
	e := r.entries
	e.mtx.RLock()
outer:
	for _, entries := range [][]Renewable{e.active, e.entries} { // tradeoff: fewer active, higher priority
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

func GetRunning(flt XactFilter) Renewable { return defaultReg.getRunning(flt) }

func (r *registry) getRunning(flt XactFilter) Renewable {
	onl := true
	flt.OnlyRunning = &onl
	return r.entries.find(flt)
}

func GetLatest(flt XactFilter) Renewable { return defaultReg.getLatest(flt) }

func (r *registry) getLatest(flt XactFilter) Renewable {
	// NOTE: relies on the find() to walk in the newer --> older order
	entry := r.entries.find(flt)
	return entry
}

func CheckBucketsBusy() (cause Renewable) {
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

func AbortAllBuckets(err error, bcks ...*cluster.Bck) { defaultReg.abortAllBuckets(err, bcks...) }

func (r *registry) abortAllBuckets(err error, bcks ...*cluster.Bck) {
	r.abort(abortArgs{bcks: bcks, err: err})
}

// AbortAll waits until abort of all xactions is finished
// Every abort is done asynchronously
func AbortAll(err error, tys ...string) { defaultReg.abortAll(err, tys...) }

func (r *registry) abortAll(err error, tys ...string) {
	var ty string
	if len(tys) > 0 {
		ty = tys[0]
	}
	r.abort(abortArgs{ty: ty, err: err, all: true})
}

func AbortAllMountpathsXactions() { defaultReg.abortAllMountpathsXactions() }

func (r *registry) abortAllMountpathsXactions() { r.abort(abortArgs{mountpaths: true}) }

func DoAbort(kind string, bck *cluster.Bck, err error) (aborted bool) {
	return defaultReg.doAbort(kind, bck, err)
}

func (r *registry) doAbort(kind string, bck *cluster.Bck, err error) (aborted bool) {
	if kind != "" {
		entry := r.getRunning(XactFilter{Kind: kind, Bck: bck})
		if entry == nil {
			return
		}
		return entry.Get().Abort(err)
	}
	if bck == nil {
		// No bucket and no kind - request for all available xactions.
		r.abortAll(err)
	} else {
		// Bucket present and no kind - request for all available bucket's xactions.
		r.abortAllBuckets(err, bck)
	}
	aborted = true
	return
}

func DoAbortByID(uuid string, err error) (aborted bool) { return defaultReg.doAbortByID(uuid, err) }

func (r *registry) doAbortByID(uuid string, err error) (aborted bool) {
	xctn := r.getXact(uuid)
	if xctn == nil {
		return
	}
	return xctn.Abort(err)
}

func GetSnap(flt XactFilter) ([]cluster.XactSnap, error) { return defaultReg.getSnap(flt) }

func (r *registry) getSnap(flt XactFilter) ([]cluster.XactSnap, error) {
	var onlyRunning bool
	if flt.OnlyRunning != nil {
		onlyRunning = *flt.OnlyRunning
	}
	if flt.ID != "" {
		xctn := r.getXact(flt.ID)
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
		return r.matchingXactsStats(func(xctn cluster.Xact) bool {
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
					entry := r.getRunning(XactFilter{Kind: kind, Bck: flt.Bck})
					if entry != nil {
						matching = append(matching, entry.Get().Snap())
					}
				}
			} else {
				entry := r.getRunning(XactFilter{Kind: flt.Kind, Bck: flt.Bck})
				if entry != nil {
					matching = append(matching, entry.Get().Snap())
				}
			}
			return matching, nil
		}
		return r.matchingXactsStats(flt.matches), nil
	}
	return r.matchingXactsStats(flt.matches), nil
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

func (r *registry) add(entry Renewable) { r.entries.add(entry) }

func (r *registry) hkDelOld() time.Duration {
	var (
		toRemove []string
		now      time.Time
		cnt      int
	)
	r.entries.mtx.RLock()
	l := len(r.entries.entries)
	for i := 0; i < l; i++ { // older (start-time wise) -> newer
		xctn := r.entries.entries[i].Get()
		if !xctn.Finished() {
			continue
		}
		if cnt == 0 {
			now = time.Now()
		}
		if xctn.EndTime().Add(entryOldAge).Before(now) {
			toRemove = append(toRemove, xctn.ID())
			cnt++
			if l-cnt < delOldThreshold {
				break
			}
		}
	}
	r.entries.mtx.RUnlock()
	if cnt == 0 {
		return delOldInterval
	}
	r.entries.mtx.Lock()
	for _, id := range toRemove {
		r.entries.del(id)
	}
	r.entries.mtx.Unlock()
	return delOldInterval
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
	r.RLock()
	if prevEntry := r.getRunning(flt); prevEntry != nil {
		xprev := prevEntry.Get()
		if usePrev(xprev, entry, bck) {
			r.RUnlock()
			return RenewRes{Entry: prevEntry, UUID: xprev.ID()}
		}
		if wpr, err := entry.WhenPrevIsRunning(prevEntry); wpr == WprUse || err != nil {
			r.RUnlock()
			xctn := prevEntry.Get()
			return RenewRes{Entry: prevEntry, Err: err, UUID: xctn.ID()}
		}
	}
	r.RUnlock()

	// second
	r.Lock()
	rns = r.renewLocked(entry, flt, bck)
	r.Unlock()
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
			time.Sleep(waitAbortDone) // TODO: better
		}
	}
	if err = entry.Start(); err != nil {
		return RenewRes{Entry: nil, Err: err, UUID: ""}
	}
	r.add(entry)
	return RenewRes{Entry: entry, Err: nil, UUID: ""}
}

//////////////////////
// registry entries //
//////////////////////

func newRegistryEntries() *entries {
	return &entries{
		entries: make([]Renewable, 0, entriesCap),
	}
}

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
		for idx := len(e.entries) - 1; idx >= 0; idx-- {
			entry := e.entries[idx]
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
	for _, entry := range e.entries {
		if !matcher(entry) {
			return
		}
	}
}

// NOTE: is called under lock
func (e *entries) del(id string) {
	for idx, entry := range e.entries {
		xctn := entry.Get()
		if xctn.ID() == id {
			debug.AssertMsg(xctn.Finished(), xctn.String())
			nlen := len(e.entries) - 1
			e.entries[idx] = e.entries[nlen]
			e.entries = e.entries[:nlen]
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

func (e *entries) add(entry Renewable) {
	e.mtx.Lock()
	e.active = append(e.active, entry)
	e.entries = append(e.entries, entry)
	e.mtx.Unlock()
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
