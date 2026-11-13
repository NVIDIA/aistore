// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/xact"
)

// core renewal logic -------------------

func (r *registry) renewFlt(entry Renewable, flt *Flt) RenewRes {
	var retried bool

again_rlock:
	rns, notStarting, done, terminal := r._renewPrev(entry, flt)
	if done {
		if notStarting != nil {
			nlog.Errorln(notStarting, "- not starting a new one of the same kind")
		}
		return rns
	}

	// (was running when looked up, but now is terminal)
	if terminal {
		if !retried {
			retried = true // just once
			time.Sleep(waitTerminalCleanup)
			goto again_rlock
		}
	}

	// serialized path
	rns, aborted := r._renewSerial(entry, flt)

	if aborted {
		// abort to propagate & registry to settle
		time.Sleep(waitPrevAborted)
		retried = true // ditto
		goto again_rlock
	}
	return rns
}

// under the read lock, try to use the previous (running) xaction
// - done means that rns is final (the previous xaction or an error)
// - terminal means that the previous one is found but not running (the caller retries once)
// neither means calling _renewSerial under write lock
//
//nolint:staticcheck // making an exception
func (r *registry) _renewPrev(entry Renewable, flt *Flt) (rns RenewRes, notStarting error, done, terminal bool) {
	r.renewMtx.RLock()
	defer r.renewMtx.RUnlock()

	prevEntry := r.getRunning(flt)
	if prevEntry == nil {
		return rns, nil, false, false
	}
	xprev := prevEntry.Get()
	debug.Assert(xprev != nil)
	if !xprev.IsRunning() {
		return rns, nil, false, true // terminal
	}
	if usePrev(xprev, entry, flt) {
		return RenewRes{Entry: prevEntry, UUID: xprev.ID()}, nil, true, false
	}
	if wpr, err := entry.WhenPrevIsRunning(prevEntry); wpr == WprUse || err != nil {
		if cmn.IsErrXactUsePrev(err) && wpr != WprUse {
			notStarting = err
		}
		xctn := prevEntry.Get()
		return RenewRes{Entry: prevEntry, Err: err, UUID: xctn.ID()}, notStarting, true, false // done
	}
	return rns, nil, false, false
}

func (r *registry) _renewSerial(entry Renewable, flt *Flt) (RenewRes, bool) {
	r.renewMtx.Lock()
	defer r.renewMtx.Unlock()
	return r._renewLocked(entry, flt)
}

// usePrev: default policies for reusing a currently running xaction
// 1. same UUID: always reuse
// 2. non `xact.Demand`: delegate to WhenPrevIsRunning() for full control
// 3. ActGetBatch: is an `xact.Demand` but requires full control via its own WhenPrevIsRunning()
// 4. xact.Demand scope-based heuristics:
//    - non-bucket scope (cluster/node): generally safe to reuse across requests
//    - bucket-scoped:
//      - single bucket: Must match exactly (name, namespace, backend)
//      - transform/copy: both source and destination buckets must match
//      - different buckets => cannot reuse

func usePrev(xprev core.Xact, nentry Renewable, flt *Flt) bool {
	pkind, nkind := xprev.Kind(), nentry.Kind()
	debug.Assertf(pkind == nkind && pkind != "", "%s != %s", pkind, nkind)
	pdtor, ndtor := xact.Table[pkind], xact.Table[nkind]
	debug.Assert(pdtor.Scope == ndtor.Scope)

	// same ID
	if xprev.ID() != "" && xprev.ID() == nentry.UUID() {
		return true // yes, use prev
	}
	if _, ok := xprev.(xact.Demand); !ok {
		return false // upon return call xaction-specific WhenPrevIsRunning()
	}
	//
	// on-demand
	//
	if nkind == apc.ActGetBatch { // NOTE: want full control via WhenPrevIsRunning
		return false
	}
	if pdtor.Scope != xact.ScopeB { // TODO: too loose, too broad
		return true
	}
	bck := flt.Bck
	debug.AssertFunc(func() bool { return !bck.IsEmpty() })
	if !bck.Equal(xprev.Bck(), true /*same BID*/, true) {
		return false
	}
	// on-demand (from-bucket, to-bucket)
	from, to := xprev.FromTo()
	if len(flt.Buckets) == 2 && from != nil && to != nil {
		for _, bck := range flt.Buckets {
			if !bck.Equal(from, true /*same BID*/, true) && !bck.Equal(to, true, true) {
				return false
			}
		}
	}
	return true
}

func (r *registry) _renewLocked(entry Renewable, flt *Flt) (rns RenewRes, aborted bool) {
	var (
		xprev core.Xact
		wpr   WPR
		err   error
	)
	if prevEntry := r.getRunning(flt); prevEntry != nil {
		xprev = prevEntry.Get()
		if usePrev(xprev, entry, flt) {
			return RenewRes{Entry: prevEntry, UUID: xprev.ID()}, false
		}
		wpr, err = entry.WhenPrevIsRunning(prevEntry)
		if wpr == WprUse || err != nil {
			return RenewRes{Entry: prevEntry, Err: err, UUID: xprev.ID()}, false
		}
		debug.Assert(wpr == WprAbort || wpr == WprKeepAndStartNew)
		if wpr == WprAbort {
			xprev.Abort(cmn.ErrXactRenewAbort)
			return RenewRes{}, true
		}
	}
	if err = entry.Start(); err != nil {
		// Start may fail after InitBase creates the lifecycle context.
		if xctn := entry.Get(); xctn != nil && !cos.IsTypedNil(xctn) {
			xctn.CancelContext()
		}
		return RenewRes{Err: err}, false
	}

	// add to registry
	e := &r.entries
	e.mtx.Lock()
	e._add(entry)
	e.mtx.Unlock()

	return RenewRes{Entry: entry}, false
}
