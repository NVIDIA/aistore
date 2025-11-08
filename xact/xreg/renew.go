// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/xact"
)

// core renewal logic -------------------

func (r *registry) _renewFlt(entry Renewable, flt *Flt) RenewRes {
	var retried bool

again_rlock:
	r.renewMtx.RLock()

	var (
		prevEntry = r.getRunning(flt)
		xprev     core.Xact
		havePrev  bool
		terminal  bool
	)
	if prevEntry != nil {
		havePrev = true
		xprev = prevEntry.Get()
		debug.Assert(xprev != nil)

		terminal = !xprev.IsRunning()

		// regular path
		if !terminal {
			if usePrev(xprev, entry, flt) {
				r.renewMtx.RUnlock()
				return RenewRes{Entry: prevEntry, UUID: xprev.ID()}
			}
			if wpr, err := entry.WhenPrevIsRunning(prevEntry); wpr == WprUse || err != nil {
				r.renewMtx.RUnlock()
				if cmn.IsErrXactUsePrev(err) && wpr != WprUse {
					nlog.Errorln(err, "- not starting a new one of the same kind")
				}
				xctn := prevEntry.Get()
				return RenewRes{Entry: prevEntry, Err: err, UUID: xctn.ID()}
			}
		}
	}

	r.renewMtx.RUnlock()

	//
	// TOCTOU race (was running when looked up, but now is terminal)
	//
	if havePrev && terminal {
		if !retried {
			retried = true // just once
			time.Sleep(waitTerminalCleanup)
			goto again_rlock
		}
	}

	// serialized path
	r.renewMtx.Lock()
	rns, aborted := r.renewLocked(entry, flt)
	r.renewMtx.Unlock()

	if aborted {
		// abort to propagate & registry to settle
		time.Sleep(waitPrevAborted)
		retried = true // prevent any more retries
		goto again_rlock
	}
	return rns
}

// usePrev: default policies for reusing a currently running xaction
//
// 1. same UUID: always reuse
//
// 2. non `xact.Demand`: delegate to WhenPrevIsRunning() for full control
//
// 3. ActGetBatch: is an `xact.Demand` but requires full control via its own WhenPrevIsRunning()
//
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
	debug.Assert(!bck.IsEmpty())
	if !bck.Equal(xprev.Bck(), true, true) {
		return false
	}
	// on-demand (from-bucket, to-bucket)
	from, to := xprev.FromTo()
	if len(flt.Buckets) == 2 && from != nil && to != nil {
		for _, bck := range flt.Buckets {
			if !bck.Equal(from, true, true) && !bck.Equal(to, true, true) {
				return false
			}
		}
	}
	return true
}

func (r *registry) renewLocked(entry Renewable, flt *Flt) (rns RenewRes, aborted bool) {
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
		return RenewRes{Err: err}, false
	}

	// add to registry
	e := &r.entries
	e.mtx.Lock()
	e._add(entry)
	e.mtx.Unlock()

	return RenewRes{Entry: entry}, false
}
