// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xs"
)

// compare running the same via (generic) t.xstart
// the caller owns retry/fallback policy for this terminal outcome.
func (t *target) blobdlBackground(params *core.BlobParams, oa *cmn.ObjAttrs) (string, core.Xact, error) {
	debug.Func(func() { debug.Assert(params.RespWriter == nil) })
	// destination is concatenated onto a mountpath (reject traversal)
	if err := cos.ValidateOname(params.Lom.ObjName); err != nil {
		return "", nil, fmt.Errorf("%s: %w", badBlobRequest, err)
	}
	if err := t.checkBlobdlCap(); err != nil {
		return "", nil, err
	}

	if oa != nil {
		if params.BlobThreshold > 0 && oa.Size < params.BlobThreshold {
			return "", nil, nil
		}
		return t.blobdlWlock(params, oa)
	}

	// TODO:
	// compare this background rlock -> unlock -> wlock sequence with
	// blobdlLocked's GET-path lock upgrade, and consider converging the two
	lom, latestVer := params.Lom, params.Msg.LatestVer
	if !lom.TryLock(false) {
		return "", nil, cmn.NewErrBusy("blob", lom.Cname())
	}

	oa, deleted, err := lom.LoadLatest(latestVer)
	lom.Unlock(false)

	// w/ assorted returns
	switch {
	case deleted: // remotely
		debug.Assert(latestVer && err != nil)
		return "", nil, err
	case oa != nil:
		debug.Assert(latestVer && err == nil)
		// not latest
	case err == nil:
		return "", nil, nil // nothing to do
	case !cmn.IsErrObjNought(err):
		return "", nil, err
	}

	if oa == nil {
		oa, _, err = t.HeadCold(lom, nil /*origReq*/)
		if err != nil {
			return "", nil, err
		}
	}

	if params.BlobThreshold > 0 && oa.Size < params.BlobThreshold {
		// below threshold, not qualified for blob-download
		return "", nil, nil
	}
	return t.blobdlWlock(params, oa)
}

// w-lock, re-check presence, and start:
// - the caller qualified this download without holding the w-lock
// - present and latest-ver: the caller's `oa` predates the w-lock
// - an empty xid ("") with no error => nothing to do
func (t *target) blobdlWlock(params *core.BlobParams, oa *cmn.ObjAttrs) (string, core.Xact, error) {
	lom := params.Lom
	if !lom.TryLock(true) {
		return "", nil, cmn.NewErrBusy("blob", lom.Cname())
	}

	switch err := lom.Load(false /*cache it*/, true /*locked*/); {
	case err == nil:
		if !params.Msg.LatestVer {
			lom.Unlock(true)
			return "", nil, nil // present
		}
		res := lom.CheckRemoteMD(true /*locked*/, false /*sync*/, nil /*origReq*/) // HeadCold
		if res.Eq {
			lom.Unlock(true)
			return "", nil, nil // nothing to do
		}
		if res.Err != nil {
			lom.Unlock(true)
			return "", nil, res.Err // (incl. remotely deleted)
		}
		oa = res.ObjAttrs // current remote
	case !cmn.IsErrObjNought(err):
		lom.Unlock(true)
		return "", nil, err
	}
	return t._blobdl(params, oa, nil)
}

// blobdlLocked mirrors cold GET's locking:
// - qualify under rlock
// - upgrade rlock -> wlock
// - hold wlock through the blocking operation
func (t *target) blobdlLocked(params *core.BlobParams, rsphdr *rsphdr) (string, core.Xact, error) {
	debug.Func(func() { debug.Assert(params.RespWriter != nil && rsphdr != nil && rsphdr.hdr != nil) })
	if err := t.checkBlobdlCap(); err != nil {
		return "", nil, err
	}
	var uplock *_uplock
	lom := params.Lom

	// policy: an explicit blob GET fails w/ ErrBusy when there's a write in progress
	if !lom.TryLock(false) {
		return "", nil, cmn.NewErrBusy("blob", lom.Cname())
	}
	debug.Func(func() { debug.Assert(lom.IsLocked() == apc.LockRead) })

do:
	oa, deleted, err := lom.LoadLatest(params.Msg.LatestVer)
	switch {
	case deleted: // remotely
		debug.Func(func() { debug.Assert(params.Msg.LatestVer && err != nil) })
		lom.Unlock(false)
		return "", nil, err
	case oa != nil:
		debug.Func(func() { debug.Assert(params.Msg.LatestVer && err == nil) })
		// remotely newer
	case err == nil:
		lom.Unlock(false)
		return "", nil, nil // the object is already warm and can be served directly
	case !cmn.IsErrObjNought(err):
		lom.Unlock(false)
		return "", nil, err
	default:
		oa, _, err = t.HeadCold(lom, nil /*origReq*/)
		if err != nil {
			lom.Unlock(false)
			return "", nil, err
		}
	}
	if params.BlobThreshold > 0 && oa.Size < params.BlobThreshold {
		lom.Unlock(false)
		return "", nil, nil
	}

	// see the `goi.lom.UpgradeLock()` path in tgtobj.go
	if !lom.UpgradeLock() {
		if uplock == nil {
			uplock = newUplock(cmn.GCO.Get(), mono.NanoTime())
			nlog.Warningln(uplockWarn, lom.String())
		}
		if err = uplock.do(lom); err != nil {
			lom.Unlock(false)
			return "", nil, err
		}
		goto do
	}
	return t._blobdl(params, oa, rsphdr)
}

// returns an empty xid ("") if nothing to do
// the caller owns retry/fallback policy for this terminal outcome.
// rsphdr: GET response header (nil when not via GET)
func (t *target) _blobdl(params *core.BlobParams, oa *cmn.ObjAttrs, rsphdr *rsphdr) (string, core.Xact, error) {
	debug.Func(func() {
		debug.Assertf(params.Lom.IsLocked() == apc.LockWrite,
			"%s must be w-locked (have %d)", params.Lom.Cname(), params.Lom.IsLocked())
	})
	xid := cos.GenUUID()
	rns := xs.RenewBlobDl(xid, params, oa)
	if !rns.IsNew() { // only a newly registered xaction may be started below
		if cmn.IsErrXactUsePrev(rns.Err) {
			rns.Err = cmn.NewErrBusy("blob", params.Lom.Cname())
		}
		return "", nil, rns.Err
	}

	xblob := rns.Entry.Get().(*xs.XactBlobDl)
	notif := &xact.NotifXact{
		Base: nl.Base{When: core.UponTerm, Dsts: []string{equalIC}, F: t.notifyTerm},
		Xact: xblob,
	}
	if params.TermCB != nil {
		notif.F = params.TermCB // (no IC)
	}
	xblob.AddNotif(notif)
	// a) via x-start, x-blob-download
	if params.RespWriter == nil {
		xact.GoRunW(xblob)
		return xblob.ID(), xblob, nil
	}
	// b) via GET (blocking w/ simultaneous transmission)
	debug.Func(func() { debug.Assert(rsphdr != nil && rsphdr.lom == params.Lom) })
	// Admission succeeded: object size is known and can now be published in response headers.
	rsphdr.oa, rsphdr.size = oa, oa.Size
	rsphdr.set()
	xblob.Run(nil)
	return xblob.ID(), nil, blobdlTermErr(xblob)
}

func (t *target) checkBlobdlCap() error {
	cs := fs.Cap()
	if cs.Err() != nil {
		cs = t.oos(cmn.GCO.Get())
	}
	return cs.Err()
}

func blobdlTermErr(xblob *xs.XactBlobDl) error {
	if err := xblob.AbortErr(); err != nil {
		return err
	}
	return xblob.Err()
}
