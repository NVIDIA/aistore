// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/res"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/NVIDIA/aistore/xact/xs"
)

// TODO: uplift via higher-level query and similar (#668)

// verb /v1/xactions
func (t *target) xactHandler(w http.ResponseWriter, r *http.Request) {
	if _, err := t.parseURL(w, r, apc.URLPathXactions.L, 0, true); err != nil {
		return
	}
	switch r.Method {
	case http.MethodGet:
		t.httpxget(w, r)
	case http.MethodPut:
		t.httpxput(w, r)
	case http.MethodPost:
		t.httpxpost(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodGet, http.MethodPut)
	}
}

//
// GET
//

func (t *target) httpxget(w http.ResponseWriter, r *http.Request) {
	var (
		xactMsg xact.QueryMsg
		bck     *meta.Bck
		query   = r.URL.Query()
		what    = query.Get(apc.QparamWhat)
	)
	if uuid := query.Get(apc.QparamUUID); uuid != "" {
		t.xget(w, r, what, uuid)
		return
	}
	if cmn.ReadJSON(w, r, &xactMsg) != nil {
		return
	}
	debug.Assert(xactMsg.Kind == "" || xact.IsValidKind(xactMsg.Kind), xactMsg.Kind)

	//
	// TODO: add user option to return idle xactions (separately)
	//
	if what == apc.WhatAllRunningXacts {
		var inout = core.AllRunningInOut{Kind: xactMsg.Kind}
		xreg.GetAllRunning(&inout, false /*periodic*/)
		t.writeJSON(w, r, inout.Running, what)
		return
	}

	if what != apc.WhatQueryXactStats {
		t.writeErrf(w, r, fmtUnknownQue, what)
		return
	}

	if xactMsg.Bck.Name != "" {
		bck = meta.CloneBck(&xactMsg.Bck)
		if err := bck.Init(t.owner.bmd); err != nil {
			t.writeErr(w, r, err, http.StatusNotFound, Silent)
			return
		}
	}
	xactQuery := xreg.Flt{
		ID: xactMsg.ID, Kind: xactMsg.Kind, Bck: bck, OnlyRunning: xactMsg.OnlyRunning,
	}
	t.xquery(w, r, what, xactQuery)
}

func (t *target) httpxput(w http.ResponseWriter, r *http.Request) {
	var (
		xargs xact.ArgsMsg
		bck   *meta.Bck
	)
	msg, err := t.readActionMsg(w, r)
	if err != nil {
		return
	}
	if err := cos.MorphMarshal(msg.Value, &xargs); err != nil {
		t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, msg.Action, msg.Value, err)
		return
	}
	if !xargs.Bck.IsEmpty() {
		bck = meta.CloneBck(&xargs.Bck)
		if err := bck.Init(t.owner.bmd); err != nil && msg.Action != apc.ActXactStop {
			// proceed anyway to stop
			t.writeErr(w, r, err)
			return
		}
	}
	if cmn.Rom.FastV(4, cos.SmoduleAIS) {
		nlog.Infoln(msg.Action, xargs.String())
	}
	switch msg.Action {
	case apc.ActXactStart:
		debug.Assert(xact.IsValidKind(xargs.Kind), xargs.String())
		if xargs.Kind == apc.ActPrefetchObjects {
			// TODO: consider adding `Value any` to generic `xact.ArgsMsg`
			errCode, err := t.runPrefetch(xargs.ID, bck, &apc.PrefetchMsg{})
			if err != nil {
				t.writeErr(w, r, err, errCode)
			}
			return
		}
		// the rest "startables"
		if err := t.xstart(&xargs, bck, msg); err != nil {
			t.writeErr(w, r, err)
			return
		}
	case apc.ActXactStop:
		debug.Assert(xact.IsValidKind(xargs.Kind) || xact.IsValidUUID(xargs.ID), xargs.String())
		err := cmn.ErrXactUserAbort
		if msg.Name == cmn.ErrXactICNotifAbort.Error() {
			err = cmn.ErrXactICNotifAbort
		}
		flt := xreg.Flt{ID: xargs.ID, Kind: xargs.Kind, Bck: bck}
		xreg.DoAbort(flt, err)
	default:
		t.writeErrAct(w, r, msg.Action)
	}
}

func (t *target) xget(w http.ResponseWriter, r *http.Request, what, uuid string) {
	if what != apc.WhatXactStats {
		t.writeErrf(w, r, fmtUnknownQue, what)
		return
	}
	xctn, err := xreg.GetXact(uuid)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	if xctn != nil {
		t.writeJSON(w, r, xctn.Snap(), what)
		return
	}
	err = cmn.NewErrXactNotFoundError("[" + uuid + "]")
	t.writeErr(w, r, err, http.StatusNotFound, Silent)
}

func (t *target) xquery(w http.ResponseWriter, r *http.Request, what string, xactQuery xreg.Flt) {
	stats, err := xreg.GetSnap(xactQuery)
	if err == nil {
		t.writeJSON(w, r, stats, what)
		return
	}
	if cmn.IsErrXactNotFound(err) {
		t.writeErr(w, r, err, http.StatusNotFound, Silent)
	} else {
		t.writeErr(w, r, err)
	}
}

//
// PUT
//

func (t *target) xstart(args *xact.ArgsMsg, bck *meta.Bck, msg *apc.ActMsg) error {
	const erfmb = "global xaction %q does not require bucket (%s) - ignoring it and proceeding to start"
	const erfmn = "xaction %q requires a bucket to start"

	if !xact.IsValidKind(args.Kind) {
		return fmt.Errorf(cmn.FmtErrUnknown, t, "xaction kind", args.Kind)
	}
	if dtor := xact.Table[args.Kind]; dtor.Scope == xact.ScopeB && bck == nil {
		return fmt.Errorf(erfmn, args.Kind)
	}
	switch args.Kind {
	// 1. global x-s
	case apc.ActLRU:
		if bck != nil {
			nlog.Errorf(erfmb, args.Kind, bck)
		}
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go t.runLRU(args.ID, wg, args.Force, args.Buckets...)
		wg.Wait()
	case apc.ActStoreCleanup:
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go t.runStoreCleanup(args.ID, wg, args.Buckets...)
		wg.Wait()
	case apc.ActResilver:
		if bck != nil {
			nlog.Errorf(erfmb, args.Kind, bck)
		}
		notif := &xact.NotifXact{
			Base: nl.Base{
				When: core.UponTerm,
				Dsts: []string{equalIC},
				F:    t.notifyTerm,
			},
		}
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go t.runResilver(res.Args{UUID: args.ID, Notif: notif}, wg)
		wg.Wait()
	case apc.ActLoadLomCache:
		rns := xreg.RenewBckLoadLomCache(args.ID, bck)
		return rns.Err
	case apc.ActBlobDl:
		debug.Assert(msg.Name != "")
		lom := core.AllocLOM(msg.Name)
		err := lom.InitBck(&args.Bck)
		if err == nil {
			// (compare w/ alternative t.blobdl path via dedicated api.BlobDownload)
			err = _blobdl(args.ID, lom, &apc.BlobMsg{})
		}
		if err != nil {
			core.FreeLOM(lom)
		}
		return err
	// 3. cannot start
	case apc.ActPutCopies:
		return fmt.Errorf("cannot start %q (is driven by PUTs into a mirrored bucket)", args)
	case apc.ActDownload, apc.ActEvictObjects, apc.ActDeleteObjects, apc.ActMakeNCopies, apc.ActECEncode:
		return fmt.Errorf("initiating %q must be done via a separate documented API", args)
	// 4. unknown
	case "":
		return fmt.Errorf("%q: unspecified (empty) xaction kind", args)
	default:
		return cmn.NewErrUnsupp("start xaction", args.Kind)
	}
	return nil
}

//
// POST
//

// client: plstcx.go
func (t *target) httpxpost(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		xctn   core.Xact
		amsg   *apc.ActMsg
		tcomsg cmn.TCObjsMsg
	)
	if amsg, err = t.readActionMsg(w, r); err != nil {
		return
	}

	xactID := amsg.Name
	if xctn, err = xreg.GetXact(xactID); err != nil {
		t.writeErr(w, r, err)
		return
	}
	xtco, ok := xctn.(*xs.XactTCObjs)
	debug.Assert(ok)

	if err = cos.MorphMarshal(amsg.Value, &tcomsg); err != nil {
		t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, "special", amsg.Value, err)
		return
	}
	xtco.Do(&tcomsg)
}
