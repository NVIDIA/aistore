// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"strings"
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
	if xactMsg.Kind != "" {
		if err := xact.CheckValidKind(xactMsg.Kind); err != nil {
			t.writeErr(w, r, err)
			return
		}
	}

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
	t.xquery(w, r, what, &xactQuery)
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

	// TODO: not checking `xargs.Buckets` vs `xargs.Bck` and not initializing the former :NOTE

	if cmn.Rom.V(4, cos.ModAIS) {
		nlog.Infoln(msg.Action, xargs.String())
	}
	switch msg.Action {
	case apc.ActXactStart:
		if err := xact.CheckValidKind(xargs.Kind); err != nil {
			t.writeErrf(w, r, "%v: %s", err, xargs.String())
			return
		}
		if xargs.Kind == apc.ActPrefetchObjects {
			ecode, err := t.runPrefetch(xargs.ID, bck, &apc.PrefetchMsg{})
			if err != nil {
				t.writeErr(w, r, err, ecode)
			}
			return
		}
		// all other _startable_ xactions
		xid, err := t.xstart(&xargs, bck, msg)
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
		if xid != "" {
			writeXid(w, xid)
		}
	case apc.ActXactStop:
		if xargs.Kind != "" {
			if err := xact.CheckValidKind(xargs.Kind); err != nil {
				t.writeErrf(w, r, "%v: %s", err, xargs.String())
				return
			}
		}
		if xargs.ID != "" {
			if err := xact.CheckValidUUID(xargs.ID); err != nil {
				t.writeErrf(w, r, "%v: %s", err, xargs.String())
				return
			}
		}
		if xargs.Kind == "" && xargs.ID == "" {
			t.writeErrf(w, r, "cannot stop xaction given '%s' - expecting a valid kind and/or UUID", xargs.String())
			return
		}

		err := cos.Ternary(msg.Name == cmn.ErrXactICNotifAbort.Error(), cmn.ErrXactICNotifAbort, cmn.ErrXactUserAbort)
		flt := xreg.Flt{ID: xargs.ID, Kind: xargs.Kind, Bck: bck}
		xreg.DoAbort(&flt, err)
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

func (t *target) xquery(w http.ResponseWriter, r *http.Request, what string, xactQuery *xreg.Flt) {
	stats, err := xreg.GetSnap(xactQuery)
	if err == nil {
		t.writeJSON(w, r, stats, what) // ok
		return
	}

	xactID := xactQuery.ID
	switch {
	case cmn.IsErrXactNotFound(err):
		t.writeErr(w, r, err, http.StatusNotFound, Silent)
	case xactID != "" && strings.IndexByte(xactID, xact.SepaID[0]) > 0:
		var (
			uuids = strings.Split(xactID, xact.SepaID)
			errN  error
		)
		for _, xid := range uuids {
			xactQuery.ID = xid
			stats, err := xreg.GetSnap(xactQuery)
			if err == nil {
				t.writeJSON(w, r, stats, what) // ok
				return
			}
			errN = err
		}
		if cmn.IsErrXactNotFound(err) {
			t.writeErr(w, r, errN, http.StatusNotFound, Silent)
		} else {
			t.writeErr(w, r, errN)
		}
	default:
		t.writeErr(w, r, err)
	}
}

//
// PUT
//

func (t *target) xstart(args *xact.ArgsMsg, bck *meta.Bck, msg *apc.ActMsg) (xid string, _ error) {
	const erfmb = "global xaction %q does not require bucket (%s) - ignoring it and proceeding to start"
	const erfmn = "xaction %q requires a bucket to start"

	if dtor := xact.Table[args.Kind]; dtor.Scope == xact.ScopeB && bck == nil {
		return xid, fmt.Errorf(erfmn, args.Kind)
	}
	switch args.Kind {
	// 1. global x-s
	case apc.ActLRU:
		if bck != nil {
			nlog.Errorf(erfmb, args.Kind, bck)
		}
		wg := &sync.WaitGroup{}
		wg.Add(1)
		if len(args.Buckets) == 0 && !args.Bck.IsEmpty() {
			args.Buckets = []cmn.Bck{args.Bck}
		}
		go t.runLRU(args.ID, wg, args.Force, args.Buckets...)
		wg.Wait()
	case apc.ActStoreCleanup:
		wg := &sync.WaitGroup{}
		wg.Add(1)
		if len(args.Buckets) == 0 && !args.Bck.IsEmpty() {
			args.Buckets = []cmn.Bck{args.Bck}
		}
		go t.runSpaceCleanup(args, wg)
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
		if args.ID == "" {
			args.ID = cos.GenUUID()
			xid = args.ID
		}
		resargs := &res.Args{
			UUID:  args.ID,
			Notif: notif,
			Custom: xreg.ResArgs{
				Config: cmn.GCO.Get(),
			},
		}
		go t.runResilver(resargs, wg)
		wg.Wait()
	case apc.ActRechunk:
		return t.runRechunk(args.ID, bck, &xreg.RechunkArgs{
			ObjSizeLimit: int64(bck.Props.Chunks.ObjSizeLimit),
			ChunkSize:    int64(bck.Props.Chunks.ChunkSize),
		})
	case apc.ActLoadLomCache:
		rns := xreg.RenewBckLoadLomCache(args.ID, bck)
		return xid, rns.Err
	case apc.ActBlobDl:
		debug.Assert(msg.Name != "")
		lom := core.AllocLOM(msg.Name)
		err := lom.InitBck(&args.Bck)
		if err == nil {
			params := &core.BlobParams{
				Lom: lom,
				Msg: &apc.BlobMsg{}, // default tunables when executing via x-start API
			}
			xid, _, err = t.blobdl(params, nil /*oa*/)
		}
		if err != nil {
			core.FreeLOM(lom)
		}
		return xid, err
	// 3. cannot start
	case apc.ActPutCopies:
		return xid, fmt.Errorf("cannot start %q (is driven by PUTs into a mirrored bucket)", args)
	case apc.ActDownload, apc.ActEvictObjects, apc.ActDeleteObjects, apc.ActMakeNCopies, apc.ActECEncode:
		return xid, fmt.Errorf("initiating %q must be done via a separate documented API", args)
	// 4. unknown
	case "":
		return xid, fmt.Errorf("%q: unspecified (empty) xaction kind", args)
	default:
		return xid, cmn.NewErrUnsupp("start xaction", args.Kind)
	}
	return xid, nil
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
		tcomsg cmn.TCOMsg
	)
	if amsg, err = t.readActionMsg(w, r); err != nil {
		return
	}

	xactID := amsg.Name
	if strings.IndexByte(xactID, xact.SepaID[0]) > 0 {
		uuids := strings.Split(xactID, xact.SepaID)
		for _, xid := range uuids {
			if xctn, err = xreg.GetXact(xid); err == nil {
				break
			}
		}
	} else {
		if xctn, err = xreg.GetXact(xactID); err != nil {
			t.writeErr(w, r, err)
			return
		}
	}
	if xctn == nil {
		t.writeErr(w, r, cos.NewErrNotFound(t, xactID), http.StatusNotFound)
		return
	}

	xtco, ok := xctn.(*xs.XactTCO)
	debug.Assert(ok)

	if err = cos.MorphMarshal(amsg.Value, &tcomsg); err != nil {
		t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, "special", amsg.Value, err)
		return
	}
	xtco.ContMsg(&tcomsg)
}
