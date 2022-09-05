// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/res"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// TODO: uplift via higher-level query and similar (#668)

// verb /v1/xactions
func (t *target) xactHandler(w http.ResponseWriter, r *http.Request) {
	var (
		xactMsg xact.QueryMsg
		bck     *cluster.Bck
	)
	if _, err := t.apiItems(w, r, 0, true, apc.URLPathXactions.L); err != nil {
		return
	}
	switch r.Method {
	case http.MethodGet:
		var (
			query = r.URL.Query()
			what  = query.Get(apc.QparamWhat)
		)
		if uuid := query.Get(apc.QparamUUID); uuid != "" {
			t.getXactByID(w, r, what, uuid)
			return
		}
		if cmn.ReadJSON(w, r, &xactMsg) != nil {
			return
		}
		if xactMsg.Bck.Name != "" {
			bck = cluster.CloneBck(&xactMsg.Bck)
			if err := bck.Init(t.owner.bmd); err != nil {
				t.writeErrSilent(w, r, err, http.StatusNotFound)
				return
			}
		}
		xactQuery := xreg.XactFilter{
			ID: xactMsg.ID, Kind: xactMsg.Kind, Bck: bck, OnlyRunning: xactMsg.OnlyRunning,
		}
		t.queryMatchingXact(w, r, what, xactQuery)
	case http.MethodPut:
		msg, err := t.readActionMsg(w, r)
		if err != nil {
			return
		}
		if err := cos.MorphMarshal(msg.Value, &xactMsg); err != nil {
			t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, msg.Action, msg.Value, err)
			return
		}
		if !xactMsg.Bck.IsEmpty() {
			bck = cluster.CloneBck(&xactMsg.Bck)
			if err := bck.Init(t.owner.bmd); err != nil && msg.Action != apc.ActXactStop {
				// apc.ActXactStop: proceed anyway
				t.writeErr(w, r, err)
				return
			}
		}
		switch msg.Action {
		case apc.ActXactStart:
			if err := t.cmdXactStart(&xactMsg, bck); err != nil {
				t.writeErr(w, r, err)
				return
			}
		case apc.ActXactStop:
			err := cmn.ErrXactUserAbort
			if msg.Name == cmn.ErrXactICNotifAbort.Error() {
				err = cmn.ErrXactICNotifAbort
			}
			flt := xreg.XactFilter{ID: xactMsg.ID, Kind: xactMsg.Kind, Bck: bck}
			xreg.DoAbort(flt, err)
		default:
			t.writeErrAct(w, r, msg.Action)
		}
	default:
		cmn.WriteErr405(w, r, http.MethodGet, http.MethodPut)
	}
}

func (t *target) getXactByID(w http.ResponseWriter, r *http.Request, what, uuid string) {
	if what != apc.GetWhatXactStats {
		t.writeErrf(w, r, fmtUnknownQue, what)
		return
	}
	xctn := xreg.GetXact(uuid)
	if xctn != nil {
		t.writeJSON(w, r, xctn.Snap(), what)
		return
	}
	err := cmn.NewErrXactNotFoundError("[" + uuid + "]")
	t.writeErrSilent(w, r, err, http.StatusNotFound)
}

func (t *target) queryMatchingXact(w http.ResponseWriter, r *http.Request, what string, xactQuery xreg.XactFilter) {
	debug.Assert(what == apc.GetWhatQueryXactStats)
	if what != apc.GetWhatQueryXactStats {
		t.writeErrf(w, r, fmtUnknownQue, what)
		return
	}
	stats, err := xreg.GetSnap(xactQuery)
	if err == nil {
		t.writeJSON(w, r, stats, what)
		return
	}
	if _, ok := err.(*cmn.ErrXactionNotFound); ok {
		t.writeErrSilent(w, r, err, http.StatusNotFound)
	} else {
		t.writeErr(w, r, err)
	}
}

func (t *target) cmdXactStart(xactMsg *xact.QueryMsg, bck *cluster.Bck) error {
	const erfmb = "global xaction %q does not require bucket (%s) - ignoring it and proceeding to start"
	const erfmn = "xaction %q requires a bucket to start"

	if !xact.IsValidKind(xactMsg.Kind) {
		return fmt.Errorf(cmn.FmtErrUnknown, t, "xaction kind", xactMsg.Kind)
	}

	if dtor := xact.Table[xactMsg.Kind]; dtor.Scope == xact.ScopeBck && bck == nil {
		return fmt.Errorf(erfmn, xactMsg.Kind)
	}

	switch xactMsg.Kind {
	// 1. globals
	case apc.ActLRU:
		if bck != nil {
			glog.Errorf(erfmb, xactMsg.Kind, bck)
		}
		ext := &xact.QueryMsgLRU{}
		if err := cos.MorphMarshal(xactMsg.Ext, ext); err != nil {
			return err
		}
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go t.runLRU(xactMsg.ID, wg, ext.Force, xactMsg.Buckets...)
		wg.Wait()
	case apc.ActStoreCleanup:
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go t.runStoreCleanup(xactMsg.ID, wg, xactMsg.Buckets...)
		wg.Wait()
	case apc.ActResilver:
		if bck != nil {
			glog.Errorf(erfmb, xactMsg.Kind, bck)
		}
		notif := &xact.NotifXact{
			NotifBase: nl.NotifBase{
				When: cluster.UponTerm,
				Dsts: []string{equalIC},
				F:    t.callerNotifyFin,
			},
		}
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go t.runResilver(res.Args{UUID: xactMsg.ID, Notif: notif}, wg)
		wg.Wait()
	// 2. with bucket
	case apc.ActPrefetchObjects:
		args := &cmn.SelectObjsMsg{}
		rns := xreg.RenewPrefetch(xactMsg.ID, t, bck, args)
		xctn := rns.Entry.Get()
		xctn.AddNotif(&xact.NotifXact{
			NotifBase: nl.NotifBase{
				When: cluster.UponTerm,
				Dsts: []string{equalIC},
				F:    t.callerNotifyFin,
			},
			Xact: xctn,
		})
		go xctn.Run(nil)
	case apc.ActLoadLomCache:
		rns := xreg.RenewBckLoadLomCache(t, xactMsg.ID, bck)
		return rns.Err
	// 3. cannot start
	case apc.ActPutCopies:
		return fmt.Errorf("cannot start %q (is driven by PUTs into a mirrored bucket)", xactMsg)
	case apc.ActDownload, apc.ActEvictObjects, apc.ActDeleteObjects, apc.ActMakeNCopies, apc.ActECEncode:
		return fmt.Errorf("initiating %q must be done via a separate documented API", xactMsg)
	// 4. unknown
	case "":
		return fmt.Errorf("%q: unspecified (empty) xaction kind", xactMsg)
	default:
		return cmn.NewErrUnsupp("start xaction", xactMsg.Kind)
	}
	return nil
}
