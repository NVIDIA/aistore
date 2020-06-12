// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/xaction"
)

// TODO: uplift via higher-level query and similar (#668)

// verb /v1/xactions
func (t *targetrunner) xactHandler(w http.ResponseWriter, r *http.Request) {
	if _, err := t.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Xactions); err != nil {
		return
	}
	switch r.Method {
	case http.MethodGet:
		var (
			bck     *cluster.Bck
			xactMsg = cmn.XactionMsg{}
			query   = r.URL.Query()
			what    = query.Get(cmn.URLParamWhat)
		)
		if uuid := query.Get(cmn.URLParamUUID); uuid != "" {
			t.getXactByID(w, r, what, uuid)
			return
		}
		if cmn.ReadJSON(w, r, &xactMsg) != nil {
			return
		}
		if xactMsg.Bck.Name != "" {
			bck = cluster.NewBckEmbed(xactMsg.Bck)
			if err := bck.Init(t.owner.bmd, t.si); err != nil {
				t.invalmsghdlrsilent(w, r, err.Error(), http.StatusNotFound)
				return
			}
		}
		xactQuery := xaction.XactQuery{
			ID: xactMsg.ID, Kind: xactMsg.Kind, Bck: bck,
			OnlyRunning: !xactMsg.All, Finished: xactMsg.Finished,
		}
		t.queryMatchingXact(w, r, what, xactQuery)
	case http.MethodPut:
		var (
			msg     cmn.ActionMsg
			xactMsg cmn.XactionMsg
			bck     *cluster.Bck
		)
		if cmn.ReadJSON(w, r, &msg) != nil {
			return
		}
		if err := cmn.TryUnmarshal(msg.Value, &xactMsg); err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
		if !xactMsg.Bck.IsEmpty() {
			bck = cluster.NewBckEmbed(xactMsg.Bck)
			if err := bck.Init(t.owner.bmd, t.si); err != nil {
				t.invalmsghdlr(w, r, err.Error())
				return
			}
		}
		switch msg.Action {
		case cmn.ActXactStart:
			if err := t.cmdXactStart(r, xactMsg, bck); err != nil {
				t.invalmsghdlr(w, r, err.Error())
				return
			}
		case cmn.ActXactStop:
			xaction.Registry.DoAbort(xactMsg.Kind, bck)
			return
		default:
			t.invalmsghdlr(w, r, fmt.Sprintf(fmtUnknownAct, msg))
		}
	default:
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /xactions path")
	}
}

func (t *targetrunner) getXactByID(w http.ResponseWriter, r *http.Request, what, uuid string) {
	if what != cmn.GetWhatXactStats {
		t.invalmsghdlr(w, r, fmt.Sprintf(fmtUnknownQue, what))
		return
	}
	stats, err := xaction.Registry.GetStatsByID(uuid) // stats.XactStats
	if err == nil {
		t.writeJSON(w, r, stats, what)
		return
	}
	if _, ok := err.(cmn.XactionNotFoundError); ok {
		t.invalmsghdlrsilent(w, r, err.Error(), http.StatusNotFound)
	} else {
		t.invalmsghdlr(w, r, err.Error())
	}
}

func (t *targetrunner) queryMatchingXact(w http.ResponseWriter, r *http.Request, what string, xactQuery xaction.XactQuery) {
	if what != cmn.QueryXactStats {
		t.invalmsghdlr(w, r, fmt.Sprintf(fmtUnknownQue, what))
		return
	}
	stats, err := xaction.Registry.GetStats(xactQuery) // []stats.XactStats
	if err == nil {
		t.writeJSON(w, r, stats, what)
		return
	}
	if _, ok := err.(cmn.XactionNotFoundError); ok {
		t.invalmsghdlrsilent(w, r, err.Error(), http.StatusNotFound)
	} else {
		t.invalmsghdlr(w, r, err.Error())
	}
}

func (t *targetrunner) cmdXactStart(r *http.Request, xactMsg cmn.XactionMsg, bck *cluster.Bck) error {
	const erfmb = "global xaction %q does not require bucket (%s) - ignoring it and proceeding to start"
	const erfmn = "xaction %q requires a bucket to start"
	switch xactMsg.Kind {
	// 1. globals
	case cmn.ActLRU:
		if bck != nil {
			glog.Errorf(erfmb, xactMsg.Kind, bck)
		}
		go t.RunLRU(xactMsg.ID)
	case cmn.ActResilver:
		if bck != nil {
			glog.Errorf(erfmb, xactMsg.Kind, bck)
		}
		go t.rebManager.RunResilver(xactMsg.ID, false /*skipGlobMisplaced*/)
	// 2. with bucket
	case cmn.ActPrefetch:
		if bck == nil {
			return fmt.Errorf(erfmn, xactMsg.Kind)
		}
		args := &xaction.DeletePrefetchArgs{
			Ctx:      t.contextWithAuth(r.Header),
			RangeMsg: &cmn.RangeMsg{},
		}
		xact, err := xaction.Registry.RenewPrefetch(t, bck, args)
		if err != nil {
			return err
		}
		go xact.Run(args)
	// 3. cannot start
	case cmn.ActPutCopies:
		return fmt.Errorf("cannot start xaction %q - it is invoked automatically by PUTs into mirrored bucket", xactMsg.Kind)
	case cmn.ActDownload, cmn.ActEvictObjects, cmn.ActDelete, cmn.ActMakeNCopies, cmn.ActECEncode:
		return fmt.Errorf("initiating xaction %q must be done via a separate documented API", xactMsg.Kind)
	// 4. unknown
	case "":
		return errors.New("unspecified (empty) xaction kind")
	default:
		return fmt.Errorf("starting %q xaction is unsupported", xactMsg.Kind)
	}
	return nil
}
