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
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction"
	jsoniter "github.com/json-iterator/go"
)

type (
	xactMsg struct {
		ID   string  `json:"id"`
		Kind string  `json:"kind"`
		Bck  cmn.Bck `json:"bck"`
	}
)

// TODO: uplift via higher-level query and similar (#668)

// verb /v1/xactions
func (t *targetrunner) xactHandler(w http.ResponseWriter, r *http.Request) {
	msg := &cmn.ActionMsg{}
	if cmn.ReadJSON(w, r, msg) != nil {
		return
	}
	if _, err := t.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Xactions); err != nil {
		return
	}
	switch r.Method {
	case http.MethodGet:
		var (
			query   = r.URL.Query()
			what    = query.Get(cmn.URLParamWhat)
			xactMsg cmn.XactionExtMsg
		)
		if err := cmn.TryUnmarshal(msg.Value, &xactMsg); err != nil {
			t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
			return
		}
		var bck *cluster.Bck
		if xactMsg.Bck.Name != "" {
			bck = cluster.NewBckEmbed(xactMsg.Bck)
			if err := bck.Init(t.owner.bmd, t.si); err != nil {
				t.invalmsghdlrsilent(w, r, err.Error(), http.StatusNotFound)
				return
			}
		}
		xactQuery := xaction.XactQuery{ID: xactMsg.ID, Kind: msg.Name, Bck: bck, OnlyRecent: !xactMsg.All}
		switch what {
		case cmn.GetWhatXactStats:
			body, err := t.queryXactStats(xactQuery)
			if err != nil {
				if _, ok := err.(cmn.XactionNotFoundError); ok {
					t.invalmsghdlrsilent(w, r, err.Error(), http.StatusNotFound)
				} else {
					t.invalmsghdlr(w, r, err.Error())
				}
				return
			}
			t.writeJSON(w, r, body, what)
		case cmn.GetWhatXactRunStatus:
			running := xaction.Registry.IsXactRunning(xactQuery)
			status := &cmn.XactRunningStatus{Kind: xactQuery.Kind, Running: running}
			if bck != nil {
				status.Bck = bck.Bck
			}
			t.writeJSON(w, r, cmn.MustMarshal(status), what)
		default:
			t.invalmsghdlr(w, r, fmt.Sprintf(fmtUnknownQue, what))
		}
	case http.MethodPut:
		var (
			xmsg xactMsg
			bck  *cluster.Bck
		)

		if err := cmn.TryUnmarshal(msg.Value, &xmsg); err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}

		if !xmsg.Bck.IsEmpty() {
			bck = cluster.NewBckEmbed(xmsg.Bck)
			if err := bck.Init(t.owner.bmd, t.si); err != nil {
				t.invalmsghdlr(w, r, err.Error())
				return
			}
		}
		switch msg.Action {
		case cmn.ActXactStart:
			if err := t.cmdXactStart(r, xmsg, bck); err != nil {
				t.invalmsghdlr(w, r, err.Error())
				return
			}
		case cmn.ActXactStop:
			xaction.Registry.DoAbort(xmsg.Kind, bck)
			return
		default:
			t.invalmsghdlr(w, r, fmt.Sprintf(fmtUnknownAct, msg))
		}
	default:
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /xactions path")
	}
}

func (t *targetrunner) queryXactStats(query xaction.XactQuery) ([]byte, error) {
	xactStatsMap, err := xaction.Registry.GetStats(query)
	if err != nil {
		return nil, err
	}
	xactStats := make([]stats.XactStats, 0, len(xactStatsMap))
	for _, stat := range xactStatsMap {
		if stat == nil {
			continue
		}
		xactStats = append(xactStats, stat)
	}
	return jsoniter.Marshal(xactStats)
}

func (t *targetrunner) cmdXactStart(r *http.Request, xmsg xactMsg, bck *cluster.Bck) error {
	const erfmb = "global xaction %q does not require bucket (%s) - ignoring it and proceeding to start"
	const erfmn = "xaction %q requires a bucket to start"
	switch xmsg.Kind {
	// 1. globals
	case cmn.ActLRU:
		if bck != nil {
			glog.Errorf(erfmb, xmsg.Kind, bck)
		}
		go t.RunLRU(xmsg.ID)
	case cmn.ActResilver:
		if bck != nil {
			glog.Errorf(erfmb, xmsg.Kind, bck)
		}
		go t.rebManager.RunResilver(xmsg.ID, false /*skipGlobMisplaced*/)
	// 2. with bucket
	case cmn.ActPrefetch:
		if bck == nil {
			return fmt.Errorf(erfmn, xmsg.Kind)
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
	case cmn.ActECPut:
		if bck == nil {
			return fmt.Errorf(erfmn, xmsg.Kind)
		}
		ec.ECM.RestoreBckPutXact(bck)
	case cmn.ActECGet:
		if bck == nil {
			return fmt.Errorf(erfmn, xmsg.Kind)
		}
		ec.ECM.RestoreBckGetXact(bck)
	case cmn.ActECRespond:
		if bck == nil {
			return fmt.Errorf(erfmn, xmsg.Kind)
		}
		ec.ECM.RestoreBckRespXact(bck)
	// 3. cannot start
	case cmn.ActPutCopies:
		return fmt.Errorf("cannot start xaction %q - it is invoked automatically by PUTs into mirrored bucket", xmsg.Kind)
	case cmn.ActDownload, cmn.ActEvictObjects, cmn.ActDelete, cmn.ActMakeNCopies, cmn.ActECEncode:
		return fmt.Errorf("initiating xaction %q must be done via a separate documented API", xmsg.Kind)
	// 4. unknown
	case "":
		return errors.New("unspecified (empty) xaction kind")
	default:
		return fmt.Errorf("starting %q xaction is unsupported", xmsg.Kind)
	}
	return nil
}
