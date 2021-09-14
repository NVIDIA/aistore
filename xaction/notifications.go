// Package xaction provides core functionality for the AIStore extended actions.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

import (
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/nl"
	jsoniter "github.com/json-iterator/go"
)

type (
	NotifXactListener struct {
		nl.NotifListenerBase
	}

	NotifXact struct {
		nl.NotifBase
		Xact cluster.Xact
	}
)

// interface guard
var (
	_ cluster.Notif    = (*NotifXact)(nil)
	_ nl.NotifListener = (*NotifXactListener)(nil)
)

func NewXactNL(uuid, action string, smap *cluster.Smap, srcs cluster.NodeMap, bck ...cmn.Bck) *NotifXactListener {
	if srcs == nil {
		srcs = smap.Tmap.ActiveMap()
	}
	return &NotifXactListener{
		NotifListenerBase: *nl.NewNLB(uuid, action, smap, srcs, 0, bck...),
	}
}

func (*NotifXactListener) UnmarshalStats(rawMsg []byte) (stats interface{}, finished, aborted bool, err error) {
	xactStats := &BaseStatsExt{}
	if err = jsoniter.Unmarshal(rawMsg, xactStats); err != nil {
		return
	}
	stats = xactStats
	aborted = xactStats.Aborted()
	finished = xactStats.Finished()
	return
}

func (nxb *NotifXactListener) QueryArgs() cmn.ReqArgs {
	args := cmn.ReqArgs{Method: http.MethodGet, Query: make(url.Values, 2)}
	args.Query.Set(cmn.URLParamWhat, cmn.GetWhatXactStats)
	args.Query.Set(cmn.URLParamUUID, nxb.UUID())
	args.Path = cmn.URLPathXactions.S
	return args
}

func (nxb *NotifXactListener) AbortArgs() cmn.ReqArgs {
	msg := cmn.ActionMsg{
		Action: cmn.ActXactStop,
		Value: QueryMsg{
			ID: nxb.UUID(),
		},
	}
	args := cmn.ReqArgs{Method: http.MethodPut}
	args.Body = cos.MustMarshal(msg)
	args.Path = cmn.URLPathXactions.S
	return args
}

//
// NotifXact
//
func (nx *NotifXact) ToNotifMsg() cluster.NotifMsg {
	return cluster.NotifMsg{
		UUID: nx.Xact.ID(),
		Kind: nx.Xact.Kind(),
		Data: cos.MustMarshal(nx.Xact.Stats()),
	}
}
