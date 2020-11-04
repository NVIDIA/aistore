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

var (
	_ cluster.Notif    = &NotifXact{}
	_ nl.NotifListener = &NotifXactListener{}
)

func NewXactNL(uuid, action string, smap *cluster.Smap, srcs cluster.NodeMap, bck ...cmn.Bck) *NotifXactListener {
	if srcs == nil {
		srcs = smap.Tmap.ActiveMap()
	}
	return &NotifXactListener{
		NotifListenerBase: *nl.NewNLB(uuid, action, smap, srcs, 0, bck...),
	}
}

func (nxb *NotifXactListener) UnmarshalStats(rawMsg []byte) (stats interface{}, finished, aborted bool, err error) {
	xactStats := &BaseXactStatsExt{}
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
	args.Path = cmn.JoinWords(cmn.Version, cmn.Xactions)
	return args
}

func (nxb *NotifXactListener) AbortArgs() cmn.ReqArgs {
	msg := cmn.ActionMsg{
		Action: cmn.ActXactStop,
		Value: XactReqMsg{
			ID: nxb.UUID(),
		},
	}
	args := cmn.ReqArgs{Method: http.MethodPut}
	args.Body = cmn.MustMarshal(msg)
	args.Path = cmn.JoinWords(cmn.Version, cmn.Xactions)
	return args
}

//
// NotifXact
//
func (nx *NotifXact) ToNotifMsg() cluster.NotifMsg {
	return cluster.NotifMsg{
		UUID: nx.Xact.ID().String(),
		Kind: nx.Xact.Kind(),
		Data: cmn.MustMarshal(nx.Xact.Stats()),
	}
}
