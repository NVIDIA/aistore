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
	"github.com/NVIDIA/aistore/notifications"
	jsoniter "github.com/json-iterator/go"
)

type (
	NotifXactListener struct {
		notifications.NotifListenerBase
	}

	NotifXact struct {
		notifications.NotifBase
		Xact cluster.Xact
	}
)

var (
	_ cluster.Notif               = &NotifXact{}
	_ notifications.NotifListener = &NotifXactListener{}
)

func NewXactNL(uuid string, smap *cluster.Smap, srcs cluster.NodeMap, ty int, action string, bck ...cmn.Bck) *NotifXactListener {
	return &NotifXactListener{
		NotifListenerBase: *notifications.NewNLB(uuid, smap, srcs, ty, action, 0, bck...),
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
	args.Path = cmn.JoinWords(cmn.Version, cmn.Cluster)
	return args
}

//
// NotifXact
//
func (nx *NotifXact) ToNotifMsg() cluster.NotifMsg {
	msg := cluster.NotifMsg{Ty: int32(nx.Category())}
	msg.UUID = nx.Xact.ID().String()
	msg.Data = cmn.MustMarshal(nx.Xact.Stats())
	return msg
}
