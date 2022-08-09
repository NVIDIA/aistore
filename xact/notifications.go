// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/api/apc"
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
		Xact cluster.Xact
		nl.NotifBase
	}
)

// interface guard
var (
	_ cluster.Notif    = (*NotifXact)(nil)
	_ nl.NotifListener = (*NotifXactListener)(nil)
)

func NewXactNL(uuid, action string, smap *cluster.Smap, srcs cluster.NodeMap, bck ...*cmn.Bck) *NotifXactListener {
	if srcs == nil {
		srcs = smap.Tmap.ActiveMap()
	}
	return &NotifXactListener{
		NotifListenerBase: *nl.NewNLB(uuid, action, smap, srcs, 0, bck...),
	}
}

func (*NotifXactListener) UnmarshalStats(rawMsg []byte) (stats interface{}, finished, aborted bool, err error) {
	xactStats := &SnapExt{}
	if err = jsoniter.Unmarshal(rawMsg, xactStats); err != nil {
		return
	}
	stats = xactStats
	aborted = xactStats.IsAborted()
	finished = xactStats.Finished()
	return
}

func (nxb *NotifXactListener) QueryArgs() cmn.HreqArgs {
	args := cmn.HreqArgs{Method: http.MethodGet, Query: make(url.Values, 2)}
	args.Query.Set(apc.QparamWhat, apc.GetWhatXactStats)
	args.Query.Set(apc.QparamUUID, nxb.UUID())
	args.Path = apc.URLPathXactions.S
	return args
}

// (see also downloader.AbortArgs)
func (nxb *NotifXactListener) AbortArgs() cmn.HreqArgs {
	msg := apc.ActionMsg{
		Action: apc.ActXactStop,
		Name:   cmn.ErrXactICNotifAbort.Error(),
		Value:  QueryMsg{ID: nxb.UUID(), Kind: nxb.Kind()},
	}
	args := cmn.HreqArgs{Method: http.MethodPut}
	args.Body = cos.MustMarshal(msg)
	args.Path = apc.URLPathXactions.S
	return args
}

func (nx *NotifXact) ToNotifMsg() cluster.NotifMsg {
	return cluster.NotifMsg{
		UUID: nx.Xact.ID(),
		Kind: nx.Xact.Kind(),
		Data: cos.MustMarshal(nx.Xact.Snap()),
	}
}
