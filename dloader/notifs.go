// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package dloader

import (
	"net/http"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/nl"
	jsoniter "github.com/json-iterator/go"
)

type (
	NotifDownloadListerner struct {
		nl.NotifListenerBase
	}
	NotifDownload struct {
		nl.NotifBase
		job jobif
	}
)

// interface guard
var (
	_ nl.NotifListener = (*NotifDownloadListerner)(nil)
	_ cluster.Notif    = (*NotifDownload)(nil)
)

func NewDownloadNL(jobID, action string, smap *cluster.Smap, progressInterval time.Duration) *NotifDownloadListerner {
	return &NotifDownloadListerner{
		NotifListenerBase: *nl.NewNLB(jobID, action, smap, smap.Tmap.ActiveMap(), progressInterval),
	}
}

func (*NotifDownloadListerner) UnmarshalStats(rawMsg []byte) (stats any, finished, aborted bool, err error) {
	dlStatus := &StatusResp{}
	if err = jsoniter.Unmarshal(rawMsg, dlStatus); err != nil {
		return
	}
	stats = dlStatus
	aborted = dlStatus.Aborted
	finished = dlStatus.JobFinished()
	return
}

func (nd *NotifDownloadListerner) QueryArgs() cmn.HreqArgs {
	var (
		xactID = "nqui-" + cos.GenUUID()
		q      = url.Values{apc.QparamUUID: []string{xactID}} // compare w/ p.dladm
		args   = cmn.HreqArgs{Method: http.MethodGet, Query: q}
		dlBody = AdminBody{
			ID: nd.UUID(), // jobID
		}
	)
	args.Path = apc.URLPathDownload.S
	args.Body = cos.MustMarshal(dlBody)
	return args
}

func (nd *NotifDownloadListerner) AbortArgs() cmn.HreqArgs {
	var (
		xactID = "nabrt-" + cos.GenUUID()
		q      = url.Values{apc.QparamUUID: []string{xactID}} // ditto
		args   = cmn.HreqArgs{Method: http.MethodDelete, Query: q}
		dlBody = AdminBody{
			ID: nd.UUID(),
		}
	)
	args.Path = apc.URLPathDownloadAbort.S
	args.Body = cos.MustMarshal(dlBody)
	return args
}

//
// NotifDownloader
//

func (nd *NotifDownload) ToNotifMsg() cluster.NotifMsg {
	msg := cluster.NotifMsg{UUID: nd.job.ID(), Kind: apc.ActDownload}
	stats, err := nd.job.ActiveStats()
	if err != nil {
		msg.ErrMsg = err.Error()
	} else {
		msg.Data = cos.MustMarshal(stats)
	}
	return msg
}
