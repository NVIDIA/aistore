// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package dload

import (
	"net/http"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/nl"

	jsoniter "github.com/json-iterator/go"
)

type (
	NotifDownloadListerner struct {
		nl.ListenerBase
	}
	NotifDownload struct {
		nl.Base
		job jobif
	}
)

// interface guard
var (
	_ nl.Listener = (*NotifDownloadListerner)(nil)
	_ core.Notif  = (*NotifDownload)(nil)
)

func NewDownloadNL(jobID, kind string, smap *meta.Smap, progressInterval time.Duration) *NotifDownloadListerner {
	return &NotifDownloadListerner{
		ListenerBase: *nl.NewNLB(jobID, kind, "" /*causal action*/, smap.Tmap.ActiveMap(), progressInterval),
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
		xid    = "nqui-" + cos.GenUUID()
		q      = url.Values{apc.QparamUUID: []string{xid}} // compare w/ p.dladm
		args   = cmn.HreqArgs{Method: http.MethodGet, Query: q}
		dlBody = AdminBody{
			ID: nd.UUID(), // jobID
		}
	)
	args.Path = apc.URLPathDownload.S
	args.Body = cos.MustMarshal(dlBody)
	return args
}

//
// NotifDownloader
//

func (nd *NotifDownload) ToNotifMsg(aborted bool) core.NotifMsg {
	msg := core.NotifMsg{UUID: nd.job.ID(), Kind: apc.ActDownload, AbortedX: aborted}
	stats, err := nd.job.ActiveStats()
	if err != nil {
		msg.ErrMsg = err.Error()
	} else {
		msg.Data = cos.MustMarshal(stats)
	}
	return msg
}
