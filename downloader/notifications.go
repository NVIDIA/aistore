// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/nl"
	jsoniter "github.com/json-iterator/go"
)

type (
	NotifDownloadListerner struct {
		nl.NotifListenerBase
	}

	NotifDownload struct {
		nl.NotifBase
		DlJob DlJob
	}
)

var (
	_ nl.NotifListener = &NotifDownloadListerner{}
	_ cluster.Notif    = &NotifDownload{} // interface guard
)

func NewDownloadNL(uuid string, smap *cluster.Smap, srcs cluster.NodeMap, action string,
	progressInterval time.Duration, bck ...cmn.Bck) *NotifDownloadListerner {
	return &NotifDownloadListerner{
		NotifListenerBase: *nl.NewNLB(uuid, smap, srcs, action, progressInterval, bck...),
	}
}

func (nd *NotifDownloadListerner) UnmarshalStats(rawMsg []byte) (stats interface{}, finished, aborted bool, err error) {
	dlStatus := &DlStatusResp{}
	if err = jsoniter.Unmarshal(rawMsg, dlStatus); err != nil {
		return
	}
	stats = dlStatus
	aborted = dlStatus.Aborted
	finished = dlStatus.JobFinished()
	return
}

func (nd *NotifDownloadListerner) QueryArgs() cmn.ReqArgs {
	args := cmn.ReqArgs{Method: http.MethodGet}
	dlBody := DlAdminBody{
		ID: nd.UUID(),
	}
	args.Path = cmn.JoinWords(cmn.Version, cmn.Download)
	args.Body = cmn.MustMarshal(dlBody)
	return args
}

func (nd *NotifDownloadListerner) AbortArgs() cmn.ReqArgs {
	args := cmn.ReqArgs{Method: http.MethodDelete}
	dlBody := DlAdminBody{
		ID: nd.UUID(),
	}
	args.Path = cmn.JoinWords(cmn.Version, cmn.Download, cmn.Abort)
	args.Body = cmn.MustMarshal(dlBody)
	return args
}

//
// NotifDownloader
//

func (nd *NotifDownload) ToNotifMsg() cluster.NotifMsg {
	msg := cluster.NotifMsg{UUID: nd.DlJob.ID(), Kind: cmn.ActDownload}
	stats, err := nd.DlJob.ActiveStats()
	if err != nil {
		msg.ErrMsg = err.Error()
	} else {
		msg.Data = cmn.MustMarshal(stats)
	}
	return msg
}
