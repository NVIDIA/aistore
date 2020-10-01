// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/xaction/registry"
	jsoniter "github.com/json-iterator/go"
)

// NOTE: This request is internal so we can have asserts there.
// [METHOD] /v1/download
func (t *targetrunner) downloadHandler(w http.ResponseWriter, r *http.Request) {
	var (
		response   interface{}
		respErr    error
		statusCode int
	)
	xact, err := registry.Registry.RenewDownloader(t, t.statsT)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusInternalServerError)
		return
	}
	downloaderXact := xact.(*downloader.Downloader)
	switch r.Method {
	case http.MethodPost:
		_, err := cmn.MatchRESTItems(r.URL.Path, 0, false, cmn.Version, cmn.Download)
		debug.AssertNoErr(err)

		var (
			ctx              = context.Background()
			uuid             = r.URL.Query().Get(cmn.URLParamUUID)
			dlb              = downloader.DlBody{}
			progressInterval = downloader.DownloadProgressInterval
		)
		debug.Assert(uuid != "")
		if err := cmn.ReadJSON(w, r, &dlb); err != nil {
			return
		}

		dlBodyBase := downloader.DlBase{}
		if err := jsoniter.Unmarshal(dlb.RawMessage, &dlBodyBase); err != nil {
			return
		}

		if dlBodyBase.ProgressInterval != "" {
			if dur, err := time.ParseDuration(dlBodyBase.ProgressInterval); err == nil {
				progressInterval = dur
			} else {
				t.invalmsghdlrf(w, r, "%s: invalid progress interval %q, err: %v", t.si, dlBodyBase.ProgressInterval, err)
				return
			}
		}

		bck := cluster.NewBckEmbed(dlBodyBase.Bck)
		if err := bck.Init(t.Bowner(), t.Snode()); err != nil {
			t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
			return
		}
		if err := bck.Allow(cmn.AccessSYNC); err != nil {
			t.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
			return
		}

		dlJob, err := downloader.ParseStartDownloadRequest(ctx, t, bck, uuid, dlb, downloaderXact)
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("Downloading: %s", dlJob.ID())
		}

		dlJob.AddNotif(&downloader.NotifDownload{
			NotifBase: nl.NotifBase{
				When:     cluster.UponProgress,
				Interval: progressInterval,
				Dsts:     []string{equalIC},
				F:        t.callerNotifyFin,
				P:        t.callerNotifyProgress,
			},
		}, dlJob)
		response, respErr, statusCode = downloaderXact.Download(dlJob)
	case http.MethodGet:
		_, err := cmn.MatchRESTItems(r.URL.Path, 0, false, cmn.Version, cmn.Download)
		debug.AssertNoErr(err)

		payload := &downloader.DlAdminBody{}
		if err := cmn.ReadJSON(w, r, payload); err != nil {
			return
		}
		debug.AssertNoErr(payload.Validate(false /*requireID*/))

		if payload.ID != "" {
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("Getting status of download: %v", payload)
			}
			response, respErr, statusCode = downloaderXact.JobStatus(payload.ID, payload.OnlyActiveTasks)
		} else {
			var regex *regexp.Regexp
			if payload.Regex != "" {
				if regex, err = regexp.CompilePOSIX(payload.Regex); err != nil {
					cmn.InvalidHandlerWithMsg(w, r, err.Error())
					return
				}
			}
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("Listing downloads")
			}
			response, respErr, statusCode = downloaderXact.ListJobs(regex)
		}
	case http.MethodDelete:
		items, err := cmn.MatchRESTItems(r.URL.Path, 1, false, cmn.Version, cmn.Download)
		debug.AssertNoErr(err)

		payload := &downloader.DlAdminBody{}
		if err = cmn.ReadJSON(w, r, payload); err != nil {
			return
		}
		debug.AssertNoErr(payload.Validate(true))

		switch items[0] {
		case cmn.Abort:
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("Aborting download: %v", payload)
			}
			response, respErr, statusCode = downloaderXact.AbortJob(payload.ID)
		case cmn.Remove:
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("Removing download: %v", payload)
			}
			response, respErr, statusCode = downloaderXact.RemoveJob(payload.ID)
		default:
			cmn.AssertMsg(false,
				fmt.Sprintf("Invalid action for DELETE request: %s (expected either %s or %s).",
					items[0], cmn.Abort, cmn.Remove))
			return
		}
	default:
		cmn.AssertMsg(false,
			fmt.Sprintf("Invalid http method %s; expected one of %s, %s, %s",
				r.Method, http.MethodGet, http.MethodPost, http.MethodDelete))
		return
	}

	if statusCode >= http.StatusBadRequest {
		cmn.InvalidHandlerWithMsg(w, r, respErr.Error(), statusCode)
		return
	}

	if response != nil {
		b := cmn.MustMarshal(response)
		if _, err := w.Write(b); err != nil {
			glog.Errorf("Failed to write to http response: %s.", err.Error())
		}
	}
}
