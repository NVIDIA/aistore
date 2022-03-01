// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"regexp"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/xact/xreg"
	jsoniter "github.com/json-iterator/go"
)

// NOTE: This request is internal so we can have asserts there.
// [METHOD] /v1/download
func (t *target) downloadHandler(w http.ResponseWriter, r *http.Request) {
	var (
		response   interface{}
		respErr    error
		statusCode int
	)
	if !t.ensureIntraControl(w, r, false /* from primary */) {
		return
	}
	rns := xreg.RenewDownloader(t, t.statsT)
	if rns.Err != nil {
		t.writeErr(w, r, rns.Err, http.StatusInternalServerError)
		return
	}
	xctn := rns.Entry.Get()
	downloaderXact := xctn.(*downloader.Downloader)
	switch r.Method {
	case http.MethodPost:
		if _, err := t.checkRESTItems(w, r, 0, false, apc.URLPathDownload.L); err != nil {
			return
		}
		var (
			uuid             = r.URL.Query().Get(apc.QparamUUID)
			dlb              = downloader.DlBody{}
			progressInterval = downloader.DownloadProgressInterval
		)
		if uuid == "" {
			debug.Assert(false)
			t.writeErrMsg(w, r, "missing UUID in query")
			return
		}
		if err := cmn.ReadJSON(w, r, &dlb); err != nil {
			return
		}

		dlBodyBase := downloader.DlBase{}
		if err := jsoniter.Unmarshal(dlb.RawMessage, &dlBodyBase); err != nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, t, "download message", cmn.BytesHead(dlb.RawMessage), err)
			t.writeErr(w, r, err)
			return
		}

		if dlBodyBase.ProgressInterval != "" {
			if dur, err := time.ParseDuration(dlBodyBase.ProgressInterval); err == nil {
				progressInterval = dur
			} else {
				t.writeErrf(w, r, "%s: invalid progress interval %q, err: %v",
					t.si, dlBodyBase.ProgressInterval, err)
				return
			}
		}

		bck := cluster.CloneBck(&dlBodyBase.Bck)
		if err := bck.Init(t.Bowner()); err != nil {
			t.writeErr(w, r, err)
			return
		}
		dlJob, err := downloader.ParseStartDownloadRequest(t, bck, uuid, dlb, downloaderXact)
		if err != nil {
			t.writeErr(w, r, err)
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
		response, statusCode, respErr = downloaderXact.Download(dlJob)
	case http.MethodGet:
		if _, err := t.checkRESTItems(w, r, 0, false, apc.URLPathDownload.L); err != nil {
			return
		}
		payload := &downloader.DlAdminBody{}
		if err := cmn.ReadJSON(w, r, payload); err != nil {
			return
		}
		if err := payload.Validate(false /*requireID*/); err != nil {
			debug.Assert(false)
			t.writeErr(w, r, err)
			return
		}
		if payload.ID != "" {
			response, statusCode, respErr = downloaderXact.JobStatus(payload.ID, payload.OnlyActiveTasks)
		} else {
			var (
				regex *regexp.Regexp
				err   error
			)
			if payload.Regex != "" {
				if regex, err = regexp.CompilePOSIX(payload.Regex); err != nil {
					t.writeErr(w, r, err)
					return
				}
			}
			response, statusCode, respErr = downloaderXact.ListJobs(regex)
		}
	case http.MethodDelete:
		items, err := t.checkRESTItems(w, r, 1, false, apc.URLPathDownload.L)
		if err != nil {
			return
		}
		payload := &downloader.DlAdminBody{}
		if err = cmn.ReadJSON(w, r, payload); err != nil {
			return
		}
		if err = payload.Validate(true /*requireID*/); err != nil {
			debug.Assert(false)
			t.writeErr(w, r, err)
			return
		}

		switch items[0] {
		case apc.Abort:
			response, statusCode, respErr = downloaderXact.AbortJob(payload.ID)
		case apc.Remove:
			response, statusCode, respErr = downloaderXact.RemoveJob(payload.ID)
		default:
			t.writeErrAct(w, r, items[0])
			return
		}
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodPost)
		return
	}

	if statusCode >= http.StatusBadRequest {
		t.writeErr(w, r, respErr, statusCode)
		return
	}

	if response != nil {
		b := cos.MustMarshal(response)
		if _, err := w.Write(b); err != nil {
			glog.Errorf("Failed to write to HTTP response, err: %v", err)
		}
	}
}
