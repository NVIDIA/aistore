// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
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

// [METHOD] /v1/download
func (t *target) downloadHandler(w http.ResponseWriter, r *http.Request) {
	var (
		response   any
		respErr    error
		statusCode int
	)
	if !t.ensureIntraControl(w, r, false /* from primary */) {
		return
	}

	switch r.Method {
	case http.MethodPost:
		if _, err := t.apiItems(w, r, 0, false, apc.URLPathDownload.L); err != nil {
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
			err = fmt.Errorf(cmn.FmtErrUnmarshal, t, "download message", cos.BHead(dlb.RawMessage), err)
			t.writeErr(w, r, err)
			return
		}

		if dlBodyBase.ProgressInterval != "" {
			dur, err := time.ParseDuration(dlBodyBase.ProgressInterval)
			if err != nil {
				t.writeErrf(w, r, "%s: invalid progress interval %q: %v", t, dlBodyBase.ProgressInterval, err)
				return
			}
			progressInterval = dur
		}

		bck := cluster.CloneBck(&dlBodyBase.Bck)
		if err := bck.Init(t.Bowner()); err != nil {
			t.writeErr(w, r, err)
			return
		}

		dlxact, err := t.renewdl()
		if err != nil {
			t.writeErr(w, r, err, http.StatusInternalServerError)
			return
		}
		dlJob, err := downloader.ParseStartDownloadRequest(t, bck, uuid, dlb, dlxact)
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
		response, statusCode, respErr = dlxact.Download(dlJob)
	case http.MethodGet:
		if _, err := t.apiItems(w, r, 0, false, apc.URLPathDownload.L); err != nil {
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
			// TODO -- FIXME: don't renew
			dlxact, err := t.renewdl()
			if err != nil {
				t.writeErr(w, r, err, http.StatusInternalServerError)
				return
			}

			response, statusCode, respErr = dlxact.JobStatus(payload.ID, payload.OnlyActiveTasks)
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
			response, statusCode, respErr = downloader.ListJobs(regex)
		}
	case http.MethodDelete:
		items, err := t.apiItems(w, r, 1, false, apc.URLPathDownload.L)
		if err != nil {
			return
		}
		actdelete := items[0]
		if actdelete != apc.Abort && actdelete != apc.Remove {
			t.writeErrAct(w, r, actdelete)
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

		dlxact, err := t.renewdl()
		if err != nil {
			t.writeErr(w, r, err, http.StatusInternalServerError)
			return
		}
		if actdelete == apc.Abort {
			response, statusCode, respErr = dlxact.AbortJob(payload.ID)
		} else { // apc.Remove
			response, statusCode, respErr = dlxact.RemoveJob(payload.ID)
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
		w.Write(cos.MustMarshal(response))
	}
}

func (t *target) renewdl() (*downloader.Xact, error) {
	rns := xreg.RenewDownloader(t, t.statsT)
	if rns.Err != nil {
		return nil, rns.Err
	}
	xctn := rns.Entry.Get()
	return xctn.(*downloader.Xact), nil
}
