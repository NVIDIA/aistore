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
	"github.com/NVIDIA/aistore/ext/dload"
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
			query            = r.URL.Query()
			xactID           = query.Get(apc.QparamUUID)
			jobID            = query.Get(apc.QparamJobID)
			dlb              = dload.Body{}
			progressInterval = dload.DownloadProgressInterval
		)
		debug.Assertf(cos.IsValidUUID(xactID) && cos.IsValidUUID(jobID), "%q, %q", xactID, jobID)
		if err := cmn.ReadJSON(w, r, &dlb); err != nil {
			return
		}

		dlBodyBase := dload.Base{}
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

		xdl, err := t.renewdl(xactID)
		if err != nil {
			t.writeErr(w, r, err, http.StatusInternalServerError)
			return
		}
		dljob, err := dload.ParseStartRequest(t, bck, jobID, dlb, xdl)
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("Downloading: %s", dljob.ID())
		}

		dljob.AddNotif(&dload.NotifDownload{
			NotifBase: nl.NotifBase{
				When:     cluster.UponProgress,
				Interval: progressInterval,
				Dsts:     []string{equalIC},
				F:        t.callerNotifyFin,
				P:        t.callerNotifyProgress,
			},
		}, dljob)
		response, statusCode, respErr = xdl.Download(dljob)

	case http.MethodGet:
		if _, err := t.apiItems(w, r, 0, false, apc.URLPathDownload.L); err != nil {
			return
		}
		msg := &dload.AdminBody{}
		if err := cmn.ReadJSON(w, r, msg); err != nil {
			return
		}
		if err := msg.Validate(false /*requireID*/); err != nil {
			debug.Assert(false)
			t.writeErr(w, r, err)
			return
		}

		if msg.ID != "" {
			xactID := r.URL.Query().Get(apc.QparamUUID)
			debug.Assert(cos.IsValidUUID(xactID))
			xdl, err := t.renewdl(xactID)
			if err != nil {
				t.writeErr(w, r, err, http.StatusInternalServerError)
				return
			}
			response, statusCode, respErr = xdl.JobStatus(msg.ID, msg.OnlyActive)
		} else {
			var regex *regexp.Regexp
			if msg.Regex != "" {
				rgx, err := regexp.CompilePOSIX(msg.Regex)
				if err != nil {
					t.writeErr(w, r, err)
					return
				}
				regex = rgx
			}
			response, statusCode, respErr = dload.ListJobs(regex, msg.OnlyActive)
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

		payload := &dload.AdminBody{}
		if err = cmn.ReadJSON(w, r, payload); err != nil {
			return
		}
		if err = payload.Validate(true /*requireID*/); err != nil {
			debug.Assert(false)
			t.writeErr(w, r, err)
			return
		}

		xactID := r.URL.Query().Get(apc.QparamUUID)
		debug.Assertf(cos.IsValidUUID(xactID), "%q", xactID)
		xdl, err := t.renewdl(xactID)
		if err != nil {
			t.writeErr(w, r, err, http.StatusInternalServerError)
			return
		}
		if actdelete == apc.Abort {
			response, statusCode, respErr = xdl.AbortJob(payload.ID)
		} else { // apc.Remove
			response, statusCode, respErr = xdl.RemoveJob(payload.ID)
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

func (t *target) renewdl(xactID string) (*dload.Xact, error) {
	rns := xreg.RenewDownloader(t, t.statsT, xactID)
	if rns.Err != nil {
		return nil, rns.Err
	}
	xctn := rns.Entry.Get()
	return xctn.(*dload.Xact), nil
}
