// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ext/dload"
	"github.com/NVIDIA/aistore/fs"
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
		// disallow to run when above high wm (let alone OOS)
		cs := fs.Cap()
		if err := cs.Err(); err != nil {
			t.writeErr(w, r, err, http.StatusInsufficientStorage)
			return
		}
		if _, err := t.parseURL(w, r, apc.URLPathDownload.L, 0, false); err != nil {
			return
		}
		var (
			query            = r.URL.Query()
			xid              = query.Get(apc.QparamUUID)
			jobID            = query.Get(apc.QparamJobID)
			dlb              = dload.Body{}
			progressInterval = dload.DownloadProgressInterval
		)
		debug.Assertf(cos.IsValidUUID(xid) && cos.IsValidUUID(jobID), "%q, %q", xid, jobID)
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

		bck := meta.CloneBck(&dlBodyBase.Bck)
		if err := bck.Init(t.Bowner()); err != nil {
			t.writeErr(w, r, err)
			return
		}

		xdl, err := renewdl(xid, bck)
		if err != nil {
			t.writeErr(w, r, err, http.StatusInternalServerError)
			return
		}
		dljob, err := dload.ParseStartRequest(bck, jobID, dlb, xdl)
		if err != nil {
			xdl.Abort(err)
			t.writeErr(w, r, err)
			return
		}
		if cmn.Rom.FastV(4, cos.SmoduleAIS) {
			nlog.Infoln("Downloading:", dljob.ID())
		}

		dljob.AddNotif(&dload.NotifDownload{
			Base: nl.Base{
				When:     core.UponProgress,
				Interval: progressInterval,
				Dsts:     []string{equalIC},
				F:        t.notifyTerm,
				P:        t.notifyProgress,
			},
		}, dljob)
		response, statusCode, respErr = xdl.Download(dljob)

	case http.MethodGet:
		if _, err := t.parseURL(w, r, apc.URLPathDownload.L, 0, false); err != nil {
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
			xid := r.URL.Query().Get(apc.QparamUUID)
			debug.Assert(cos.IsValidUUID(xid))
			xdl, err := renewdl(xid, nil)
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
		items, err := t.parseURL(w, r, apc.URLPathDownload.L, 1, false)
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

		xid := r.URL.Query().Get(apc.QparamUUID)
		debug.Assertf(cos.IsValidUUID(xid), "%q", xid)
		xdl, err := renewdl(xid, nil)
		if err != nil {
			t.writeErr(w, r, err, http.StatusInternalServerError)
			return
		}
		if actdelete == apc.Abort {
			response, statusCode, respErr = xdl.AbortJob(payload.ID)
		} else { // apc.Remove
			response, statusCode, respErr = xdl.RemoveJob(payload.ID)
		}

		// keep it quiet
		if statusCode == http.StatusNotFound {
			debug.Assert(response == nil)
			t.writeErr(w, r, respErr, statusCode, Silent)
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
		w.Header().Set(cos.HdrContentType, cos.ContentJSON)
		w.Header().Set(cos.HdrContentLength, strconv.Itoa(len(b)))
		w.Write(b)
	}
}

func renewdl(xid string, bck *meta.Bck) (*dload.Xact, error) {
	rns := xreg.RenewDownloader(xid, bck)
	if rns.Err != nil {
		return nil, rns.Err
	}
	xctn := rns.Entry.Get()
	return xctn.(*dload.Xact), nil
}
