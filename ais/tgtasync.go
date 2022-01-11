// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"net/http"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/reb"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/NVIDIA/aistore/xs"
)

// listObjects returns a list of objects in a bucket (with optional prefix).
func (t *target) listObjects(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, actMsg *aisMsg) (ok bool) {
	var (
		msg   *cmn.ListObjsMsg
		query = r.URL.Query()
	)
	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.HdrCallerID)
		glog.Infof("%s %s <= (%s)", r.Method, bck, pid)
	}
	if err := cos.MorphMarshal(actMsg.Value, &msg); err != nil {
		t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, actMsg.Action, actMsg.Value, err)
		return
	}
	if !bck.IsAIS() && !msg.IsFlagSet(cmn.LsPresent) {
		maxCloudPageSize := t.Backend(bck).MaxPageSize()
		if msg.PageSize > maxCloudPageSize {
			t.writeErrf(w, r, "page size %d exceeds the supported maximum (%d)", msg.PageSize, maxCloudPageSize)
			return false
		}
		if msg.PageSize == 0 {
			msg.PageSize = maxCloudPageSize
		}
	}
	debug.Assert(msg.PageSize != 0)
	debug.Assert(cos.IsValidUUID(msg.UUID))

	rns := xreg.RenewObjList(t, bck, msg.UUID, msg)
	xctn := rns.Entry.Get()
	// Double check that xaction has not gone before starting page read.
	// Restart xaction if needed.
	if rns.Err == xs.ErrGone {
		rns = xreg.RenewObjList(t, bck, msg.UUID, msg)
		xctn = rns.Entry.Get()
	}
	if rns.Err != nil {
		t.writeErr(w, r, rns.Err)
		return
	}
	if !rns.IsRunning() {
		go xctn.Run(nil)
	}

	resp := xctn.(*xs.ObjListXact).Do(msg)
	if resp.Err != nil {
		t.writeErr(w, r, resp.Err, resp.Status)
		return false
	}

	debug.Assert(resp.Status == http.StatusOK)
	debug.Assert(resp.BckList.UUID != "")

	if fs.MarkerExists(cmn.RebalanceMarker) || reb.IsActiveGFN() {
		resp.BckList.Flags |= cmn.BckListFlagRebalance
	}

	return t.writeMsgPack(w, r, resp.BckList, "list_objects")
}

func (t *target) bucketSummary(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, actMsg *aisMsg) {
	query := r.URL.Query()
	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.HdrCallerID)
		glog.Infof("%s %s <= (%s)", r.Method, bck, pid)
	}

	var msg cmn.BucketSummaryMsg
	if err := cos.MorphMarshal(actMsg.Value, &msg); err != nil {
		t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, actMsg.Action, actMsg.Value, err)
		return
	}
	t.doAsync(w, r, actMsg.Action, bck, &msg)
}

// asynchronous bucket request
// - creates a new task that runs in background
// - returns status of a running task by its ID
// - returns the result of a task by its ID
func (t *target) doAsync(w http.ResponseWriter, r *http.Request, action string, bck *cluster.Bck,
	msg *cmn.BucketSummaryMsg) {
	var (
		query      = r.URL.Query()
		taskAction = query.Get(cmn.URLParamTaskAction)
		silent     = cos.IsParseBool(query.Get(cmn.URLParamSilent))
		ctx        = context.Background()
	)
	if taskAction == cmn.TaskStart {
		if action != cmn.ActSummaryBck {
			t.writeErrAct(w, r, action)
			return
		}
		rns := xreg.RenewBckSummary(ctx, t, bck, msg)
		if rns.Err != nil {
			t.writeErr(w, r, rns.Err, http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		return
	}

	xctn := xreg.GetXact(msg.UUID)
	// task never started
	if xctn == nil {
		err := cmn.NewErrNotFound("%s: task %q", t.si, msg.UUID)
		if silent {
			t.writeErrSilent(w, r, err, http.StatusNotFound)
		} else {
			t.writeErr(w, r, err, http.StatusNotFound)
		}
		return
	}

	// task still running
	if !xctn.Finished() {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	// task has finished
	result, err := xctn.Result()
	if err != nil {
		if cmn.IsErrBucketNought(err) {
			t.writeErr(w, r, err, http.StatusGone)
		} else {
			t.writeErr(w, r, err)
		}
		return
	}

	if taskAction == cmn.TaskResult {
		// return the final result only if it is requested explicitly
		t.writeJSON(w, r, result, "")
	}
}
