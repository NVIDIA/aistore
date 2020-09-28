// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"fmt"
	"net/http"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/bcklist"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/notifications"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/registry"
)

// listObjects returns a list of objects in a bucket (with optional prefix).
func (t *targetrunner) listObjects(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, actionMsg *aisMsg) (ok bool) {
	query := r.URL.Query()
	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.HeaderCallerID)
		glog.Infof("%s %s <= (%s)", r.Method, bck, pid)
	}

	var msg *cmn.SelectMsg
	if err := cmn.MorphMarshal(actionMsg.Value, &msg); err != nil {
		t.invalmsghdlrf(w, r, "unable to unmarshal 'value' in request to a cmn.SelectMsg: %v", actionMsg.Value)
		return
	}

	if !bck.IsAIS() && !msg.IsFlagSet(cmn.SelectCached) {
		maxCloudPageSize := t.Cloud(bck).MaxPageSize()
		if msg.PageSize == 0 {
			msg.PageSize = maxCloudPageSize
		} else if msg.PageSize > maxCloudPageSize {
			t.invalmsghdlrf(w, r, "page size exceeds the maximum value (got: %d, max expected: %d)", msg.PageSize, maxCloudPageSize)
			return false
		}
	}
	cmn.Assert(msg.PageSize != 0)

	xact, isNew, err := registry.Registry.RenewBckListNewXact(t, bck, msg.UUID, msg)
	// Double check that xaction has not gone before starting page read.
	// Restart xaction if needed.
	if err == bcklist.ErrGone {
		xact, isNew, err = registry.Registry.RenewBckListNewXact(t, bck, msg.UUID, msg)
	}
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	if isNew {
		xact.AddNotif(&xaction.NotifXact{
			NotifBase: notifications.NotifBase{When: cluster.UponTerm, Ty: notifications.NotifCache, Dsts: []string{equalIC}, F: t.callerNotifyFin},
		})

		go xact.Run()
	}

	bckList, status, err := t.waitBckListResp(xact, msg)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), status)
		return false
	}

	debug.Assert(status == http.StatusOK)
	debug.Assert(bckList.UUID != "")

	return t.writeMsgPack(w, r, bckList, "list_objects")
}

func (t *targetrunner) bucketSummary(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, actionMsg *aisMsg) (ok bool) {
	query := r.URL.Query()
	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.HeaderCallerID)
		glog.Infof("%s %s <= (%s)", r.Method, bck, pid)
	}

	var msg cmn.BucketSummaryMsg
	if err := cmn.MorphMarshal(actionMsg.Value, &msg); err != nil {
		err := fmt.Errorf("unable to unmarshal 'value' in request to a cmn.BucketSummaryMsg: %v", actionMsg.Value)
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	ok = t.doAsync(w, r, actionMsg.Action, bck, &msg)
	return
}

func (t *targetrunner) waitBckListResp(xact *bcklist.BckListTask, msg *cmn.SelectMsg) (*cmn.BucketList, int, error) {
	ch := make(chan *bcklist.BckListResp) // unbuffered
	xact.Do(msg, ch)
	resp := <-ch
	return resp.BckList, resp.Status, resp.Err
}

// asynchronous bucket request
// - creates a new task that runs in background
// - returns status of a running task by its ID
// - returns the result of a task by its ID
func (t *targetrunner) doAsync(w http.ResponseWriter, r *http.Request, action string,
	bck *cluster.Bck, msg *cmn.BucketSummaryMsg) bool {
	var (
		query      = r.URL.Query()
		taskAction = query.Get(cmn.URLParamTaskAction)
		silent     = cmn.IsParseBool(query.Get(cmn.URLParamSilent))
		ctx        = context.Background()
	)
	if taskAction == cmn.TaskStart {
		var (
			status = http.StatusInternalServerError
			err    error
		)

		switch action {
		case cmn.ActSummaryBucket:
			_, err = registry.Registry.RenewBckSummaryXact(ctx, t, bck, msg)
		default:
			t.invalmsghdlrf(w, r, "invalid action: %s", action)
			return false
		}

		if err != nil {
			t.invalmsghdlr(w, r, err.Error(), status)
			return false
		}

		w.WriteHeader(http.StatusAccepted)
		return true
	}

	xact := registry.Registry.GetXact(msg.UUID)
	// task never started
	if xact == nil {
		s := fmt.Sprintf("Task %s not found", msg.UUID)
		if silent {
			t.invalmsghdlrsilent(w, r, s, http.StatusNotFound)
		} else {
			t.invalmsghdlr(w, r, s, http.StatusNotFound)
		}
		return false
	}

	// task still running
	if !xact.Finished() {
		w.WriteHeader(http.StatusAccepted)
		return true
	}
	// task has finished
	result, err := xact.Result()
	if err != nil {
		if cmn.IsErrBucketNought(err) {
			t.invalmsghdlr(w, r, err.Error(), http.StatusGone)
		} else {
			t.invalmsghdlr(w, r, err.Error())
		}
		return false
	}

	if taskAction == cmn.TaskResult {
		// return the final result only if it is requested explicitly
		return t.writeJSON(w, r, result, "")
	}

	return true
}
