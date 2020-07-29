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
	"github.com/NVIDIA/aistore/xaction"
	"github.com/tinylib/msgp/msgp"
)

// List objects returns a list of objects in a bucket (with optional prefix)
// Special case:
// If URL contains cachedonly=true then the function returns the list of
// locally cached objects. Paging is used to return a long list of objects
func (t *targetrunner) listObjects(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, actionMsg *aisMsg) (ok bool) {
	query := r.URL.Query()
	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.HeaderCallerID)
		glog.Infof("%s %s <= (%s)", r.Method, bck, pid)
	}

	var msg *cmn.SelectMsg
	if err := cmn.MorphMarshal(actionMsg.Value, &msg); err != nil {
		err := fmt.Errorf("unable to unmarshal 'value' in request to a cmn.SelectMsg: %v", actionMsg.Value)
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	xact, isNew, err := xaction.Registry.RenewBckListNewXact(t, bck, msg.UUID, msg)
	// Double check that xaction has not gone before starting page read.
	// Restart xaction if needed.
	if err == bcklist.ErrGone {
		xact, isNew, err = xaction.Registry.RenewBckListNewXact(t, bck, msg.UUID, msg)
	}
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	if isNew {
		smap := t.owner.smap.get()
		xact.AddNotif(&cmn.NotifXact{
			NotifBase: cmn.NotifBase{
				When: cmn.UponTerm,
				Dsts: smap.IC.Keys(),
				F:    t.xactCallerNotify,
			},
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

	mw := msgp.NewWriterSize(w, 10*cmn.KiB)
	if err := bckList.EncodeMsg(mw); err != nil {
		return false
	}
	return mw.Flush() == nil
}

func (t *targetrunner) bucketSummary(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, actionMsg *aisMsg) (ok bool) {
	query := r.URL.Query()
	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.HeaderCallerID)
		glog.Infof("%s %s <= (%s)", r.Method, bck, pid)
	}

	var msg cmn.SelectMsg
	if err := cmn.MorphMarshal(actionMsg.Value, &msg); err != nil {
		err := fmt.Errorf("unable to unmarshal 'value' in request to a cmn.SelectMsg: %v", actionMsg.Value)
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
	bck *cluster.Bck, smsg *cmn.SelectMsg) bool {
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
			_, err = xaction.Registry.RenewBckSummaryXact(ctx, t, bck, smsg)
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

	xact := xaction.Registry.GetXact(smsg.UUID)
	// task never started
	if xact == nil {
		s := fmt.Sprintf("Task %s not found", smsg.UUID)
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
