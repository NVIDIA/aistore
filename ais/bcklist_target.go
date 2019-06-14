// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
)

// List bucket returns a list of objects in a bucket (with optional prefix)
// Special case:
// If URL contains cachedonly=true then the function returns the list of
// locally cached objects. Paging is used to return a long list of objects
func (t *targetrunner) listbucket(w http.ResponseWriter, r *http.Request, bucket string,
	bckIsLocal bool, actionMsg *actionMsgInternal) (ok bool) {
	query := r.URL.Query()
	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.URLParamProxyID)
		bmd := t.bmdowner.get()
		glog.Infof("%s %s <= (%s)", r.Method, bmd.Bstring(bucket, bckIsLocal), pid)
	}

	var msg cmn.SelectMsg
	getMsgJSON := cmn.MustMarshal(actionMsg.Value)
	if err := jsoniter.Unmarshal(getMsgJSON, &msg); err != nil {
		errstr := fmt.Sprintf("Unable to unmarshal 'value' in request to a cmn.SelectMsg: %v", actionMsg.Value)
		t.invalmsghdlr(w, r, errstr)
		return
	}
	ok = t.listBucketAsync(w, r, bucket, bckIsLocal, &msg)
	return
}

// asynchronous list bucket request
// - creates a new task that collects objects in background
// - returns status of a running task by its ID
// - returns the result of a task by its ID
// TODO: support cloud buckets
func (t *targetrunner) listBucketAsync(w http.ResponseWriter, r *http.Request, bucket string, bckIsLocal bool, msg *cmn.SelectMsg) bool {
	query := r.URL.Query()
	taskAction := query.Get(cmn.URLParamTaskAction)
	useCache, _ := cmn.ParseBool(r.URL.Query().Get(cmn.URLParamCached))
	silent, _ := cmn.ParseBool(r.URL.Query().Get(cmn.URLParamSilent))
	ctx := t.contextWithAuth(r.Header)
	// create task call
	if taskAction == cmn.ListTaskStart {
		xact := t.xactions.renewBckListXact(ctx, t, bucket, bckIsLocal, msg, useCache)
		if xact == nil {
			return false
		}

		w.WriteHeader(http.StatusAccepted)
		return true
	}

	xactStats := t.xactions.GetTaskXact(msg.TaskID)
	// task never started
	if xactStats == nil {
		if silent {
			t.invalmsghdlrsilent(w, r, "Task not found", http.StatusNotFound)
		} else {
			t.invalmsghdlr(w, r, "Task not found", http.StatusNotFound)
		}
		return false
	}
	// task still running
	if !xactStats.Get().Finished() {
		w.WriteHeader(http.StatusAccepted)
		return true
	}
	// task has finished
	xtask, ok := xactStats.Get().(*xactBckListTask)
	if !ok || xtask == nil {
		t.invalmsghdlr(w, r, fmt.Sprintf("Invalid task type: %T", xactStats.Get()), http.StatusInternalServerError)
		return false
	}
	st := (*taskState)(xtask.res.Load())
	if st.Err != nil {
		t.invalmsghdlr(w, r, fmt.Sprintf("Task failed: %v", st.Err), http.StatusInternalServerError)
		return false
	}

	if msg.Fast {
		if bckList, ok := st.Result.(*cmn.BucketList); ok && bckList != nil {
			const minloaded = 10 // check that many randomly-selected
			if len(bckList.Entries) > minloaded {
				go func(bckEntries []*cmn.BucketEntry) {
					var (
						bckProvider = cmn.BckProviderFromLocal(bckIsLocal)
						l           = len(bckEntries)
						m           = l / minloaded
						loaded      int
					)
					if l < minloaded {
						return
					}
					for i := 0; i < l; i += m {
						lom, errstr := cluster.LOM{T: t, Bucket: bucket,
							Objname: bckEntries[i].Name, BucketProvider: bckProvider}.Init()
						if errstr == "" && lom.IsLoaded() { // loaded?
							loaded++
						}
					}
					renew := loaded < minloaded/2
					if glog.FastV(4, glog.SmoduleAIS) {
						glog.Errorf("%s: loaded %d/%d, renew=%t", t.si, loaded, minloaded, renew)
					}
					if renew {
						t.xactions.renewBckLoadLomCache(bucket, t, bckIsLocal)
					}
				}(bckList.Entries)
			}
		}
	}

	if taskAction == cmn.ListTaskResult {
		// return the final result only if it is requested explicitly
		body := cmn.MustMarshal(st.Result)
		return t.writeJSON(w, r, body, "listbucket")
	}

	// default action: return task status 200 = successfully completed
	w.WriteHeader(http.StatusOK)
	return true
}
