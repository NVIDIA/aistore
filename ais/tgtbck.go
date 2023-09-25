// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"strconv"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/reb"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/NVIDIA/aistore/xact/xs"
)

//
// httpbck* handlers
//

// GET /v1/buckets[/bucket-name]
func (t *target) httpbckget(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.parseURL(w, r, 0, true, apc.URLPathBuckets.L)
	if err != nil {
		return
	}
	msg, err := t.readAisMsg(w, r)
	if err != nil {
		return
	}
	t.ensureLatestBMD(msg, r)

	var bckName string
	if len(apiItems) > 0 {
		bckName = apiItems[0]
	}
	switch msg.Action {
	case apc.ActList:
		dpq := dpqAlloc()
		if err := dpq.fromRawQ(r.URL.RawQuery); err != nil {
			dpqFree(dpq)
			t.writeErr(w, r, err)
			return
		}
		qbck, err := newQbckFromQ(bckName, nil, dpq)
		dpqFree(dpq)
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
		// list buckets if `qbck` indicates a bucket-type query
		// (see api.ListBuckets and the line below)
		if !qbck.IsBucket() {
			qbck.Name = msg.Name
			t.listBuckets(w, r, qbck)
			return
		}
		bck := meta.CloneBck((*cmn.Bck)(qbck))
		if err := bck.Init(t.owner.bmd); err != nil {
			if cmn.IsErrRemoteBckNotFound(err) {
				t.BMDVersionFixup(r)
				err = bck.Init(t.owner.bmd)
			}
			if err != nil {
				t.statsT.IncErr(stats.ListCount)
				t.writeErr(w, r, err)
				return
			}
		}
		begin := mono.NanoTime()
		if ok := t.listObjects(w, r, bck, msg); !ok {
			t.statsT.IncErr(stats.ListCount)
			return
		}
		delta := mono.SinceNano(begin)
		t.statsT.AddMany(
			cos.NamedVal64{Name: stats.ListCount, Value: 1},
			cos.NamedVal64{Name: stats.ListLatency, Value: delta},
		)
	case apc.ActSummaryBck:
		var (
			bsumMsg apc.BsummCtrlMsg
			query   = r.URL.Query()
		)
		qbck, err := newQbckFromQ(bckName, query, nil)
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
		if err := cos.MorphMarshal(msg.Value, &bsumMsg); err != nil {
			t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, msg.Action, msg.Value, err)
			return
		}
		// if in fact it is a specific named bucket
		bck := (*meta.Bck)(qbck)
		if qbck.IsBucket() {
			if err := bck.Init(t.owner.bmd); err != nil {
				if cmn.IsErrRemoteBckNotFound(err) {
					t.BMDVersionFixup(r)
					err = bck.Init(t.owner.bmd)
				}
				if err != nil {
					t.writeErr(w, r, err)
					return
				}
			}
		}
		t.bsumm(w, r, query, msg.Action, bck, &bsumMsg)
	default:
		t.writeErrAct(w, r, msg.Action)
	}
}

// there's a difference between looking for all (any) provider vs a specific one -
// in the former case the fact that (the corresponding backend is not configured)
// is not an error
func (t *target) listBuckets(w http.ResponseWriter, r *http.Request, qbck *cmn.QueryBcks) {
	var (
		bcks   cmn.Bcks
		config = cmn.GCO.Get()
		bmd    = t.owner.bmd.get()
		err    error
		code   int
	)
	if qbck.Provider != "" {
		if qbck.IsAIS() || qbck.IsHTTP() { // built-in providers
			bcks = bmd.Select(qbck)
		} else {
			bcks, code, err = t.blist(qbck, config, bmd)
			if err != nil {
				if _, ok := err.(*cmn.ErrMissingBackend); !ok {
					err = cmn.NewErrFailedTo(t, "list buckets", qbck.String(), err, code)
				}
				t.writeErr(w, r, err, code)
				return
			}
		}
	} else /* all providers */ {
		for provider := range apc.Providers {
			var buckets cmn.Bcks
			qbck.Provider = provider
			if qbck.IsAIS() || qbck.IsHTTP() {
				buckets = bmd.Select(qbck)
			} else {
				buckets, code, err = t.blist(qbck, config, bmd)
				if err != nil {
					if _, ok := err.(*cmn.ErrMissingBackend); !ok { // note on top of this func
						t.writeErr(w, r, err, code)
						return
					}
				}
			}
			bcks = append(bcks, buckets...)
		}
	}

	sort.Sort(bcks)
	t.writeJSON(w, r, bcks, "list-buckets")
}

func (t *target) blist(qbck *cmn.QueryBcks, config *cmn.Config, bmd *bucketMD) (bcks cmn.Bcks, errCode int, err error) {
	// validate
	debug.Assert(!qbck.IsAIS())
	if qbck.IsCloud() || qbck.IsHDFS() { // must be configured
		if config.Backend.Get(qbck.Provider) == nil {
			err = &cmn.ErrMissingBackend{Provider: qbck.Provider}
			return
		}
	} else if qbck.IsRemoteAIS() && qbck.Ns.IsAnyRemote() {
		if config.Backend.Get(apc.AIS) == nil {
			nlog.Warningln(&cmn.ErrMissingBackend{Provider: qbck.Provider, Msg: "no remote ais clusters"})
			return
			// otherwise go ahead and try to list below
		}
	}
	// hdfs cannot list
	if qbck.IsHDFS() {
		bcks = bmd.Select(qbck)
		return
	}
	backend := t.Backend((*meta.Bck)(qbck))
	if qbck.IsBucket() {
		var (
			bck = (*meta.Bck)(qbck)
			ctx = context.Background()
		)
		_, errCode, err = backend.HeadBucket(ctx, bck)
		if err == nil {
			bcks = cmn.Bcks{bck.Clone()}
		} else if errCode == http.StatusNotFound {
			err = nil
		}
	} else {
		bcks, errCode, err = backend.ListBuckets(*qbck)
	}
	if err == nil && len(bcks) > 1 {
		sort.Sort(bcks)
	}
	return
}

// listObjects returns a list of objects in a bucket (with optional prefix).
func (t *target) listObjects(w http.ResponseWriter, r *http.Request, bck *meta.Bck, actMsg *aisMsg) (ok bool) {
	var msg *apc.LsoMsg
	if err := cos.MorphMarshal(actMsg.Value, &msg); err != nil {
		t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, actMsg.Action, actMsg.Value, err)
		return
	}
	if !bck.IsAIS() && !msg.IsFlagSet(apc.LsObjCached) {
		maxRemotePageSize := t.Backend(bck).MaxPageSize()
		if msg.PageSize > maxRemotePageSize {
			t.writeErrf(w, r, "page size %d exceeds the supported maximum (%d)", msg.PageSize, maxRemotePageSize)
			return false
		}
		if msg.PageSize == 0 {
			msg.PageSize = maxRemotePageSize
		}
	}
	debug.Assert(msg.PageSize > 0 && msg.PageSize < 100000 && cos.IsValidUUID(msg.UUID))

	// (advanced) user-selected target to execute remote ls
	if msg.SID != "" {
		smap := t.owner.smap.get()
		if smap.GetTarget(msg.SID) == nil {
			err := &errNodeNotFound{"list-objects failure:", msg.SID, t.si, smap}
			t.writeErr(w, r, err)
			return
		}
	}

	var (
		xctn cluster.Xact
		rns  = xreg.RenewLso(t, bck, msg.UUID, msg)
	)
	// check that xaction hasn't finished prior to this page read, restart if needed
	if rns.Err == xs.ErrGone {
		runtime.Gosched()
		rns = xreg.RenewLso(t, bck, msg.UUID, msg)
	}
	if rns.Err != nil {
		t.writeErr(w, r, rns.Err)
		return
	}
	// run
	xctn = rns.Entry.Get()
	if !rns.IsRunning() {
		xact.GoRunW(xctn)
	}
	xls := xctn.(*xs.LsoXact)

	if config := cmn.GCO.Get(); config.FastV(4, cos.SmoduleAIS) {
		var s string
		if msg.ContinuationToken != "" {
			s = " cont=" + msg.ContinuationToken
		}
		if msg.SID != "" {
			s += " via t[" + msg.SID + "]"
		}
		nlog.Infoln(xls.Name() + s)
	}

	resp := xls.Do(msg) // NOTE: blocking request/response
	if resp.Err != nil {
		t.writeErr(w, r, resp.Err, resp.Status)
		return false
	}
	debug.Assert(resp.Lst.UUID == msg.UUID)

	// TODO: `Flags` have limited usability, consider to remove
	marked := xreg.GetRebMarked()
	if marked.Xact != nil || marked.Interrupted || reb.IsGFN() {
		resp.Lst.Flags = 1
	}

	return t.writeMsgPack(w, resp.Lst, "list_objects")
}

func (t *target) bsumm(w http.ResponseWriter, r *http.Request, q url.Values, action string, bck *meta.Bck, msg *apc.BsummCtrlMsg) {
	var (
		taskAction = q.Get(apc.QparamTaskAction)
	)
	if taskAction == apc.TaskStart {
		if action != apc.ActSummaryBck {
			t.writeErrAct(w, r, action)
			return
		}
		rns := xreg.RenewBckSummary(t, bck, msg)
		if rns.Err != nil {
			t.writeErr(w, r, rns.Err, http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		return
	}

	xctn, err := xreg.GetXact(msg.UUID)
	if err != nil {
		t.writeErr(w, r, err, http.StatusInternalServerError)
		return
	}

	// never started
	if xctn == nil {
		err := cos.NewErrNotFound("%s: x-%s[%s] (failed to start?)", t, apc.ActSummaryBck, msg.UUID)
		t._erris(w, r, q.Get(apc.QparamSilent), err, http.StatusNotFound)
		return
	}

	// still running
	if !xctn.Finished() {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	// finished
	result, err := xctn.Result()
	if err != nil {
		if cmn.IsErrBucketNought(err) {
			t.writeErr(w, r, err, http.StatusGone)
		} else {
			t.writeErr(w, r, err)
		}
		return
	}
	if taskAction == apc.TaskResult {
		// return the final result only if it is requested explicitly
		t.writeJSON(w, r, result, "bucket-summary")
	}
}

// DELETE { action } /v1/buckets/bucket-name
// (evict | delete) (list | range)
func (t *target) httpbckdelete(w http.ResponseWriter, r *http.Request, apireq *apiRequest) {
	msg := aisMsg{}
	if err := readJSON(w, r, &msg); err != nil {
		return
	}
	if err := t.parseReq(w, r, apireq); err != nil {
		return
	}
	if err := apireq.bck.Init(t.owner.bmd); err != nil {
		if cmn.IsErrRemoteBckNotFound(err) {
			t.BMDVersionFixup(r)
			err = apireq.bck.Init(t.owner.bmd)
		}
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
	}

	switch msg.Action {
	case apc.ActEvictRemoteBck:
		keepMD := cos.IsParseBool(apireq.query.Get(apc.QparamKeepRemote))
		// HDFS buckets will always keep metadata so they can re-register later
		if apireq.bck.IsHDFS() || keepMD {
			nlp := newBckNLP(apireq.bck)
			nlp.Lock()
			defer nlp.Unlock()

			err := fs.DestroyBucket(msg.Action, apireq.bck.Bucket(), apireq.bck.Props.BID)
			if err != nil {
				t.writeErr(w, r, err)
				return
			}
			// Recreate bucket directories (now empty), since bck is still in BMD
			errs := fs.CreateBucket(apireq.bck.Bucket(), false /*nilbmd*/)
			if len(errs) > 0 {
				debug.AssertNoErr(errs[0])
				t.writeErr(w, r, errs[0]) // only 1 err is possible for 1 bck
			}
		}
	case apc.ActDeleteObjects, apc.ActEvictObjects:
		lrMsg := &apc.ListRange{}
		if err := cos.MorphMarshal(msg.Value, lrMsg); err != nil {
			t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, msg.Action, msg.Value, err)
			return
		}
		for _, name := range lrMsg.ObjNames {
			if !t.isValidObjname(w, r, name) {
				return
			}
		}
		rns := xreg.RenewEvictDelete(msg.UUID, t, msg.Action /*xaction kind*/, apireq.bck, lrMsg)
		if rns.Err != nil {
			t.writeErr(w, r, rns.Err)
			return
		}
		xctn := rns.Entry.Get()
		xctn.AddNotif(&xact.NotifXact{
			Base: nl.Base{
				When: cluster.UponTerm,
				Dsts: []string{equalIC},
				F:    t.notifyTerm,
			},
			Xact: xctn,
		})
		go xctn.Run(nil)
	default:
		t.writeErrAct(w, r, msg.Action)
	}
}

// POST /v1/buckets/bucket-name
func (t *target) httpbckpost(w http.ResponseWriter, r *http.Request, apireq *apiRequest) {
	msg, err := t.readAisMsg(w, r)
	if err != nil {
		return
	}
	if err := t.parseReq(w, r, apireq); err != nil {
		return
	}

	t.ensureLatestBMD(msg, r)

	if err := apireq.bck.Init(t.owner.bmd); err != nil {
		if cmn.IsErrRemoteBckNotFound(err) {
			t.BMDVersionFixup(r)
			err = apireq.bck.Init(t.owner.bmd)
		}
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
	}

	switch msg.Action {
	case apc.ActPrefetchObjects:
		var (
			err   error
			lrMsg = &apc.ListRange{}
		)
		if !apireq.bck.IsRemote() {
			t.writeErrf(w, r, "%s: expecting remote bucket, got %s, action=%s",
				t.si, apireq.bck, msg.Action)
			return
		}
		if err = cos.MorphMarshal(msg.Value, lrMsg); err != nil {
			t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, msg.Action, msg.Value, err)
			return
		}
		rns := xreg.RenewPrefetch(msg.UUID, t, apireq.bck, lrMsg)
		xctn := rns.Entry.Get()
		go xctn.Run(nil)
	default:
		t.writeErrAct(w, r, msg.Action)
	}
}

// HEAD /v1/buckets/bucket-name
func (t *target) httpbckhead(w http.ResponseWriter, r *http.Request, apireq *apiRequest) {
	var (
		bucketProps cos.StrKVs
		err         error
		ctx         = context.Background()
		hdr         = w.Header()
		code        int
	)
	if err = t.parseReq(w, r, apireq); err != nil {
		return
	}
	inBMD := true
	if err = apireq.bck.Init(t.owner.bmd); err != nil {
		if !cmn.IsErrRemoteBckNotFound(err) { // is ais
			t.writeErr(w, r, err)
			return
		}
		inBMD = false
	}
	if cmn.FastV(5, cos.SmoduleAIS) {
		pid := apireq.query.Get(apc.QparamProxyID)
		nlog.Infof("%s %s <= %s", r.Method, apireq.bck, pid)
	}

	debug.Assert(!apireq.bck.IsAIS())

	if apireq.bck.IsHTTP() {
		originalURL := apireq.query.Get(apc.QparamOrigURL)
		ctx = context.WithValue(ctx, cos.CtxOriginalURL, originalURL)
		if !inBMD && originalURL == "" {
			err = cmn.NewErrRemoteBckNotFound(apireq.bck.Bucket())
			t.writeErr(w, r, err, http.StatusNotFound, Silent)
			return
		}
	}
	// + cloud
	bucketProps, code, err = t.Backend(apireq.bck).HeadBucket(ctx, apireq.bck)
	if err != nil {
		if !inBMD {
			if code == http.StatusNotFound {
				err = cmn.NewErrRemoteBckNotFound(apireq.bck.Bucket())
				t.writeErr(w, r, err, code, Silent)
			} else {
				err = cmn.NewErrFailedTo(t, "HEAD remote bucket", apireq.bck, err, code)
				t._erris(w, r, apireq.query.Get(apc.QparamSilent), err, code)
			}
			return
		}
		nlog.Warningf("%s: bucket %s, err: %v(%d)", t, apireq.bck, err, code)
		bucketProps = make(cos.StrKVs)
		bucketProps[apc.HdrBackendProvider] = apireq.bck.Provider
		bucketProps[apc.HdrRemoteOffline] = strconv.FormatBool(apireq.bck.IsRemote())
	}
	for k, v := range bucketProps {
		if k == apc.HdrBucketVerEnabled && apireq.bck.Props != nil {
			if curr := strconv.FormatBool(apireq.bck.VersionConf().Enabled); curr != v {
				// e.g., change via vendor-provided CLI and similar
				nlog.Errorf("%s: %s versioning got out of sync: %s != %s", t, apireq.bck, v, curr)
			}
		}
		hdr.Set(k, v)
	}
}
