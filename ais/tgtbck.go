// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/reb"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/NVIDIA/aistore/xact/xs"
)

//
// httpbck* handlers: list buckets/objects, summary (ditto), delete(bucket), head(bucket)
//

// GET /v1/buckets[/bucket-name]
func (t *target) httpbckget(w http.ResponseWriter, r *http.Request, dpq *dpq) {
	apiItems, err := t.parseURL(w, r, apc.URLPathBuckets.L, 0, true)
	if err != nil {
		return
	}
	if err = t.checkIntraCall(r.Header, false); err != nil {
		t.writeErr(w, r, err)
		return
	}
	msg, err := t.readAisMsg(w, r)
	if err != nil {
		return
	}
	t.ensureLatestBMD(msg, r)

	if err := dpq.parse(r.URL.RawQuery); err != nil {
		t.writeErr(w, r, err)
		return
	}

	switch msg.Action {
	case apc.ActList:
		var bckName string
		if len(apiItems) > 0 {
			bckName = apiItems[0]
		}
		qbck, err := newQbckFromQ(bckName, nil, dpq)
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
				if dpq.dontAddRemote {
					// don't add it - proceed anyway (TODO: assert(wantOnlyRemote) below)
					err = nil
				} else {
					// in an inlikely case updated BMD's in flight
					t.BMDVersionFixup(r)
					err = bck.Init(t.owner.bmd)
				}
			}
			if err != nil {
				t.writeErr(w, r, err)
				return
			}
		}
		var (
			begin = mono.NanoTime()
			lsmsg *apc.LsoMsg
		)
		if err := cos.MorphMarshal(msg.Value, &lsmsg); err != nil {
			t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, msg.Action, msg.Value, err)
			return
		}
		if !cos.IsValidUUID(lsmsg.UUID) {
			debug.Assert(false, lsmsg.UUID)
			t.writeErrf(w, r, "list-objects: invalid UUID %q", lsmsg.UUID)
			return
		}
		if ok := t.listObjects(w, r, bck, lsmsg); !ok {
			t.statsT.IncBck(stats.ErrListCount, bck.Bucket())
			return
		}

		delta := mono.SinceNano(begin)
		vlabs := map[string]string{stats.VarlabBucket: bck.Cname("")}
		t.statsT.IncWith(stats.ListCount, vlabs)
		t.statsT.AddWith(
			cos.NamedVal64{Name: stats.ListLatency, Value: delta, VarLabs: vlabs},
		)
	case apc.ActSummaryBck:
		var bucket, phase string // txn
		if len(apiItems) == 0 {
			t.writeErrURL(w, r)
			return
		}
		if len(apiItems) == 1 {
			phase = apiItems[0]
		} else {
			bucket, phase = apiItems[0], apiItems[1]
		}
		if phase != apc.ActBegin && phase != apc.ActQuery {
			t.writeErrURL(w, r)
			return
		}

		qbck, err := newQbckFromQ(bucket, nil, dpq)
		if err != nil {
			t.writeErr(w, r, err)
			return
		}

		var bsumMsg apc.BsummCtrlMsg
		if err := cos.MorphMarshal(msg.Value, &bsumMsg); err != nil {
			t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, msg.Action, msg.Value, err)
			return
		}
		// if in fact it is a specific named bucket
		bck := (*meta.Bck)(qbck)
		if qbck.IsBucket() {
			if err := bck.Init(t.owner.bmd); err != nil {
				if cmn.IsErrRemoteBckNotFound(err) {
					if bsumMsg.DontAddRemote || dpq.dontAddRemote {
						// don't add it - proceed anyway
						err = nil
					} else {
						t.BMDVersionFixup(r)
						err = bck.Init(t.owner.bmd)
					}
				}
				if err != nil {
					t.writeErr(w, r, err)
					return
				}
			}
		}
		t.bsumm(w, r, phase, bck, &bsumMsg, dpq)
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
		if qbck.IsAIS() || qbck.IsHT() { // built-in providers
			bcks = bmd.Select(qbck)
		} else {
			bcks, code, err = t.blist(qbck, config)
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
			if qbck.IsAIS() || qbck.IsHT() {
				buckets = bmd.Select(qbck)
			} else {
				buckets, code, err = t.blist(qbck, config)
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

func (t *target) blist(qbck *cmn.QueryBcks, config *cmn.Config) (bcks cmn.Bcks, ecode int, err error) {
	// validate
	debug.Assert(!qbck.IsAIS())
	if qbck.IsCloud() { // must be configured
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
	backend := t.Backend((*meta.Bck)(qbck))
	if qbck.IsBucket() {
		var (
			bck = (*meta.Bck)(qbck)
			ctx = context.Background()
		)
		_, ecode, err = backend.HeadBucket(ctx, bck)
		if err == nil {
			bcks = cmn.Bcks{bck.Clone()}
		} else if ecode == http.StatusNotFound {
			err = nil
		}
	} else {
		bcks, ecode, err = backend.ListBuckets(*qbck)
	}
	if err == nil && len(bcks) > 1 {
		sort.Sort(bcks)
	}
	return
}

// returns `cmn.LsoRes` containing object names and (requested) props
// control/scope - via `apc.LsoMsg`
func (t *target) listObjects(w http.ResponseWriter, r *http.Request, bck *meta.Bck, lsmsg *apc.LsoMsg) (ok bool) {
	// (advanced) user-selected target to execute remote ls
	if lsmsg.SID != "" {
		smap := t.owner.smap.get()
		if smap.GetTarget(lsmsg.SID) == nil {
			err := &errNodeNotFound{t.si, smap, "list-objects failure:", lsmsg.SID}
			t.writeErr(w, r, err)
			return
		}
	}

	var (
		xctn core.Xact
		rns  = xreg.RenewLso(bck, lsmsg.UUID, lsmsg, r.Header)
	)
	// check that xaction hasn't finished prior to this page read, restart if needed
	if rns.Err == xs.ErrGone {
		runtime.Gosched()
		rns = xreg.RenewLso(bck, lsmsg.UUID, lsmsg, r.Header)
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

	// NOTE: blocking next-page request
	resp := xls.Do(lsmsg)
	if resp.Err != nil {
		t.writeErr(w, r, resp.Err, resp.Status)
		return false
	}
	debug.Assert(resp.Lst.UUID == lsmsg.UUID)

	// TODO: `Flags` have limited usability, consider to remove
	marked := xreg.GetRebMarked()
	if marked.Xact != nil || marked.Interrupted || reb.IsGFN() {
		resp.Lst.Flags = 1
	}

	return t.writeMsgPack(w, resp.Lst, "list_objects")
}

func (t *target) bsumm(w http.ResponseWriter, r *http.Request, phase string, bck *meta.Bck, msg *apc.BsummCtrlMsg, dpq *dpq) {
	if phase == apc.ActBegin {
		rns := xreg.RenewBckSummary(bck, msg)
		if rns.Err != nil {
			t.writeErr(w, r, rns.Err, http.StatusInternalServerError, Silent)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		return
	}

	debug.Assert(phase == apc.ActQuery, phase)
	xctn, err := xreg.GetXact(msg.UUID) // vs. hk.OldAgeX removal
	if err != nil {
		t.writeErr(w, r, err, http.StatusInternalServerError)
		return
	}

	// never started
	if xctn == nil {
		err := cos.NewErrNotFound(t, apc.ActSummaryBck+" job "+msg.UUID)
		t._erris(w, r, err, http.StatusNotFound, dpq.silent)
		return
	}

	xsumm := xctn.(*xs.XactNsumm)
	result, err := xsumm.Result()
	if err != nil {
		if cmn.IsErrBucketNought(err) {
			t.writeErr(w, r, err, http.StatusGone)
		} else {
			t.writeErr(w, r, err)
		}
		return
	}
	if !xctn.Finished() {
		if len(result) == 0 {
			w.WriteHeader(http.StatusAccepted)
		} else {
			w.WriteHeader(http.StatusPartialContent)
		}
	}
	t.writeJSON(w, r, result, xsumm.Name())
}

// DELETE { action } /v1/buckets/bucket-name
// (evict | delete) (list | range)
func (t *target) httpbckdelete(w http.ResponseWriter, r *http.Request, apireq *apiRequest) {
	msg := actMsgExt{}
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
		if !keepMD {
			t.writeErrAct(w, r, apc.ActEvictRemoteBck) // (instead, expecting updated BMD from primary)
			return
		}
		// arrived via p.destroyBucketData()
		var (
			wg  = &sync.WaitGroup{}
			nlp = newBckNLP(apireq.bck)
		)
		nlp.Lock()
		defer nlp.Unlock()
		defer wg.Wait()

		core.UncacheBcks(wg, apireq.bck)
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
	case apc.ActDeleteObjects, apc.ActEvictObjects:
		lrMsg := &apc.ListRange{}
		if err := cos.MorphMarshal(msg.Value, lrMsg); err != nil {
			t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, msg.Action, msg.Value, err)
			return
		}
		// extra safety check
		for _, name := range lrMsg.ObjNames {
			if err := cmn.ValidateOname(name); err != nil {
				t.writeErr(w, r, err)
				return
			}
		}
		rns := xreg.RenewEvictDelete(msg.UUID, msg.Action /*xaction kind*/, apireq.bck, lrMsg)
		if rns.Err != nil {
			t.writeErr(w, r, rns.Err)
			return
		}
		xctn := rns.Entry.Get()
		notif := &xact.NotifXact{
			Base: nl.Base{When: core.UponTerm, Dsts: []string{equalIC}, F: t.notifyTerm},
			Xact: xctn,
		}
		xctn.AddNotif(notif)
		xact.GoRunW(xctn)
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
	if msg.Action != apc.ActPrefetchObjects {
		t.writeErrAct(w, r, msg.Action)
		return
	}
	if err := t.parseReq(w, r, apireq); err != nil {
		return
	}
	// extra check
	t.ensureLatestBMD(msg, r)
	if err := apireq.bck.Init(t.owner.bmd); err != nil {
		t.writeErr(w, r, err)
		return
	}

	prfMsg := &apc.PrefetchMsg{}
	if err := cos.MorphMarshal(msg.Value, prfMsg); err != nil {
		t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, msg.Action, msg.Value, err)
		return
	}
	if ecode, err := t.runPrefetch(msg.UUID, apireq.bck, prfMsg); err != nil {
		t.writeErr(w, r, err, ecode)
	}
}

// handle apc.ActPrefetchObjects <-- via api.Prefetch* and api.StartX*
func (t *target) runPrefetch(xactID string, bck *meta.Bck, prfMsg *apc.PrefetchMsg) (int, error) {
	cs := fs.Cap()
	if err := cs.Err(); err != nil {
		return http.StatusInsufficientStorage, err
	}
	rns := xreg.RenewPrefetch(xactID, bck, prfMsg)
	if rns.Err != nil {
		return http.StatusBadRequest, rns.Err
	}

	xctn := rns.Entry.Get()
	notif := &xact.NotifXact{
		Base: nl.Base{When: core.UponTerm, Dsts: []string{equalIC}, F: t.notifyTerm},
		Xact: xctn,
	}
	xctn.AddNotif(notif)

	xact.GoRunW(xctn)
	return 0, nil
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
	if cmn.Rom.FastV(5, cos.SmoduleAIS) {
		pid := apireq.query.Get(apc.QparamProxyID)
		nlog.Infoln(r.Method, apireq.bck, "<=", pid)
	}

	debug.Assert(!apireq.bck.IsAIS())

	if apireq.bck.IsHT() {
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
				t.writeErr(w, r, err, code, Silent)
			} else {
				err = cmn.NewErrFailedTo(t, "HEAD remote bucket", apireq.bck, err, code)
				t._erris(w, r, err, code, cos.IsParseBool(apireq.query.Get(apc.QparamSilent)))
			}
			return
		}
		nlog.Warningf("%s: bucket %s, err: %v(%d)", t, apireq.bck.String(), err, code)
		bucketProps = make(cos.StrKVs)
		bucketProps[apc.HdrBackendProvider] = apireq.bck.Provider
		bucketProps[apc.HdrRemoteOffline] = strconv.FormatBool(apireq.bck.IsRemote())
	}
	for k, v := range bucketProps {
		if k == apc.HdrBucketVerEnabled && apireq.bck.Props != nil {
			if curr := strconv.FormatBool(apireq.bck.VersionConf().Enabled); curr != v {
				// e.g., change via vendor-provided CLI and similar
				nlog.Errorf("%s: %s versioning got out of sync: %s != %s", t, apireq.bck.String(), v, curr)
			}
		}
		hdr.Set(k, v)
	}
}
