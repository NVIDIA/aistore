// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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
	if err = t.checkIntraCall(r, false); err != nil {
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
		var bckName, phase string
		if len(apiItems) > 0 {
			bckName = apiItems[0]
			if len(apiItems) > 1 {
				phase = apiItems[1]
				if err := apc.Validate2PC(phase); err != nil {
					debug.AssertNoErr(err)
					t.writeErr(w, r, err)
					return
				}
			}
		}
		qbck, err := qbckFromDpq(bckName, dpq)
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
		// list buckets if `qbck` indicates a bucket-type query
		// (see api.ListBuckets and the line below)
		if !qbck.IsBucket() {
			qbck.Name = msg.Name
			t.listBuckets(w, r, qbck, dpq)
			return
		}
		// list objects
		t.bgetObjects(w, r, qbck, msg, dpq, phase)

	case apc.ActSummaryBck, apc.ActSummaryShard:
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
		if phase != apc.Begin2PC && phase != apc.Query2PC {
			t.writeErrURL(w, r)
			return
		}

		qbck, err := qbckFromDpq(bucket, dpq)
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
		if msg.Action == apc.ActSummaryBck {
			t.bgetBckSumm(w, r, qbck, msg, dpq, phase)
		} else {
			t.bgetShardSumm(w, r, qbck, msg, dpq, phase)
		}

	case apc.ActShowNBI:
		var bckName string
		if len(apiItems) > 0 {
			bckName = apiItems[0]
		}
		qbck, err := qbckFromDpq(bckName, dpq)
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
		t.bgetNBI(w, r, qbck, msg, dpq)

	default:
		t.writeErrAct(w, r, msg.Action)
	}
}

func (t *target) _resolveQbck(w http.ResponseWriter, r *http.Request, qbck *cmn.QueryBcks, dontAddRemote bool) (*meta.Bck, error) {
	bck := meta.CloneBck((*cmn.Bck)(qbck))
	err := bck.Init(t.owner.bmd)
	if err == nil {
		return bck, nil
	}
	if cmn.IsErrRemoteBckNotFound(err) {
		if dontAddRemote {
			return bck, nil
		}
		t.BMDVersionFixup(r)
		err = bck.Init(t.owner.bmd)
	}
	if err != nil {
		t.writeErr(w, r, err)
		return nil, err
	}
	return bck, nil
}

func (t *target) bgetObjects(w http.ResponseWriter, r *http.Request, qbck *cmn.QueryBcks, msg *actMsgExt, dpq *dpq, phase string) {
	bck, err := t._resolveQbck(w, r, qbck, dpq.dontAddRemote)
	if err != nil {
		return
	}

	var (
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

	ok := t.listObjects(w, r, bck, lsmsg, phase)

	if !ok {
		if phase != apc.Abort2PC {
			t.statsT.IncBck(stats.ErrListCount, bck.Bucket())
		}
		return
	}
	if phase == apc.Begin2PC || phase == apc.Abort2PC { // 2PC control-only phases do not produce pages
		return
	}
}

func (t *target) bgetBckSumm(w http.ResponseWriter, r *http.Request, qbck *cmn.QueryBcks, msg *actMsgExt, dpq *dpq, phase string) {
	var bsumMsg apc.BsummCtrlMsg
	err := cos.MorphMarshal(msg.Value, &bsumMsg)
	if err != nil {
		t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, msg.Action, msg.Value, err)
		return
	}
	// if in fact it is a specific named bucket
	bck := (*meta.Bck)(qbck)
	if qbck.IsBucket() {
		bck, err = t._resolveQbck(w, r, qbck, bsumMsg.DontAddRemote || dpq.dontAddRemote)
		if err != nil {
			return
		}
	}
	t.bckSumm(w, r, phase, bck, &bsumMsg, dpq)
}

func (t *target) bgetShardSumm(w http.ResponseWriter, r *http.Request, qbck *cmn.QueryBcks, msg *actMsgExt, dpq *dpq, phase string) {
	if !qbck.IsBucket() {
		err := cmn.NewErrNotImpl("shard-summary", "bucket queries")
		t.writeErr(w, r, err)
		return
	}
	var summMsg apc.ShardSummMsg
	if err := cos.MorphMarshal(msg.Value, &summMsg); err != nil {
		t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, msg.Action, msg.Value, err)
		return
	}
	summMsg.Prefix = cos.TrimPrefix(summMsg.Prefix)
	if err := cos.ValidatePrefix("bad shard-summary request", summMsg.Prefix); err != nil {
		t.writeErr(w, r, err)
		return
	}
	bck, err := t._resolveQbck(w, r, qbck, true /*dont-add-remote*/)
	if err != nil {
		return
	}
	t.shardSumm(w, r, phase, bck, &summMsg, dpq)
}

// there's a difference between looking for all (any) provider vs a specific one -
// in the former case the fact that (the corresponding backend is not configured)
// is not an error
func (t *target) listBuckets(w http.ResponseWriter, r *http.Request, qbck *cmn.QueryBcks, dpq *dpq) {
	var (
		bcks   cmn.Bcks
		config = cmn.GCO.Get()
		bmd    = t.owner.bmd.get()
		err    error
		code   int
	)
	if qbck.Provider != "" {
		if qbck.IsAIS() || qbck.IsHT() { // built-in providers
			bcks = bmd.Select(qbck, dpq.system)
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
				buckets = bmd.Select(qbck, dpq.system)
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
			return nil, 0, &cmn.ErrMissingBackend{Provider: qbck.Provider}
		}
	} else if qbck.IsRemoteAIS() && qbck.Ns.IsAnyRemote() {
		if config.Backend.Get(apc.AIS) == nil {
			nlog.Warningln(&cmn.ErrMissingBackend{Provider: qbck.Provider, Msg: "no remote ais clusters"})
			return nil, 0, nil
			// otherwise go ahead and try to list below
		}
	}

	bp := t.Backend((*meta.Bck)(qbck))
	if qbck.IsBucket() {
		bck := (*meta.Bck)(qbck)
		ctx := context.Background()
		_, ecode, err = bp.HeadBucket(ctx, bck)
		if err == nil {
			bcks = cmn.Bcks{bck.Clone()}
		} else if ecode == http.StatusNotFound {
			err = nil
		}
	} else {
		bcks, ecode, err = bp.ListBuckets(*qbck)
		if err == nil && len(bcks) > 1 {
			sort.Sort(bcks)
		}
	}

	return bcks, ecode, err
}

// returns `cmn.LsoRes` containing object names and (requested) props
// control/scope - via `apc.LsoMsg`
func (t *target) listObjects(w http.ResponseWriter, r *http.Request, bck *meta.Bck, lsmsg *apc.LsoMsg, phase string) bool /*ok*/ {
	// (advanced) user-selected target to execute remote ls
	if lsmsg.SID != "" {
		smap := t.owner.smap.get()
		if smap.GetTarget(lsmsg.SID) == nil {
			err := &errNodeNotFound{t.si, smap, "list-objects failure:", lsmsg.SID}
			t.writeErr(w, r, err)
			return false
		}
	}

	if lsmsg.IsFlagSet(apc.LsNBI) {
		if ok := t._resolveInvName(w, r, bck, lsmsg); !ok {
			return false
		}
	}

	var (
		xls *xs.LsoXact
	)
	if phase == apc.Commit2PC || phase == apc.Abort2PC {
		// commit: xls must exist
		xctn, err := xreg.GetXact(lsmsg.UUID)
		switch {
		case err != nil:
			t.writeErr(w, r, err)
			return false
		case xctn == nil:
			if phase == apc.Commit2PC {
				err = cmn.NewErrXactNotFoundError(xact.Cname(apc.ActList, lsmsg.UUID))
				t.writeErr(w, r, err, http.StatusNotFound, Silent)
			}
			return false
		case xctn.IsAborted() || xctn.IsDone():
			if phase == apc.Commit2PC {
				err := fmt.Errorf("late commit phase: %s already finished", xact.Cname(apc.ActList, lsmsg.UUID))
				if e := xctn.AbortErr(); e != nil {
					err = e
				}
				t.writeErr(w, r, err)
			}
			return false
		case phase == apc.Abort2PC:
			err := fmt.Errorf("2pc abort: %s", xact.Cname(apc.ActList, lsmsg.UUID))
			xctn.Abort(err)
			return false
		}
		xls = xctn.(*xs.LsoXact)

		// reset idle timer (here and elsewhere)
		xls.IncPending()
		xls.DecPending()
	} else {
		// renew-or-create. Begin relies on RenewLso->Start() running synchronously:
		// Start() opens R-flow streams and RegRecv() completes before begin acks.
		// Commit/abort must instead find an already renewed xls.
		rns := xreg.RenewLso(bck, lsmsg.UUID, lsmsg, r.Header)
		if rns.Err == xs.ErrGone {
			runtime.Gosched()
			rns = xreg.RenewLso(bck, lsmsg.UUID, lsmsg, r.Header)
		}
		if rns.Err != nil {
			t.writeErr(w, r, rns.Err)
			return false
		}
		xctn := rns.Entry.Get()
		if !rns.IsRunning() {
			xact.GoRunW(xctn)
		}
		xls = xctn.(*xs.LsoXact)

		if phase == apc.Begin2PC {
			// R-flow:
			// - beginStreams (in x-lso.Start) including dm.RegRecv
			// - have a fresh full idle window for commit phase (and first page)
			// - ack without paging
			xls.IncPending()
			xls.DecPending()
			w.WriteHeader(http.StatusOK)
			return true
		}
	}

	// commit, or single-phase: drive paging
	resp := xls.Do(lsmsg)
	if resp == nil {
		// (unlikely shutdown)
		w.WriteHeader(http.StatusNoContent)
		return false
	}
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

	return t.writeMsgPack(w, resp.Lst, "list_objects") // ok
}

// resolve NBI (native bucket inventory) name: locate-and-set when not provided,
// validate when provided
func (t *target) _resolveInvName(w http.ResponseWriter, r *http.Request, bck *meta.Bck, lsmsg *apc.LsoMsg) bool /*ok*/ {
	debug.AssertNoErr(lsmsg.ValidateNBI()) // checked by proxy

	if invName := r.Header.Get(apc.HdrInvName); invName == "" {
		// must exist and be single
		nbis, err := fs.CollectNBI(bck.Bucket())
		if err != nil {
			t.writeErrf(w, r, "failed to collect bucket inventories: %w", err)
			return false
		}
		if l := len(nbis); l != 1 {
			if l == 0 {
				t.writeErrf(w, r, "bucket %q has no inventories", bck.Cname(""))
			} else {
				t.writeErrf(w, r, "missing %q header: bucket %q has %d inventories; please specify which one",
					apc.HdrInvName, bck.Cname(""), l)
			}
			return false
		}
		invName = nbis.SingleName()
		if cmn.Rom.V(4, cos.ModAIS) {
			nlog.Infoln("located inventory", invName, "for bucket", bck.Cname(""))
		}
		r.Header.Set(apc.HdrInvName, invName)
	} else if err := cos.CheckAlphaPlus(invName, "inventory name"); err != nil {
		t.writeErr(w, r, err)
		return false
	}
	return true
}

func (t *target) bckSumm(w http.ResponseWriter, r *http.Request, phase string, bck *meta.Bck, msg *apc.BsummCtrlMsg, dpq *dpq) {
	if phase == apc.Begin2PC {
		rns := xreg.RenewBckSummary(bck, msg)
		if rns.Err != nil {
			t.writeErr(w, r, rns.Err, http.StatusInternalServerError, Silent)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		return
	}

	debug.Assert(phase == apc.Query2PC, phase)
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
	if !xctn.IsDone() {
		if len(result) == 0 {
			w.WriteHeader(http.StatusAccepted)
		} else {
			w.WriteHeader(http.StatusPartialContent)
		}
	}
	t.writeJSON(w, r, result, xsumm.Name())
}

func (t *target) shardSumm(w http.ResponseWriter, r *http.Request, phase string, bck *meta.Bck, msg *apc.ShardSummMsg, dpq *dpq) {
	if phase == apc.Begin2PC {
		rns := xreg.RenewBckShardSumm(bck, msg)
		if rns.Err != nil {
			t.writeErr(w, r, rns.Err, http.StatusInternalServerError, Silent)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		return
	}

	debug.Assert(phase == apc.Query2PC, phase)
	xctn, err := xreg.GetXact(msg.UUID)
	if err != nil {
		t.writeErr(w, r, err, http.StatusInternalServerError)
		return
	}
	if xctn == nil {
		err := cos.NewErrNotFound(t, apc.ActSummaryShard+" job "+msg.UUID)
		t._erris(w, r, err, http.StatusNotFound, dpq.silent)
		return
	}

	xsumm, ok := xctn.(*xs.XactShardSumm)
	if !ok {
		debug.Assert(false, "expected xs.XactShardSumm, got", xctn.String())
		err := cos.NewErrNotFound(t, apc.ActSummaryShard+" job "+msg.UUID)
		t._erris(w, r, err, http.StatusNotFound, dpq.silent)
		return
	}
	res, err := xsumm.Result()
	if err != nil {
		if cmn.IsErrBucketNought(err) {
			t.writeErr(w, r, err, http.StatusGone)
		} else {
			t.writeErr(w, r, err)
		}
		return
	}

	if !xctn.IsDone() {
		if res.IsEmpty() {
			debug.Assert(res.TarSize == 0 && res.Shards == 0 && res.ShardSize == 0 &&
				res.ArchivedObjs == 0 && res.StaleIndexes == 0 && res.InvalidIndexes == 0)
			// no progress yet
			w.WriteHeader(http.StatusAccepted)
		} else {
			// partial progress
			w.WriteHeader(http.StatusPartialContent)
		}
	}
	// done
	t.writeJSON(w, r, res, xsumm.Name())
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

	bck := apireq.bck
	switch msg.Action {
	case apc.ActEvictRemoteBck:
		keepMD := cos.IsParseBool(apireq.query.Get(apc.QparamKeepRemote))
		if !keepMD {
			t.writeErrAct(w, r, apc.ActEvictRemoteBck) // (instead, expecting updated BMD from primary)
			return
		}
		// arrived via p.evictRemoteKeepMD
		// (compare with t.destroyBucket transaction)
		var (
			wg  = &sync.WaitGroup{}
			nlp = newBckNLP(bck)
			xid = apireq.query.Get(apc.QparamUUID)
		)
		nlp.Lock()
		defer nlp.Unlock()
		defer wg.Wait()

		// start and immdiately finish xaction with a singular purpose:
		// to have a record in xreg (via `ais show job`): name and timestamp only
		debug.Assert(strings.HasPrefix(xid, xact.PrefixEvictKeepID), xid)
		_ = xreg.RenewEvictDelete(xid, apc.ActEvictRemoteBck, bck, nil)

		core.LcacheClearBcks(wg, bck)
		err := fs.DestroyBucket(msg.Action, bck.Bucket(), bck.Props.BID)
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
		// Recreate bucket directories (now empty), since bck is still in BMD
		errs := fs.CreateBucket(bck.Bucket(), false /*nilbmd*/)
		if len(errs) > 0 {
			debug.AssertNoErr(errs[0])
			t.writeErr(w, r, errs[0]) // only 1 err is possible for 1 bck
		}
	case apc.ActDeleteObjects, apc.ActEvictObjects:
		evdMsg := &apc.EvdMsg{}
		if err := cos.MorphMarshal(msg.Value, evdMsg); err != nil {
			t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, msg.Action, msg.Value, err)
			return
		}
		// NOTE: validate object names - each name individually
		for _, name := range evdMsg.ObjNames {
			if err := cos.ValidateOname(name); err != nil {
				t.writeErr(w, r, err)
				return
			}
		}
		rns := xreg.RenewEvictDelete(msg.UUID, msg.Action /*xaction kind*/, bck, evdMsg)
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
	case apc.ActDestroyNBI:
		t.destroyNBI(w, r, bck, &msg)
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

	// create-bucket => HEAD(remote) flow
	if msg.Action == apc.ActHeadBckWith {
		oneshotBprops := cmn.Bprops{}
		if err := cos.MorphMarshal(msg.Value, &oneshotBprops); err != nil {
			t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, msg.Action, msg.Value, err)
			return
		}
		t._bckhead(w, r, apireq, &oneshotBprops)
		return
	}

	// extra check
	t.ensureLatestBMD(msg, r)
	if err := apireq.bck.Init(t.owner.bmd); err != nil {
		t.writeErr(w, r, err)
		return
	}

	switch msg.Action {
	case apc.ActPrefetchObjects:
		prfMsg := &apc.PrefetchMsg{}
		if err = cos.MorphMarshal(msg.Value, prfMsg); err != nil {
			t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, msg.Action, msg.Value, err)
			return
		}
		if ecode, err := t.runPrefetch(msg.UUID, apireq.bck, prfMsg); err != nil {
			t.writeErr(w, r, err, ecode)
			return
		}
	case apc.ActRechunk:
		rechunkMsg := &apc.RechunkMsg{}
		if err = cos.MorphMarshal(msg.Value, rechunkMsg); err != nil {
			t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, msg.Action, msg.Value, err)
			return
		}
		_, err = t.runRechunk(msg.UUID, apireq.bck, rechunkMsg)
	case apc.ActIndexShard:
		sishMsg := &apc.IndexShardMsg{}
		if err = cos.MorphMarshal(msg.Value, sishMsg); err != nil {
			t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, msg.Action, msg.Value, err)
			return
		}
		_, err = t.runIndexShard(msg.UUID, apireq.bck, sishMsg)
	default:
		t.writeErrAct(w, r, msg.Action)
	}

	// common return
	if err != nil {
		t.writeErr(w, r, err)
	}
}

func (t *target) runRechunk(xactID string, bck *meta.Bck, rechunkMsg *apc.RechunkMsg) (xid string, err error) {
	if err := xreg.LimitedCoexistence(t.si, bck, apc.ActRechunk); err != nil {
		return "", err
	}
	rns := xreg.RenewBckRechunks(bck, xactID, rechunkMsg)
	if rns.Err != nil {
		return "", rns.Err
	}
	xctn := rns.Entry.Get()
	notif := &xact.NotifXact{
		Base: nl.Base{When: core.UponTerm, Dsts: []string{equalIC}, F: t.notifyTerm},
		Xact: xctn,
	}
	xctn.AddNotif(notif)

	if cmn.Rom.V(5, cos.ModAIS) {
		nlog.Infoln("start rechunk", bck.String(), "xid", xactID)
	}
	xact.GoRunW(xctn)
	return xctn.ID(), nil
}

func (t *target) runIndexShard(xactID string, bck *meta.Bck, msg *apc.IndexShardMsg) (xid string, err error) {
	if err := xreg.LimitedCoexistence(t.si, bck, apc.ActIndexShard); err != nil {
		return "", err
	}
	rns := xreg.RenewBckShardIndex(bck, xactID, msg)
	if rns.Err != nil {
		return "", rns.Err
	}
	xctn := rns.Entry.Get()
	notif := &xact.NotifXact{
		Base: nl.Base{When: core.UponTerm, Dsts: []string{equalIC}, F: t.notifyTerm},
		Xact: xctn,
	}
	xctn.AddNotif(notif)
	xact.GoRunW(xctn)
	return xctn.ID(), nil
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
// see related:
// - t.httpbckpost() + apc.ActHeadBckWith
// - p.headRemoteBck()
func (t *target) httpbckhead(w http.ResponseWriter, r *http.Request, apireq *apiRequest) {
	if err := t.parseReq(w, r, apireq); err != nil {
		return
	}
	t._bckhead(w, r, apireq, nil)
}

func (t *target) _bckhead(w http.ResponseWriter, r *http.Request, apireq *apiRequest, oneshotBprops *cmn.Bprops) {
	var (
		bck   = apireq.bck
		inBMD = true
	)
	errV := bck.Init(t.owner.bmd)
	if errV != nil {
		if !cmn.IsErrRemoteBckNotFound(errV) { // is ais
			t.writeErr(w, r, errV)
			return
		}
		inBMD = false

		// when called via ActHeadBckWith, apireq.bck.Props are one-shot props (not coming from BMD)
		bck.Props = oneshotBprops
	} else if oneshotBprops != nil {
		nlog.Warningf("%s: bucket %s is already in BMD; ignoring creation-time props (use bucket props API or CLI to update)", t, bck)
	}
	if cmn.Rom.V(5, cos.ModAIS) {
		pid := apireq.query.Get(apc.QparamPID)
		nlog.Infoln(r.Method, bck, "<=", pid)
	}

	debug.Assert(!bck.IsAIS())

	ctx := context.Background()
	if bck.IsHT() {
		originalURL := apireq.query.Get(apc.QparamOrigURL)
		ctx = context.WithValue(ctx, cos.CtxOriginalURL, originalURL)
		if !inBMD && originalURL == "" {
			err := cmn.NewErrRemBckNotFound(bck.Bucket())
			t.writeErr(w, r, err, http.StatusNotFound, Silent)
			return
		}
	}

	// + cloud
	bp := t.Backend(bck)
	bpropsKV, code, err := bp.HeadBucket(ctx, bck)
	if err != nil {
		if !inBMD {
			if code == http.StatusNotFound {
				t.writeErr(w, r, err, code, Silent)
			} else {
				err = cmn.NewErrFailedTo(t, "HEAD remote bucket", bck, err, code)
				t._erris(w, r, err, code, cos.IsParseBool(apireq.query.Get(apc.QparamSilent)))
			}
			return
		}
		nlog.Warningf("%s: bucket %s, err: %v(%d)", t, bck.String(), err, code)
		bpropsKV = make(cos.StrKVs)
		bpropsKV[apc.HdrBackendProvider] = bck.Provider
		bpropsKV[apc.HdrRemoteOffline] = strconv.FormatBool(bck.IsRemote())
	}

	hdr := w.Header()
	for k, v := range bpropsKV {
		if k == apc.HdrBucketVerEnabled && bck.Props != nil {
			if curr := strconv.FormatBool(bck.VersionConf().Enabled); curr != v {
				// e.g., change via vendor-provided CLI and similar
				nlog.Errorf("%s: %s versioning got out of sync: %s != %s", t, bck.String(), v, curr)
			}
		}
		hdr.Set(k, v)
	}
}
