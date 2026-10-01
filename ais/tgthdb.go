// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
)

// head-batch receiver: POST /v1/objects (see cmn/headbatch)

const hdbBodyMax = 64 * cos.MiB // apc.HdbSizeMax items

// a bucket of the request and its stats
type hdbBck struct {
	err    error // Init error: fails the bucket's items
	bck    meta.Bck
	nheads int64 // HEADs that compared
	lat    int64 // their total latency (ns)
}

// Init validates the name and provider
func (b *hdbBck) init(bck *cmn.Bck, bowner meta.Bowner) {
	b.bck = meta.Bck(*bck)
	b.err = b.bck.Init(bowner)
}

// a bucket that failed to init fails its items
func (b *hdbBck) head(in *cmn.HdbIn, resp *apc.HdbResp, i int) {
	if b.err != nil {
		resp.Set(i, apc.HdbFailed, b.err)
		return
	}
	started := mono.NanoTime()
	if hdbOne(&b.bck, in, resp, i) {
		b.nheads++
		b.lat += mono.SinceNano(started)
	}
}

// count the HEADs that actually compared
func (b *hdbBck) addStats(statsT stats.Tracker) {
	if b.nheads == 0 {
		return
	}
	vlabs := bvlabs(&b.bck)
	statsT.AddWith(
		cos.NamedVal64{Name: stats.HeadCount, Value: b.nheads, VarLabs: vlabs},
		cos.NamedVal64{Name: stats.HeadLatencyTotal, Value: b.lat, VarLabs: vlabs},
	)
}

// T2T only: the sender is always another target (intra-control net, signed when enabled)
func (t *target) httpobjhdb(w http.ResponseWriter, r *http.Request, dpq *dpq) {
	if !reqIsIntraCtrl(r) {
		err := fmt.Errorf("%s: %v over %s network", apc.HdbTag, errNotIntraControl, reqNetName(_reqNet(r)))
		t.writeErr(w, r, err, http.StatusForbidden)
		return
	}
	if ecode, err := t.checkIntra(r, nil /*smap*/, false /*only primary*/); err != nil {
		t.writeErr(w, r, fmt.Errorf(fmtErrInvIntraObj, t.si, r.Method, r.RemoteAddr, err), ecode)
		return
	}
	if err := dpq.parse(r.URL.RawQuery); err != nil {
		t.writeErr(w, r, err)
		return
	}
	if err := t.objHeadBatch(w, r); err != nil {
		t._erris(w, r, err, 0, dpq.silent)
	}
}

// unpack the request and compare each object with the local copy
// the response is positional: Status[i] answers In[i]
func (t *target) objHeadBatch(w http.ResponseWriter, r *http.Request) error {
	if r.ContentLength <= 0 || r.ContentLength > hdbBodyMax {
		return fmt.Errorf("%s: invalid content length %d, expecting (0, %d]", apc.HdbTag, r.ContentLength, hdbBodyMax)
	}
	body, err := cos.ReadAllN(r.Body, r.ContentLength)
	cos.Close(r.Body)
	if err != nil {
		return err
	}
	var req cmn.HdbReq
	if err := req.Unpack(cos.NewUnpacker(body)); err != nil {
		return err
	}
	if err := req.Validate(); err != nil {
		return err
	}

	// init the buckets, compare each object, add stats per bucket
	var (
		bcks = make([]hdbBck, len(req.Bcks))
		resp = apc.NewHdbResp(len(req.In))
	)
	for i := range req.Bcks {
		bcks[i].init(&req.Bcks[i], t.owner.bmd)
	}
	for i := range req.In {
		bcks[req.In[i].Bidx].head(&req.In[i], resp, i)
	}
	for i := range bcks {
		bcks[i].addStats(t.statsT)
	}

	out := resp.NewPack()
	hdr := w.Header()
	hdr.Set(cos.HdrContentType, cos.ContentBinary)
	hdr.Set(cos.HdrContentLength, strconv.Itoa(len(out)))
	if _, err := w.Write(out); err != nil {
		nlog.Warningln(t.String(), apc.HdbTag+":", err) // (broken pipe; benign)
	}
	return nil
}

// establish the identity of one object
// returns true when the comparison succeeded
// (no name validation: T2T only, names originate from the sender's own LOMs)
func hdbOne(bck *meta.Bck, in *cmn.HdbIn, resp *apc.HdbResp, i int) bool {
	lom := core.AllocLOM(in.Name)
	defer core.FreeLOM(lom)

	if err := lom.InitBck(bck); err != nil {
		resp.Set(i, apc.HdbFailed, err)
		return false
	}
	// a locked object should not block the batch
	if !lom.TryLock(false /*exclusive*/) {
		resp.Set(i, apc.HdbBusy, nil)
		return false
	}
	defer lom.Unlock(false)

	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		if cos.IsNotExist(err) || cmn.IsErrObjNought(err) {
			resp.Set(i, apc.HdbMissing, nil)
		} else {
			resp.Set(i, apc.HdbFailed, err)
		}
		return false
	}

	// the sender's item is the base, our copy is the "remote" one
	if eqErr := in.CheckEq(lom.ObjAttrs()); eqErr != nil {
		resp.Set(i, apc.HdbDiverged, eqErr)
	} else {
		resp.Set(i, apc.HdbSame, nil)
	}
	return true
}
