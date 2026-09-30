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

// head-batch receiver: POST /v1/objects/<bucket-name> (see cmn/headbatch)

const hdbBodyMax = 64 * cos.MiB // apc.HdbSizeMax items

// T2T only: the sender is always another target (intra-control net, signed when enabled)
func (t *target) httpobjhdb(w http.ResponseWriter, r *http.Request, apireq *apiRequest) {
	if ecode, err := t.checkIntra(r, nil /*smap*/, false /*only primary*/); err != nil {
		t.writeErr(w, r, fmt.Errorf(fmtErrInvIntraObj, t.si, r.Method, r.RemoteAddr, err), ecode)
		return
	}
	if t.parseReq(w, r, apireq) != nil {
		return
	}
	var ecode int
	err := apireq.bck.Init(t.owner.bmd)
	if err == nil {
		err = t.objHeadBatch(w, r, apireq.bck)
	} else if cmn.IsErrBucketNought(err) {
		ecode = http.StatusNotFound
	}
	if err != nil {
		t._erris(w, r, err, ecode, apireq.dpq.silent)
	}
}

func (t *target) objHeadBatch(w http.ResponseWriter, r *http.Request, bck *meta.Bck) error {
	if r.ContentLength <= 0 || r.ContentLength > hdbBodyMax {
		return fmt.Errorf("head-batch: invalid content length %d, expecting (0, %d]", r.ContentLength, hdbBodyMax)
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

	var (
		started = mono.NanoTime()
		resp    = apc.NewHdbResp(len(req.In))
		nheads  int64
	)
	for i := range req.In {
		if hdbOne(bck, &req.In[i], resp, i) {
			nheads++
		}
	}

	// count the HEADs that actually compared
	if nheads > 0 {
		vlabs := bvlabs(bck)
		t.statsT.AddWith(
			cos.NamedVal64{Name: stats.HeadCount, Value: nheads, VarLabs: vlabs},
			cos.NamedVal64{Name: stats.HeadLatencyTotal, Value: mono.SinceNano(started), VarLabs: vlabs},
		)
	}

	out := resp.NewPack()
	hdr := w.Header()
	hdr.Set(cos.HdrContentType, cos.ContentBinary)
	hdr.Set(cos.HdrContentLength, strconv.Itoa(len(out)))
	if _, err := w.Write(out); err != nil {
		nlog.Warningln(t.String(), "head-batch:", err) // (broken pipe; benign)
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
