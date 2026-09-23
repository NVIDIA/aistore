// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core/meta"
)

//
// head-batch batching: one object at a time in, requests of at most `size` objects per peer out
// - not safe for concurrent use: one batcher per jogger or worker
// - Add only queues, the producer calls Flush
// - the callback may Add, but not Flush
// - the batcher keeps the identity of the object (LIF), not the LOM
// - to abort, the producer drops the batcher
//

type (
	// one object
	HdbEnt struct {
		LIF    LIF
		Tsi    *meta.Snode // the peer that will answer
		Cause  string
		Oa     cmn.ObjAttrs
		Status apc.HdbStatus
	}
	// pending objects with same peer
	hdbGroup struct {
		tsi  *meta.Snode
		ents []HdbEnt
	}
	// batches T2T HEAD(object) requests per peer
	HdbBatcher struct {
		cb     func(*HdbEnt)
		groups map[string]*hdbGroup // by peer ID
		req    cmn.HdbReq
		mod    int // cos.ModReb, cos.ModSpace (logging)
		size   int
		total  int
		stats  struct {
			nbatches    int64
			nfellbk     int64 // items answered by per-object HEAD
			nunanswered int64 // items whose peer did not answer
		}
		flushing bool
	}
	// NewHdbBatcher arguments (zero Mod and Size select the defaults)
	HdbArgs struct {
		Cb   func(*HdbEnt) // per-object completion callback
		Mod  int           // cos.ModReb, cos.ModSpace (logging)
		Size int           // max items per request
	}
)

// props needed for CheckEq in the fallback path
var hdbProps = []string{apc.GetPropsChecksum, apc.GetPropsCustom}

func NewHdbBatcher(args *HdbArgs) *HdbBatcher {
	debug.Assert(args.Cb != nil)
	size := args.Size
	if size <= 0 {
		size = apc.HdbSizeDflt
	}
	size = min(size, apc.HdbSizeMax)
	return &HdbBatcher{
		cb:     args.Cb,
		groups: make(map[string]*hdbGroup, 8),
		req:    cmn.HdbReq{In: make([]cmn.HdbIn, 0, size)},
		mod:    args.Mod,
		size:   size,
	}
}

// enqueue
func (hb *HdbBatcher) Add(lif LIF, tsi *meta.Snode, oa *cmn.ObjAttrs) {
	debug.Assert(oa != nil)
	tid := tsi.ID()
	g := hb.groups[tid]
	if g == nil {
		g = &hdbGroup{tsi: tsi, ents: make([]HdbEnt, 0, hb.size)}
		hb.groups[tid] = g
	}
	ent := HdbEnt{LIF: lif, Tsi: tsi}
	ent.Oa.CopyFrom(oa, false /*skipCksum*/)
	g.ents = append(g.ents, ent)
	hb.total++
}

// complete all pending objects
func (hb *HdbBatcher) Flush() {
	if hb.flushing {
		debug.Assert(false, "Flush from the completion callback")
		return
	}
	hb.flushing = true
	for hb.total > 0 {
		for tid, g := range hb.groups {
			delete(hb.groups, tid)
			hb.flush(g)
		}
	}
	hb.flushing = false
}

func (hb *HdbBatcher) Pending() int { return hb.total }

func (hb *HdbBatcher) TakeStats() (nbatches, nfellbk, nunanswered int64) {
	st := &hb.stats
	nbatches, nfellbk, nunanswered = st.nbatches, st.nfellbk, st.nunanswered
	st.nbatches, st.nfellbk, st.nunanswered = 0, 0, 0
	return nbatches, nfellbk, nunanswered
}

// complete only one group
// TODO: when the peer does not answer, complete the rest of the group as apc.HdbNone (no more requests)
func (hb *HdbBatcher) flush(g *hdbGroup) {
	ents := g.ents
	hb.total -= len(ents)
	for len(ents) > 0 {
		n := min(len(ents), hb.size)
		if n == 1 {
			hb.fallback(ents[:n], g.tsi)
		} else {
			hb.batch(ents[:n], g.tsi)
		}
		ents = ents[n:]
	}
}

func (hb *HdbBatcher) batch(ents []HdbEnt, tsi *meta.Snode) {
	req := &hb.req
	req.Bcks, req.In = req.Bcks[:0], req.In[:0]
	for i := range ents {
		b, objName := cmn.ParseUname(ents[i].LIF.Uname)
		req.In = append(req.In, cmn.HdbIn{ObjAttrs: ents[i].Oa, Name: objName, Bidx: req.AddBck(&b)})
	}
	hb.stats.nbatches++

	resp, err := T.HeadBatchT2T(req, tsi)
	if err != nil {
		if cmn.IsErrHdbUnsupported(err) {
			cmn.SparseWarn(hb.mod, hb.stats.nbatches, tsi.StringEx(), apc.HdbTag, "of", len(ents),
				"unsupported:", err, "[ falling back to per-object HEAD ]")
			hb.fallback(ents, tsi)
			return
		}
		hb.stats.nunanswered += int64(len(ents))
		cmn.SparseWarn(hb.mod, hb.stats.nbatches, tsi.StringEx(), apc.HdbTag, "of", len(ents),
			"failed:", err, "[ no per-object fallback ]")
		cause := err.Error()
		for i := range ents {
			ents[i].Status, ents[i].Cause = apc.HdbNone, cause
			hb.cb(&ents[i])
		}
		return
	}
	var m int
	for i := range ents {
		ents[i].Status = resp.Status[i]
		if m < len(resp.Msg) && resp.Msg[m].Idx == int32(i) {
			ents[i].Cause = resp.Msg[m].Msg
			m++
		}
		hb.cb(&ents[i])
	}
}

// per-object HEAD with local comparison
func (hb *HdbBatcher) fallback(ents []HdbEnt, tsi *meta.Snode) {
	hb.stats.nfellbk += int64(len(ents))
	for i := range ents {
		ent := &ents[i]
		ent.head(tsi)
		hb.cb(ent)
	}
}

func (ent *HdbEnt) head(tsi *meta.Snode) {
	lom, err := ent.LIF.LOM()
	if err != nil {
		ent.Status, ent.Cause = apc.HdbFailed, err.Error()
		return
	}
	op, err := T.HeadObjT2T(lom, tsi, hdbProps...)
	FreeLOM(lom)
	switch {
	case err == nil && op != nil:
		if eqErr := ent.Oa.CheckEq(op); eqErr != nil {
			ent.Status, ent.Cause = apc.HdbDiverged, eqErr.Error()
		} else {
			ent.Status = apc.HdbSame
		}
	case err == nil:
		ent.Status, ent.Cause = apc.HdbFailed, "peer returned no props"
	case cmn.IsErrHTTPNotFound(err):
		ent.Status = apc.HdbMissing
	default:
		ent.Status, ent.Cause = apc.HdbFailed, err.Error()
	}
}
