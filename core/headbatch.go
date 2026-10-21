// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core/meta"
)

//
// head-batch batching: one object at a time in, requests of at most `size` objects per peer out
// - not safe for concurrent use: one batcher per jogger or worker
// - Add sends a group when it reaches `size`
// - the callback may Add (only queues), but not Flush
// - the batcher reuses the entry after the callback returns (see HdbArgs.Cb)
// - Add may run the callback: do not hold a LOM lock when you call Add
// - Add needs a loaded LOM, so the caller may free the LOM after Add returns
// - to abort, the producer drops the batcher
//

type (
	// one object (a fixed-size copy of the attributes that ObjAttrs.CheckEq reads)
	HdbEnt struct {
		bck    cmn.Bck
		name   string
		LIF    LIF
		Tsi    *meta.Snode // the peer that will answer
		Cause  string
		cksum  cos.Cksum
		md     [len(cmn.CheckEqKeys)]string
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
			npeerbusy   int64 // items whose peer rejected the request (429: critical load)
		}
		flushing bool
	}
	// NewHdbBatcher arguments
	HdbArgs struct {
		// per-object completion callback
		// NOTE: the entry is valid only until the callback returns, so copy `*ent` to keep it
		Cb   func(*HdbEnt)
		Mod  int
		Size int // max items per request (zero selects the default)
	}
)

// props needed for CheckEq in the fallback path
var hdbProps = []string{apc.GetPropsChecksum, apc.GetPropsCustom}

var hdbGroupPool = sync.Pool{New: func() any { return new(hdbGroup) }}

func allocHdbGroup(tsi *meta.Snode, size int) *hdbGroup {
	g := hdbGroupPool.Get().(*hdbGroup)
	g.tsi = tsi
	if cap(g.ents) < size {
		g.ents = make([]HdbEnt, 0, size)
	}
	return g
}

// return the group to the pool
func freeHdbGroup(g *hdbGroup) {
	clear(g.ents)
	g.tsi, g.ents = nil, g.ents[:0]
	hdbGroupPool.Put(g)
}

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
		req:    cmn.HdbReq{In: make([]cmn.HdbIn, size)},
		mod:    args.Mod,
		size:   size,
	}
}

// enqueue and send the group when it is full
func (hb *HdbBatcher) Add(lom *LOM, tsi *meta.Snode) {
	tid := tsi.ID()
	g := hb.groups[tid]
	if g == nil {
		g = allocHdbGroup(tsi, hb.size)
		hb.groups[tid] = g
	}
	g.ents = append(g.ents, HdbEnt{Tsi: tsi})
	g.ents[len(g.ents)-1].init(lom)
	hb.total++

	if len(g.ents) < hb.size || hb.flushing {
		return
	}
	delete(hb.groups, tid)
	hb.flushing = true
	hb.flush(g)
	hb.flushing = false
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

func (hb *HdbBatcher) TakeStats() (nbatches, nfellbk, nunanswered, npeerbusy int64) {
	st := &hb.stats
	nbatches, nfellbk, nunanswered, npeerbusy = st.nbatches, st.nfellbk, st.nunanswered, st.npeerbusy
	st.nbatches, st.nfellbk, st.nunanswered, st.npeerbusy = 0, 0, 0, 0
	return nbatches, nfellbk, nunanswered, npeerbusy
}

// complete only one group
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
	freeHdbGroup(g)
}

func (hb *HdbBatcher) batch(ents []HdbEnt, tsi *meta.Snode) {
	req := &hb.req
	req.Bcks, req.In = req.Bcks[:0], req.In[:len(ents)]
	for i := range ents {
		ents[i].fill(&req.In[i], req.AddBck(&ents[i].bck))
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
		tag := "failed:"
		if cmn.IsErrTooManyRequests(err) {
			hb.stats.npeerbusy += int64(len(ents))
			tag = "busy:"
		} else {
			hb.stats.nunanswered += int64(len(ents))
		}
		cmn.SparseWarn(hb.mod, hb.stats.nbatches, tsi.StringEx(), apc.HdbTag, "of", len(ents),
			tag, err, "[ no per-object fallback ]")
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
		var in cmn.HdbIn
		ent.fill(&in, 0)
		if eqErr := in.CheckEq(op); eqErr != nil {
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

// copy from the LOM
func (ent *HdbEnt) init(lom *LOM) {
	b := lom.Bucket()
	ent.bck = cmn.Bck{Name: b.Name, Provider: b.Provider, Ns: b.Ns}
	ent.LIF = lom.LIF()
	ent.name = lom.OrigObjName()
	if ck := lom.Checksum(); ck != nil {
		ent.cksum = *ck
	}
	for i, key := range cmn.CheckEqKeys {
		ent.md[i], _ = lom.GetCustomKey(key)
	}
	debug.Assert(lom.loaded())
}

// set the request item
func (ent *HdbEnt) fill(in *cmn.HdbIn, bidx int32) {
	in.Name, in.Bidx, in.Size = ent.name, bidx, ent.LIF.Size
	in.Cksum = &ent.cksum
	clear(in.CustomMD)
	for i, val := range ent.md {
		if val != "" {
			in.SetCustomKey(cmn.CheckEqKeys[i], val)
		}
	}
}
