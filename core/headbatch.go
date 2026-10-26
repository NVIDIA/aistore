// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/load"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
)

//
// head-batch batching: one object at a time in, requests of at most `size` objects per peer out
// - the job owns the batcher: its producers call Add and the job calls End after they stop
// - Add copies from the LOM so the caller may free the LOM after Add returns
// - a full group goes to a sender goroutine, at most HdbArgs.Senders at a time (Add waits)
// - at Critical load, best-effort drops the group while strict (above the high watermark) waits to send the group
// - End sends the remaining groups and waits for all senders
// - after the abort or a change of targets in HdbArgs.Smap, a sender sends nothing and runs no callback
// - Stats returns the counters at any time
//
// the callback (HdbArgs.Cb) runs on the senders at the same time and must not call Add or End (deadlock)
// the entry is valid only until the callback returns, so copy `*ent` to keep it
//

const hdbLoadIval = time.Second // min interval between two load samples

type (
	// one object (a fixed-size copy of the attributes that ObjAttrs.CheckEq reads)
	HdbEnt struct {
		bck    cmn.Bck
		name   string
		LIF    LIF
		Tsi    *meta.Snode // the peer that will answer
		Cause  string
		ver    string // lom.Version (Unchanged only: the peer has its own version)
		cksum  cos.Cksum
		md     [len(cmn.CheckEqKeys)]string
		Status apc.HdbStatus
	}
	// pending objects with same peer
	hdbGroup struct {
		tsi  *meta.Snode
		ents []HdbEnt
	}
	// sends one group at a time
	hdbSender struct {
		hb   *HdbBatcher
		smap *meta.Smap // last Smap with the original targets
		req  cmn.HdbReq
	}
	// see HdbStats
	hdbCounters struct {
		batches, fellback, unanswered, peerBusy, dropped, smapChanged, throttled, waited, partial atomic.Int64

		ndropped, nchanged atomic.Int64 // groups (sparse logging)
	}
	// one caller samples the load, the others read the last sample
	hdbThrottle struct {
		adv   load.Advice // under mu
		mu    sync.Mutex
		next  atomic.Int64 // mono time of the next sample
		sleep atomic.Int64 // last load.Advice.Sleep
	}
	// batches T2T HEAD(object) requests per peer
	HdbBatcher struct {
		groups map[string]*hdbGroup // by peer ID (under mu)
		idle   chan *hdbSender
		cb     func(*HdbEnt)
		abort  cos.Stopper
		capFn  func() fs.CapStatus
		logHdr string
		thr    hdbThrottle
		cnt    hdbCounters
		size   int
		mod    int // cos.ModReb, cos.ModSpace (logging)
		mu     sync.Mutex
	}
	// returned by Stats and End
	HdbStats struct {
		Batches     int64 // batch requests
		Fellback    int64 // objects answered by per-object HEAD
		Unanswered  int64 // objects whose peer did not answer
		PeerBusy    int64 // objects whose peer answered 429 (critical load)
		Dropped     int64 // objects with no callback: best-effort or the abort
		SmapChanged int64 // objects with no callback: the targets changed
		Throttled   int64 // objects in groups at Critical load (best-effort: also Dropped)
		Waited      int64 // full groups that waited for a sender
		Partial     int64 // groups at End
	}
	// NewHdbBatcher arguments
	HdbArgs struct {
		// per-object completion callback
		Cb      func(*HdbEnt)
		Abort   cos.Stopper // required
		Smap    *meta.Smap  // required
		LogHdr  string      // the log header of the job
		Mod     int
		Size    int // max items per request
		Senders int // max requests in flight
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

// write the counters
func (st *HdbStats) Str(sb *cos.SB, sep byte) {
	for i, kv := range [...]struct {
		name string
		v    int64
	}{
		{" hdb-batches", st.Batches}, {" hdb-fellback", st.Fellback}, {" hdb-unanswered", st.Unanswered},
		{" hdb-peer-busy", st.PeerBusy}, {" hdb-dropped", st.Dropped}, {" hdb-smap-changed", st.SmapChanged},
		{" hdb-throttled", st.Throttled}, {" hdb-waited", st.Waited}, {" hdb-partial", st.Partial},
	} {
		if i == 0 || kv.v > 0 {
			sb.WriteString(kv.name)
			sb.WriteUint8(sep)
			sb.WriteInt64(kv.v)
		}
	}
}

////////////////
// HdbBatcher //
////////////////

func NewHdbBatcher(args *HdbArgs) *HdbBatcher {
	debug.Assert(args.Cb != nil && args.Abort != nil && args.Smap != nil)
	size := args.Size
	if size <= 0 {
		size = apc.HdbSizeDflt
	}
	size = min(size, apc.HdbSizeMax)
	n := args.Senders
	if n <= 0 {
		n = max(fs.NumAvail(), 1)
	}
	hb := &HdbBatcher{
		groups: make(map[string]*hdbGroup, 8),
		idle:   make(chan *hdbSender, n),
		cb:     args.Cb,
		abort:  args.Abort,
		capFn:  fs.Cap,
		logHdr: args.LogHdr,
		size:   size,
		mod:    args.Mod,
	}
	hb.thr.init()
	for range n {
		hb.idle <- &hdbSender{hb: hb, smap: args.Smap, req: cmn.HdbReq{In: make([]cmn.HdbIn, size)}}
	}
	return hb
}

// the counters as updated throughout the job
func (hb *HdbBatcher) Stats() HdbStats {
	c := &hb.cnt
	return HdbStats{
		Batches:     c.batches.Load(),
		Fellback:    c.fellback.Load(),
		Unanswered:  c.unanswered.Load(),
		PeerBusy:    c.peerBusy.Load(),
		Dropped:     c.dropped.Load(),
		SmapChanged: c.smapChanged.Load(),
		Throttled:   c.throttled.Load(),
		Waited:      c.waited.Load(),
		Partial:     c.partial.Load(),
	}
}

func (hb *HdbBatcher) strict() bool {
	cs := hb.capFn()
	return int64(cs.PctMax) > cs.HighWM
}

// enqueue and send the group when it is full
func (hb *HdbBatcher) Add(lom *LOM, tsi *meta.Snode) {
	if hb.abort.IsAborted() {
		hb.cnt.dropped.Inc()
		return
	}
	ent := HdbEnt{Tsi: tsi}
	ent.init(lom)

	tid := tsi.ID()
	hb.mu.Lock()
	if hb.groups == nil {
		hb.mu.Unlock()
		debug.Assert(false, "Add after End")
		hb.cnt.dropped.Inc()
		return
	}
	g := hb.groups[tid]
	if g == nil {
		g = allocHdbGroup(tsi, hb.size)
		hb.groups[tid] = g
	}
	g.ents = append(g.ents, ent)
	if len(g.ents) < hb.size {
		hb.mu.Unlock()
		return
	}
	delete(hb.groups, tid)
	hb.mu.Unlock()

	hb.send(g)
}

// throttle, then start an idle sender
func (hb *HdbBatcher) send(g *hdbGroup) {
	if sleep := hb.thr.get(); sleep > 0 {
		hb.cnt.throttled.Add(int64(len(g.ents)))
		if !hb.strict() {
			n := hb.cnt.ndropped.Inc()
			cmn.SparseWarn(hb.mod, n, hb.logHdr, apc.HdbTag, "dropped", len(g.ents), "objects at Critical load [ groups:", n, "]")
			hb.drop(g)
			return
		}
		time.Sleep(sleep)
	}
	var s *hdbSender
	select {
	case s = <-hb.idle:
	default:
		if len(g.ents) == hb.size { // not End
			if n := hb.cnt.waited.Inc(); cos.Sparse(n) {
				nlog.Warningln(hb.logHdr, apc.HdbTag, "all", cap(hb.idle), "senders are busy [ wait:", n, "]")
			}
		}
		select {
		case s = <-hb.idle:
		case <-hb.abort.ChanAbort():
			hb.drop(g)
			return
		}
	}
	go hb.run(s, g)
}

func (hb *HdbBatcher) run(s *hdbSender, g *hdbGroup) {
	s.flush(g)
	hb.idle <- s
}

func (hb *HdbBatcher) drop(g *hdbGroup) {
	hb.cnt.dropped.Add(int64(len(g.ents)))
	freeHdbGroup(g)
}

// complete the remaining groups and wait for all senders
func (hb *HdbBatcher) End() HdbStats {
	hb.mu.Lock()
	groups := hb.groups
	hb.groups = nil
	hb.mu.Unlock()
	if groups == nil {
		debug.Assert(false, "second End")
		return hb.Stats()
	}

	hb.cnt.partial.Store(int64(len(groups)))
	for _, g := range groups {
		hb.send(g)
	}
	for range cap(hb.idle) {
		<-hb.idle
	}
	return hb.Stats()
}

/////////////////
// hdbThrottle //
/////////////////

func (th *hdbThrottle) init() {
	th.adv.Init(load.FlMem|load.FlCla, &load.Extra{RW: false}) // metadata-only
	th.sleep.Store(int64(th.adv.Sleep))
	th.next.Store(mono.NanoTime() + int64(hdbLoadIval))
}

// zero: do not throttle
func (th *hdbThrottle) get() time.Duration {
	now := mono.NanoTime()
	if now < th.next.Load() || !th.mu.TryLock() {
		return time.Duration(th.sleep.Load())
	}
	th.adv.Refresh()
	sleep := th.adv.Sleep
	th.sleep.Store(int64(sleep))
	th.next.Store(now + int64(hdbLoadIval))
	th.mu.Unlock()
	return sleep
}

///////////////
// hdbSender //
///////////////

// skip the remaining `n` objects after the abort or a change of targets
func (s *hdbSender) skip(n int) bool {
	var (
		hb  = s.hb
		cur = T.Sowner().Get()
	)
	switch {
	case hb.abort.IsAborted():
		hb.cnt.dropped.Add(int64(n))
	case !s.smap.CompareTargets(cur):
		hb.cnt.smapChanged.Add(int64(n))
		cnt := hb.cnt.nchanged.Inc()
		cmn.SparseWarn(hb.mod, cnt, hb.logHdr, apc.HdbTag, "targets changed:", s.smap.StringEx(), "=>", cur.StringEx(),
			"[ skipped:", n, "objects, groups:", cnt, "]")
	default:
		s.smap = cur // same targets
		return false
	}
	return true
}

// complete only one group
func (s *hdbSender) flush(g *hdbGroup) {
	if n := len(g.ents); !s.skip(n) {
		if n == 1 {
			s.fallback(g.ents, g.tsi)
		} else {
			s.batch(g.ents, g.tsi)
		}
	}
	freeHdbGroup(g)
}

func (s *hdbSender) batch(ents []HdbEnt, tsi *meta.Snode) {
	req := &s.req
	req.Bcks, req.In = req.Bcks[:0], req.In[:len(ents)]
	for i := range ents {
		ents[i].fill(&req.In[i], req.AddBck(&ents[i].bck))
	}
	hb := s.hb
	nb := hb.cnt.batches.Inc()

	resp, err := T.HeadBatchT2T(req, tsi)
	if err != nil {
		if cmn.IsErrHdbUnsupported(err) {
			cmn.SparseWarn(hb.mod, nb, hb.logHdr, tsi.StringEx(), apc.HdbTag, "of", len(ents),
				"unsupported:", err, "[ falling back to per-object HEAD ]")
			s.fallback(ents, tsi)
			return
		}
		tag := "failed:"
		if cmn.IsErrTooManyRequests(err) {
			// TODO: above the high watermark, retry the groups of a busy peer (429) once at End
			hb.cnt.peerBusy.Add(int64(len(ents)))
			tag = "busy:"
		} else {
			hb.cnt.unanswered.Add(int64(len(ents)))
		}
		cmn.SparseWarn(hb.mod, nb, hb.logHdr, tsi.StringEx(), apc.HdbTag, "of", len(ents),
			tag, err, "[ no per-object fallback ]")
		cause := err.Error()
		for i := range ents {
			ents[i].Status, ents[i].Cause = apc.HdbNone, cause
		}
		s.complete(ents)
		return
	}
	var m int
	for i := range ents {
		ents[i].Status = resp.Status[i]
		if m < len(resp.Msg) && resp.Msg[m].Idx == int32(i) {
			ents[i].Cause = resp.Msg[m].Msg
			m++
		}
	}
	s.complete(ents)
}

// per-object HEAD with local comparison
func (s *hdbSender) fallback(ents []HdbEnt, tsi *meta.Snode) {
	for i := range ents {
		if s.skip(len(ents) - i) {
			return
		}
		ent := &ents[i]
		ent.head(tsi)
		s.hb.cnt.fellback.Inc()
		if s.skip(len(ents) - i) {
			return
		}
		s.hb.cb(ent)
	}
}

// run the callbacks
func (s *hdbSender) complete(ents []HdbEnt) {
	for i := range ents {
		if s.skip(len(ents) - i) {
			return
		}
		s.hb.cb(&ents[i])
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

// the LOM has the same attributes that the peer compared
func (ent *HdbEnt) Unchanged(lom *LOM) bool {
	if lom.Lsize() != ent.LIF.Size || lom.Version() != ent.ver || !lom.Checksum().Equal(&ent.cksum) {
		return false
	}
	for i, key := range cmn.CheckEqKeys {
		if val, _ := lom.GetCustomKey(key); val != ent.md[i] {
			return false
		}
	}
	return true
}

// copy from the LOM
func (ent *HdbEnt) init(lom *LOM) {
	b := lom.Bucket()
	ent.bck = cmn.Bck{Name: b.Name, Provider: b.Provider, Ns: b.Ns}
	ent.LIF = lom.LIF()
	ent.name = lom.OrigObjName()
	ent.ver = lom.Version()
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
