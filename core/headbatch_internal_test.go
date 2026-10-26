// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools/tassert"
)

type (
	hdbTarget struct {
		Target
		batchErr error                    // HeadBatchT2T fails with this error
		onBatch  func()                   // HeadBatchT2T calls it first
		nop      bool                     // HeadBatchT2T answers and records nothing (benchmark)
		status   map[string]apc.HdbStatus // HeadBatchT2T answer by "bck/obj"
		heads    map[string]hdbHead       // HeadObjT2T answer by "bck/obj"
		names    map[string][]string      // the objects in all HeadBatchT2T calls by peer ID
		items    map[string]cmn.HdbIn     // the last request item by "bck/obj"
		size     int64                    // sum of the sizes in all HeadBatchT2T requests
		sowner   hdbSowner
		smap     *meta.Smap   // the current Smap at NewHdbBatcher
		cs       fs.CapStatus // zero: best-effort
		nmpath   int          // mountpaths and senders (zero: one)
		mu       sync.Mutex   // protects names, items, and size
	}
	hdbHead struct {
		op  *cmn.ObjectPropsV2
		err error
	}
	hdbDone   map[string]HdbEnt
	hdbBowner struct{ meta.BMD }
	// the batcher calls only Get
	hdbSowner struct {
		meta.Sowner
		smap atomic.Pointer[meta.Smap]
	}
	// IsAborted and ChanAbort as in xact.Base
	hdbStopper struct {
		cos.Stopper
		ch      chan error
		aborted atomic.Bool
	}
	// the first callback blocks until release (holds the sender)
	hdbGate struct {
		entered chan struct{}
		open    chan struct{}
		first   sync.Once
		once    sync.Once
	}
)

var hdbOa = &cmn.ObjAttrs{Cksum: cos.NewCksum(cos.ChecksumOneXxh, "0123456789abcdef")}

// above the high watermark: strict
var hdbHigh = fs.CapStatus{HighWM: 90, PctMax: 91}

func (t *hdbTarget) HeadBatchT2T(req *cmn.HdbReq, tsi *meta.Snode) (*apc.HdbResp, error) {
	if t.onBatch != nil {
		t.onBatch()
	}
	if t.batchErr != nil {
		return nil, t.batchErr
	}
	resp := apc.NewHdbResp(len(req.In))
	if t.nop {
		return resp, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for i := range req.In {
		in := &req.In[i]
		name := req.Bcks[in.Bidx].Name + "/" + in.Name
		t.names[tsi.ID()] = append(t.names[tsi.ID()], name)
		cp := *in
		cp.CustomMD, cp.Cksum = maps.Clone(in.CustomMD), in.Cksum.Clone()
		t.items[name] = cp
		t.size += in.Size
		status, ok := t.status[name]
		if !ok {
			status = apc.HdbSame
		}
		var cause error
		if status == apc.HdbDiverged {
			cause = errors.New("diverged " + name)
		}
		resp.Set(i, status, cause)
	}
	return resp, nil
}

func (*hdbTarget) Bowner() meta.Bowner { return hdbBuckets }

func (t *hdbTarget) Sowner() meta.Sowner { return &t.sowner }

func (o *hdbSowner) Get() *meta.Smap { return o.smap.Load() }

func (t *hdbTarget) HeadObjT2T(lom *LOM, _ *meta.Snode, _ ...string) (*cmn.ObjectPropsV2, error) {
	h := t.heads[lom.Bck().Name+"/"+lom.ObjName]
	return h.op, h.err
}

func newHdbStopper() *hdbStopper { return &hdbStopper{ch: make(chan error, 1)} }

func (s *hdbStopper) IsAborted() bool         { return s.aborted.Load() }
func (s *hdbStopper) ChanAbort() <-chan error { return s.ch }

func (s *hdbStopper) abort() {
	s.aborted.Store(true)
	s.ch <- errors.New("abort")
	close(s.ch)
}

func (d hdbDone) cb(ent *HdbEnt) { d[hdbKey(ent)] = *ent }

func newHdbGate() *hdbGate { return &hdbGate{entered: make(chan struct{}), open: make(chan struct{})} }

func (g *hdbGate) release() { g.once.Do(func() { close(g.open) }) }

func (g *hdbGate) cb(*HdbEnt) {
	g.first.Do(func() {
		close(g.entered)
		<-g.open
	})
}

var hdbBuckets = newHdbBowner()

// buckets "a", "b", and "c" (lif.LOM needs them)
func newHdbBowner() *hdbBowner {
	o := &hdbBowner{meta.BMD{Providers: make(meta.Providers)}}
	for i, name := range []string{"a", "b", "c"} {
		o.Add(meta.NewBck(name, apc.AIS, cmn.NsGlobal, &cmn.Bprops{BID: NewBID(uint64(i+1), true)}))
	}
	return o
}

func (o *hdbBowner) Get() *meta.BMD { return &o.BMD }

// install the peer and make a batcher
func hdbInit(tb testing.TB, tt *hdbTarget, args HdbArgs) (*HdbBatcher, hdbDone) {
	prev := T
	T = tt
	tb.Cleanup(func() { T = prev })
	tt.names, tt.items = make(map[string][]string), make(map[string]cmn.HdbIn)
	if args.Smap == nil {
		args.Smap = hdbSmap(1, nil, "t1", "t2")
	}
	tt.sowner.smap.Store(cos.NonZero(tt.smap, args.Smap))
	fs.NewTestMFS(nil)
	for i := range max(tt.nmpath, 1) {
		_, err := fs.AddTestMpath(tb.TempDir(), fmt.Sprintf("t%d", i))
		tassert.CheckFatal(tb, err)
	}
	done := make(hdbDone)
	if args.Cb == nil {
		args.Cb = done.cb
	}
	if args.Abort == nil {
		args.Abort = newHdbStopper()
	}
	hb := NewHdbBatcher(&args)
	hb.capFn = func() fs.CapStatus { return tt.cs }
	hdbPinLoad(hb, 0) // not the load of the host
	return hb, done
}

// pin the load sample (zero: do not throttle)
func hdbPinLoad(hb *HdbBatcher, sleep time.Duration) {
	hb.thr.sleep.Store(int64(sleep))
	hb.thr.next.Store(math.MaxInt64)
}

// wait until an Add waits for a sender
func hdbWaited(tb testing.TB, hb *HdbBatcher) {
	deadline := time.Now().Add(10 * time.Second)
	for hb.cnt.waited.Load() == 0 {
		tassert.Fatalf(tb, time.Now().Before(deadline), "Add did not wait for a sender")
		time.Sleep(time.Millisecond)
	}
}

// the LOM of the object with the attributes
func hdbLOM(tb testing.TB, bck, name string, oa *cmn.ObjAttrs) *LOM {
	lom := AllocLOM(name)
	b := meta.Bck{Name: bck, Provider: apc.AIS, Ns: cmn.NsGlobal}
	tassert.CheckFatal(tb, lom.InitBck(&b))
	lom.setbid(lom.Bprops().BID) // loaded
	lom.CopyAttrs(oa, false /*skipCksum*/)
	return lom
}

// the batcher must not keep the LOM
func hdbAdd(tb testing.TB, hb *HdbBatcher, tsi *meta.Snode, bck, name string, oa *cmn.ObjAttrs) {
	lom := hdbLOM(tb, bck, name, oa)
	hb.Add(lom, tsi)
	FreeLOM(lom)
}

func hdbKey(ent *HdbEnt) string { return ent.bck.Name + "/" + ent.name }

func hdbPeer(tid string) *meta.Snode { return &meta.Snode{DaeID: tid} }

func hdbSmap(ver int64, pids []string, tids ...string) *meta.Smap {
	smap := &meta.Smap{Version: ver, Pmap: make(meta.NodeMap), Tmap: make(meta.NodeMap)}
	for _, pid := range pids {
		smap.Pmap[pid] = hdbPeer(pid)
	}
	for _, tid := range tids {
		smap.Tmap[tid] = hdbPeer(tid)
	}
	return smap
}

func hdbCheckStats(tb testing.TB, st, want HdbStats) {
	tassert.Errorf(tb, st == want, "stats: got %+v, expected %+v", st, want)
}

// full groups go out at once and End sends the partial groups
func TestHdbBatcherBatches(t *testing.T) {
	tt := &hdbTarget{status: map[string]apc.HdbStatus{"a/o2": apc.HdbMissing, "b/o3": apc.HdbDiverged}, cs: hdbHigh}
	hb, done := hdbInit(t, tt, HdbArgs{Size: 4})
	for i := range 5 {
		bck := []string{"a", "b"}[i%2]
		hdbAdd(t, hb, hdbPeer("t1"), bck, fmt.Sprintf("o%d", i), &cmn.ObjAttrs{Size: int64(i + 1)})
	}
	for i := range 3 {
		hdbAdd(t, hb, hdbPeer("t2"), "c", fmt.Sprintf("o%d", i), hdbOa)
	}
	st := hb.End()

	tassert.Errorf(t, slices.Equal(tt.names["t1"], []string{"a/o0", "b/o1", "a/o2", "b/o3"}) && len(tt.names["t2"]) == 3,
		"batches: %v", tt.names)
	tassert.Errorf(t, tt.size == 1+2+3+4, "the request must carry the attributes: size %d", tt.size)
	tassert.Errorf(t, done["a/o2"].Status == apc.HdbMissing && done["b/o3"].Status == apc.HdbDiverged &&
		done["b/o3"].Cause == "diverged b/o3", "a/o2: %s, b/o3: %s %q", done["a/o2"].Status, done["b/o3"].Status, done["b/o3"].Cause)
	tassert.Errorf(t, len(done) == 8, "%d completed", len(done))
	hdbCheckStats(t, st, HdbStats{Batches: 2, Fellback: 1, Partial: 2})
}

// an unsupported peer gets per-object HEAD
func TestHdbBatcherFallback(t *testing.T) {
	var (
		cksum = cos.NewCksum(cos.ChecksumOneXxh, "0123456789abcdef")
		props = func(size int64, md cos.StrKVs) hdbHead {
			return hdbHead{op: &cmn.ObjectPropsV2{ObjAttrs: cmn.ObjAttrs{Size: size, Cksum: cksum, CustomMD: md}}}
		}
	)
	tests := map[string]struct {
		head   hdbHead
		status apc.HdbStatus
	}{
		"same":     {props(10, nil), apc.HdbSame},
		"diverged": {props(20, nil), apc.HdbDiverged},
		"crc32c":   {props(10, cos.StrKVs{cmn.CRC32CObjMD: "bb"}), apc.HdbDiverged}, // only a custom key differs
		"missing":  {hdbHead{err: cmn.NewErrHTTP(nil, errors.New("not found"), http.StatusNotFound)}, apc.HdbMissing},
		"failed":   {hdbHead{err: errors.New("broken pipe")}, apc.HdbFailed},
		"noprops":  {hdbHead{}, apc.HdbFailed},
	}
	oa := &cmn.ObjAttrs{Size: 10, Cksum: cksum, CustomMD: cos.StrKVs{cmn.CRC32CObjMD: "aa"}}
	tt := &hdbTarget{batchErr: fmt.Errorf("t1: %w", cmn.ErrHdbUnsupported), heads: make(map[string]hdbHead, len(tests)), cs: hdbHigh}
	hb, done := hdbInit(t, tt, HdbArgs{})
	for name, test := range tests {
		tt.heads["a/"+name] = test.head
		hdbAdd(t, hb, hdbPeer("t1"), "a", name, oa)
	}
	st := hb.End()

	for name, test := range tests {
		ent := done["a/"+name]
		tassert.Errorf(t, ent.Status == test.status, "%s: got %s, expected %s", name, ent.Status, test.status)
	}
	hdbCheckStats(t, st, HdbStats{Batches: 1, Fellback: int64(len(tests)), Partial: 1})
}

// a failed batch: no answer, 429 (peer-busy, no fallback), or the abort during the per-object fallback
func TestHdbBatcherBatchErr(t *testing.T) {
	for name, test := range map[string]struct {
		err   error
		abort bool
		want  HdbStats
	}{
		"refused": {err: errors.New("connection refused"), want: HdbStats{Batches: 1, Unanswered: 2}},
		"busy":    {err: cmn.NewErrTooManyRequests(errors.New("rejected"), http.StatusTooManyRequests), want: HdbStats{Batches: 1, PeerBusy: 2}},
		"abort":   {err: fmt.Errorf("t1: %w", cmn.ErrHdbUnsupported), abort: true, want: HdbStats{Batches: 1, Dropped: 2}},
	} {
		t.Run(name, func(t *testing.T) {
			ab := newHdbStopper()
			tt := &hdbTarget{batchErr: test.err}
			if test.abort {
				tt.onBatch = ab.abort
			}
			hb, done := hdbInit(t, tt, HdbArgs{Size: 2, Abort: ab})
			hdbAdd(t, hb, hdbPeer("t1"), "a", "o1", hdbOa)
			hdbAdd(t, hb, hdbPeer("t1"), "a", "o2", hdbOa)
			hdbCheckStats(t, hb.End(), test.want)
			for key, ent := range done {
				tassert.Errorf(t, ent.Status == apc.HdbNone && ent.Cause == test.err.Error(), "%s: %s %q", key, ent.Status, ent.Cause)
			}
		})
	}
}

// the request carries the attributes that CheckEq reads and nothing from the previous batch
func TestHdbBatcherRequest(t *testing.T) {
	tt := &hdbTarget{cs: hdbHigh}
	hb, _ := hdbInit(t, tt, HdbArgs{Size: 2})
	oa := &cmn.ObjAttrs{Size: 7, Cksum: cos.NewCksum(cos.ChecksumOneXxh, "0123456789abcdef"), CustomMD: cos.StrKVs{
		cos.HdrContentType: "application/x-tar", // CheckEq does not read it
	}}
	want := make(cos.StrKVs)
	for _, key := range cmn.CheckEqKeys {
		want[key] = "v-" + key
		oa.SetCustomKey(key, want[key])
	}
	hdbAdd(t, hb, hdbPeer("t1"), "a", "o1", oa)
	hdbAdd(t, hb, hdbPeer("t1"), "a", "o2", oa)
	// the next batch reuses the items
	hdbAdd(t, hb, hdbPeer("t1"), "a", "o3", &cmn.ObjAttrs{Size: 8})
	hdbAdd(t, hb, hdbPeer("t1"), "a", "o4", &cmn.ObjAttrs{Size: 8})
	hb.End()

	in := tt.items["a/o1"]
	tassert.Errorf(t, in.Size == 7 && in.Cksum.Equal(oa.Cksum), "a/o1: %v", &in.ObjAttrs)
	tassert.Errorf(t, maps.Equal(in.CustomMD, want), "a/o1: custom MD %v, expected %v", in.CustomMD, want)
	in = tt.items["a/o3"]
	tassert.Errorf(t, in.Size == 8 && cos.NoneC(in.Cksum) && len(in.CustomMD) == 0, "a/o3: %v", &in.ObjAttrs)
}

// a copy that changed after the peer compared it must not pass
func TestHdbEntUnchanged(t *testing.T) {
	hdbInit(t, &hdbTarget{}, HdbArgs{})
	attrs := func(size int64, cksum, etag string) *cmn.ObjAttrs {
		return &cmn.ObjAttrs{Size: size, Cksum: cos.NewCksum(cos.ChecksumOneXxh, cksum), CustomMD: cos.StrKVs{cmn.ETag: etag}}
	}
	var ent HdbEnt
	lom := hdbLOM(t, "a", "o1", attrs(10, "0123456789abcdef", "e1"))
	ent.init(lom)
	FreeLOM(lom)
	put := attrs(10, "0123456789abcdef", "e1")
	put.SetVersion("2")
	for name, test := range map[string]struct {
		oa   *cmn.ObjAttrs
		want bool
	}{
		"same":     {attrs(10, "0123456789abcdef", "e1"), true},
		"size":     {attrs(11, "0123456789abcdef", "e1"), false},
		"checksum": {attrs(10, "fedcba9876543210", "e1"), false},
		"etag":     {attrs(10, "0123456789abcdef", "e2"), false},
		"version":  {put, false},
	} {
		lom := hdbLOM(t, "a", "o1", test.oa)
		tassert.Errorf(t, ent.Unchanged(lom) == test.want, "%s: expected %t", name, test.want)
		FreeLOM(lom)
	}
}

// all senders are busy: Add waits until a sender is free or the job aborts
func TestHdbBatcherWait(t *testing.T) {
	for name, want := range map[string]HdbStats{
		"release": {Batches: 2, Fellback: 2, Waited: 1, Partial: 2},
		"abort":   {Batches: 1, Dropped: 1 + 2 + 1 + 1, Waited: 1, Partial: 1},
	} {
		t.Run(name, func(t *testing.T) {
			g, ab := newHdbGate(), newHdbStopper()
			hb, _ := hdbInit(t, &hdbTarget{}, HdbArgs{Size: 2, Abort: ab, Cb: g.cb}) // one mountpath, one sender
			t.Cleanup(g.release)
			hdbAdd(t, hb, hdbPeer("t2"), "a", "o0", hdbOa)
			hdbAdd(t, hb, hdbPeer("t1"), "a", "o1", hdbOa)
			hdbAdd(t, hb, hdbPeer("t1"), "a", "o2", hdbOa)
			<-g.entered
			hdbAdd(t, hb, hdbPeer("t1"), "b", "o0", hdbOa)
			added := make(chan struct{})
			go func() {
				hdbAdd(t, hb, hdbPeer("t1"), "b", "o1", hdbOa) // the group is full (wait for the sender)
				close(added)
			}()
			hdbWaited(t, hb)
			if name == "abort" {
				ab.abort()
			}
			g.release()
			<-added
			hdbAdd(t, hb, hdbPeer("t1"), "c", "o0", hdbOa)
			hdbCheckStats(t, hb.End(), want)
		})
	}
}

// at Critical load, best-effort drops the groups while strict sleeps and then sends them
func TestHdbBatcherThrottle(t *testing.T) {
	const sleep = 20 * time.Millisecond
	for name, test := range map[string]struct {
		cs   fs.CapStatus
		min  time.Duration // strict sleeps
		want HdbStats
	}{
		"best-effort": {want: HdbStats{Dropped: 3, Throttled: 3, Partial: 1}},
		"strict":      {cs: hdbHigh, min: 2 * sleep, want: HdbStats{Batches: 1, Fellback: 1, Throttled: 3, Partial: 1}},
	} {
		t.Run(name, func(t *testing.T) {
			hb, _ := hdbInit(t, &hdbTarget{cs: test.cs}, HdbArgs{Size: 2})
			hdbPinLoad(hb, sleep)
			started := time.Now()
			for i := range 3 { // a full group and a partial group
				hdbAdd(t, hb, hdbPeer("t1"), "a", fmt.Sprintf("o%d", i), hdbOa)
			}
			hdbCheckStats(t, hb.End(), test.want)
			tassert.Errorf(t, time.Since(started) >= test.min, "took %v, expected at least %v", time.Since(started), test.min)
		})
	}
}

// a new target stops the requests
func TestHdbBatcherSmap(t *testing.T) {
	for name, test := range map[string]struct {
		smap *meta.Smap
		want HdbStats
	}{
		"target": {hdbSmap(2, nil, "t1", "t2", "t3"), HdbStats{SmapChanged: 3, Partial: 1}},
		"proxy":  {hdbSmap(2, []string{"p1"}, "t1", "t2"), HdbStats{Batches: 1, Fellback: 1, Partial: 1}},
	} {
		t.Run(name, func(t *testing.T) {
			tt := &hdbTarget{cs: hdbHigh, smap: test.smap}
			hb, _ := hdbInit(t, tt, HdbArgs{Size: 2})
			for i := range 3 {
				hdbAdd(t, hb, hdbPeer("t1"), "a", fmt.Sprintf("o%d", i), hdbOa)
			}
			hdbCheckStats(t, hb.End(), test.want)
		})
	}
}

// each object completes once
func TestHdbBatcherConcurrent(t *testing.T) {
	const (
		nprod  = 8
		nobj   = 200
		nmpath = 4
	)
	var (
		mu   sync.Mutex
		tt   = &hdbTarget{cs: hdbHigh, nmpath: nmpath}
		done = make(map[string]int, nprod*nobj)
		wg   sync.WaitGroup
	)
	cb := func(ent *HdbEnt) {
		mu.Lock()
		done[ent.Tsi.ID()+":"+hdbKey(ent)]++
		mu.Unlock()
	}
	hb, _ := hdbInit(t, tt, HdbArgs{Size: 16, Cb: cb})
	tassert.Fatalf(t, cap(hb.idle) == nmpath, "%d senders, expected %d", cap(hb.idle), nmpath)
	for p := range nprod {
		wg.Add(1)
		go hdbProduce(t, hb, &wg, p, nobj)
	}
	wg.Wait()
	st := hb.End()

	tassert.Errorf(t, len(done) == nprod*nobj, "%d completed, expected %d", len(done), nprod*nobj)
	for key, cnt := range done {
		tassert.Fatalf(t, cnt == 1, "%s: %d callbacks", key, cnt)
	}
	tassert.Errorf(t, st.Dropped == 0 && st.SmapChanged == 0 && st.Unanswered == 0, "stats: %+v", st)
}

// `nobj` objects across three peers
func hdbProduce(tb testing.TB, hb *HdbBatcher, wg *sync.WaitGroup, p, nobj int) {
	defer wg.Done()
	peers := []*meta.Snode{hdbPeer("t1"), hdbPeer("t2"), hdbPeer("t3")}
	for i := range nobj {
		hdbAdd(tb, hb, peers[i%len(peers)], "a", fmt.Sprintf("p%d-o%d", p, i), hdbOa)
	}
}

// allocations for each Add
func BenchmarkHdbBatcher(b *testing.B) {
	hb, _ := hdbInit(b, &hdbTarget{nop: true, cs: hdbHigh}, HdbArgs{Cb: func(*HdbEnt) {}})
	lom := hdbLOM(b, "a", "o1", hdbOa)
	defer FreeLOM(lom)
	tsi := hdbPeer("t1")

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		hb.Add(lom, tsi)
	}
	if st := hb.End(); st.Dropped+st.SmapChanged != 0 {
		b.Fatalf("%+v", st)
	}
}
