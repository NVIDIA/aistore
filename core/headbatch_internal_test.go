// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"testing"

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
		status   map[string]apc.HdbStatus // HeadBatchT2T answer by "bck/obj"
		heads    map[string]hdbHead       // HeadObjT2T answer by "bck/obj"
		names    map[string][]string      // the objects in all HeadBatchT2T calls by peer ID
		items    map[string]cmn.HdbIn     // the last request item by "bck/obj"
		size     int64                    // sum of the sizes in all HeadBatchT2T requests
	}
	hdbHead struct {
		op  *cmn.ObjectPropsV2
		err error
	}
	hdbDone   map[string]HdbEnt
	hdbBowner struct{ meta.BMD }
)

var hdbOa = &cmn.ObjAttrs{Cksum: cos.NewCksum(cos.ChecksumOneXxh, "0123456789abcdef")}

func (t *hdbTarget) HeadBatchT2T(req *cmn.HdbReq, tsi *meta.Snode) (*apc.HdbResp, error) {
	if t.batchErr != nil {
		return nil, t.batchErr
	}
	resp := apc.NewHdbResp(len(req.In))
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
		switch status {
		case apc.HdbDiverged:
			cause = errors.New("diverged " + name)
		case apc.HdbBusy:
			delete(t.status, name)
		}
		resp.Set(i, status, cause)
	}
	return resp, nil
}

func (*hdbTarget) Bowner() meta.Bowner { return hdbBuckets }

func (t *hdbTarget) HeadObjT2T(lom *LOM, _ *meta.Snode, _ ...string) (*cmn.ObjectPropsV2, error) {
	h := t.heads[lom.Bck().Name+"/"+lom.ObjName]
	return h.op, h.err
}

func (d hdbDone) cb(ent *HdbEnt) { d[hdbKey(ent)] = *ent }

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
	fs.NewTestMFS(nil) // one mountpath (lif.LOM needs it)
	_, err := fs.AddTestMpath(tb.TempDir(), "t0")
	tassert.CheckFatal(tb, err)
	done := make(hdbDone)
	if args.Cb == nil {
		args.Cb = done.cb
	}
	return NewHdbBatcher(&args), done
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

func hdbCheckStats(tb testing.TB, hb *HdbBatcher, nbatches, nfellbk, nunanswered, npeerbusy int64) {
	b, f, u, p := hb.TakeStats()
	tassert.Errorf(tb, b == nbatches && f == nfellbk && u == nunanswered && p == npeerbusy,
		"stats: got (%d, %d, %d, %d), expected (%d, %d, %d, %d)", b, f, u, p, nbatches, nfellbk, nunanswered, npeerbusy)
}

func TestHdbBatcherFlush(t *testing.T) {
	tt := &hdbTarget{status: map[string]apc.HdbStatus{"a/o2": apc.HdbMissing, "b/o3": apc.HdbDiverged}}
	hb, done := hdbInit(t, tt, HdbArgs{Size: 4})
	for i := range 5 {
		bck := []string{"a", "b"}[i%2]
		hdbAdd(t, hb, hdbPeer("t1"), bck, fmt.Sprintf("o%d", i), &cmn.ObjAttrs{Size: int64(i + 1)})
	}
	for i := range 3 {
		hdbAdd(t, hb, hdbPeer("t2"), "c", fmt.Sprintf("o%d", i), hdbOa)
	}
	tassert.Fatalf(t, len(done) == 4 && hb.Pending() == 4, "Add must send only the full group: %d completed, %d pending",
		len(done), hb.Pending())

	hb.Flush()
	tassert.Errorf(t, slices.Equal(tt.names["t1"], []string{"a/o0", "b/o1", "a/o2", "b/o3"}) && len(tt.names["t2"]) == 3,
		"batches: %v", tt.names)
	tassert.Errorf(t, tt.size == 1+2+3+4, "the request must carry the attributes: size %d", tt.size)
	tassert.Errorf(t, done["a/o2"].Status == apc.HdbMissing && done["b/o3"].Status == apc.HdbDiverged &&
		done["b/o3"].Cause == "diverged b/o3", "a/o2: %s, b/o3: %s %q", done["a/o2"].Status, done["b/o3"].Status, done["b/o3"].Cause)
	tassert.Errorf(t, len(done) == 8 && hb.Pending() == 0, "%d completed, %d pending", len(done), hb.Pending())
	hdbCheckStats(t, hb, 2, 1, 0, 0)
	hdbCheckStats(t, hb, 0, 0, 0, 0) // TakeStats resets
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
	tt := &hdbTarget{batchErr: fmt.Errorf("t1: %w", cmn.ErrHdbUnsupported), heads: make(map[string]hdbHead, len(tests))}
	hb, done := hdbInit(t, tt, HdbArgs{})
	for name, test := range tests {
		tt.heads["a/"+name] = test.head
		hdbAdd(t, hb, hdbPeer("t1"), "a", name, oa)
	}
	hb.Flush()

	for name, test := range tests {
		ent := done["a/"+name]
		tassert.Errorf(t, ent.Status == test.status, "%s: got %s, expected %s", name, ent.Status, test.status)
	}
	hdbCheckStats(t, hb, 1, int64(len(tests)), 0, 0)
}

// any other batch error completes the objects as HdbNone
// (429: the peer is under critical load, counted as peer-busy and not as unanswered)
func TestHdbBatcherUnanswered(t *testing.T) {
	for _, test := range []struct {
		err        error
		unanswered int64
		peerBusy   int64
	}{
		{errors.New("connection refused"), 2, 0},
		{cmn.NewErrTooManyRequests(errors.New("rejected"), http.StatusTooManyRequests), 0, 2},
	} {
		tt := &hdbTarget{batchErr: test.err}
		hb, done := hdbInit(t, tt, HdbArgs{})
		hdbAdd(t, hb, hdbPeer("t1"), "a", "o1", hdbOa)
		hdbAdd(t, hb, hdbPeer("t1"), "a", "o2", hdbOa)
		hb.Flush()

		tassert.Errorf(t, len(done) == 2, "%v: expected 2 completed, got %d", test.err, len(done))
		for key, ent := range done {
			tassert.Errorf(t, ent.Status == apc.HdbNone && ent.Cause == test.err.Error(), "%s: %s %q", key, ent.Status, ent.Cause)
		}
		hdbCheckStats(t, hb, 1, 0, test.unanswered, test.peerBusy)
	}
}

// the callback Add only queues and Flush sends its objects
func TestHdbBatcherRetry(t *testing.T) {
	tt := &hdbTarget{status: map[string]apc.HdbStatus{"a/o1": apc.HdbBusy, "a/o2": apc.HdbBusy}}
	var hb *HdbBatcher
	hb, _ = hdbInit(t, tt, HdbArgs{Size: 2, Cb: func(ent *HdbEnt) {
		if ent.Status == apc.HdbBusy {
			hdbAdd(t, hb, ent.Tsi, ent.bck.Name, ent.name, hdbOa)
		}
	}})
	hdbAdd(t, hb, hdbPeer("t1"), "a", "o1", hdbOa)
	hdbAdd(t, hb, hdbPeer("t1"), "a", "o2", hdbOa)
	tassert.Fatalf(t, hb.Pending() == 2, "the callback must only queue the retries, pending: %d", hb.Pending())
	hb.Flush()

	tassert.Errorf(t, hb.Pending() == 0, "Flush must send the retries, pending: %d", hb.Pending())
	hdbCheckStats(t, hb, 2, 0, 0, 0)
}

// the request carries the attributes that CheckEq reads and nothing from the previous batch
func TestHdbBatcherRequest(t *testing.T) {
	tt := &hdbTarget{}
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
	in := tt.items["a/o1"]
	tassert.Errorf(t, in.Size == 7 && in.Cksum.Equal(oa.Cksum), "a/o1: %v", &in.ObjAttrs)
	tassert.Errorf(t, maps.Equal(in.CustomMD, want), "a/o1: custom MD %v, expected %v", in.CustomMD, want)

	// the next batch reuses the items
	hdbAdd(t, hb, hdbPeer("t1"), "a", "o3", &cmn.ObjAttrs{Size: 8})
	hdbAdd(t, hb, hdbPeer("t1"), "a", "o4", &cmn.ObjAttrs{Size: 8})
	in = tt.items["a/o3"]
	tassert.Errorf(t, in.Size == 8 && cos.NoneC(in.Cksum) && len(in.CustomMD) == 0, "a/o3: %v", &in.ObjAttrs)
}
