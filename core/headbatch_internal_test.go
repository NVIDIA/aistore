// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"errors"
	"fmt"
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

func (d hdbDone) cb(ent *HdbEnt) { d[hdbKey(&ent.LIF)] = *ent }

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
	tt.names = make(map[string][]string)
	fs.NewTestMFS(nil) // one mountpath (lif.LOM needs it)
	_, err := fs.AddTestMpath(tb.TempDir(), "t0")
	tassert.CheckFatal(tb, err)
	done := make(hdbDone)
	if args.Cb == nil {
		args.Cb = done.cb
	}
	return NewHdbBatcher(&args), done
}

func hdbLIF(bck, name string) LIF {
	b := cmn.Bck{Name: bck, Provider: apc.AIS, Ns: cmn.NsGlobal}
	return LIF{Uname: string(b.MakeUname(name))}
}

func hdbKey(lif *LIF) string {
	b, name := cmn.ParseUname(lif.Uname)
	return b.Name + "/" + name
}

func hdbPeer(tid string) *meta.Snode { return &meta.Snode{DaeID: tid} }

func hdbCheckStats(tb testing.TB, hb *HdbBatcher, nbatches, nfellbk, nunanswered int64) {
	b, f, u := hb.TakeStats()
	tassert.Errorf(tb, b == nbatches && f == nfellbk && u == nunanswered,
		"stats: got (%d, %d, %d), expected (%d, %d, %d)", b, f, u, nbatches, nfellbk, nunanswered)
}

func TestHdbBatcherFlush(t *testing.T) {
	tt := &hdbTarget{status: map[string]apc.HdbStatus{"a/o2": apc.HdbMissing, "b/o3": apc.HdbDiverged}}
	hb, done := hdbInit(t, tt, HdbArgs{Size: 4})
	for i := range 5 {
		bck := []string{"a", "b"}[i%2]
		hb.Add(hdbLIF(bck, fmt.Sprintf("o%d", i)), hdbPeer("t1"), &cmn.ObjAttrs{Size: int64(i + 1)})
	}
	for i := range 3 {
		hb.Add(hdbLIF("c", fmt.Sprintf("o%d", i)), hdbPeer("t2"), hdbOa)
	}
	tassert.Fatalf(t, len(done) == 0 && hb.Pending() == 8, "Add must not send: %d completed", len(done))

	hb.Flush()
	tassert.Errorf(t, slices.Equal(tt.names["t1"], []string{"a/o0", "b/o1", "a/o2", "b/o3"}) && len(tt.names["t2"]) == 3,
		"batches: %v", tt.names)
	tassert.Errorf(t, tt.size == 1+2+3+4, "the request must carry the attributes: size %d", tt.size)
	tassert.Errorf(t, done["a/o2"].Status == apc.HdbMissing && done["b/o3"].Status == apc.HdbDiverged &&
		done["b/o3"].Cause == "diverged b/o3", "a/o2: %s, b/o3: %s %q", done["a/o2"].Status, done["b/o3"].Status, done["b/o3"].Cause)
	tassert.Errorf(t, len(done) == 8 && hb.Pending() == 0, "%d completed, %d pending", len(done), hb.Pending())
	hdbCheckStats(t, hb, 2, 1, 0)
	hdbCheckStats(t, hb, 0, 0, 0) // TakeStats resets
}

// an unsupported peer gets per-object HEAD
func TestHdbBatcherFallback(t *testing.T) {
	cksum := cos.NewCksum(cos.ChecksumOneXxh, "0123456789abcdef")
	tests := map[string]struct {
		head   hdbHead
		status apc.HdbStatus
	}{
		"same":     {hdbHead{op: &cmn.ObjectPropsV2{ObjAttrs: cmn.ObjAttrs{Size: 10, Cksum: cksum}}}, apc.HdbSame},
		"diverged": {hdbHead{op: &cmn.ObjectPropsV2{ObjAttrs: cmn.ObjAttrs{Size: 20, Cksum: cksum}}}, apc.HdbDiverged},
		"missing":  {hdbHead{err: cmn.NewErrHTTP(nil, errors.New("not found"), http.StatusNotFound)}, apc.HdbMissing},
		"failed":   {hdbHead{err: errors.New("broken pipe")}, apc.HdbFailed},
		"noprops":  {hdbHead{}, apc.HdbFailed},
	}
	tt := &hdbTarget{batchErr: fmt.Errorf("t1: %w", cmn.ErrHdbUnsupported), heads: make(map[string]hdbHead, len(tests))}
	hb, done := hdbInit(t, tt, HdbArgs{})
	for name, test := range tests {
		tt.heads["a/"+name] = test.head
		hb.Add(hdbLIF("a", name), hdbPeer("t1"), &cmn.ObjAttrs{Size: 10, Cksum: cksum})
	}
	hb.Flush()

	for name, test := range tests {
		ent := done["a/"+name]
		tassert.Errorf(t, ent.Status == test.status, "%s: got %s, expected %s", name, ent.Status, test.status)
	}
	hdbCheckStats(t, hb, 1, int64(len(tests)), 0)
}

// any other batch error completes the objects as HdbNone
func TestHdbBatcherUnanswered(t *testing.T) {
	tt := &hdbTarget{batchErr: errors.New("connection refused")}
	hb, done := hdbInit(t, tt, HdbArgs{})
	hb.Add(hdbLIF("a", "o1"), hdbPeer("t1"), hdbOa)
	hb.Add(hdbLIF("a", "o2"), hdbPeer("t1"), hdbOa)
	hb.Flush()

	tassert.Errorf(t, len(done) == 2, "expected 2 completed, got %d", len(done))
	for key, ent := range done {
		tassert.Errorf(t, ent.Status == apc.HdbNone && ent.Cause == tt.batchErr.Error(), "%s: %s %q", key, ent.Status, ent.Cause)
	}
	hdbCheckStats(t, hb, 1, 0, 2)
}

// Flush also sends the objects that the callback adds
func TestHdbBatcherRetry(t *testing.T) {
	tt := &hdbTarget{status: map[string]apc.HdbStatus{"a/o1": apc.HdbBusy, "a/o2": apc.HdbBusy}}
	var hb *HdbBatcher
	hb, _ = hdbInit(t, tt, HdbArgs{Cb: func(ent *HdbEnt) {
		if ent.Status == apc.HdbBusy {
			hb.Add(ent.LIF, ent.Tsi, &ent.Oa)
		}
	}})
	hb.Add(hdbLIF("a", "o1"), hdbPeer("t1"), hdbOa)
	hb.Add(hdbLIF("a", "o2"), hdbPeer("t1"), hdbOa)
	hb.Flush()

	tassert.Errorf(t, hb.Pending() == 0, "Flush must send the retries, pending: %d", hb.Pending())
	hdbCheckStats(t, hb, 2, 0, 0)
}
