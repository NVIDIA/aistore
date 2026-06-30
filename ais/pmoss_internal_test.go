// Package ais: internal unit tests
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools/tassert"

	jsoniter "github.com/json-iterator/go"
)

// ais/ml_test.go

func newTestSmapForMossScan(nodeCount int) *smapX {
	smap := newSmap()
	for i := range nodeCount {
		id := fmt.Sprintf("t%d", i)
		si := newSnode(id, apc.Target, meta.NetInfo{}, meta.NetInfo{}, meta.NetInfo{})
		smap.Tmap[id] = si
	}
	return smap
}

func TestMossScan_CollectsBuckets(t *testing.T) {
	body := []byte(`{
		"in": [
			{"objname": "obj1", "bucket": "b1"},
			{"objname": "obj2", "bucket": "b1"},
			{"objname": "obj3", "bucket": "b2"}
		]
	}`)

	smap := newTestSmapForMossScan(3)
	bck := &meta.Bck{Name: "default", Provider: apc.AIS}

	iter := jsoniter.ConfigFastest.BorrowIterator(body)
	bcks, _, err := mossScan(iter, bck, smap, 3, apc.ColocNone)
	jsoniter.ConfigFastest.ReturnIterator(iter)

	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(bcks) == 2, "expected 2 buckets, got %d", len(bcks))
	tassert.Errorf(t, bcks[0].Name == "b1", "expected first bucket to be 'b1', got %s", bcks[0].Name)
	tassert.Errorf(t, bcks[1].Name == "b2", "expected second bucket to be 'b2', got %s", bcks[1].Name)
}

func TestMossScan_ColocReturnsDT(t *testing.T) {
	body := []byte(`{
		"in": [
			{"objname": "obj1", "bucket": "b1"},
			{"objname": "obj2", "bucket": "b1"},
			{"objname": "obj3", "bucket": "b2"}
		]
	}`)

	smap := newTestSmapForMossScan(5)
	dflt := &meta.Bck{Name: "default", Provider: apc.AIS}

	iter := jsoniter.ConfigFastest.BorrowIterator(body)
	_, dt, err := mossScan(iter, dflt, smap, 3, apc.ColocOne)
	jsoniter.ConfigFastest.ReturnIterator(iter)

	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, dt != nil, "expected non-nil designated target")
}

func TestMossScan_DefaultBucket(t *testing.T) {
	body := []byte(`{
		"in": [
			{"objname": "obj1"},
			{"objname": "obj2"}
		]
	}`)

	smap := newTestSmapForMossScan(7)
	dflt := &meta.Bck{Name: "default", Provider: apc.AIS}

	iter := jsoniter.ConfigFastest.BorrowIterator(body)
	bcks, _, err := mossScan(iter, dflt, smap, 3, apc.ColocNone)
	jsoniter.ConfigFastest.ReturnIterator(iter)

	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(bcks) == 1, "expected 1 bucket, got %d", len(bcks))
	tassert.Errorf(t, bcks[0].Name == "default", "expected default bucket, got %s", bcks[0].Name)
	tassert.Errorf(t, bcks[0].Provider == apc.AIS, "expected AIS provider, got %q", bcks[0].Provider)
}

func TestMossScan_MissingBucket(t *testing.T) {
	body := []byte(`{"in":[{"objname":"obj1"}]}`)

	smap := newTestSmapForMossScan(1)

	iter := jsoniter.ConfigFastest.BorrowIterator(body)
	_, _, err := mossScan(iter, nil, smap, 3, apc.ColocNone)
	jsoniter.ConfigFastest.ReturnIterator(iter)

	tassert.Fatalf(t, err != nil, "expected missing bucket error")
}

// the proxy must authorize the same bucket the target will read; both sides resolve
// each entry through apc.MossIn, so a case-insensitive JSON key (e.g. "Bucket") maps identically
func TestMossScan_DecodeMatchesTarget(t *testing.T) {
	body := []byte(`{"in":[{"objname":"secret","Bucket":"private"}]}`)
	dflt := &meta.Bck{Name: "public", Provider: apc.AIS}

	// proxy side
	iter := jsoniter.ConfigFastest.BorrowIterator(body)
	bcks, _, err := mossScan(iter, dflt, newTestSmapForMossScan(1), 1, apc.ColocNone)
	jsoniter.ConfigFastest.ReturnIterator(iter)
	tassert.CheckFatal(t, err)

	// target side: how the target actually decodes the same body
	var req apc.MossReq
	tassert.CheckFatal(t, jsoniter.Unmarshal(body, &req))
	tgt, err := meta.BckFromUBP(req.In[0].Uname, req.In[0].Bucket, req.In[0].Provider)
	tassert.CheckFatal(t, err)

	// proxy must authorize the SAME bucket the target will read
	tassert.Fatalf(t, len(bcks) == 1 && bcks[0].Name == tgt.Name,
		"proxy authorized %q but target reads %q", bcks[0].Name, tgt.Name)
}

func TestMossScan_DecodeMatchesTargetTopLevelIn(t *testing.T) {
	body := []byte(`{"In":[{"objname":"secret","bucket":"private"}]}`)
	dflt := &meta.Bck{Name: "public", Provider: apc.AIS}

	iter := jsoniter.ConfigFastest.BorrowIterator(body)
	bcks, _, err := mossScan(iter, dflt, newTestSmapForMossScan(1), 1, apc.ColocNone)
	jsoniter.ConfigFastest.ReturnIterator(iter)
	tassert.CheckFatal(t, err)

	var req apc.MossReq
	tassert.CheckFatal(t, jsoniter.Unmarshal(body, &req))
	tgt, err := meta.BckFromUBP(req.In[0].Uname, req.In[0].Bucket, req.In[0].Provider)
	tassert.CheckFatal(t, err)

	tassert.Fatalf(t, len(bcks) == 1 && bcks[0].Name == tgt.Name,
		"proxy authorized %q but target reads %q", bcks[0].Name, tgt.Name)
}
