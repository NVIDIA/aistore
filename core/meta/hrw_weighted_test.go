// Package meta_test: unit tests for the package
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package meta_test

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/xoshiro256"
	"github.com/NVIDIA/aistore/core/meta"

	onexxh "github.com/OneOfOne/xxhash"
)

func hrwBucket(uniform bool) *meta.Bck {
	b := meta.NewBck("hrw", apc.AIS, cmn.NsGlobal, &cmn.Bprops{})
	if uniform {
		b.Props.Placement = &cmn.PlacementConf{Weighting: cmn.WeightingUniform}
	}
	return b
}

func hrwIDs(nodes meta.Nodes) []string {
	ids := make([]string, len(nodes))
	for i, si := range nodes {
		ids[i] = si.ID()
	}
	return ids
}

func hrwOwner(t *testing.T, b *meta.Bck, m *meta.Smap, digest uint64) string {
	t.Helper()
	si, err := m.HrwObjHash2T(b, digest)
	if err != nil {
		t.Fatal(err)
	}
	return si.ID()
}

func hrwRank(t *testing.T, b *meta.Bck, m *meta.Smap, digest uint64, count int) []string {
	t.Helper()
	sis, err := m.HrwObjTargetList(b, digest, count)
	if err != nil {
		t.Fatal(err)
	}
	return hrwIDs(sis)
}

func TestWeightedHRWUniform(t *testing.T) {
	for _, w := range []int64{0, 3, math.MaxInt64} {
		m := newWeightedSmap(map[string]int64{"a": w, "b": w, "c": w})
		b := hrwBucket(false)
		for i := range 1024 {
			name := fmt.Sprintf("dir/object-%d", i)
			uname := string(b.MakeUname(name))
			digest := onexxh.Checksum64S(cos.UnsafeB(uname), cos.MLCG32)
			owner, err := m.HrwName2T(cos.UnsafeB(uname))
			if err != nil {
				t.Fatal(err)
			}
			byName, err := m.HrwObjName2T(b, name)
			if err != nil || byName.ID() != owner.ID() || hrwOwner(t, b, m, digest) != owner.ID() {
				t.Fatalf("weight %d: legacy owner %s, bucket owner %v, err %v", w, owner, byName, err)
			}
			legacy, err := m.HrwTargetList(&uname, 3)
			if err != nil || !slices.Equal(hrwRank(t, b, m, digest, 3), hrwIDs(legacy)) {
				t.Fatalf("weight %d: legacy ranking differs, err %v", w, err)
			}
		}
	}
}

func TestWeightedHRWProportional(t *testing.T) {
	b := hrwBucket(false)
	for _, weights := range []map[string]int64{
		{"a": 1, "b": 3, "c": 6},
		{"a": 1 << 60, "b": 1<<60 + 1, "c": 1<<60 + 3},
	} {
		base := newWeightedSmap(weights)
		for _, mult := range []int64{2, math.MaxInt64 / 6} {
			if weights["c"] > math.MaxInt64/mult {
				continue
			}
			scaled := make(map[string]int64, len(weights))
			for id, w := range weights {
				scaled[id] = w * mult
			}
			other := newWeightedSmap(scaled)
			if !base.SamePlacementWeights(other) {
				t.Fatal("proportional weights must compare equal")
			}
			for i := range 4096 {
				digest := xoshiro256.Hash(uint64(i))
				rank := hrwRank(t, b, base, digest, 3)
				if !slices.Equal(rank, hrwRank(t, b, other, digest, 3)) || rank[0] != hrwOwner(t, b, other, digest) {
					t.Fatalf("weights %v * %d changed placement for %x", weights, mult, digest)
				}
			}
		}
	}
}

func TestWeightedHRWShares(t *testing.T) {
	const n = 100_000
	b := hrwBucket(false)
	m := newWeightedSmap(map[string]int64{"a": 1, "b": 3, "c": 6})
	counts := make(map[string]int)
	for i := range n {
		counts[hrwOwner(t, b, m, xoshiro256.Hash(uint64(i)))]++
	}
	for id, w := range map[string]float64{"a": 1, "b": 3, "c": 6} {
		p := w / 10
		want := n * p
		tolerance := 6 * math.Sqrt(n*p*(1-p))
		if math.Abs(float64(counts[id])-want) > tolerance {
			t.Errorf("%s: got %d, want %.0f +/- %.0f", id, counts[id], want, tolerance)
		}
	}
	t.Logf("1:3:6 shares: %v", counts)
}

func TestWeightedHRWMovement(t *testing.T) {
	b := hrwBucket(false)
	for _, change := range []string{"increase", "decrease", "join", "remove", "maintenance", "decommission"} {
		t.Run(change, func(t *testing.T) {
			base := newWeightedSmap(map[string]int64{"a": 1, "b": 3, "c": 6})
			other := newWeightedSmap(map[string]int64{"a": 1, "b": 3, "c": 6})
			id := "b"
			switch change {
			case "increase":
				other.Tmap[id].SetPlacementWeight(9)
			case "decrease":
				other.Tmap[id].SetPlacementWeight(1)
			case "join":
				id = "d"
				other.Tmap[id] = newWeightedSmap(map[string]int64{id: 4}).Tmap[id]
			case "remove":
				delete(other.Tmap, id)
			case "maintenance":
				other.Tmap[id].Flags = meta.SnodeMaint
			case "decommission":
				other.Tmap[id].Flags = meta.SnodeDecomm
			}
			moved := 0
			for i := range 4096 {
				digest := xoshiro256.Hash(uint64(i))
				a, z := hrwOwner(t, b, base, digest), hrwOwner(t, b, other, digest)
				if a != z {
					moved++
					switch change {
					case "increase", "join":
						if z != id {
							t.Fatalf("unrelated move %s => %s", a, z)
						}
					default:
						if a != id {
							t.Fatalf("unrelated move %s => %s", a, z)
						}
					}
				}
				left := hrwRank(t, b, base, digest, len(base.Tmap))
				right := hrwRank(t, b, other, digest, len(other.Tmap))
				left = slices.DeleteFunc(left, func(s string) bool { return s == id })
				right = slices.DeleteFunc(right, func(s string) bool { return s == id })
				if !slices.Equal(left, right) {
					t.Fatalf("unchanged targets reordered: %v vs %v", left, right)
				}
			}
			if moved == 0 {
				t.Fatal("expected ownership changes")
			}
		})
	}
}

func TestWeightedHRWPolicyAndAvailability(t *testing.T) {
	m := newWeightedSmap(map[string]int64{"a": 1, "b": 3, "c": 6, "d": 4})
	m.Tmap["d"].Flags = meta.SnodeMaint
	for _, uniform := range []bool{false, true} {
		b := hrwBucket(uniform)
		for i := range 256 {
			name := strconv.Itoa(i)
			digest := onexxh.Checksum64S(b.MakeUname(name), cos.MLCG32)
			full := hrwRank(t, b, m, digest, 4) // requesting all returns the three active targets
			if len(full) != 3 || slices.Contains(full, "d") || full[0] != hrwOwner(t, b, m, digest) {
				t.Fatalf("invalid active ranking %v", full)
			}
			for count := 1; count <= 3; count++ {
				if !slices.Equal(full[:count], hrwRank(t, b, m, digest, count)) {
					t.Fatal("ranking is not prefix-consistent")
				}
			}
			if uniform {
				si, err := m.HrwHash2T(digest)
				if err != nil || si.ID() != full[0] {
					t.Fatalf("uniform override ignored: %v, %v", full, err)
				}
			}
			byName, err := m.HrwObjName2T(b, name)
			if err != nil || byName.ID() != full[0] {
				t.Fatalf("name/digest mismatch: %v", err)
			}
			all, err := m.HrwObjHash2Tall(b, digest)
			if err != nil {
				t.Fatal(err)
			}
			active := newWeightedSmap(map[string]int64{"a": 1, "b": 3, "c": 6, "d": 4})
			if all.ID() != hrwOwner(t, b, active, digest) {
				t.Fatal("maintenance-inclusive selection differs from the all-active map")
			}
		}
		m.Tmap["c"].Flags = meta.SnodeDecomm
		for _, count := range []int{-1, 0, 3, 5} {
			if _, err := m.HrwObjTargetList(b, 0, count); err == nil {
				t.Errorf("count %d: expected an error", count)
			}
		}
		m.Tmap["c"].Flags = 0
	}
	for _, tsi := range m.Tmap {
		tsi.Flags = meta.SnodeMaint
	}
	b := hrwBucket(false)
	if _, err := m.HrwObjHash2T(b, 0); err == nil {
		t.Fatal("expected no active targets")
	}
	if nodes, err := m.HrwObjTargetList(b, 0, len(m.Tmap)); err != nil || len(nodes) != 0 {
		t.Fatalf("expected an empty list, got %v, %v", nodes, err)
	}
	if _, err := (&meta.Smap{}).HrwObjHash2T(b, 0); err == nil {
		t.Fatal("expected no targets")
	}
	if _, err := (&meta.Smap{}).HrwTargetList(nil, 1); !cmn.IsErrNotEnoughTargets(err) {
		t.Fatalf("legacy list must check availability before hashing: %v", err)
	}
}

func TestWeightedHRWTiesAndInvalidWeights(t *testing.T) {
	b := hrwBucket(false)
	for _, w := range []int64{0, 1, math.MaxInt64} {
		m := newWeightedSmap(map[string]int64{"a": w, "b": w, "c": w})
		for _, si := range m.Tmap {
			si.IDDigest = 42 // force a hash collision
		}
		for range 64 {
			if hrwOwner(t, b, m, 0) != "a" || !slices.Equal(hrwRank(t, b, m, 0, 3), []string{"a", "b", "c"}) {
				t.Fatal("hash ties depend on map order")
			}
			si, err := m.HrwHash2T(0)
			if err != nil || si.ID() != "a" {
				t.Fatal("uniform selector breaks ties differently")
			}
			uname := "collision"
			legacy, err := m.HrwTargetList(&uname, 3)
			if err != nil || !slices.Equal(hrwIDs(legacy), []string{"a", "b", "c"}) {
				t.Fatalf("legacy list breaks ties differently: %v", err)
			}
		}
	}
	// Distinguish adjacent weights above float64's exact integer range, even
	// when every target has the same hash and ID order favors the lighter one.
	m := newWeightedSmap(map[string]int64{"a": 1 << 60, "b": 1<<60 + 1, "c": 1<<60 + 2})
	for _, si := range m.Tmap {
		si.IDDigest = 42
	}
	if !slices.Equal(hrwRank(t, b, m, 0, 3), []string{"c", "b", "a"}) || hrwOwner(t, b, m, 0) != "c" {
		t.Fatal("lost low bits of large weights")
	}
	// (mixed zero and positive weights are not detected: unweighted check probes a single target)
	for _, w := range []int64{-1} {
		m := newWeightedSmap(map[string]int64{"a": w, "b": 3})
		if _, err := m.HrwObjHash2T(b, 0); err == nil {
			t.Fatalf("weight %d: expected invalid weights", w)
		}
		if _, err := m.HrwObjTargetList(b, 0, 1); err == nil {
			t.Fatalf("weight %d: expected invalid list weights", w)
		}
	}
}

///////////////
// benchmarks //
///////////////

func benchmarkHrwSmap(n int) *meta.Smap {
	m := &meta.Smap{Tmap: make(meta.NodeMap, n)}
	for i := range n {
		si := &meta.Snode{}
		si.Init(fmt.Sprintf("t%08d", i), apc.Target, nil)
		m.Tmap[si.ID()] = si
	}
	return m
}

func BenchmarkHRWUniform(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			m := benchmarkHrwSmap(n)
			var digest uint64
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				digest = xoshiro256.Hash(digest + 1)
				if _, err := m.HrwHash2T(digest); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkHRWBucket(b *testing.B) {
	for _, mode := range []string{"none", "equal", "weighted", "override"} {
		for _, n := range []int{10, 100, 1000} {
			b.Run(fmt.Sprintf("%s/%d", mode, n), func(b *testing.B) {
				m := benchmarkHrwSmap(n)
				bck := meta.NewBck("hrw", apc.AIS, cmn.NsGlobal, &cmn.Bprops{})
				for _, si := range m.Tmap {
					switch mode {
					case "equal":
						si.SetPlacementWeight(3)
					case "weighted", "override":
						si.SetPlacementWeight(int64(si.IDDigest%6 + 1))
					}
				}
				if mode == "override" {
					bck.Props.Placement = &cmn.PlacementConf{Weighting: cmn.WeightingUniform}
				}
				var digest uint64
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					digest = xoshiro256.Hash(digest + 1)
					if _, err := m.HrwObjHash2T(bck, digest); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkHRWTargetList(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			m := benchmarkHrwSmap(n)
			bck := hrwBucket(false)
			for _, si := range m.Tmap {
				si.SetPlacementWeight(int64(si.IDDigest%6 + 1))
			}
			var digest uint64
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				digest = xoshiro256.Hash(digest + 1)
				if _, err := m.HrwObjTargetList(bck, digest, 3); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// nil bucket: cluster default, same as a bucket without placement override
func TestWeightedHRWNilBucket(t *testing.T) {
	m := newWeightedSmap(map[string]int64{"a": 1, "b": 3, "c": 6})
	b := hrwBucket(false)
	for i := range 1024 {
		digest := xoshiro256.Hash(uint64(i))
		owner, err := m.HrwObjHash2T(nil, digest)
		if err != nil || owner.ID() != hrwOwner(t, b, m, digest) {
			t.Fatalf("nil bucket: %v, %v", owner, err)
		}
		list, err := m.HrwObjTargetList(nil, digest, 3)
		if err != nil || !slices.Equal(hrwIDs(list), hrwRank(t, b, m, digest, 3)) {
			t.Fatalf("nil bucket list: %v, %v", list, err)
		}
		all, err := m.HrwObjHash2Tall(nil, digest)
		if err != nil || all.ID() != owner.ID() {
			t.Fatalf("nil bucket (all): %v, %v", all, err)
		}
	}
}
