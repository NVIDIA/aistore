// Package meta: cluster-level metadata
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package meta

import (
	"math"
	"math/big"
	"strconv"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/xoshiro256"
)

func TestHrwLog(t *testing.T) {
	// Pin fixed-point rounding across architectures and future changes. These
	// values are within one Q56 unit of high-precision -ln((hash+1)/2^64).
	for _, test := range []struct{ hash, want uint64 }{
		{0, 3196577161300663914},
		{1, 3146630643155341040},
		{2, 3117413802998738369},
		{3, 3096684125010018167},
		{0xffffffff, 1598288580650331957},
		{0x100000000, 1598288580633554741},
		{0x7fffffffffffffff, 49946518145322873},
		{0x8000000000000000, 49946518145322873},
		{0xc000000000000000, 20729677988720201},
		{math.MaxUint64 - 256, 1},
		{math.MaxUint64 - 1, 1},
		{math.MaxUint64, 1},
	} {
		got := hrwLog(test.hash)
		if got != test.want {
			t.Errorf("hash %x: got %d, want %d", test.hash, got, test.want)
		}
	}
	for i := range 10_000 {
		hash := xoshiro256.Hash(uint64(i + 1))
		got := float64(hrwLog(hash)) * 0x1p-56
		want := -math.Log(float64(hash) * 0x1p-64)
		if math.Abs(got-want) > 2e-15*max(want, 1) {
			t.Fatalf("hash %x: got %.18g, reference %.18g", hash, got, want)
		}
	}
	// Dense checks across each normalization boundary, including the lower endpoint.
	for e := range 64 {
		point := uint64(1) << e
		lo, hi := point-min(point, 1024), point+1024
		prev := hrwLog(lo)
		for h := lo + 1; h <= hi; h++ {
			next := hrwLog(h)
			if next > prev || next == 0 {
				t.Fatalf("non-monotonic log at %x: %d => %d", h, prev, next)
			}
			prev = next
		}
	}
}

func TestHrwScoreProducts(t *testing.T) {
	a, z := &Snode{}, &Snode{}
	a.Init("a", apc.Target, nil)
	z.Init("z", apc.Target, nil)
	for i := range 1024 {
		left := hrwScore{si: a, hash: xoshiro256.Hash(uint64(i)), weight: math.MaxInt64 - int64(i)}
		right := hrwScore{si: z, hash: xoshiro256.Hash(uint64(i + 1)), weight: int64(i + 1)}
		left.log, right.log = hrwLog(left.hash), hrwLog(right.hash)
		var lhs, rhs, weight big.Int
		lhs.SetUint64(left.log)
		weight.SetInt64(right.weight)
		lhs.Mul(&lhs, &weight)
		rhs.SetUint64(right.log)
		weight.SetInt64(left.weight)
		rhs.Mul(&rhs, &weight)
		want := lhs.Cmp(&rhs) < 0
		if left.better(&right) != want || right.better(&left) == want {
			t.Fatalf("128-bit comparison differs from big.Int at %d", i)
		}
	}
	left := hrwScore{si: a, hash: math.MaxUint64, weight: 1}
	right := hrwScore{si: z, hash: math.MaxUint64 - 1, weight: 1}
	if !left.better(&right) || right.better(&left) {
		t.Fatal("equal weights lost the full hash ordering")
	}
	left = hrwScore{si: a, hash: 7, log: 1, weight: 1}
	right = hrwScore{si: z, hash: 7, log: 2, weight: 2}
	if !left.better(&right) || right.better(&left) {
		t.Fatal("equal weighted scores did not use node IDs")
	}
}

func TestHrwObjMultiHome(t *testing.T) {
	m := &Smap{Tmap: make(NodeMap)}
	for i, id := range []string{"a", "b", "c"} {
		si := &Snode{}
		si.Init(id, apc.Target, nil)
		si.SetPlacementWeight(int64(i + 1))
		si.InitNetNamer()
		m.Tmap[id] = si
	}
	b := NewBck("hrw", apc.AIS, cmn.NsGlobal, &cmn.Bprops{})
	for _, placement := range []*cmn.PlacementConf{nil, {Weighting: cmn.WeightingUniform}} {
		b.Props.Placement = placement
		owner, err := m.HrwObjName2T(b, "object")
		if err != nil {
			t.Fatal(err)
		}
		si, netName, err := m.HrwObjMultiHome(b, "object")
		if err != nil || si != owner || netName != owner.nmr.name() {
			t.Fatalf("multihome: %v %s %v; owner %v", si, netName, err, owner)
		}
	}
	if _, netName, err := (&Smap{}).HrwObjMultiHome(b, "object"); err == nil || netName != cmn.NetPublic {
		t.Fatalf("empty multihome: %s %v", netName, err)
	}
}

func TestHrwLogLowerBound(t *testing.T) {
	check := func(hash uint64) {
		if lb, l := hrwLogLB(hash), hrwLog(hash); lb > l {
			t.Fatalf("hash %x: lower bound %d exceeds log %d", hash, lb, l)
		}
	}
	for i := range 1_000_000 {
		check(xoshiro256.Hash(uint64(i)))
	}
	// interval edges: every exponent, every table entry
	for e := range 64 {
		for t := range uint64(256) {
			edge := (uint64(1)<<63 + t<<55) >> e
			for _, h := range []uint64{edge - 2, edge - 1, edge, edge + 1} {
				check(h)
			}
		}
	}
	for _, h := range []uint64{0, 1, math.MaxUint64 - 1, math.MaxUint64} {
		check(h)
	}
}

// pruning by hrwLogLB must not change the result: compare with exhaustive exact scores
func TestHrwPruneExact(t *testing.T) {
	for _, weights := range [][]int64{
		{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		{1, 99, 1, 99, 50, 2, 3, 98, 97, 1},
		{1 << 40, 1<<40 + 1, 3 << 40, 1 << 41, 5 << 40},
	} {
		m := &Smap{Tmap: make(NodeMap, len(weights))}
		for i, w := range weights {
			si := &Snode{}
			si.Init("t"+strconv.Itoa(i), apc.Target, nil)
			si.SetPlacementWeight(w)
			m.Tmap[si.ID()] = si
		}
		m.InitDigests()
		for i := range 20_000 {
			digest := xoshiro256.Hash(uint64(i))
			var best *hrwScore
			for _, tsi := range m.Tmap {
				s := &hrwScore{si: tsi, hash: xoshiro256.Hash(tsi.digest() ^ digest), weight: tsi.PlacementWeight()}
				s.log = hrwLog(s.hash) // exact: no pruning
				if best == nil || s.better(best) {
					best = s
				}
			}
			got, err := m.hrwWeightedHash2T(digest, false)
			if err != nil || got != best.si {
				t.Fatalf("weights %v, digest %x: pruned %v, exact %s (%v)", weights, digest, got, best.si, err)
			}
			list, err := m.hrwWeightedTargetList(digest, 3)
			if err != nil || list[0] != best.si {
				t.Fatalf("weights %v, digest %x: list head %v, exact %s (%v)", weights, digest, list, best.si, err)
			}
		}
	}
}
