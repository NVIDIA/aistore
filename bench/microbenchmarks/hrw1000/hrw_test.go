// Package hrw1000_test contains the corresponding micro-benchmarks.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package hrw1000_test

import (
	"fmt"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
)

// usage:
// $ go test -bench=. -benchtime=10s

// tunables
const (
	lenNodeID = 13
	numObjs   = 4_000_000 // number of objects in a bucket (constant to share common init between benches)
	lenUname  = 80
)

type test struct {
	nodes int // nodes in a cluster
}

var objs cos.StrSet

func BenchmarkHRW(b *testing.B) {
	tests := []test{
		{10},
		{100},
		{1000},
		{10_000},
	}
	ini()
	for _, t := range tests {
		name := t.name()
		b.Run(name, func(b *testing.B) { t.run(b) })
	}
}

func ini() {
	objs = make(cos.StrSet, numObjs)
	for range numObjs {
		objs[cos.CryptoRandS(lenUname)] = struct{}{}
	}
}

func (t *test) name() string {
	return fmt.Sprintf("cluster[%s]-bucket[%s]-uname[%d]", cos.FormatBigNum(t.nodes), cos.FormatBigNum(numObjs), lenUname)
}

func (t *test) run(b *testing.B) {
	// I. initialize
	var smap meta.Smap
	smap.Tmap = make(meta.NodeMap, t.nodes)

	for range t.nodes {
		tname := cos.CryptoRandS(lenNodeID)
		smap.Tmap[tname] = &meta.Snode{}
	}

	// II. run
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for uname := range objs { // one epoch
			for pb.Next() {
				_, err := smap.HrwName2T(cos.UnsafeB(uname))
				cos.AssertNoErr(err)
			}
		}
	})
}
