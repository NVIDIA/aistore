// Package uuid_test benchmarks ID generation.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package uuid_test

import (
	"sync/atomic"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
)

var genIDSink atomic.Value

// go test ./bench/micro/uuid -run '^$' -bench . -benchmem -cpu 1,8
func BenchmarkGenID(b *testing.B)         { benchmarkGenID(b, false) }
func BenchmarkGenIDParallel(b *testing.B) { benchmarkGenID(b, true) }

func benchmarkGenID(b *testing.B, parallel bool) {
	cos.InitShortID(1)
	for _, test := range []struct {
		name string
		gen  func() string
	}{
		{"GenUUID", cos.GenUUID},
		{"CryptoRandS11", func() string { return cos.CryptoRandS(11) }},
		{"CryptoRandS16", func() string { return cos.CryptoRandS(16) }},
		{"GenTie", cos.GenTie},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			if parallel {
				b.RunParallel(func(pb *testing.PB) {
					var id string
					for pb.Next() {
						id = test.gen()
					}
					genIDSink.Store(id)
				})
				return
			}
			var id string
			for b.Loop() {
				id = test.gen()
			}
			genIDSink.Store(id)
		})
	}
}
