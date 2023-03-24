// Package mono_test contains standard vs monotonic clock benchmark
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package mono_test

import (
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn/mono"
)

// go test -tags=mono -bench="Fast|Std"

func BenchmarkFast(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = mono.Since(mono.NanoTime())
		}
	})
}

func BenchmarkStd(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = mono.Since(time.Now().UnixNano())
		}
	})
}
