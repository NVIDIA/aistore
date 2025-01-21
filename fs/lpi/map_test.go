// Package lpi: local page iterator
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package lpi_test

import (
	"testing"
)

// [TODO] consider and bench lpi.Page alternatives:
// - sorted (reusable) slice with binary search
// - what else?

func clearMap(m map[int]struct{}) {
	for k := range m {
		delete(m, k)
	}
}

func BenchmarkClearFunction(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		m := make(map[int]struct{})
		for pb.Next() {
			for i := range 1000 {
				m[i] = struct{}{}
			}
			clear(m)
		}
	})
}

func BenchmarkManualClear(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		m := make(map[int]struct{})
		for pb.Next() {
			for i := range 1000 {
				m[i] = struct{}{}
			}
			clearMap(m)
		}
	})
}
