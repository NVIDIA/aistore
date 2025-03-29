// Package lpi_test: local page iterator
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package lpi_test

import (
	"strconv"
	"sync"
	"testing"
)

func _clr(m map[int]struct{}) {
	for k := range m {
		delete(m, k)
	}
}

func BenchClear(b *testing.B) {
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

func BenchManualClear(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		m := make(map[int]struct{})
		for pb.Next() {
			for i := range 1000 {
				m[i] = struct{}{}
			}
			_clr(m)
		}
	})
}

// init
var keys []string

func init() {
	keys = make([]string, 1000)
	for i := range 1000 {
		keys[i] = "key-" + strconv.Itoa(i)
	}
}

var pool = sync.Pool{
	New: func() any {
		return make(map[string]struct{}, 1000)
	},
}

func BenchmarkPool(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m := pool.Get().(map[string]struct{})
			for i := range 1000 {
				k := keys[i]
				m[k] = struct{}{}
			}
			clear(m)
			pool.Put(m)
		}
	})
}

func BenchmarkNoPool(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m := make(map[string]struct{}, 1000)
			for i := range 1000 {
				k := keys[i]
				m[k] = struct{}{}
			}
		}
	})
}
