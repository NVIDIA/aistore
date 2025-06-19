// Package memsys_test contains the corresponding micro-benchmarks.
/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION. All rights reserved.
 */
package syncmap_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/NVIDIA/aistore/cmn/mono"
)

// $ go test -bench=. -benchmem
// $ go test -bench=. -benchtime=10s -benchmem

const mapSize = 16 // power of 2

func BenchmarkSyncMap(b *testing.B) {
	var m sync.Map
	for i := range mapSize {
		m.Store(i, new(int64))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			j := int(mono.NanoTime() & (mapSize - 1)) // = mapSize -1
			for i := range mapSize {
				k := (j + i) & (mapSize - 1)
				v, ok := m.Load(k)
				if ok {
					val := v.(*int64)
					atomic.AddInt64(val, 1)
				}
			}
		}
	})
}

func BenchmarkRegMap(b *testing.B) {
	m := make(map[int]*int64, mapSize)
	for i := range mapSize {
		m[i] = new(int64)
	}
	lock := &sync.RWMutex{}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			j := int(mono.NanoTime() & (mapSize - 1)) // = mapSize -1
			for i := range mapSize {
				k := (j + i) & (mapSize - 1)
				lock.RLock()
				val, ok := m[k]
				lock.RUnlock()
				if ok {
					atomic.AddInt64(val, 1)
				}
			}
		}
	})
}
