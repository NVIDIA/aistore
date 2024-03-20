// Package memsys_test contains the corresponding micro-benchmarks.
/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION. All rights reserved.
 */
package syncmap_test

import (
	"sync"
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

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			j := int(mono.NanoTime() & (mapSize - 1)) // = mapSize -1
			for i := j; i < j+mapSize; i++ {
				v, ok := m.Load(i)
				if ok {
					val := v.(*int64)
					*val++
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

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			j := int(mono.NanoTime() & (mapSize - 1)) // = mapSize -1
			for i := j; i < j+mapSize; i++ {
				lock.RLock()
				val, ok := m[i]
				lock.RUnlock()
				if ok {
					*val++
				}
			}
		}
	})
}
