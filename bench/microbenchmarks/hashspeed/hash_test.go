// Package hashspeed is a benchmark througput benchmark
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package hashspeed

import (
	"hash"
	"math/rand"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/OneOfOne/xxhash"
	"github.com/minio/highwayhash"
)

// Examples:
//
// go test -bench=.
// go test -bench=Through -benchtime=10s
// go test -bench=Through -benchtime=30s -benchmem

const maxSize = 64 * 1024 * 1024

var (
	cores = runtime.GOMAXPROCS(0)
	hwkey [32]byte
	vec   = make([][]byte, cores)
)

func BenchmarkThroughput(b *testing.B) {
	rnd := rand.New(rand.NewSource(0xa5a5a5a5a5a5))
	for i := range vec {
		vec[i] = make([]byte, maxSize)
		rnd.Read(vec[i])
	}
	tests := []struct {
		name    string
		size    int64
		newHash func() (hash.Hash, error)
	}{
		{
			name:    "highwayhash-1M",
			size:    1024 * 1024,
			newHash: func() (hash.Hash, error) { return highwayhash.New(hwkey[:]) },
		},
		{
			name:    "highwayhash-8M",
			size:    8 * 1024 * 1024,
			newHash: func() (hash.Hash, error) { return highwayhash.New(hwkey[:]) },
		},
		{
			name:    "highwayhash-64M",
			size:    64 * 1024 * 1024,
			newHash: func() (hash.Hash, error) { return highwayhash.New(hwkey[:]) },
		},
		{
			name:    "xxhash-1M",
			size:    1024 * 1024,
			newHash: func() (hash.Hash, error) { return xxhash.New64(), nil },
		},
		{
			name:    "xxhash-8M",
			size:    8 * 1024 * 1024,
			newHash: func() (hash.Hash, error) { return xxhash.New64(), nil },
		},
		{
			name:    "xxhash-64M",
			size:    64 * 1024 * 1024,
			newHash: func() (hash.Hash, error) { return xxhash.New64(), nil },
		},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			throughput(b, test.size, test.newHash)
		})
	}
}

func throughput(b *testing.B, size int64, newHash func() (hash.Hash, error)) {
	var iter uint64
	b.SetBytes(size)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			h, err := newHash()
			assert(err == nil, "new")
			i := atomic.AddUint64(&iter, 1) % uint64(cores)
			l, err := h.Write(vec[i][:size])
			assert(int64(l) == size && err == nil, "write")
			h.Sum(nil)
		}
	})
}

func assert(cond bool, msg string) {
	if !cond {
		panic(msg)
	}
}
