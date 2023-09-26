// Package hashspeed is a benchmark througput benchmark
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package hashspeed

import (
	"hash"
	"math/rand"
	"runtime"
	"testing"
	"unsafe"

	"github.com/OneOfOne/xxhash"
	"github.com/minio/highwayhash"
)

// Examples:
//
// go test -bench=.
// go test -bench=Through -benchtime=10s
// go test -bench=Through -benchtime=30s -benchmem
//
// go test -v -tags=debug -bench=ID -benchtime=10s

const (
	numThr = 64 * 1024 * 1024
	numIDs = 262144

	sizeID = 16
)

const MLCG32 = 1103515245 // xxhash seed

var (
	cores = runtime.GOMAXPROCS(0)
	hwkey [32]byte
	vec   = make([][]byte, cores)

	vids       = make([][]byte, numIDs)
	vecAligned = make([][]int32, numIDs)
)

// 6 1 &[88 158 198 47 235 152 81 107 49 248 216 192 90 239 228 121]
// 6 2 [2fc69e58 6b5198eb -3f2707cf 79e4ef5a]
// 6 3 [88 158 198 47 235 152 81 107 49 248 216 192 90 239 228 121]

func BenchmarkID(b *testing.B) {
	rnd := rand.New(rand.NewSource(0xa5a5a5a5a5a5))
	for i := range vids {
		vids[i] = make([]byte, sizeID+1) // NOTE: to force misalign
		rnd.Read(vids[i])

		vecAligned[i] = make([]int32, 4)
		bytes := (*[sizeID]byte)(unsafe.Pointer(&vecAligned[i][0]))
		copy(bytes[:], vids[i])
	}

	b.Run("aligned", func(b *testing.B) {
		aligned(b)
	})

	b.Run("na", func(b *testing.B) {
		na(b)
	})
}

func aligned(b *testing.B) {
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i := 0; i < numIDs; i++ {
				v := vecAligned[i]
				bytes := (*[sizeID]byte)(unsafe.Pointer(&v[0]))
				_ = xxhash.Checksum64S(bytes[:], MLCG32)
			}
		}
	})
}

func na(b *testing.B) {
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i := 0; i < numIDs; i++ {
				v := vids[i][1:] // force misalignment
				_ = xxhash.Checksum64S(v, MLCG32)
			}
		}
	})
}

func BenchmarkThroughput(b *testing.B) {
	rnd := rand.New(rand.NewSource(0xa5a5a5a5a5a5))
	for i := range vec {
		vec[i] = make([]byte, numThr)
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
	b.SetBytes(size)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			h, err := newHash()
			assert(err == nil, "new")
			i = (i + 1) % cores
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
