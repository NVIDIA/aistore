// Package hashspeed is a benchmark througput benchmark
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package hashspeed

import (
	cryptorand "crypto/rand"
	"hash"
	"runtime"
	"strconv"
	"testing"
	"unsafe"

	onexxh "github.com/OneOfOne/xxhash"
	cesxxh "github.com/cespare/xxhash/v2"
)

// Examples:
//
// go test -bench=.
// go test -bench=Through -benchtime=10s
// go test -bench=Through -benchtime=30s -benchmem
//
// go test -v -tags=debug -bench=ID -benchtime=10s

const (
	largeSize = 64 * 1024 * 1024
	numIDs    = 512 * 1024

	maxSizeID = 128
)

const MLCG32 = 1103515245 // xxhash seed

var (
	cores = runtime.GOMAXPROCS(0)
	vec   = make([][]byte, cores)

	vids       = make([][]byte, numIDs)
	vecAligned = make([][]int32, numIDs)
)

// 6 1 &[88 158 198 47 235 152 81 107 49 248 216 192 90 239 228 121]
// 6 2 [2fc69e58 6b5198eb -3f2707cf 79e4ef5a]
// 6 3 [88 158 198 47 235 152 81 107 49 248 216 192 90 239 228 121]

func BenchmarkID(b *testing.B) {
	var u int32
	for i := range vids {
		vids[i] = make([]byte, maxSizeID+1) // NOTE: to force misalign
		cryptorand.Read(vids[i])

		vecAligned[i] = make([]int32, maxSizeID/unsafe.Sizeof(u))
		bytes := (*[maxSizeID]byte)(unsafe.Pointer(&vecAligned[i][0]))
		copy(bytes[:], vids[i])
	}
	for _, size := range []int{16, 32, 64, 128} {
		s := strconv.Itoa(size)
		b.Run("aligned-one-"+s, func(b *testing.B) {
			aligned_one(b, size)
		})
		b.Run("na-one-"+s, func(b *testing.B) {
			na_one(b, size)
		})
		b.Run("aligned-cesxxh-"+s, func(b *testing.B) {
			aligned_cesxxh(b, size)
		})
		b.Run("na-cesxxh-"+s, func(b *testing.B) {
			na_cesxxh(b, size)
		})
	}
}

func aligned_one(b *testing.B, size int) {
	var (
		u int32
		n = size / int(unsafe.Sizeof(u))
	)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i := 0; i < numIDs; i++ {
				v := vecAligned[i][:n]
				bytes := (*[maxSizeID]byte)(unsafe.Pointer(&v[0]))
				_ = onexxh.Checksum64S(bytes[:size], MLCG32)
			}
		}
	})
}

func na_one(b *testing.B, size int) {
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i := 0; i < numIDs; i++ {
				v := vids[i][1 : size+1] // force misalignment
				_ = onexxh.Checksum64S(v, MLCG32)
			}
		}
	})
}

func aligned_cesxxh(b *testing.B, size int) {
	var (
		u int32
		n = size / int(unsafe.Sizeof(u))
	)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i := 0; i < numIDs; i++ {
				v := vecAligned[i][:n]
				bytes := (*[maxSizeID]byte)(unsafe.Pointer(&v[0]))
				h := cesxxh.New()
				h.Write(bytes[:size])
				_ = h.Sum64()
			}
		}
	})
}

func na_cesxxh(b *testing.B, size int) {
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i := 0; i < numIDs; i++ {
				v := vids[i][1 : size+1] // force misalignment
				h := cesxxh.New()
				h.Write(v)
				_ = h.Sum64()
			}
		}
	})
}

func BenchmarkThroughput(b *testing.B) {
	for i := range vec {
		vec[i] = make([]byte, largeSize)
		cryptorand.Read(vec[i])
	}
	tests := []struct {
		name    string
		size    int64
		newHash func() hash.Hash64
	}{
		{
			name: "cesxxh-1M", size: 1024 * 1024,
			newHash: func() hash.Hash64 { return cesxxh.New() },
		},
		{
			name: "cesxxh-8M", size: 8 * 1024 * 1024,
			newHash: func() hash.Hash64 { return cesxxh.New() },
		},
		{
			name: "cesxxh-64M", size: 64 * 1024 * 1024,
			newHash: func() hash.Hash64 { return cesxxh.New() },
		},
		{
			name: "xxhash-1M", size: 1024 * 1024,
			newHash: func() hash.Hash64 { return onexxh.New64() },
		},
		{
			name:    "xxhash-8M",
			size:    8 * 1024 * 1024,
			newHash: func() hash.Hash64 { return onexxh.New64() },
		},
		{
			name: "xxhash-64M", size: 64 * 1024 * 1024,
			newHash: func() hash.Hash64 { return onexxh.New64() },
		},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			throughput(b, test.size, test.newHash)
		})
	}
}

func throughput(b *testing.B, size int64, newHash func() hash.Hash64) {
	b.SetBytes(size)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			h := newHash()
			i = (i + 1) % cores
			l, err := h.Write(vec[i][:size])
			assert(int64(l) == size && err == nil, "write")
			h.Sum64()
		}
	})
}

func assert(cond bool, msg string) {
	if !cond {
		panic(msg)
	}
}
