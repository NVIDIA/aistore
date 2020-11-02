// Package memsys_test contains the corresponding micro-benchmarks.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package memsys_test

import (
	"crypto/rand"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/memsys"
)

// 1. Run with all defaults:
// $ go test -bench=. -benchmem
//
// 2. Run each bench for 10s with the immediate release of freed pages:
// $ GODEBUG=madvdontneed=1 go test -bench=. -benchtime=10s -benchmem

func BenchmarkWRF(b *testing.B) {
	tests := []struct{ payloadSz int64 }{
		{cmn.MiB * 4},
		{cmn.MiB},
		{cmn.KiB * 128},
		{cmn.KiB * 32},
		{cmn.KiB * 4},
		{cmn.KiB * 2},
		{cmn.KiB},
		{128},
	}
	for _, test := range tests {
		name := cmn.B2S(test.payloadSz, 0)
		b.Run(name, func(b *testing.B) { wrf(b, test.payloadSz) })
	}
}

func wrf(b *testing.B, payloadSz int64) {
	// 1. init default MMSAs
	gmm := memsys.DefaultPageMM()
	defer gmm.Terminate()
	smm := memsys.DefaultSmallMM()
	defer smm.Terminate()

	// 2. equalize initial conditions
	cmn.FreeMemToOS()
	buf := make([]byte, cmn.KiB*128)
	n, _ := rand.Read(buf)
	cmn.Assert(n == cmn.KiB*128)

	// 3. select MMSA & slab for the specified payload size
	mm, slab := gmm.SelectMemAndSlab(payloadSz)
	slabSize := slab.Size()

	// 4. run in parallel
	b.SetBytes(slabSize)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sgl := mm.NewSGL(payloadSz, slabSize)
			for sz := int64(0); sz < payloadSz; sz += slabSize {
				n, _ := sgl.Write(buf[:slabSize])
				cmn.Assert(n == int(slabSize))
			}
			for sz := int64(0); sz < payloadSz; sz += slabSize {
				n, _ := sgl.Read(buf[:slabSize])
				cmn.Assert(n == int(slabSize))
			}
			sgl.Free()
		}
	})
}
