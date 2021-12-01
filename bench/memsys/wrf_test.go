// Package memsys_test contains the corresponding micro-benchmarks.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package memsys_test

import (
	"crypto/rand"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/memsys"
)

// 1. Run with all defaults:
// $ go test -bench=. -benchmem
//
// 2. Run each bench for 10s:
// $ go test -bench=. -benchtime=10s -benchmem

func BenchmarkWRF(b *testing.B) {
	tests := []struct{ payloadSz int64 }{
		{cos.MiB * 4},
		{cos.MiB},
		{cos.KiB * 128},
		{cos.KiB * 32},
		{cos.KiB * 4},
		{cos.KiB * 2},
		{cos.KiB},
		{128},
	}
	for _, test := range tests {
		name := cos.B2S(test.payloadSz, 0)
		b.Run(name, func(b *testing.B) { wrf(b, test.payloadSz) })
	}
}

func wrf(b *testing.B, payloadSz int64) {
	// 1. init default MMSAs
	gmm := memsys.PageMM()

	// 2. equalize initial conditions
	cos.FreeMemToOS()
	buf := make([]byte, cos.KiB*128)
	n, _ := rand.Read(buf)
	cos.Assert(n == cos.KiB*128)

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
				cos.Assert(n == int(slabSize))
			}
			for sz := int64(0); sz < payloadSz; sz += slabSize {
				n, _ := sgl.Read(buf[:slabSize])
				cos.Assert(n == int(slabSize))
			}
			sgl.Free()
		}
	})
}
