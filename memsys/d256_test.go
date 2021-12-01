// Package memsys provides memory management and Slab allocation
// with io.Reader and io.Writer interfaces on top of a scatter-gather lists
// (of reusable buffers)
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package memsys_test

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/memsys"
)

// 1. Run with all defaults:
// $ go test -bench=. -benchmem ./d256_test.go
//
// 2. Run a given bench for 30s:
// $ go test -bench=BenchmarkLargeAllocMax -benchtime=30s -benchmem ./d256_test.go

const (
	largeobj = cos.MiB * 8
	smallobj = cos.KiB * 512
	largefil = cos.GiB
)

// largobj alloc
func BenchmarkLargeAllocMax(b *testing.B) {
	benchAlloc(b, largeobj, memsys.MaxPageSlabSize)
}

func BenchmarkLargeAlloc128K(b *testing.B) {
	benchAlloc(b, largeobj, cos.KiB*128)
}

func BenchmarkLargeAlloc64K(b *testing.B) {
	benchAlloc(b, largeobj, cos.KiB*64)
}

func BenchmarkLargeAlloc32K(b *testing.B) {
	benchAlloc(b, largeobj, cos.KiB*32)
}

// smallobj alloc
func BenchmarkSmallAllocMax(b *testing.B) {
	benchAlloc(b, smallobj, memsys.MaxPageSlabSize)
}

func BenchmarkSmallAlloc128K(b *testing.B) {
	benchAlloc(b, smallobj, cos.KiB*128)
}

func BenchmarkSmallAlloc64K(b *testing.B) {
	benchAlloc(b, smallobj, cos.KiB*64)
}

func BenchmarkSmallAlloc32K(b *testing.B) {
	benchAlloc(b, smallobj, cos.KiB*32)
}

func benchAlloc(b *testing.B, objsiz, sbufSize int64) {
	mem := &memsys.MMSA{Name: "dmem", MinPctFree: 50}
	mem.Init(0, false, false)
	defer mem.Terminate(false)

	// reset initial conditions & start b-timer
	cos.FreeMemToOS()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sgl := mem.NewSGL(objsiz, sbufSize)
		_ = sgl
	}
}

// largobj write
func BenchmarkLargeWriteMax(b *testing.B) {
	benchWrite(b, largeobj, memsys.MaxPageSlabSize)
}

func BenchmarkLargeWrite128K(b *testing.B) {
	benchWrite(b, largeobj, cos.KiB*128)
}

func BenchmarkLargeWrite64K(b *testing.B) {
	benchWrite(b, largeobj, cos.KiB*64)
}

func BenchmarkLargeWrite32K(b *testing.B) {
	benchWrite(b, largeobj, cos.KiB*32)
}

// smallobj write
func BenchmarkSmallWriteMax(b *testing.B) {
	benchWrite(b, smallobj, memsys.MaxPageSlabSize)
}

func BenchmarkSmallWrite128K(b *testing.B) {
	benchWrite(b, smallobj, cos.KiB*128)
}

func BenchmarkSmallWrite64K(b *testing.B) {
	benchWrite(b, smallobj, cos.KiB*64)
}

func BenchmarkSmallWrite32K(b *testing.B) {
	benchWrite(b, smallobj, cos.KiB*32)
}

func benchWrite(b *testing.B, objsiz, sbufSize int64) {
	mem := &memsys.MMSA{Name: "emem", MinPctFree: 50}
	mem.Init(0, false, false)
	defer mem.Terminate(false)

	// reset initial conditions & start b-timer
	cos.FreeMemToOS()
	buf := make([]byte, cos.KiB*128)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sgl := mem.NewSGL(objsiz, sbufSize)
		for siz := 0; siz < int(objsiz); siz += len(buf) {
			sgl.Write(buf)
		}
	}
}

// largobj write => read => free
func BenchmarkLargeWRFMax(b *testing.B) {
	benchWRF(b, largeobj, memsys.MaxPageSlabSize)
}

func BenchmarkLargeWRF128K(b *testing.B) {
	benchWRF(b, largeobj, cos.KiB*128)
}

func BenchmarkLargeWRF64K(b *testing.B) {
	benchWRF(b, largeobj, cos.KiB*64)
}

func BenchmarkLargeWRF32K(b *testing.B) {
	benchWRF(b, largeobj, cos.KiB*32)
}

// smallobj write => read => free
func BenchmarkSmallWRFMax(b *testing.B) {
	benchWRF(b, smallobj, memsys.MaxPageSlabSize)
}

func BenchmarkSmallWRF128K(b *testing.B) {
	benchWRF(b, smallobj, cos.KiB*128)
}

func BenchmarkSmallWRF64K(b *testing.B) {
	benchWRF(b, smallobj, cos.KiB*64)
}

func BenchmarkSmallWRF32K(b *testing.B) {
	benchWRF(b, smallobj, cos.KiB*32)
}

func benchWRF(b *testing.B, objsiz, sbufSize int64) {
	mem := &memsys.MMSA{Name: "fmem", MinPctFree: 50}
	mem.Init(0, false, false)
	defer mem.Terminate(false)
	cha := make(chan *memsys.SGL, 1024*16)

	// reset initial conditions
	cos.FreeMemToOS()
	l := cos.KiB * 128
	buf := make([]byte, l)

	// delayed sgl.Free
	go func(cha chan *memsys.SGL) {
		time.Sleep(time.Second * 5)
		for {
			sgl := <-cha
			sgl.Free()
		}
	}(cha)

	b.ResetTimer() // <==== start

	for i := 0; i < b.N; i++ {
		sgl := mem.NewSGL(objsiz, sbufSize)
		for siz := 0; siz < int(objsiz); siz += l {
			n, _ := sgl.Write(buf)
			cos.Assert(n == l)
		}
		for siz := 0; siz < int(objsiz); siz += l {
			n, _ := sgl.Read(buf)
			cos.Assert(n == l)
		}
		select {
		case cha <- sgl:
		default:
		}
	}
	b.StopTimer() // wo/ defers
}

// file read to sgl
func BenchmarkLargeFileMax(b *testing.B) {
	benchFile(b, memsys.MaxPageSlabSize)
}

func BenchmarkLargeFile128K(b *testing.B) {
	benchFile(b, cos.KiB*128)
}

func BenchmarkLargeFile64K(b *testing.B) {
	benchFile(b, cos.KiB*64)
}

func BenchmarkLargeFile32K(b *testing.B) {
	benchFile(b, cos.KiB*32)
}

func benchFile(b *testing.B, sbufSize int64) {
	mem := &memsys.MMSA{Name: "gmem", MinPctFree: 50}
	mem.Init(0, false, false)
	defer mem.Terminate(false)

	// reset initial conditions
	cos.FreeMemToOS()

	file, err := os.CreateTemp("/tmp", "")
	if err != nil {
		b.Fatal(err)
	}
	n, _ := file.Write(make([]byte, largefil))
	if int64(n) != largefil {
		b.Fatal(n, largefil)
	}

	defer func() {
		file.Close()
		os.Remove(file.Name())
	}()

	slab, err := mem.GetSlab(sbufSize)
	cos.AssertNoErr(err)
	buf := slab.Alloc()
	defer slab.Free(buf)

	if int64(len(buf)) != sbufSize {
		b.Fatal(len(buf), sbufSize)
	}

	b.ResetTimer() // start timing it
	for i := 0; i < b.N; i++ {
		file.Seek(0, io.SeekStart)
		n, _ := io.CopyBuffer(io.Discard, file, buf)
		if n != largefil {
			b.Fatal(n, largefil)
		}
	}
	b.StopTimer() // wo/ defers
}
