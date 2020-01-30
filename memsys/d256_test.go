// Package memsys provides memory management and Slab allocation
// with io.Reader and io.Writer interfaces on top of a scatter-gather lists
// (of reusable buffers)
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package memsys_test

import (
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/memsys"
)

// HOW TO RUN:
//
// $ GODEBUG=madvdontneed=1 go test -v -bench=. -benchmem ./d256_test.go
//
// or, to run a single selected benchmark for 30s:
//
// $ GODEBUG=madvdontneed=1 go test -bench=BenchmarkLargeAllocMax -benchtime=30s -benchmem ./d256_test.go
//
// NOTE: running these benchmarks for less than 40s will likely generate non-reproducible (unstable) results

const (
	largeobj = cmn.MiB * 8
	smallobj = cmn.KiB * 512
	largefil = cmn.GiB
)

// largobj alloc
func BenchmarkLargeAllocMax(b *testing.B) {
	benchAlloc(b, largeobj, memsys.MaxPageSlabSize)
}
func BenchmarkLargeAlloc128K(b *testing.B) {
	benchAlloc(b, largeobj, cmn.KiB*128)
}
func BenchmarkLargeAlloc64K(b *testing.B) {
	benchAlloc(b, largeobj, cmn.KiB*64)
}
func BenchmarkLargeAlloc32K(b *testing.B) {
	benchAlloc(b, largeobj, cmn.KiB*32)
}

// smallobj alloc
func BenchmarkSmallAllocMax(b *testing.B) {
	benchAlloc(b, smallobj, memsys.MaxPageSlabSize)
}
func BenchmarkSmallAlloc128K(b *testing.B) {
	benchAlloc(b, smallobj, cmn.KiB*128)
}
func BenchmarkSmallAlloc64K(b *testing.B) {
	benchAlloc(b, smallobj, cmn.KiB*64)
}
func BenchmarkSmallAlloc32K(b *testing.B) {
	benchAlloc(b, smallobj, cmn.KiB*32)
}

func benchAlloc(b *testing.B, objsiz, sbufSize int64) {
	mem := &memsys.MMSA{MinPctFree: 50, Name: "dmem"}
	err := mem.Init(false /*panicOnErr*/)
	defer mem.Terminate()
	if err != nil {
		b.Fatal(err)
	}

	// reset initial conditions & start b-timer
	cmn.FreeMemToOS()
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
	benchWrite(b, largeobj, cmn.KiB*128)
}
func BenchmarkLargeWrite64K(b *testing.B) {
	benchWrite(b, largeobj, cmn.KiB*64)
}
func BenchmarkLargeWrite32K(b *testing.B) {
	benchWrite(b, largeobj, cmn.KiB*32)
}

// smallobj write
func BenchmarkSmallWriteMax(b *testing.B) {
	benchWrite(b, smallobj, memsys.MaxPageSlabSize)
}
func BenchmarkSmallWrite128K(b *testing.B) {
	benchWrite(b, smallobj, cmn.KiB*128)
}
func BenchmarkSmallWrite64K(b *testing.B) {
	benchWrite(b, smallobj, cmn.KiB*64)
}
func BenchmarkSmallWrite32K(b *testing.B) {
	benchWrite(b, smallobj, cmn.KiB*32)
}

func benchWrite(b *testing.B, objsiz, sbufSize int64) {
	mem := &memsys.MMSA{MinPctFree: 50, Name: "dmem"}
	err := mem.Init(false /*panicOnErr*/)
	defer mem.Terminate()
	if err != nil {
		b.Fatal(err)
	}

	// reset initial conditions & start b-timer
	cmn.FreeMemToOS()
	buf := make([]byte, cmn.KiB*128)
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
	benchWRF(b, largeobj, cmn.KiB*128)
}
func BenchmarkLargeWRF64K(b *testing.B) {
	benchWRF(b, largeobj, cmn.KiB*64)
}
func BenchmarkLargeWRF32K(b *testing.B) {
	benchWRF(b, largeobj, cmn.KiB*32)
}

// smallobj write => read => free
func BenchmarkSmallWRFMax(b *testing.B) {
	benchWRF(b, smallobj, memsys.MaxPageSlabSize)
}
func BenchmarkSmallWRF128K(b *testing.B) {
	benchWRF(b, smallobj, cmn.KiB*128)
}
func BenchmarkSmallWRF64K(b *testing.B) {
	benchWRF(b, smallobj, cmn.KiB*64)
}
func BenchmarkSmallWRF32K(b *testing.B) {
	benchWRF(b, smallobj, cmn.KiB*32)
}

func benchWRF(b *testing.B, objsiz, sbufSize int64) {
	mem := &memsys.MMSA{MinPctFree: 50, Name: "dmem"}
	err := mem.Init(false /*panicOnErr*/)
	cha := make(chan *memsys.SGL, 1024*16)
	defer mem.Terminate()
	if err != nil {
		b.Fatal(err)
	}

	// reset initial conditions
	cmn.FreeMemToOS()
	l := cmn.KiB * 128
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
			cmn.Assert(n == l)
		}
		for siz := 0; siz < int(objsiz); siz += l {
			n, _ := sgl.Read(buf)
			cmn.Assert(n == l)
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
	benchFile(b, cmn.KiB*128)
}
func BenchmarkLargeFile64K(b *testing.B) {
	benchFile(b, cmn.KiB*64)
}
func BenchmarkLargeFile32K(b *testing.B) {
	benchFile(b, cmn.KiB*32)
}

func benchFile(b *testing.B, sbufSize int64) {
	mem := &memsys.MMSA{MinPctFree: 50, Name: "dmem"}
	err := mem.Init(false /*panicOnErr*/)
	defer mem.Terminate()
	if err != nil {
		b.Fatal(err)
	}

	// reset initial conditions
	cmn.FreeMemToOS()

	file, err := ioutil.TempFile("/tmp", "")
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
	cmn.AssertNoErr(err)
	buf := slab.Alloc()
	defer slab.Free(buf)

	if int64(len(buf)) != sbufSize {
		b.Fatal(len(buf), sbufSize)
	}

	b.ResetTimer() // start timing it
	for i := 0; i < b.N; i++ {
		file.Seek(0, io.SeekStart)
		n, _ := io.CopyBuffer(ioutil.Discard, file, buf)
		if n != largefil {
			b.Fatal(n, largefil)
		}
	}
	b.StopTimer() // wo/ defers
}
