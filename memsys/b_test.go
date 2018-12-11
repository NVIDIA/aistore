// Package memsys provides memory management and Slab allocation
// with io.Reader and io.Writer interfaces on top of a scatter-gather lists
// (of reusable buffers)
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package memsys_test

import (
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/memsys"
	"github.com/NVIDIA/dfcpub/tutils"
	"github.com/OneOfOne/xxhash"
)

func Test_sglhash(t *testing.T) {
	mem := &memsys.Mem2{Period: time.Second * 20, MinFree: cmn.GiB, Name: "amem", Debug: verbose}
	err := mem.Init(true /* ignore errors */)
	defer mem.Stop(nil)
	if err != nil {
		t.Fatal(err)
	}

	seed := time.Now().UnixNano()

	rnd0 := rand.New(rand.NewSource(seed))
	size := rnd0.Int63n(cmn.GiB) + cmn.KiB

	rnd1 := rand.New(rand.NewSource(seed))
	xxh1 := xxhash.New64()
	sgl := mem.NewSGLWithHash(size, xxh1)
	buf := sgl.Slab().Alloc()
	copyRand(sgl, rnd1, size, buf)
	sum1 := sgl.ComputeHash()

	rnd2 := rand.New(rand.NewSource(seed))
	xxh2 := xxhash.New64()
	copyRand(xxh2, rnd2, size, buf)
	sum2 := xxh2.Sum64()

	if sum1 != sum2 {
		t.Fatalf("same seed: %x != %x\n", sum1, sum2)
	}

	xxh3 := xxhash.New64()
	io.CopyBuffer(xxh3, sgl, buf)
	sum3 := xxh3.Sum64()

	if sum1 != sum3 {
		t.Fatalf("read sgl: %x != %x\n", sum1, sum3)
	}
	tutils.Logf("all hashes are equal (%x)\n", sum1)
}

func copyRand(dst io.Writer, rnd *rand.Rand, size int64, buf []byte) error {
	l64 := int64(len(buf))
	for rem, i := size, int64(0); i <= size/l64; i++ {
		n := int(cmn.MinI64(l64, rem))
		rnd.Read(buf[:n])
		m, err := dst.Write(buf[:n])
		if err != nil {
			return err
		}
		cmn.Assert(m == n)
		rem -= int64(m)
	}
	return nil
}
