// Package memsys provides memory management and Slab allocation
// with io.Reader and io.Writer interfaces on top of a scatter-gather lists
// (of reusable buffers)
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package memsys

import (
	"bytes"
	"io"
	"math/rand"
	"testing/iotest"

	"github.com/NVIDIA/aistore/cmn/cos"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SGL", func() {
	mm := PageMM()

	randReader := func(size int64) ([]byte, io.Reader) {
		buf := make([]byte, size)
		rand.Read(buf)
		return buf, bytes.NewBuffer(buf)
	}

	It("should perform write and read for SGL", func() {
		sgl := mm.NewSGL(0)
		err := cos.FloodWriter(sgl, 10*cos.MiB)
		Expect(err).ToNot(HaveOccurred())
		err = iotest.TestReader(sgl, sgl.Bytes())
		Expect(err).ToNot(HaveOccurred())
	})

	It("should properly write to SGL using WriteByte method", func() {
		size := int64(cos.MiB)
		buf, _ := randReader(size)

		sgl := mm.NewSGL(cos.KiB)

		for i := int64(0); i < size; i++ {
			err := sgl.WriteByte(buf[i])
			Expect(err).ToNot(HaveOccurred())
		}
		b := sgl.ReadAll()
		Expect(b).To(HaveLen(int(size)))
		Expect(b).To(BeEquivalentTo(buf))
	})

	Describe("ReadFrom", func() {
		It("should properly write to SGL using ReadFrom method", func() {
			size := int64(11*cos.MiB + 2*cos.KiB + 123)
			buf, r := randReader(size)

			sgl := mm.NewSGL(0)

			n, err := sgl.ReadFrom(r)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(size))

			b := sgl.ReadAll()
			Expect(b).To(HaveLen(int(size)))
			Expect(b).To(BeEquivalentTo(buf))
		})

		It("should properly write to SGL with big slab using ReadFrom method", func() {
			size := int64(11*cos.MiB + 2*cos.KiB + 123)
			buf, r := randReader(size)

			sgl := mm.NewSGL(0, MaxPageSlabSize)

			n, err := sgl.ReadFrom(r)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(size))

			b := sgl.ReadAll()
			Expect(b).To(HaveLen(int(size)))
			Expect(b).To(BeEquivalentTo(buf))
		})

		It("should properly write to preallocated SGL using ReadFrom method", func() {
			size := int64(11*cos.MiB + 2*cos.KiB + 123)
			buf, r := randReader(size)

			sgl := mm.NewSGL(size)

			n, err := sgl.ReadFrom(r)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(size))

			b := sgl.ReadAll()
			Expect(b).To(HaveLen(int(size)))
			Expect(b).To(BeEquivalentTo(buf))
		})

		It("should properly write multiple times to SGL using ReadFrom method", func() {
			size := int64(11*cos.MiB + 2*cos.KiB + 123)
			_, r := randReader(size)

			sgl := mm.NewSGL(size)
			n, err := sgl.ReadFrom(r)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(size))

			_, r = randReader(size)
			n, err = sgl.ReadFrom(r)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(size))

			b := sgl.ReadAll()
			Expect(b).To(HaveLen(2 * int(size)))
		})
	})
})
