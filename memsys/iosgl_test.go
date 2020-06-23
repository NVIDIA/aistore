// Package memsys provides memory management and Slab allocation
// with io.Reader and io.Writer interfaces on top of a scatter-gather lists
// (of reusable buffers)
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package memsys

import (
	"bytes"
	"io"
	"math/rand"

	"github.com/NVIDIA/aistore/cmn"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SGL", func() {
	mm := DefaultPageMM()

	randReader := func(size int64) ([]byte, io.Reader) {
		buf := make([]byte, size)
		rand.Read(buf)
		return buf, bytes.NewBuffer(buf)
	}

	Describe("ReadFrom", func() {
		It("should properly write to SGL using ReadFrom method", func() {
			size := int64(11*cmn.MiB + 2*cmn.KiB + 123)
			buf, r := randReader(size)

			sgl := mm.NewSGL(0)

			n, err := sgl.ReadFrom(r)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(size))

			b, err := sgl.ReadAll()
			Expect(err).ToNot(HaveOccurred())
			Expect(b).To(HaveLen(int(size)))
			Expect(b).To(BeEquivalentTo(buf))
		})

		It("should properly write to SGL with big slab using ReadFrom method", func() {
			size := int64(11*cmn.MiB + 2*cmn.KiB + 123)
			buf, r := randReader(size)

			sgl := mm.NewSGL(0, MaxPageSlabSize)

			n, err := sgl.ReadFrom(r)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(size))

			b, err := sgl.ReadAll()
			Expect(err).ToNot(HaveOccurred())
			Expect(b).To(HaveLen(int(size)))
			Expect(b).To(BeEquivalentTo(buf))
		})

		It("should properly write to preallocated SGL using ReadFrom method", func() {
			size := int64(11*cmn.MiB + 2*cmn.KiB + 123)
			buf, r := randReader(size)

			sgl := mm.NewSGL(size)

			n, err := sgl.ReadFrom(r)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(size))

			b, err := sgl.ReadAll()
			Expect(err).ToNot(HaveOccurred())
			Expect(b).To(HaveLen(int(size)))
			Expect(b).To(BeEquivalentTo(buf))
		})

		It("should properly write multiple times to SGL using ReadFrom method", func() {
			size := int64(11*cmn.MiB + 2*cmn.KiB + 123)
			_, r := randReader(size)

			sgl := mm.NewSGL(size)
			n, err := sgl.ReadFrom(r)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(size))

			_, r = randReader(size)
			n, err = sgl.ReadFrom(r)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(size))

			b, err := sgl.ReadAll()
			Expect(err).ToNot(HaveOccurred())
			Expect(b).To(HaveLen(2 * int(size)))
		})
	})
})
