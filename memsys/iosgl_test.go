// Package memsys provides memory management and Slab allocation
// with io.Reader and io.Writer interfaces on top of a scatter-gather lists
// (of reusable buffers)
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package memsys_test

import (
	"bytes"
	cryptorand "crypto/rand"
	"io"
	"strings"
	"testing/iotest"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/memsys"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func randReader(size int64) ([]byte, io.Reader) {
	buf := make([]byte, size)
	cryptorand.Read(buf)
	return buf, bytes.NewBuffer(buf)
}

var _ = Describe("SGL", func() {
	mm := memsys.PageMM()

	It("should perform write and read for SGL", func() {
		sgl := mm.NewSGL(0)
		err := cos.FloodWriter(sgl, 10*cos.MiB)
		Expect(err).ToNot(HaveOccurred())
		err = iotest.TestReader(sgl, sgl.Bytes())
		Expect(err).ToNot(HaveOccurred())
	})

	It("should read lines from SGL", func() {
		rnd := cos.NowRand()
		sgl := mm.NewSGL(0)
		num := rnd.IntN(1000) + 1
		arr := make([]string, num)
		str := []byte("A")
		for i := range num {
			str[0] = byte('A' + i%26)
			// NOTE in re (+1): skipping zero-length lines
			arr[i] = strings.Repeat(string(str), rnd.IntN(256)+1) + "\n"
			sgl.Write([]byte(arr[i]))
		}
		i := 0
		for {
			line, err := sgl.NextLine(nil, true)
			if err == io.EOF {
				break
			}
			Expect(err).ToNot(HaveOccurred())
			l := len(arr[i])
			Expect(line).To(BeEquivalentTo([]byte(arr[i][:l-1])))
			i++
		}
		Expect(i).To(Equal(num))
	})

	It("should properly write to SGL using WriteByte method", func() {
		size := int64(cos.MiB)
		buf, _ := randReader(size)

		sgl := mm.NewSGL(cos.KiB)

		for i := range size {
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

			sgl := mm.NewSGL(0, memsys.MaxPageSlabSize)

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
