// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos_test

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn/cos"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("BytePacker", func() {
	It("Boolean", func() {
		packer := cos.NewPacker(nil, 10)
		arr := []bool{true, false, false, true}
		for _, v := range arr {
			packer.WriteBool(v)
		}
		bytes := packer.Bytes()
		unpacker := cos.NewUnpacker(bytes)
		for _, v := range arr {
			read, err := unpacker.ReadBool()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(read).To(Equal(v))
		}
	})
	It("[U]int64", func() {
		packer := cos.NewPacker(nil, 80)
		for i := 100; i < 104; i++ {
			packer.WriteInt64(int64(i))
			packer.WriteUint64(uint64(i * 0xffffff))
		}
		bytes := packer.Bytes()
		unpacker := cos.NewUnpacker(bytes)
		for i := 100; i < 104; i++ {
			i64, err := unpacker.ReadInt64()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(i64).To(Equal(int64(i)))
			u64, err := unpacker.ReadUint64()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(u64).To(Equal(uint64(i * 0xffffff)))
		}
	})
	It("[U]int32", func() {
		packer := cos.NewPacker(nil, 80)
		for i := 100; i < 104; i++ {
			packer.WriteInt32(int32(i))
			packer.WriteUint32(uint32(i * 0xffff))
		}
		bytes := packer.Bytes()
		unpacker := cos.NewUnpacker(bytes)
		for i := 100; i < 104; i++ {
			i32, err := unpacker.ReadInt32()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(i32).To(Equal(int32(i)))
			u32, err := unpacker.ReadUint32()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(u32).To(Equal(uint32(i * 0xffff)))
		}
	})
	It("[U]int16", func() {
		packer := cos.NewPacker(nil, 80)
		for i := 100; i < 104; i++ {
			packer.WriteInt16(int16(i))
			packer.WriteUint16(uint16(i * 0xff))
		}
		bytes := packer.Bytes()
		unpacker := cos.NewUnpacker(bytes)
		for i := 100; i < 104; i++ {
			i16, err := unpacker.ReadInt16()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(i16).To(Equal(int16(i)))
			u16, err := unpacker.ReadUint16()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(u16).To(Equal(uint16(i * 0xff)))
		}
	})
	It("String", func() {
		packer := cos.NewPacker(nil, 120)
		for i := 100; i < 104; i++ {
			packer.WriteString(fmt.Sprintf("test-%04d", i))
		}
		bytes := packer.Bytes()
		unpacker := cos.NewUnpacker(bytes)
		for i := 100; i < 104; i++ {
			s, err := unpacker.ReadString()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(s).To(Equal(fmt.Sprintf("test-%04d", i)))
		}
	})
	It("MapStrU16", func() {
		packer := cos.NewPacker(nil, 400)
		m := make(cos.MapStrUint16, 4)
		for i := 100; i < 104; i++ {
			key := fmt.Sprintf("test-%04d", i)
			m[key] = uint16(i + 1)
		}
		for range 4 {
			packer.WriteMapStrUint16(m)
		}
		bytes := packer.Bytes()
		unpacker := cos.NewUnpacker(bytes)
		for range 4 {
			mp, err := unpacker.ReadMapStrUint16()
			Expect(err).ShouldNot(HaveOccurred())
			for k, v := range mp {
				val, ok := m[k]
				Expect(ok).To(BeTrue())
				Expect(val).To(Equal(v))
			}
		}
	})
})
