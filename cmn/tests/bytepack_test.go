// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package tests_test

import (
	"bytes"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/trand"
)

// A structure to test nested binary packing
type pck struct {
	id     int64
	group  int16
	name   string
	data   []byte
	parent *pck
}

func (p *pck) Pack(wr *cos.BytePack) {
	// write POD and variable-length fields in turns
	wr.WriteString(p.name)
	wr.WriteInt64(p.id)
	wr.WriteBytes(p.data)
	wr.WriteInt16(p.group)
	// A marker trick: before saving inner structure, put a boolean
	// marker that indicates if unpacker should skip reading inner struct
	if p.parent == nil {
		wr.WriteByte(0)
	} else {
		wr.WriteByte(1)
		wr.WriteAny(p.parent)
	}
}

func (p *pck) Unpack(rd *cos.ByteUnpack) (err error) {
	if p.name, err = rd.ReadString(); err != nil {
		return
	}
	if p.id, err = rd.ReadInt64(); err != nil {
		return
	}
	if p.data, err = rd.ReadBytes(); err != nil {
		return
	}
	if p.group, err = rd.ReadInt16(); err != nil {
		return
	}

	var exists byte
	if exists, err = rd.ReadByte(); err != nil {
		return
	}
	// Read inner struct only of the marker is `true`.
	if exists != 0 {
		// Do not forget to initialize inner field otherwise it may panic
		// if it is `nil` at this point.
		p.parent = &pck{}
		rd.ReadAny(p.parent)
	}
	return
}

func (p *pck) PackedSize() int {
	//    id              name&data len
	sz := cos.SizeofI64 + cos.SizeofLen*2 +
		// group        name len      data len      inner pointer marker
		cos.SizeofI16 + len(p.name) + len(p.data) + 1
	if p.parent != nil {
		// If inner struct is not `nil`, add its size to the total.
		sz += p.parent.PackedSize()
	}
	return sz
}

func TestBytePackStruct(t *testing.T) {
	first := &pck{
		id:     0x01020304,
		group:  0x0507,
		name:   "first",
		data:   nil,
		parent: nil,
	}
	second := &pck{
		id:    0x11121314,
		group: 0x1517,
		name:  "second item",
		data:  []byte("abcde"),
		parent: &pck{
			id:     0x21222324,
			group:  0x2527,
			name:   "inner item",
			data:   []byte("hijkl"),
			parent: nil,
		},
	}

	packer := cos.NewPacker(nil, first.PackedSize()+second.PackedSize())
	packer.WriteAny(first)
	packer.WriteAny(second)

	readFirst := &pck{}
	readSecond := &pck{}
	unpacker := cos.NewUnpacker(packer.Bytes())
	err := unpacker.ReadAny(readFirst)
	tassert.CheckFatal(t, err)
	err = unpacker.ReadAny(readSecond)
	tassert.CheckFatal(t, err)
	if first.id != readFirst.id ||
		first.group != readFirst.group ||
		first.name != readFirst.name ||
		len(readFirst.data) != 0 ||
		readFirst.parent != nil {
		t.Errorf("First: Read %+v mismatches original %+v", readFirst, first)
	}
	if second.id != readSecond.id ||
		second.group != readSecond.group ||
		second.name != readSecond.name ||
		!bytes.Equal(second.data, readSecond.data) ||
		readSecond.parent == nil {
		t.Errorf("Second: Read %+v mismatches original %+v", readSecond, second)
	}
	if second.parent.id != readSecond.parent.id ||
		second.parent.group != readSecond.parent.group ||
		second.parent.name != readSecond.parent.name ||
		!bytes.Equal(second.parent.data, readSecond.parent.data) ||
		readSecond.parent.parent != nil {
		t.Errorf("Second inner: Read %+v mismatches original %+v", readSecond.parent, second.parent)
	}
}

func BenchmarkPackWriteString(b *testing.B) {
	const (
		bufSize    = 64 * 1024 // 64KB
		stringLen  = 80
		numStrings = 1000
		entrySize  = cos.SizeofLen + stringLen // 4 + 80 = 84 bytes per WriteString
	)

	strs := make([]string, 0, numStrings)
	for range numStrings {
		strs = append(strs, trand.String(stringLen))
	}

	buf := make([]byte, bufSize)
	p := cos.NewPacker(buf, bufSize)
	written := 0

	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		// If buffer is about to overflow, reinitialize
		if written+entrySize > bufSize {
			b.StopTimer()
			p = cos.NewPacker(buf, bufSize)
			written = 0
			b.StartTimer()
		}
		p.WriteString(strs[i%numStrings])
		written += entrySize
	}
}
