// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"encoding/binary"
	"errors"
	"unsafe"

	"github.com/NVIDIA/aistore/cmn/debug"
)

// The module provides a way to encode/decode data as a compact binary slice.
// Comparison with JSON:
// Pros:
//  - more compact
//  - no reflection
//  - private fields can be serialized
//  - allows to pack a few separate structs or mix structs with PODs in
//    the same packet (impossible in JSON)
//  - depending on a value of a struct field you can implement
//    different pack/unpack functions to use only selected fields
// Cons:
//  - no type checking: garbage in, garbage out. JSON can skip invalid fields
//  - every struct that needs binary serialization must implement
//    both interfaces: Packer and Unpacker
//  - no automatic struct marshal/unmarshal. It is caller responsibility
//    to allocate long enough buffer for packing and for saving/restoring the
//    fields/structs in correct order
//  - in general, a field cannot be omitted. It can be simulated with markers.
//    Please, read tests/bytepack_test.go to see how to save/restore inner
//    pointer to a structure with a marker.
//    Hint: look for a field "parent".
//
// Notes on size calculation:
//  - for POD types: use constants from `cmn`: look for `Sizeof..` ones
//  - for strings and slices: do not forget to add size of length marker to
//    final PackedSize result for every slice and string. Never use SizeofI64
//    or any other integer size, use only `SizeofLen` that always represents
//    the current length of a size marker in the binary
//  - if a struct has an inner struct: do not forget to add its size to total
//
// How to use it:
// Packing:
// 1. Implement interfaces for all struct that are going to use binary packing
// 2. Add a method to a caller that calculates total pack size and allocates
//    a buffer for packing. It is OK to just allocate/use a buffer with
//    predefined big size.
// 3. Write all data to the create packer (cos.NewPacker)
// 4. When all data is written, get the packed data with `packer.Bytes()`.
// 5. Send this value as a regular `[]byte` slice
// NOTE: if you need to know how many bytes the packer contains, use
//    `len(packer.Bytes())` - `packer.Bytes` is a cheap operation as it
//    return a slice of internal byte array
// Unpacking:
// 1. Read `[]byte` from the source
// 2. Create unpacker with `NewUnpacker([]byte)`
// 3. Read data from unpacker in the same order you put the data in `Packing`
// 4. Reading never panics, so check read result for `error==nil` to be sure
//    that the data have been read

type (
	BytePack struct {
		off int
		b   []byte
	}

	ByteUnpack struct {
		off int
		b   []byte
	}

	MapStrUint16 map[string]uint16

	// Every object that is going to use binary representation instead of
	// JSON must implement two following methods
	Unpacker interface {
		Unpack(unpacker *ByteUnpack) error
	}

	Packer interface {
		Pack(packer *BytePack)
		PackedSize() int
	}
)

// Internally size of byte slice or string is packed as this integer type.
// Since binary packed structs are used for fairly small amount of data,
// it is possible to use int32 instead of int64 to keep the length of the data.
const SizeofLen = SizeofI32

var ErrBufferUnderrun = errors.New("buffer underrun")

// PackedStrLen returns the size occupied by a given string in the output
func PackedStrLen(s string) int {
	return SizeofLen + len(s)
}

func NewUnpacker(buf []byte) *ByteUnpack {
	return &ByteUnpack{b: buf}
}

func NewPacker(buf []byte, bufLen int) *BytePack {
	if buf == nil {
		return &BytePack{b: make([]byte, bufLen)}
	}
	return &BytePack{b: buf}
}

//
// Unpacker
//

func (br *ByteUnpack) Bytes() []byte {
	return br.b
}

func (br *ByteUnpack) ReadByte() (byte, error) {
	if br.off >= len(br.b) {
		return 0, ErrBufferUnderrun
	}
	Assert(br.off < len(br.b))
	b := br.b[br.off]
	br.off++
	return b, nil
}

func (br *ByteUnpack) ReadBool() (bool, error) {
	bt, err := br.ReadByte()
	return bt != 0, err
}

func (br *ByteUnpack) ReadInt64() (int64, error) {
	n, err := br.ReadUint64()
	return int64(n), err
}

func (br *ByteUnpack) ReadUint64() (uint64, error) {
	if len(br.b)-br.off < SizeofI64 {
		return 0, ErrBufferUnderrun
	}
	n := binary.BigEndian.Uint64(br.b[br.off:])
	br.off += SizeofI64
	return n, nil
}

func (br *ByteUnpack) ReadInt16() (int16, error) {
	n, err := br.ReadUint16()
	return int16(n), err
}

func (br *ByteUnpack) ReadUint16() (uint16, error) {
	if len(br.b)-br.off < SizeofI16 {
		return 0, ErrBufferUnderrun
	}
	n := binary.BigEndian.Uint16(br.b[br.off:])
	br.off += SizeofI16
	return n, nil
}

func (br *ByteUnpack) ReadInt32() (int32, error) {
	n, err := br.ReadUint32()
	return int32(n), err
}

func (br *ByteUnpack) ReadUint32() (uint32, error) {
	if len(br.b)-br.off < SizeofI32 {
		return 0, ErrBufferUnderrun
	}
	n := binary.BigEndian.Uint32(br.b[br.off:])
	br.off += SizeofI32
	return n, nil
}

func (br *ByteUnpack) ReadBytes() ([]byte, error) {
	if len(br.b)-br.off < SizeofLen {
		return nil, ErrBufferUnderrun
	}
	l, err := br.ReadUint32()
	if err != nil {
		return nil, err
	}
	if len(br.b)-br.off < int(l) {
		return nil, ErrBufferUnderrun
	}
	start := br.off
	br.off += int(l)
	return br.b[start : start+int(l)], nil
}

func (br *ByteUnpack) ReadString() (string, error) {
	bytes, err := br.ReadBytes()
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func (br *ByteUnpack) ReadAny(st Unpacker) error {
	return st.Unpack(br)
}

func (br *ByteUnpack) ReadMapStrUint16() (MapStrUint16, error) {
	l, err := br.ReadInt32()
	if err != nil {
		return nil, err
	}
	mp := make(MapStrUint16, l)
	for ; l > 0; l-- {
		key, err := br.ReadString()
		if err != nil {
			return nil, err
		}
		val, err := br.ReadUint16()
		if err != nil {
			return nil, err
		}
		mp[key] = val
	}
	return mp, nil
}

//
// Packer
//

func (bw *BytePack) WriteByte(b byte) {
	bw.b[bw.off] = b
	bw.off++
}

func (bw *BytePack) WriteBool(b bool) {
	if b {
		bw.b[bw.off] = 1
	} else {
		bw.b[bw.off] = 0
	}
	bw.off++
}

func (bw *BytePack) WriteInt64(i int64) {
	bw.WriteUint64(uint64(i))
}

func (bw *BytePack) WriteUint64(i uint64) {
	binary.BigEndian.PutUint64(bw.b[bw.off:], i)
	bw.off += SizeofI64
}

func (bw *BytePack) WriteInt16(i int16) {
	bw.WriteUint16(uint16(i))
}

func (bw *BytePack) WriteUint16(i uint16) {
	binary.BigEndian.PutUint16(bw.b[bw.off:], i)
	bw.off += SizeofI16
}

func (bw *BytePack) WriteInt32(i int32) {
	bw.WriteUint32(uint32(i))
}

func (bw *BytePack) WriteUint32(i uint32) {
	binary.BigEndian.PutUint32(bw.b[bw.off:], i)
	bw.off += SizeofI32
}

func (bw *BytePack) WriteBytes(b []byte) {
	bw.WriteString(*(*string)(unsafe.Pointer(&b)))
}

func (bw *BytePack) WriteString(s string) {
	l := len(s)
	bw.WriteUint32(uint32(l))
	if l == 0 {
		return
	}
	written := copy(bw.b[bw.off:], s)
	Assert(written == l)
	bw.off += l
}

func (bw *BytePack) WriteMapStrUint16(mp MapStrUint16) {
	l := int32(len(mp))
	bw.WriteInt32(l)
	if l == 0 {
		return
	}
	for k, v := range mp {
		bw.WriteString(k)
		bw.WriteUint16(v)
	}
}

func (bw *BytePack) WriteAny(st Packer) {
	prev := bw.off
	st.Pack(bw)
	debug.Assertf(
		bw.off-prev == st.PackedSize(),
		"%T declared %d, saved %d: %+v", st, st.PackedSize(), bw.off-prev, st,
	)
}

func (bw *BytePack) Bytes() []byte {
	return bw.b[:bw.off]
}
