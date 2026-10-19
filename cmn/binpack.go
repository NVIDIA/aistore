// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// Binary pack and unpack.
//
//	integers: big-endian
//	str:      | len uint16 | bytes |
//	attrs:    | size int64 | atime int64 | cksum-type str | cksum-value str | version str | (key str, value str)... | "" |

//
// pack
//

func packedStrSize(s string) int { return cos.SizeofI16 + len(s) }

func InsString(off int, to []byte, str string) int {
	return InsBytes(off, to, cos.UnsafeB(str))
}

func InsBytes(off int, to, b []byte) int {
	l := len(b)
	debug.Assert(l <= math.MaxUint16, "the field is uint16")
	binary.BigEndian.PutUint16(to[off:], uint16(l))
	off += cos.SizeofI16
	n := copy(to[off:], b)
	debug.Assert(n == l)
	return off + l
}

func InsUint16(off int, to []byte, i int) int {
	debug.Assert(i >= 0 && i <= math.MaxUint16)
	binary.BigEndian.PutUint16(to[off:], uint16(i))
	return off + cos.SizeofI16
}

func insInt64(off int, to []byte, i int64) int {
	return InsUint64(off, to, uint64(i))
}

func InsUint64(off int, to []byte, i uint64) int {
	binary.BigEndian.PutUint64(to[off:], i)
	return off + cos.SizeofI64
}

func packedAttrsSize(attr *ObjAttrs) int {
	ty, val := attrsCksum(attr)
	n := cos.SizeofI64 + cos.SizeofI64 + packedStrSize(ty) + packedStrSize(val) + packedStrSize(attr.Version())
	for k, v := range attr.GetCustomMD() {
		n += packedStrSize(k) + packedStrSize(v)
	}
	return n + packedStrSize("") // term
}

func InsAttrs(off int, to []byte, attr *ObjAttrs) int {
	off = insInt64(off, to, attr.Size)
	off = insInt64(off, to, attr.Atime)
	ty, val := attrsCksum(attr)
	off = InsString(off, to, ty)
	off = InsString(off, to, val)
	off = InsString(off, to, attr.Version())
	for k, v := range attr.GetCustomMD() {
		debug.Assert(k != "")
		off = InsString(off, to, k)
		off = InsString(off, to, v)
	}
	return InsString(off, to, "") // term
}

func attrsCksum(attr *ObjAttrs) (ty, val string) {
	if cksum := attr.Checksum(); cksum != nil {
		ty, val = cksum.Ty(), cksum.Val()
	}
	return ty, val
}

//
// unpack
//

type BinDecoder struct {
	b   []byte
	off int
	bad bool
}

func NewBinDecoder(b []byte) BinDecoder { return BinDecoder{b: b} }

func (d *BinDecoder) Bad() bool { return d.bad }
func (d *BinDecoder) Off() int  { return d.off }
func (d *BinDecoder) len() int  { return len(d.b) - d.off }

func (d *BinDecoder) take(n int) []byte {
	if d.bad {
		return nil
	}
	if n < 0 || d.off > len(d.b) || n > len(d.b)-d.off {
		d.bad = true
		return nil
	}
	b := d.b[d.off : d.off+n]
	d.off += n
	return b
}

func (d *BinDecoder) Bytes() []byte {
	b := d.take(cos.SizeofI16)
	if d.bad {
		return nil
	}
	return d.take(int(binary.BigEndian.Uint16(b)))
}

func (d *BinDecoder) Str() string {
	return string(d.Bytes())
}

// returns `prev` when equal
func (d *BinDecoder) StrCached(prev string) string {
	b := d.Bytes()
	if d.bad {
		return ""
	}
	if len(b) == len(prev) && (len(b) == 0 || bytes.Equal(b, cos.UnsafeB(prev))) {
		return prev
	}
	return string(b)
}

func (d *BinDecoder) Uint16() int {
	b := d.take(cos.SizeofI16)
	if d.bad {
		return 0
	}
	return int(binary.BigEndian.Uint16(b))
}

func (d *BinDecoder) Uint64() uint64 {
	b := d.take(cos.SizeofI64)
	if d.bad {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

func (d *BinDecoder) int64() int64 {
	return int64(d.Uint64())
}

func (d *BinDecoder) Attrs(attr, prev *ObjAttrs) {
	var prevCksumTyp, prevVersion string
	if prev != nil {
		if cksum := prev.Checksum(); cksum != nil {
			prevCksumTyp = cksum.Ty()
		}
		prevVersion = prev.Version()
	}

	attr.Size = d.int64()
	attr.Atime = d.int64()
	cksumTyp := d.StrCached(prevCksumTyp)
	cksumVal := d.Str()
	version := d.StrCached(prevVersion)
	if d.bad {
		return
	}

	attr.SetCksum(cksumTyp, cksumVal)
	attr.SetVersion(version)
	for {
		// custom-MD keys are never cached
		k := d.Str()
		if d.bad || k == "" {
			return
		}
		v := d.Str()
		if d.bad {
			return
		}
		attr.SetCustomKey(k, v)
	}
}
