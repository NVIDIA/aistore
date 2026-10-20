// Package binpack provides primitives to pack and unpack binary intra-cluster messages
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package binpack

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// Layout:
//
//	integers: big-endian
//	str:      | len uint16 | bytes |

//
// pack
//

func StrSize(s string) int { return cos.SizeofI16 + len(s) }

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

func InsInt64(off int, to []byte, i int64) int {
	return InsUint64(off, to, uint64(i))
}

func InsUint64(off int, to []byte, i uint64) int {
	binary.BigEndian.PutUint64(to[off:], i)
	return off + cos.SizeofI64
}

//
// unpack
//

// Decoder remembers the first read past the end
// After that, all reads return zero values and Bad returns true
type Decoder struct {
	b   []byte
	off int
	bad bool
}

func NewDecoder(b []byte) Decoder { return Decoder{b: b} }

func (d *Decoder) Bad() bool { return d.bad }
func (d *Decoder) Off() int  { return d.off }
func (d *Decoder) Len() int  { return len(d.b) - d.off }

func (d *Decoder) take(n int) []byte {
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

func (d *Decoder) Bytes() []byte {
	b := d.take(cos.SizeofI16)
	if d.bad {
		return nil
	}
	return d.take(int(binary.BigEndian.Uint16(b)))
}

func (d *Decoder) Str() string {
	return string(d.Bytes())
}

// returns `prev` when equal
func (d *Decoder) StrCached(prev string) string {
	b := d.Bytes()
	if d.bad {
		return ""
	}
	if len(b) == len(prev) && (len(b) == 0 || bytes.Equal(b, cos.UnsafeB(prev))) {
		return prev
	}
	return string(b)
}

func (d *Decoder) Uint16() int {
	b := d.take(cos.SizeofI16)
	if d.bad {
		return 0
	}
	return int(binary.BigEndian.Uint16(b))
}

func (d *Decoder) Uint64() uint64 {
	b := d.take(cos.SizeofI64)
	if d.bad {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

func (d *Decoder) Int64() int64 {
	return int64(d.Uint64())
}
