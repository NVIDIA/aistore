// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"unsafe"

	"github.com/NVIDIA/aistore/cmn/debug"
)

// yet another string buffer with assorted convenience

type SB struct {
	buf []byte
}

func (sb *SB) String() string {
	return unsafe.String(unsafe.SliceData(sb.buf), len(sb.buf))
}

func (sb *SB) CloneString() string { return string(sb.buf) }

func (sb *SB) Bytes() []byte { return sb.buf }

// - reserve n bytes at the tail of sb.buf
// - update sb's length
// - return the corresponding byte slice
func (sb *SB) ReserveAppend(n int) []byte {
	debug.Assertf(cap(sb.buf) >= len(sb.buf)+n, "insufficient capacity: %d vs %d", cap(sb.buf), len(sb.buf)+n)
	off := len(sb.buf)
	sb.buf = sb.buf[:off+n]
	return sb.buf[off : off+n]
}

func (sb *SB) Reset(want int, allowShrink bool) {
	const (
		headroom = 64
	)
	var (
		prev = len(sb.buf)
		curr = cap(sb.buf)
	)
	want = max(want, prev+headroom)
	switch {
	case curr < want:
		// alloc
		sb.buf = make([]byte, 0, want)
	case allowShrink && curr > (want<<2):
		// shrink
		sb.buf = make([]byte, 0, want<<1)
	default:
		// reuse as is
		sb.buf = sb.buf[:0]
	}
}

func (sb *SB) Len() int { return len(sb.buf) }
func (sb *SB) Cap() int { return cap(sb.buf) }

func (sb *SB) WriteUint8(c byte) { sb.buf = append(sb.buf, c) }

func (sb *SB) WriteString(s string) { sb.buf = append(sb.buf, s...) }
func (sb *SB) WriteBytes(b []byte)  { sb.buf = append(sb.buf, b...) }
