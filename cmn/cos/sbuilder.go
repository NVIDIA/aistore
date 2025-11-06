// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"unsafe"
)

// implementation:
// - reusable, single-threaded, best-effort, and once-allocated
// motivation:
// - to optimally replace `strings.Sbuilder` when applicable
// usage:
// - currently, scrub only

type Sbuilder struct {
	buf []byte
}

func (b *Sbuilder) String() string {
	return unsafe.String(unsafe.SliceData(b.buf), len(b.buf))
}

func (b *Sbuilder) Reset(want int) {
	const (
		headroom = 64
	)
	var (
		prev = len(b.buf)
		curr = cap(b.buf)
	)
	want = max(want, prev+headroom)
	switch {
	case curr < want:
		// alloc
		b.buf = make([]byte, 0, want)
	case curr > want<<2:
		// shrink
		b.buf = make([]byte, 0, want<<1)
	default:
		// reuse as is
		b.buf = b.buf[:0]
	}
}

func (b *Sbuilder) Len() int { return len(b.buf) }
func (b *Sbuilder) Cap() int { return cap(b.buf) }

func (b *Sbuilder) WriteUint8(c byte) { b.buf = append(b.buf, c) }

func (b *Sbuilder) WriteString(s string) { b.buf = append(b.buf, s...) }
