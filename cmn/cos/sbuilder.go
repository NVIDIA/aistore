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
// - to optimally replace `strings.Builder` when applicable
// TODO:
// - use elsewhere (currently, scrub only)

type Builder struct {
	buf []byte
}

func (b *Builder) String() string {
	return unsafe.String(unsafe.SliceData(b.buf), len(b.buf))
}

func (b *Builder) Reset(size int) {
	switch {
	case b.buf == nil:
		b.buf = make([]byte, 0, size)
	case len(b.buf) >= size-size>>1: // vs. previous usage
		b.buf = make([]byte, 0, size<<1)
	default:
		b.buf = b.buf[:0]
	}
}

func (b *Builder) Len() int { return len(b.buf) }
func (b *Builder) Cap() int { return cap(b.buf) }

func (b *Builder) WriteByte(c byte)     { b.buf = append(b.buf, c) }
func (b *Builder) WriteString(s string) { b.buf = append(b.buf, s...) }
