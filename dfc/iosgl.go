// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

// Reader and (streaming) Writer on top of a Scatter Gather List (SGL) of reusable buffers

import (
	"errors"
	"io"
)

const initialScatterSize = 16

// FIXME: make it right
var clientbuffers = newbuffers(128 * 1024)

type SGLIO struct {
	sgl       [][]byte
	buffs     buffif
	fixedsize int64
	woff      int64 // stream
	roff      int64
}

func NewSGLIO(t *targetrunner, oosize uint64) *SGLIO {
	var (
		buffs     buffif
		fixedsize int64
	)
	osize := int64(oosize)
	if t == nil {
		buffs = clientbuffers
		fixedsize = clientbuffers.fixedsize
	} else if osize > t.buffers32k.fixedsize {
		buffs = t.buffers
		fixedsize = t.buffers.fixedsize
	} else if osize > t.buffers4k.fixedsize {
		buffs = t.buffers32k
		fixedsize = t.buffers32k.fixedsize
	} else {
		assert(osize != 0)
		buffs = t.buffers4k
		fixedsize = t.buffers4k.fixedsize
	}
	n := osize / fixedsize
	if n*fixedsize < osize {
		n++
	}
	sgl := make([][]byte, n, n)
	for i := 0; i < int(n); i++ {
		sgl[i] = buffs.alloc()
	}
	return &SGLIO{sgl: sgl, buffs: buffs, fixedsize: fixedsize}
}

func (z *SGLIO) Cap() int64 { return int64(len(z.sgl)) * z.fixedsize }

func (z *SGLIO) min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func (z *SGLIO) Write(p []byte) (n int, err error) {
	wlen := len(p)
	needtot := z.woff + int64(wlen)
	assert(needtot <= z.Cap()) // FIXME: grow
	idx, off, poff := z.woff/z.fixedsize, z.woff%z.fixedsize, 0
	for wlen > 0 {
		size := z.min(z.fixedsize-off, int64(wlen))
		buf := z.sgl[idx]
		copy(buf[off:], p[poff:poff+int(size)])
		z.woff += size
		idx++
		off = 0
		wlen -= int(size)
		poff += int(size)
	}
	return len(p), nil
}

func (z *SGLIO) Read(b []byte) (n int, err error) {
	if z.roff >= z.woff {
		return 0, io.EOF
	}
	idx, off := int(z.roff/z.fixedsize), z.roff%z.fixedsize
	buf := z.sgl[idx]
	size := z.min(int64(len(b)), z.woff-z.roff)
	n = copy(b[:size], buf[off:])
	z.roff += int64(n)
	for n < len(b) && idx < len(z.sgl)-1 {
		idx++
		buf = z.sgl[idx]
		size = z.min(int64(len(b)-n), z.woff-z.roff)
		n1 := copy(b[n:n+int(size)], buf)
		z.roff += int64(n1)
		n += n1
	}
	return
}

// reuse already allocated SGL
func (z *SGLIO) Reset() {
	z.woff, z.roff = 0, 0
}

func (z *SGLIO) Close() error {
	return nil
}

func (z *SGLIO) Free() {
	for i := 0; i < len(z.sgl); i++ {
		z.buffs.free(z.sgl[i])
	}
	z.sgl = nil
}

//========================================================================
//
// SGL Reader
//
//========================================================================
type Reader struct {
	z *SGLIO
	i int64 // current reading index
}

func (r *Reader) Len() int {
	if r.i >= r.z.Cap() {
		return 0
	}
	return int(r.z.Cap() - r.i)
}

func (r *Reader) Size() int64 { return r.z.Cap() }

func (r *Reader) Read(b []byte) (n int, err error) {
	if r.i >= r.z.Cap() {
		return 0, io.EOF
	}
	idx, off := int(r.i/r.z.fixedsize), r.i%r.z.fixedsize
	buf := r.z.sgl[idx]
	n = copy(b, buf[off:])
	for n < len(b) && idx < len(r.z.sgl)-1 {
		idx++
		buf = r.z.sgl[idx]
		n += copy(b[n:], buf)
	}
	r.i += int64(n)
	return
}

func (r *Reader) Seek(offset int64, whence int) (ret int64, err error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.z.roff + offset
	case io.SeekEnd:
		abs = r.z.woff + offset
	default:
		return 0, errors.New("Seek: invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("Seek: negative position")
	}
	r.i = abs
	return abs, nil
}

func NewReader(z *SGLIO) *Reader { return &Reader{z, 0} }
