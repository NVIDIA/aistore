/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package iosgl provides Reader and (streaming) Writer on top of a Scatter Gather List (SGL) of reusable buffers.
package iosgl

import (
	"errors"
	"io"

	"github.com/NVIDIA/dfcpub/common"
)

type (
	// implements io.ReadWriteCloser  + Reset
	SGL struct {
		sgl  [][]byte
		slab *Slab
		woff int64 // stream
		roff int64
	}
	// uses the underlying SGL to implement io.ReadWriteCloser + io.Seeker
	Reader struct {
		z    *SGL
		roff int64
	}
	// uses the underlying SGL to implement io.ReadWriteCloser + io.Seeker
	SliceReader struct {
		z          *SGL
		roff       int64
		soff, slen int64
	}
)

// SGL implements io.ReadWriteCloser  + Reset (see https://golang.org/pkg/io/#ReadWriteCloser)
//
// SGL grows "automatically" and on demand upon writing.
// The package does not provide any mechanism to limit the sizes
// of allocated slabs or to react on memory pressure by dynamically shrinking slabs
// at runtime. The responsibility to call sgl.Reclaim (see below) lies with the user.

func NewSGL(immediateSize int64 /* the size to allocate at contrustion time */) *SGL {
	slab := SelectSlab(immediateSize)
	n := common.DivCeil(immediateSize, slab.Size())
	sgl := make([][]byte, n)
	for i := 0; i < int(n); i++ {
		sgl[i] = slab.Alloc()
	}
	return &SGL{sgl: sgl, slab: slab}
}

func (z *SGL) Cap() int64  { return int64(len(z.sgl)) * z.slab.Size() }
func (z *SGL) Size() int64 { return z.woff }
func (z *SGL) Slab() *Slab { return z.slab }

func (z *SGL) grow(toSize int64) {
	for z.Cap() < toSize {
		z.sgl = append(z.sgl, z.slab.Alloc()) // FIXME: OOM
	}
}

func (z *SGL) Write(p []byte) (n int, err error) {
	wlen := len(p)
	needtot := z.woff + int64(wlen)
	if needtot > z.Cap() {
		z.grow(needtot)
	}
	idx, off, poff := z.woff/z.slab.Size(), z.woff%z.slab.Size(), 0
	for wlen > 0 {
		size := common.MinI64(z.slab.Size()-off, int64(wlen))
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

func (z *SGL) Read(b []byte) (n int, err error) {
	n, err, z.roff = z.readAtOffset(b, z.roff)
	return
}

func (z *SGL) readAtOffset(b []byte, roffin int64) (n int, err error, roff int64) {
	roff = roffin
	if roff >= z.woff {
		err = io.EOF
		return
	}
	idx, off := int(roff/z.slab.Size()), roff%z.slab.Size()
	buf := z.sgl[idx]
	size := common.MinI64(int64(len(b)), z.woff-roff)
	n = copy(b[:size], buf[off:])
	roff += int64(n)
	for n < len(b) && idx < len(z.sgl)-1 {
		idx++
		buf = z.sgl[idx]
		size = common.MinI64(int64(len(b)-n), z.woff-roff)
		n1 := copy(b[n:n+int(size)], buf)
		roff += int64(n1)
		n += n1
	}
	if n < len(b) {
		err = io.EOF
	}
	return
}

// reuse already allocated SGL
func (z *SGL) Reset() { z.woff, z.roff = 0, 0 }

func (z *SGL) Close() error { return nil }

func (z *SGL) Free() {
	for i := 0; i < len(z.sgl); i++ {
		z.slab.Free(z.sgl[i])
	}
	z.sgl = z.sgl[:0]
	z.sgl, z.slab = nil, nil
	z.woff = 0xDEADBEEF
}

// Reclaim slab-allocated memory on demand
// Quoting https://golang.org/pkg/sync/#Pool:
// "Any item stored in the Pool may be removed automatically at any time without notification."
// Meaning, GC. However, when GC starts "shrinking" the Pool, it is may be already too late
// in a certain sense.
//
// Use this method to explicitly dereference buffers that belong to this SGL.
func (z *SGL) Reclaim() {
	for i := 0; i < len(z.sgl); i++ {
		z.sgl[i] = nil
	}
	z.sgl = z.sgl[:0]
	z.sgl, z.slab = nil, nil
	z.woff = 0xDEADBEEF
}

//
// SGL Reader - implements io.ReadWriteCloser + io.Seeker
// A given SGL can be simultaneously utilized by multiple Readers
//

func NewReader(z *SGL) *Reader { return &Reader{z, 0} }

func (r *Reader) Open() (io.ReadCloser, error) { return NewReader(r.z), nil }

func (r *Reader) Close() error { return nil }

func (r *Reader) Read(b []byte) (n int, err error) {
	n, err, r.roff = r.z.readAtOffset(b, r.roff)
	return
}

func (r *Reader) Seek(from int64, whence int) (offset int64, err error) {
	switch whence {
	case io.SeekStart:
		offset = from
	case io.SeekCurrent:
		offset = r.roff + from
	case io.SeekEnd:
		offset = r.z.woff + from
	default:
		return 0, errors.New("invalid whence")
	}
	if offset < 0 {
		return 0, errors.New("negative position")
	}
	r.roff = offset
	return
}

//
// SGL Slice Reader - implements io.ReadWriteCloser + io.Seeker within given bounds
//

func NewSliceReader(z *SGL, soff, slen int64) *SliceReader {
	return &SliceReader{z: z, roff: 0, soff: soff, slen: slen}
}

func (r *SliceReader) Close() error { return nil }

func (r *SliceReader) Read(b []byte) (n int, err error) {
	var (
		offout int64
		offin  = r.roff + r.soff
		rem    = common.MinI64(r.z.woff-offin, r.slen-r.roff)
	)
	if rem < int64(len(b)) {
		b = b[:int(rem)]
		err = io.EOF
	}

	n, _, offout = r.z.readAtOffset(b, offin)
	r.roff = offout - r.soff
	return
}

func (r *SliceReader) Seek(from int64, whence int) (offset int64, err error) {
	switch whence {
	case io.SeekStart:
		offset = from
	case io.SeekCurrent:
		offset = r.roff + from
	case io.SeekEnd:
		offset = common.MinI64(r.z.woff, r.roff+r.soff+r.slen) + from
	default:
		return 0, errors.New("invalid whence")
	}
	if offset < 0 {
		return 0, errors.New("negative position")
	}
	r.roff = offset
	return
}
