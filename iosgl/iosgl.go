/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package iosgl provides Reader and (streaming) Writer on top of a Scatter Gather List (SGL) of reusable buffers.
package iosgl

import (
	"errors"
	"io"
	"sync"
)

const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB

	largeSizeUseThresh = MiB / 2
	minSizeUnknown     = 32 * KiB
)

var fixedSizes = []int64{4 * KiB, 16 * KiB, 32 * KiB, 64 * KiB, 128 * KiB}
var allSlabs = []*Slab{nil, nil, nil, nil, nil} // Note: the length of allSlabs must equal the length of fixedSizes.

func init() {
	for i, f := range fixedSizes {
		allSlabs[i] = newSlab(f)
	}
}

type SGL struct {
	sgl  [][]byte
	slab *Slab
	woff int64 // stream
	roff int64
}

func NewSGL(oosize uint64) *SGL {
	osize := int64(oosize)
	if osize == 0 {
		osize = minSizeUnknown
	}
	slab := SelectSlab(osize)
	n := divCeil(osize, slab.Size())
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
		size := MinI64(z.slab.Size()-off, int64(wlen))
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
	size := MinI64(int64(len(b)), z.woff-roff)
	n = copy(b[:size], buf[off:])
	roff += int64(n)
	for n < len(b) && idx < len(z.sgl)-1 {
		idx++
		buf = z.sgl[idx]
		size = MinI64(int64(len(b)-n), z.woff-roff)
		n1 := copy(b[n:n+int(size)], buf)
		roff += int64(n1)
		n += n1
	}
	return
}

// reuse already allocated SGL
func (z *SGL) Reset() {
	z.woff, z.roff = 0, 0
}

func (z *SGL) Close() error {
	return nil
}

func (z *SGL) Free() {
	for i := 0; i < len(z.sgl); i++ {
		z.slab.Free(z.sgl[i])
	}
	z.sgl = nil
	z.woff = 0xDEADBEEF
}

type Reader struct {
	z    *SGL
	roff int64
}

func (r *Reader) Len() int {
	if r.roff >= r.z.Cap() {
		return 0
	}
	return int(r.z.Cap() - r.roff)
}

func (r *Reader) Close() error {
	return nil
}

func (r *Reader) Size() int64 { return r.z.Cap() }

func (r *Reader) Read(b []byte) (n int, err error) {
	n, err, r.roff = r.z.readAtOffset(b, r.roff)
	return
}

func (r *Reader) Seek(from int64, whence int) (offset int64, err error) {
	switch whence {
	case io.SeekStart:
		offset = from
	case io.SeekCurrent:
		offset = r.z.roff + from
	case io.SeekEnd:
		offset = r.z.woff + from
	default:
		return 0, errors.New("invalid whence from *Reader.Seek")
	}
	if offset < 0 {
		return 0, errors.New("negative position from *Reader.Seek")
	}
	r.roff = offset
	return
}

func (r *Reader) Open() (io.ReadCloser, error) {
	return NewReader(r.z), nil
}

func NewReader(z *SGL) *Reader { return &Reader{z, 0} }

// MinI64 returns min value of a and b for int64 types
func MinI64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func divCeil(a, b int64) int64 {
	d, r := a/b, a%b
	if r > 0 {
		return d + 1
	}
	return d
}

type Slab struct {
	pool      *sync.Pool
	fixedSize int64
}

func newSlab(fixedSize int64) *Slab {
	pool := &sync.Pool{
		New: func() interface{} {
			return make([]byte, fixedSize)
		},
	}
	return &Slab{pool, fixedSize}
}

func SelectSlab(osize int64) *Slab {
	if osize >= largeSizeUseThresh { // precondition to use the largest slab
		return allSlabs[len(allSlabs)-1]
	}
	if osize == 0 { // when the size is unknown
		return allSlabs[len(allSlabs)-2]
	}
	for i := len(allSlabs) - 2; i >= 0; i-- {
		if osize >= fixedSizes[i] {
			return allSlabs[i]
		}
	}
	return allSlabs[0]
}

func (s *Slab) Alloc() []byte {
	return s.pool.Get().([]byte)
}

func (s *Slab) Free(buf []byte) {
	s.pool.Put(buf)
}

func (s *Slab) Size() int64 {
	return s.fixedSize
}

func AllocFromSlab(desiredSize int64) ([]byte, *Slab) {
	slab := SelectSlab(desiredSize)
	return slab.Alloc(), slab
}

func FreeToSlab(buf []byte, s *Slab) {
	s.Free(buf)
}
