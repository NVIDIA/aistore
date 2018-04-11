// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"errors"
	"io"
	"sync"
)

const (
	largeSizeUseThresh = MiB / 2
	warnSizeThresh     = GiB
	minSizeUnknown     = 32 * KiB
)

var fixedsizes = []int64{4 * KiB, 16 * KiB, 32 * KiB, 64 * KiB, 128 * KiB}
var allslabs = []slabif{nil, nil, nil, nil, nil} // NOTE: len() must be equal len(fixedsizes)

//======================================================================
//
// simple slab allocator
//
//======================================================================
type slabif interface {
	alloc() []byte
	free(buf []byte)
	getsize() int64
}

type slab struct {
	pool      *sync.Pool
	fixedsize int64
}

func init() {
	for i, fixedsize := range fixedsizes {
		allslabs[i] = newslab(fixedsize)
	}
}

func newslab(fixedsize int64) *slab {
	pool := &sync.Pool{
		New: func() interface{} {
			return make([]byte, fixedsize)
		},
	}
	return &slab{pool, fixedsize}
}

func selectslab(osize int64) slabif {
	if osize >= largeSizeUseThresh { // precondition to use the largest slab
		return allslabs[len(allslabs)-1]
	}
	if osize == 0 { // when the size is unknown
		return allslabs[len(allslabs)-2]
	}
	for i := len(allslabs) - 2; i >= 0; i-- {
		if osize >= fixedsizes[i] {
			return allslabs[i]
		}
	}
	return allslabs[0]
}

func (slab *slab) alloc() []byte {
	return slab.pool.Get().([]byte)
}

func (slab *slab) free(buf []byte) {
	slab.pool.Put(buf)
}

func (slab *slab) getsize() int64 {
	return slab.fixedsize
}

//===========
//
// client API
//
//===========
func AllocFromSlab(desiredsize int64) ([]byte, interface{}) {
	slabif := selectslab(desiredsize)
	return slabif.alloc(), slabif
}

func FreeToSlab(buf []byte, handle interface{}) {
	slabif := handle.(slabif)
	slabif.free(buf)
}

//======================================================================
//
// Reader and (streaming) Writer on top of a Scatter Gather List (SGL) of reusable buffers
//
//======================================================================
type SGLIO struct {
	sgl  [][]byte
	slab slabif
	woff int64 // stream
	roff int64
}

func NewSGLIO(oosize uint64) *SGLIO {
	osize := int64(oosize)
	if osize == 0 {
		osize = minSizeUnknown
	}
	slab := selectslab(osize)
	n := divCeil(osize, slab.getsize())
	sgl := make([][]byte, n)
	for i := 0; i < int(n); i++ {
		sgl[i] = slab.alloc()
	}
	return &SGLIO{sgl: sgl, slab: slab}
}

func (z *SGLIO) Cap() int64  { return int64(len(z.sgl)) * z.slab.getsize() }
func (z *SGLIO) Size() int64 { return z.woff }

func (z *SGLIO) grow(tosize int64) {
	for z.Cap() < tosize {
		l := len(z.sgl)
		z.sgl = append(z.sgl, nil)
		z.sgl[l] = z.slab.alloc() // FIXME: OOM
	}
}

func (z *SGLIO) Write(p []byte) (n int, err error) {
	wlen := len(p)
	needtot := z.woff + int64(wlen)
	if needtot > z.Cap() {
		z.grow(needtot)
	}
	idx, off, poff := z.woff/z.slab.getsize(), z.woff%z.slab.getsize(), 0
	for wlen > 0 {
		size := min64(z.slab.getsize()-off, int64(wlen))
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
	n, err, z.roff = z.readAtOffset(b, z.roff)
	return
}

func (z *SGLIO) readAtOffset(b []byte, roffin int64) (n int, err error, roff int64) {
	roff = roffin
	if roff >= z.woff {
		err = io.EOF
		return
	}
	idx, off := int(roff/z.slab.getsize()), roff%z.slab.getsize()
	buf := z.sgl[idx]
	size := min64(int64(len(b)), z.woff-roff)
	n = copy(b[:size], buf[off:])
	roff += int64(n)
	for n < len(b) && idx < len(z.sgl)-1 {
		idx++
		buf = z.sgl[idx]
		size = min64(int64(len(b)-n), z.woff-roff)
		n1 := copy(b[n:n+int(size)], buf)
		roff += int64(n1)
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
		z.slab.free(z.sgl[i])
	}
	z.sgl = nil
	z.woff = 0xDEADBEEF
}

//========================================================================
//
// SGL Reader
//
//========================================================================
type Reader struct {
	z    *SGLIO
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
		return 0, errors.New("Seek: invalid whence")
	}
	if offset < 0 {
		return 0, errors.New("Seek: negative position")
	}
	r.roff = offset
	return
}

func (r *Reader) Open() (io.ReadCloser, error) {
	return NewReader(r.z), nil
}

func NewReader(z *SGLIO) *Reader { return &Reader{z, 0} }
