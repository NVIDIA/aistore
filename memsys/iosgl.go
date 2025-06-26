// Package memsys provides memory management and slab/SGL allocation with io.Reader and io.Writer interfaces
// on top of scatter-gather lists of reusable buffers.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package memsys

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

// interface guard
var (
	_ io.ByteScanner = (*SGL)(nil)

	_ io.ReaderFrom = (*SGL)(nil)
	_ cos.WriterAt  = (*SGL)(nil)
	_ io.WriterTo   = (*SGL)(nil)
	_ cos.WriterTo2 = (*SGL)(nil) // simplified io.WriteTo to write entire (0 -:- woff) content

	_ cos.ReadOpenCloser = (*SGL)(nil)
	_ cos.ReadOpenCloser = (*Reader)(nil)
)

type (
	// implements io.ReadWriteCloser + Reset
	SGL struct {
		slab *Slab
		sgl  [][]byte
		woff int64
		roff int64
	}
	// uses the underlying SGL to implement io.ReadWriteCloser + io.Seeker
	Reader struct {
		z    *SGL
		roff int64
	}
)

/////////////
// sglPool //
/////////////

const numPools = 8

var (
	pgPools      [numPools]sync.Pool
	smPools      [numPools]sync.Pool
	sgl0         SGL
	pgIdx, smIdx atomic.Uint32
)

func _allocSGL(isPage bool) (z *SGL) {
	var pool *sync.Pool
	if isPage {
		idx := (pgIdx.Load() + 1) % numPools
		pool = &pgPools[idx]
	} else {
		idx := (smIdx.Load() + 1) % numPools
		pool = &smPools[idx]
	}
	if v := pool.Get(); v != nil {
		z = v.(*SGL)
	} else {
		z = &SGL{}
	}
	return
}

func _freeSGL(z *SGL, isPage bool) {
	var pool *sync.Pool
	if isPage {
		idx := pgIdx.Inc() % numPools
		pool = &pgPools[idx]
	} else {
		idx := smIdx.Inc() % numPools
		pool = &smPools[idx]
	}
	sgl := z.sgl[:0]
	*z = sgl0
	z.sgl = sgl
	pool.Put(z)
}

/////////
// SGL //
/////////

func (z *SGL) Cap() int64  { return int64(len(z.sgl)) * z.slab.Size() }
func (z *SGL) Size() int64 { return z.woff }
func (z *SGL) Roff() int64 { return z.roff }
func (z *SGL) Slab() *Slab { return z.slab }
func (z *SGL) IsNil() bool { return z == nil || z.slab == nil }

// grows on demand upon writing
func (z *SGL) grow(toSize int64) {
	z.slab.muget.Lock()
	for z.Cap() < toSize {
		z.sgl = append(z.sgl, z.slab._alloc())
	}
	z.slab.muget.Unlock()
}

// usage via io.Copy(z, source), whereby `z` reads from the `source` until EOF
// see also: WriteTo
func (z *SGL) ReadFrom(r io.Reader) (n int64, _ error) {
	for {
		if c := z.Cap(); z.woff > c-128 {
			z.grow(c + max(z.slab.Size(), DefaultBufSize))
		}
		idx := z.woff / z.slab.Size()
		off := z.woff % z.slab.Size()
		buf := z.sgl[idx]

		written, err := r.Read(buf[off:])
		z.woff += int64(written)
		n += int64(written)
		if err != nil {
			if err == io.EOF {
				return n, nil
			}
			return n, err
		}
	}
}

// simplified for speed
// - disregards roff, usage is strictly limited to writing an _entire_ sgl
// - compare w/ WriteTo below
func (z *SGL) WriteTo2(dst io.Writer) error {
	rem := z.woff
	siz := z.slab.Size()
	for _, buf := range z.sgl {
		l := min(rem, siz)
		if l <= 0 {
			break
		}
		written, err := dst.Write(buf[:l])
		rem -= l
		if err != nil {
			if cmn.Rom.FastV(5, cos.SmoduleMemsys) {
				nlog.Errorln(err)
			}
			return err
		}
		debug.Assert(written == int(l), written, " vs ", l)
	}
	return nil
}

// compliant io.WriterTo interface impl-n (compare w/ WriteTo2)
// usage via io.Copy(dst, z), whereby `z` writes to the `dst` until EOF
// see also: ReadFrom
func (z *SGL) WriteTo(dst io.Writer) (n int64, _ error) {
	var (
		idx = int(z.roff / z.slab.Size())
		off = z.roff % z.slab.Size()
	)
	for {
		rem := z.Len()
		if rem <= 0 {
			break
		}
		buf := z.sgl[idx]
		siz := min(z.slab.Size()-off, rem)
		written, err := dst.Write(buf[off : off+siz])
		m := int64(written)
		n += m
		z.roff += m
		if m < siz && err == nil {
			err = io.ErrShortWrite
		}
		if err != nil {
			if cmn.Rom.FastV(5, cos.SmoduleMemsys) {
				nlog.Errorln(err)
			}
			return n, err
		}
		debug.Assert(m == siz, m, " vs ", siz) // (unlikely m > siz)
		idx++
		off = 0
	}
	return n, nil
}

func (z *SGL) Write(p []byte) (n int, err error) {
	wlen := len(p)
	if needtot := z.woff + int64(wlen); needtot > z.Cap() {
		z.grow(needtot)
	}
	idx, off, poff := z.woff/z.slab.Size(), z.woff%z.slab.Size(), 0
	for wlen > 0 {
		size := min(z.slab.Size()-off, int64(wlen))
		buf := z.sgl[idx]
		src := p[poff : poff+int(size)]
		copy(buf[off:], src)
		z.woff += size
		idx++
		off = 0
		wlen -= int(size)
		poff += int(size)
	}
	return len(p), nil
}

func (z *SGL) WriteByte(c byte) error {
	if needtot := z.woff + 1; needtot > z.Cap() {
		z.grow(needtot)
	}
	idx, off := z.woff/z.slab.Size(), z.woff%z.slab.Size()
	buf := z.sgl[idx]
	buf[off] = c
	z.woff++
	return nil
}

func (z *SGL) Read(b []byte) (n int, err error) {
	n, z.roff, err = z._readAt(b, z.roff)
	return
}

func (z *SGL) ReadByte() (byte, error) {
	var (
		b           [1]byte
		_, off, err = z._readAt(b[:], z.roff)
	)
	z.roff = off
	return b[0], err
}

func (z *SGL) UnreadByte() error {
	if z.roff == 0 {
		return errors.New("memsys: cannot unread-byte at zero offset")
	}
	z.roff--
	return nil
}

func (z *SGL) _readAt(b []byte, roffin int64) (n int, roff int64, err error) {
	roff = roffin
	if roff >= z.woff {
		return 0, roff, io.EOF
	}
	var (
		idx, off = int(roff / z.slab.Size()), roff % z.slab.Size()
		buf      = z.sgl[idx]
		size     = min(int64(len(b)), z.woff-roff)
	)
	n = copy(b[:size], buf[off:])
	roff += int64(n)
	for n < len(b) && idx < len(z.sgl)-1 {
		idx++
		buf = z.sgl[idx]
		size = min(int64(len(b)-n), z.woff-roff)
		n1 := copy(b[n:n+int(size)], buf)
		roff += int64(n1)
		n += n1
	}
	if n < len(b) {
		err = io.EOF
	}
	return n, roff, err
}

// ReadAll is a strictly _convenience_ method as it performs heap allocation.
// Still, it's an optimized alternative to the generic cos.ReadAll which
// normally returns err == nil (and not io.EOF) upon successful reading until EOF.
// ReadAll always returns err == nil.
func (z *SGL) ReadAll() (b []byte) {
	b = make([]byte, z.Size())
	for off, i := 0, 0; i < len(z.sgl); i++ {
		n := copy(b[off:], z.sgl[i])
		off += n
	}
	return
}

func (z *SGL) ErrDumpAt() {
	if z.roff >= z.woff {
		return
	}
	var (
		idx, off = int(z.roff / z.slab.Size()), int(z.roff % z.slab.Size())
		buf      = z.sgl[idx]
		part     = buf[off:]
	)
	s := fmt.Sprintf("at %2d: %s", idx, cos.BHead(part, 128))
	nlog.ErrorDepth(1, s)
	if idx < len(z.sgl)-1 {
		buf = z.sgl[idx+1]
		part = buf[0:]
		s = fmt.Sprintf("at %2d: %s", idx+1, cos.BHead(part, 128))
		nlog.ErrorDepth(1, s)
	}
}

func (z *SGL) NextLine(lin []byte, advanceRoff bool) (lout []byte, err error) {
	if z.roff >= z.woff {
		return nil, io.EOF
	}

	var (
		l        int
		part     []byte
		idx, off = int(z.roff / z.slab.Size()), int(z.roff % z.slab.Size())
		buf      = z.sgl[idx]
		i        = bytes.IndexRune(buf[off:], '\n')
	)
	if i <= 0 {
		part = buf[off:]
		l = len(part)
		// when line's spliced across two bufs
		if idx >= len(z.sgl)-1 {
			return nil, errors.New("sgl last buf with a partial line: " + cos.BHead(part, 128))
		}
		buf = z.sgl[idx+1]
		off = 0
		i = bytes.IndexRune(buf, '\n')
		if i < 0 {
			return nil, fmt.Errorf("missing eol: %q, %q", cos.BHead(part, 128), cos.BHead(buf))
		}
	}
	if cap(lin) < i+l {
		debug.Assert(lin == nil, "check initial line buf cap: ", cap(lin), " vs ", i+l)
		lout = make([]byte, i+l)
	} else {
		lout = lin[:i+l]
	}

	// copy the `part`
	copy(lout, part)
	copy(lout[l:], buf[off:off+i])

	if advanceRoff {
		// i.e., read it
		z.roff += int64(i + l + 1)
	}
	return lout, nil
}

// NOTE assert and use with caution.
func (z *SGL) WriteAt(p []byte, off int64) (n int, err error) {
	debug.Assert(z.woff >= off+int64(len(p)))

	prevWriteOff := z.woff
	z.woff = off
	n, err = z.Write(p)
	z.woff = prevWriteOff
	return n, err
}

// reuse already allocated SGL (compare with Reader below)
func (z *SGL) Reset()  { z.woff, z.roff = 0, 0 }
func (z *SGL) Rewind() { z.roff = 0 }

func (z *SGL) Len() int64                        { return z.woff - z.roff }
func (z *SGL) Open() (cos.ReadOpenCloser, error) { return NewReader(z), nil }

func (*SGL) Close() error { return nil } // NOTE: no-op

func (z *SGL) Free() {
	debug.Assert(z.slab != nil)
	s := z.slab
	s.muput.Lock()
	for _, buf := range z.sgl {
		size := cap(buf)
		debug.Assert(int64(size) == s.Size())
		b := buf[:size] // always freeing original (fixed buffer) size
		debug.DeadBeefLarge(b)
		s.put = append(s.put, b)
	}
	s.muput.Unlock()
	_freeSGL(z, z.slab.m.isPage())
}

// NOTE assert and use with caution: heap allocation (via ReadAll)
// is intended for tests (and only tests)
func (z *SGL) Bytes() (b []byte) {
	cos.Assert(z.roff == 0)
	if z.woff >= z.slab.Size() {
		b = z.ReadAll()
		return
	}
	return z.sgl[0][:z.woff]
}

////////////
// Reader //
////////////

// Reader implements (io.ReadWriteCloser + io.Seeker) on top of an existing SGL.
// In the most common write-once-read-many usage scenario, SGL can be simultaneously
// read via multiple concurrent Readers.
//
// See related sgl methods: `Reset` and `Open`

func NewReader(z *SGL) *Reader                      { return &Reader{z, 0} }
func (r *Reader) Open() (cos.ReadOpenCloser, error) { return NewReader(r.z), nil }
func (*Reader) Close() error                        { return nil }

func (r *Reader) Read(b []byte) (n int, err error) {
	n, r.roff, err = r.z._readAt(b, r.roff)
	return n, err
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
		return 0, errors.New("memsys: invalid whence")
	}
	if offset < 0 {
		return 0, errors.New("memsys: negative position")
	}
	r.roff = offset
	return
}
