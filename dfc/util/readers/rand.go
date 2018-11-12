/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

package readers

import (
	"errors"
	"io"
	"math/rand"
	"time"

	"github.com/NVIDIA/dfcpub/cmn"
)

const randReaderBufferSize = 64 * 1024 // size of internal buffer filled with random data

// randReader implements io.Reader and io.WriterTo
// It use an internal buffer instead of a file as data backing.
// Implementing io.WriterTo makes it possible to speed up the data transfer
type randReader struct {
	size   int64  // size of a fake file
	offset int64  // the current reading position
	buf    []byte // internal buffer filled with random data
}

var (
	_ io.Reader   = &randReader{}
	_ io.WriterTo = &randReader{}
)

// Read implements the io.Reader interface.
func (r *randReader) Read(buf []byte) (int, error) {
	if r.offset >= r.size {
		return 0, io.EOF
	}

	// duplicate internal buffer with random data until the
	// destination buffer is full
	want := cmn.MinI64(int64(len(buf)), r.size-r.offset)
	bytesLeft := want
	written := int64(0)
	for bytesLeft > 0 {
		toWrite := cmn.MinI64(bytesLeft, randReaderBufferSize)
		copy(buf[written:], r.buf[:toWrite])
		bytesLeft -= toWrite
		written += toWrite
	}

	r.offset += want
	return int(want), nil
}

// WriteTo implements the io.WriterTo interface.
// io.Copy uses WriteTo interface to avoid extra memory allocation and
// copying data by chunks if a Reader implements io.WriterTo interface.
// It may increase throughput by a few GB
func (r *randReader) WriteTo(w io.Writer) (int64, error) {
	want := r.size - r.offset
	bytesLeft := want
	written := int64(0)
	for bytesLeft > 0 {
		toWrite := cmn.MinI64(bytesLeft, randReaderBufferSize)
		w.Write(r.buf[:toWrite])
		bytesLeft -= toWrite
		written += toWrite
	}

	r.offset += want
	return want, nil
}

// Seek implements the tutils.Reader interface.
func (r *randReader) Seek(offset int64, whence int) (int64, error) {
	var abs int64

	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.offset + offset
	case io.SeekEnd:
		abs = r.size + offset
	default:
		return 0, errors.New("RandReader.Seek: invalid whence")
	}

	if abs < 0 {
		return 0, errors.New("RandReader.Seek: negative position")
	}

	if abs >= r.size {
		r.offset = r.size
		return r.offset, nil
	}

	return abs, nil
}

// NewRandReader returns a new randReader with prefilled internal buffer
func NewRandReader(size int64) *randReader {
	rr := &randReader{
		size: size,
		buf:  make([]byte, randReaderBufferSize),
	}
	seed := time.Now().UnixNano()
	rsrc := rand.New(rand.NewSource(seed))
	rsrc.Read(rr.buf)
	return rr
}
