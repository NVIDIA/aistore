// Package readers provides implementation for common reader types
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package readers

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/memsys"
)

const (
	// ReaderTypeFile defines the name for file reader
	ReaderTypeFile = "file"
	// ReaderTypeSG defines the name for sg reader
	ReaderTypeSG = "sg"
	// ReaderTypeRand defines the name for rand reader
	ReaderTypeRand = "rand"
)

// Reader is the interface a client works with to read in data and send to a HTTP server
type Reader interface {
	io.ReadCloser
	io.Seeker
	Open() (io.ReadCloser, error)
	Cksum() *cmn.Cksum
}

// randReader implements Reader.
// It doesn't not use a file or allocated memory as data backing.
type randReader struct {
	seed   int64
	rnd    *rand.Rand
	size   int64
	offset int64
	cksum  *cmn.Cksum
}

var (
	mmsa = memsys.DefaultPageMM()

	// interface guard
	_ Reader = &randReader{}
)

// Read implements the Reader interface.
func (r *randReader) Read(buf []byte) (int, error) {
	available := r.size - r.offset
	if available == 0 {
		return 0, io.EOF
	}

	want := int64(len(buf))
	n := cmn.MinI64(want, available)
	actual, err := r.rnd.Read(buf[:n])
	if err != nil {
		return 0, nil
	}

	r.offset += int64(actual)
	return actual, nil
}

// Open implements the Reader interface.
// Returns a new rand reader using the same seed.
func (r *randReader) Open() (io.ReadCloser, error) {
	return &randReader{
		seed:  r.seed,
		rnd:   rand.New(rand.NewSource(r.seed)),
		size:  r.size,
		cksum: r.cksum,
	}, nil
}

// Close implements the Reader interface.
func (r *randReader) Close() error {
	return nil
}

// Seek implements the Reader interface.
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
		return 0, errors.New("invalid whence")
	}

	if abs < 0 {
		return 0, errors.New("negative offset position")
	}

	if abs >= r.size {
		r.offset = r.size
		return r.offset, nil
	}

	r.rnd = rand.New(rand.NewSource(r.seed))
	r.offset = 0
	actual, err := io.CopyN(ioutil.Discard, r, abs)
	if err != nil {
		return 0, err
	}

	if actual != abs {
		err := fmt.Errorf("failed to seek to %d, seeked to %d instead", offset, actual)
		return 0, err
	}

	return abs, nil
}

// XXHash implements the Reader interface.
func (r *randReader) Cksum() *cmn.Cksum {
	return r.cksum
}

// NewRandReader returns a new randReader
func NewRandReader(size int64, cksumType string) (Reader, error) {
	var (
		cksum *cmn.Cksum
		err   error
		seed  = mono.NanoTime()
	)
	slab, err := mmsa.GetSlab(cmn.KiB * 32)
	if err != nil {
		return nil, err
	}
	buf := slab.Alloc()
	defer slab.Free(buf)
	rand1 := rand.New(rand.NewSource(seed))
	rr := &rrLimited{rand1, size, 0}
	if cksumType != cmn.ChecksumNone {
		_, cksumHash, err := cmn.CopyAndChecksum(ioutil.Discard, rr, buf, cksumType)
		if err != nil {
			return nil, err
		}
		cksum = cksumHash.Clone()
	}
	rand1dup := rand.New(rand.NewSource(seed))
	return &randReader{
		seed:  seed,
		rnd:   rand1dup,
		size:  size,
		cksum: cksum,
	}, nil
}

type rrLimited struct {
	random *rand.Rand
	size   int64
	off    int64
}

func (rr *rrLimited) Read(p []byte) (n int, err error) {
	rem := int(cmn.MinI64(rr.size-rr.off, int64(len(p))))
	n, _ = rr.random.Read(p[:rem]) // never fails
	rr.off += int64(n)
	if rem < len(p) {
		err = io.EOF
	}
	return
}

type fileReader struct {
	*os.File
	fullName string // Example: "/dir/ais/smoke/bGzhWKWoxHDSePnELftx"
	name     string // Example: smoke/bGzhWKWoxHDSePnELftx
	cksum    *cmn.Cksum
}

var _ Reader = &fileReader{}

// Open implements the Reader interface.
func (r *fileReader) Open() (io.ReadCloser, error) {
	return os.Open(r.fullName)
}

// XXHash implements the Reader interface.
func (r *fileReader) Cksum() *cmn.Cksum {
	return r.cksum
}

// NewFileReader creates/opens the file, populates it with random data, and returns a new fileReader
// Caller is responsible for closing
func NewFileReader(filepath, name string, size int64, cksumType string) (Reader, error) {
	var (
		cksum  *cmn.Cksum
		fn     = path.Join(filepath, name)
		f, err = os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0666) // wr-wr-wr-
	)
	if err != nil {
		return nil, err
	}

	cksumHash, err := copyRandWithHash(f, size, cksumType, cmn.NowRand())
	if err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	if cksumType != cmn.ChecksumNone {
		cksum = cksumHash.Clone()
	}
	// FIXME: No need to have 'f' in fileReader?
	return &fileReader{f, fn, name, cksum}, nil
}

// NewFileReaderFromFile opens an existing file, reads it to compute checksum,
// and returns a new reader.
// See also (and note the difference from): NewFileReader
// Caller responsible for closing
func NewFileReaderFromFile(fn, cksumType string) (Reader, error) {
	var (
		cksum  *cmn.Cksum
		f, err = os.Open(fn)
	)
	if err != nil {
		return nil, err
	}
	if cksumType != cmn.ChecksumNone {
		buf, slab := mmsa.Alloc()
		_, cksumHash, err := cmn.CopyAndChecksum(ioutil.Discard, f, buf, cksumType)
		if err != nil {
			return nil, err
		}
		slab.Free(buf)
		cksum = cksumHash.Clone()
	}

	return &fileReader{f, fn, "" /* ais prefix */, cksum}, nil
}

type sgReader struct {
	memsys.Reader
	cksum *cmn.Cksum
}

var _ Reader = &sgReader{}

// XXHash implements the Reader interface.
func (r *sgReader) Cksum() *cmn.Cksum {
	return r.cksum
}

// NewSGReader returns a new sgReader
func NewSGReader(sgl *memsys.SGL, size int64, cksumType string) (Reader, error) {
	var cksum *cmn.Cksum
	if size > 0 {
		cksumHash, err := copyRandWithHash(sgl, size, cksumType, cmn.NowRand())
		if err != nil {
			return nil, err
		}
		if cksumType != cmn.ChecksumNone {
			cksum = cksumHash.Clone()
		}
	}

	r := memsys.NewReader(sgl)
	return &sgReader{*r, cksum}, nil
}

type bytesReader struct {
	*bytes.Buffer
	buf []byte
}

// Open implements the Reader interface.
func (r *bytesReader) Open() (io.ReadCloser, error) {
	return &bytesReader{bytes.NewBuffer(r.buf), r.buf}, nil
}

// Close implements the Reader interface.
func (r *bytesReader) Close() error {
	return nil
}

func (r *bytesReader) Cksum() *cmn.Cksum {
	return nil // niy
}

func (r *bytesReader) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

// NewBytesReader returns a new bytesReader
func NewBytesReader(buf []byte) Reader {
	return &bytesReader{bytes.NewBuffer(buf), buf}
}

// ParamReader is used to pass in parameters when creating a new reader
type ParamReader struct {
	Type       string      // file | sg | inmem | rand
	SGL        *memsys.SGL // When Type == sg
	Path, Name string      // When Type == file; path and name of file to be created (if not already existing)
	Size       int64
}

// NewReader returns a data reader; type of reader returned is based on the parameters provided
func NewReader(p ParamReader, cksumType string) (Reader, error) {
	switch p.Type {
	case ReaderTypeSG:
		if p.SGL == nil {
			return nil, fmt.Errorf("SGL is empty while reader type is SGL")
		}
		return NewSGReader(p.SGL, p.Size, cksumType)
	case ReaderTypeRand:
		return NewRandReader(p.Size, cksumType)
	case ReaderTypeFile:
		return NewFileReader(p.Path, p.Name, p.Size, cksumType)
	default:
		return nil, fmt.Errorf("unknown memory type for creating inmem reader")
	}
}

// copyRandWithHash reads data from random source and writes it to a writer while
// optionally computing xxhash
// See related: memsys_test.copyRand
func copyRandWithHash(w io.Writer, size int64, cksumType string, rnd *rand.Rand) (*cmn.CksumHash, error) {
	var (
		cksum   *cmn.CksumHash
		rem     = size
		buf, s  = mmsa.Alloc()
		blkSize = int64(len(buf))
	)
	defer s.Free(buf)

	if cksumType != cmn.ChecksumNone {
		cksum = cmn.NewCksumHash(cksumType)
	}
	for i := int64(0); i <= size/blkSize; i++ {
		n := int(cmn.MinI64(blkSize, rem))
		rnd.Read(buf[:n])
		m, err := w.Write(buf[:n])
		if err != nil {
			return nil, err
		}
		if cksumType != cmn.ChecksumNone {
			cksum.H.Write(buf[:m])
		}
		cmn.Assert(m == n)
		rem -= int64(m)
	}
	if cksumType != cmn.ChecksumNone {
		cksum.Finalize()
	}
	return cksum, nil
}
