// Package readers provides implementation for common reader types
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package readers

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/ext/dsort/shard"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tools/tarch"
)

const (
	// TypeFile defines the name for file reader
	TypeFile = "file"
	// TypeSG defines the name for sg reader
	TypeSG = "sg"
	// TypeRand defines the name for rand reader
	TypeRand = "rand"
	// TypeTar defines the name for random TAR reader
	TypeTar = "tar"
)

type (
	Reader interface {
		cos.ReadOpenCloser
		io.Seeker
		Cksum() *cos.Cksum
	}
	randReader struct {
		seed   int64
		rnd    *seededReader
		size   int64
		offset int64
		cksum  *cos.Cksum
	}
	tarReader struct {
		b []byte
		bytes.Reader
		cksum *cos.Cksum
	}
	rrLimited struct {
		random *seededReader
		size   int64
		off    int64
	}
	fileReader struct {
		*os.File
		filePath string // Example: "/dir/ais/"
		name     string // Example: "smoke/bGzhWKWoxHDSePnELftx"
		cksum    *cos.Cksum
	}
	sgReader struct {
		memsys.Reader
		cksum *cos.Cksum
	}
	bytesReader struct {
		*bytes.Buffer
		buf []byte
	}

	// (aisloader only)
	Params struct {
		Type       string      // file | sg | inmem | rand
		SGL        *memsys.SGL // When Type == sg
		Path, Name string      // When Type == file; path and name of file to be created (if not already existing)
		Size       int64
	}
)

// interface guard
var (
	_ Reader = (*randReader)(nil)
	_ Reader = (*tarReader)(nil)
	_ Reader = (*fileReader)(nil)
	_ Reader = (*sgReader)(nil)
)

////////////////
// randReader //
////////////////

func NewRand(size int64, cksumType string) (Reader, error) {
	var (
		cksum *cos.Cksum
		seed  = mono.NanoTime()
	)
	rand1 := newSeededReader(uint64(seed))
	if cksumType != cos.ChecksumNone {
		rr := &rrLimited{rand1, size, 0}
		_, cksumHash, err := cos.CopyAndChecksum(io.Discard, rr, nil, cksumType)
		if err != nil {
			return nil, err
		}
		cksum = cksumHash.Clone()
	}
	rand1dup := newSeededReader(uint64(seed))
	return &randReader{
		seed:  seed,
		rnd:   rand1dup,
		size:  size,
		cksum: cksum,
	}, nil
}

func (r *randReader) Read(buf []byte) (int, error) {
	available := r.size - r.offset
	if available == 0 {
		return 0, io.EOF
	}

	want := int64(len(buf))
	n := min(want, available)
	actual, err := r.rnd.Read(buf[:n])
	if err != nil {
		return 0, nil
	}

	r.offset += int64(actual)
	return actual, nil
}

// Open implements the Reader interface.
// Returns a new rand reader using the same seed.
func (r *randReader) Open() (cos.ReadOpenCloser, error) {
	return &randReader{
		seed:  r.seed,
		rnd:   newSeededReader(uint64(r.seed)),
		size:  r.size,
		cksum: r.cksum,
	}, nil
}

// Close implements the Reader interface.
func (*randReader) Close() error { return nil }

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

	r.rnd = newSeededReader(uint64(r.seed))
	r.offset = 0
	actual, err := io.CopyN(io.Discard, r, abs)
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
func (r *randReader) Cksum() *cos.Cksum {
	return r.cksum
}

func (rr *rrLimited) Read(p []byte) (n int, err error) {
	rem := int(min(rr.size-rr.off, int64(len(p))))
	n, _ = rr.random.Read(p[:rem]) // never fails
	rr.off += int64(n)
	if rem < len(p) {
		err = io.EOF
	}
	return
}

////////////////
// fileReader //
////////////////

// creates/opens the file, populates it with random data, and returns a new fileReader
// NOTE: Caller is responsible for closing.
func NewRandFile(filepath, name string, size int64, cksumType string) (Reader, error) {
	var (
		cksum     *cos.Cksum
		cksumHash *cos.CksumHash
		fn        = path.Join(filepath, name)
		f, err    = os.OpenFile(fn, os.O_RDWR|os.O_CREATE, cos.PermRWR)
		exists    bool
	)
	if err != nil {
		return nil, err
	}
	if size == -1 {
		// checksum existing file
		exists = true
		if cksumType != cos.ChecksumNone {
			debug.Assert(cksumType != "")
			_, cksumHash, err = cos.CopyAndChecksum(io.Discard, f, nil, cksumType)
		}
	} else {
		// Write random file
		cksumHash, err = copyRandWithHash(f, size, cksumType)
	}
	if err == nil {
		_, err = f.Seek(0, io.SeekStart)
	}

	if err != nil {
		// cleanup and ret
		f.Close()
		if !exists {
			os.Remove(fn)
		}
		return nil, err
	}

	if cksumType != cos.ChecksumNone {
		cksum = cksumHash.Clone()
	}
	return &fileReader{f, filepath, name, cksum}, nil
}

// NewExistingFile opens an existing file, reads it to compute checksum, and returns a new reader.
// NOTE: Caller responsible for closing.
func NewExistingFile(fn, cksumType string) (Reader, error) {
	return NewRandFile(fn, "", -1, cksumType)
}

func (r *fileReader) Open() (cos.ReadOpenCloser, error) {
	cksumType := cos.ChecksumNone
	if r.cksum != nil {
		cksumType = r.cksum.Type()
	}
	return NewRandFile(r.filePath, r.name, -1, cksumType)
}

// XXHash implements the Reader interface.
func (r *fileReader) Cksum() *cos.Cksum {
	return r.cksum
}

//////////////
// sgReader //
//////////////

func NewSG(sgl *memsys.SGL, size int64, cksumType string) (Reader, error) {
	var cksum *cos.Cksum
	if size > 0 {
		cksumHash, err := copyRandWithHash(sgl, size, cksumType)
		if err != nil {
			return nil, err
		}
		if cksumType != cos.ChecksumNone {
			cksum = cksumHash.Clone()
		}
	}

	r := memsys.NewReader(sgl)
	return &sgReader{*r, cksum}, nil
}

func (r *sgReader) Cksum() *cos.Cksum {
	return r.cksum
}

/////////////////
// bytesReader //
/////////////////

func NewBytes(buf []byte) Reader                    { return &bytesReader{bytes.NewBuffer(buf), buf} }
func (*bytesReader) Close() error                   { return nil }
func (*bytesReader) Cksum() *cos.Cksum              { return nil }
func (*bytesReader) Seek(int64, int) (int64, error) { return 0, nil }

func (r *bytesReader) Open() (cos.ReadOpenCloser, error) {
	return &bytesReader{bytes.NewBuffer(r.buf), r.buf}, nil
}

///////////////
// tarReader //
///////////////

func newTarReader(size int64, cksumType string) (r Reader, err error) {
	var (
		singleFileSize = min(size, int64(cos.KiB))
		buff           = bytes.NewBuffer(nil)
	)
	err = tarch.CreateArchCustomFilesToW(buff, tar.FormatUnknown, archive.ExtTar, max(int(size/singleFileSize), 1),
		int(singleFileSize), shard.ContentKeyInt, ".cls", true)
	if err != nil {
		return nil, err
	}
	cksum, err := cos.ChecksumBytes(buff.Bytes(), cksumType)
	if err != nil {
		return nil, err
	}
	return &tarReader{
		b:      buff.Bytes(),
		Reader: *bytes.NewReader(buff.Bytes()),
		cksum:  cksum,
	}, err
}

func (*tarReader) Close() error        { return nil }
func (r *tarReader) Cksum() *cos.Cksum { return r.cksum }

func (r *tarReader) Open() (cos.ReadOpenCloser, error) {
	return &tarReader{
		Reader: *bytes.NewReader(r.b),
		cksum:  r.cksum,
		b:      r.b,
	}, nil
}

//
// for convenience
//

func New(p Params, cksumType string) (Reader, error) {
	switch p.Type {
	case TypeSG:
		debug.Assert(p.SGL != nil)
		return NewSG(p.SGL, p.Size, cksumType)
	case TypeRand:
		return NewRand(p.Size, cksumType)
	case TypeFile:
		return NewRandFile(p.Path, p.Name, p.Size, cksumType)
	case TypeTar:
		return newTarReader(p.Size, cksumType)
	default:
		return nil, errors.New("unknown memory type for creating inmem reader")
	}
}

// copyRandWithHash reads data from random source and writes it to a writer while
// optionally computing xxhash
// See related: memsys_test.copyRand
func copyRandWithHash(w io.Writer, size int64, cksumType string) (*cos.CksumHash, error) {
	var (
		cksum   *cos.CksumHash
		rem     = size
		buf, s  = memsys.PageMM().Alloc()
		blkSize = int64(len(buf))
		seed    = uint64(mono.NanoTime())
	)
	defer s.Free(buf)

	if cksumType != cos.ChecksumNone {
		cksum = cos.NewCksumHash(cksumType)
	}
	for i := int64(0); i <= size/blkSize; i++ {
		n := int(min(blkSize, rem))
		// Fill buffer with deterministic random data (faster than crypto/rand)
		for j := 0; j <= len(buf)-cos.SizeofI64; j += cos.SizeofI64 {
			binary.BigEndian.PutUint64(buf[j:], seed+uint64(j))
		}
		m, err := w.Write(buf[:n])
		if err != nil {
			return nil, err
		}
		if cksumType != cos.ChecksumNone {
			cksum.H.Write(buf[:m])
		}
		debug.Assert(m == n)
		rem -= int64(m)
	}
	if cksumType != cos.ChecksumNone {
		cksum.Finalize()
	}
	return cksum, nil
}
