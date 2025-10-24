// Package readers provides implementation for common reader types
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package readers

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
)

const (
	File = "file" // file reader
	SG   = "sg"   // sgl-based reader
	Rand = "rand" // random reader
)

const ExistingFileSize = -1 // reader type "file" only

// readers: interface and concrete types
type (
	Reader interface {
		cos.ReadOpenCloser
		io.Seeker
		Reset()
		Cksum() *cos.Cksum
	}
	randReader struct {
		truffle *truffleReader
		size    int64
		offset  int64
		cksum   *cos.Cksum
	}
	rrLimited struct {
		random *truffleReader
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
		sgl   *memsys.SGL
	}
	bytesReader struct {
		*bytes.Reader
		buf []byte
	}
)

// construction params
type (
	Arg struct {
		SGL        *memsys.SGL // when Type == "sg"
		Arch       *Arch       // when the content is archive
		Type       string      // "file" | "sg" | "inmem" | "rand"
		Path, Name string      // when Type == "file"; path and name of file to be created if doesn't exist
		CksumType  string
		Size       int64
	}
)

// interface guard
var (
	_ Reader = (*randReader)(nil)
	_ Reader = (*fileReader)(nil)
	_ Reader = (*sgReader)(nil)
)

func New(a *Arg) (Reader, error) {
	if err := a.validate(); err != nil {
		return nil, err
	}
	switch a.Type {
	case SG:
		debug.Assert(a.SGL != nil)
		return newSG(a)
	case Rand:
		return newRand(a)
	case File:
		return newRandFile(a)
	default:
		err := fmt.Errorf("unknown reader type %q", a.Type)
		debug.AssertNoErr(err)
		return nil, err
	}
}

func (a *Arg) validate() error {
	// size
	switch {
	case a.Size > 0:
		return nil
	case a.Size < 0 && a.Size != ExistingFileSize:
		return fmt.Errorf("readers.Arg.Size must be either -1 (indicating existing file) or non-negative (got %d)", a.Size)
	case a.Size == ExistingFileSize && a.Type != File:
		return fmt.Errorf("readers.Arg.Size = -1 (indicating existing file) requires file reader (got %q)", a.Type)
	case a.Size == 0 && a.Arch == nil:
		return errors.New("readers.Arg.Size must be positive for non-archive content (got 0)")
	}
	return nil
}

////////////////
// randReader //
////////////////

func newRand(a *Arg) (Reader, error) {
	// randReader is intentionally lightweight - keeping it this way
	if a.Arch != nil {
		return nil, fmt.Errorf("reader %q does not support archival content; use %q or %q", Rand, SG, File)
	}

	truffle := newTruffle()

	var cksum *cos.Cksum
	if a.CksumType != cos.ChecksumNone {
		rr := &rrLimited{truffle, a.Size, 0}
		_, cksumHash, err := cos.CopyAndChecksum(io.Discard, rr, nil, a.CksumType)
		if err != nil {
			return nil, err
		}
		cksum = cksumHash.Clone()
		truffle.setPos(0)
	}

	return &randReader{
		truffle: truffle,
		size:    a.Size,
		cksum:   cksum,
	}, nil
}

func (r *randReader) Read(buf []byte) (int, error) {
	available := r.size - r.offset
	if available == 0 {
		return 0, io.EOF
	}

	want := int64(len(buf))
	n := min(want, available)
	actual, err := r.truffle.Read(buf[:n])
	if err != nil {
		return 0, err
	}

	r.offset += int64(actual)
	return actual, nil
}

func (r *randReader) Open() (cos.ReadOpenCloser, error) {
	return &randReader{
		truffle: r.truffle.Open(),
		size:    r.size,
		cksum:   r.cksum,
	}, nil
}

func (*randReader) Close() error { return nil }

func (r *randReader) Reset() { r.truffle.setPos(0) }

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

	r.truffle.setPos(uint64(abs))
	r.offset = abs
	return abs, nil
}

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
// NOTE: caller is responsible for closing.
func newRandFile(a *Arg) (Reader, error) {
	var (
		cksum     *cos.Cksum
		cksumHash *cos.CksumHash
		fn        = path.Join(a.Path, a.Name)
		exists    bool
	)

	f, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE, cos.PermRWR)
	if err != nil {
		return nil, err
	}

	switch {
	case a.Size == ExistingFileSize:
		// checksum existing file
		exists = true
		if a.CksumType != cos.ChecksumNone {
			debug.Assert(a.CksumType != "")
			_, cksumHash, err = cos.CopyAndChecksum(io.Discard, f, nil, a.CksumType)
		}
	case a.Arch != nil:
		if err := a.Arch.Init(a.Size); err != nil {
			return nil, err
		}

		// Write archive shard into file
		cksumHash, err = a.Arch.write(f, a.CksumType)
	default:
		// Write plain random bytes
		cksumHash, err = writeTruffleWithHash(f, a.Size, a.CksumType)
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

	if a.CksumType != cos.ChecksumNone {
		cksum = cksumHash.Clone()
	}
	return &fileReader{f, a.Path, a.Name, cksum}, nil
}

func (r *fileReader) Open() (cos.ReadOpenCloser, error) {
	cksumType := cos.ChecksumNone
	if r.cksum != nil {
		cksumType = r.cksum.Type()
	}
	return newRandFile(&Arg{Path: r.filePath, Name: r.name, Size: ExistingFileSize, CksumType: cksumType})
}

func (r *fileReader) Cksum() *cos.Cksum {
	return r.cksum
}

func (r *fileReader) Reset() { r.File.Seek(0, io.SeekStart) }

//////////////
// sgReader //
//////////////

func newSG(a *Arg) (Reader, error) {
	var (
		cksumHash *cos.CksumHash
		err       error
	)

	if a.Arch != nil {
		// (A) archival content
		if err := a.Arch.Init(a.Size); err != nil {
			return nil, err
		}
		cksumHash, err = a.Arch.write(a.SGL, a.CksumType)
	} else if a.Size > 0 {
		// (B) plain random payload
		cksumHash, err = writeTruffleWithHash(a.SGL, a.Size, a.CksumType)
	}
	if err != nil {
		return nil, err
	}

	var cksum *cos.Cksum
	if a.CksumType != cos.ChecksumNone {
		cksum = cksumHash.Clone()
	}
	r := memsys.NewReader(a.SGL)
	return &sgReader{*r, cksum, a.SGL}, nil
}

func (r *sgReader) Cksum() *cos.Cksum {
	return r.cksum
}

func (r *sgReader) Reset() {
	rr := memsys.NewReader(r.sgl)
	r.Reader = *rr
}

/////////////////
// bytesReader //
/////////////////

func NewBytes(buf []byte) Reader {
	return &bytesReader{
		Reader: bytes.NewReader(buf),
		buf:    buf,
	}
}

// Seek is simply inherited

func (*bytesReader) Close() error      { return nil }
func (*bytesReader) Cksum() *cos.Cksum { return nil }
func (r *bytesReader) Reset()          { r.Seek(0, io.SeekStart) }

func (r *bytesReader) Open() (cos.ReadOpenCloser, error) {
	return &bytesReader{
		Reader: bytes.NewReader(r.buf),
		buf:    r.buf,
	}, nil
}

//
// readers' own utilities
//

// stream truffle bytes into w and (optionally) compute checksum
func writeTruffleWithHash(w io.Writer, size int64, cksumType string) (*cos.CksumHash, error) {
	tr := newTruffle()
	lim := &rrLimited{random: tr, size: size, off: 0}

	// cos.CopyAndChecksum copies from lim -> w and returns the running checksum (if requested).
	// It also avoids double-buffering on our side.
	_, h, err := cos.CopyAndChecksum(w, lim, nil, cksumType)
	if err != nil {
		return nil, err
	}
	return h, nil
}
