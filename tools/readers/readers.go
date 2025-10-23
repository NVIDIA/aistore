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
	File = "file" // file reader
	SG   = "sg"   // sgl-based reader
	Rand = "rand" // random reader
	Tar  = "tar"  // TAR reader
)

const ExistingFileSize = -1

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
	tarReader struct {
		b []byte
		bytes.Reader
		cksum *cos.Cksum
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
	Arch struct {
		Mime    string // archive.ExtTar|ExtTgz|ExtTarGz|ExtZip|ExtTarLz4
		Num     int    // files per shard
		MinSize int64  // min file size
		MaxSize int64  // max file size
		Prefix  string // optional prefix inside archive (e.g., "trunk-", "a/b/c/trunk-")
	}
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
	_ Reader = (*tarReader)(nil)
	_ Reader = (*fileReader)(nil)
	_ Reader = (*sgReader)(nil)
)

func New(a *Arg) (Reader, error) {
	switch a.Type {
	case SG:
		debug.Assert(a.SGL != nil)
		return newSG(a)
	case Rand:
		return newRand(a)
	case File:
		return newRandFile(a)
	case Tar:
		return newTarReader(a)
	default:
		return nil, errors.New("unknown memory type for creating inmem reader")
	}
}

////////////////
// randReader //
////////////////

func newRand(a *Arg) (Reader, error) {
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
		f, err    = os.OpenFile(fn, os.O_RDWR|os.O_CREATE, cos.PermRWR)
		exists    bool
	)
	if err != nil {
		return nil, err
	}
	if a.Size == ExistingFileSize {
		// checksum existing file
		exists = true
		if a.CksumType != cos.ChecksumNone {
			debug.Assert(a.CksumType != "")
			_, cksumHash, err = cos.CopyAndChecksum(io.Discard, f, nil, a.CksumType)
		}
	} else {
		// Write random file
		cksumHash, err = copyRandWithHash(f, a.Size, a.CksumType)
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
	var cksum *cos.Cksum
	if a.Size > 0 {
		cksumHash, err := copyRandWithHash(a.SGL, a.Size, a.CksumType)
		if err != nil {
			return nil, err
		}
		if a.CksumType != cos.ChecksumNone {
			cksum = cksumHash.Clone()
		}
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

///////////////
// tarReader //
///////////////

func newTarReader(a *Arg) (r Reader, err error) {
	var (
		singleFileSize = min(a.Size, int64(cos.KiB))
		buff           = bytes.NewBuffer(nil)
	)
	err = tarch.CreateArchCustomFilesToW(buff, tar.FormatUnknown, archive.ExtTar, max(int(a.Size/singleFileSize), 1),
		int(singleFileSize), shard.ContentKeyInt, ".cls", true /*missing keys*/, false /*exact size*/)
	if err != nil {
		return nil, err
	}
	cksum, err := cos.ChecksumBytes(buff.Bytes(), a.CksumType)
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

func (r *tarReader) Reset() { r.Reader.Seek(0, io.SeekStart) }

func (r *tarReader) Open() (cos.ReadOpenCloser, error) {
	return &tarReader{
		Reader: *bytes.NewReader(r.b),
		cksum:  r.cksum,
		b:      r.b,
	}, nil
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
