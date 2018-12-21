/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package tutils

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"time"

	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/memsys"
	"github.com/OneOfOne/xxhash"
)

const (
	// ReaderTypeFile defines the name for file reader
	ReaderTypeFile = "file"
	// ReaderTypeSG defines the name for sg reader
	ReaderTypeSG = "sg"
	// ReaderTypeRand defines the name for rand reader
	ReaderTypeRand = "rand"
	// ReaderTypeInMem defines the name for inmem reader
	ReaderTypeInMem = "inmem"
)

// Reader is the interface a client works with to read in data and send to a HTTP server
type Reader interface {
	io.ReadCloser
	io.Seeker
	Open() (io.ReadCloser, error)
	XXHash() string
	Description() string
}

// description returns a string constructed from a name and a xxhash
func description(name, hash string) string {
	return name + " xxhash " + hash[:8] + "..."
}

// randReader implements Reader.
// It doesn't not use a file or allocated memory as data backing.
type randReader struct {
	seed   int64
	rnd    *rand.Rand
	size   int64
	offset int64
	xxHash string
}

var _ Reader = &randReader{}

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
		seed:   r.seed,
		rnd:    rand.New(rand.NewSource(r.seed)),
		size:   r.size,
		xxHash: r.xxHash,
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
		return 0, errors.New("RandReader.Seek: invalid whence")
	}

	if abs < 0 {
		return 0, errors.New("RandReader.Seek: negative position")
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
		return 0, fmt.Errorf("RandReader.Seek: failed to seek to %d, seeked to %d instead", offset, actual)
	}

	return abs, nil
}

// XXHash implements the Reader interface.
func (r *randReader) XXHash() string {
	return r.xxHash
}

// Description implements the Reader interface.
func (r *randReader) Description() string {
	return description("RandReader", r.xxHash)
}

// NewRandReader returns a new randReader
func NewRandReader(size int64, withHash bool) (Reader, error) {
	var (
		hash string
		err  error
		seed = time.Now().UnixNano()
	)
	slab, err := Mem2.GetSlab2(cmn.KiB * 32)
	if err != nil {
		return nil, err
	}
	buf := slab.Alloc()
	defer slab.Free(buf)
	rand1 := rand.New(rand.NewSource(seed))
	rr := &rrLimited{rand1, size, 0}
	if withHash {
		xx := xxhash.New64()
		_, err := cmn.ReceiveAndChecksum(ioutil.Discard, rr, buf, xx)
		if err != nil {
			return nil, err
		}
		hashIn64 := xx.Sum64()
		hashInBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(hashInBytes, hashIn64)
		hash = hex.EncodeToString(hashInBytes)
	}
	rand1dup := rand.New(rand.NewSource(seed))
	return &randReader{
		seed:   seed,
		rnd:    rand1dup,
		size:   size,
		xxHash: hash,
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

type inMemReader struct {
	bytes.Reader
	data   *bytes.Buffer
	xxHash string
}

var _ Reader = &inMemReader{}

// bytesReaderCloser is a helper for being able to do multi read on a byte buffer
type bytesReaderCloser struct {
	bytes.Reader
}

func (q *bytesReaderCloser) Close() error {
	return nil
}

// Open implements the Reader interface.
func (r *inMemReader) Open() (io.ReadCloser, error) {
	return &bytesReaderCloser{*bytes.NewReader(r.data.Bytes())}, nil
}

// Close implements the Reader interface.
func (r *inMemReader) Close() error {
	return nil
}

// Description implements the Reader interface.
func (r *inMemReader) Description() string {
	return description("InMemReader", r.xxHash)
}

// XXHash implements the Reader interface.
func (r *inMemReader) XXHash() string {
	return r.xxHash
}

// NewInMemReader returns a new inMemReader
func NewInMemReader(size int64, withHash bool) (Reader, error) {
	data := bytes.NewBuffer(make([]byte, size))
	data.Reset()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	hash, err := copyRandWithHash(data, size, withHash, rnd)
	if err != nil {
		return nil, err
	}

	return &inMemReader{*bytes.NewReader(data.Bytes()), data, hash}, nil
}

type fileReader struct {
	*os.File
	fullName string // Example: "/dir/dfc/smoke/bGzhWKWoxHDSePnELftx"
	name     string // Example: smoke/bGzhWKWoxHDSePnELftx
	xxHash   string
}

var _ Reader = &fileReader{}

// Open implements the Reader interface.
func (r *fileReader) Open() (io.ReadCloser, error) {
	return os.Open(r.fullName)
}

// XXHash implements the Reader interface.
func (r *fileReader) XXHash() string {
	return r.xxHash
}

// Description implements the Reader interface.
func (r *fileReader) Description() string {
	return description("FileReader "+r.name, r.xxHash)
}

// NewFileReader creates/opens the file, populates it with random data, closes it and returns a new fileReader
func NewFileReader(filepath, name string, size int64, withHash bool) (Reader, error) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	fn := path.Join(filepath, name)

	f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE, 0666) //wr-wr-wr-
	if err != nil {
		return nil, err
	}
	defer f.Close()

	hash, err := copyRandWithHash(f, size, withHash, rnd)
	if err != nil {
		return nil, err
	}

	// FIXME: No need to have 'f' in fileReader?
	return &fileReader{f, fn, name, hash}, nil
}

// NewFileReaderFromFile opens an existing file, read data to compute hash, closes it and returns
// a new fileReader.
// Note: The difference between NewFileReader and this is NewFileReader generates a file from random
//       data first, then returns the file as the source for DFC put. This reader doesn't generate a
//       file but reads an existing file to compute xxHash, then returns the same file as source for
//       DFC put.
func NewFileReaderFromFile(fn string, withHash bool) (Reader, error) {
	f, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var hash string
	if withHash {
		buf, slab := Mem2.AllocFromSlab2(cmn.DefaultBufSize)
		if _, hash, err = cmn.ReadWriteWithHash(f, ioutil.Discard, buf); err != nil {
			return nil, err
		}
		slab.Free(buf)
	}

	return &fileReader{nil, fn, "" /* dfc prefix */, hash}, nil
}

type sgReader struct {
	memsys.Reader
	xxHash string
}

var _ Reader = &sgReader{}

// Description implements the Reader interface.
func (r *sgReader) Description() string {
	return description("SGReader", r.xxHash)
}

// XXHash implements the Reader interface.
func (r *sgReader) XXHash() string {
	return r.xxHash
}

// NewSGReader returns a new sgReader
func NewSGReader(sgl *memsys.SGL, size int64, withHash bool) (Reader, error) {
	var (
		hash string
		err  error
	)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	if size > 0 {
		hash, err = copyRandWithHash(sgl, size, withHash, rnd)
		if err != nil {
			return nil, err
		}
	}

	r := memsys.NewReader(sgl)
	return &sgReader{*r, hash}, nil
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

// Description implements the Reader interface.
func (r *bytesReader) Description() string {
	return "not implemented"
}

// XXHash implements the Reader interface.
func (r *bytesReader) XXHash() string {
	return "not implemented"
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
func NewReader(p ParamReader) (Reader, error) {
	switch p.Type {
	case ReaderTypeSG:
		if p.SGL == nil {
			return nil, fmt.Errorf("SGL is empty while reader type is SGL")
		}
		return NewSGReader(p.SGL, p.Size, true /* withHash */)
	case ReaderTypeRand:
		return NewRandReader(p.Size, true /* withHash */)
	case ReaderTypeInMem:
		return NewInMemReader(p.Size, true /* withHash */)
	case ReaderTypeFile:
		return NewFileReader(p.Path, p.Name, p.Size, true /* withHash */)
	default:
		return nil, fmt.Errorf("Unknown memory type for creating inmem reader")
	}
}
