// Package readers holds various implementations of client.Reader
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package readers

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
	"time"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
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

// helper functions
func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

// populateData reads data from random source and writes to a writer,
// calculates and returns xxhash (if needed)
func populateData(w io.Writer, size int64, withHash bool, rnd *rand.Rand) (string, error) {
	var (
		left = size
		hash string
		h    *xxhash.XXHash64
	)

	blkSize := int64(1048576)
	blk := make([]byte, blkSize)

	if withHash {
		h = xxhash.New64()
	}

	for i := int64(0); i <= size/blkSize; i++ {
		n := min(blkSize, left)
		rnd.Read(blk[:n])
		m, err := w.Write(blk[:n])
		if err != nil {
			return "", err
		}

		if withHash {
			h.Write(blk[:m])
		}

		left -= int64(m)
	}

	if withHash {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(h.Sum64()))
		hash = hex.EncodeToString(b)
	}

	return hash, nil
}

// description returns a string constructed from a name and a xxhash
func description(name, hash string) string {
	return name + " xxhash " + hash[:8] + "..."
}

// randReader implements client.Reader.
// It doesn't not use a file or allocated memory as data backing.
type randReader struct {
	seed   int64
	rnd    *rand.Rand
	size   int64
	offset int64
	xxHash string
}

var _ client.Reader = &randReader{}

// Read implements the client.Reader interface.
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

// Open implements the client.Reader interface.
// Returns a new rand reader using the same seed.
func (r *randReader) Open() (io.ReadCloser, error) {
	return &randReader{
		seed:   r.seed,
		rnd:    rand.New(rand.NewSource(r.seed)),
		size:   r.size,
		xxHash: r.xxHash,
	}, nil
}

// Close implements the client.Reader interface.
func (r *randReader) Close() error {
	return nil
}

// Seek implements the client.Reader interface.
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

// XXHash implements the client.Reader interface.
func (r *randReader) XXHash() string {
	return r.xxHash
}

// Description implements the client.Reader interface.
func (r *randReader) Description() string {
	return description("RandReader", r.xxHash)
}

// NewRandReader returns a new randReader
func NewRandReader(size int64, withHash bool) (client.Reader, error) {
	var (
		hash string
		err  error
	)

	seed := time.Now().UnixNano()
	if withHash {
		hash, err = populateData(ioutil.Discard, size, true, rand.New(rand.NewSource(seed)))
		if err != nil {
			return nil, err
		}
	}

	return &randReader{
		seed:   seed,
		rnd:    rand.New(rand.NewSource(seed)),
		size:   size,
		xxHash: hash,
	}, nil
}

type inMemReader struct {
	bytes.Reader
	data   *bytes.Buffer
	xxHash string
}

var _ client.Reader = &inMemReader{}

// bytesReaderCloser is a helper for being able to do multi read on a byte buffer
type bytesReaderCloser struct {
	bytes.Reader
}

func (q *bytesReaderCloser) Close() error {
	return nil
}

// Open implements the client.Reader interface.
func (r *inMemReader) Open() (io.ReadCloser, error) {
	return &bytesReaderCloser{*bytes.NewReader(r.data.Bytes())}, nil
}

// Close implements the client.Reader interface.
func (r *inMemReader) Close() error {
	return nil
}

// Description implements the client.Reader interface.
func (r *inMemReader) Description() string {
	return description("InMemReader", r.xxHash)
}

// XXHash implements the client.Reader interface.
func (r *inMemReader) XXHash() string {
	return r.xxHash
}

// NewInMemReader returns a new inMemReader
func NewInMemReader(size int64, withHash bool) (client.Reader, error) {
	data := bytes.NewBuffer(make([]byte, size))
	data.Reset()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	hash, err := populateData(data, size, withHash, rnd)
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

var _ client.Reader = &fileReader{}

// Open implements the client.Reader interface.
func (r *fileReader) Open() (io.ReadCloser, error) {
	return os.Open(r.fullName)
}

// XXHash implements the client.Reader interface.
func (r *fileReader) XXHash() string {
	return r.xxHash
}

// Description implements the client.Reader interface.
func (r *fileReader) Description() string {
	return description("FileReader "+r.name, r.xxHash)
}

// NewFileReader returns a new fileReader
// If no error, returns with the file opened, caller needs to close the file.
func NewFileReader(path, name string, size int64, withHash bool) (client.Reader, error) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	fn := path + "/" + name

	f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE, 0666) //wr-wr-wr-
	if err != nil {
		return nil, err
	}

	hash, err := populateData(f, size, withHash, rnd)
	if err != nil {
		return nil, err
	}

	return &fileReader{f, fn, name, hash}, nil
}

type sgReader struct {
	*dfc.Reader
	xxHash string
}

var _ client.Reader = &sgReader{}

// Description implements the client.Reader interface.
func (r *sgReader) Description() string {
	return description("SGReader", r.xxHash)
}

// XXHash implements the client.Reader interface.
func (r *sgReader) XXHash() string {
	return r.xxHash
}

// NewSGReader returns a new sgReader
func NewSGReader(sgl *dfc.SGLIO, size int64, withHash bool) (client.Reader, error) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	hash, err := populateData(sgl, size, withHash, rnd)
	if err != nil {
		return nil, err
	}

	return &sgReader{dfc.NewReader(sgl), hash}, nil
}

// ParamReader is used to pass in parameters when creating a new reader
type ParamReader struct {
	Type       string     // file | sg | inmem | rand
	SGL        *dfc.SGLIO // When Type == sg
	Path, Name string     // When Type == file; path and name of file to be created (if not already existing)
	Size       int64
}

// NewReader returns a data reader; type of reader returned is based on the parameters provided
func NewReader(p ParamReader) (client.Reader, error) {
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
