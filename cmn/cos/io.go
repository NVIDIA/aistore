// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"bytes"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/cmn/debug"
)

// POSIX permissions
const (
	PermRWR   os.FileMode = 0o640
	PermRWRR  os.FileMode = 0o644 // (archived)
	PermRWXRX os.FileMode = 0o750

	configDirMode = PermRWXRX | os.ModeDir
)

const ContentLengthUnknown = -1

const PathSeparator = string(filepath.Separator)

// follows below:
// - readers: interfaces
// - readers: implementations
// - handles
// - writers

// readers: interfaces
type (
	ReadOpenCloser interface {
		io.ReadCloser
		Open() (ReadOpenCloser, error)
	}
	ROCS interface {
		ReadOpenCloser
		io.Seeker
		OpenDup() (ROCS, error)
	}
	ReadSizer interface {
		io.Reader
		Size() int64
	}
	ReadCloseSizer interface {
		io.ReadCloser
		Size() int64
	}
	ReadOpenCloseSizer interface { // see sizedReader below
		ReadOpenCloser
		Size() int64
	}
	ReadReaderAt interface {
		io.Reader
		io.ReaderAt
	}
)

// readers: implementations
type (
	sizedReader struct {
		io.Reader
		size int64
	}

	nopReader struct {
		size   int
		offset int
	}
	ReaderWithArgs struct {
		R       io.Reader
		ReadCb  func(int, error)
		DeferCb func()
		Rsize   int64
	}
	nopOpener struct{ io.ReadCloser }
)

// handles (and even more readers)
type (
	FileHandle struct {
		*os.File
		fqn string
	}
	// SectionHandle is a section of reader with optional padding that implements
	// ReadOpenCloser interface.
	SectionHandle struct {
		r         io.ReaderAt
		s         *io.SectionReader
		offset    int64 // slice start
		size      int64 // slice length
		padding   int64 // padding size
		padOffset int64 // offset inside padding when reading a file
	}
	// FileSectionHandle opens a file and reads a section of it with optional
	// padding. It implements the ReadOpenCloser interface.
	FileSectionHandle struct {
		fh  *FileHandle
		sec *SectionHandle
	}
	// ByteReader is a byte buffer(made from []byte) that implements
	// ReadOpenCloser interface
	ByteReader struct {
		*bytes.Reader
		b []byte
	}
)

// writers
type (
	WriterAt interface {
		io.Writer
		io.WriterAt
	}
	WriteSizer interface {
		io.Writer
		Size() int64
	}

	nopWriteCloser struct {
		io.Writer
	}

	WriterMulti struct{ writers []io.Writer }

	// WriterOnly is a helper struct to hide `io.ReaderFrom` interface implementation
	// As far as http.ResponseWriter (and its underlying tcp conn.), the following are tradeoffs:
	// [-] sendfile (when sending), or
	// [-] copy_file_range (when writing local files)
	// [+] use (reusable) buffer, reduce code path, reduce locking
	WriterOnly struct{ io.Writer }

	// common between `Buffer` (below) and `memsys.SGL`
	WriterTo2 interface {
		WriteTo2(dst io.Writer) error
	}
	Buffer struct {
		b *bytes.Buffer
	}
)

// core.LOM: reader and writer
type (
	LomReader interface {
		io.ReadCloser
		io.ReaderAt
	}
	LomReaderOpener interface {
		LomReader
		Open() (ReadOpenCloser, error)
	}
	LomWriter interface {
		io.WriteCloser
		Sync() error
	}
)

// interface guard
var (
	_ io.Reader = (*nopReader)(nil)

	_ ROCS = (*FileHandle)(nil)
	_ ROCS = (*ByteReader)(nil)

	_ ReadSizer = (*sizedReader)(nil)

	_ ReadOpenCloser = (*SectionHandle)(nil)
	_ ReadOpenCloser = (*FileSectionHandle)(nil)
	_ ReadOpenCloser = (*nopOpener)(nil)

	_ io.WriteCloser = (*nopWriteCloser)(nil)
)

///////////////
// nopReader //
///////////////

func NopReader(size int64) io.Reader {
	return &nopReader{
		size:   int(size),
		offset: 0,
	}
}

func (r *nopReader) Read(b []byte) (int, error) {
	left := r.size - r.offset
	if left == 0 {
		return 0, io.EOF
	}

	toRead := min(len(b), left)
	r.offset += toRead
	return toRead, nil
}

////////////////////
// nopWriteCloser //
////////////////////

func (*nopWriteCloser) Close() error {
	return nil
}

// Helper function
func NopWriteCloser(w io.Writer) io.WriteCloser {
	return &nopWriteCloser{Writer: w}
}

////////////////
// ByteReader //
////////////////

func NewByteReader(bt []byte) *ByteReader           { return &ByteReader{bytes.NewReader(bt), bt} }
func (*ByteReader) Close() error                    { return nil }
func (b *ByteReader) Open() (ReadOpenCloser, error) { return NewByteReader(b.b), nil }
func (b *ByteReader) OpenDup() (ROCS, error)        { return NewByteReader(b.b), nil }

///////////////
// nopOpener //
///////////////

func NopOpener(r io.ReadCloser) ReadOpenCloser     { return &nopOpener{r} }
func (n *nopOpener) Open() (ReadOpenCloser, error) { return n, nil }

////////////////
// FileHandle //
////////////////

func NewFileHandle(fqn string) (*FileHandle, error) {
	file, err := os.Open(fqn)
	if err != nil {
		return nil, err
	}
	return &FileHandle{file, fqn}, nil
}

func (f *FileHandle) Open() (ReadOpenCloser, error) { return NewFileHandle(f.fqn) }
func (f *FileHandle) OpenDup() (ROCS, error)        { return NewFileHandle(f.fqn) }

////////////
// Sized* //
////////////

func NewSizedReader(r io.Reader, size int64) ReadSizer { return &sizedReader{r, size} }
func (f *sizedReader) Size() int64                     { return f.size }

////////////////////
// ReaderWithArgs //
////////////////////

func (r *ReaderWithArgs) Size() int64 { return r.Rsize }

func (r *ReaderWithArgs) Read(p []byte) (n int, err error) {
	n, err = r.R.Read(p)
	if r.ReadCb != nil {
		r.ReadCb(n, err)
	}
	return n, err
}

func (*ReaderWithArgs) Open() (ReadOpenCloser, error) { panic("not supported") }

func (r *ReaderWithArgs) Close() (err error) {
	if rc, ok := r.R.(io.ReadCloser); ok {
		err = rc.Close()
	}
	if r.DeferCb != nil {
		r.DeferCb()
	}
	return err
}

///////////////////
// SectionHandle //
///////////////////

func NewSectionHandle(r io.ReaderAt, offset, size, padding int64) *SectionHandle {
	debug.Assert(padding >= 0)
	sec := io.NewSectionReader(r, offset, size)
	return &SectionHandle{r, sec, offset, size, padding, 0}
}

func (f *SectionHandle) Open() (ReadOpenCloser, error) {
	return NewSectionHandle(f.r, f.offset, f.size, f.padding), nil
}

// Reads a reader section. When the slice finishes but the buffer is not filled
// yet, act as if it reads a few more bytes from somewhere.
func (f *SectionHandle) Read(buf []byte) (n int, err error) {
	var fromPad int64

	// if it is still reading a file from disk - just continue reading
	if f.padOffset == 0 {
		n, err = f.s.Read(buf)
		// if it reads fewer bytes than expected and it does not fail,
		// try to "read" from padding
		if f.padding == 0 || n == len(buf) || (err != nil && err != io.EOF) {
			return n, err
		}
		fromPad = min(int64(len(buf)-n), f.padding)
	} else {
		// slice is already read, keep reading padding bytes
		fromPad = min(int64(len(buf)), f.padding-f.padOffset)
	}

	// either buffer is full or end of padding is reached. Nothing to read
	if fromPad <= 0 {
		debug.Assert(fromPad == 0)
		return n, io.EOF
	}

	// the number of remaining bytes in padding is enough to complete read request
	for idx := n; idx < n+int(fromPad); idx++ {
		buf[idx] = 0
	}

	debug.Assert(n < math.MaxInt-int(fromPad))
	n += int(fromPad)

	// check for integer overflow
	debug.Assert(f.padOffset <= math.MaxInt-fromPad)
	f.padOffset += fromPad

	if f.padOffset < f.padding {
		return n, nil
	}
	return n, io.EOF
}

func (*SectionHandle) Close() error { return nil }

///////////////////////
// FileSectionHandle //
///////////////////////

// NewFileSectionHandle opens file which is expected at `fqn` and defines
// a SectionHandle on it to only read a specified section.
func NewFileSectionHandle(fqn string, offset, size int64) (*FileSectionHandle, error) {
	fh, err := NewFileHandle(fqn)
	if err != nil {
		return nil, err
	}
	sec := NewSectionHandle(fh, offset, size, 0)
	return &FileSectionHandle{fh: fh, sec: sec}, nil
}

func (f *FileSectionHandle) Open() (ReadOpenCloser, error) {
	return NewFileSectionHandle(f.fh.fqn, f.sec.offset, f.sec.size)
}

func (f *FileSectionHandle) Read(buf []byte) (int, error) { return f.sec.Read(buf) }
func (f *FileSectionHandle) Close() error                 { return f.fh.Close() }

/////////////////
// WriterMulti //
/////////////////

func NewWriterMulti(w ...io.Writer) *WriterMulti { return &WriterMulti{w} }

func (mw *WriterMulti) Write(b []byte) (n int, err error) {
	l := len(b)
	for _, w := range mw.writers {
		n, err = w.Write(b)
		if err == nil && n == l {
			continue
		}
		if err == nil {
			err = io.ErrShortWrite
		}
		return
	}
	n = l
	return
}

////////////
// Buffer //
////////////

func NewBuffer(b []byte) *Buffer {
	return &Buffer{b: bytes.NewBuffer(b)}
}

func (w *Buffer) WriteTo2(dst io.Writer) (err error) {
	_, err = w.b.WriteTo(dst)
	return err
}
