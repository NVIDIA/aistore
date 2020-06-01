// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/debug"
)

type (
	nopReader struct {
		size   int
		offset int
	}
	ReadOpenCloser interface {
		io.ReadCloser
		Open() (io.ReadCloser, error)
	}
	// ReadSizer is the interface that adds Size method to the basic reader.
	ReadSizer interface {
		io.Reader
		Size() int64
	}
	CallbackROC struct {
		ReadOpenCloser
		readCallback func(int, error)
	}
	CallbackRC struct {
		io.ReadCloser
		readCallback func(int, error)
	}
	FileHandle struct {
		*os.File
		fqn string
	}
	// FileSectionHandle is a slice of already opened file with optional padding
	// that implements ReadOpenCloser interface
	FileSectionHandle struct {
		s         *io.SectionReader
		padding   int64 // padding size
		padOffset int64 // offset inside padding when reading a file
	}
	// ByteHandle is a byte buffer(made from []byte) that implements
	// ReadOpenCloser interface
	ByteHandle struct {
		b []byte
		*bytes.Reader
	}
	// SizedReader is simple struct which implements ReadSizer interface.
	SizedReader struct {
		io.Reader
		size int64
	}
	nopOpener   struct{ io.ReadCloser }
	WriterMulti struct{ writers []io.Writer }
)

var (
	_ io.Reader      = &nopReader{}
	_ ReadOpenCloser = &FileHandle{}
	_ ReadSizer      = &SizedReader{}
	_ ReadOpenCloser = &FileSectionHandle{}
	_ ReadOpenCloser = &nopOpener{}
	_ ReadOpenCloser = &ByteHandle{}
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

	toRead := Min(len(b), left)
	r.offset += toRead
	return toRead, nil
}

// DrainReader reads and discards all the data from a reader.
func DrainReader(r io.Reader) error {
	_, err := io.Copy(ioutil.Discard, r)
	return err
}

////////////////
// ByteHandle //
////////////////

func NewByteHandle(bt []byte) *ByteHandle {
	return &ByteHandle{bt, bytes.NewReader(bt)}
}

func (b *ByteHandle) Close() error {
	return nil
}
func (b *ByteHandle) Open() (io.ReadCloser, error) {
	return ioutil.NopCloser(bytes.NewReader(b.b)), nil
}

///////////////
// nopOpener //
///////////////

func NopOpener(r io.ReadCloser) ReadOpenCloser {
	return &nopOpener{r}
}

func (n *nopOpener) Open() (io.ReadCloser, error) {
	return n, nil
}

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

func (f *FileHandle) Open() (io.ReadCloser, error) {
	return os.Open(f.fqn)
}

/////////////////
// SizedReader //
/////////////////

func NewSizedReader(r io.Reader, size int64) *SizedReader {
	return &SizedReader{r, size}
}

func (f *SizedReader) Size() int64 {
	return f.size
}

/////////////////
// CallbackROC //
/////////////////

func NewCallbackReadOpenCloser(r ReadOpenCloser, readCb func(int, error)) *CallbackROC {
	return &CallbackROC{
		ReadOpenCloser: r,
		readCallback:   readCb,
	}
}

func (r *CallbackROC) Read(p []byte) (n int, err error) {
	n, err = r.ReadOpenCloser.Read(p)
	r.readCallback(n, err)
	return n, err
}

func (r *CallbackROC) Open() (io.ReadCloser, error) {
	rc, err := r.ReadOpenCloser.Open()
	if err != nil {
		return rc, err
	}

	return NewCallbackReadCloser(rc, r.readCallback), nil
}

////////////////
// CallbackRC //
////////////////

func NewCallbackReadCloser(r io.ReadCloser, readCb func(int, error)) *CallbackRC {
	return &CallbackRC{
		ReadCloser:   r,
		readCallback: readCb,
	}
}

func (r *CallbackRC) Read(p []byte) (n int, err error) {
	n, err = r.ReadCloser.Read(p)
	r.readCallback(n, err)
	return n, err
}

///////////////////////
// FileSectionHandle //
///////////////////////

func NewFileSectionHandle(r io.ReaderAt, offset, size, padding int64) (*FileSectionHandle, error) {
	sec := io.NewSectionReader(r, offset, size)
	return &FileSectionHandle{sec, padding, 0}, nil
}

func (f *FileSectionHandle) Open() (io.ReadCloser, error) {
	_, err := f.s.Seek(0, io.SeekStart)
	f.padOffset = 0
	return f, err
}

func (f *FileSectionHandle) Reset() error {
	_, err := f.s.Seek(0, io.SeekStart)
	f.padOffset = 0
	return err
}

// Reads a file slice. When the slice finishes but the buffer is not filled yet,
// act as if it reads a few more bytes from somewhere
func (f *FileSectionHandle) Read(buf []byte) (n int, err error) {
	var fromPad int64

	// if it is still reading a file from disk - just continue reading
	if f.padOffset == 0 {
		n, err = f.s.Read(buf)
		// if it reads fewer bytes than expected and it does not fail,
		// try to "read" from padding
		if f.padding == 0 || n == len(buf) || (err != nil && err != io.EOF) {
			return n, err
		}
		fromPad = MinI64(int64(len(buf)-n), f.padding)
	} else {
		// slice is already read, keep reading padding bytes
		fromPad = MinI64(int64(len(buf)), f.padding-f.padOffset)
	}

	// either buffer is full or end of padding is reached. Nothing to read
	if fromPad == 0 {
		return n, io.EOF
	}

	// the number of remained bytes in padding is enough to complete read request
	for idx := n; idx < n+int(fromPad); idx++ {
		buf[idx] = 0
	}
	n += int(fromPad)
	f.padOffset += fromPad

	if f.padOffset < f.padding {
		return n, nil
	}
	return n, io.EOF
}

func (f *FileSectionHandle) Close() error { return nil }

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

///////////////////////
// misc file and dir //
///////////////////////

// CreateDir creates directory if does not exists. Does not return error when
// directory already exists.
func CreateDir(dir string) error {
	return os.MkdirAll(dir, configDirMode)
}

// CreateFile creates file and ensures that the directories for the file will be
// created if they do not yet exist.
func CreateFile(fname string) (*os.File, error) {
	if err := CreateDir(filepath.Dir(fname)); err != nil {
		return nil, err
	}
	return os.Create(fname)
}

// Rename renames file ensuring that the parent's directory of dst exists. Creates
// destination directory when it does not exist.
// NOTE: Rename should not be used to move objects across different disks, see: fs.MvFile.
func Rename(src, dst string) error {
	if err := os.Rename(src, dst); err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		// Retry with created directory - slow path.
		if err := CreateDir(filepath.Dir(dst)); err != nil {
			return err
		}
		return os.Rename(src, dst)
	}
	return nil
}

// RemoveFile removes object from path and ignores if the path no longer exists.
func RemoveFile(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// and computes checksum if requested
func CopyFile(src, dst string, buf []byte, cksumType string) (written int64, cksum *CksumHash, err error) {
	var (
		srcFile, dstFile *os.File
	)
	if srcFile, err = os.Open(src); err != nil {
		return
	}
	if dstFile, err = CreateFile(dst); err != nil {
		glog.Errorf("Failed to create %s: %v", dst, err)
		debug.AssertNoErr(srcFile.Close())
		return
	}
	written, cksum, err = CopyAndChecksum(dstFile, srcFile, buf, cksumType)
	if err != nil {
		glog.Errorf("Failed to copy %s -> %s: %v", src, dst, err)
	}
	debug.AssertNoErr(dstFile.Close())
	debug.AssertNoErr(srcFile.Close())
	return
}

// Saves the reader directly to a local file, xxhash-checksums if requested
func SaveReader(fqn string, reader io.Reader, buf []byte, cksumType string,
	size int64, dirMustExist string) (cksum *CksumHash, err error) {
	Assert(fqn != "")
	if dirMustExist != "" {
		if _, err := os.Stat(dirMustExist); err != nil {
			return nil, fmt.Errorf("failed to save-safe %s: directory %s %w", fqn, dirMustExist, err)
		}
	}
	var (
		written   int64
		file, erc           = CreateFile(fqn)
		writer    io.Writer = file
	)
	if erc != nil {
		return nil, erc
	}
	defer func() {
		if err != nil {
			os.Remove(fqn)
		}
	}()

	if size >= 0 {
		reader = io.LimitReader(reader, size)
	}
	written, cksum, err = CopyAndChecksum(writer, reader, buf, cksumType)
	erc = file.Close()

	if err != nil {
		err = fmt.Errorf("failed to save to %q: %w", fqn, err)
		return
	}
	if size >= 0 && written != size {
		err = fmt.Errorf("wrong size when saving to %q: expected %d, got %d", fqn, size, written)
		return
	}
	if erc != nil {
		err = fmt.Errorf("failed to close %q: %w", fqn, erc)
		return
	}
	return
}

// same as above, plus rename
func SaveReaderSafe(tmpfqn, fqn string, reader io.Reader, buf []byte, cksumType string,
	size int64, dirMustExist string) (cksum *CksumHash, err error) {
	if cksum, err = SaveReader(tmpfqn, reader, buf, cksumType, size, dirMustExist); err != nil {
		return nil, err
	}
	if err := Rename(tmpfqn, fqn); err != nil {
		return nil, err
	}
	return cksum, nil
}

// Read only the first line of a file.
// Do not use for big files: it reads all the content and then extracts the first
// line. Use for files that may contains a few lines with trailing EOL
func ReadOneLine(filename string) (string, error) {
	var line string
	err := ReadLines(filename, func(l string) error {
		line = l
		return io.EOF
	})
	return line, err
}

// Read only the first line of a file and return it as uint64
// Do not use for big files: it reads all the content and then extracts the first
// line. Use for files that may contains a few lines with trailing EOL
func ReadOneUint64(filename string) (uint64, error) {
	line, err := ReadOneLine(filename)
	if err != nil {
		return 0, err
	}
	val, err := strconv.ParseUint(line, 10, 64)
	return val, err
}

// Read only the first line of a file and return it as int64
// Do not use for big files: it reads all the content and then extracts the first
// line. Use for files that may contains a few lines with trailing EOL
func ReadOneInt64(filename string) (int64, error) {
	line, err := ReadOneLine(filename)
	if err != nil {
		return 0, err
	}
	val, err := strconv.ParseInt(line, 10, 64)
	return val, err
}

// Read a file line by line and call a callback for each line until the file
// ends or a callback returns io.EOF
func ReadLines(filename string, cb func(string) error) error {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	lineReader := bufio.NewReader(bytes.NewBuffer(b))
	for {
		line, _, err := lineReader.ReadLine()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}

		if err := cb(string(line)); err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
	}
	return nil
}

// CopyAndChecksum reads io.Reader and writes io.Writer; returns bytes written, checksum, and error
func CopyAndChecksum(w io.Writer, r io.Reader, buf []byte, cksumType string) (n int64, cksum *CksumHash, err error) {
	if cksumType == ChecksumNone || cksumType == "" {
		n, err = io.CopyBuffer(w, r, buf)
		return
	}
	cksum = NewCksumHash(cksumType)
	var mw io.Writer = cksum.H
	if w != ioutil.Discard {
		mw = NewWriterMulti(cksum.H, w)
	}
	n, err = io.CopyBuffer(mw, r, buf)
	cksum.Finalize()
	return
}
