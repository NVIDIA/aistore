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
	WriterAt interface {
		io.Writer
		io.WriterAt
	}
	// ReadSizer is the interface that adds Size method to the basic reader.
	ReadSizer interface {
		io.Reader
		Size() int64
	}
	CallbackROC struct {
		roc          ReadOpenCloser
		readCallback func(int, error)
		// Number of bytes we've already read, counting from last `Open`.
		readBytes int
		// Since we could possibly reopen a reader we must keep track of the
		// bytes we already reported to `readCallback` so there is no duplications.
		// This value is preserved across all the `Open`'s.
		reportedBytes int
	}
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

	// WriterOnly is helper struct to hide `io.ReaderFrom` implementation which
	// can use some heuristics to improve performance but can result in not
	// using `buffer` provided in `io.CopyBuffer`. See: https://golang.org/doc/go1.15#os.
	WriterOnly struct{ io.Writer }
)

// interface guard
var (
	_ io.Reader      = (*nopReader)(nil)
	_ ReadOpenCloser = (*FileHandle)(nil)
	_ ReadSizer      = (*SizedReader)(nil)
	_ ReadOpenCloser = (*SectionHandle)(nil)
	_ ReadOpenCloser = (*FileSectionHandle)(nil)
	_ ReadOpenCloser = (*nopOpener)(nil)
	_ ReadOpenCloser = (*ByteHandle)(nil)
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
		roc:          r,
		readCallback: readCb,
	}
}

func (r *CallbackROC) Read(p []byte) (n int, err error) {
	n, err = r.roc.Read(p)
	r.readBytes += n
	if r.readBytes > r.reportedBytes {
		diff := r.readBytes - r.reportedBytes
		r.readCallback(diff, err)
		r.reportedBytes += diff
	}
	return n, err
}

func (r *CallbackROC) Open() (io.ReadCloser, error) {
	rc, err := r.roc.Open()
	if err != nil {
		return rc, err
	}
	return &CallbackROC{
		roc:           NopOpener(rc),
		readCallback:  r.readCallback,
		readBytes:     0,
		reportedBytes: r.reportedBytes,
	}, nil
}

func (r *CallbackROC) Close() error { return r.roc.Close() }

///////////////////
// SectionHandle //
///////////////////

func NewSectionHandle(r io.ReaderAt, offset, size, padding int64) *SectionHandle {
	sec := io.NewSectionReader(r, offset, size)
	return &SectionHandle{r, sec, offset, size, padding, 0}
}

func (f *SectionHandle) Open() (io.ReadCloser, error) {
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

func (f *SectionHandle) Close() error { return nil }

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

func (f *FileSectionHandle) Open() (io.ReadCloser, error) {
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
	var srcFile, dstFile *os.File
	if srcFile, err = os.Open(src); err != nil {
		return
	}
	if dstFile, err = CreateFile(dst); err != nil {
		glog.Errorf("Failed to create %s: %v", dst, err)
		Close(srcFile)
		return
	}
	written, cksum, err = CopyAndChecksum(dstFile, srcFile, buf, cksumType)
	if err != nil {
		glog.Errorf("Failed to copy %s -> %s: %v", src, dst, err)
	}
	Close(dstFile)
	Close(srcFile)
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
		file, erc = CreateFile(fqn)
		writer    = WriterOnly{file} // Hiding `ReadFrom` for `*os.File` introduced in Go1.15.
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
		os.Remove(tmpfqn)
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

// ChecksumBytes computes checksum of given bytes using additional buffer.
func ChecksumBytes(b []byte, cksumType string) (cksum *Cksum, err error) {
	_, hash, err := CopyAndChecksum(ioutil.Discard, bytes.NewReader(b), nil, cksumType)
	if err != nil {
		return nil, err
	}
	return &hash.Cksum, nil
}

// DrainReader reads and discards all the data from a reader.
// No need for `io.CopyBuffer` as `ioutil.Discard` has efficient `io.ReaderFrom` implementation.
func DrainReader(r io.Reader) {
	_, err := io.Copy(ioutil.Discard, r)
	if err == nil || IsEOF(err) {
		return
	}
	debug.AssertNoErr(err)
}

// FloodWriter writes `n` random bytes to provided writer.
func FloodWriter(w io.Writer, n int64) error {
	_, err := io.CopyN(w, NowRand(), n)
	return err
}

func Close(closer io.Closer) {
	err := closer.Close()
	debug.AssertNoErr(err)
}
