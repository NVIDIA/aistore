// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"bufio"
	"bytes"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strconv"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
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

// readers
type (
	ReadOpenCloser interface {
		io.ReadCloser
		Open() (ReadOpenCloser, error)
	}
	// ReadSizer is the interface that adds Size method to io.Reader.
	ReadSizer interface {
		io.Reader
		Size() int64
	}
	// ReadCloseSizer is the interface that adds Size method to io.ReadCloser.
	ReadCloseSizer interface {
		io.ReadCloser
		Size() int64
	}
	// ReadOpenCloseSizer is the interface that adds Size method to ReadOpenCloser.
	ReadOpenCloseSizer interface {
		ReadOpenCloser
		Size() int64
	}
	sizedReader struct {
		io.Reader
		size int64
	}

	ReadReaderAt interface {
		io.Reader
		io.ReaderAt
	}
	LomReader interface {
		io.ReadCloser
		io.ReaderAt
	}

	// implementations

	nopReader struct {
		size   int
		offset int
	}
	deferRCS struct {
		ReadCloseSizer
		cb func()
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
	ReaderArgs struct {
		R       io.Reader
		ReadCb  func(int, error)
		DeferCb func()
		Size    int64
	}
	ReaderWithArgs struct {
		args ReaderArgs
	}
	nopOpener struct{ io.ReadCloser }
)

// handles (and more readers)
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
	// ByteHandle is a byte buffer(made from []byte) that implements
	// ReadOpenCloser interface
	ByteHandle struct {
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

// interface guard
var (
	_ io.Reader      = (*nopReader)(nil)
	_ ReadOpenCloser = (*FileHandle)(nil)
	_ ReadOpenCloser = (*CallbackROC)(nil)
	_ ReadSizer      = (*sizedReader)(nil)
	_ ReadOpenCloser = (*SectionHandle)(nil)
	_ ReadOpenCloser = (*FileSectionHandle)(nil)
	_ ReadOpenCloser = (*nopOpener)(nil)
	_ ReadOpenCloser = (*ByteHandle)(nil)
)

// including "unexpecting EOF" to accommodate unsized streaming and
// early termination of the other side (prior to sending the first byte)
func IsEOF(err error) bool {
	return err == io.EOF || err == io.ErrUnexpectedEOF ||
		errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF)
}

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

////////////////
// ByteHandle //
////////////////

func NewByteHandle(bt []byte) *ByteHandle           { return &ByteHandle{bytes.NewReader(bt), bt} }
func (*ByteHandle) Close() error                    { return nil }
func (b *ByteHandle) Open() (ReadOpenCloser, error) { return NewByteHandle(b.b), nil }

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

func (f *FileHandle) Open() (ReadOpenCloser, error) {
	return NewFileHandle(f.fqn)
}

////////////
// Sized* //
////////////

func NewSizedReader(r io.Reader, size int64) ReadSizer { return &sizedReader{r, size} }
func (f *sizedReader) Size() int64                     { return f.size }

//////////////
// deferRCS //
//////////////

func NewDeferRCS(r ReadCloseSizer, cb func()) ReadCloseSizer {
	if cb == nil {
		return r
	}
	return &deferRCS{r, cb}
}

func (r *deferRCS) Close() (err error) {
	err = r.ReadCloseSizer.Close()
	r.cb()
	return
}

/////////////////
// CallbackROC //
/////////////////

func NewCallbackReadOpenCloser(r ReadOpenCloser, readCb func(int, error), reportedBytes ...int) *CallbackROC {
	var rb int
	if len(reportedBytes) > 0 {
		rb = reportedBytes[0]
	}
	return &CallbackROC{
		roc:           r,
		readCallback:  readCb,
		readBytes:     0,
		reportedBytes: rb,
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

func (r *CallbackROC) Open() (ReadOpenCloser, error) {
	rc, err := r.roc.Open()
	if err != nil {
		return rc, err
	}
	return NewCallbackReadOpenCloser(rc, r.readCallback, r.reportedBytes), nil
}

func (r *CallbackROC) Close() error { return r.roc.Close() }

////////////////////
// ReaderWithArgs //
////////////////////

func NewReaderWithArgs(args ReaderArgs) *ReaderWithArgs {
	return &ReaderWithArgs{args: args}
}

func (r *ReaderWithArgs) Size() int64 { return r.args.Size }

func (r *ReaderWithArgs) Read(p []byte) (n int, err error) {
	n, err = r.args.R.Read(p)
	if r.args.ReadCb != nil {
		r.args.ReadCb(n, err)
	}
	return n, err
}

func (*ReaderWithArgs) Open() (ReadOpenCloser, error) { panic("not supported") }

func (r *ReaderWithArgs) Close() (err error) {
	if rc, ok := r.args.R.(io.ReadCloser); ok {
		err = rc.Close()
	}
	if r.args.DeferCb != nil {
		r.args.DeferCb()
	}
	return err
}

///////////////////
// SectionHandle //
///////////////////

func NewSectionHandle(r io.ReaderAt, offset, size, padding int64) *SectionHandle {
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

///////////////////////
// misc file and dir //
///////////////////////

// ExpandPath replaces common abbreviations in file path (eg. `~` with absolute
// path to the current user home directory) and cleans the path.
func ExpandPath(path string) string {
	if path == "" || path[0] != '~' {
		return filepath.Clean(path)
	}
	if len(path) > 1 && path[1] != '/' {
		return filepath.Clean(path)
	}

	currentUser, err := user.Current()
	if err != nil {
		return filepath.Clean(path)
	}
	return filepath.Clean(filepath.Join(currentUser.HomeDir, path[1:]))
}

// CreateDir creates directory if does not exist.
// If the directory already exists returns nil.
func CreateDir(dir string) error {
	return os.MkdirAll(dir, configDirMode)
}

// CreateFile creates a new write-only (O_WRONLY) file with default cos.PermRWR permissions.
// NOTE: if the file pathname doesn't exist it'll be created.
// NOTE: if the file already exists it'll be also silently truncated.
func CreateFile(fqn string) (*os.File, error) {
	if err := CreateDir(filepath.Dir(fqn)); err != nil {
		return nil, err
	}
	return os.OpenFile(fqn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, PermRWR)
}

// (creates destination directory if doesn't exist)
func Rename(src, dst string) (err error) {
	err = os.Rename(src, dst)
	if err == nil || !os.IsNotExist(err) {
		return
	}
	// create and retry (slow path)
	err = CreateDir(filepath.Dir(dst))
	if err == nil {
		err = os.Rename(src, dst)
	}
	return
}

// RemoveFile removes path; returns nil upon success or if the path does not exist.
func RemoveFile(path string) (err error) {
	err = os.Remove(path)
	if os.IsNotExist(err) {
		err = nil
	}
	return
}

// and computes checksum if requested
func CopyFile(src, dst string, buf []byte, cksumType string) (written int64, cksum *CksumHash, err error) {
	var srcFile, dstFile *os.File
	if srcFile, err = os.Open(src); err != nil {
		return
	}
	if dstFile, err = CreateFile(dst); err != nil {
		nlog.Errorln("Failed to create", dst+":", err)
		Close(srcFile)
		return
	}
	written, cksum, err = CopyAndChecksum(dstFile, srcFile, buf, cksumType)
	Close(srcFile)
	defer func() {
		if err == nil {
			return
		}
		if nestedErr := RemoveFile(dst); nestedErr != nil {
			nlog.Errorf("Nested (%v): failed to remove %s, err: %v", err, dst, nestedErr)
		}
	}()
	if err != nil {
		nlog.Errorln("Failed to copy", src, "=>", dst+":", err)
		Close(dstFile)
		return
	}
	if err = FlushClose(dstFile); err != nil {
		nlog.Errorln("Failed to flush and close", dst+":", err)
	}
	return
}

func SaveReaderSafe(tmpfqn, fqn string, reader io.Reader, buf []byte, cksumType string, size int64) (cksum *CksumHash,
	err error) {
	if cksum, err = SaveReader(tmpfqn, reader, buf, cksumType, size); err != nil {
		return
	}
	if err = Rename(tmpfqn, fqn); err != nil {
		os.Remove(tmpfqn)
	}
	return
}

// Saves the reader directly to `fqn`, checksums if requested
func SaveReader(fqn string, reader io.Reader, buf []byte, cksumType string, size int64) (cksum *CksumHash, err error) {
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

// a slightly modified excerpt from https://github.com/golang/go/blob/master/src/io/io.go#L407
// - regular streaming copy with `io.WriteTo` and `io.ReaderFrom` not checked and not used
// - buffer _must_ be provided
// - see also: WriterOnly comment (above)
func CopyBuffer(dst io.Writer, src io.Reader, buf []byte) (written int64, err error) {
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if ew != nil {
				if nw > 0 && nw <= nr {
					written += int64(nw)
				}
				err = ew
				break
			}
			if nw < 0 || nw > nr {
				err = errors.New("cos.CopyBuffer: invalid write")
				break
			}
			written += int64(nw)
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, err
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
	b, err := os.ReadFile(filename)
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

// CopyAndChecksum reads from `r` and writes to `w`; returns num bytes copied and checksum, or error
func CopyAndChecksum(w io.Writer, r io.Reader, buf []byte, cksumType string) (n int64, cksum *CksumHash, err error) {
	debug.Assert(w != io.Discard || buf == nil) // io.Discard is io.ReaderFrom

	if cksumType == ChecksumNone || cksumType == "" {
		n, err = io.CopyBuffer(w, r, buf)
		return n, nil, err
	}

	cksum = NewCksumHash(cksumType)
	var mw io.Writer = cksum.H
	if w != io.Discard {
		mw = NewWriterMulti(cksum.H, w)
	}
	n, err = io.CopyBuffer(mw, r, buf)
	cksum.Finalize()
	return n, cksum, err
}

// ChecksumBytes computes checksum of given bytes using additional buffer.
func ChecksumBytes(b []byte, cksumType string) (cksum *Cksum, err error) {
	_, hash, err := CopyAndChecksum(io.Discard, bytes.NewReader(b), nil, cksumType)
	if err != nil {
		return nil, err
	}
	return &hash.Cksum, nil
}

// DrainReader reads and discards all the data from a reader.
// No need for `io.CopyBuffer` as `io.Discard` has efficient `io.ReaderFrom` implementation.
func DrainReader(r io.Reader) {
	_, err := io.Copy(io.Discard, r)
	if err == nil || IsEOF(err) {
		return
	}
	debug.AssertNoErr(err)
}

// FloodWriter writes `n` random bytes to provided writer.
func FloodWriter(w io.Writer, n int64) error {
	_, err := io.CopyN(w, cryptorand.Reader, n)
	return err
}

func Close(closer io.Closer) {
	err := closer.Close()
	debug.AssertNoErr(err)
}

func FlushClose(file *os.File) (err error) {
	err = fflush(file)
	debug.AssertNoErr(err)
	err = file.Close()
	debug.AssertNoErr(err)
	return
}

// NOTE:
// - file.Close() is implementation dependent as far as flushing dirty buffers;
// - journaling filesystems, such as xfs, generally provide better guarantees but, again, not 100%
// - see discussion at https://lwn.net/Articles/788938;
// - going forward, some sort of `rename_barrier()` would be a much better alternative
// - doesn't work in testing environment - currently disabled, see #1141 and comments

const fsyncDisabled = true

func fflush(file *os.File) (err error) {
	if fsyncDisabled {
		return
	}
	return file.Sync()
}
