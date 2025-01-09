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

// instead of os.ReadAll
func ReadAllN(r io.Reader, size int64) (b []byte, err error) {
	if size == 0 {
		debug.Func(func() {
			n, _ := io.Copy(io.Discard, r)
			debug.Assert(n == 0, "expected zero, got ", n)
		})
		return nil, nil
	}
	buf := &bytes.Buffer{}
	if size != ContentLengthUnknown {
		buf = bytes.NewBuffer(make([]byte, 0, size))
	}
	_, err = io.Copy(buf, r)
	debug.Func(func() {
		if err == nil && size != ContentLengthUnknown {
			debug.Assert(buf.Len() == int(size), buf.Len(), " vs ", size)
		}
	})
	return buf.Bytes(), err
}

func ReadAll(r io.Reader) ([]byte, error) {
	buf := &bytes.Buffer{}
	_, err := io.Copy(buf, r)
	return buf.Bytes(), err
}

// including "unexpecting EOF" to accommodate unsized streaming and
// early termination of the other side (prior to sending the first byte)
func IsEOF(err error) bool {
	return err == io.EOF || err == io.ErrUnexpectedEOF ||
		errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF)
}

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
// [NOTE]
// - if the file pathname doesn't exist it'll be created
// - but if it does it'll be also silently truncated
func CreateFile(fqn string) (*os.File, error) {
	if err := CreateDir(filepath.Dir(fqn)); err != nil {
		return nil, err
	}
	return os.OpenFile(fqn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, PermRWR)
}

// (creates destination directory if doesn't exist)
func Rename(src, dst string) error {
	err := os.Rename(src, dst)
	if err == nil {
		return nil
	}
	if !os.IsNotExist(err) {
		return checkMvErr(err, dst)
	}

	// slow path
	err = CreateDir(filepath.Dir(dst))
	if err == nil || os.IsExist(err) /*race*/ {
		err = os.Rename(src, dst)
	}
	if err == nil {
		return nil
	}
	return checkMvErr(err, dst)
}

// RemoveFile removes path; returns nil upon success or if the path does not exist.
func RemoveFile(path string) (err error) {
	err = os.Remove(path)
	if os.IsNotExist(err) {
		err = nil
	}
	return err
}

// and computes checksum, if requested
func CopyFile(src, dst string, buf []byte, cksumType string) (written int64, cksum *CksumHash, err error) {
	const tag = "copy-file:"
	var srcfh, dstfh *os.File
	if srcfh, err = os.Open(src); err != nil {
		return 0, nil, err
	}
	if dstfh, err = CreateFile(dst); err != nil {
		nlog.Errorln(tag, err)
		Close(srcfh)
		return 0, nil, err
	}
	written, cksum, err = CopyAndChecksum(dstfh, srcfh, buf, cksumType)
	Close(srcfh)
	if err != nil {
		Close(dstfh)
		goto eret
	}
	if err = FlushClose(dstfh); err != nil {
		dstfh.Close()
		goto eret
	}
	return written, cksum, err

eret:
	nlog.Errorln(tag, err)
	if nerr := RemoveFile(dst); nerr != nil {
		nlog.Errorln(tag, "nested err: [", nerr, "]")
	}
	return 0, nil, err
}

// Saves the `reader` directly to `fqn`, checksums if requested
func SaveReader(fqn string, reader io.Reader, buf []byte, cksumType string, size int64) (*CksumHash, error) {
	wfh, erc := CreateFile(fqn)
	if erc != nil {
		return nil, erc
	}
	cksum, err := _save(fqn, WriterOnly{wfh} /*hide ReadFrom*/, reader, buf, cksumType, size)
	erc = wfh.Close()

	if err == nil && erc != nil {
		err = fmt.Errorf("failed to close %s: %w", fqn, erc)
	}
	if err != nil {
		os.Remove(fqn) // cleanup
	}
	return cksum, err
}

func _save(fqn string, w io.Writer, r io.Reader, buf []byte, cksumType string, size int64) (*CksumHash, error) {
	if size >= 0 {
		r = io.LimitReader(r, size)
	}
	written, cksum, err := CopyAndChecksum(w, r, buf, cksumType)
	if err != nil {
		return nil, fmt.Errorf("failed to copy-and-checksum to %s: %w", fqn, err)
	}
	if size >= 0 && written != size {
		err = fmt.Errorf("wrong size %s: expected %d, got %d", fqn, size, written) // (unlikely)
	}
	return cksum, err
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
	io.Copy(io.Discard, r)
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
