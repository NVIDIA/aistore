// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"strings"
)

// supported archive types (file extensions)
const (
	ExtTar    = ".tar"
	ExtTgz    = ".tgz"
	ExtTarTgz = ".tar.gz"
	ExtZip    = ".zip"

	TarBlockSize = 512 // Size of each block in a tar stream
)

var ArchExtensions = []string{ExtTar, ExtTgz, ExtTarTgz, ExtZip}

func IsGzipped(filename string) bool {
	return strings.HasSuffix(filename, ExtTgz) || strings.HasSuffix(filename, ExtTarTgz)
}

type (
	ErrUnknownMime struct {
		detail string
	}
)

func (e *ErrUnknownMime) Error() string            { return "unknown mime type \"" + e.detail + "\"" }
func NewUnknownMimeError(d string) *ErrUnknownMime { return &ErrUnknownMime{d} }

// Map user-specified mime type OR the filename's extension to one of the supported ArchExtensions
func Mime(mime, filename string) (ext string, err error) {
	// user-specified (intended) format takes precedence
	if mime != "" {
		if strings.Contains(mime, ExtTarTgz[1:]) { // ExtTarTgz contains ExtTar
			return ExtTarTgz, nil
		}
		for _, ext := range ArchExtensions {
			if strings.Contains(mime, ext[1:]) {
				return ext, nil
			}
		}
		err = NewUnknownMimeError(mime)
		return
	}
	// otherwise, by extension
	for _, ext := range ArchExtensions {
		if strings.HasSuffix(filename, ext) {
			return ext, nil
		}
	}
	err = NewUnknownMimeError(filename)
	return
}

// OpenTarForAppend opens a TAR and uses tar's reader Next() to skip
// to the position right _after_ the last file in the TAR
// (padding bytes including).
//
// Background:
//    TAR file is padded with one or more 512-byte blocks of zero bytes.
//    The blocks must be overwritten, otherwise newly added files won't be
//    accessible. Different TAR formats (such as `ustar`, `pax` and `GNU`)
//    write different number of zero blocks.
func OpenTarForAppend(objName, workFQN string) (*os.File, error) {
	fh, err := os.OpenFile(workFQN, os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	err = seekTarEnd(objName, fh)
	if err != nil {
		fh.Close()
	}
	return fh, err
}

func seekTarEnd(objName string, fh *os.File) error {
	var pos, size int64
	twr := tar.NewReader(fh)
	for {
		st, err := twr.Next()
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
		pos, _ = fh.Seek(0, io.SeekCurrent)
		size = st.Size
	}
	if pos == 0 {
		return fmt.Errorf("%s: could not detect the end of archive", objName)
	}
	padded := CeilAlignInt64(size, TarBlockSize)
	_, err := fh.Seek(pos+padded, io.SeekStart)
	return err
}
