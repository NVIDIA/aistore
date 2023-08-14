// Package archive: write, read, copy, append, list primitives
// across all supported formats
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package archive

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/memsys"
)

// supported archive types (file extensions); see also archExts in cmd/cli/cli/const.go
// NOTE: when adding/removing formats - update:
//   - FileExtensions
//   - allMagics
//   - ext/dsort/shard/rw.go
const (
	ExtTar    = ".tar"
	ExtTgz    = ".tgz"
	ExtTarGz  = ".tar.gz"
	ExtZip    = ".zip"
	ExtTarLz4 = ".tar.lz4"
)

const (
	sizeDetectMime = 512
)

// - here and elsewhere, mime (string) is a "." + IANA mime
// - for standard MIME types, see: cmn/cos/http_headers.go
// - references:
//   * https://en.wikipedia.org/wiki/List_of_file_signatures
//   * https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types

type detect struct {
	mime   string // '.' + IANA mime
	sig    []byte
	offset int
}

var FileExtensions = []string{ExtTar, ExtTgz, ExtTarGz, ExtZip, ExtTarLz4}

// standard file signatures
var (
	magicTar  = detect{offset: 257, sig: []byte("ustar"), mime: ExtTar}
	magicGzip = detect{sig: []byte{0x1f, 0x8b}, mime: ExtTarGz}
	magicZip  = detect{sig: []byte{0x50, 0x4b}, mime: ExtZip}
	magicLz4  = detect{sig: []byte{0x04, 0x22, 0x4d, 0x18}, mime: ExtTarLz4}

	allMagics = []detect{magicTar, magicGzip, magicZip, magicLz4} // NOTE: must contain all
)

// motivation: prevent from creating archives with non-standard extensions
func Strict(mime, filename string) (m string, err error) {
	if mime != "" {
		if m, err = normalize(mime); err != nil {
			return
		}
	}
	m, err = byExt(filename)
	if err != nil || mime == "" {
		return
	}
	if mime != m {
		// user-defined (non-empty) MIME must correspond
		err = fmt.Errorf("mime mismatch %q vs %q", mime, m)
	}
	return
}

func Mime(mime, filename string) (string, error) {
	if mime != "" {
		return normalize(mime)
	}
	return byExt(filename)
}

// e.g. MIME: "application/zip"
func normalize(mime string) (string, error) {
	switch {
	case strings.Contains(mime, ExtTarGz[1:]): // ExtTarGz contains ExtTar
		return ExtTarGz, nil
	case strings.Contains(mime, ExtTarLz4[1:]): // ditto
		return ExtTarLz4, nil
	default:
		for _, ext := range FileExtensions {
			if strings.Contains(mime, ext[1:]) {
				return ext, nil
			}
		}
	}
	return "", NewErrUnknownMime(mime)
}

// by filename extension
func byExt(filename string) (string, error) {
	for _, ext := range FileExtensions {
		if strings.HasSuffix(filename, ext) {
			return ext, nil
		}
	}
	return "", NewErrUnknownFileExt(filename, "")
}

// NOTE convention: caller may pass nil `smm` _not_ to spend time (usage: listing and reading)
func MimeFile(file *os.File, smm *memsys.MMSA, mime, archname string) (m string, err error) {
	m, err = Mime(mime, archname)
	if err == nil || IsErrUnknownMime(err) {
		return
	}
	if smm == nil {
		err = NewErrUnknownFileExt(archname, "not reading file magic")
		return
	}
	// by magic
	var (
		n         int
		buf, slab = smm.AllocSize(sizeDetectMime)
	)
	m, n, err = _detect(file, archname, buf)
	if n > 0 {
		_, err = file.Seek(0, io.SeekStart)
	}
	slab.Free(buf)
	return
}

// NOTE:
// - on purpose redundant vs the above - not to open file if can be avoided
// - convention: caller may pass nil `smm` _not_ to spend time (usage: listing and reading)
func MimeFQN(smm *memsys.MMSA, mime, archname string) (m string, err error) {
	m, err = Mime(mime, archname)
	if err == nil || IsErrUnknownMime(err) {
		return
	}
	if smm == nil {
		err = NewErrUnknownFileExt(archname, "not reading file magic")
		return
	}
	fh, err := os.Open(archname)
	if err != nil {
		return "", err
	}
	buf, slab := smm.AllocSize(sizeDetectMime)
	m, _, err = _detect(fh, archname, buf)
	slab.Free(buf)
	cos.Close(fh)
	return
}

func _detect(file *os.File, archname string, buf []byte) (m string, n int, err error) {
	n, err = file.Read(buf)
	if err != nil {
		return
	}
	if n < sizeDetectMime {
		err = NewErrUnknownFileExt(archname, "file is too short")
		return
	}
	for _, magic := range allMagics {
		if n > magic.offset && bytes.HasPrefix(buf[magic.offset:], magic.sig) {
			m = magic.mime
			return // ok
		}
	}
	err = fmt.Errorf("failed to detect file signature in %q", archname)
	return
}

func EqExt(ext1, ext2 string) bool {
	switch {
	case ext1 == ext2:
		return true
	case ext1 == ExtTarGz && ext2 == ExtTgz:
		return true
	case ext2 == ExtTarGz && ext1 == ExtTgz:
		return true
	}
	return false
}
