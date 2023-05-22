// Package archive
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
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
)

// standard file signatures
var (
	magicTar  = detect{offset: 257, sig: []byte("ustar"), mime: ExtTar}
	magicGzip = detect{sig: []byte{0x1f, 0x8b}, mime: ExtTarTgz}
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

func normalize(mime string) (string, error) {
	debug.Assert(mime != "", "mime empty")
	if strings.Contains(mime, ExtTarTgz[1:]) { // ExtTarTgz contains ExtTar
		return ExtTarTgz, nil
	}
	for _, ext := range FileExtensions {
		if strings.Contains(mime, ext[1:]) {
			return ext, nil
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
	return "", NewErrUnknownFileExt(filename)
}

const sizeDetectMime = 512

// NOTE convention: caller may pass nil `smm` _not_ to spend time (usage: listing and reading)
func MimeFile(file *os.File, smm *memsys.MMSA, mime, archname string) (m string, err error) {
	m, err = Mime(mime, archname)
	if err == nil || IsErrUnknownMime(err) {
		return
	}
	if smm == nil {
		err = NewErrUnknownFileExt(archname + " (not reading file magic)")
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
		err = NewErrUnknownFileExt(archname + " (not reading file magic)")
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
		err = NewErrUnknownFileExt(archname + " (file too short)")
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
