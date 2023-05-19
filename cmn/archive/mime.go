// Package archive
/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
 */
package archive

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
)

// compare w/ ais/archive.go `mimeFQN` and friends
func Mime(mime, filename string) (string, error) {
	if mime != "" {
		return ByMime(mime)
	}
	return MimeByExt(filename)
}

// user-specified (intended) format always takes precedence
// compare w/ ais/archive.go `mimeFQN`
func ByMime(mime string) (ext string, err error) {
	debug.Assert(mime != "", mime)
	if strings.Contains(mime, ExtTarTgz[1:]) { // ExtTarTgz contains ExtTar
		return ExtTarTgz, nil
	}
	for _, ext := range ArchExtensions {
		if strings.Contains(mime, ext[1:]) {
			return ext, nil
		}
	}
	return "", NewUnknownMimeError(mime)
}

// by filename extension
func MimeByExt(filename string) (ext string, err error) {
	for _, ext := range ArchExtensions {
		if strings.HasSuffix(filename, ext) {
			return ext, nil
		}
	}
	err = NewUnknownMimeError(filename)
	return
}

// Exists for all ais-created/appended TARs - common code to set auxiliary bits in a header
// NOTE:
// - currently, not using os.Getuid/gid (or user.Current) to set Uid/Gid, and
// - not calling standard tar.FileInfoHeader(finfo-of-the-file-to-archive) as well
// - see also: /usr/local/go/src/archive/tar/common.go
func SetAuxTarHeader(hdr *tar.Header) {
	hdr.Mode = int64(cos.PermRWRR)
}

func mimeByMagic(buf []byte, n int, magic detect) (ok bool) {
	if n < sizeDetectMime {
		return
	}
	if !ok {
		// finally, compare signature
		ok = n > magic.offset && bytes.HasPrefix(buf[magic.offset:], magic.sig)
	}
	return
}

// a superset of Mime above
func MimeFile(file *os.File, smm *memsys.MMSA, mime, filename string) (m string, err error) {
	// simple first
	if mime != "" {
		// user-defined goi.archive.mime (apc.QparamArchmime)
		// means there will be no attempting to detect signature
		return ByMime(mime)
	}
	if m, err = MimeByExt(filename); err == nil {
		return
	}
	// otherwise, by magic
	return _detect(file, smm, filename)
}

// MAY open file and read the signature
func MimeFQN(smm *memsys.MMSA, mime, fqn string) (_ string, err error) {
	if mime != "" {
		// user-defined mime (apc.QparamArchmime)
		return ByMime(mime)
	}
	mime, err = MimeByExt(fqn)
	if err == nil {
		return mime, nil
	}
	fh, err := os.Open(fqn)
	if err != nil {
		return "", err
	}
	mime, err = _detect(fh, smm, fqn)
	cos.Close(fh)
	return mime, err
}

func _detect(file *os.File, smm *memsys.MMSA, filename string) (m string, err error) {
	var (
		buf, slab = smm.AllocSize(sizeDetectMime)
		n         int
	)
	n, err = file.Read(buf)
	if err != nil {
		return
	}
	if n < sizeDetectMime {
		file.Seek(0, io.SeekStart)
		return "", NewUnknownMimeError(filename + " is too short")
	}
	for _, magic := range allMagics {
		if mimeByMagic(buf, n, magic) {
			m = magic.mime
			break
		}
	}
	_, err = file.Seek(0, io.SeekStart)
	debug.AssertNoErr(err)
	slab.Free(buf)
	return
}
