// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/vmihailenco/msgpack"
)

const (
	sizeDetectMime = 512
)

type (
	cslLimited struct {
		io.LimitedReader
	}
	cslClose struct {
		gzr io.ReadCloser
		R   io.Reader
		N   int64
	}
	cslFile struct {
		file io.ReadCloser
		size int64
	}

	// References:
	// * https://en.wikipedia.org/wiki/List_of_file_signatures
	// * https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types
	detect struct {
		mime   string // '.' + IANA mime
		sig    []byte
		offset int
	}
)

var (
	magicTar  = detect{offset: 257, sig: []byte("ustar"), mime: cos.ExtTar}
	magicGzip = detect{sig: []byte{0x1f, 0x8b}, mime: cos.ExtTarTgz}
	magicZip  = detect{sig: []byte{0x50, 0x4b}, mime: cos.ExtZip}

	allMagics = []detect{magicTar, magicGzip, magicZip} // NOTE: must contain all
)

func (csl *cslLimited) Size() int64 { return csl.N }
func (*cslLimited) Close() error    { return nil }

func (csc *cslClose) Read(b []byte) (int, error) { return csc.R.Read(b) }
func (csc *cslClose) Size() int64                { return csc.N }
func (csc *cslClose) Close() error               { return csc.gzr.Close() }

func (csf *cslFile) Read(b []byte) (int, error) { return csf.file.Read(b) }
func (csf *cslFile) Size() int64                { return csf.size }
func (csf *cslFile) Close() error               { return csf.file.Close() }

func notFoundInArch(filename, archname string) error {
	return cmn.NewErrNotFound("file %q in archive %q", filename, archname)
}

//
// next() to `filename` and return a limited reader
// to read the corresponding file from archive
//

func freadTar(lreader io.Reader, filename, archname string) (*cslLimited, error) {
	tr := tar.NewReader(lreader)
	for {
		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				err = notFoundInArch(filename, archname)
			}
			return nil, err
		}
		if hdr.Name == filename || archNamesEq(hdr.Name, filename) {
			return &cslLimited{LimitedReader: io.LimitedReader{R: lreader, N: hdr.Size}}, nil
		}
	}
}

func freadTgz(lreader io.Reader, filename, archname string) (csc *cslClose, err error) {
	var (
		gzr *gzip.Reader
		csl *cslLimited
	)
	if gzr, err = gzip.NewReader(lreader); err != nil {
		return
	}
	if csl, err = freadTar(gzr, filename, archname); err != nil {
		return
	}
	csc = &cslClose{gzr: gzr /*to close*/, R: csl /*to read from*/, N: csl.N /*size*/}
	return
}

func freadZip(lreaderAt cos.ReadReaderAt, filename, archname string, size int64) (csf *cslFile, err error) {
	var zr *zip.Reader
	if zr, err = zip.NewReader(lreaderAt, size); err != nil {
		return
	}
	for _, f := range zr.File {
		finfo := f.FileInfo()
		if finfo.IsDir() {
			continue
		}
		if f.FileHeader.Name == filename || archNamesEq(f.FileHeader.Name, filename) {
			csf = &cslFile{size: finfo.Size()}
			csf.file, err = f.Open()
			return
		}
	}
	err = notFoundInArch(filename, archname)
	return
}

func freadMsgpack(lreaderAt cos.ReadReaderAt, filename, archname string) (csf *cslFile, err error) {
	var (
		dst any
		dec = msgpack.NewDecoder(lreaderAt)
	)
	if err = dec.Decode(&dst); err != nil {
		return
	}
	out, ok := dst.(map[string]any)
	if !ok {
		debug.FailTypeCast(dst)
		return nil, fmt.Errorf("unexpected type (%T)", dst)
	}
	v, ok := out[filename]
	if !ok {
		var name string
		for name, v = range out {
			if archNamesEq(name, filename) {
				goto found
			}
		}
		return nil, notFoundInArch(filename, archname)
	}
found:
	vout := v.([]byte)
	csf = &cslFile{size: int64(len(vout))}
	csf.file = io.NopCloser(bytes.NewReader(vout))
	return
}

// NOTE: in re `--absolute-names` (simplified)
func archNamesEq(n1, n2 string) bool {
	if n1[0] == filepath.Separator {
		n1 = n1[1:]
	}
	if n2[0] == filepath.Separator {
		n2 = n2[1:]
	}
	return n1 == n2
}

//
// target Mime utils (see also cmn/cos/archive.go)
//

func mimeByMagic(buf []byte, n int, magic detect, zerosOk bool) (ok bool) {
	if n < sizeDetectMime {
		return
	}
	if zerosOk {
		ok = true
		for i := 0; i < sizeDetectMime; i++ {
			if buf[i] != 0 {
				ok = false
				break
			}
		}
	}
	if !ok {
		// finally, compare signature
		ok = n > magic.offset && bytes.HasPrefix(buf[magic.offset:], magic.sig)
	}
	return
}

// a superset of cos.Mime (adds magic)
func mimeFile(file *os.File, smm *memsys.MMSA, mime, filename string) (m string, err error) {
	// simple first
	if mime != "" {
		// user-defined goi.archive.mime (apc.QparamArchmime)
		// means there will be no attempting to detect signature
		return cos.ByMime(mime)
	}
	if m, err = cos.MimeByExt(filename); err == nil {
		return
	}
	// otherwise, by magic
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
		return "", cos.NewUnknownMimeError(filename + " is too short")
	}
	for _, magic := range allMagics {
		if mimeByMagic(buf, n, magic, false) {
			m = magic.mime
			break
		}
	}
	_, err = file.Seek(0, io.SeekStart)
	debug.AssertNoErr(err)
	slab.Free(buf)
	return
}

// _may_ open file and call the above
func mimeFQN(smm *memsys.MMSA, mime, fqn string) (_ string, err error) {
	if mime != "" {
		// user-defined mime (apc.QparamArchmime)
		return cos.ByMime(mime)
	}
	mime, err = cos.MimeByExt(fqn)
	if err == nil {
		return mime, nil
	}
	fh, err := os.Open(fqn)
	if err != nil {
		return "", err
	}
	mime, err = mimeFile(fh, smm, mime, fqn)
	cos.Close(fh)
	return mime, err
}

//
// copy archive one file at a time, and APPEND new `reader` at the end
//

func apndTgz(lomr io.Reader, tw *tar.Writer /*over gzw*/, nhdr *tar.Header, nr io.Reader, buf []byte) (err error) {
	var gzr *gzip.Reader
	if gzr, err = gzip.NewReader(lomr); err != nil {
		return
	}
	tr := tar.NewReader(gzr)
	for {
		var hdr *tar.Header
		hdr, err = tr.Next()
		if err != nil {
			break
		}
		// copy one
		csl := &cslLimited{LimitedReader: io.LimitedReader{R: tr, N: hdr.Size}}
		if err = tw.WriteHeader(hdr); err == nil {
			_, err = io.CopyBuffer(tw, csl, buf)
		}
		if err != nil {
			break
		}
	}
	if err == io.EOF {
		err = tw.WriteHeader(nhdr)
	}
	if err == nil {
		_, err = io.CopyBuffer(tw, nr, buf)
	}
	cos.Close(gzr)
	return
}
