// Package archive
/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
 */
package archive

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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
	magicTar  = detect{offset: 257, sig: []byte("ustar"), mime: ExtTar}
	magicGzip = detect{sig: []byte{0x1f, 0x8b}, mime: ExtTarTgz}
	magicZip  = detect{sig: []byte{0x50, 0x4b}, mime: ExtZip}

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
	return cos.NewErrNotFound("file %q in archive %q", filename, archname)
}

//
// next() to `filename` and return a limited reader
// to read the corresponding file from archive
//

func Read(file *os.File, archname, filename, mime string, size int64) (cos.ReadCloseSizer, error) {
	switch mime {
	case ExtTar:
		return readTar(file, filename, archname)
	case ExtTarTgz, ExtTgz:
		return readTgz(file, filename, archname)
	case ExtZip:
		return readZip(file, filename, archname, size)
	case ExtMsgpack:
		return readMsgpack(file, filename, archname)
	default:
		debug.Assert(false)
		return nil, NewUnknownMimeError(mime)
	}
}

func readTar(lreader io.Reader, filename, archname string) (*cslLimited, error) {
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

func readTgz(lreader io.Reader, filename, archname string) (csc *cslClose, err error) {
	var (
		gzr *gzip.Reader
		csl *cslLimited
	)
	if gzr, err = gzip.NewReader(lreader); err != nil {
		return
	}
	if csl, err = readTar(gzr, filename, archname); err != nil {
		return
	}
	csc = &cslClose{gzr: gzr /*to close*/, R: csl /*to read from*/, N: csl.N /*size*/}
	return
}

func readZip(lreaderAt cos.ReadReaderAt, filename, archname string, size int64) (csf *cslFile, err error) {
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

func readMsgpack(lreaderAt cos.ReadReaderAt, filename, archname string) (csf *cslFile, err error) {
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
