// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// References:
// * https://en.wikipedia.org/wiki/List_of_file_signatures
// * https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types

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

	detect struct {
		offset int
		sig    []byte
		mime   string // '.' + IANA mime
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

/////////////////////////
// GET OBJECT: archive //
/////////////////////////

func (goi *getObjInfo) freadArch(file *os.File) (cos.ReadCloseSizer, error) {
	mime, err := goi.mime(file)
	if err != nil {
		return nil, err
	}
	archname := filepath.Join(goi.lom.Bck().Name, goi.lom.ObjName)
	filename := goi.archive.filename
	switch mime {
	case cos.ExtTar:
		return freadTar(file, filename, archname)
	case cos.ExtTarTgz, cos.ExtTgz:
		return freadTgz(file, filename, archname)
	case cos.ExtZip:
		return freadZip(file, filename, archname, goi.lom.SizeBytes())
	default:
		debug.Assert(false)
		return nil, cos.NewUnknownMimeError(mime)
	}
}

func (goi *getObjInfo) mime(file *os.File) (m string, err error) {
	// either ok or non-empty user-defined mime type (that must work)
	if m, err = cos.Mime(goi.archive.mime, goi.lom.ObjName); err == nil || goi.archive.mime != "" {
		return
	}
	// otherwise, by magic
	var (
		buf, slab = goi.t.smm.AllocSize(sizeDetectMime)
		n         int
	)
	n, err = file.Read(buf)
	for _, magic := range allMagics {
		if n > magic.offset && bytes.HasPrefix(buf[magic.offset:], magic.sig) {
			m = magic.mime
			break
		}
	}
	if m == "" {
		if err == nil {
			err = cos.NewUnknownMimeError(goi.lom.ObjName)
		} else {
			err = cos.NewUnknownMimeError(err.Error())
		}
	} else {
		err = nil
	}
	if n > 0 {
		file.Seek(0, io.SeekStart)
	}
	slab.Free(buf)
	return
}

func freadTar(reader io.Reader, filename, archname string) (*cslLimited, error) {
	tr := tar.NewReader(reader)
	for {
		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				err = notFoundInArch(filename, archname)
			}
			return nil, err
		}
		if hdr.Name == filename || archNamesEq(hdr.Name, filename) {
			return &cslLimited{LimitedReader: io.LimitedReader{R: reader, N: hdr.Size}}, nil
		}
	}
}

func freadTgz(reader io.Reader, filename, archname string) (csc *cslClose, err error) {
	var (
		gzr *gzip.Reader
		csl *cslLimited
	)
	if gzr, err = gzip.NewReader(reader); err != nil {
		return
	}
	if csl, err = freadTar(gzr, filename, archname); err != nil {
		return
	}
	csc = &cslClose{gzr: gzr /*to close*/, R: csl /*to read from*/, N: csl.N /*size*/}
	return
}

func freadZip(readerAt cos.ReadReaderAt, filename, archname string, size int64) (csf *cslFile, err error) {
	var zr *zip.Reader
	if zr, err = zip.NewReader(readerAt, size); err != nil {
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
