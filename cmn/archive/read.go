// Package archive: write, read, copy, append, list primitives
// across all supported formats
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package archive

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"io"
	"path/filepath"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/pierrec/lz4/v3"
)

// usage boils down to a) constructing (`NewReader`) and b) iterating (`Range`) - that's all
// (all supported formats)
type (
	ReadCB func(filename string, reader cos.ReadCloseSizer, hdr any) (bool /*stop*/, error)

	Reader interface {
		// call rcb (reader callback) with each archived file; stop upon EOF
		// or when rcb returns true (ie., stop) or any error
		ReadUntil(rcb ReadCB) error

		// simple/single selection of a given archived filename (full path)
		ReadOne(filename string) (cos.ReadCloseSizer, error)

		// private
		init(fh io.Reader) error
	}
)

// private implementation
type (
	baseR struct {
		fh io.Reader
	}
	tarReader struct {
		baseR
		tr *tar.Reader
	}
	tgzReader struct {
		tr  tarReader
		gzr *gzip.Reader
	}
	zipReader struct {
		baseR
		size int64
		zr   *zip.Reader
	}
	lz4Reader struct {
		tr  tarReader
		lzr *lz4.Reader
	}
)

// interface guard
var (
	_ Reader = (*tarReader)(nil)
	_ Reader = (*tgzReader)(nil)
	_ Reader = (*zipReader)(nil)
	_ Reader = (*lz4Reader)(nil)
)

func NewReader(mime string, fh io.Reader, size ...int64) (ar Reader, err error) {
	switch mime {
	case ExtTar:
		ar = &tarReader{}
	case ExtTgz, ExtTarGz:
		ar = &tgzReader{}
	case ExtZip:
		debug.Assert(len(size) > 0 && size[0] > 0, "size required")
		ar = &zipReader{size: size[0]}
	case ExtTarLz4:
		ar = &lz4Reader{}
	default:
		debug.Assert(false, mime)
	}
	err = ar.init(fh)
	return
}

// baseR

func (br *baseR) init(fh io.Reader) { br.fh = fh }

// tarReader

func (tr *tarReader) init(fh io.Reader) error {
	tr.baseR.init(fh)
	tr.tr = tar.NewReader(fh)
	return nil
}

func (tr *tarReader) ReadUntil(rcb ReadCB) error {
	for {
		hdr, err := tr.tr.Next()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}
		csl := &cslLimited{LimitedReader: io.LimitedReader{R: tr.tr, N: hdr.Size}}
		if stop, err := rcb(hdr.Name, csl, hdr); stop || err != nil {
			return err
		}
	}
}

func (tr *tarReader) ReadOne(filename string) (cos.ReadCloseSizer, error) {
	debug.Assert(filename != "", "missing archived filename (pathname)")
	for {
		hdr, err := tr.tr.Next()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return nil, err
		}
		if hdr.Name == filename || namesEq(hdr.Name, filename) {
			return &cslLimited{LimitedReader: io.LimitedReader{R: tr.tr, N: hdr.Size}}, nil
		}
	}
}

// tgzReader

func (tgr *tgzReader) init(fh io.Reader) (err error) {
	tgr.gzr, err = gzip.NewReader(fh)
	if err != nil {
		return
	}
	tgr.tr.baseR.init(tgr.gzr)
	tgr.tr.tr = tar.NewReader(tgr.gzr)
	return
}

func (tgr *tgzReader) ReadUntil(rcb ReadCB) (err error) {
	err = tgr.tr.ReadUntil(rcb)
	erc := tgr.gzr.Close()
	if err == nil {
		err = erc
	}
	return err
}

// here and elsewhere, `filename` indicates extraction of a single named archived file
func (tgr *tgzReader) ReadOne(filename string) (cos.ReadCloseSizer, error) {
	reader, err := tgr.tr.ReadOne(filename)
	if err != nil {
		tgr.gzr.Close()
		return reader, err
	}
	if reader != nil {
		// when the method returns non-nil reader it is the responsibility of the caller to close the former
		// otherwise, gzip.Reader is always closed upon return
		csc := &cslClose{gzr: tgr.gzr /*to close*/, R: reader /*to read from*/, N: reader.Size()}
		return csc, err
	}
	return nil, tgr.gzr.Close()
}

// zipReader

func (zr *zipReader) init(fh io.Reader) (err error) {
	readerAt, ok := fh.(io.ReaderAt)
	debug.Assert(ok, "expecting io.ReaderAt")
	zr.baseR.init(fh)
	zr.zr, err = zip.NewReader(readerAt, zr.size)
	return
}

func (zr *zipReader) ReadUntil(rcb ReadCB) (err error) {
	for _, f := range zr.zr.File {
		finfo := f.FileInfo()
		if finfo.IsDir() {
			continue
		}
		debug.Assertf(finfo.Size() == int64(f.FileHeader.UncompressedSize64),
			"%d vs %d", finfo.Size(), f.FileHeader.UncompressedSize64)

		csf := &cslFile{size: int64(f.FileHeader.UncompressedSize64)}
		if csf.file, err = f.Open(); err != nil {
			return err
		}
		if stop, err := rcb(f.FileHeader.Name, csf, &f.FileHeader); stop || err != nil {
			return err
		}
	}
	return
}

func (zr *zipReader) ReadOne(filename string) (reader cos.ReadCloseSizer, err error) {
	debug.Assert(filename != "", "missing archived filename (pathname)")
	for _, f := range zr.zr.File {
		finfo := f.FileInfo()
		if finfo.IsDir() {
			continue
		}
		debug.Assertf(finfo.Size() == int64(f.FileHeader.UncompressedSize64),
			"%d vs %d", finfo.Size(), f.FileHeader.UncompressedSize64)

		if f.FileHeader.Name == filename || namesEq(f.FileHeader.Name, filename) {
			csf := &cslFile{size: finfo.Size()}
			csf.file, err = f.Open()
			return csf, err
		}
	}
	return nil, nil
}

// lz4Reader

func (lzr *lz4Reader) init(fh io.Reader) error {
	lzr.lzr = lz4.NewReader(fh)
	lzr.tr.baseR.init(lzr.lzr)
	lzr.tr.tr = tar.NewReader(lzr.lzr)
	return nil
}

func (lzr *lz4Reader) ReadUntil(rcb ReadCB) error {
	return lzr.tr.ReadUntil(rcb)
}

func (lzr *lz4Reader) ReadOne(filename string) (cos.ReadCloseSizer, error) {
	return lzr.tr.ReadOne(filename)
}

//
// more limited readers
//

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
)

//
// assorted 'limited' readers
//

func (csl *cslLimited) Size() int64 { return csl.N }
func (*cslLimited) Close() error    { return nil } // NopCloser, unlike the other two (below)

func (csc *cslClose) Read(b []byte) (int, error) { return csc.R.Read(b) }
func (csc *cslClose) Size() int64                { return csc.N }
func (csc *cslClose) Close() error               { return csc.gzr.Close() }

func (csf *cslFile) Read(b []byte) (int, error) { return csf.file.Read(b) }
func (csf *cslFile) Size() int64                { return csf.size }
func (csf *cslFile) Close() error               { return csf.file.Close() }

// in re `--absolute-names` (simplified)
func namesEq(n1, n2 string) bool {
	if n1[0] == filepath.Separator {
		n1 = n1[1:]
	}
	if n2[0] == filepath.Separator {
		n2 = n2[1:]
	}
	return n1 == n2
}
