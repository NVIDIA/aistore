// Package archive
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package archive

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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

//
// assorted 'limited' readers
//

func (csl *cslLimited) Size() int64 { return csl.N }
func (*cslLimited) Close() error    { return nil }

func (csc *cslClose) Read(b []byte) (int, error) { return csc.R.Read(b) }
func (csc *cslClose) Size() int64                { return csc.N }
func (csc *cslClose) Close() error               { return csc.gzr.Close() }

func (csf *cslFile) Read(b []byte) (int, error) { return csf.file.Read(b) }
func (csf *cslFile) Size() int64                { return csf.size }
func (csf *cslFile) Close() error               { return csf.file.Close() }

//
// next() to the spec-ed `filename` and return the corresponding limited reader (LR)
//

func GetReader(fh *os.File, archname, filename, mime string, size int64) (cos.ReadCloseSizer, error) {
	switch mime {
	case ExtTar:
		return tarLR(fh, filename, archname)
	case ExtTarTgz, ExtTgz:
		return tgzLR(fh, filename, archname)
	case ExtZip:
		return zipLR(fh, filename, archname, size)
	default: // unlikely
		err := NewErrUnknownMime(mime)
		debug.AssertNoErr(err)
		return nil, err
	}
}

func tarLR(fh io.Reader, filename, archname string) (*cslLimited, error) {
	tr := tar.NewReader(fh)
	for {
		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				err = notFound(filename, archname)
			}
			return nil, err
		}
		if hdr.Name == filename || namesEq(hdr.Name, filename) {
			return &cslLimited{LimitedReader: io.LimitedReader{R: fh, N: hdr.Size}}, nil // Ok
		}
	}
}

func tgzLR(fh *os.File, filename, archname string) (csc *cslClose, err error) {
	var (
		gzr *gzip.Reader
		csl *cslLimited
	)
	if gzr, err = gzip.NewReader(fh); err != nil {
		return
	}
	if csl, err = tarLR(gzr, filename, archname); err != nil {
		return
	}
	csc = &cslClose{gzr: gzr /*to close*/, R: csl /*to read from*/, N: csl.N /*size*/}
	return
}

func zipLR(fh *os.File, filename, archname string, size int64) (csf *cslFile, err error) {
	var zr *zip.Reader
	if zr, err = zip.NewReader(fh, size); err != nil {
		return
	}
	for _, f := range zr.File {
		finfo := f.FileInfo()
		if finfo.IsDir() {
			continue
		}
		if f.FileHeader.Name == filename || namesEq(f.FileHeader.Name, filename) {
			csf = &cslFile{size: finfo.Size()}
			csf.file, err = f.Open()
			return
		}
	}
	err = notFound(filename, archname)
	return
}

//
// helpers
//

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

func notFound(filename, archname string) error {
	return cos.NewErrNotFound("%q in archive %q", filename, archname)
}
