// Package archive: write, read, copy, append, list primitives
// across all supported formats
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package archive

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"

	"github.com/pierrec/lz4/v4"
)

const (
	_regexp = iota // default (and slow)
	_prefix
	_suffix
	_substr
	_wdskey
)

var MatchMode = [...]string{
	"regexp",
	"prefix",
	"suffix",
	"substr",
	"wdskey", // WebDataset convention - pathname without extension (https://github.com/webdataset/webdataset#the-webdataset-format)
}

// to use, construct (`NewReader`) and iterate (`RangeUntil`)
// (all supported formats)
// simple/single selection is also supported (`ReadOne`)
type (
	ArchRCB interface {
		Call(filename string, reader cos.ReadCloseSizer, hdr any) (bool /*stop*/, error)
	}

	Reader interface {
		// - call rcb (reader's callback) with each matching archived file, where:
		//   - `regex` is the matching string that gets interpreted according
		//      to one of the enumerated "matching modes" (see MatchMode);
		//   - an empty `regex` is just another case of cos.EmptyMatchAll - i.e., matches all archived files
		// - stop upon EOF, or when rcb returns true (ie., stop) or any error
		ReadUntil(rcb ArchRCB, regex, mmode string) error

		// simple/single selection of a given archived filename (full path)
		ReadOne(filename string) (cos.ReadCloseSizer, error)

		// private
		init(fh io.Reader) error
	}
)

type ErrMatchMode struct{ mmode string }

// private
type (
	matcher struct {
		re    *regexp.Regexp // when (and if) compiled
		regex string
		mmode string
	}
)

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
		zr   *zip.Reader
		size int64
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

// matcher

func (m *matcher) init() (err error) {
	// making exception for match-all("", "*")
	if cos.MatchAll(m.regex) {
		m.mmode = MatchMode[_prefix]
	}
	switch m.mmode {
	case MatchMode[_regexp]:
		debug.Assert(!cos.MatchAll(m.regex)) // match-all("", "*") must use mmode == prefix
		m.re, err = regexp.Compile(m.regex)
	case MatchMode[_prefix], MatchMode[_suffix], MatchMode[_substr], MatchMode[_wdskey]:
		// do nothing
	default:
		err = &ErrMatchMode{m.mmode}
	}
	return err
}

func (m *matcher) do(filename string) bool {
	if m.regex == "" { // empty regex matches all archived filenames
		return true
	}
	if m.re != nil {
		return m.re.MatchString(filename)
	}
	switch m.mmode {
	case MatchMode[_prefix]:
		return strings.HasPrefix(filename, m.regex)
	case MatchMode[_suffix]:
		return strings.HasSuffix(filename, m.regex)
	case MatchMode[_substr]:
		return strings.Contains(filename, m.regex)
	default:
		debug.Assert(m.mmode == MatchMode[_wdskey], m.mmode)
		return m.regex == cos.WdsKey(filename)
	}
}

// tarReader

func (tr *tarReader) init(fh io.Reader) error {
	tr.baseR.init(fh)
	tr.tr = tar.NewReader(fh)
	return nil
}

func (tr *tarReader) ReadUntil(rcb ArchRCB, regex, mmode string) error {
	matcher := matcher{regex: regex, mmode: mmode}
	if err := matcher.init(); err != nil {
		return err
	}
	for {
		hdr, err := tr.tr.Next()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}
		if !matcher.do(hdr.Name) {
			continue
		}
		csl := &cslLimited{LimitedReader: io.LimitedReader{R: tr.tr, N: hdr.Size}}
		if stop, err := rcb.Call(hdr.Name, csl, hdr); stop || err != nil {
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

func (tgr *tgzReader) ReadUntil(rcb ArchRCB, regex, mmode string) (err error) {
	err = tgr.tr.ReadUntil(rcb, regex, mmode)
	if erc := tgr.gzr.Close(); err == nil && erc != nil {
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
	debug.Assert(ok, "zipReader: expecting io.ReaderAt")
	zr.baseR.init(fh)
	zr.zr, err = zip.NewReader(readerAt, zr.size)
	return
}

func (zr *zipReader) ReadUntil(rcb ArchRCB, regex, mmode string) error {
	matcher := matcher{regex: regex, mmode: mmode}
	if err := matcher.init(); err != nil {
		return err
	}
	for _, f := range zr.zr.File {
		finfo := f.FileInfo()
		if finfo.IsDir() {
			continue
		}
		debug.Assertf(finfo.Size() == int64(f.FileHeader.UncompressedSize64),
			"%d vs %d", finfo.Size(), f.FileHeader.UncompressedSize64)

		if !matcher.do(f.FileHeader.Name) {
			continue
		}

		csf := &cslFile{size: int64(f.FileHeader.UncompressedSize64)}
		r, err := f.Open()
		if err != nil {
			return err
		}
		csf.file = r
		if stop, e := rcb.Call(f.FileHeader.Name, csf, &f.FileHeader); stop || e != nil {
			return e
		}
	}
	return nil
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

func (lzr *lz4Reader) ReadUntil(rcb ArchRCB, regex, mmode string) error {
	return lzr.tr.ReadUntil(rcb, regex, mmode)
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

//////////////////
// ErrMatchMode //
//////////////////

func (e *ErrMatchMode) Error() string {
	return fmt.Sprintf("invalid matching mode %q, expecting one of: %v", e.mmode, MatchMode)
}

func ValidateMatchMode(mmode string) (_ string, err error) {
	if cos.MatchAll(mmode) {
		return MatchMode[_prefix], nil
	}
	for i := range MatchMode {
		if MatchMode[i] == mmode {
			return mmode, nil
		}
	}
	return "", &ErrMatchMode{mmode}
}
