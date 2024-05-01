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
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/pierrec/lz4/v3"
)

type MatchMode int

const (
	Regexp MatchMode = iota // default (and slow)
	Prefix
	Suffix
	Substr
	WdsKey // WebDataset convention - pathname without extension (https://github.com/webdataset/webdataset#the-webdataset-format)

	// ------------------ (m.b. the last)
	_lastmode
)

var MatchModeText = []string{
	"regexp",
	"prefix",
	"suffix",
	"substr",
	"wdskey",
}

// to use, construct (`NewReader`) and iterate (`RangeUntil`)
// (all supported formats)
// simple/single selection is also supported (`ReadOne`)
type (
	ReadCB func(filename string, reader cos.ReadCloseSizer, hdr any) (bool /*stop*/, error)

	Reader interface {
		// - call rcb (reader's callback) with each matching archived file
		//   (if `regex` is empty - each archived file);
		// - stop upon EOF, or when rcb returns true (ie., stop) or any error
		ReadUntil(rcb ReadCB, regex string, mode MatchMode) error

		// simple/single selection of a given archived filename (full path)
		ReadOne(filename string) (cos.ReadCloseSizer, error)

		// private
		init(fh io.Reader) error
	}
)

// private implementation
type (
	matcher struct {
		regex string
		mode  MatchMode
		// when (and if) compiled
		re *regexp.Regexp
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

// matcher

func (m *matcher) init() (err error) {
	if m.mode < 0 || m.mode >= _lastmode {
		return fmt.Errorf("invalid match-mode %d", m.mode)
	}
	switch {
	case m.regex == "": // 1. empty match matches all archived filenames
		debug.Assert(m.mode == 0)
		return nil
	case m.mode > 0: // 2. fast and simple string-based match: prefix, et al.
		debug.Assert(m.regex == "")
		return nil
	default: // finally, 3. regex
		m.re, err = regexp.Compile(m.regex)
		return err
	}
}

func (m *matcher) do(filename string) bool {
	if m.regex == "" { // empty regex matches all archived filenames
		return true
	}
	if m.re != nil {
		return m.re.MatchString(filename)
	}
	switch m.mode {
	case Prefix:
		return strings.HasPrefix(filename, m.regex)
	case Suffix:
		return strings.HasSuffix(filename, m.regex)
	case Substr:
		return strings.Contains(filename, m.regex)
	default:
		debug.Assert(m.mode == WdsKey, m.mode)
		return m.regex == cos.WdsKey(filename)
	}
}

// tarReader

func (tr *tarReader) init(fh io.Reader) error {
	tr.baseR.init(fh)
	tr.tr = tar.NewReader(fh)
	return nil
}

func (tr *tarReader) ReadUntil(rcb ReadCB, regex string, mode MatchMode) (err error) {
	matcher := matcher{regex: regex, mode: mode}
	if err = matcher.init(); err != nil {
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

func (tgr *tgzReader) ReadUntil(rcb ReadCB, regex string, mode MatchMode) (err error) {
	err = tgr.tr.ReadUntil(rcb, regex, mode)
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

func (zr *zipReader) ReadUntil(rcb ReadCB, regex string, mode MatchMode) (err error) {
	matcher := matcher{regex: regex, mode: mode}
	if err = matcher.init(); err != nil {
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

func (lzr *lz4Reader) ReadUntil(rcb ReadCB, regex string, mode MatchMode) error {
	return lzr.tr.ReadUntil(rcb, regex, mode)
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
