// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"io"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

const (
	fmtErrNotFound = "file %q in archive \"%s/%s\""
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
)

func (csl *cslLimited) Size() int64  { return csl.N }
func (csl *cslLimited) Close() error { return nil }

func (csc *cslClose) Read(b []byte) (int, error) { return csc.R.Read(b) }
func (csc *cslClose) Size() int64                { return csc.N }
func (csc *cslClose) Close() error               { return csc.gzr.Close() }

func (csf *cslFile) Read(b []byte) (int, error) { return csf.file.Read(b) }
func (csf *cslFile) Size() int64                { return csf.size }
func (csf *cslFile) Close() error               { return csf.file.Close() }

func extractArch(file *os.File, filename string, lom *cluster.LOM) (cos.ReadCloseSizer, error) {
	switch {
	case strings.HasSuffix(lom.ObjName, cos.ExtTarTgz) || strings.HasSuffix(lom.ObjName, cos.ExtTgz):
		return extractTgz(file, filename, lom)
	case strings.HasSuffix(lom.ObjName, cos.ExtZip):
		return extractZip(file, filename, lom)
	default:
		return extractTar(file, filename, lom)
	}
}

func extractTar(reader io.Reader, filename string, lom *cluster.LOM) (*cslLimited, error) {
	tr := tar.NewReader(reader)
	for {
		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				err = cmn.NewNotFoundError(fmtErrNotFound, filename, lom.Bucket(), lom.ObjName)
			}
			return nil, err
		}
		if hdr.Name != filename {
			continue
		}
		return &cslLimited{LimitedReader: io.LimitedReader{R: reader, N: hdr.Size}}, nil
	}
}

func extractTgz(reader io.Reader, filename string, lom *cluster.LOM) (csc *cslClose, err error) {
	var (
		gzr *gzip.Reader
		csl *cslLimited
	)
	if gzr, err = gzip.NewReader(reader); err != nil {
		return
	}
	if csl, err = extractTar(gzr, filename, lom); err != nil {
		return
	}
	csc = &cslClose{gzr: gzr /*to close*/, R: csl /*to read from*/, N: csl.N /*size*/}
	return
}

func extractZip(readerAt cos.ReadReaderAt, filename string, lom *cluster.LOM) (csf *cslFile, err error) {
	var zr *zip.Reader
	if zr, err = zip.NewReader(readerAt, lom.Size()); err != nil {
		return
	}
	for _, f := range zr.File {
		header := f.FileHeader
		if header.Name != filename {
			continue
		}
		finfo := f.FileInfo()
		if finfo.IsDir() {
			continue
		}
		csf = &cslFile{size: finfo.Size()}
		csf.file, err = f.Open()
		return
	}
	err = cmn.NewNotFoundError(fmtErrNotFound, filename, lom.Bucket(), lom.ObjName)
	return
}
