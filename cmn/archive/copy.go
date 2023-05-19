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

	"github.com/NVIDIA/aistore/cmn/cos"
)

// copying src-arch => dst-arch for subsequent APPEND
// TODO -- FIXME: checksum

// copy TAR or TGZ (`src` => `tw`) one file at a time;
// opens specific arch reader and always closes it;
// `tw` is the writer that can be further used to write (ie., append)
func CopyT(src io.Reader, tw *tar.Writer, buf []byte, tgz bool) (err error) {
	var (
		gzr *gzip.Reader
		tr  *tar.Reader
	)
	if tgz {
		if gzr, err = gzip.NewReader(src); err != nil {
			return
		}
		tr = tar.NewReader(gzr)
	} else {
		tr = tar.NewReader(src)
	}
	for err == nil {
		var hdr *tar.Header
		hdr, err = tr.Next()
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			break
		}
		// copy next one
		csl := &io.LimitedReader{R: tr, N: hdr.Size}
		if err = tw.WriteHeader(hdr); err == nil {
			_, err = io.CopyBuffer(tw, csl, buf)
		}
	}
	if gzr != nil {
		cos.Close(gzr)
	}
	return
}

// TODO: check subdirs
func CopyZ(src io.ReaderAt, size int64, zw *zip.Writer, buf []byte) (err error) {
	var zr *zip.Reader
	if zr, err = zip.NewReader(src, size); err != nil {
		return
	}
	for _, f := range zr.File {
		var (
			zipr io.ReadCloser
			zipw io.Writer
		)
		if f.FileInfo().IsDir() {
			continue
		}
		zipr, err = f.Open()
		if err != nil {
			break
		}
		hdr := f.FileHeader
		zipw, err = zw.CreateHeader(&hdr)
		if err == nil {
			_, err = io.CopyBuffer(zipw, zipr, buf)
		}
		zipr.Close()
		if err != nil {
			break
		}
	}
	return
}
