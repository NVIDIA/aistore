// Package archive: write, read, copy, append, list primitives
// across all supported formats
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package archive

import (
	"archive/tar"
	"archive/zip"
	"io"
)

// copy `src` => `tw` destination, one file at a time
// handles .tar, .tar.gz, and .tar.lz4
// - open specific arch reader
// - always close it
// - `tw` is the writer that can be further used to write (ie., append)
//
// see also: cpZip below
func cpTar(src io.Reader, tw *tar.Writer, buf []byte) (err error) {
	tr := tar.NewReader(src)
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
	return err
}

func cpZip(src io.ReaderAt, size int64, zw *zip.Writer, buf []byte) (err error) {
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
			reader := &io.LimitedReader{R: zipr, N: int64(hdr.UncompressedSize64)}
			_, err = io.CopyBuffer(zipw, reader, buf)
		}
		zipr.Close()
		if err != nil {
			break
		}
	}
	return err
}
