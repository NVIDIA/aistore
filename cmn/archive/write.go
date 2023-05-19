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
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
)

type (
	Writer interface {
		Write(nameInArch string, oah cos.OAH, reader io.Reader) error
		Fini()
		Copy(src io.Reader, size ...int64) error

		// private
		init(w io.Writer, cksum *cos.CksumHashSize)
	}
	baseW struct {
		wmul io.Writer
		mu   sync.Mutex
		buf  []byte
		slab *memsys.Slab
	}
	tarWriter struct {
		baseW
		tw *tar.Writer
	}
	tgzWriter struct {
		tw  tarWriter
		gzw *gzip.Writer
	}
	zipWriter struct {
		baseW
		zw *zip.Writer
	}
)

// interface guard
var (
	_ Writer = (*tarWriter)(nil)
	_ Writer = (*tgzWriter)(nil)
	_ Writer = (*zipWriter)(nil)
)

func NewWriter(mime string, w io.Writer, cksum *cos.CksumHashSize) (aw Writer) {
	switch mime {
	case ExtTar:
		aw = &tarWriter{}
	case ExtTgz, ExtTarTgz:
		aw = &tgzWriter{}
	case ExtZip:
		aw = &zipWriter{}
	default:
		debug.Assert(false, mime)
	}
	aw.init(w, cksum)
	return
}

///////////////
// tarWriter //
///////////////

func (tw *tarWriter) init(w io.Writer, cksum *cos.CksumHashSize) {
	tw.buf, tw.slab = memsys.PageMM().Alloc()
	tw.wmul = w
	if cksum != nil {
		tw.wmul = cos.NewWriterMulti(w, cksum)
	}
	tw.tw = tar.NewWriter(tw.wmul)
}

func (tw *tarWriter) Fini() {
	tw.slab.Free(tw.buf)
	tw.tw.Close()
}

func (tw *tarWriter) Write(fullname string, oah cos.OAH, reader io.Reader) (err error) {
	hdr := tar.Header{
		Typeflag: tar.TypeReg,
		Name:     fullname,
		Size:     oah.SizeBytes(),
		ModTime:  time.Unix(0, oah.AtimeUnix()),
	}
	SetAuxTarHeader(&hdr)
	// one at a time
	tw.mu.Lock()
	if err = tw.tw.WriteHeader(&hdr); err == nil {
		_, err = io.CopyBuffer(tw.tw, reader, tw.buf)
	}
	tw.mu.Unlock()
	return
}

func (tw *tarWriter) Copy(src io.Reader, _ ...int64) error {
	return CopyT(src, tw.tw, tw.buf, false)
}

///////////////
// tgzWriter //
///////////////

func (tzw *tgzWriter) init(w io.Writer, cksum *cos.CksumHashSize) {
	tzw.tw.buf, tzw.tw.slab = memsys.PageMM().Alloc()
	tzw.tw.wmul = w
	if cksum != nil {
		tzw.tw.wmul = cos.NewWriterMulti(w, cksum)
	}
	tzw.gzw = gzip.NewWriter(tzw.tw.wmul)
	tzw.tw.tw = tar.NewWriter(tzw.gzw)
}

func (tzw *tgzWriter) Fini() {
	tzw.tw.Fini()
	tzw.gzw.Close()
}

func (tzw *tgzWriter) Write(fullname string, oah cos.OAH, reader io.Reader) error {
	return tzw.tw.Write(fullname, oah, reader)
}

func (tzw *tgzWriter) Copy(src io.Reader, _ ...int64) error {
	return CopyT(src, tzw.tw.tw, tzw.tw.buf, true)
}

///////////////
// zipWriter //
///////////////

func (zw *zipWriter) init(w io.Writer, cksum *cos.CksumHashSize) {
	zw.buf, zw.slab = memsys.PageMM().Alloc()
	zw.wmul = w
	if cksum != nil {
		zw.wmul = cos.NewWriterMulti(w, cksum)
	}
	zw.zw = zip.NewWriter(zw.wmul)
}

func (zw *zipWriter) Fini() {
	zw.slab.Free(zw.buf)
	zw.zw.Close()
}

func (zw *zipWriter) Write(fullname string, oah cos.OAH, reader io.Reader) error {
	ziphdr := zip.FileHeader{
		Name:               fullname,
		Comment:            fullname,
		UncompressedSize64: uint64(oah.SizeBytes()),
		Modified:           time.Unix(0, oah.AtimeUnix()),
	}
	zw.mu.Lock()
	zipw, err := zw.zw.CreateHeader(&ziphdr)
	if err == nil {
		_, err = io.CopyBuffer(zipw, reader, zw.buf)
	}
	zw.mu.Unlock()
	return err
}

func (zw *zipWriter) Copy(src io.Reader, size ...int64) error {
	r, ok := src.(io.ReaderAt)
	debug.Assert(ok && len(size) == 1)
	return CopyZ(r, size[0], zw.zw, zw.buf)
}
