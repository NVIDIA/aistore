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
	"io"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/memsys"

	"github.com/pierrec/lz4/v4"
)

type (
	HeaderCallback func(any)
	Opts           struct {
		CB        HeaderCallback
		TarFormat tar.Format
		Serialize bool
	}
)

type (
	Writer interface {
		// Init specific writer
		Write(nameInArch string, oah cos.OAH, reader io.Reader) error
		// Close, cleanup
		Fini() error
		// Copy arch, with potential subsequent APPEND
		Copy(src io.Reader, size ...int64) error

		// private
		init(w io.Writer, cksum *cos.CksumHashSize, opts *Opts)
	}
	baseW struct {
		wmul io.Writer
		lck  sync.Locker // serialize: (multi-object => single shard)
		cb   HeaderCallback
		slab *memsys.Slab
		buf  []byte
	}
	tarWriter struct {
		tw *tar.Writer
		baseW
		format tar.Format
	}
	tgzWriter struct {
		gzw *gzip.Writer
		tw  tarWriter
	}
	zipWriter struct {
		zw *zip.Writer
		baseW
	}
	lz4Writer struct {
		lzw *lz4.Writer
		tw  tarWriter
	}
)

// interface guard
var (
	_ Writer = (*tarWriter)(nil)
	_ Writer = (*tgzWriter)(nil)
	_ Writer = (*zipWriter)(nil)
	_ Writer = (*lz4Writer)(nil)
)

// calls init() -> open(),alloc()
func NewWriter(mime string, w io.Writer, cksum *cos.CksumHashSize, opts *Opts) (aw Writer) {
	switch mime {
	case ExtTar:
		aw = &tarWriter{}
	case ExtTgz, ExtTarGz:
		aw = &tgzWriter{}
	case ExtZip:
		aw = &zipWriter{}
	case ExtTarLz4:
		aw = &lz4Writer{}
	default:
		debug.Assert(false, mime)
	}
	aw.init(w, cksum, opts)
	return
}

// baseW

func (bw *baseW) init(w io.Writer, cksum *cos.CksumHashSize, opts *Opts) {
	bw.buf, bw.slab = memsys.PageMM().Alloc()

	bw.lck = cos.NopLocker{}
	bw.cb = nopTarHeader
	if opts != nil {
		if opts.CB != nil {
			bw.cb = opts.CB
		}
		if opts.Serialize {
			bw.lck = &sync.Mutex{}
		}
	}
	bw.wmul = w
	if cksum != nil {
		bw.wmul = cos.NewWriterMulti(w, cksum)
	}
}

// tarWriter

// tar.FormatUnknown lets standard library choose USTAR (most compatible) or PAX (extended features) as needed.
// Can be overridden via opts.TarFormat if specific format required (e.g., FormatGNU for GNU tar compatibility)

func (tw *tarWriter) init(w io.Writer, cksum *cos.CksumHashSize, opts *Opts) {
	tw.baseW.init(w, cksum, opts)

	tw.format = tar.FormatUnknown // default: auto-select most compatible format

	if opts != nil {
		tw.format = opts.TarFormat
	}
	debug.Assert(tw.format == tar.FormatUnknown || tw.format == tar.FormatUSTAR ||
		tw.format == tar.FormatPAX || tw.format == tar.FormatGNU, tw.format.String())

	tw.tw = tar.NewWriter(tw.wmul)
}

func (tw *tarWriter) Fini() error {
	defer tw.slab.Free(tw.buf)

	return tw.tw.Close()
}

func (tw *tarWriter) Write(fullname string, oah cos.OAH, reader io.Reader) (err error) {
	hdr := tar.Header{
		Typeflag: tar.TypeReg,
		Name:     fullname,
		Size:     oah.Lsize(),
		ModTime:  time.Unix(0, oah.AtimeUnix()),
		Mode:     int64(cos.PermRWRR),
		Format:   tw.format,
	}
	tw.cb(&hdr)
	tw.lck.Lock()
	if err = tw.tw.WriteHeader(&hdr); err == nil {
		_, err = io.CopyBuffer(tw.tw, reader, tw.buf)
	}
	tw.lck.Unlock()
	return err
}

func (tw *tarWriter) Copy(src io.Reader, _ ...int64) error {
	return cpTar(src, tw.tw, tw.buf)
}

// set Uid/Gid bits in TAR header
// - note: cos.PermRWRR default
// - not calling standard tar.FileInfoHeader

func nopTarHeader(any) {}

func SetTarHeader(hdr any) {
	thdr := hdr.(*tar.Header)
	{
		thdr.Uid = os.Getuid()
		thdr.Gid = os.Getgid()
	}
}

// tgzWriter

func (tzw *tgzWriter) init(w io.Writer, cksum *cos.CksumHashSize, opts *Opts) {
	tzw.tw.baseW.init(w, cksum, opts)
	tzw.gzw = gzip.NewWriter(tzw.tw.wmul)
	tzw.tw.tw = tar.NewWriter(tzw.gzw)
}

func (tzw *tgzWriter) Fini() error {
	// close (and note: tar.close flushes)
	if err := tzw.tw.Fini(); err != nil {
		tzw.gzw.Close() // Try to close gzip anyway
		return err
	}

	return tzw.gzw.Close()
}

func (tzw *tgzWriter) Write(fullname string, oah cos.OAH, reader io.Reader) error {
	return tzw.tw.Write(fullname, oah, reader)
}

func (tzw *tgzWriter) Copy(src io.Reader, _ ...int64) error {
	gzr, err := gzip.NewReader(src)
	if err != nil {
		return err
	}
	err = cpTar(gzr, tzw.tw.tw, tzw.tw.buf)
	cos.Close(gzr)
	return err
}

// zipWriter
// in re streaming use case, note: ZIP writer doesn't have explicit flush

func (zw *zipWriter) init(w io.Writer, cksum *cos.CksumHashSize, opts *Opts) {
	zw.baseW.init(w, cksum, opts)
	zw.zw = zip.NewWriter(zw.wmul)
}

func (zw *zipWriter) Fini() error {
	defer zw.slab.Free(zw.buf)
	return zw.zw.Close()
}

func (zw *zipWriter) Write(fullname string, oah cos.OAH, reader io.Reader) error {
	ziphdr := zip.FileHeader{
		Name:               fullname,
		Comment:            fullname,
		UncompressedSize64: uint64(oah.Lsize()),
		Modified:           time.Unix(0, oah.AtimeUnix()),
	}
	zw.cb(&ziphdr)
	zw.lck.Lock()
	zipw, err := zw.zw.CreateHeader(&ziphdr)
	if err == nil {
		_, err = io.CopyBuffer(zipw, reader, zw.buf)
	}
	zw.lck.Unlock()
	return err
}

func (zw *zipWriter) Copy(src io.Reader, size ...int64) error {
	r, ok := src.(io.ReaderAt)
	debug.Assert(ok && len(size) == 1)
	return cpZip(r, size[0], zw.zw, zw.buf)
}

// lz4Writer

func (lzw *lz4Writer) init(w io.Writer, cksum *cos.CksumHashSize, opts *Opts) {
	var (
		blockSize = lz4.Block256Kb
	)
	lzw.tw.baseW.init(w, cksum, opts)
	lzw.lzw = lz4.NewWriter(lzw.tw.wmul)

	if cmn.Rom.Features().IsSet(feat.LZ4Block1MB) {
		blockSize = lz4.Block1Mb
	}
	err := lzw.lzw.Apply(
		lz4.BlockSizeOption(blockSize),
		lz4.ChecksumOption(cmn.Rom.Features().IsSet(feat.LZ4FrameChecksum)),
		lz4.BlockChecksumOption(false),
	)
	debug.AssertNoErr(err)

	lzw.tw.tw = tar.NewWriter(lzw.lzw)
}

func (lzw *lz4Writer) Fini() error {
	// close (and note: tar.close flushes)
	if err := lzw.tw.Fini(); err != nil {
		lzw.lzw.Close() // Try to close lz4 anyway
		return err
	}

	return lzw.lzw.Close()
}

func (lzw *lz4Writer) Write(fullname string, oah cos.OAH, reader io.Reader) error {
	return lzw.tw.Write(fullname, oah, reader)
}

func (lzw *lz4Writer) Copy(src io.Reader, _ ...int64) error {
	lzr := lz4.NewReader(src)
	return cpTar(lzr, lzw.tw.tw, lzw.tw.buf)
}
