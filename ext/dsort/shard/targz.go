//go:build sharding

// Package shard provides Extract(shard), Create(shard), and associated methods
// across all supported archival formats (see cmn/archive/mime.go)
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package shard

import (
	"archive/tar"
	"compress/gzip"
	"io"

	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
)

type tgzRW struct {
	ext string
}

// interface guard
var _ RW = (*tgzRW)(nil)

func NewTargzRW(ext string) RW { return &tgzRW{ext: ext} }

func (*tgzRW) IsCompressed() bool   { return true }
func (*tgzRW) SupportsOffset() bool { return true }
func (*tgzRW) MetadataSize() int64  { return archive.TarBlockSize } // size of tar header with padding

// Extract reads the tarball f and extracts its metadata.
// Writes work tar
func (trw *tgzRW) Extract(lom *core.LOM, r cos.ReadReaderAt, extractor RecordExtractor, toDisk bool) (int64, int, error) {
	ar, err := archive.NewReader(trw.ext, r)
	if err != nil {
		return 0, 0, err
	}
	c := &rcbCtx{parent: trw, extractor: extractor, shardName: lom.ObjName, toDisk: toDisk, fromTar: true}
	err = c.extract(lom, ar)

	return c.extractedSize, c.extractedCount, err
}

// create local shard based on Shard
func (*tgzRW) Create(s *Shard, tarball io.Writer, loader ContentLoader) (written int64, err error) {
	var (
		gzw, _   = gzip.NewWriterLevel(tarball, gzip.BestSpeed)
		tw       = tar.NewWriter(gzw)
		rdReader = newTarRecordDataReader()
	)
	written, err = writeCompressedTar(s, tw, gzw, loader, rdReader)

	// note the order of closing: tw, gzw, and eventually tarball (by the caller)
	rdReader.free()
	cos.Close(tw)
	cos.Close(gzw)
	return written, err
}
