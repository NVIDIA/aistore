// Package shard provides Extract(shard), Create(shard), and associated methods
// across all suppported archival formats (see cmn/archive/mime.go)
/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION. All rights reserved.
 */
package shard

import (
	"archive/tar"
	"io"

	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/pierrec/lz4/v3"
)

type tlz4RW struct {
	ext string
}

// interface guard
var _ RW = (*tlz4RW)(nil)

func NewTarlz4RW() RW { return &tlz4RW{ext: archive.ExtTarLz4} }

func (*tlz4RW) IsCompressed() bool   { return true }
func (*tlz4RW) SupportsOffset() bool { return true }
func (*tlz4RW) MetadataSize() int64  { return archive.TarBlockSize } // size of tar header with padding

// Extract  the tarball f and extracts its metadata.
func (trw *tlz4RW) Extract(lom *core.LOM, r cos.ReadReaderAt, extractor RecordExtractor, toDisk bool) (int64, int, error) {
	ar, err := archive.NewReader(trw.ext, r)
	if err != nil {
		return 0, 0, err
	}
	c := &rcbCtx{parent: trw, extractor: extractor, shardName: lom.ObjName, toDisk: toDisk}
	err = c.extract(lom, ar)

	return c.extractedSize, c.extractedCount, err
}

// create local shard based on Shard
func (*tlz4RW) Create(s *Shard, tarball io.Writer, loader ContentLoader) (written int64, err error) {
	var (
		lzw      = lz4.NewWriter(tarball)
		tw       = tar.NewWriter(lzw)
		rdReader = newTarRecordDataReader()
	)
	written, err = writeCompressedTar(s, tw, lzw, loader, rdReader)

	// note the order of closing: tw, gzw, and eventually tarball (by the caller)
	rdReader.free()
	cos.Close(tw)
	cos.Close(lzw)
	return written, err
}
