// Package shard provides Extract(shard), Create(shard), and associated methods
// across all suppported archival formats (see cmn/archive/mime.go)
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package shard

import (
	"io"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// interface guard
var _ Creator = (*nopRW)(nil)

type nopRW struct {
	internal Creator
}

func NopRW(internal Creator) Creator {
	return &nopRW{internal: internal}
}

// Extract reads the tarball f and extracts its metadata.
func (t *nopRW) Extract(lom *cluster.LOM, r cos.ReadReaderAt, extractor RecordExtractor, toDisk bool) (extractedSize int64,
	extractedCount int, err error) {
	return t.internal.Extract(lom, r, extractor, toDisk)
}

// Create creates a new shard locally based on the Shard.
func (*nopRW) Create(s *Shard, w io.Writer, loader ContentLoader) (written int64, err error) {
	var n int64
	for _, rec := range s.Records.All() {
		for _, obj := range rec.Objects {
			n, err = loader.Load(w, rec, obj)
			if err != nil {
				return
			}
			written += n
		}
	}
	return written, nil
}

func (*nopRW) SupportsOffset() bool  { return true }
func (t *nopRW) MetadataSize() int64 { return t.internal.MetadataSize() }
