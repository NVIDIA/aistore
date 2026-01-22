//go:build dsort

// Package shard provides Extract(shard), Create(shard), and associated methods
// across all supported archival formats (see cmn/archive/mime.go)
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package shard

import (
	"io"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
)

// interface guard
var _ RW = (*nopRW)(nil)

type nopRW struct {
	internal RW
}

func NopRW(internal RW) RW { return &nopRW{internal: internal} }

func (n *nopRW) IsCompressed() bool   { return n.internal.IsCompressed() }
func (n *nopRW) SupportsOffset() bool { return n.internal.SupportsOffset() }
func (n *nopRW) MetadataSize() int64  { return n.internal.MetadataSize() }

// Extract reads the tarball f and extracts its metadata.
func (n *nopRW) Extract(lom *core.LOM, r cos.ReadReaderAt, extractor RecordExtractor, toDisk bool) (extractedSize int64,
	extractedCount int, err error) {
	return n.internal.Extract(lom, r, extractor, toDisk)
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
