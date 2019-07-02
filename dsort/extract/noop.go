// Package extract provides provides functions for working with compressed files
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package extract

import (
	"io"

	"github.com/NVIDIA/aistore/fs"
)

var (
	_ ExtractCreator = &nopExtractCreator{}
)

type nopExtractCreator struct {
	internal ExtractCreator
}

func NopExtractCreator(internal ExtractCreator) ExtractCreator {
	return &nopExtractCreator{internal: internal}
}

// ExtractShard reads the tarball f and extracts its metadata.
func (t *nopExtractCreator) ExtractShard(fqn fs.ParsedFQN, r *io.SectionReader, extractor RecordExtractor, toDisk bool) (extractedSize int64, extractedCount int, err error) {
	return t.internal.ExtractShard(fqn, r, extractor, toDisk)
}

// CreateShard creates a new shard locally based on the Shard.
func (t *nopExtractCreator) CreateShard(s *Shard, w io.Writer, loadContent LoadContentFunc) (written int64, err error) {
	var (
		n int64
	)

	for _, rec := range s.Records.All() {
		for _, obj := range rec.Objects {
			n, err = loadContent(w, rec, obj)
			if err != nil {
				return
			}
			written += n
		}
	}

	return written, nil
}

func (t *nopExtractCreator) UsingCompression() bool {
	return false
}

func (t *nopExtractCreator) SupportsOffset() bool {
	return true
}

func (t *nopExtractCreator) MetadataSize() int64 {
	return t.internal.MetadataSize()
}
