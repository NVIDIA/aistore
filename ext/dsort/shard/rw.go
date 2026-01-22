//go:build dsort

// Package shard provides Extract(shard), Create(shard), and associated methods
// across all supported archival formats (see cmn/archive/mime.go)
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package shard

import (
	"io"

	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
)

type RW interface {
	Extract(lom *core.LOM, r cos.ReadReaderAt, extractor RecordExtractor, toDisk bool) (int64, int, error)
	Create(s *Shard, w io.Writer, loader ContentLoader) (int64, error)
	IsCompressed() bool
	SupportsOffset() bool
	MetadataSize() int64
}

var (
	RWs = map[string]RW{
		archive.ExtTar:    &tarRW{archive.ExtTar},
		archive.ExtTgz:    &tgzRW{archive.ExtTgz},
		archive.ExtTarGz:  &tgzRW{archive.ExtTarGz},
		archive.ExtTarLz4: &tlz4RW{archive.ExtTarLz4},
		archive.ExtZip:    &zipRW{archive.ExtZip},
	}
)

func IsCompressed(ext string) bool {
	rw, ok := RWs[ext]
	if !ok {
		nlog.Errorf("IsCompressed: unsupported or empty extension %q, defaulting to false", ext)
		return false
	}
	return rw.IsCompressed()
}
