// Package shard provides Extract(shard), Create(shard), and associated methods
// across all suppported archival formats (see cmn/archive/mime.go)
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package shard

import (
	"io"

	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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
	debug.Assert(ok, ext)
	return rw.IsCompressed()
}
