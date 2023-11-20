// Package shard provides Extract(shard), Create(shard), and associated methods
// across all suppported archival formats (see cmn/archive/mime.go)
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package shard

import (
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/archive"
)

type global struct {
	t cluster.Target
}

var (
	g global

	// padding buffer (zero-filled)
	padBuf [archive.TarBlockSize]byte
)

func Init(t cluster.Target) {
	g.t = t
}
