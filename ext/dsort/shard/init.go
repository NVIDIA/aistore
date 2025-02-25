// Package shard provides Extract(shard), Create(shard), and associated methods
// across all suppported archival formats (see cmn/archive/mime.go)
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package shard

import (
	"github.com/NVIDIA/aistore/cmn/archive"
)

// padding buffer (zero-filled)
var (
	padBuf [archive.TarBlockSize]byte
)
