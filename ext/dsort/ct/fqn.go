// Package ct provides additional dsort-specific content types
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ct

import (
	"github.com/NVIDIA/aistore/fs"
)

// for common content types, see fs/content.go

const (
	DsortFileCT = "ds"
	DsortWorkCT = "dw"

	WorkfileRecvShard   = "recv-shard"
	WorkfileCreateShard = "create-shard"
)

// interface guard
var _ fs.ContentRes = (*DsortFile)(nil)

type DsortFile struct{}

func (*DsortFile) MakeUbase(base string, _ ...string) string { return base }

func (*DsortFile) ParseUbase(base string) fs.ContentInfo {
	return fs.ContentInfo{Base: base, Ok: true}
}
