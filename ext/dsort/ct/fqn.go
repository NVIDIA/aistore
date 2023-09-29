// Package ct provides additional dsort-specific content types
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ct

import (
	"github.com/NVIDIA/aistore/fs"
)

// for common content types, see fs/content.go

const (
	DsortFileType     = "ds"
	DsortWorkfileType = "dw"

	WorkfileRecvShard   = "recv-shard"
	WorkfileCreateShard = "create-shard"
)

// interface guard
var _ fs.ContentResolver = (*DsortFile)(nil)

type DsortFile struct{}

func (*DsortFile) PermToEvict() bool                  { return false }
func (*DsortFile) PermToMove() bool                   { return false }
func (*DsortFile) PermToProcess() bool                { return false }
func (*DsortFile) GenUniqueFQN(base, _ string) string { return base }

func (*DsortFile) ParseUniqueFQN(base string) (orig string, old, ok bool) {
	return base, false, true
}
