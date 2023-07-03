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
	DSortFileType     = "ds"
	DSortWorkfileType = "dw"

	WorkfileRecvShard   = "recv-shard"
	WorkfileCreateShard = "create-shard"
)

// interface guard
var _ fs.ContentResolver = (*DSortFile)(nil)

type DSortFile struct{}

func (*DSortFile) PermToEvict() bool                  { return false }
func (*DSortFile) PermToMove() bool                   { return false }
func (*DSortFile) PermToProcess() bool                { return false }
func (*DSortFile) GenUniqueFQN(base, _ string) string { return base }

func (*DSortFile) ParseUniqueFQN(base string) (orig string, old, ok bool) {
	return base, false, true
}
