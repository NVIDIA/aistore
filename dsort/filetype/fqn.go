// Package filetype provides the implementation of custom content file type for dsort.
// This content type is used when creating files during local extraction phase if no memory is
// left to be used by the given dsort process.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package filetype

import (
	"github.com/NVIDIA/aistore/fs"
)

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
