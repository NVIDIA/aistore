// Package filetype provides the implementation of custom content file type for dsort.
// This content type is used when creating files during local extraction phase if no memory is
// left to be used by the given dsort process.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package filetype

import (
	"github.com/NVIDIA/aistore/fs"
)

const (
	DSortFileType     = "dsort"
	DSortWorkfileType = "dsortw"

	WorkfileRecvShard   = "recv-shard"
	WorkfileCreateShard = "create-shard"
)

var (
	_ fs.ContentResolver = &DSortFile{}
)

type DSortFile struct{}

func (df *DSortFile) PermToEvict() bool                  { return false }
func (df *DSortFile) PermToMove() bool                   { return false }
func (df *DSortFile) PermToProcess() bool                { return false }
func (df *DSortFile) GenUniqueFQN(base, _ string) string { return base }
func (df *DSortFile) ParseUniqueFQN(base string) (orig string, old bool, ok bool) {
	return base, false, true
}
