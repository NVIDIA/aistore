/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package filetype

import (
	"github.com/NVIDIA/dfcpub/fs"
)

// Here is implementation of custom content file type for dsort. This content type
// is used when creating files during local extraction phase if no memory is
// left to be used by the given dsort process.

const DSortFileType = "dsort"
const DSortWorkfileType = "dsortw"

var (
	_ fs.ContentResolver = &DSortFile{}
)

type DSortFile struct{}

func (df *DSortFile) PermToEvict() bool                       { return false }
func (df *DSortFile) PermToMove() bool                        { return false }
func (df *DSortFile) PermToProcess() bool                     { return false }
func (df *DSortFile) GenUniqueFQN(base, prefix string) string { return base }
func (df *DSortFile) ParseUniqueFQN(base string) (orig string, old bool, ok bool) {
	return base, false, true
}
