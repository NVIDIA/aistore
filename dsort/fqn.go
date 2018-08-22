/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dsort provides APIs for distributed archive file shuffling.
package dsort

import (
	"github.com/NVIDIA/dfcpub/fs"
)

// Here is implementation of custom content file type for dsort. This content type
// is used when creating files during local extraction phase if no memory is
// left to be used by the given dsort process.

const dSortFileType = "dsort"
const dSortWorkfileType = "dsortw"

var (
	_ fs.ContentResolver = &dsortFile{}
)

type dsortFile struct{}

func (df *dsortFile) PermToEvict() bool                       { return false }
func (df *dsortFile) PermToMove() bool                        { return false }
func (df *dsortFile) PermToProcess() bool                     { return false }
func (df *dsortFile) GenUniqueFQN(base, prefix string) string { return base }
func (df *dsortFile) ParseUniqueFQN(base string) (orig string, old bool, ok bool) {
	return base, false, true
}
