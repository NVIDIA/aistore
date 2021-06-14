// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"github.com/NVIDIA/aistore/fs"
)

type (
	SliceSpec struct{}
	MetaSpec  struct{}
)

// interface guard
var (
	_ fs.ContentResolver = (*SliceSpec)(nil)
	_ fs.ContentResolver = (*MetaSpec)(nil)
)

func (*SliceSpec) PermToMove() bool    { return true }
func (*SliceSpec) PermToEvict() bool   { return false }
func (*SliceSpec) PermToProcess() bool { return false }

func (*SliceSpec) GenUniqueFQN(base, _ string) string { return base }

func (*SliceSpec) ParseUniqueFQN(base string) (orig string, old, ok bool) {
	return base, false, true
}

func (*MetaSpec) PermToMove() bool    { return true }
func (*MetaSpec) PermToEvict() bool   { return false }
func (*MetaSpec) PermToProcess() bool { return false }

func (*MetaSpec) GenUniqueFQN(base, _ string) string { return base }

func (*MetaSpec) ParseUniqueFQN(base string) (orig string, old, ok bool) {
	return base, false, true
}
