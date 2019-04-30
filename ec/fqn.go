// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"github.com/NVIDIA/aistore/fs"
)

type SliceSpec struct{}
type MetaSpec struct{}

var (
	_ fs.ContentResolver = &SliceSpec{}
	_ fs.ContentResolver = &MetaSpec{}
)

func (wf *SliceSpec) PermToMove() bool    { return true }
func (wf *SliceSpec) PermToEvict() bool   { return false }
func (wf *SliceSpec) PermToProcess() bool { return false }

func (wf *SliceSpec) GenUniqueFQN(base, _ string) string { return base }
func (wf *SliceSpec) ParseUniqueFQN(base string) (orig string, old bool, ok bool) {
	return base, false, true
}

func (wf *MetaSpec) PermToMove() bool    { return true }
func (wf *MetaSpec) PermToEvict() bool   { return false }
func (wf *MetaSpec) PermToProcess() bool { return false }

func (wf *MetaSpec) GenUniqueFQN(base, _ string) string { return base }
func (wf *MetaSpec) ParseUniqueFQN(base string) (orig string, old bool, ok bool) {
	// TODO: old can mean that there is no corresponding replica/slice but it
	// seems a heavy operation
	return base, false, true
}
