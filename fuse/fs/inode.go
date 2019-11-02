// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/jacobsa/fuse/fuseops"
)

type Inode interface {
	sync.Locker
	Parent() Inode
	ID() fuseops.InodeID
	Attributes() fuseops.InodeAttributes
	SetAttributes(fuseops.InodeAttributes)
	AsChildEntry() fuseops.ChildInodeEntry
	IncLookupCount()
	DecLookupCountN(n uint64) uint64
	Path() string
	IsDir() bool
}

type baseInode struct {
	id          fuseops.InodeID
	attrs       fuseops.InodeAttributes
	path        string
	lookupCount atomic.Uint64
	mu          sync.Mutex
}

func newBaseInode(id fuseops.InodeID, attrs fuseops.InodeAttributes, path string) baseInode {
	return baseInode{
		id:    id,
		attrs: attrs,
		path:  path,
	}
}

func (in *baseInode) Lock() {
	in.mu.Lock()
}

func (in *baseInode) Unlock() {
	in.mu.Unlock()
}

// ID returns inode number (ID).
func (in *baseInode) ID() fuseops.InodeID {
	return in.id
}

// Path returns a path that maps to an inode.
// The file system does not support hard links,
// so there is only one path for each inode.
// Note: Path does not start with a separator.
func (in *baseInode) Path() string {
	return in.path
}

// Attributes returns inode's attributes (mode, size, atime...).
// REQUIRES_LOCK(in)
func (in *baseInode) Attributes() (attrs fuseops.InodeAttributes) {
	return in.attrs
}

// SetAttributes sets inode attributes.
// REQUIRES_LOCK(in)
func (in *baseInode) SetAttributes(attrs fuseops.InodeAttributes) {
	in.attrs = attrs
}

// AsChildEntry returns a fuseops.ChildInodeEntry struct
// constructed from the current inode state.
// REQUIRES_LOCK(in)
func (in *baseInode) AsChildEntry() (en fuseops.ChildInodeEntry) {
	en.Child = in.ID()
	en.Attributes = in.Attributes()
	return
}

// IncLookupCount atomically increments inode's lookup count.
func (in *baseInode) IncLookupCount() {
	in.lookupCount.Add(1)
}

// DecLookupCountN atomically decrements inode's lookup count by n.
func (in *baseInode) DecLookupCountN(n uint64) uint64 {
	return in.lookupCount.Sub(n)
}
