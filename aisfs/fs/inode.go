// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
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
	IncLookupCount()
	DecLookupCountN(n uint64) uint64
	Name() string
	IsDir() bool
}

type baseInode struct {
	id          fuseops.InodeID
	attrs       fuseops.InodeAttributes
	name        string
	lookupCount atomic.Uint64
	mu          sync.Mutex
}

func newBaseInode(id fuseops.InodeID, attrs fuseops.InodeAttributes, name string) baseInode {
	return baseInode{
		id:    id,
		attrs: attrs,
		name:  name,
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

// Name returns a name that maps to an inode.
func (in *baseInode) Name() string {
	return in.name
}

// Attributes returns inode's attributes (mode, size, atime...).
// REQUIRES_LOCK(in)
func (in *baseInode) Attributes() (attrs fuseops.InodeAttributes) {
	return in.attrs
}

// IncLookupCount atomically increments inode's lookup count.
func (in *baseInode) IncLookupCount() {
	in.lookupCount.Add(1)
}

// DecLookupCountN atomically decrements inode's lookup count by n.
func (in *baseInode) DecLookupCountN(n uint64) uint64 {
	return in.lookupCount.Sub(n)
}
