// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/jacobsa/fuse/fuseops"
)

// AttrUpdateReq is a request struct for updating inode attributes
// corresponding to non-nil struct fields.
type AttrUpdateReq struct {
	Mode  *os.FileMode
	Size  *uint64
	Atime *time.Time
	Mtime *time.Time
}

type Inode interface {
	// Locking
	sync.Locker
	RLock()
	RUnlock()

	// General
	Parent() Inode
	ID() fuseops.InodeID
	Path() string
	IsDir() bool
	Destroy() error

	// Attributes
	Attributes() fuseops.InodeAttributes
	UpdateAttributes(*AttrUpdateReq) fuseops.InodeAttributes
	AsChildEntry() fuseops.ChildInodeEntry

	// Lookup count
	IncLookupCount()
	DecLookupCountN(n uint64) uint64
}

type baseInode struct {
	sync.RWMutex

	id          fuseops.InodeID
	attrs       fuseops.InodeAttributes
	path        string
	lookupCount atomic.Uint64
}

func newBaseInode(id fuseops.InodeID, attrs fuseops.InodeAttributes, path string) baseInode {
	return baseInode{
		id:    id,
		attrs: attrs,
		path:  path,
	}
}

// ID returns inode number (ID).
func (in *baseInode) ID() fuseops.InodeID {
	return in.id
}

// Path returns a path that maps to an inode.
// The file system does not support hard links,
// so there is only one path for each inode.
// NOTE: Path does not start with a separator.
func (in *baseInode) Path() string {
	return in.path
}

// Attributes returns inode's attributes (mode, size, atime...).
// REQUIRES_READ_LOCK(in)
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
// REQUIRES_READ_LOCK(in)
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

// Destroy releases all resources allocated by the inode.
func (*baseInode) Destroy() error {
	return nil
}
