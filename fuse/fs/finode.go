// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"io"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fuse/ais"
	"github.com/jacobsa/fuse/fuseops"
)

// Ensure interface satisfaction.
var _ Inode = &FileInode{}

type FileInode struct {
	baseInode

	parent *DirectoryInode
	object *ais.Object
}

func NewFileInode(id fuseops.InodeID, attrs fuseops.InodeAttributes, parent *DirectoryInode, object *ais.Object) Inode {
	return &FileInode{
		baseInode: newBaseInode(id, attrs, object.Name),
		parent:    parent,
		object:    object,
	}
}

func (file *FileInode) Parent() Inode {
	return file.parent
}

func (file *FileInode) IsDir() bool {
	return false
}

// REQUIRES_READ_LOCK(file)
func (file *FileInode) Size() uint64 {
	return file.attrs.Size
}

// REQUIRES_LOCK(file)
func (file *FileInode) SetSize(size uint64) {
	file.attrs.Size = size
}

/////////////
// READING //
/////////////

// REQUIRES_READ_LOCK(file)
func (file *FileInode) Load(w io.Writer, offset int64, length int64) (n int64, err error) {
	n, err = file.object.GetChunk(w, offset, length)
	if err != nil {
		return 0, err
	}
	return n, nil
}

/////////////
// WRITING //
/////////////

// REQUIRES_LOCK(file)
func (file *FileInode) Write(r cmn.ReadOpenCloser, handle string) (string, error) {
	return file.object.Append(r, handle)
}

// REQUIRES_LOCK(file)
func (file *FileInode) Flush(handle string) error {
	return file.object.Flush(handle)
}
