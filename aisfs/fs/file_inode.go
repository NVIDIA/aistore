// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"io"

	"github.com/NVIDIA/aistore/aisfs/ais"
	"github.com/jacobsa/fuse/fuseops"
)

type FileInode struct {
	baseInode
	parent *DirectoryInode
	object *ais.Object
}

// Ensure interface satisfaction.
var _ Inode = &FileInode{}

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

// REQUIRES_LOCK(file)
func (file *FileInode) Read(dst []byte, offset int64, length int) (n int, err error) {
	var (
		chunk    io.Reader
		chunkLen int64
	)

	chunk, chunkLen, err = file.object.DownloadPart(offset, length)
	if err != nil {
		return
	}

	n, err = chunk.Read(dst[:chunkLen])
	if err == io.EOF { // FIXME
		err = nil
	}
	return
}
