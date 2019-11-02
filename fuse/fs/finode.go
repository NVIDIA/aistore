// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"bytes"
	"io"

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

// REQUIRES_LOCK(file)
func (file *FileInode) Load(offset int64, length int64) (r *bytes.Reader, n int64, err error) {
	buffer := bytes.NewBuffer(make([]byte, 0, length))
	n, err = file.object.GetChunk(buffer, offset, length)
	if err != nil {
		return nil, 0, err
	}
	return bytes.NewReader(buffer.Bytes()), n, nil
}

// REQUIRES_LOCK(file)
func (file *FileInode) Read(dst []byte, offset int64) (n int, err error) {
	reader, _, err := file.Load(offset, int64(len(dst)))
	if err != nil {
		return
	}

	n, err = reader.Read(dst)
	if err == io.EOF {
		err = nil
	}
	return
}

// REQUIRES_LOCK(file)
func (file *FileInode) Write(data []byte, size uint64) (err error) {
	return file.object.Put(data, size)
}
