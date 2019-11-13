// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"io"
	"time"

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

// REQUIRES_LOCK(file)
func (file *FileInode) UpdateAttributes(req *AttrUpdateReq) fuseops.InodeAttributes {
	attrs := file.Attributes()

	modified := false
	if req.Mode != nil {
		attrs.Mode = *req.Mode
		modified = true
	}
	if req.Size != nil {
		attrs.Size = *req.Size
		modified = true
	}
	if req.Atime != nil {
		attrs.Atime = *req.Atime
		modified = true
	}
	if req.Mtime != nil {
		attrs.Mtime = *req.Mtime
		modified = true
	}

	if modified {
		attrs.Ctime = time.Now()
		file.SetAttributes(attrs)
	}
	return attrs
}

// REQUIRES_LOCK(file)
func (file *FileInode) UpdateBackingObject(obj *ais.Object) {
	cmn.Assert(obj != nil)
	size := uint64(obj.Size)
	updReq := &AttrUpdateReq{
		Size:  &size,
		Atime: &obj.Atime,
		Mtime: &obj.Atime,
	}
	file.UpdateAttributes(updReq)
	file.object = obj
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
func (file *FileInode) Write(r cmn.ReadOpenCloser, handle string, size int64) (string, error) {
	return file.object.Append(r, handle, size)
}

// REQUIRES_LOCK(file)
func (file *FileInode) Flush(handle string) error {
	return file.object.Flush(handle)
}
