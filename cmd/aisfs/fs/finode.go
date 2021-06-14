// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"io"
	"time"

	"github.com/NVIDIA/aistore/cmd/aisfs/ais"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/jacobsa/fuse/fuseops"
)

// interface guard
var _ Inode = (*FileInode)(nil)

type FileInode struct {
	baseInode

	parent *DirectoryInode

	// Object used by current inode. When possible it should be updated with
	// newer version.
	object ais.Object
}

func NewFileInode(id fuseops.InodeID, attrs fuseops.InodeAttributes, parent *DirectoryInode, object *ais.Object) Inode {
	return &FileInode{
		baseInode: newBaseInode(id, attrs, object.Name),
		parent:    parent,
		object:    *object,
	}
}

func (file *FileInode) Parent() Inode {
	return file.parent
}

func (*FileInode) IsDir() bool {
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
	// Only update object if it is newer
	if file.object.Atime.After(obj.Atime) {
		return
	}

	size := uint64(obj.Size)
	updReq := &AttrUpdateReq{
		Size: &size,
	}
	file.UpdateAttributes(updReq)
	file.object = *obj
}

/////////////
// READING //
/////////////

// REQUIRES_READ_LOCK(file)
func (file *FileInode) Load(w io.Writer, offset, length int64) (n int64, err error) {
	n, err = file.object.GetChunk(w, offset, length)
	if err != nil {
		return 0, err
	}
	now := time.Now()
	file.attrs.Atime = now
	file.object.Atime = now
	return n, nil
}

/////////////
// WRITING //
/////////////

// REQUIRES_LOCK(file)
func (file *FileInode) Write(r cos.ReadOpenCloser, handle string, size int64) (string, error) {
	newHandle, err := file.object.Append(r, handle, size)
	if err != nil {
		return newHandle, err
	}
	file.object.Size += size
	return newHandle, nil
}

// REQUIRES_LOCK(file)
func (file *FileInode) Flush(handle string, cksum *cos.Cksum) error {
	err := file.object.Flush(handle, cksum)
	if err != nil {
		return err
	}
	now := time.Now()
	file.object.Atime = now
	file.attrs.Atime = now
	file.attrs.Mtime = now
	return nil
}
