// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"context"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

func (*aisfs) OpenDir(ctx context.Context, req *fuseops.OpenDirOp) (err error) {
	return
}

func (fs *aisfs) ReadDir(ctx context.Context, req *fuseops.ReadDirOp) (err error) {
	fs.mu.RLock()
	dir := fs.lookupDirMustExist(req.Inode)
	fs.mu.RUnlock()

	dir.Lock()
	entries, err := dir.ReadEntries()
	dir.Unlock()
	if err != nil {
		return fs.handleIOError(err)
	}

	if req.Offset > fuseops.DirOffset(len(entries)) {
		err = fuse.EIO
		return
	}

	for _, en := range entries[req.Offset:] {
		n := fuseutil.WriteDirent(req.Dst[req.BytesRead:], en)
		if n == 0 {
			break
		}
		req.BytesRead += n
	}
	return
}

func (*aisfs) ReleaseDirHandle(ctx context.Context, req *fuseops.ReleaseDirHandleOp) (err error) {
	return
}

func (fs *aisfs) MkDir(ctx context.Context, req *fuseops.MkDirOp) (err error) {
	var newDir Inode

	fs.mu.RLock()
	parent := fs.lookupDirMustExist(req.Parent)
	fs.mu.RUnlock()

	result := parent.LookupEntry(req.Name)
	// If parent directory already contains an entry with req.Name
	// it is not possible to create a new directory with the same name.
	if !result.NoEntry() {
		return fuse.EEXIST
	}

	fs.mu.Lock()
	inodeID := fs.nextInodeID()
	newDir = fs.createDirectoryInode(inodeID, parent, req.Name, req.Mode)
	fs.mu.Unlock()

	parent.Lock()
	parent.NewDirEntry(req.Name, inodeID)
	parent.Unlock()

	// Locking this inode with parent already locked doesn't break
	// the valid locking order since (currently) child inodes
	// have higher ID than their respective parent inodes.
	newDir.RLock()
	req.Entry = newDir.AsChildEntry()
	newDir.RUnlock()
	newDir.IncLookupCount()
	return
}

func (fs *aisfs) RmDir(ctx context.Context, req *fuseops.RmDirOp) (err error) {
	fs.mu.RLock()
	parent := fs.lookupDirMustExist(req.Parent)
	fs.mu.RUnlock()

	result := parent.LookupEntry(req.Name)
	if result.NoEntry() {
		return fuse.ENOENT
	}

	if !result.IsDir() {
		return fuse.ENOTDIR
	}

	fs.mu.RLock()
	dir := fs.lookupDirMustExist(result.Entry.Inode)
	fs.mu.RUnlock()

	parent.Lock()
	defer parent.Unlock()

	dir.Lock()
	entries, err := dir.ReadEntries()
	dir.Unlock()
	if err != nil {
		return fs.handleIOError(err)
	}
	if len(entries) > 0 {
		return fuse.ENOTEMPTY
	}
	parent.ForgetDir(req.Name)
	return
}
