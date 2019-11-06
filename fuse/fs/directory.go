// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"context"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
)

// OpenDir creates a directory handle to be used in subsequent directory operations
// that provide a valid handle ID (also genereted here).
func (fs *aisfs) OpenDir(ctx context.Context, req *fuseops.OpenDirOp) (err error) {
	fs.mu.Lock()
	dir := fs.lookupDirMustExist(req.Inode)
	req.Handle = fs.allocateDirHandle(dir)
	fs.mu.Unlock()
	return
}

func (fs *aisfs) ReadDir(ctx context.Context, req *fuseops.ReadDirOp) (err error) {
	fs.mu.RLock()
	dh := fs.lookupDhandleMustExist(req.Handle)
	fs.mu.RUnlock()

	req.BytesRead, err = dh.readEntries(req.Offset, req.Dst)
	if err != nil {
		return fs.handleIOError(err)
	}

	return
}

// ReleaseDirHandle removes a previously issued directory handle because the handle
// will not be used in subsequent directory operations.
func (fs *aisfs) ReleaseDirHandle(ctx context.Context, req *fuseops.ReleaseDirHandleOp) (err error) {
	fs.mu.Lock()
	delete(fs.dirHandles, req.Handle)
	fs.mu.Unlock()
	return
}

func (fs *aisfs) MkDir(ctx context.Context, req *fuseops.MkDirOp) (err error) {
	var newDir Inode

	fs.mu.RLock()
	parent := fs.lookupDirMustExist(req.Parent)
	fs.mu.RUnlock()

	parent.Lock()
	defer func() {
		parent.Unlock()
		if newDir != nil {
			newDir.IncLookupCount()
		}
	}()

	result, err := parent.LookupEntry(req.Name)
	if err != nil {
		return fs.handleIOError(err)
	}

	// If parent directory already contains an entry with req.Name
	// it is not possible to create a new directory with the same name.
	if !result.NoEntry() {
		return fuse.EEXIST
	}

	fs.mu.Lock()
	newDir = fs.createDirectoryInode(parent, req.Mode, req.Name)
	fs.mu.Unlock()

	parent.LinkLocalSubdir(req.Name, newDir.ID())
	parent.NewEntry(req.Name, newDir.ID())

	// Locking this inode with parent already locked doesn't break
	// the valid locking order since (currently) child inodes
	// have higher ID than their respective parent inodes.
	newDir.RLock()
	req.Entry = newDir.AsChildEntry()
	newDir.RUnlock()
	return
}

func (fs *aisfs) RmDir(ctx context.Context, req *fuseops.RmDirOp) (err error) {
	fs.mu.RLock()
	parent := fs.lookupDirMustExist(req.Parent)
	fs.mu.RUnlock()

	parent.Lock()
	defer parent.Unlock()

	result, err := parent.LookupEntry(req.Name)
	if err != nil {
		return fs.handleIOError(err)
	}

	if result.NoEntry() {
		return fuse.ENOENT
	}

	if !result.IsDir() {
		return fuse.ENOTDIR
	}

	if ok := parent.TryDeleteLocalSubdirEntry(req.Name); !ok {
		// if directory is not local, then it is not empty
		return fuse.ENOTEMPTY
	}

	return
}
