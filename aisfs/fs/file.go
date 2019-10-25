// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"context"
	"path"
	"syscall"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
)

// OpenFile creates a file handle to be used in subsequent file operations
// that provide a valid handle ID (also generated here).
func (fs *aisfs) OpenFile(ctx context.Context, req *fuseops.OpenFileOp) (err error) {
	fs.mu.Lock()
	file := fs.lookupFileMustExist(req.Inode)
	req.Handle = fs.allocateFileHandle(file)
	fs.mu.Unlock()
	return
}

func (fs *aisfs) CreateFile(ctx context.Context, req *fuseops.CreateFileOp) (err error) {
	var newFile Inode

	fs.mu.Lock()
	parent := fs.lookupDirMustExist(req.Parent)
	fs.mu.Unlock()

	parent.Lock()
	defer func() {
		parent.Unlock()
		if newFile != nil {
			newFile.IncLookupCount()
		}
	}()

	result, err := parent.LookupEntry(req.Name)
	if err != nil {
		return fs.handleIOError(err)
	}

	if !result.NoEntry() {
		return fuse.EEXIST
	}

	fileName := path.Join(parent.Path(), req.Name)

	object, err := parent.LinkEmptyFile(fileName)
	if err != nil {
		return fs.handleIOError(err)
	}

	fs.mu.Lock()
	newFile = fs.createFileInode(parent, req.Mode, object)
	req.Handle = fs.allocateFileHandle(newFile.(*FileInode))
	fs.mu.Unlock()

	parent.NewEntry(req.Name, newFile.ID())

	// Locking this inode with parent doesn't break the valid locking order
	// since (currently) child inodes have higher ID than their respective
	// parent inodes.
	newFile.Lock()
	req.Entry = newFile.AsChildEntry()
	newFile.Unlock()
	return
}

func (fs *aisfs) ReadFile(ctx context.Context, req *fuseops.ReadFileOp) (err error) {
	fs.mu.Lock()
	fileHandle := fs.lookupFhandleMustExist(req.Handle)
	fs.mu.Unlock()

	fileHandle.file.Lock()
	req.BytesRead, err = fileHandle.file.Read(req.Dst, req.Offset, len(req.Dst))
	fileHandle.file.Unlock()

	if err != nil {
		return fs.handleIOError(err)
	}
	return
}

func (fs *aisfs) WriteFile(ctx context.Context, req *fuseops.WriteFileOp) (err error) {
	fs.mu.Lock()
	handle := fs.lookupFhandleMustExist(req.Handle)
	fs.mu.Unlock()

	err = handle.writeChunk(req.Data, uint64(req.Offset))
	if err != nil {
		return fs.handleIOError(err)
	}

	return
}

func (fs *aisfs) FlushFile(ctx context.Context, req *fuseops.FlushFileOp) (err error) {
	fs.mu.Lock()
	handle := fs.lookupFhandleMustExist(req.Handle)
	fs.mu.Unlock()

	if err = handle.flush(); err != nil {
		return fs.handleIOError(err)
	}

	return
}

func (fs *aisfs) ReleaseFileHandle(ctx context.Context, req *fuseops.ReleaseFileHandleOp) (err error) {
	fs.mu.Lock()
	delete(fs.fileHandles, req.Handle)
	fs.mu.Unlock()
	return
}

func (fs *aisfs) Unlink(ctx context.Context, req *fuseops.UnlinkOp) (err error) {
	fs.mu.Lock()
	parent := fs.lookupDirMustExist(req.Parent)
	fs.mu.Unlock()

	parent.Lock()
	defer parent.Unlock()

	lookupRes, err := parent.LookupEntry(req.Name)
	if err != nil {
		return fs.handleIOError(err)
	}

	if lookupRes.NoEntry() || lookupRes.NoInode() {
		return fuse.ENOENT
	}

	if lookupRes.IsDir() {
		fs.logf("tried to unlink directory: %q in %d", req.Name, req.Parent)
		return syscall.EISDIR
	}

	err = parent.UnlinkEntry(req.Name)
	if err != nil {
		return fs.handleIOError(err)
	}

	return
}
