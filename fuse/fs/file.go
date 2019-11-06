// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"context"
	"io"
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

	fs.mu.RLock()
	parent := fs.lookupDirMustExist(req.Parent)
	fs.mu.RUnlock()

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

	// Locking this inode with parent already locked doesn't break
	// the valid locking order since (currently) child inodes
	// have higher ID than their respective parent inodes.
	newFile.RLock()
	req.Entry = newFile.AsChildEntry()
	newFile.RUnlock()
	return
}

func (fs *aisfs) ReadFile(ctx context.Context, req *fuseops.ReadFileOp) (err error) {
	fs.mu.RLock()
	fhandle := fs.lookupFhandleMustExist(req.Handle)
	fs.mu.RUnlock()

	req.BytesRead, err = fhandle.readChunk(req.Dst, req.Offset)

	// As required by FUSE, io.EOF should not be reported as an error.
	if err == io.EOF {
		err = nil
	}

	if err != nil {
		return fs.handleIOError(err)
	}

	return
}

func (fs *aisfs) WriteFile(ctx context.Context, req *fuseops.WriteFileOp) (err error) {
	fs.mu.RLock()
	handle := fs.lookupFhandleMustExist(req.Handle)
	fs.mu.RUnlock()

	err = handle.writeChunk(req.Data, uint64(req.Offset))
	if err != nil {
		return fs.handleIOError(err)
	}

	return
}

func (fs *aisfs) FlushFile(ctx context.Context, req *fuseops.FlushFileOp) (err error) {
	fs.mu.RLock()
	handle := fs.lookupFhandleMustExist(req.Handle)
	fs.mu.RUnlock()

	if err = handle.flush(); err != nil {
		return fs.handleIOError(err)
	}

	return
}

func (fs *aisfs) ReleaseFileHandle(ctx context.Context, req *fuseops.ReleaseFileHandleOp) (err error) {
	fs.mu.Lock()

	// Lookup and release the handle's resources.
	fhandle := fs.lookupFhandleMustExist(req.Handle)
	fhandle.destroy()

	// Remove the handle from the file handles table.
	delete(fs.fileHandles, req.Handle)

	fs.mu.Unlock()
	return
}

func (fs *aisfs) Unlink(ctx context.Context, req *fuseops.UnlinkOp) (err error) {
	fs.mu.RLock()
	parent := fs.lookupDirMustExist(req.Parent)
	fs.mu.RUnlock()

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
