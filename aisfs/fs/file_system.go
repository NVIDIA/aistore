// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"context"
	"log"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/NVIDIA/aistore/aisfs/ais"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

///////////////////////////////////////////////////////////////
//
// Locking order:
//  - Lock inodes before locking the file system.
//  - When locking multiple inodes, lock them in the ascending
//    order of their IDs.
//
///////////////////////////////////////////////////////////////

const (
	Name = "aisfs"

	rootInodeName = ""
	dummyInodeID  = fuseops.InodeID(fuseops.RootInodeID + 1)
)

// File system implementation.
type aisfs struct {
	// Embedding this struct ensures that fuseutil.FileSystem is implemented.
	// Every method implementation simply returns fuse.ENOSYS.
	// This struct overrides a subset of methods.
	// If at any time in the future all methods are implemented, this can be removed.
	fuseutil.NotImplementedFileSystem

	// Cluster
	bucket *ais.Bucket

	// File System
	mountPath  string
	root       *DirectoryInode
	inodeTable map[fuseops.InodeID]Inode

	// Handles
	fileHandles  map[fuseops.HandleID]*fileHandle
	dirHandles   map[fuseops.HandleID]*dirHandle
	lastInodeID  uint64
	lastHandleID uint64

	// Access
	owner    *Owner
	modeBits *ModeBits

	// Logging
	errLog *log.Logger

	// Guards
	mu sync.Mutex
}

func NewAISFileSystemServer(mountPath, clusterURL, bucketName string, owner *Owner, errLog *log.Logger) fuse.Server {
	bucket := ais.OpenBucket(clusterURL, bucketName)

	aisfs := &aisfs{
		// Cluster
		bucket: bucket,

		// File System
		mountPath:  mountPath,
		inodeTable: make(map[fuseops.InodeID]Inode),

		// Handles
		fileHandles:  make(map[fuseops.HandleID]*fileHandle),
		dirHandles:   make(map[fuseops.HandleID]*dirHandle),
		lastInodeID:  uint64(dummyInodeID),
		lastHandleID: uint64(0),

		// Permissions
		owner: owner,
		modeBits: &ModeBits{
			File:      FilePermissionBits,
			Directory: DirectoryPermissionBits | os.ModeDir,
		},

		// Logging
		errLog: errLog,
	}

	// Create the root inode.
	aisfs.root = NewDirectoryInode(
		fuseops.RootInodeID,
		rootInodeName,
		aisfs.dirAttrs(aisfs.modeBits.Directory),
		nil, /* parent */
		bucket).(*DirectoryInode)

	aisfs.root.IncLookupCount()
	aisfs.inodeTable[fuseops.RootInodeID] = aisfs.root

	return fuseutil.NewFileSystemServer(aisfs)
}

func (fs *aisfs) nextInodeID() fuseops.InodeID {
	return fuseops.InodeID(atomic.AddUint64(&fs.lastInodeID, 1))
}

// Assumes that object != nil
func (fs *aisfs) fileAttrs(object *ais.Object) fuseops.InodeAttributes {
	return fuseops.InodeAttributes{
		Nlink: 1,
		Size:  uint64(object.Size),
		Mode:  fs.modeBits.File,
		Uid:   fs.owner.UID,
		Gid:   fs.owner.GID,
		Atime: object.Atime,
		Mtime: object.Atime,
		Ctime: object.Atime,
	}
}

func (fs *aisfs) dirAttrs(mode os.FileMode) fuseops.InodeAttributes {
	// The size of the directory will be 0. Size greater than 0 only makes
	// sense if directory entries are persisted somewhere, which is not
	// the case here. It's similar with virtual file systems like /proc:
	// `ls -ld /proc` shows directory size to be 0.
	return fuseops.InodeAttributes{
		Nlink: 1,
		Size:  0,
		Mode:  mode,
		Uid:   fs.owner.UID,
		Gid:   fs.owner.GID,
	}
}

// REQUIRES_LOCK(fs.mu)
func (fs *aisfs) allocateDirHandle(dir *DirectoryInode) fuseops.HandleID {
	handleID := fuseops.HandleID(atomic.AddUint64(&fs.lastHandleID, 1))
	fs.dirHandles[handleID] = &dirHandle{
		bucket: fs.bucket,
		id:     handleID,
		dir:    dir,
	}
	return handleID
}

// REQUIRES_LOCK(fs.mu)
func (fs *aisfs) findInode(id fuseops.InodeID) Inode {
	inode, ok := fs.inodeTable[id]
	if !ok {
		fs.fatalf("inode lookup: failed to find %d\n", id)
	}
	return inode
}

// REQUIRES_LOCK(fs.mu)
func (fs *aisfs) findDir(id fuseops.InodeID) *DirectoryInode {
	inode := fs.findInode(id)
	dirInode, ok := inode.(*DirectoryInode)
	if !ok {
		fs.fatalf("inode lookup: failed to find directory %d\n", id)
	}
	return dirInode
}

// REQUIRES_LOCK(fs.mu)
func (fs *aisfs) findFile(id fuseops.InodeID) *FileInode {
	inode := fs.findInode(id)
	fileInode, ok := inode.(*FileInode)
	if !ok {
		fs.fatalf("inode lookup: failed to find file %d\n", id)
	}
	return fileInode
}

// REQUIRES_LOCK(fs.mu)
func (fs *aisfs) findDirHandle(id fuseops.HandleID) *dirHandle {
	handle, ok := fs.dirHandles[id]
	if !ok {
		fs.fatalf("lookup directory handle: failed to find handle %d\n", id)
	}
	return handle
}

// REQUIRES_LOCKS(fs.mu, parent)
func (fs *aisfs) createFileInode(parent *DirectoryInode, object *ais.Object) Inode {
	inodeID := fs.nextInodeID()
	attrs := fs.fileAttrs(object)
	inode := NewFileInode(inodeID, attrs, parent, object)
	fs.inodeTable[inodeID] = inode
	return inode
}

// REQUIRES_LOCKS(fs.mu, parent)
func (fs *aisfs) createDirectoryInode(parent *DirectoryInode, name string, mode os.FileMode) Inode {
	inodeID := fs.nextInodeID()
	inodeName := path.Join(parent.Name(), name) + separator
	attrs := fs.dirAttrs(mode)
	inode := NewDirectoryInode(inodeID, inodeName, attrs, parent, fs.bucket)
	fs.inodeTable[inodeID] = inode
	return inode
}

/////////////////////////////////
// FILE SYSTEM INTERFACE METHODS
/////////////////////////////////

func (fs *aisfs) GetInodeAttributes(ctx context.Context, req *fuseops.GetInodeAttributesOp) (err error) {
	fs.mu.Lock()
	inode := fs.findInode(req.Inode)
	fs.mu.Unlock()

	inode.Lock()
	req.Attributes = inode.Attributes()
	inode.Unlock()
	return
}

func (fs *aisfs) LookUpInode(ctx context.Context, req *fuseops.LookUpInodeOp) (err error) {
	var (
		inode     Inode
		parent    *DirectoryInode
		lookupRes EntryLookupResult
	)

	fs.mu.Lock()
	parent = fs.findDir(req.Parent)
	fs.mu.Unlock()

	parent.Lock()
	defer func() {
		parent.Unlock()
		if inode != nil {
			inode.IncLookupCount()
		}
	}()

	lookupRes, err = parent.LookupEntry(req.Name)
	if err != nil {
		fs.logf("dentry lookup of %q in %d: %v", req.Name, req.Parent, err)
		return fuse.EIO
	}

	if lookupRes.NoEntry() {
		return fuse.ENOENT
	}

	fs.mu.Lock()
	if lookupRes.NoInode() {
		if lookupRes.IsDir() {
			inode = fs.createDirectoryInode(parent, lookupRes.Entry.Name, fs.modeBits.Directory)
		} else {
			inode = fs.createFileInode(parent, lookupRes.Object)
		}
	} else {
		inode = fs.findInode(lookupRes.Entry.Inode)
	}
	fs.mu.Unlock()

	parent.NewEntry(req.Name, inode.ID())

	req.Entry.Child = inode.ID()

	// Locking this inode with parent doesn't break the valid locking order
	// since (currently) child inodes have higher ID than their respective
	// parent inodes.
	inode.Lock()
	req.Entry.Attributes = inode.Attributes()
	inode.Unlock()
	return
}

func (fs *aisfs) ForgetInode(ctx context.Context, req *fuseops.ForgetInodeOp) (err error) {
	fs.mu.Lock()
	inode := fs.findInode(req.Inode)
	fs.mu.Unlock()
	inode.DecLookupCountN(req.N)
	return
}

// OpenDir creates a directory handle to be used in subsequent directory operations
// that provide a valid handle ID (also genereted by this function).
func (fs *aisfs) OpenDir(ctx context.Context, req *fuseops.OpenDirOp) (err error) {
	fs.mu.Lock()
	dir := fs.findDir(req.Inode)
	req.Handle = fs.allocateDirHandle(dir)
	fs.mu.Unlock()
	return
}

func (fs *aisfs) ReadDir(ctx context.Context, req *fuseops.ReadDirOp) (err error) {
	fs.mu.Lock()
	dh := fs.findDirHandle(req.Handle)
	fs.mu.Unlock()
	req.BytesRead, err = dh.readEntries(req.Offset, req.Dst)
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

func (fs *aisfs) OpenFile(ctx context.Context, req *fuseops.OpenFileOp) (err error) {
	return fuse.ENOSYS
}

func (fs *aisfs) ReadFile(ctx context.Context, req *fuseops.ReadFileOp) (err error) {
	fs.mu.Lock()
	file := fs.findFile(req.Inode)
	fs.mu.Unlock()

	file.Lock()
	req.BytesRead, err = file.Read(req.Dst, req.Offset, len(req.Dst))
	file.Unlock()

	if err != nil {
		fs.logf("Reading file %s: %v", file.Name(), err)
	}
	return
}

func (fs *aisfs) MkDir(ctx context.Context, req *fuseops.MkDirOp) (err error) {
	var (
		parent    *DirectoryInode
		newDir    Inode
		lookupRes EntryLookupResult
	)

	fs.mu.Lock()
	parent = fs.findDir(req.Parent)
	fs.mu.Unlock()

	parent.Lock()
	defer func() {
		parent.Unlock()
		if newDir != nil {
			newDir.IncLookupCount()
		}
	}()

	lookupRes, err = parent.LookupEntry(req.Name)
	if err != nil {
		fs.logf("dentry lookup of %q in %d: %v", req.Name, req.Parent, err)
		return fuse.EIO
	}

	// If parent directory already contains an entry with req.Name
	// it is not possible to create a new directory with the same name.
	if !lookupRes.NoEntry() {
		return fuse.EEXIST
	}

	fs.mu.Lock()
	newDir = fs.createDirectoryInode(parent, req.Name, req.Mode)
	fs.mu.Unlock()

	parent.NewEmptyDirEntry(req.Name, newDir.ID())

	req.Entry.Child = newDir.ID()

	// Locking this inode with parent doesn't break the valid locking order
	// since (currently) child inodes have higher ID than their respective
	// parent inodes.
	newDir.Lock()
	req.Entry.Attributes = newDir.Attributes()
	newDir.Unlock()
	return
}

func (fs *aisfs) Unlink(ctx context.Context, op *fuseops.UnlinkOp) (err error) {
	fs.mu.Lock()
	parent := fs.findDir(op.Parent)
	fs.mu.Unlock()

	parent.Lock()
	defer parent.Unlock()

	lookupRes, err := parent.LookupEntry(op.Name)
	if err != nil {
		fs.logf("dentry lookup of %q in %d: %v", op.Name, op.Parent, err)
		return fuse.EIO
	}
	if lookupRes.NoEntry() || lookupRes.NoInode() {
		return fuse.ENOENT
	}
	if lookupRes.IsDir() {
		fs.logf("tried to unlink directory: %q in %d", op.Name, op.Parent)
		return syscall.EISDIR
	}
	return parent.UnlinkEntry(op.Name)
}
