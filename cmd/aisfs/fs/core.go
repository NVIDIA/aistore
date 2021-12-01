// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"context"
	"log"
	"net/http"
	"os"
	"path"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmd/aisfs/ais"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

// Locking order:
//  - Lock inodes before locking the file system.
//  - When locking multiple inodes, lock them in the ascending
//    order of their IDs.
//  - Lock handles before locking inodes.
//  - When locking multiple handles, lock them in the ascending
//    order of their IDs.

const (
	Name = "aisfs"

	rootPath       = ""
	invalidInodeID = fuseops.InodeID(fuseops.RootInodeID + 1)
)

var (
	ns *namespace // Namespace for files and directories.

	glMem2 *memsys.MMSA // Global memory manager
)

func init() {
	glMem2 = memsys.PageMM()
}

type (
	ServerConfig struct {
		// Mount
		MountPath string

		// Cluster
		AISURL        string
		BucketName    string
		SkipVerifyCrt bool

		// Access
		Owner *Owner

		// Timeouts, tunables...
		TCPTimeout      time.Duration
		HTTPTimeout     time.Duration
		SyncInterval    atomic.Duration
		MemoryLimit     atomic.Uint64
		MaxWriteBufSize atomic.Int64
	}

	// File system implementation.
	aisfs struct {

		// Embedding this struct ensures that fuseutil.FileSystem is implemented.
		// Every method implementation simply returns fuse.ENOSYS.
		// This struct overrides a subset of methods.
		// If at any time in the future all methods are implemented, this can be removed.
		fuseutil.NotImplementedFileSystem

		// Config
		cfg *ServerConfig

		// Bucket
		bck ais.Bucket

		// HTTP client
		httpClient *http.Client

		// File System
		root        *DirectoryInode
		inodeTable  map[fuseops.InodeID]Inode
		lastInodeID atomic.Uint64

		// Handles
		fileHandles  map[fuseops.HandleID]*fileHandle
		lastHandleID atomic.Uint64

		// Access
		modeBits *ModeBits

		// Logging
		errLog *log.Logger

		// Guard
		mu sync.RWMutex
	}
)

func NewAISFileSystemServer(cfg *ServerConfig, errLog *log.Logger) (srv fuse.Server, err error) {
	// Init HTTP client.
	httpClient := cmn.NewClient(cmn.TransportArgs{
		DialTimeout: cfg.TCPTimeout,
		Timeout:     cfg.HTTPTimeout,
		UseHTTPS:    cos.IsHTTPS(cfg.AISURL),
		SkipVerify:  cfg.SkipVerifyCrt,
	})

	// Create an aisfs instance.
	aisfs := &aisfs{
		// Config
		cfg: cfg,

		// HTTP client
		httpClient: httpClient,

		// File System
		inodeTable:  make(map[fuseops.InodeID]Inode),
		lastInodeID: *atomic.NewUint64(uint64(invalidInodeID)),

		// Handles
		fileHandles:  make(map[fuseops.HandleID]*fileHandle),
		lastHandleID: *atomic.NewUint64(0),

		// Access
		modeBits: &ModeBits{
			File:      FilePermissionBits,
			Directory: DirectoryPermissionBits | os.ModeDir,
		},

		// Logging
		errLog: errLog,
	}

	// Initialize a bucket.
	apiParams := aisfs.aisAPIParams()
	bucket, err := ais.NewBucket(cfg.BucketName, apiParams)
	if err != nil {
		return nil, err
	}
	aisfs.bck = bucket

	// Create the root inode.
	aisfs.root = NewDirectoryInode(
		fuseops.RootInodeID,
		aisfs.dirAttrs(aisfs.modeBits.Directory),
		rootPath,
		nil, /*parent*/
		aisfs.bck,
	).(*DirectoryInode)

	aisfs.root.IncLookupCount()
	aisfs.inodeTable[fuseops.RootInodeID] = aisfs.root

	ns, err = newNamespace(bucket, aisfs.errLog, aisfs.cfg)
	if err != nil {
		return nil, err
	}
	return fuseutil.NewFileSystemServer(aisfs), nil
}

// API parameters needed to talk to the cluster
func (fs *aisfs) aisAPIParams() api.BaseParams {
	return api.BaseParams{
		Client: fs.httpClient,
		URL:    fs.cfg.AISURL,
	}
}

func (fs *aisfs) nextInodeID() fuseops.InodeID {
	return fuseops.InodeID(fs.lastInodeID.Inc())
}

func (fs *aisfs) nextHandleID() fuseops.HandleID {
	return fuseops.HandleID(fs.lastHandleID.Inc())
}

// Assumes that object != nil
func (fs *aisfs) fileAttrs(object *ais.Object, mode os.FileMode) fuseops.InodeAttributes {
	// Nlink will always be 1, the filesystem does not support hard links.
	return fuseops.InodeAttributes{
		Mode:  mode,
		Nlink: 1,
		Size:  uint64(object.Size),
		Uid:   fs.cfg.Owner.UID,
		Gid:   fs.cfg.Owner.GID,
		Atime: object.Atime,
		Mtime: object.Atime,
		Ctime: object.Atime,
	}
}

func (fs *aisfs) dirAttrs(mode os.FileMode) fuseops.InodeAttributes {
	// Size of the directory will be 0. Size greater than 0 only makes
	// sense if directory entries are persisted somewhere, which is not
	// the case here. It's similar with virtual file systems like /proc:
	// `ls -ld /proc` shows directory size to be 0.
	//
	// Nlink will always be 1, the filesystem does not support hard links.
	return fuseops.InodeAttributes{
		Mode:  mode,
		Nlink: 1,
		Size:  0,
		Uid:   fs.cfg.Owner.UID,
		Gid:   fs.cfg.Owner.GID,
	}
}

// REQUIRES_LOCK(fs.mu), READ_LOCKS(file)
func (fs *aisfs) allocateFileHandle(file *FileInode) fuseops.HandleID {
	id := fs.nextHandleID()
	file.RLock()
	fs.fileHandles[id] = newFileHandle(id, file)
	file.RUnlock()
	return id
}

// REQUIRES_READ_LOCK(fs.mu)
func (fs *aisfs) lookupMustExist(id fuseops.InodeID) Inode {
	inode, ok := fs.inodeTable[id]
	if !ok {
		fs.fatalf("inode lookup: failed to find %d\n", id)
	}
	return inode
}

// REQUIRES_READ_LOCK(fs.mu)
func (fs *aisfs) lookupDirMustExist(id fuseops.InodeID) *DirectoryInode {
	inode := fs.lookupMustExist(id)
	dirInode, ok := inode.(*DirectoryInode)
	if !ok {
		fs.fatalf("directory inode lookup: %d not a directory\n", id)
	}
	return dirInode
}

// REQUIRES_READ_LOCK(fs.mu)
func (fs *aisfs) lookupFileMustExist(id fuseops.InodeID) *FileInode {
	inode := fs.lookupMustExist(id)
	fileInode, ok := inode.(*FileInode)
	if !ok {
		fs.fatalf("file inode lookup: %d not a file\n", id)
	}
	return fileInode
}

// REQUIRES_READ_LOCK(fs.mu)
func (fs *aisfs) lookupFhandleMustExist(id fuseops.HandleID) *fileHandle {
	handle, ok := fs.fileHandles[id]
	if !ok {
		fs.fatalf("file handle lookup: failed to find %d\n", id)
	}
	return handle
}

// REQUIRES_LOCK(fs.mu)
func (fs *aisfs) createFileInode(inodeID fuseops.InodeID, parent *DirectoryInode, object *ais.Object, mode os.FileMode) Inode {
	attrs := fs.fileAttrs(object, mode)
	inode := NewFileInode(inodeID, attrs, parent, object)
	fs.inodeTable[inodeID] = inode
	return inode
}

// REQUIRES_LOCK(fs.mu)
func (fs *aisfs) createDirectoryInode(inodeID fuseops.InodeID, parent *DirectoryInode, entryName string, mode os.FileMode) Inode {
	attrs := fs.dirAttrs(mode)
	fspath := path.Join(parent.Path(), entryName) + separator
	inode := NewDirectoryInode(inodeID, attrs, fspath, parent, fs.bck)
	fs.inodeTable[inodeID] = inode
	return inode
}

////////////////////////////////
// FileSystem interface methods
////////////////////////////////

func (fs *aisfs) GetInodeAttributes(_ context.Context, req *fuseops.GetInodeAttributesOp) (err error) {
	fs.mu.RLock()
	inode := fs.lookupMustExist(req.Inode)
	fs.mu.RUnlock()

	inode.RLock()
	req.Attributes = inode.Attributes()
	inode.RUnlock()
	return
}

func (fs *aisfs) SetInodeAttributes(_ context.Context, req *fuseops.SetInodeAttributesOp) (err error) {
	fs.mu.RLock()
	inode := fs.lookupMustExist(req.Inode)
	fs.mu.RUnlock()

	inode.Lock()
	updReq := &AttrUpdateReq{
		Mode:  req.Mode,
		Size:  req.Size,
		Atime: req.Atime,
		Mtime: req.Mtime,
	}
	req.Attributes = inode.UpdateAttributes(updReq)
	inode.Unlock()
	return
}

func (fs *aisfs) LookUpInode(_ context.Context, req *fuseops.LookUpInodeOp) (err error) {
	var inode Inode

	fs.mu.RLock()
	parent := fs.lookupDirMustExist(req.Parent)
	fs.mu.RUnlock()

	result := parent.LookupEntry(req.Name)
	if result.NoEntry() {
		return fuse.ENOENT
	}

	fs.mu.Lock()
	if result.NoInode() {
		inodeID := fs.nextInodeID()
		if !result.IsDir() {
			inode = fs.createFileInode(inodeID, parent, result.Object, fs.modeBits.File)
		} else {
			inode = fs.createDirectoryInode(inodeID, parent, result.Entry.Name, fs.modeBits.Directory)
		}

		parent.Lock()
		if !result.IsDir() {
			parent.NewFileEntry(req.Name, inode.ID(), result.Object)
		} else {
			parent.NewDirEntry(req.Name, inode.ID())
		}
		parent.Unlock()
	} else {
		// lookup inode and update if needed
		inode = fs.lookupMustExist(result.Entry.Inode)
		if !inode.IsDir() {
			inode.Lock()
			file := inode.(*FileInode)
			file.UpdateBackingObject(result.Object)
			inode.Unlock()
		}
	}
	fs.mu.Unlock()

	// Locking this inode with parent already locked doesn't break
	// the valid locking order since (currently) child inodes
	// have higher ID than their respective parent inodes.
	inode.RLock()
	req.Entry = inode.AsChildEntry()
	inode.RUnlock()
	inode.IncLookupCount()
	return
}

func (fs *aisfs) ForgetInode(_ context.Context, req *fuseops.ForgetInodeOp) (err error) {
	fs.mu.RLock()
	inode := fs.lookupMustExist(req.Inode)
	fs.mu.RUnlock()

	if lookupCnt := inode.DecLookupCountN(req.N); lookupCnt == 0 {
		// The kernel will never use this inode again, we can destroy it.

		// Acquire locks in the correct order.
		parent := inode.Parent().(*DirectoryInode)
		inode.Lock()

		fs.mu.Lock()
		// Remove it from the inode table.
		delete(fs.inodeTable, req.Inode)
		fs.mu.Unlock()

		// Remove entryName to inode ID mapping in parent.
		name := path.Base(inode.Path())
		parent.Lock()
		parent.InvalidateInode(name, inode.IsDir())
		parent.Unlock()

		// Any future cleanup related to inode goes here.
		if err := inode.Destroy(); err != nil {
			fs.logf("error destroying inode %d: %v", req.Inode, err)
		}

		inode.Unlock()
	}

	return
}
