// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"sync"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

type dirHandle struct {
	// Handle ID
	id fuseops.HandleID

	// Directory inode that this handle is tied to
	dir *DirectoryInode

	// Directory entries
	mu      sync.Mutex
	entries []fuseutil.Dirent
}

func newDirHandle(id fuseops.HandleID, dir *DirectoryInode) *dirHandle {
	return &dirHandle{
		id:  id,
		dir: dir,
	}
}

// REQUIRES_LOCK(dh.mu)
func (dh *dirHandle) fillEntries() error {
	dh.dir.RLock()
	entries, err := dh.dir.ReadEntries()
	dh.dir.RUnlock()
	if err != nil {
		return err
	}
	dh.entries = entries
	return nil
}

// REQUIRES_LOCK(dh.mu)
func (dh *dirHandle) ensureEntires() error {
	if dh.entries != nil {
		return nil
	}
	err := dh.fillEntries()
	return err
}

func (dh *dirHandle) readEntries(offset fuseops.DirOffset, buffer []byte) (bytes int, err error) {
	dh.mu.Lock()
	err = dh.ensureEntires()
	dh.mu.Unlock()
	if err != nil {
		return
	}

	if offset > fuseops.DirOffset(len(dh.entries)) {
		err = fuse.EIO
		return
	}

	for _, en := range dh.entries[offset:] {
		n := fuseutil.WriteDirent(buffer[bytes:], en)
		if n == 0 {
			break
		}
		bytes += n
	}

	return
}
