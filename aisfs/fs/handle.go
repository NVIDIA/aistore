// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"sync"

	"github.com/NVIDIA/aistore/aisfs/ais"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

// TODO
type fileHandle struct {
}

type dirHandle struct {
	// Handle ID
	id fuseops.HandleID

	// Directory inode that this handle is tied to
	dir *DirectoryInode

	// Directory entries
	mu      sync.Mutex
	entries []fuseutil.Dirent

	// Bucket
	bucket *ais.Bucket
}

// REQUIRES_LOCK(dh.mu)
func (dh *dirHandle) fillEntries() error {
	entries, err := dh.dir.ReadEntries()
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
