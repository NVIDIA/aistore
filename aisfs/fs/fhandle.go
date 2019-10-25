// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"sync"

	"github.com/jacobsa/fuse/fuseops"
)

type fileHandle struct {
	// Handle ID
	id fuseops.HandleID

	// File inode that this handle is tied to
	file *FileInode

	// In-memory attributes
	mu     sync.Mutex
	dirty  bool
	size   uint64
	buffer []byte
}

func (fh *fileHandle) writeChunk(data []byte, offset uint64) error {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	// Allow only appending for now
	if offset != fh.size {
		return fmt.Errorf("write file (inode %d): random access write not yet implemented", fh.file.ID())
	}

	if fh.size == 0 {
		fh.buffer = make([]byte, 0, len(data))
		fh.dirty = true
	}
	fh.buffer = append(fh.buffer, data...)
	fh.size += uint64(len(data))
	return nil
}

func (fh *fileHandle) flush() error {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	if !fh.dirty {
		return nil
	}
	return fh.file.Write(fh.buffer, fh.size)
}
