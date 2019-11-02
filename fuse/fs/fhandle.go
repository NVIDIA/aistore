// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/jacobsa/fuse/fuseops"
)

const blockSize = 128 * cmn.KiB

// TODO: optimize buffer allocations
type blockReader struct {
	file    *FileInode
	block   *bytes.Reader
	blockNo int64
	valid   bool
}

func newBlockReader(file *FileInode) *blockReader {
	return &blockReader{
		file:  file,
		valid: false,
	}
}

func (r *blockReader) load(blockNo int64) (err error) {
	r.block, _, err = r.file.Load(blockNo*blockSize, blockSize)
	if err != nil {
		r.valid, r.block = false, nil
		return err
	}
	r.valid, r.blockNo = true, blockNo
	return nil
}

func (r *blockReader) read(dst []byte, blockNo int64, blockOffset int64) (n int, err error) {
	if !r.valid || r.blockNo != blockNo {
		if err = r.load(blockNo); err != nil {
			return
		}
	}

	n, err = r.block.ReadAt(dst, blockOffset)
	if err == io.EOF {
		err = nil
	}
	return
}

type fileHandle struct {
	// Handle ID
	id fuseops.HandleID

	// File inode that this handle is tied to
	file *FileInode

	mu sync.Mutex

	// Reading
	blockReader *blockReader

	// Writing
	dirty       bool
	size        uint64
	writeBuffer []byte
}

func newFileHandle(id fuseops.HandleID, file *FileInode) *fileHandle {
	return &fileHandle{
		id:          id,
		file:        file,
		blockReader: newBlockReader(file),
	}
}

func (fh *fileHandle) readChunk(dst []byte, offset int64) (n int, err error) {
	var (
		firstBlock = offset / blockSize
		lastBlock  = (offset + int64(len(dst)) - 1) / blockSize
	)

	fh.mu.Lock()
	defer fh.mu.Unlock()

	// Common case, read from one block
	if firstBlock == lastBlock {
		blockOffset := offset % blockSize
		n, err = fh.blockReader.read(dst, firstBlock, blockOffset)
		return
	}

	// Read from two adjacent blocks directly from file (object)
	n, err = fh.file.Read(dst, offset)
	return
}

func (fh *fileHandle) writeChunk(data []byte, offset uint64) error {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	// Allow only appending for now
	if offset != fh.size {
		return fmt.Errorf("write file (inode %d): random access write not yet implemented", fh.file.ID())
	}

	if fh.size == 0 {
		fh.writeBuffer = make([]byte, 0, len(data))
		fh.dirty = true
	}
	fh.writeBuffer = append(fh.writeBuffer, data...)
	fh.size += uint64(len(data))
	return nil
}

func (fh *fileHandle) flush() error {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	if !fh.dirty {
		return nil
	}
	return fh.file.Write(fh.writeBuffer, fh.size)
}
