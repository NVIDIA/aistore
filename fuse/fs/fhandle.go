// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"io"
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/jacobsa/fuse/fuseops"
)

type fileHandle struct {
	// Handle ID
	id fuseops.HandleID

	// File inode that this handle is tied to
	file     *FileInode
	fileSize int64

	// Guard
	mu sync.Mutex

	// Reading
	readBuffer *BlockBuffer

	// Writing - Note: not yet fully implemented
	dirty       bool
	wsize       uint64
	writeBuffer []byte
}

// REQUIRES_LOCK(file)
func newFileHandle(id fuseops.HandleID, file *FileInode) *fileHandle {
	return &fileHandle{
		id:       id,
		file:     file,
		fileSize: int64(file.Size()),
	}
}

// REQUIRES_LOCK(fh.mu)
func (fh *fileHandle) destroy() {
	if fh.readBuffer != nil {
		fh.readBuffer.Free()
	}
}

///////////
// READING
///////////

// REQUIRES_LOCK(fh.mu), LOCKS(fh.file)
func (fh *fileHandle) ensureReadBuffer() int64 {
	if fh.readBuffer != nil {
		return fh.readBuffer.BlockSize()
	}

	var blockSize int64
	if fh.fileSize < MinBlockSize {
		blockSize = MinBlockSize
	} else if fh.fileSize > MaxBlockSize {
		blockSize = MaxBlockSize
	} else {
		blockSize = (fh.fileSize + cmn.PageSize - 1) & cmn.PageSize
	}

	fh.readBuffer = NewBlockBuffer(blockSize)
	return blockSize
}

// LOCKS(fh.mu)
func (fh *fileHandle) readChunk(dst []byte, offset int64) (n int, err error) {
	if offset >= fh.fileSize {
		return 0, io.EOF
	}

	// Lock the handler in order to read
	fh.mu.Lock()
	defer fh.mu.Unlock()

	// Ensure that buffer is ready for reading
	blockSize := fh.ensureReadBuffer()
	dstLen := len(dst)

	for {
		blockNo := offset / blockSize
		blockOffset := offset % blockSize

		err = fh.readBuffer.EnsureBlock(blockNo, fh.file.Load)
		if err != nil {
			// In case of error is encountered while loading a block,
			// return the number of bytes read so far.
			break
		}

		var nread int
		nread, err = fh.readBuffer.ReadAt(dst[n:], blockOffset)

		n += nread
		if n == dstLen {
			// Read enough bytes, stop.
			break
		}

		offset += int64(nread)
		if err == io.EOF {
			// Error io.EOF can indicate either end of file or end of block.
			if offset == fh.fileSize {
				// End of file reached.
				break
			}
			// End of block, but not end of file reached, continue reading
			// from next block.
			err = nil
			continue
		}

		// In case of any other error encountered while reading a block,
		// return the number of bytes read so far.
		if err != nil {
			break
		}

		// No errors, not end of block, continue reading from the next block.
	}

	return
}

///////////
// WRITING
///////////

func (fh *fileHandle) writeChunk(data []byte, offset uint64) error {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	// Allow only appending for now
	if offset != fh.wsize {
		return fmt.Errorf("write file (inode %d): random access write not yet implemented", fh.file.ID())
	}

	if fh.wsize == 0 {
		fh.writeBuffer = make([]byte, 0, len(data))
		fh.dirty = true
	}
	fh.writeBuffer = append(fh.writeBuffer, data...)
	fh.wsize += uint64(len(data))
	return nil
}

func (fh *fileHandle) flush() error {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	if !fh.dirty {
		return nil
	}
	return fh.file.Write(fh.writeBuffer, fh.wsize)
}
