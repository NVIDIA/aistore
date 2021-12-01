// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"io"
	"sync"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/memsys"
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
	readBuffer *blockBuffer

	// Writing
	writeBuffer  *writeBuffer
	dirty        bool
	wsize        uint64
	appendHandle string
}

// REQUIRES_READ_LOCK(file)
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
	if fh.writeBuffer != nil {
		fh.writeBuffer.free()
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
	if fh.fileSize < minBlockSize {
		blockSize = minBlockSize
	} else if fh.fileSize > maxBlockSize {
		blockSize = maxBlockSize
	} else {
		blockSize = ((fh.fileSize + memsys.PageSize - 1) / memsys.PageSize) * memsys.PageSize
	}

	fh.readBuffer = newBlockBuffer(blockSize)
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

/////////////
// WRITING //
/////////////

func (fh *fileHandle) _writeChunk(data []byte, maxWriteBufSize int64, force bool) (err error) {
	if fh.writeBuffer == nil {
		cksumType := fh.file.object.Bck().Props.Cksum.Type
		fh.writeBuffer = newWriteBuffer(cksumType)
	}

	// Buffer the writes to make sure we don't send an append request for small data chunks.
	if _, err := fh.writeBuffer.write(data); err != nil {
		return err
	}

	// Once we have filled writing buffer to enough size (determined by `maxWriteBufSize`)
	// we need to issue an append request.
	if fh.writeBuffer.size() > maxWriteBufSize || force {
		if fh.appendHandle, err = fh.file.Write(fh.writeBuffer.reader(), fh.appendHandle, fh.writeBuffer.size()); err != nil {
			return err
		}
		fh.writeBuffer.reset()
	}

	if fh.wsize == 0 {
		fh.dirty = true
	}
	fh.wsize += uint64(len(data))
	return nil
}

func (fh *fileHandle) writeChunk(data []byte, offset uint64, maxWriteBufSize int64) (err error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	if offset != fh.wsize {
		return fmt.Errorf("write file (inode %d): random access write not yet implemented", fh.file.ID())
	}

	return fh._writeChunk(data, maxWriteBufSize, false /*force*/)
}

/////////////////////////
// READING AND WRITING //
/////////////////////////

func (fh *fileHandle) flush() error {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	if !fh.dirty {
		return nil
	}

	var (
		err   error
		cksum *cos.Cksum
	)

	if fh.writeBuffer != nil && fh.writeBuffer.size() > 0 {
		err = fh._writeChunk(nil, 0, true /*force*/)
		cksum = fh.writeBuffer.checksum()
	}

	if err == nil {
		err = fh.file.Flush(fh.appendHandle, cksum)
		if err == nil {
			fh.file.Lock()
			fh.file.SetSize(fh.wsize)
			fh.file.Unlock()
		}
	}

	fh.dirty = false
	fh.wsize = 0
	fh.appendHandle = ""
	fh.writeBuffer.free()
	fh.writeBuffer = nil
	return err
}
