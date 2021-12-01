// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"errors"
	"io"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/memsys"
)

const (
	maxBlockSize = memsys.MaxPageSlabSize
	minBlockSize = memsys.PageSize
)

type (
	loadBlockFunc func(w io.Writer, blockNo int64, blockSize int64) (n int64, err error)

	blockBuffer struct {
		sgl       *memsys.SGL
		blockSize int64
		blockNo   int64
		valid     bool
	}

	writeBuffer struct {
		sgl   *memsys.SGL
		cksum *cos.CksumHash
	}
)

// Panics if blockSize has an invalid value, see memsys.(*MMSA).NewSGL
func newBlockBuffer(blockSize int64) *blockBuffer {
	return &blockBuffer{
		blockSize: blockSize,
		sgl:       glMem2.NewSGL(blockSize, blockSize),
		valid:     false,
	}
}

func (b *blockBuffer) BlockSize() int64 {
	return b.blockSize
}

func (b *blockBuffer) Free() {
	cos.Assert(b.sgl != nil)
	b.sgl.Free()
}

func (b *blockBuffer) EnsureBlock(blockNo int64, loadBlock loadBlockFunc) (err error) {
	cos.Assert(b.sgl != nil)
	if !b.valid || b.blockNo != blockNo {
		b.valid = true
		b.blockNo = blockNo
		b.sgl.Reset()
		_, err = loadBlock(b.sgl, b.blockNo*b.blockSize, b.blockSize)
		if err != nil {
			b.valid = false
		}
	}
	return
}

func (b *blockBuffer) ReadAt(p []byte, offset int64) (n int, err error) {
	cos.Assert(b.sgl != nil)
	if !b.valid {
		return 0, errors.New("invalid block")
	}
	reader := memsys.NewReader(b.sgl)
	reader.Seek(offset, io.SeekStart)
	return reader.Read(p)
}

func newWriteBuffer(cksumType string) *writeBuffer {
	return &writeBuffer{
		sgl:   glMem2.NewSGL(maxBlockSize, maxBlockSize),
		cksum: cos.NewCksumHash(cksumType),
	}
}

func (b *writeBuffer) reader() cos.ReadOpenCloser { return memsys.NewReader(b.sgl) }
func (b *writeBuffer) size() int64                { return b.sgl.Size() }
func (b *writeBuffer) reset()                     { b.sgl.Reset() }
func (b *writeBuffer) free() {
	b.sgl.Free()
	b.cksum = nil
}

func (b *writeBuffer) write(p []byte) (int, error) {
	_, err := b.cksum.H.Write(p)
	cos.AssertNoErr(err)
	return b.sgl.Write(p)
}

func (b *writeBuffer) checksum() *cos.Cksum {
	b.cksum.Finalize()
	return b.cksum.Clone()
}
