// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"errors"
	"io"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/memsys"
)

const (
	MaxBlockSize = memsys.MaxSlabSize
	MinBlockSize = cmn.PageSize
)

type BlockBuffer struct {
	sgl       *memsys.SGL
	blockSize int64
	blockNo   int64
	valid     bool
}

type LoadBlockFunc func(w io.Writer, blockNo int64, blockSize int64) (n int64, err error)

// Panics if blockSize has an invalid value, see memsys.(*Mem2).NewSGL
func NewBlockBuffer(blockSize int64) *BlockBuffer {
	return &BlockBuffer{
		blockSize: blockSize,
		sgl:       glMem2.NewSGL(blockSize, blockSize),
		valid:     false,
	}
}

func (b *BlockBuffer) BlockSize() int64 {
	return b.blockSize
}

func (b *BlockBuffer) Free() {
	cmn.Assert(b.sgl != nil)
	b.sgl.Free()
}

func (b *BlockBuffer) EnsureBlock(blockNo int64, loadBlock LoadBlockFunc) (err error) {
	cmn.Assert(b.sgl != nil)
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

func (b *BlockBuffer) ReadAt(p []byte, offset int64) (n int, err error) {
	cmn.Assert(b.sgl != nil)
	if !b.valid {
		return 0, errors.New("invalid block")
	}
	reader := memsys.NewReader(b.sgl)
	reader.Seek(offset, io.SeekStart)
	return reader.Read(p)
}
