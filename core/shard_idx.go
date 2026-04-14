// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"bytes"
	"io"
	"time"

	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

// IdxSuffix is appended to the source object name when forming the index object
const IdxSuffix = ".idx"

// SaveShardIndex packs idx and writes it as an object in ais://.sys-shardidx.
// The system bucket must already exist in BMD.
// Write path: temp workfile -> finalize (rename) -> persist xattr.
func SaveShardIndex(archlom *LOM, idx *archive.ShardIndex) error {
	b, err := idx.Pack()
	if err != nil {
		return err
	}
	idxlom, err := newIdxLOM(archlom)
	if err != nil {
		return err
	}
	idxlom.Lock(true)
	defer idxlom.Unlock(true)

	wfqn := idxlom.GenFQN(fs.WorkCT, fs.WorkfileShardIdx)
	fh, err := idxlom.CreateWork(wfqn)
	if err != nil {
		return err
	}
	cksumType := idxlom.CksumType() // also the system bucket's checksum type
	_, ckh, err := cos.CopyAndChecksum(fh, bytes.NewReader(b), nil, cksumType)
	cos.Close(fh) // allowed to fail
	if err != nil {
		cos.RemoveFile(wfqn)
		return err
	}

	idxlom.SetSize(int64(len(b)))
	if ckh != nil {
		idxlom.SetCksum(ckh.Clone())
	} else {
		idxlom.SetCksum(cos.NoneCksum)
	}
	idxlom.SetAtimeUnix(time.Now().UnixNano())
	if err := idxlom.RenameFinalize(wfqn); err != nil {
		cos.RemoveFile(wfqn)
		return err
	}
	if err := idxlom.PersistMain(false /*not chunked*/); err != nil {
		return err
	}
	archlom.SetShardIdx(true)
	return archlom.PersistMain(false /*not chunked*/)
}

// LoadShardIndex reads and unpacks the shard index for archlom from ais://.sys-shardidx.
// Returns (nil, nil) when no index has been saved for this shard yet.
// Callers must hold at least a read lock on archlom.
func LoadShardIndex(archlom *LOM) (*archive.ShardIndex, error) {
	if !archlom.HasShardIdx() {
		return nil, nil
	}
	idxlom, err := newIdxLOM(archlom)
	if err != nil {
		return nil, err
	}
	idxlom.Lock(false)
	defer idxlom.Unlock(false)

	if err := idxlom.Load(true /*cache it*/, true /*locked*/); err != nil {
		// cannot access idxlom (missing or unreadable xattr) — repair: clear the flag
		clearShardIdx(archlom)
		if cos.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	fh, err := idxlom.Open()
	if err != nil {
		if cos.IsNotExist(err) {
			// deleted between Load and Open — repair: clear the flag
			clearShardIdx(archlom)
			return nil, nil
		}
		return nil, err
	}

	size := idxlom.Lsize()
	debug.Assert(size >= archive.ShardIdxMinLen, idxlom.Cname())
	sgl := memsys.PageMM().NewSGL(size)
	defer sgl.Free()

	_, err = io.CopyN(sgl, fh, size) // use the known size as an explicit bound
	cos.Close(fh)
	if err != nil {
		if cos.IsAnyEOF(err) {
			// file structural corruption — repair: clear the flag
			clearShardIdx(archlom)
		}
		return nil, err
	}
	idx := &archive.ShardIndex{}
	if err = idx.Unpack(sgl.ReadAll()); err != nil {
		// corrupt index — repair: clear the flag
		clearShardIdx(archlom)
		return nil, err
	}
	return idx, nil
}

// clearShardIdx clears the HasShardIdx flag on archlom.
// Best-effort: skips if archlom is already locked.
func clearShardIdx(archlom *LOM) {
	if !archlom.TryLock(true) {
		return
	}
	archlom.SetShardIdx(false)
	_ = archlom.PersistMain(false)
	archlom.Unlock(true)
}

// newIdxLOM constructs and initializes the LOM for the shard index object.
func newIdxLOM(archlom *LOM) (*LOM, error) {
	idxlom := &LOM{ObjName: archlom.Bck().SysObjName(archlom.ObjName + IdxSuffix)}
	err := idxlom.InitBck(meta.SysBckShardIdx())
	return idxlom, err
}
