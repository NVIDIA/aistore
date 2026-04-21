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
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

// Locking protocol for shard indexes
// ====================================
//
// Three lock sequences exist; archlom and idxlom are never held simultaneously in any thread:
//
//   SaveShardIndex — two sequential, non-overlapping critical sections:
//     Phase 1:  idxlom(W) only  — write the index file to ais://.sys-shardidx
//     Phase 2:  archlom(W) only — flip HasShardIdx on the source shard
//
//   LoadShardIndex — always called with archlom already locked by the caller:
//     archlom(R) [held by caller] --> idxlom(R)
//
//   External TAR operations (GET, PUT, ...):
//     archlom(R or W) only — idxlom reached only via LoadShardIndex above
//
// No-deadlock proof:
//   The only pair of locks ever held simultaneously is archlom(R) --> idxlom(R)
//   inside LoadShardIndex.  SaveShardIndex never holds both at once: idxlom(W)
//   is fully released before archlom(W) is acquired.  Future callers that hold
//   archlom and call LoadShardIndex to seek within a TAR follow the same
//   archlom --> idxlom direction.  The lock graph therefore has exactly one
//   directed edge (archlom --> idxlom) and no cycle exists, making deadlock
//   impossible under any interleaving.

// IdxSuffix is appended to the source object name when forming the index object
const IdxSuffix = ".idx"

// SaveShardIndex packs idx and writes it as an object in ais://.sys-shardidx,
// then flips HasShardIdx on archlom. Two non-overlapping critical sections —
// see locking protocol above.
func SaveShardIndex(archlom *LOM, idx *archive.ShardIndex) error {
	b, err := idx.Pack()
	if err != nil {
		return err
	}
	idxlom, err := newIdxLOM(archlom)
	if err != nil {
		return err
	}

	// Phase 1: write idxlom — idxlom(W) only, archlom not held.
	if err := writeIdxLOM(idxlom, b); err != nil {
		return err
	}

	// Window between phases: idxlom is persisted but HasShardIdx is still false on archlom.
	// Three races are possible and all are safe:
	//
	//   Concurrent reader: sees HasShardIdx=false => skips LoadShardIndex, falls back to
	//   sequential TAR scan. The index is invisible until Phase 2 completes.
	//
	//   Concurrent PUT: overwrites the shard with new content (new checksum/size).
	//   Phase 2 reloads archlom under its write lock, capturing the new metadata before
	//   setting HasShardIdx=true. The index now embeds mismatched SrcCksum/SrcSize =>
	//   IsStale() fires on the next LoadShardIndex => cleared and re-indexed.
	//
	//   Crash: idxlom exists on disk but HasShardIdx stays false. The orphaned file is
	//   harmless; the next index-shard run overwrites it and completes Phase 2.

	// Phase 2: flip HasShardIdx — archlom(W) only, idxlom fully released.
	// Reload before PersistMain: without reload, stale in-memory fields
	// (e.g. old checksum) would overwrite the current xattr.
	archlom.Lock(true)
	defer archlom.Unlock(true)
	if err := archlom.Load(false /*cache it*/, true /*locked*/); err != nil {
		return err
	}
	archlom.SetShardIdx(true)
	return archlom.PersistMain(false /*not chunked*/)
}

// writeIdxLOM writes packed index bytes to idxlom under idxlom(W).
// It is a self-contained critical section; the caller must not hold any other lock.
func writeIdxLOM(idxlom *LOM, b []byte) error {
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
	return idxlom.PersistMain(false /*not chunked*/)
}

// LoadShardIndex reads and unpacks the shard index for archlom from ais://.sys-shardidx.
// Returns (nil, nil) if absent or unreadable, (nil, ErrShardIdxStale) if stale.
// Caller must hold archlom locked (read or write); see locking protocol above.
// On error, HasShardIdx is cleared best-effort (no-op when archlom is already locked).
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
		clearShardIdx(archlom)
		if cos.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	fh, err := idxlom.Open()
	if err != nil {
		if cos.IsNotExist(err) {
			clearShardIdx(archlom)
			return nil, nil
		}
		return nil, err
	}

	size := idxlom.Lsize()
	if size < archive.ShardIdxMinLen {
		clearShardIdx(archlom)
		cos.Close(fh)
		return nil, nil
	}
	sgl := memsys.PageMM().NewSGL(size)
	defer sgl.Free()

	_, err = io.CopyN(sgl, fh, size) // use the known size as an explicit bound
	cos.Close(fh)
	if err != nil {
		if cos.IsAnyEOF(err) {
			clearShardIdx(archlom)
		}
		return nil, err
	}
	idx := &archive.ShardIndex{}
	if err = idx.Unpack(sgl.ReadAll()); err != nil {
		clearShardIdx(archlom)
		return nil, err
	}
	// Staleness check: if the shard was re-uploaded, the stored cksum/size will differ.
	if idx.IsStale(archlom.Checksum(), archlom.Lsize()) {
		clearShardIdx(archlom)
		return nil, archive.ErrShardIdxStale
	}
	return idx, nil
}

// clearShardIdx clears the HasShardIdx flag on archlom.
// Best-effort: no-op if archlom is already locked (see locking protocol above).
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
