// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
)

// Two distinct LOMs participate throughout this file:
//   archlom - the source TAR object being indexed
//   idxlom  - the object in ais://.sys-shardidx holding its packed ShardIndex
//
// The exported operations are methods on the archlom. Per repo convention (and
// revive's receiver-naming rule) the receiver is spelled `lom`, but everywhere
// below `lom` IS the archlom; `idxlom` is always named explicitly.
//
// Load and removal take archlom before idxlom. Save writes and unlocks idxlom
// before trying to lock archlom, so it never holds the two at once.
//
// clearShardIdx is the one exception: it runs from inside LoadShardIndex with idxlom(R)
// still held.

const IdxSuffix = ".idx" // is appended to the source object name when forming the index object

// Packs idx and writes it as an object in ais://.sys-shardidx,
// then flips HasShardIdx on the receiving archlom. Two non-overlapping critical
// sections - see the locking note above.
//
// Best-effort: the Phase-2 flag flip uses a non-blocking write lock. If the source shard
// is currently locked, SaveShardIndex returns cmn.IsErrBusy instead of blocking
func (lom *LOM) SaveShardIndex(idx *archive.ShardIndex) error {
	b, err := idx.Pack()
	if err != nil {
		return fmt.Errorf("%s: %w", lom.Cname(), err)
	}
	idxlom, err := lom.newIdxLOM()
	if err != nil {
		return fmt.Errorf("%s: %w", lom.Cname(), err)
	}

	// Phase 1: write idxlom - idxlom(W) only, archlom not held.
	if err := writeIdxLOM(idxlom, b); err != nil {
		return fmt.Errorf("%s: %w", lom.Cname(), err)
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

	// Phase 2: flip HasShardIdx - archlom(W) only, idxlom fully released.
	if !lom.TryLock(true) {
		// Best-effort: a single non-blocking TryLock. A busy shard must never stall indexing.
		return cmn.NewErrBusy("shard", lom.Cname())
	}
	defer lom.Unlock(true)
	// Reload before PersistMain: without reload, stale in-memory fields
	// (e.g. old checksum) would overwrite the current xattr.
	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		return fmt.Errorf("%s: %w", lom.Cname(), err)
	}
	lom.SetShardIdx(true)
	if err := lom.PersistMain(lom.IsChunked()); err != nil {
		return fmt.Errorf("%s: %w", lom.Cname(), err)
	}
	return nil
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

// Read and unpack the shard index for archlom from ais://.sys-shardidx
// Return:
// - (nil, nil)                - absent: no index recorded, or the index object is gone
// - (nil, ErrShardIdxStale)   - present but built from a prior version of the shard
// - (nil, ErrShardIdxCorrupt) - present but undecodable; re-indexing will fix it
// - (nil, <other>)            - transient: I/O failure; the index may still be good
// - (idx, nil)                - usable
//
// The receiver is the archlom; the caller must hold it locked (read or write) and loaded.
// Both matter: clearShardIdx branches on which lock is held and calls IsChunked, while the
// size and staleness checks read Lsize and Checksum.
func (lom *LOM) LoadShardIndex() (*archive.ShardIndex, error) {
	debug.Func(func() { debug.Assert(lom.IsLocked() > apc.LockNone, lom.Cname(), " is not locked") })
	if !lom.HasShardIdx() {
		return nil, nil
	}
	idxlom, err := lom.newIdxLOM()
	if err != nil {
		return nil, err
	}
	idxlom.Lock(false)
	defer idxlom.Unlock(false)

	if err := idxlom.Load(true /*cache it*/, true /*locked*/); err != nil {
		switch {
		case cos.IsNotExist(err):
			lom.clearShardIdx()
			return nil, nil
		case cmn.IsErrLmetaNotFound(err), cmn.IsErrLmetaCorrupted(err), cmn.IsErrObjDefunct(err):
			lom.clearShardIdx()
			return nil, fmt.Errorf("%w: %w", archive.ErrShardIdxCorrupt, err)
		}
		return nil, err // transient: keep the flag
	}

	fh, err := idxlom.Open()
	if err != nil {
		if cos.IsNotExist(err) {
			lom.clearShardIdx()
			return nil, nil
		}
		return nil, err // transient: keep the flag
	}

	size := idxlom.Lsize()
	if err := shardIdxLenOk(size, lom.Lsize()); err != nil {
		lom.clearShardIdx()
		cos.Close(fh)
		return nil, err
	}

	buf := make([]byte, size)
	_, err = io.ReadFull(fh, buf)
	cos.Close(fh)
	if err != nil {
		if cos.IsAnyEOF(err) {
			// short read against a size we just stat'ed: the object is truncated
			lom.clearShardIdx()
			return nil, fmt.Errorf("%w: %s truncated below %d bytes", archive.ErrShardIdxCorrupt, idxlom.Cname(), size)
		}
		return nil, err // transient: keep the flag
	}
	idx := &archive.ShardIndex{}
	if err = idx.Unpack(buf); err != nil {
		lom.clearShardIdx()
		return nil, err
	}
	// Staleness check: if the shard was re-uploaded, the stored cksum/size will differ.
	if idx.IsStale(lom.Checksum(), lom.Lsize()) {
		lom.clearShardIdx()
		return nil, archive.ErrShardIdxStale
	}
	return idx, nil
}

// rmShardIdx removes the shard index object associated with lom.
// Caller holds lom write-locked for removal; idxlom is write-locked only here.
func (lom *LOM) rmShardIdx() error {
	idxlom, err := lom.newIdxLOM()
	if err != nil {
		return err
	}
	idxlom.Lock(true)
	defer idxlom.Unlock(true)

	err = idxlom.Load(false /*cache it*/, true /*locked*/)
	if cos.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return idxlom.RemoveObj()
}

// Clear the HasShardIdx flag on the archlom, so that the next read
// skips the index entirely instead of re-loading and re-rejecting it every time.
//
// LockWrite - caller is already exclusive; persist directly
// LockRead  - non-blocking upgrade (rlock => wlock) in place (never releases the read lock on failure)
// LockNone  - no caller lock; fall back to a non-blocking TryLock
func (lom *LOM) clearShardIdx() {
	switch lom.IsLocked() {
	case apc.LockWrite:
		// caller holds it exclusively - nothing to acquire
	case apc.LockRead:
		if !lom.UpgradeLock() {
			return // concurrent readers - leave the flag to the next index-shard run
		}
		defer lom.DowngradeLock()
	default:
		if !lom.TryLock(true) {
			return
		}
		defer lom.Unlock(true)
	}
	lom.SetShardIdx(false)
	_ = lom.PersistMain(lom.IsChunked())
}

// Floor below which the shard-size bound (see shardIdxLenOk) is not applied:
// - below this floor the allocation it guards is harmless
// - above it a normal TAR still has ample margin: every indexed entry requires at least one 512-byte header block,
// plus any name-extension and data blocks.
const shardIdxSaneAlloc = cos.MiB

// sanity-check the on-disk index size before allocating a buffer for it.
//
// The upper bound is the shard's own size.
// Every indexed TAR member costs at least one 512-byte header block in the shard, while its
// index entry costs len(name) plus ~8 bytes of varint framing. Long names don't invert
// this - a PAX/GNU long name is itself stored as an extra 512-aligned record in the shard.
func shardIdxLenOk(size, shardSize int64) error {
	switch {
	case size < archive.ShardIdxMinLen:
		// too small to hold even a preamble - not a valid index, whatever produced it
		return fmt.Errorf("%w: %d bytes, below the %d-byte minimum", archive.ErrShardIdxCorrupt, size, archive.ShardIdxMinLen)
	case size <= shardIdxSaneAlloc:
		// too small to be worth second-guessing - see shardIdxSaneAlloc
	case shardSize > 0 && size > shardSize:
		// the index cannot belong to this shard - report stale, not corrupt
		return fmt.Errorf("%w: %d-byte index against a %d-byte shard", archive.ErrShardIdxStale, size, shardSize)
	}
	return nil
}

// construct and initialize the LOM for the shard index object
func (lom *LOM) newIdxLOM() (*LOM, error) {
	idxlom := &LOM{ObjName: lom.Bck().SysObjName(lom.ObjName + IdxSuffix)}
	err := idxlom.InitBck(meta.SysBckShardIdx())
	return idxlom, err
}
