// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/load"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
)

const IdxSuffix = ".idx" // is appended to the source object name when forming the index object

const shardIdxSaneAlloc = cos.MiB

func (lom *LOM) HasShardIdx() bool { return lom.md.flags&lmflShardIdx != 0 }

func (lom *LOM) lookupShardIndex(archpath string) (entry archive.ShardIndexEntry, ok bool, err error) {
	if !lom.HasShardIdx() {
		return entry, false, nil
	}
	cache, key := lom.sidx()
	value, hit := cache.Load(key)
	var cached *sidxEntry
	if hit {
		cached = value.(*sidxEntry)
	} else {
		cached = lom.loadCached(cache, key)
	}
	idx, err := cached.get()
	if err != nil || idx == nil {
		return entry, false, err
	}
	if idx.IsStale(lom.Checksum(), lom.Lsize()) {
		cache.CompareAndDelete(key, cached)
		lom.clearShardIdx()
		return entry, false, archive.ErrShardIdxStale
	}
	cached.touch()
	entry, ok = idx.Lookup(archpath)
	return entry, ok, nil
}

func (lom *LOM) sidx() (*sync.Map, sidxKey) {
	key := sidxKey{uname: lom.Uname(), bid: lom.bid()}
	return lom.mi.SidxCaches.Get(lom.CacheIdx()), key
}

func (lom *LOM) SetShardIdx(v bool) {
	if v {
		lom.md.flags |= lmflShardIdx
	} else {
		lom.md.flags &^= lmflShardIdx
	}
}

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
	if err := idxlom.writeIdx(b); err != nil {
		return fmt.Errorf("%s: %w", lom.Cname(), err)
	}
	lom.uncacheIdx()

	if !lom.TryLock(true) {
		// Best-effort: a single non-blocking TryLock. A busy shard must never stall indexing.
		return cmn.NewErrBusy("shard", lom.Cname())
	}
	defer lom.Unlock(true)
	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		return fmt.Errorf("%s: %w", lom.Cname(), err)
	}
	lom.SetShardIdx(true)
	if err := lom.PersistMain(lom.IsChunked()); err != nil {
		return fmt.Errorf("%s: %w", lom.Cname(), err)
	}
	// TODO: optionally populate the shard-index cache on (re)build, behind a feature flag.
	return nil
}

func (lom *LOM) uncacheIdx() {
	cache, key := lom.sidx()
	cache.Delete(key)
}

func (lom *LOM) writeIdx(b []byte) error {
	lom.Lock(true)
	defer lom.Unlock(true)

	wfqn := lom.GenFQN(fs.WorkCT, fs.WorkfileShardIdx)
	fh, err := lom.CreateWork(wfqn)
	if err != nil {
		return err
	}
	cksumType := lom.CksumType() // also the system bucket's checksum type
	_, ckh, err := cos.CopyAndChecksum(fh, bytes.NewReader(b), nil, cksumType)
	cos.Close(fh) // allowed to fail
	if err != nil {
		cos.RemoveFile(wfqn)
		return err
	}

	lom.SetSize(int64(len(b)))
	if ckh != nil {
		lom.SetCksum(ckh.Clone())
	} else {
		lom.SetCksum(cos.NoneCksum)
	}
	lom.SetAtimeUnix(time.Now().UnixNano())
	if err := lom.RenameFinalize(wfqn); err != nil {
		cos.RemoveFile(wfqn)
		return err
	}
	return lom.PersistMain(false /*not chunked*/)
}

// Read and unpack the shard index for archlom from ais://.sys-shardidx
// Return:
// - (nil, nil)                - absent: no index recorded, or the index object is gone
// - (nil, ErrShardIdxStale)   - present but needs rebuilding (source or index version changed)
// - (nil, ErrShardIdxCorrupt) - present but undecodable; re-indexing will fix it
// - (nil, <other>)            - transient: I/O failure; the index may still be good
// - (idx, nil)                - usable
func (lom *LOM) LoadShardIndex() (*archive.ShardIndex, error) {
	return lom.loadShardIndex(T.PageMM())
}

func (lom *LOM) loadShardIndex(mm *memsys.MMSA) (*archive.ShardIndex, error) {
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

	idx, err := archive.ReadShardIndex(fh, size, mm)
	cos.Close(fh)
	if err != nil {
		// clear the flag so that the next read goes straight to the sequential scan
		if errors.Is(err, archive.ErrShardIdxCorrupt) || errors.Is(err, archive.ErrShardIdxStale) {
			lom.clearShardIdx()
		}
		return nil, fmt.Errorf("%s: %w", idxlom.Cname(), err)
	}
	// Staleness check: if the shard was re-uploaded, the stored cksum/size will differ.
	if idx.IsStale(lom.Checksum(), lom.Lsize()) {
		idx.Free()
		lom.clearShardIdx()
		return nil, archive.ErrShardIdxStale
	}
	return idx, nil
}

func (lom *LOM) rmShardIdx() error {
	idxlom, err := lom.newIdxLOM()
	if err != nil {
		return err
	}
	idxlom.Lock(true)
	defer idxlom.Unlock(true)

	err = idxlom.Load(false /*cache it*/, true /*locked*/)
	if cos.IsNotExist(err) {
		lom.uncacheIdx()
		return nil
	}
	if err != nil {
		return err
	}
	if err := idxlom.RemoveObj(); err != nil {
		return err
	}
	lom.uncacheIdx()
	return nil
}

// Clear the HasShardIdx flag on the archlom, so that the next read
// skips the index entirely instead of re-loading and re-rejecting it every time.
// LockWrite - caller is already exclusive; persist directly
// LockRead  - non-blocking upgrade (rlock => wlock) in place (never releases the read lock on failure)
// LockNone  - no caller lock; fall back to a non-blocking TryLock
func (lom *LOM) clearShardIdx() {
	lom.uncacheIdx()
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

// sanity-check the on-disk index size before allocating a buffer for it.
// The upper bound is the shard's own size.
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

//
// Shard Index Cache --------------------------------------------------------------------------------------------------------
//

// Timing derives from load.Ival (memsys hk cadence, graded by memory):
// - housekeeping runs every Ival
// - below High: evict entries idle longer than 2*Ival (tightens as Low => Moderate)
// - High: start at Ival, halve with each consecutive High pass, down to sidxIdleMin
// - Critical: clear all
const (
	sidxAtimeGran = 10 * time.Second  // hit path: update atime at most this often
	sidxIdleMin   = 2 * sidxAtimeGran // High: threshold floor (never evict continuously hit entries)
	sidxMaxBatch  = 0xff              // re-grade memory at least every 256 cold misses
)

type (
	sidxKey struct {
		uname string
		bid   uint64
	}
	sidxEntry struct {
		idx   *archive.ShardIndex
		err   error
		atime atomic.Int64
		ready atomic.Bool
	}
	// g.sidx: admission and eviction
	sidx struct {
		adv      load.Advice // protected by mu
		mu       sync.Mutex
		grade    atomic.Int32 // last sampled load.Load
		batch    atomic.Int64
		nmiss    atomic.Int64
		idleHigh time.Duration // owned by the evicting gate; 0 when not under High
		evicting atomic.Bool
	}
)

// loadCached coalesces concurrent cold loads. Cached indexes are heap-owned:
// eviction only removes the map reference and lets GC protect concurrent readers.
func (lom *LOM) loadCached(cache *sync.Map, key sidxKey) *sidxEntry {
	entry := &sidxEntry{}
	if value, loaded := cache.LoadOrStore(key, entry); loaded {
		return value.(*sidxEntry)
	}
	if !g.sidx.admit() {
		// Not admitted (memory High or above): this read, and any reader that finds
		// this entry, falls back to the sequential TAR scan. By design: no index memory
		// gets allocated under pressure - not even pooled and short-lived, as pre-cache
		// LoadShardIndex did. Hence, slower than uncached reads - a deliberate trade.
		entry.ready.Store(true)
		cache.CompareAndDelete(key, entry)
		return entry
	}

	entry.idx, entry.err = lom.loadShardIndex(nil)
	if entry.idx != nil && entry.err == nil {
		entry.atime.Store(mono.NanoTime())
	}
	entry.ready.Store(true)
	if entry.idx == nil || entry.err != nil {
		cache.CompareAndDelete(key, entry)
	}
	return entry
}

func (entry *sidxEntry) get() (*archive.ShardIndex, error) {
	if !entry.ready.Load() {
		// TODO: simplified retry-once can be reimagined
		runtime.Gosched()
		if !entry.ready.Load() {
			return nil, nil // another reader is loading: fall back to TAR scan
		}
	}
	return entry.idx, entry.err
}

func (entry *sidxEntry) touch() {
	now, prev := mono.NanoTime(), entry.atime.Load()
	if prev == 0 || now-prev >= int64(sidxAtimeGran) {
		entry.atime.CAS(prev, now)
	}
}

func (c *sidx) init(runHK bool) {
	c.adv.Init(load.FlMem, &load.Extra{RW: true})
	grade := c.store()
	if runHK {
		hk.Reg("sidx"+hk.NameSuffix, c.housekeep, load.Ival(g.pmm, grade))
	}
}

// admit a cold miss; nothing new gets cached at High and above.
// Memory is re-graded at the advised batch cadence; a rising grade triggers eviction.
func (c *sidx) admit() bool {
	if c.nmiss.Inc()&c.batch.Load() != 0 || !c.mu.TryLock() {
		return load.Load(c.grade.Load()) < load.High
	}
	prev, grade := c.refresh()
	c.mu.Unlock()

	if grade > prev && grade >= load.High {
		c.evictAsync(grade)
	}
	return grade < load.High
}

// refresh samples memory via load.Advice; caller owns c.mu.
func (c *sidx) refresh() (prev, grade load.Load) {
	prev = load.Load(c.grade.Load())
	c.adv.Refresh()
	grade = c.store()
	return
}

// caller owns c.mu (or runs single-threaded init)
func (c *sidx) store() (grade load.Load) {
	grade = c.adv.MemLoad()
	c.grade.Store(int32(grade))
	c.batch.Store(min(c.adv.Batch, sidxMaxBatch))
	return grade
}

// Age idle entries at any grade; sample independently of misses so an all-hit
// workload still yields cache memory when the process comes under pressure elsewhere.
func (c *sidx) housekeep(now int64) time.Duration {
	c.mu.Lock()
	_, grade := c.refresh()
	c.mu.Unlock()
	c.evictAsync(grade)
	return hk.Jitter(load.Ival(g.pmm, grade), now)
}

func (c *sidx) evictAsync(grade load.Load) {
	if c.evicting.CAS(false, true) {
		go c.evict(grade)
	}
}

func (c *sidx) evict(grade load.Load) {
	for {
		if grade == load.Critical {
			c.clearAll()
		} else {
			c.evictIdle(c.idleFor(grade))
		}
		c.evicting.Store(false)

		// pick up a higher pressure grade sampled concurrently while this pass owned the gate
		next := load.Load(c.grade.Load())
		if next <= grade || next < load.High || !c.evicting.CAS(false, true) {
			return
		}
		grade = next
	}
}

// idle threshold for the grade (see "Timing" above); caller owns the evicting gate
func (c *sidx) idleFor(grade load.Load) time.Duration {
	ival := load.Ival(g.pmm, grade)
	if grade < load.High {
		c.idleHigh = 0
		return 2 * ival
	}
	idle := c.idleHigh
	if idle == 0 {
		idle = ival
	}
	c.idleHigh = max(idle/2, sidxIdleMin)
	return idle
}

func (*sidx) evictIdle(idle time.Duration) {
	now := mono.NanoTime()
	for _, mi := range fs.GetAvail() {
		for i := range cos.MultiHashMapCount {
			cache := mi.SidxCaches.Get(i)
			cache.Range(func(k, v any) bool {
				if e := v.(*sidxEntry); e.ready.Load() && now-e.atime.Load() > int64(idle) {
					cache.CompareAndDelete(k, v)
				}
				return true
			})
		}
	}
}

func (*sidx) clearAll() {
	for _, mi := range fs.GetAvail() {
		for i := range cos.MultiHashMapCount {
			mi.SidxCaches.Get(i).Clear()
		}
	}
}
