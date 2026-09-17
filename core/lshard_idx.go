// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"bytes"
	"errors"
	"fmt"
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
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/cmn/oom"
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
	var (
		cache, key = lom.sidx()
		value, hit = cache.Load(key)
		cached     *sidxSlot
	)
	if hit {
		cached = value.(*sidxSlot)
	} else {
		g.sidx.stats.miss.Inc()
		cached = lom.loadCached(cache, key)
	}

	idx, err := cached.get()
	if err != nil || idx == nil {
		return entry, false, err
	}
	if idx.IsStale(lom.Checksum(), lom.Lsize()) {
		cache.CompareAndDelete(key, cached)
		lom.clearSidx()
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

// Packs idx into a workfile, then (re)loads, validates, and commits under source(W).
// Lock order: source(W) => index(W). A busy source returns ErrBusy without committing.
func (lom *LOM) SaveShardIndex(idx *archive.ShardIndex) error {
	b, err := idx.Pack()
	if err != nil {
		return fmt.Errorf("%s: %w", lom.Cname(), err)
	}
	idxlom, err := lom.newIdxLOM()
	if err != nil {
		return fmt.Errorf("%s: %w", lom.Cname(), err)
	}

	// phase 1: prepare shard-idx
	wfqn, err := idxlom.prepSidx(b)
	if err != nil {
		return fmt.Errorf("%s: %w", lom.Cname(), err)
	}
	defer cos.RemoveFile(wfqn)

	if !lom.TryLock(true) {
		// best-effort: a busy shard must never stall indexing
		return cmn.NewErrBusy("shard", lom.Cname())
	}
	defer lom.Unlock(true)
	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		return fmt.Errorf("%s: %w", lom.Cname(), err)
	}
	// archlom may have been rewritten
	if idx.IsStale(lom.Checksum(), lom.Lsize()) {
		return cmn.NewErrBusy("shard", lom.Cname(), "rewritten while indexing")
	}

	// phase 2: commit shard-idx
	idxlom.Lock(true)
	committed, err := idxlom.commitSidx(wfqn)
	if err != nil && committed {
		err = errors.Join(err, idxlom.RemoveObj())
	}
	idxlom.Unlock(true)
	if err != nil {
		if committed {
			lom.uncacheSidx()
			if lom.HasShardIdx() {
				lom.SetShardIdx(false)
				if errPersist := lom.PersistMain(lom.IsChunked()); errPersist != nil {
					err = errors.Join(err, errPersist)
				}
			}
		}
		return fmt.Errorf("%s: %w", lom.Cname(), err)
	}

	lom.uncacheSidx()
	lom.SetShardIdx(true)
	if err := lom.PersistMain(lom.IsChunked()); err != nil {
		return fmt.Errorf("%s: %w", lom.Cname(), err)
	}
	// TODO: optionally populate the shard-index cache on (re)build, behind a feature flag.
	return nil
}

func (lom *LOM) uncacheSidx() {
	cache, key := lom.sidx()
	cache.Delete(key)
}

func (lom *LOM) prepSidx(b []byte) (string, error) {
	wfqn := lom.GenFQN(fs.WorkCT, fs.WorkfileShardIdx)
	fh, err := lom.CreateWork(wfqn)
	if err != nil {
		return "", err
	}
	cksumType := lom.CksumType() // also the system bucket's checksum type
	_, ckh, err := cos.CopyAndChecksum(fh, bytes.NewReader(b), nil, cksumType)
	cos.Close(fh) // allowed to fail
	if err != nil {
		cos.RemoveFile(wfqn)
		return "", err
	}

	lom.SetSize(int64(len(b)))
	if ckh != nil {
		lom.SetCksum(ckh.Clone())
	} else {
		lom.SetCksum(cos.NoneCksum)
	}
	lom.SetAtimeUnix(time.Now().UnixNano())
	return wfqn, nil
}

// caller holds the index write lock
func (lom *LOM) commitSidx(wfqn string) (committed bool, err error) {
	if err := lom.RenameFinalize(wfqn); err != nil {
		return false, err
	}
	return true, lom.PersistMain(false /*not chunked*/)
}

// Read and unpack the shard index for archlom from ais://.sys-shardidx
// Return:
// - (nil, nil)                - absent: no index recorded, or the index object is gone
// - (nil, ErrShardIdxStale)   - present but needs rebuilding (source or index version changed)
// - (nil, ErrShardIdxCorrupt) - present but undecodable; re-indexing will fix it
// - (nil, <other>)            - other load failure; the index may still be good
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
			lom.clearSidx()
			return nil, nil
		case cmn.IsErrLmetaNotFound(err), cmn.IsErrLmetaCorrupted(err), cmn.IsErrObjDefunct(err):
			lom.clearSidx()
			return nil, fmt.Errorf("%w: %w", archive.ErrShardIdxCorrupt, err)
		}
		return nil, err // transient: keep the flag
	}

	fh, err := idxlom.Open()
	if err != nil {
		if cos.IsNotExist(err) {
			lom.clearSidx()
			return nil, nil
		}
		return nil, err // transient: keep the flag
	}

	size := idxlom.Lsize()
	if err := _checkSidxSize(size, lom.Lsize()); err != nil {
		lom.clearSidx()
		cos.Close(fh)
		return nil, err
	}

	idx, err := archive.ReadShardIndex(fh, size, mm)
	cos.Close(fh)
	if err != nil {
		// clear the flag so that the next read goes straight to the sequential scan
		if errors.Is(err, archive.ErrShardIdxCorrupt) || errors.Is(err, archive.ErrShardIdxStale) {
			lom.clearSidx()
		}
		return nil, fmt.Errorf("%s: %w", idxlom.Cname(), err)
	}
	// Staleness check: if the shard was re-uploaded, the stored cksum/size will differ.
	if idx.IsStale(lom.Checksum(), lom.Lsize()) {
		idx.Free()
		lom.clearSidx()
		return nil, archive.ErrShardIdxStale
	}
	return idx, nil
}

// remove the index object and uncache (regardless of errors)
func (lom *LOM) rmSidx() error {
	defer lom.uncacheSidx()
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

// is called upon any update (PUT, cold GET, etc.):
// - clear the flag, remove the index object, and uncache
// - lock order: archlom(W) => idxlom(W), same as archlom(R) => idxlom(R)
func (lom *LOM) dropSidx() {
	lom.SetShardIdx(false)
	if err := lom.rmSidx(); err != nil {
		nlog.Warningln(lom.Cname(), "failed to remove shard index:", err)
	}
}

// clear the `lmflShardIdx` flag on the archlom
// - LockWrite - caller is already exclusive; persist directly
// - LockRead  - non-blocking upgrade (rlock => wlock) in place (never releases the read lock on failure)
// - LockNone  - no caller lock; fall back to a non-blocking TryLock
func (lom *LOM) clearSidx() {
	lom.uncacheSidx()
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

// sanity-check the on-disk index size before allocating a buffer
func _checkSidxSize(size, shardSize int64) error {
	switch {
	case size < archive.ShardIdxMinLen:
		// too small to hold even a preamble - not a valid index, whatever produced it
		return fmt.Errorf("%w: %d bytes, below the %d-byte minimum", archive.ErrShardIdxCorrupt, size, archive.ShardIdxMinLen)
	case size <= shardIdxSaneAlloc:
		// ok
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
// - below High: evict entries idle longer than max(2*Ival, sidxIdleMin)
// - High: start at max(Ival, sidxIdleMin), halve with each consecutive High pass
// - Critical: clear all
const (
	sidxAtimeGran = 10 * time.Second  // hit path: update atime at most this often
	sidxIdleMin   = 2 * sidxAtimeGran // threshold floor (never evict continuously hit entries)
	sidxMaxBatch  = 0xff              // re-grade memory at least every 256 cold misses
	sidxRegrade   = 64 * cos.MiB      // re-grade after this much cache growth
)

type (
	sidxKey struct {
		uname string
		bid   uint64
	}
	sidxSlot struct {
		idx   *archive.ShardIndex
		err   error
		wg    sync.WaitGroup
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
		nbytes   atomic.Int64  // loaded since the last memory sample
		idleHigh time.Duration // owned by the evicting gate; 0 when not under High
		evicting atomic.Bool
		stats    struct {
			miss    atomic.Int64 // not found in cache
			load    atomic.Int64 // cold loads won and cached
			wait    atomic.Int64 // found still loading: waited for it
			deny    atomic.Int64 // not admitted: fell back to scan
			evict   atomic.Int64 // idle entries evicted
			clear   atomic.Int64 // clear-all passes (critical)
			lastSum int64        // hk-only
		}
	}
)

// loadCached coalesces concurrent cold loads. Cached indexes are heap-owned:
// eviction only removes the map reference and lets GC protect concurrent readers.
func (lom *LOM) loadCached(cache *sync.Map, key sidxKey) *sidxSlot {
	slot := &sidxSlot{}
	slot.wg.Add(1)
	if value, loaded := cache.LoadOrStore(key, slot); loaded {
		return value.(*sidxSlot)
	}
	defer slot.wg.Done()
	if !g.sidx.admit() {
		// Not admitted (memory High or above): this read, and any reader that finds
		// this slot, falls back to the sequential TAR scan. By design: no index memory
		// gets allocated under pressure - not even pooled and short-lived, as pre-cache
		// LoadShardIndex did. Hence, slower than uncached reads - a deliberate trade.
		g.sidx.stats.deny.Inc()
		slot.ready.Store(true)
		cache.CompareAndDelete(key, slot)
		return slot
	}

	slot.idx, slot.err = lom.loadShardIndex(nil) // heap-owned (see above)
	if slot.idx != nil && slot.err == nil {
		slot.atime.Store(mono.NanoTime())
		g.sidx.loaded(slot.idx.MemSize())
		g.sidx.stats.load.Inc()
	}
	slot.ready.Store(true)
	if slot.idx == nil || slot.err != nil {
		cache.CompareAndDelete(key, slot)
	}
	return slot
}

func (entry *sidxSlot) get() (*archive.ShardIndex, error) {
	if !entry.ready.Load() {
		// reader 1: read the index from disk; populate the cache via LoadOrStore; mark ready
		// reader 2: performs Load prior to `ready` above; wait _unconditionally_
		g.sidx.stats.wait.Inc()
		entry.wg.Wait()
	}
	return entry.idx, entry.err
}

func (entry *sidxSlot) touch() {
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
// Memory is re-graded at the batch cadence or the byte trigger; a rising grade triggers eviction.
func (c *sidx) admit() bool {
	due := c.nmiss.Inc()&c.batch.Load() == 0 || c.nbytes.Load() >= sidxRegrade
	if !due || !c.mu.TryLock() {
		return load.Load(c.grade.Load()) < load.High
	}
	prev, grade := c.refresh()
	c.mu.Unlock()

	c.evictRaised(prev, grade)
	return grade < load.High
}

// loaded accounts cache growth and re-grades as soon as it crosses the trigger.
func (c *sidx) loaded(size int64) {
	if c.nbytes.Add(size) < sidxRegrade {
		return
	}
	c.mu.Lock()
	if c.nbytes.Load() < sidxRegrade {
		c.mu.Unlock()
		return
	}
	prev, grade := c.refresh()
	c.mu.Unlock()
	c.evictRaised(prev, grade)
}

func (c *sidx) evictRaised(prev, grade load.Load) {
	if grade > prev && grade >= load.High {
		c.evictAsync(grade)
	}
}

// refresh samples memory via load.Advice; caller owns c.mu.
func (c *sidx) refresh() (prev, grade load.Load) {
	prev = load.Load(c.grade.Load())
	c.nbytes.Store(0) // before sampling: loads completing from here on count toward the next one
	c.adv.RefreshNoGC()
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

// age idle entries at any grade; sample independently of misses so an all-hit
// workload still yields cached memory
func (c *sidx) housekeep(now int64) time.Duration {
	c.mu.Lock()
	prev, grade := c.refresh()
	c.mu.Unlock()
	// sustained critical grade admits nothing new (one clear-all pass per transition is enough)
	if grade != load.Critical || prev != load.Critical {
		c.evictAsync(grade)
	}
	c._stats(grade)
	return hk.Jitter(load.Ival(g.pmm, grade), now)
}

// verbose only; log when changed
func (c *sidx) _stats(grade load.Load) {
	if !cmn.Rom.V(4, cos.ModCore) {
		return
	}
	s := &c.stats
	miss, loaded, wait, deny, evict, clr := s.miss.Load(), s.load.Load(), s.wait.Load(), s.deny.Load(), s.evict.Load(), s.clear.Load()
	sum := miss + loaded + wait + deny + evict + clr
	if sum == s.lastSum {
		return
	}
	s.lastSum = sum
	nlog.Infoln("shard-index cache: miss", miss, "load", loaded, "wait", wait, "deny", deny,
		"evict", evict, "clear", clr, "mem", load.Text[grade])
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
			oom.FreeToOS(true /*force*/)
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
		return max(2*ival, sidxIdleMin)
	}
	idle := c.idleHigh
	if idle == 0 {
		idle = max(ival, sidxIdleMin)
	}
	c.idleHigh = max(idle/2, sidxIdleMin)
	return idle
}

func (c *sidx) evictIdle(idle time.Duration) {
	now := mono.NanoTime()
	for _, mi := range fs.GetAvail() {
		for i := range cos.MultiHashMapCount {
			cache := mi.SidxCaches.Get(i)
			cache.Range(func(k, v any) bool {
				if e := v.(*sidxSlot); e.ready.Load() && now-e.atime.Load() > int64(idle) {
					if cache.CompareAndDelete(k, v) {
						c.stats.evict.Inc()
					}
				}
				return true
			})
		}
	}
}

func (c *sidx) clearAll() {
	c.stats.clear.Inc()
	for _, mi := range fs.GetAvail() {
		for i := range cos.MultiHashMapCount {
			mi.SidxCaches.Get(i).Clear()
		}
	}
}
