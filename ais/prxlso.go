// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/hk"
)

//  Brief theory of operation ================================================
//
//  * BUFFER - container for a single request that keeps entries so they won't
//    be re-requested. Thanks to buffering, we eliminate the case when a given
//    object is requested more than once.
//  * CACHE  - container shared by multiple requests which are identified with
//    the same id. Thanks to caching, we reuse previously calculated requests.
//
// Buffering is designed to work for a single request and is identified by
// list-objects uuid. Each buffer consists of:
// - a *main buffer* that in turn contains entries ready to be returned to the
// client (user), and
// - *leftovers* - per target structures consisting of entries that couldn't
// be included into the *main buffer* yet.
// When a buffer doesn't contain enough entries, the new entries
// are loaded and added to *leftovers*. After this, they are merged and put
// into the *main buffer* so they can be returned to the client.
//
// Caching is thread safe and is used across multiple requests (clients).
// Each request is identified by its `cacheReqID`. List-objects requests
// that share the same ID will also share a common cache.
//
// Cache consists of contiguous intervals of `cmn.LsoEntry`.
// Cached response (to a request) is valid if and only if the request can be
// fulfilled by a single cache interval (otherwise, cache cannot be trusted
// as we don't know how many objects can fit in the requested interval).

// internal timers (rough estimates)
const (
	cacheIntervalTTL = 10 * time.Minute // *cache interval's* time to live
	lsobjBufferTTL   = 10 * time.Minute // *lsobj buffer* time to live
	qmTimeHk         = 10 * time.Minute // housekeeping timer
	qmTimeHkMax      = time.Hour        // max HK time (when no activity whatsoever)
)

type (
	// Request buffer per target.
	lsobjBufferTarget struct {
		// Leftovers entries which we keep locally so they will not be requested
		// again by the proxy. Out of these `currentBuff` is extended.
		entries cmn.LsoEntries
		// Determines if the target is done with listing.
		done bool
	}

	// Request buffer that corresponds to a single `uuid`.
	lsobjBuffer struct {
		// Contains the last entry that was returned to the user.
		nextToken string
		// Currently maintained buffer that keeps the entries sorted
		// and ready to be dispatched to the client.
		currentBuff cmn.LsoEntries
		// Buffers for each target that are finally merged and the entries are
		// appended to the `currentBuff`.
		leftovers map[string]*lsobjBufferTarget // targetID (string) -> target buffer
		// Timestamp of the last access to this buffer. Idle buffers get removed
		// after `lsobjBufferTTL`.
		lastAccess atomic.Int64
	}

	// Contains all lsobj buffers.
	lsobjBuffers struct {
		buffers sync.Map // request uuid (string) -> buffer (*lsobjBuffer)
	}

	// Cache request ID. This identifies and splits requests into
	// multiple caches that these requests can use.
	cacheReqID struct {
		bck    *cmn.Bck
		prefix string
	}

	// Single (contiguous) interval of `cmn.LsoEntry`.
	cacheInterval struct {
		// Contains the previous entry (`ContinuationToken`) that was requested
		// to get this interval. Thanks to this we can match and merge two
		// adjacent intervals.
		token string
		// Entries that are contained in this interval. They are sorted and ready
		// to be dispatched to the client.
		entries cmn.LsoEntries
		// Contains the timestamp of the last access to this interval. Idle interval
		// gets removed after `cacheIntervalTTL`.
		lastAccess int64
		// Determines if this is the last page/interval (no more objects after
		// the last entry).
		last bool
	}

	// Contains additional parameters to interval request.
	reqParams struct {
		prefix string
	}

	// Single cache that corresponds to single `cacheReqID`.
	lsobjCache struct {
		mtx       sync.RWMutex
		intervals []*cacheInterval
	}

	// Contains all lsobj caches.
	lsobjCaches struct {
		caches sync.Map // cache id (cacheReqID) -> cache (*lsobjCache)
	}

	lsobjMem struct {
		b *lsobjBuffers
		c *lsobjCaches
		d time.Duration
	}
)

func (qm *lsobjMem) init() {
	qm.b = &lsobjBuffers{}
	qm.c = &lsobjCaches{}
	qm.d = qmTimeHk
	hk.Reg("lsobj-buffer-cache"+hk.NameSuffix, qm.housekeep, qmTimeHk)
}

func (qm *lsobjMem) housekeep() time.Duration {
	num := qm.b.housekeep()
	num += qm.c.housekeep()
	if num == 0 {
		qm.d = min(qm.d+qmTimeHk, qmTimeHkMax)
	} else {
		qm.d = qmTimeHk
	}
	return qm.d
}

/////////////////
// lsobjBuffer //
/////////////////

// mergeTargetBuffers merges `b.leftovers` buffers into `b.currentBuff`.
// It returns `filled` equal to `true` if there was anything to merge, otherwise `false`.
func (b *lsobjBuffer) mergeTargetBuffers() (filled bool) {
	var (
		totalCnt int
		allDone  = true
	)
	// If `b.leftovers` is empty then there was no initial `set`.
	if len(b.leftovers) == 0 {
		return false
	}
	for _, list := range b.leftovers {
		totalCnt += len(list.entries)
		allDone = allDone && list.done
	}
	// If there are no entries and some targets are not yet done then there wasn't `set`.
	if totalCnt == 0 && !allDone {
		return false
	}

	var (
		minObj  string
		entries = make(cmn.LsoEntries, 0, totalCnt)
	)
	for _, list := range b.leftovers {
		for i := range list.entries {
			if list.entries[i] == nil {
				list.entries = list.entries[:i]
				break
			}
		}
		entries = append(entries, list.entries...)

		if list.done || len(list.entries) == 0 {
			continue
		}
		if minObj == "" || list.entries[len(list.entries)-1].Name < minObj {
			minObj = list.entries[len(list.entries)-1].Name
		}
	}

	cmn.SortLso(entries)

	if minObj != "" {
		idx := sort.Search(len(entries), func(i int) bool {
			return entries[i].Name > minObj
		})
		entries = entries[:idx]
	}
	for id := range b.leftovers {
		b.leftovers[id].entries = nil
	}
	b.currentBuff = append(b.currentBuff, entries...)
	return true
}

func (b *lsobjBuffer) get(token string, size int64) (entries cmn.LsoEntries, hasEnough bool) {
	b.lastAccess.Store(mono.NanoTime())

	// If user requested something before what we have currently in the buffer
	// then we just need to forget it.
	if token < b.nextToken {
		b.leftovers = nil
		b.currentBuff = nil
		b.nextToken = token
		return nil, false
	}

	filled := b.mergeTargetBuffers()

	// Move to first object after token.
	idx := sort.Search(len(b.currentBuff), func(i int) bool {
		return b.currentBuff[i].Name > token
	})
	entries = b.currentBuff[idx:]

	if size > int64(len(entries)) {
		// In case we don't have enough entries and we haven't filled anything then
		// we must request more (if filled then we don't have enough because it's end).
		if !filled {
			return nil, false
		}
		size = int64(len(entries))
	}

	// Move buffer after returned entries.
	b.currentBuff = entries[size:]
	// Select only the entries that need to be returned to user.
	entries = entries[:size]
	if len(entries) > 0 {
		b.nextToken = entries[len(entries)-1].Name
	}
	return entries, true
}

func (b *lsobjBuffer) set(id string, entries cmn.LsoEntries, size int64) {
	if b.leftovers == nil {
		b.leftovers = make(map[string]*lsobjBufferTarget, 5)
	}
	b.leftovers[id] = &lsobjBufferTarget{
		entries: entries,
		done:    len(entries) < int(size),
	}
	b.lastAccess.Store(mono.NanoTime())
}

func (b *lsobjBuffers) last(id, token string) string {
	v, ok := b.buffers.LoadOrStore(id, &lsobjBuffer{})
	if !ok {
		return token
	}
	buffer := v.(*lsobjBuffer)
	if len(buffer.currentBuff) == 0 {
		return token
	}
	last := buffer.currentBuff[len(buffer.currentBuff)-1].Name
	if cmn.TokenGreaterEQ(token, last) {
		return token
	}
	return last
}

func (b *lsobjBuffers) get(id, token string, size int64) (entries cmn.LsoEntries, hasEnough bool) {
	v, _ := b.buffers.LoadOrStore(id, &lsobjBuffer{})
	return v.(*lsobjBuffer).get(token, size)
}

func (b *lsobjBuffers) set(id, targetID string, entries cmn.LsoEntries, size int64) {
	v, _ := b.buffers.LoadOrStore(id, &lsobjBuffer{})
	v.(*lsobjBuffer).set(targetID, entries, size)
}

func (b *lsobjBuffers) housekeep() (num int) {
	b.buffers.Range(func(key, value any) bool {
		buffer := value.(*lsobjBuffer)
		num++
		if mono.Since(buffer.lastAccess.Load()) > lsobjBufferTTL {
			b.buffers.Delete(key)
		}
		return true
	})
	return
}

///////////////////
// cacheInterval //
///////////////////

func (ci *cacheInterval) contains(token string) bool {
	if ci.token == token {
		return true
	}
	if len(ci.entries) > 0 {
		return ci.entries[0].Name <= token && token <= ci.entries[len(ci.entries)-1].Name
	}
	return false
}

func (ci *cacheInterval) get(token string, objCnt int64, params reqParams) (entries cmn.LsoEntries, hasEnough bool) {
	ci.lastAccess = mono.NanoTime()
	entries = ci.entries

	start := ci.find(token)
	if params.prefix != "" {
		// Move `start` to first entry that starts with `params.prefix`.
		for ; start < uint(len(entries)); start++ {
			if strings.HasPrefix(entries[start].Name, params.prefix) {
				break
			}
			if entries[start].Name > params.prefix {
				// Prefix is fully contained in the interval (but there are no entries), examples:
				//  * interval = ["a", "z"], token = "", objCnt = 1, prefix = "b"
				//  * interval = ["a", "z"], token = "a", objCnt = 1, prefix = "b"
				return cmn.LsoEntries{}, true
			}
		}
		if !ci.last && start == uint(len(entries)) {
			// Prefix is out of the interval (right boundary), examples:
			//  * interval = ["b", "y"], token = "", objCnt = 1, prefix = "z"
			//  * interval = ["b", "y"], token = "", objCnt = 1, prefix = "ya"
			return nil, false
		}
	}
	entries = entries[start:]

	end := min(len(entries), int(objCnt))
	if params.prefix != "" {
		// Move `end-1` to last entry that starts with `params.prefix`.
		for ; end > 0; end-- {
			if strings.HasPrefix(entries[end-1].Name, params.prefix) {
				break
			}
		}
		if !ci.last && end < len(entries) {
			// We filtered out entries that start with `params.prefix` and
			// the entries are fully contained in the interval, examples:
			//  * interval = ["a", "ma", "mb", "z"], token = "", objCnt = 4, prefix = "m"
			//  * interval = ["a", "z"], token = "", objCnt = 2, prefix = "a"
			return entries[:end], true
		}
	}
	entries = entries[:end]

	if ci.last || len(entries) >= int(objCnt) {
		return entries, true
	}
	return nil, false
}

func (ci *cacheInterval) find(token string) (idx uint) {
	if ci.token == token {
		return 0
	}
	return uint(sort.Search(len(ci.entries), func(i int) bool {
		return ci.entries[i].Name > token
	}))
}

func (ci *cacheInterval) append(objs *cacheInterval) {
	idx := ci.find(objs.token)
	ci.entries = append(ci.entries[:idx], objs.entries...)
	ci.last = objs.last
	ci.lastAccess = mono.NanoTime()
}

func (ci *cacheInterval) prepend(objs *cacheInterval) {
	debug.Assert(!objs.last)
	objs.append(ci)
	*ci = *objs
}

////////////////
// lsobjCache //
////////////////

// PRECONDITION: `c.mtx` must be at least rlocked.
func (c *lsobjCache) findInterval(token string) *cacheInterval {
	// TODO: finding intervals should be faster than just walking.
	for _, interval := range c.intervals {
		if interval.contains(token) {
			return interval
		}
	}
	return nil
}

// PRECONDITION: `c.mtx` must be locked.
func (c *lsobjCache) merge(start, end, cur *cacheInterval) {
	debug.AssertRWMutexLocked(&c.mtx)

	if start == nil && end == nil {
		c.intervals = append(c.intervals, cur)
	} else if start != nil && end == nil {
		start.append(cur)
	} else if start == nil && end != nil {
		end.prepend(cur)
	} else if start != nil && end != nil {
		if start == end {
			// `cur` is part of some interval.
			return
		}

		start.append(cur)
		start.append(end)
		c.removeInterval(end)
	} else {
		debug.Assert(false)
	}
}

// PRECONDITION: `c.mtx` must be locked.
func (c *lsobjCache) removeInterval(ci *cacheInterval) {
	debug.AssertRWMutexLocked(&c.mtx)

	// TODO: this should be faster
	for idx := range c.intervals {
		if c.intervals[idx] == ci {
			ci.entries = nil
			c.intervals = append(c.intervals[:idx], c.intervals[idx+1:]...)
			return
		}
	}
}

func (c *lsobjCache) get(token string, objCnt int64, params reqParams) (entries cmn.LsoEntries, hasEnough bool) {
	c.mtx.RLock()
	if interval := c.findInterval(token); interval != nil {
		entries, hasEnough = interval.get(token, objCnt, params)
	}
	c.mtx.RUnlock()
	return
}

func (c *lsobjCache) set(token string, entries cmn.LsoEntries, size int64) {
	var (
		end *cacheInterval
		cur = &cacheInterval{
			token:      token,
			entries:    entries,
			last:       len(entries) < int(size),
			lastAccess: mono.NanoTime(),
		}
	)
	c.mtx.Lock()
	start := c.findInterval(token)
	if len(cur.entries) > 0 {
		end = c.findInterval(entries[len(entries)-1].Name)
	}
	c.merge(start, end, cur)
	c.mtx.Unlock()
}

func (c *lsobjCache) invalidate() {
	c.mtx.Lock()
	c.intervals = nil
	c.mtx.Unlock()
}

/////////////////
// lsobjCaches //
/////////////////

func (c *lsobjCaches) get(reqID cacheReqID, token string, objCnt int64) (entries cmn.LsoEntries, hasEnough bool) {
	if v, ok := c.caches.Load(reqID); ok {
		if entries, hasEnough = v.(*lsobjCache).get(token, objCnt, reqParams{}); hasEnough {
			return
		}
	}

	// When `prefix` is requested we must also check if there is enough entries
	// in the "main" (whole bucket) cache with given prefix.
	if reqID.prefix != "" {
		// We must adjust parameters and cache id.
		params := reqParams{prefix: reqID.prefix}
		reqID = cacheReqID{bck: reqID.bck}

		if v, ok := c.caches.Load(reqID); ok {
			return v.(*lsobjCache).get(token, objCnt, params)
		}
	}
	return nil, false
}

func (c *lsobjCaches) set(reqID cacheReqID, token string, entries cmn.LsoEntries, size int64) {
	v, _ := c.caches.LoadOrStore(reqID, &lsobjCache{})
	v.(*lsobjCache).set(token, entries, size)
}

func (c *lsobjCaches) invalidate(bck *cmn.Bck) {
	c.caches.Range(func(key, value any) bool {
		id := key.(cacheReqID)
		if id.bck.Equal(bck) {
			value.(*lsobjCache).invalidate()
		}
		return true
	})
}

// TODO: factor-in memory pressure.
func (c *lsobjCaches) housekeep() (num int) {
	var toRemove []*cacheInterval
	c.caches.Range(func(key, value any) bool {
		cache := value.(*lsobjCache)
		cache.mtx.Lock()
		for _, interval := range cache.intervals {
			num++
			if mono.Since(interval.lastAccess) > cacheIntervalTTL {
				toRemove = append(toRemove, interval)
			}
		}
		for _, interval := range toRemove {
			cache.removeInterval(interval)
		}
		if len(cache.intervals) == 0 {
			c.caches.Delete(key)
		}
		cache.mtx.Unlock()
		toRemove = toRemove[:0]
		return true
	})
	return
}
