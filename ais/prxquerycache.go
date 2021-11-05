// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/hk"
)

// This file contains implementation for two concepts:
//  * BUFFER - container for a single request that keeps entries so they won't
//    be re-requested. Thanks to buffering we eliminate the case when a given
//    object is requested more than once.
//  * CACHE  - container shared by multiple requests which are identified with
//    the same id. Thanks to caching we reuse previously calculated requests.
//
// Buffering was designed to work for single request and is identified by list
// objects uuid. Each buffer consist of "main buffer", that contains entries
// which are ready to be returned to the client (user), and "leftovers" which
// are per target structures that consist of entries that couldn't be included to
// "main buffer" yet. When buffer doesn't contain enough entries the new entries
// are fetched and added to "leftovers". After this they are merged and put into
// "main buffer" so they can be returned to the client.
//
// Caching was designed to be used by multiple requests (clients) so it is
// thread safe. Each request is identified by id (`cacheReqID`). The requests
// that share the same id will also share a common cache. Cache consist of
// (contiguous) intervals which contain entries. Only when request can be fully
// answered by a single interval is considered valid response. Otherwise cache
// cannot be trusted (we don't know how many objects can be in the gap).

const (
	// Determines how long "cache interval" is valid. If there was no access to
	// given interval for this long it will be removed.
	cacheLiveTimeout = 40 * time.Minute
	// Determines how often cache housekeeping function is invoked.
	cacheHkInterval = 10 * time.Minute

	// Determines how long buffer is valid. If there was no access to
	// a given buffer for this long it will be forgotten.
	bufferLiveTimeout = 10 * time.Minute
	// Determines how often buffer housekeeping function is invoked.
	bufferHkInterval = 5 * time.Minute
)

type (
	// Request buffer per target.
	queryBufferTarget struct {
		// Determines if the target is done with listing.
		done bool
		// Leftovers entries which we keep locally so they will not be requested
		// again by the proxy. Out of these `currentBuff` is extended.
		entries []*cmn.BucketEntry
	}

	// Request buffer that corresponds to single `uuid`.
	queryBuffer struct {
		// Contains the last entry that was returned to the user.
		nextToken string
		// Currently maintained buffer that keeps the entries which are sorted
		// and ready to be dispatched to the client.
		currentBuff []*cmn.BucketEntry
		// Buffers for each target that are finally merged and the entries are
		// appended to `currentBuff`.
		leftovers map[string]*queryBufferTarget // targetID (string) -> target buffer
		// Contains the timestamp of the last access to this buffer. If given
		// predefined time passes the buffer will be forgotten.
		lastAccess atomic.Int64
	}

	// Contains all query buffers.
	queryBuffers struct {
		buffers sync.Map // request uuid (string) -> buffer (*queryBuffer)
	}

	// Cache request ID. This identifies and splits requests into
	// multiple caches that these requests can use.
	cacheReqID struct {
		bck    cmn.Bck
		prefix string
	}

	// Single (contiguous) interval of entries.
	cacheInterval struct {
		// Contains the previous entry (`ContinuationToken`) that was requested
		// to get this interval. Thanks to this we can match and merge two
		// adjacent intervals.
		token string
		// Entries that are contained in this interval. They are sorted and ready
		// to be dispatched to the client.
		entries []*cmn.BucketEntry
		// Determines if this is the last page/interval (this means there is no
		// more objects after the last entry).
		last bool
		// Contains the timestamp of the last access to this interval. If given
		// predefined time passes the interval will be removed.
		lastAccess int64
	}

	// Contains additional parameters to interval request.
	reqParams struct {
		prefix string
	}

	// Single cache that corresponds to single `cacheReqID`.
	queryCache struct {
		mtx       sync.RWMutex
		intervals []*cacheInterval
	}

	// Contains all query caches.
	queryCaches struct {
		caches sync.Map // cache id (cacheReqID) -> cache (*queryCache)
	}

	queryMem struct {
		b *queryBuffers
		c *queryCaches
	}
)

func (qm *queryMem) init() {
	qm.b = newQueryBuffers()
	qm.c = newQueryCaches()
}

// mergeTargetBuffers merges `b.leftovers` buffers into `b.currentBuff`.
// It returns `filled` equal to `true` if there was anything to merge, otherwise `false`.
func (b *queryBuffer) mergeTargetBuffers() (filled bool) {
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
		minObj  = ""
		entries = make([]*cmn.BucketEntry, 0, totalCnt)
	)
	for _, list := range b.leftovers {
		entries = append(entries, list.entries...)

		if list.done || len(list.entries) == 0 {
			continue
		}
		if minObj == "" || list.entries[len(list.entries)-1].Name < minObj {
			minObj = list.entries[len(list.entries)-1].Name
		}
	}

	cmn.SortBckEntries(entries)

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

func (b *queryBuffer) get(token string, size uint) (entries []*cmn.BucketEntry, hasEnough bool) {
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

	if size > uint(len(entries)) {
		// In case we don't have enough entries and we haven't filled anything then
		// we must request more (if filled then we don't have enough because it's end).
		if !filled {
			return nil, false
		}
		size = uint(len(entries))
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

func (b *queryBuffer) set(id string, entries []*cmn.BucketEntry, size uint) {
	if b.leftovers == nil {
		b.leftovers = make(map[string]*queryBufferTarget, 5)
	}
	b.leftovers[id] = &queryBufferTarget{
		entries: entries,
		done:    uint(len(entries)) < size,
	}
	b.lastAccess.Store(mono.NanoTime())
}

func newQueryBuffers() *queryBuffers {
	b := &queryBuffers{}
	hk.Reg("query-buffer", b.housekeep, bufferHkInterval)
	return b
}

func (b *queryBuffers) last(id, token string) string {
	v, ok := b.buffers.LoadOrStore(id, &queryBuffer{})
	if !ok {
		return token
	}
	buffer := v.(*queryBuffer)
	if len(buffer.currentBuff) == 0 {
		return token
	}

	last := buffer.currentBuff[len(buffer.currentBuff)-1].Name
	if cmn.TokenIncludesObject(token, last) {
		return token
	}
	return last
}

func (b *queryBuffers) get(id, token string, size uint) (entries []*cmn.BucketEntry, hasEnough bool) {
	v, _ := b.buffers.LoadOrStore(id, &queryBuffer{})
	return v.(*queryBuffer).get(token, size)
}

func (b *queryBuffers) set(id, targetID string, entries []*cmn.BucketEntry, size uint) {
	v, _ := b.buffers.LoadOrStore(id, &queryBuffer{})
	v.(*queryBuffer).set(targetID, entries, size)
}

func (b *queryBuffers) housekeep() time.Duration {
	b.buffers.Range(func(key, value interface{}) bool {
		buffer := value.(*queryBuffer)
		if mono.Since(buffer.lastAccess.Load()) > bufferLiveTimeout {
			b.buffers.Delete(key)
		}
		return true
	})
	return bufferHkInterval
}

func (ci *cacheInterval) contains(token string) bool {
	if ci.token == token {
		return true
	}
	if len(ci.entries) > 0 {
		return ci.entries[0].Name <= token && token <= ci.entries[len(ci.entries)-1].Name
	}
	return false
}

func (ci *cacheInterval) get(token string, objCnt uint, params reqParams) (entries []*cmn.BucketEntry, hasEnough bool) {
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
				return []*cmn.BucketEntry{}, true
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

	end := cos.MinUint(uint(len(entries)), objCnt)
	if params.prefix != "" {
		// Move `end-1` to last entry that starts with `params.prefix`.
		for ; end > 0; end-- {
			if strings.HasPrefix(entries[end-1].Name, params.prefix) {
				break
			}
		}
		if !ci.last && end < uint(len(entries)) {
			// We filtered out entries that start with `params.prefix` and
			// the entries are fully contained in the interval, examples:
			//  * interval = ["a", "ma", "mb", "z"], token = "", objCnt = 4, prefix = "m"
			//  * interval = ["a", "z"], token = "", objCnt = 2, prefix = "a"
			return entries[:end], true
		}
	}
	entries = entries[:end]

	if ci.last || uint(len(entries)) >= objCnt {
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
	cos.Assert(!objs.last)
	objs.append(ci)
	*ci = *objs
}

// PRECONDITION: `c.mtx` must be at least rlocked.
func (c *queryCache) findInterval(token string) *cacheInterval {
	// TODO: finding intervals should be faster than just walking.
	for _, interval := range c.intervals {
		if interval.contains(token) {
			return interval
		}
	}
	return nil
}

// PRECONDITION: `c.mtx` must be locked.
func (c *queryCache) merge(start, end, cur *cacheInterval) {
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
		cos.Assert(false)
	}
}

// PRECONDITION: `c.mtx` must be locked.
func (c *queryCache) removeInterval(ci *cacheInterval) {
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

func (c *queryCache) get(token string, objCnt uint, params reqParams) (entries []*cmn.BucketEntry, hasEnough bool) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	if interval := c.findInterval(token); interval != nil {
		return interval.get(token, objCnt, params)
	}
	return nil, false
}

func (c *queryCache) set(token string, entries []*cmn.BucketEntry, size uint) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	var (
		start = c.findInterval(token)
		end   *cacheInterval
		cur   = &cacheInterval{
			token:      token,
			entries:    entries,
			last:       uint(len(entries)) < size,
			lastAccess: mono.NanoTime(),
		}
	)
	if len(cur.entries) > 0 {
		end = c.findInterval(entries[len(entries)-1].Name)
	}
	c.merge(start, end, cur)
}

func (c *queryCache) invalidate() {
	c.mtx.Lock()
	c.intervals = nil
	c.mtx.Unlock()
}

func newQueryCaches() *queryCaches {
	c := &queryCaches{}
	hk.Reg("query-cache", c.housekeep, cacheHkInterval)
	return c
}

func (c *queryCaches) get(reqID cacheReqID, token string, objCnt uint) (entries []*cmn.BucketEntry, hasEnough bool) {
	if v, ok := c.caches.Load(reqID); ok {
		if entries, hasEnough = v.(*queryCache).get(token, objCnt, reqParams{}); hasEnough {
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
			return v.(*queryCache).get(token, objCnt, params)
		}
	}
	return nil, false
}

func (c *queryCaches) set(reqID cacheReqID, token string, entries []*cmn.BucketEntry, size uint) {
	v, _ := c.caches.LoadOrStore(reqID, &queryCache{})
	v.(*queryCache).set(token, entries, size)
}

func (c *queryCaches) invalidate(bck cmn.Bck) {
	c.caches.Range(func(key, value interface{}) bool {
		id := key.(cacheReqID)
		if id.bck.Equal(bck) {
			value.(*queryCache).invalidate()
		}
		return true
	})
}

// TODO: Missing housekeep based on memory pressure.
func (c *queryCaches) housekeep() time.Duration {
	c.caches.Range(func(key, value interface{}) bool {
		cache := value.(*queryCache)
		cache.mtx.Lock()
		defer cache.mtx.Unlock()

		var toRemove []*cacheInterval
		for _, interval := range cache.intervals {
			if mono.Since(interval.lastAccess) > cacheLiveTimeout {
				toRemove = append(toRemove, interval)
			}
		}
		for _, interval := range toRemove {
			cache.removeInterval(interval)
		}
		if len(cache.intervals) == 0 {
			c.caches.Delete(key)
		}
		return true
	})
	return cacheHkInterval
}
