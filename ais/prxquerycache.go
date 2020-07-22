// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"sort"
	"sync"

	"github.com/NVIDIA/aistore/cmn"
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
// are per target structure that consist of entries that couldn't be included to
// "main buffer" yet. When buffer doesn't contain enough entries the new entries
// are fetched and added to "leftovers". After this they are merged and put into
// "main buffer" so they can be returned to the client.
//
// Caching was designed to be used by multiple requests (clients) so it is
// concurrent-safe. Each request is identified by id (`cacheReqID`). The requests
// that share the same id will also share a common cache. Cache consist of
// (contiguous) intervals which contain entries. Only when request can be fully
// answered by a single interval is considered valid response. Otherwise cache
// cannot be trusted (we don't know how many objects can be in the gap).

type (
	// Single buffer per target.
	queryBufferTarget struct {
		// Determines if the target is done with listing.
		done bool
		// Leftovers entries which we keep locally so they will not be requested
		// again by the proxy. Out of these `currentBuff` is extended.
		entries []*cmn.BucketEntry
	}

	// Single request buffer that corresponds to single `uuid`.
	queryBuffer struct {
		// Currently maintained buffer that keeps the entries which are sorted
		// and ready to be dispatched to the client.
		currentBuff []*cmn.BucketEntry
		// Buffers for each target that are finally merged and the entries are
		// appended to `currentBuff`.
		leftovers map[string]*queryBufferTarget // targetID (string) -> target buffer
	}

	// Contains all query buffers.
	queryBuffers struct {
		buffers sync.Map // request uuid (string) -> buffer (*queryBuffer)
	}

	// Cache request ID. This identifies and splits requests into
	// multiple caches that these requests can use.
	// TODO: Having a cache per bucket may be too optimistic. We may need to take
	//  into account other factors and `SelectMsg` parameters.
	cacheReqID struct {
		bck cmn.Bck
	}

	// Single (contiguous) interval of entries.
	cacheInterval struct {
		// Contains the previous entry (`PageMarker`) that was requested to get
		// this interval. Thanks to this we can match and merge two adjacent
		// intervals.
		token string
		// Entries that are contained in this interval. They are sorted and ready
		// to be dispatched to the client.
		entries []*cmn.BucketEntry
		// Determines if this is the last page/interval (this means there is no
		// more objects after the last entry).
		last bool
	}

	// Single cache that corresponds to single `cacheReqID`.
	queryCache struct {
		mtx       sync.RWMutex
		intervals []*cacheInterval
	}

	// Contains all query caches.
	queryCaches struct {
		caches sync.Map // cache_id (string, see: `cacheReqID`) -> cache (*queryCache)
	}
)

// TODO: eventually these variables should not exist as a globals.
var (
	qb = &queryBuffers{}
	qc = &queryCaches{}
)

func mergeTargetBuffers(lists map[string]*queryBufferTarget) (entries []*cmn.BucketEntry) {
	for _, l := range lists {
		entries = append(entries, l.entries...)
	}

	if len(entries) == 0 {
		return entries
	}

	cmn.SortBckEntries(entries)

	minObj := ""
	for _, list := range lists {
		if list.done || len(list.entries) == 0 {
			continue
		}
		if minObj == "" || list.entries[len(list.entries)-1].Name < minObj {
			minObj = list.entries[len(list.entries)-1].Name
		}
	}
	if minObj == "" {
		return entries
	}

	idx := sort.Search(len(entries), func(i int) bool {
		return entries[i].Name > minObj
	})
	entries = entries[:idx]
	return entries
}

func (b *queryBuffer) hasEnough(token string, size uint) bool {
	if size == 0 {
		return false
	}

	idx := sort.Search(len(b.currentBuff), func(i int) bool {
		return b.currentBuff[i].Name > token
	})
	return uint(len(b.currentBuff[idx:])) >= size
}

func (b *queryBuffer) get(token string, size uint) []*cmn.BucketEntry {
	newEntries := mergeTargetBuffers(b.leftovers)
	b.currentBuff = append(b.currentBuff, newEntries...)
	for id := range b.leftovers {
		b.leftovers[id].entries = nil
	}
	idx := sort.Search(len(b.currentBuff), func(i int) bool {
		return b.currentBuff[i].Name > token
	})
	b.currentBuff = b.currentBuff[idx:]

	if size > uint(len(b.currentBuff)) {
		size = uint(len(b.currentBuff))
	}
	entries := b.currentBuff[:size]
	b.currentBuff = b.currentBuff[size:]
	return entries
}

func (b *queryBuffer) add(id string, entries []*cmn.BucketEntry, size uint) {
	b.leftovers[id] = &queryBufferTarget{
		entries: entries,
		done:    uint(len(entries)) < size,
	}
}

func (b *queryBuffers) hasEnough(id, token string, size uint) bool {
	v, ok := b.buffers.LoadOrStore(id, &queryBuffer{
		leftovers: make(map[string]*queryBufferTarget),
	})
	if !ok {
		return false
	}
	return v.(*queryBuffer).hasEnough(token, size)
}

func (b *queryBuffers) last(id, token string) string {
	v, ok := b.buffers.LoadOrStore(id, &queryBuffer{
		leftovers: make(map[string]*queryBufferTarget),
	})
	if !ok {
		return token
	}
	buffer := v.(*queryBuffer)
	if len(buffer.currentBuff) == 0 {
		return token
	}

	last := buffer.currentBuff[len(buffer.currentBuff)-1].Name
	if cmn.PageMarkerIncludesObject(token, last) {
		return token
	}
	return last
}

func (b *queryBuffers) get(id, token string, size uint) []*cmn.BucketEntry {
	v, _ := b.buffers.Load(id)
	return v.(*queryBuffer).get(token, size)
}

func (b *queryBuffers) set(id, targetID string, entries []*cmn.BucketEntry, size uint) {
	v, _ := b.buffers.LoadOrStore(id, &queryBuffer{
		leftovers: make(map[string]*queryBufferTarget),
	})
	v.(*queryBuffer).add(targetID, entries, size)
}

func (c cacheReqID) String() string { return c.bck.String() }

func (ci *cacheInterval) contains(token string) bool {
	if ci.token == token {
		return true
	}
	if len(ci.entries) > 0 {
		return ci.entries[0].Name <= token && token <= ci.entries[len(ci.entries)-1].Name
	}
	return false
}

func (ci *cacheInterval) get(token string, objCnt uint) (entries []*cmn.BucketEntry, hasEnough bool) {
	var (
		start = ci.find(token)
		end   = uint(start) + objCnt
	)
	if ci.last {
		end = cmn.MinUint(uint(len(ci.entries)), end)
	}
	if end <= uint(len(ci.entries)) {
		return ci.entries[start:end], true
	}
	return nil, false
}

func (ci *cacheInterval) find(token string) (idx int) {
	if ci.token == token {
		return 0
	}
	return sort.Search(len(ci.entries), func(i int) bool {
		return ci.entries[i].Name > token
	})
}

func (ci *cacheInterval) append(objs *cacheInterval) {
	idx := ci.find(objs.token)
	ci.entries = append(ci.entries[:idx], objs.entries...)
	ci.last = objs.last
}

func (ci *cacheInterval) prepend(objs *cacheInterval) {
	cmn.Assert(!objs.last)
	idx := objs.find(ci.token)
	ci.entries = append(objs.entries[idx:], ci.entries...)
}

// PRECONDITION: `c.mtx` must be rlocked.
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
		cmn.Assert(false)
	}
}

// PRECONDITION: `c.mtx` must be locked.
func (c *queryCache) removeInterval(ci *cacheInterval) {
	// TODO: this should be faster
	for idx := range c.intervals {
		if c.intervals[idx] == ci {
			c.intervals = append(c.intervals[:idx], c.intervals[idx+1:]...)
			return
		}
	}
}

func (c *queryCache) get(token string, objCnt uint) (entries []*cmn.BucketEntry, hasEnough bool) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	if interval := c.findInterval(token); interval != nil {
		return interval.get(token, objCnt)
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
			token:   token,
			entries: entries,
			last:    uint(len(entries)) < size,
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

func (c *queryCaches) get(reqID cacheReqID, token string, objCnt uint) (entries []*cmn.BucketEntry, hasEnough bool) {
	v, ok := c.caches.Load(reqID.String())
	if !ok {
		return nil, false
	}
	return v.(*queryCache).get(token, objCnt)
}

func (c *queryCaches) set(reqID cacheReqID, token string, entries []*cmn.BucketEntry, size uint) {
	v, _ := c.caches.LoadOrStore(reqID.String(), &queryCache{})
	v.(*queryCache).set(token, entries, size)
}

func (c *queryCaches) invalidate(reqID cacheReqID) {
	v, ok := c.caches.Load(reqID.String())
	if !ok {
		return
	}
	v.(*queryCache).invalidate()
}
