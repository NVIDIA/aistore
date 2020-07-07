// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
	jsoniter "github.com/json-iterator/go"
)

// The motivation behind listObjectsCache is to limit transportation overhead
// between proxy and targets when exactly the same list objects requests are made.
// This includes use-case when separate AI-training machines execute the same job,
// ask for the same objects list.

// When user asks proxy for next N objects (in given order), it doesn't know where
// those objects are. In the worst case scenario they can all be on a single target.
// Hence, we have to query each target for N objects, merge results and select first N from it.
// If being naive, we would have to discard the rest of the objects.
// The cache allows us to not forget objects which we didn't use. Instead, we keep them in memory
// for future requests.

// Entry point for the cache usage is Next(N) method, which returns next N objects
// for given (bucket, prefix, fast, page marker). If not all requested objects are present
// in the cache, they are fetched from targets.

// The flow:
// - User asks for N objects
// For each target:
//   next(N):
//     - if N objects from the target are in cache return them
//     - if 0 objects are in cache, fetch N objects to cache and return them
//     - if 0 < x < N objects are in cache, fetch N objects to cache and return
//       first N objects from cache
// objs = selectFirst(N, merge(targetResults))
// send objs to user

// Cache structure:
// listObjectsCache -> (bucket, prefix, fast - from smsg) -> page marker -> TARGET ID -> targetCacheEntry

// Cache invalidation
// If error occurs when fetching information from targets, task's cache is invalidated.
// Otherwise cache is invalidated when the proxy is low on memory resources.
// User can explicitly invalidate cache (free the memory to the system) via API call.

const hkListObjectName = "list-objects-cache"

type (
	listObjectsCache struct {
		p            *proxyrunner
		requestCache sync.Map // string(bck, prefix, fast) ->  *requestCacheEntry
	}

	requestCacheEntry struct {
		pageMarkersCache sync.Map // page Marker -> *pageMarkerCacheEntry
		parent           *listObjectsCache
		lastUsage        time.Time
	}

	pageMarkerCacheEntry struct {
		mtx          sync.Mutex
		targetsCache sync.Map // target ID -> *targetCacheEntry
		parent       *requestCacheEntry
		bck          *cluster.Bck
	}

	targetCacheEntry struct {
		parent *pageMarkerCacheEntry
		t      *cluster.Snode
		buff   []*cmn.BucketEntry
		done   bool
	}

	targetListObjsResult struct {
		status int
		err    error
		list   *cmn.BucketList
	}

	fetchResult struct {
		err   error
		lists []*cmn.BucketList
		allOK bool
	}
)

var (
	listCache             *listObjectsCache
	bucketPrefixStaleTime = 5 * cmn.GCO.Get().Client.ListObjects
)

func newListObjectsCache(p *proxyrunner) *listObjectsCache {
	return &listObjectsCache{
		p: p,
	}
}

func initListObjectsCache(p *proxyrunner) {
	// ListObjects timeout was set to 0 in config.
	// We should be housekeep from time to time anyway.
	if bucketPrefixStaleTime == 0 {
		bucketPrefixStaleTime = 5 * time.Minute
	}

	listCache = newListObjectsCache(p)
	hk.Reg(hkListObjectName, func() time.Duration { return housekeepListCache(p) }, bucketPrefixStaleTime)
}

// TODO: Remove old entries, or those which take a lot of memory
// until MemPressure/PctMemUsed falls below some level.
func housekeepListCache(p *proxyrunner) time.Duration {
	if p.gmm.MemPressure() <= memsys.MemPressureModerate {
		return bucketPrefixStaleTime
	}

	now := time.Now()
	listCache.requestCache.Range(func(k, v interface{}) bool {
		requestEntry := v.(*requestCacheEntry)
		if requestEntry.lastUsage.Add(bucketPrefixStaleTime).Before(now) {
			listCache.requestCache.Delete(k)
		}
		return true
	})

	return bucketPrefixStaleTime
}

func newPageMarkerCacheEntry(bck *cluster.Bck, parent *requestCacheEntry) *pageMarkerCacheEntry {
	return &pageMarkerCacheEntry{
		parent: parent,
		bck:    bck,
	}
}

func newRequestCacheEntry(parent *listObjectsCache) *requestCacheEntry {
	return &requestCacheEntry{
		parent: parent,
	}
}

func newTargetCacheEntry(parent *pageMarkerCacheEntry, t *cluster.Snode) *targetCacheEntry {
	return &targetCacheEntry{
		parent: parent,
		t:      t,
	}
}

//////////////////////////
//   listObjectsCache   //
//////////////////////////

func (c *listObjectsCache) next(smap *cluster.Smap, smsg cmn.SelectMsg, bck *cluster.Bck, pageSize uint) (result fetchResult) {
	cmn.Assert(smsg.UUID != "")
	if smap.CountTargets() == 0 {
		return fetchResult{err: fmt.Errorf("no targets registered")}
	}
	entries := c.allTargetsEntries(smsg, smap, bck)
	cmn.Assert(len(entries) > 0)
	entries[0].parent.mtx.Lock()
	result = c.initResultsFromEntries(entries, smsg, pageSize, smsg.UUID)
	if result.allOK && result.err == nil {
		result = c.fetchAll(entries, smsg, pageSize)
	}
	entries[0].parent.mtx.Unlock()

	if result.err != nil {
		c.requestCache.Delete(smsg.ListObjectsCacheID(bck.Bck))
	}

	return result
}

func (c *listObjectsCache) targetEntry(t *cluster.Snode, smsg cmn.SelectMsg, bck *cluster.Bck) *targetCacheEntry {
	id := smsg.ListObjectsCacheID(bck.Bck)
	requestEntry, ok := c.getRequestEntry(id)
	if !ok {
		v, _ := c.requestCache.LoadOrStore(id, newRequestCacheEntry(c))
		requestEntry = v.(*requestCacheEntry)
	}

	defer func() {
		requestEntry.lastUsage = time.Now()
	}()

	pageMarkerEntry, ok := requestEntry.getPageMarkerCacheEntry(smsg.PageMarker)
	if !ok {
		v, _ := requestEntry.pageMarkersCache.LoadOrStore(smsg.PageMarker, newPageMarkerCacheEntry(bck, requestEntry))
		pageMarkerEntry = v.(*pageMarkerCacheEntry)
	}

	targetEntry, ok := pageMarkerEntry.targetsCache.Load(t.DaemonID)
	if !ok {
		targetEntry, _ = pageMarkerEntry.targetsCache.LoadOrStore(t.DaemonID, newTargetCacheEntry(pageMarkerEntry, t))
	}
	return targetEntry.(*targetCacheEntry)
}

func (c *listObjectsCache) leftovers(smsg cmn.SelectMsg, bck *cluster.Bck) map[string]*targetCacheEntry {
	if smsg.Passthrough {
		return nil
	}
	id := smsg.ListObjectsCacheID(bck.Bck)
	requestEntry, ok := c.getRequestEntry(id)
	if !ok {
		return nil
	}

	// find pages that are unused or partially used
	tce := make(map[string]*targetCacheEntry)
	requestEntry.pageMarkersCache.Range(func(k, v interface{}) bool {
		pageMarkerEntry := v.(*pageMarkerCacheEntry)
		pageMarkerEntry.targetsCache.Range(func(k, v interface{}) bool {
			targetEntry := v.(*targetCacheEntry)
			cnt := len(targetEntry.buff)
			if cnt == 0 {
				return true
			}
			// Last object is included into PageMarker = the page was already sent
			if cmn.PageMarkerIncludesObject(smsg.PageMarker, targetEntry.buff[cnt-1].Name) {
				return true
			}
			entry, ok := tce[targetEntry.t.ID()]
			if !ok {
				entry = &targetCacheEntry{parent: targetEntry.parent, t: targetEntry.t, buff: make([]*cmn.BucketEntry, 0)}
				tce[targetEntry.t.ID()] = entry
			}
			entry.done = entry.done || targetEntry.done
			// First case: the entire page was unused
			if !cmn.PageMarkerIncludesObject(smsg.PageMarker, targetEntry.buff[0].Name) {
				entry.buff = append(entry.buff, targetEntry.buff...)
				return true
			}
			// Seconds case: partially used page
			cond := func(i int) bool { return !cmn.PageMarkerIncludesObject(smsg.PageMarker, targetEntry.buff[i].Name) }
			idx := sort.Search(len(targetEntry.buff), cond)
			entry.buff = append(entry.buff, targetEntry.buff[idx:]...)
			return true
		})
		return true
	})

	return tce
}

func (c *listObjectsCache) allTargetsEntries(smsg cmn.SelectMsg, smap *cluster.Smap, bck *cluster.Bck) []*targetCacheEntry {
	result := make([]*targetCacheEntry, 0, len(smap.Tmap))
	// First, get the data from the cache that was not sent yet
	partial := c.leftovers(smsg, bck)
	for _, t := range smap.Tmap {
		var (
			targetLeftovers *targetCacheEntry
			ok              bool
		)
		if smsg.Passthrough {
			// In passthrough mode we have to create "normal" but fake cache page.
			reqEntry := newRequestCacheEntry(c)
			pmEntry := newPageMarkerCacheEntry(bck, reqEntry)
			entry := newTargetCacheEntry(pmEntry, t)
			result = append(result, entry)
			continue
		}
		if len(partial) != 0 {
			targetLeftovers, ok = partial[t.ID()]
		}
		// If nothing is found for a target in the cache, initialize a new
		// cache page. Without it, the new page leftovers can be lost.
		if !ok || len(targetLeftovers.buff) == 0 {
			targetEntry := c.targetEntry(t, smsg, bck)
			result = append(result, targetEntry)
			continue
		}

		// Order of pages in cache may be random. Sort them right away
		less := func(i, j int) bool { return targetLeftovers.buff[i].Name < targetLeftovers.buff[j].Name }
		sort.Slice(targetLeftovers.buff, less)
		result = append(result, targetLeftovers)
	}
	return result
}

func (c *listObjectsCache) initResults(smap *cluster.Smap, smsg cmn.SelectMsg, bck *cluster.Bck, size uint, newUUID string) fetchResult {
	entries := c.allTargetsEntries(smsg, smap, bck)
	return c.initResultsFromEntries(entries, smsg, size, newUUID)
}

// initResultsFromEntries notifies targets to prepare next objects page.
// It returns information if all calls succeed, and if there were any errors.
func (c *listObjectsCache) initResultsFromEntries(entries []*targetCacheEntry, smsg cmn.SelectMsg, size uint, newUUID string) fetchResult {
	ch := c.initAllTargets(entries, smsg, size, newUUID)
	return gatherTargetListObjsResults(smsg.UUID, ch, 0, &smsg)
}

// fetchAll returns next `size` object names from each target. It include additional information
// if all calls to targets succeeded and if there were any errors. It cache has buffered object names
// it might return results without making any API calls.
func (c *listObjectsCache) fetchAll(entries []*targetCacheEntry, smsg cmn.SelectMsg, size uint) fetchResult {
	wg := &sync.WaitGroup{}
	wg.Add(len(entries))
	resCh := make(chan *targetListObjsResult, len(entries))
	for _, entry := range entries {
		entry.fetch(smsg, size, wg, resCh)
	}

	wg.Wait()
	close(resCh)
	return gatherTargetListObjsResults(smsg.UUID, resCh, len(entries), &smsg)
}

// Discard all entries of given task which were included in marker `until`.
func (c *listObjectsCache) discard(smsg *cmn.SelectMsg, bck *cluster.Bck) {
	id := smsg.ListObjectsCacheID(bck.Bck)
	if _, ok := c.getRequestEntry(id); ok {
		c.requestCache.Delete(id)
	}
}

func (c *listObjectsCache) getRequestEntry(cacheID string) (*requestCacheEntry, bool) {
	v, ok := c.requestCache.Load(cacheID)
	if !ok {
		return nil, false
	}
	return v.(*requestCacheEntry), true
}

////////////////////////////
//   requestCacheEntry 	 //
///////////////////////////
func (c *requestCacheEntry) getPageMarkerCacheEntry(pageMarker string) (*pageMarkerCacheEntry, bool) {
	v, ok := c.pageMarkersCache.Load(pageMarker)
	if !ok {
		return nil, false
	}
	return v.(*pageMarkerCacheEntry), true
}

//////////////////////////////
//   pageMarkerCacheEntry   //
//////////////////////////////

// Gathers init results for each target on `resultCh`
func (c *listObjectsCache) initAllTargets(entries []*targetCacheEntry, smsg cmn.SelectMsg, size uint, newUUID string) (resultCh chan *targetListObjsResult) {
	resultCh = make(chan *targetListObjsResult, len(entries))
	wg := &sync.WaitGroup{}
	wg.Add(len(entries))
	for _, targetEntry := range entries {
		targetEntry.init(smsg, size, wg, resultCh, newUUID)
	}
	wg.Wait()
	close(resultCh)
	return
}

//////////////////////////
//   targetCacheEntry   //
/////////////////////////

func (c *targetCacheEntry) init(smsg cmn.SelectMsg, size uint, wg *sync.WaitGroup, resCh chan *targetListObjsResult, newUUID string) {
	cacheSufficient := (uint(len(c.buff)) >= size && size != 0) || c.done
	if !smsg.Passthrough && cacheSufficient {
		// Everything that is requested is already in the cache, we don't have to do any API calls.
		// Returning StatusOK as if we did a request.
		resCh <- &targetListObjsResult{status: http.StatusOK, err: nil}
		wg.Done()
		return
	}

	// Make an actual call to the target.
	go func() {
		resCh <- c.initOnRemote(smsg, newUUID)
		wg.Done()
	}()
}

func (c *targetCacheEntry) initOnRemote(smsg cmn.SelectMsg, newUUID string) (result *targetListObjsResult) {
	p := c.parent.parent.parent.p
	bck := c.parent.bck

	_, q := p.initAsyncQuery(bck, &smsg, newUUID)
	args := c.newListObjectsTaskMsg(smsg, bck, q) // Changes PageMarker to point to last element in buff.
	status, err := c.renewTaskOnRemote(args)
	return &targetListObjsResult{status: status, err: err}
}

// Returns next `size` objects or less if no more exists.
// If everything that is requested already is present in the cache, don't make any API calls.
func (c *targetCacheEntry) fetch(smsg cmn.SelectMsg, size uint, wg *sync.WaitGroup, resCh chan *targetListObjsResult) {
	j := 0
	for _, entry := range c.buff {
		if entry.Name < smsg.PageMarker {
			j++
		} else {
			break
		}
	}
	// discard entries which somehow don't fit the request. They're name is smaller than pageMarker,
	// which means that user already has them from previous requests.
	c.buff = cmn.DiscardFirstEntries(c.buff, j)

	// We have everything in cache or target has nothing more.
	// We didn't do init request to the target.
	if (uint(len(c.buff)) >= size && size != 0) || c.done {
		if size == 0 {
			size = uint(len(c.buff))
		} else {
			size = uint(cmn.Min(len(c.buff), int(size)))
		}
		resCh <- &targetListObjsResult{list: &cmn.BucketList{Entries: c.buff[:size]}, status: http.StatusOK}
		wg.Done()
		return
	}

	go func() {
		resCh <- c.fetchFromRemote(smsg, size)
		wg.Done()
	}()
}

// Has to be called with Lock!
// Fetches objects from target, appends them to buffer and returns required number of objects.
func (c *targetCacheEntry) fetchFromRemote(smsg cmn.SelectMsg, size uint) *targetListObjsResult {
	p := c.parent.parent.parent.p
	bck := c.parent.bck

	args := c.newListObjectsTaskMsg(smsg, bck, newTaskResultQuery(bck.Bck))
	args.req.Method = http.MethodPost

	// Target  prepare the final result.
	res := p.call(*args)

	preallocSize := cmn.DefaultListPageSize
	if smsg.PageSize != 0 {
		preallocSize = smsg.PageSize
	}

	if res.err != nil {
		return &targetListObjsResult{list: nil, status: res.status, err: res.err}
	}
	if len(res.outjson) == 0 {
		s := cmn.Min(int(size), len(c.buff))
		if s == 0 {
			s = len(c.buff)
		}
		return &targetListObjsResult{list: &cmn.BucketList{Entries: c.buff[:s]}, status: res.status, err: res.err}
	}

	bucketList := &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, preallocSize)}
	if err := jsoniter.Unmarshal(res.outjson, &bucketList); err != nil {
		return &targetListObjsResult{list: nil, status: http.StatusInternalServerError, err: err}
	}
	res.outjson = nil
	if len(bucketList.Entries) < int(size) || size == 0 {
		c.done = true
	}

	if smsg.Passthrough {
		return &targetListObjsResult{list: bucketList, status: http.StatusOK}
	}

	c.buff = append(c.buff, bucketList.Entries...)
	entries := c.buff[:cmn.Min(int(size), len(c.buff))]
	if size == 0 {
		// return all object names
		entries = c.buff
	}

	return &targetListObjsResult{list: &cmn.BucketList{Entries: entries}, status: http.StatusOK}
}

// Prepares callArgs for list object init or list objects result call.
// Should be called with Lock or RLock acquired.
func (c *targetCacheEntry) newListObjectsTaskMsg(smsg cmn.SelectMsg, bck *cluster.Bck, q url.Values) *callArgs {
	p := c.parent.parent.parent.p
	if len(c.buff) > 0 {
		// Request only new objects.
		smsg.PageMarker = c.buff[len(c.buff)-1].Name
	}

	// Cache all props, filter only requested props later.
	smsg.Props = strings.Join(cmn.GetPropsAll, ",")

	var (
		config = cmn.GCO.Get()
		smap   = p.owner.smap.get()
		aisMsg = p.newAisMsg(&cmn.ActionMsg{Action: cmn.ActListObjects, Value: smsg}, smap, nil)
		body   = cmn.MustMarshal(aisMsg)
	)
	return &callArgs{
		si: c.t,
		req: cmn.ReqArgs{
			Method: http.MethodPost,
			Path:   cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name),
			Query:  q,
			Body:   body,
		},
		timeout: config.Timeout.MaxHostBusy + config.Timeout.CplaneOperation,
	}
}

func (c *targetCacheEntry) renewTaskOnRemote(args *callArgs) (int, error) {
	res := c.parent.parent.parent.p.call(*args)
	return res.status, res.err
}

func gatherTargetListObjsResults(uuid string, ch chan *targetListObjsResult, expectedListsSize int, smsg *cmn.SelectMsg) (result fetchResult) {
	result.allOK = true
	allNotFound := true
	result.lists = make([]*cmn.BucketList, 0, expectedListsSize)
	requestedProps := smsg.PropsSet()

	for singleResult := range ch {
		result.err = singleResult.err
		if singleResult.status == http.StatusNotFound {
			continue
		}
		allNotFound = false
		if result.err != nil || singleResult.status != http.StatusOK {
			result.allOK = false
			break
		}

		result.lists = append(result.lists, filteredPropsList(singleResult.list, requestedProps))
	}

	if allNotFound {
		result.allOK = false
		result.err = fmt.Errorf("task %s %s", uuid, cmn.DoesNotExist)
	}
	return result
}

// Filters only requested props. New bucket list is allocated!
func filteredPropsList(list *cmn.BucketList, propsSet cmn.StringSet) (resultList *cmn.BucketList) {
	if list == nil {
		return nil
	}

	resultList = &cmn.BucketList{}
	resultList.PageMarker = list.PageMarker
	resultList.Entries = make([]*cmn.BucketEntry, len(list.Entries))

	for i, entry := range list.Entries {
		newEntry := &cmn.BucketEntry{}
		resultList.Entries[i] = newEntry
		newEntry.Flags = entry.Flags
		newEntry.Name = entry.Name

		if propsSet.Contains(cmn.GetPropsChecksum) {
			newEntry.Checksum = entry.Checksum
		}
		if propsSet.Contains(cmn.GetPropsSize) {
			newEntry.Size = entry.Size
		}
		if propsSet.Contains(cmn.GetPropsAtime) {
			newEntry.Atime = entry.Atime
		}
		if propsSet.Contains(cmn.GetPropsVersion) {
			newEntry.Version = entry.Version
		}
		if propsSet.Contains(cmn.GetTargetURL) {
			newEntry.TargetURL = entry.TargetURL
		}
		if propsSet.Contains(cmn.GetPropsCopies) {
			newEntry.Copies = entry.Copies
		}
	}

	return resultList
}

func newTaskResultQuery(bck cmn.Bck) (q url.Values) {
	q = cmn.AddBckToQuery(q, bck)
	q.Set(cmn.URLParamTaskAction, cmn.TaskResult)
	q.Set(cmn.URLParamSilent, "true")
	return q
}
