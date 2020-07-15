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

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
	jsoniter "github.com/json-iterator/go"
)

// The motivation behind list-objects caching is to (drastically) reduce latency
// of listing large buckets by multiple users.
// This includes (but is not limited to) the AI use case when training workers execute the same
// logic and list the same dataset.

// When a user asks AIS proxy for the next N random objects (in a given order), the user cannot
// know where those objects are located in the cluster. In the worst-case scenario, all objects
// could reside on a single target. Hence, we query each target for the N (objects),
// merge-sort the results, and select the first N from it. Naively, we would be discarding the
// rest - cache, though, allows us /not to forget/ but use the results for the subsequent requests
// and across multiple users.

// A given cache instance is defined by the (bucket, prefix, fast) tuple. The main entry point is
// the next() method that returns the next N objects. Caches populate themselves from the storage
// targets on as-needed basis.

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
// listObjCache -> (bucket, prefix, fast - from smsg) -> TARGET ID -> locTarget

// Cache invalidation
// If error occurs when fetching information from targets, task's cache is invalidated.
// Otherwise cache is invalidated when the proxy is low on memory resources.
// User can explicitly invalidate cache (free the memory to the system) via API call.

const hkListObjectName = "list-objects-cache"

type (
	// TODO: when starting to list, run XactBckLoadLomCache on each target async
	listObjCache struct {
		mtx  sync.Mutex
		p    *proxyrunner
		reqs map[string]*locReq // string(bck, prefix, fast) ->  *locReq
	}

	locReq struct {
		mtx       sync.Mutex
		targets   map[string]*locTarget // target ID -> *locTarget
		bck       *cluster.Bck
		parent    *listObjCache
		msg       *cmn.SelectMsg
		lastUsage int64
	}

	locTarget struct {
		mtx    sync.Mutex
		parent *locReq
		t      *cluster.Snode
		buff   []*cmn.BucketEntry
		done   bool
	}

	locTargetResp struct {
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
	listCache             *listObjCache
	bucketPrefixStaleTime = 5 * cmn.GCO.Get().Client.ListObjects
)

func newListObjectsCache(p *proxyrunner) *listObjCache {
	return &listObjCache{p: p, reqs: make(map[string]*locReq)}
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

	now := mono.NanoTime()
	listCache.mtx.Lock()
	defer listCache.mtx.Unlock()

	for k, v := range listCache.reqs {
		if v.lastUsage+int64(bucketPrefixStaleTime) < now {
			delete(listCache.reqs, k)
		}
	}

	return bucketPrefixStaleTime
}

func newRequestCacheEntry(parent *listObjCache, bck *cluster.Bck, msg *cmn.SelectMsg) *locReq {
	return &locReq{
		parent:  parent,
		bck:     bck,
		targets: make(map[string]*locTarget),
		msg:     msg,
	}
}

func newTargetCacheEntry(parent *locReq, t *cluster.Snode) *locTarget {
	return &locTarget{parent: parent, t: t}
}

//////////////////////////
//   listObjCache   //
//////////////////////////

func (c *listObjCache) next(smap *cluster.Smap, smsg cmn.SelectMsg, bck *cluster.Bck, pageSize uint, nextPage bool) (result fetchResult) {
	cmn.Assert(smsg.UUID != "")
	if smap.CountTargets() == 0 {
		return fetchResult{err: fmt.Errorf("no targets registered")}
	}
	entries := c.allTargetsEntries(smsg, smap, bck)
	cmn.Assert(len(entries) > 0)
	entries[0].parent.mtx.Lock()
	result = c.initResultsFromEntries(entries, smsg, pageSize, smsg.UUID, nextPage)
	if result.allOK && result.err == nil {
		result = c.fetchAll(entries, smsg, pageSize)
	}
	entries[0].parent.mtx.Unlock()

	if result.err != nil {
		c.mtx.Lock()
		delete(c.reqs, smsg.ListObjectsCacheID(bck.Bck))
		c.mtx.Unlock()
	}
	return result
}

func (c *listObjCache) targetEntry(t *cluster.Snode, smsg cmn.SelectMsg, bck *cluster.Bck) *locTarget {
	id := smsg.ListObjectsCacheID(bck.Bck)
	c.mtx.Lock()
	requestEntry, ok := c.reqs[id]
	if !ok {
		requestEntry = newRequestCacheEntry(c, bck, &smsg)
		c.reqs[id] = requestEntry
	}
	c.mtx.Unlock()
	defer func() {
		requestEntry.lastUsage = mono.NanoTime()
	}()

	requestEntry.mtx.Lock()
	targetEntry, ok := requestEntry.targets[t.ID()]
	if !ok {
		targetEntry = newTargetCacheEntry(requestEntry, t)
		requestEntry.targets[t.ID()] = targetEntry
	}
	requestEntry.mtx.Unlock()
	return targetEntry
}

func (c *listObjCache) leftovers(smsg cmn.SelectMsg, bck *cluster.Bck) map[string]*locTarget {
	if smsg.Passthrough {
		return nil
	}
	id := smsg.ListObjectsCacheID(bck.Bck)
	requestEntry, ok := c.getRequestEntry(id)
	if !ok {
		return nil
	}

	// find pages that are unused or partially used
	requestEntry.mtx.Lock()
	defer requestEntry.mtx.Unlock()
	tce := make(map[string]*locTarget)
	for _, targetEntry := range requestEntry.targets {
		targetEntry.mtx.Lock()
		cnt := len(targetEntry.buff)
		if cnt == 0 || cmn.PageMarkerIncludesObject(smsg.PageMarker, targetEntry.buff[cnt-1].Name) {
			targetEntry.mtx.Unlock()
			continue
		}
		entry, ok := tce[targetEntry.t.ID()]
		if !ok {
			entry = newTargetCacheEntry(targetEntry.parent, targetEntry.t)
			tce[targetEntry.t.ID()] = entry
		}
		entry.done = entry.done || targetEntry.done
		// First case: the entire page was unused
		if !cmn.PageMarkerIncludesObject(smsg.PageMarker, targetEntry.buff[0].Name) {
			entry.buff = append(entry.buff, targetEntry.buff...)
			targetEntry.mtx.Unlock()
			continue
		}
		// Seconds case: partially used page
		cond := func(i int) bool { return !cmn.PageMarkerIncludesObject(smsg.PageMarker, targetEntry.buff[i].Name) }
		idx := sort.Search(len(targetEntry.buff), cond)
		entry.buff = append(entry.buff, targetEntry.buff[idx:]...)
		targetEntry.mtx.Unlock()
	}

	return tce
}

func (c *listObjCache) allTargetsEntries(smsg cmn.SelectMsg, smap *cluster.Smap, bck *cluster.Bck) []*locTarget {
	result := make([]*locTarget, 0, len(smap.Tmap))
	// First, get the data from the cache that was not sent yet
	partial := c.leftovers(smsg, bck)
	for _, t := range smap.Tmap {
		var (
			targetLeftovers *locTarget
			ok              bool
		)
		if smsg.Passthrough {
			// In passthrough mode we have to create "normal" but fake cache page.
			reqEntry := newRequestCacheEntry(c, bck, &smsg)
			entry := newTargetCacheEntry(reqEntry, t)
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

func (c *listObjCache) initResults(smap *cluster.Smap, smsg cmn.SelectMsg, bck *cluster.Bck, size uint, newUUID string) fetchResult {
	entries := c.allTargetsEntries(smsg, smap, bck)
	return c.initResultsFromEntries(entries, smsg, size, newUUID, true)
}

// initResultsFromEntries notifies targets to prepare next objects page.
// It returns information if all calls succeed, and if there were any errors.
func (c *listObjCache) initResultsFromEntries(entries []*locTarget, smsg cmn.SelectMsg, size uint, newUUID string, nextPage bool) fetchResult {
	ch := c.initAllTargets(entries, smsg, size, newUUID, nextPage)
	return gatherTargetListObjsResults(smsg.UUID, ch, 0, &smsg)
}

// fetchAll returns next `size` object names from each target. It include additional information
// if all calls to targets succeeded and if there were any errors. It cache has buffered object names
// it might return results without making any API calls.
func (c *listObjCache) fetchAll(entries []*locTarget, smsg cmn.SelectMsg, size uint) fetchResult {
	wg := &sync.WaitGroup{}
	wg.Add(len(entries))
	resCh := make(chan *locTargetResp, len(entries))
	for _, entry := range entries {
		entry.fetch(smsg, size, wg, resCh)
	}

	wg.Wait()
	close(resCh)
	return gatherTargetListObjsResults(smsg.UUID, resCh, len(entries), &smsg)
}

// Discard all entries of given task which were included in marker `until`.
func (c *listObjCache) discard(smsg *cmn.SelectMsg, bck *cluster.Bck) {
	id := smsg.ListObjectsCacheID(bck.Bck)
	c.mtx.Lock()
	delete(c.reqs, id)
	c.mtx.Unlock()
}

func (c *listObjCache) getRequestEntry(cacheID string) (*locReq, bool) {
	c.mtx.Lock()
	req, ok := c.reqs[cacheID]
	c.mtx.Unlock()
	return req, ok
}

// Gathers init results for each target on `resultCh`
func (c *listObjCache) initAllTargets(entries []*locTarget, smsg cmn.SelectMsg, size uint, newUUID string, nextPage bool) (resultCh chan *locTargetResp) {
	resultCh = make(chan *locTargetResp, len(entries))
	wg := &sync.WaitGroup{}
	wg.Add(len(entries))
	for _, targetEntry := range entries {
		targetEntry.init(smsg, size, wg, resultCh, newUUID, nextPage)
	}
	wg.Wait()
	close(resultCh)
	return
}

//////////////////////////
//   locTarget   //
/////////////////////////

func (c *locTarget) init(smsg cmn.SelectMsg, size uint, wg *sync.WaitGroup, resCh chan *locTargetResp, newUUID string, nextPage bool) {
	cond := func(i int) bool { return !cmn.PageMarkerIncludesObject(smsg.PageMarker, c.buff[i].Name) }
	idx := sort.Search(len(c.buff), cond)
	remains := c.buff[idx:]
	cacheSufficient := (uint(len(remains)) >= size && size != 0) || c.done
	if cacheSufficient {
		// Everything that is requested is already in the cache, we don't have to do any API calls.
		// Returning StatusOK as if we did a request.
		resCh <- &locTargetResp{status: http.StatusOK, err: nil}
		wg.Done()
		return
	}

	// Make an actual call to the target.
	go func() {
		resCh <- c.initOnRemote(smsg, newUUID, nextPage)
		wg.Done()
	}()
}

func (c *locTarget) initOnRemote(smsg cmn.SelectMsg, newUUID string, nextPage bool) (result *locTargetResp) {
	p := c.parent.parent.p
	bck := c.parent.bck

	_, q := p.initAsyncQuery(bck, &smsg, newUUID, nextPage)
	args := c.newListObjectsTaskMsg(smsg, bck, q) // Changes PageMarker to point to last element in buff.
	status, err := c.renewTaskOnRemote(args)
	return &locTargetResp{status: status, err: err}
}

// Returns next `size` objects or less if no more exists.
// If everything that is requested already is present in the cache, don't make any API calls.
func (c *locTarget) fetch(smsg cmn.SelectMsg, size uint, wg *sync.WaitGroup, resCh chan *locTargetResp) {
	cond := func(i int) bool { return !cmn.PageMarkerIncludesObject(smsg.PageMarker, c.buff[i].Name) }
	j := sort.Search(len(c.buff), cond)
	// discard entries which somehow don't fit the request. They're name is smaller than pageMarker,
	// which means that user already has them from previous requests.
	bf := c.buff[j:]

	// We have everything in cache or target has nothing more.
	// We didn't do init request to the target.
	if (uint(len(bf)) >= size && size != 0) || c.done {
		if size == 0 {
			size = uint(len(bf))
		} else {
			size = uint(cmn.Min(len(bf), int(size)))
		}
		resCh <- &locTargetResp{list: &cmn.BucketList{Entries: bf[:size], UUID: smsg.UUID}, status: http.StatusOK}
		wg.Done()
		return
	}

	go func() {
		resCh <- c.fetchFromRemote(smsg, size)
		wg.Done()
	}()
}

// TODO: gaps, overlaps
func (c *locTarget) mergePage(page []*cmn.BucketEntry) {
	if len(page) == 0 {
		return
	}
	l := len(c.buff)
	if l == 0 {
		c.buff = page
		return
	}
	// The page preceds items in the cache
	if cmn.PageMarkerIncludesObject(c.buff[0].Name, page[len(page)-1].Name) {
		c.buff = append(page, c.buff...)
		return
	}
	// The page follows the cache
	if cmn.PageMarkerIncludesObject(page[0].Name, c.buff[len(c.buff)-1].Name) {
		c.buff = append(c.buff, page...)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("Page %q : %q discarded", page[0].Name, page[len(page)-1].Name)
	}
}

// TODO: it is a kludge called once and before another calle that looks the same.
// Why it is required:
//   - a target cache, when it detects that it does not have enough entries and
//     the target is not done with sending all the data, fetches another page.
//   - after receiving the page, the cache merges it with its list
//   - the trouble is that the cache may be a "temporary" objects, and adding
//     to it automatically discards the entries that are not fit page size
//   - why it can be "temporary": before fetching new data, the proxy cache
//     builds a list of local pages that may be real or temporary
//   - if the target is not done yet, it is fine and the next request detects
//     page missing objects, just requests the new page from target. The target
//     returns the page from its local cache and everything looks good
//   - but if the target is done with streaming. The discarded tail is lost
//     forever, making listObjects to return a few entries fewer
func (rq *locReq) mergeCache(node *cluster.Snode, page []*cmn.BucketEntry) {
	tgt, ok := rq.targets[node.ID()]
	cmn.Assert(ok)
	tgt.mtx.Lock()
	tgt.mergePage(page)
	tgt.mtx.Unlock()
}

// Has to be called with Lock!
// Fetches objects from target, appends them to buffer and returns required number of objects.
func (c *locTarget) fetchFromRemote(smsg cmn.SelectMsg, size uint) *locTargetResp {
	p := c.parent.parent.p
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
		return &locTargetResp{list: nil, status: res.status, err: res.err}
	}
	if len(res.outjson) == 0 {
		s := cmn.Min(int(size), len(c.buff))
		if s == 0 {
			s = len(c.buff)
		}
		return &locTargetResp{list: &cmn.BucketList{Entries: c.buff[:s], UUID: smsg.UUID}, status: res.status, err: res.err}
	}

	bucketList := &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, preallocSize)}
	if err := jsoniter.Unmarshal(res.outjson, &bucketList); err != nil {
		return &locTargetResp{list: nil, status: http.StatusInternalServerError, err: err}
	}
	res.outjson = nil
	if len(bucketList.Entries) < int(size) || size == 0 {
		c.done = true
	}

	if smsg.Passthrough {
		return &locTargetResp{list: bucketList, status: http.StatusOK}
	}

	c.parent.mergeCache(c.t, bucketList.Entries)

	c.mtx.Lock()
	c.mergePage(bucketList.Entries)
	cond := func(i int) bool { return !cmn.PageMarkerIncludesObject(smsg.PageMarker, c.buff[i].Name) }
	j := sort.Search(len(c.buff), cond)
	c.mtx.Unlock()
	j = cmn.Max(j, 0)
	if size != 0 {
		last := cmn.Min(len(c.buff), int(size)+j)
		return &locTargetResp{list: &cmn.BucketList{Entries: c.buff[j:last], UUID: smsg.UUID}, status: http.StatusOK}
	}
	return &locTargetResp{list: &cmn.BucketList{Entries: c.buff[j:], UUID: smsg.UUID}, status: http.StatusOK}
}

// Prepares callArgs for list object init or list objects result call.
// Should be called with Lock or RLock acquired.
func (c *locTarget) newListObjectsTaskMsg(smsg cmn.SelectMsg, bck *cluster.Bck, q url.Values) *callArgs {
	p := c.parent.parent.p
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

func (c *locTarget) renewTaskOnRemote(args *callArgs) (int, error) {
	res := c.parent.parent.p.call(*args)
	return res.status, res.err
}

func gatherTargetListObjsResults(uuid string, ch chan *locTargetResp, expectedListsSize int, smsg *cmn.SelectMsg) (result fetchResult) {
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
