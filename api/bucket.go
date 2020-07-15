// Package api provides RESTful API to AIS object storage
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
)

const (
	initialPollInterval = 50 * time.Millisecond
	maxPollInterval     = 10 * time.Second
)

// SetBucketProps API
//
// Set the properties of a bucket using the bucket name and the entire bucket
// property structure to be set.
// Validation of the properties passed in is performed by AIStore Proxy.
func SetBucketProps(baseParams BaseParams, bck cmn.Bck, props cmn.BucketPropsToUpdate, query ...url.Values) (string, error) {
	b := cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActSetBprops, Value: props})
	return patchBucketProps(baseParams, bck, b, query...)
}

// ResetBucketProps API
//
// Reset the properties of a bucket, identified by its name, to the global configuration.
func ResetBucketProps(baseParams BaseParams, bck cmn.Bck, query ...url.Values) (string, error) {
	b := cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActResetBprops})
	return patchBucketProps(baseParams, bck, b, query...)
}

func patchBucketProps(baseParams BaseParams, bck cmn.Bck, body []byte, query ...url.Values) (xactID string, err error) {
	var q url.Values
	if len(query) > 0 {
		q = query[0]
	}
	q = cmn.AddBckToQuery(q, bck)
	baseParams.Method = http.MethodPatch
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name)
	err = DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: path, Body: body, Query: q}, &xactID)
	return
}

// HeadBucket API
//
// Returns the properties of a bucket specified by its name.
// Converts the string type fields returned from the HEAD request to their
// corresponding counterparts in the BucketProps struct
func HeadBucket(baseParams BaseParams, bck cmn.Bck, query ...url.Values) (p *cmn.BucketProps, err error) {
	var (
		path = cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name)
		q    url.Values
	)
	p = &cmn.BucketProps{}
	baseParams.Method = http.MethodHead
	if len(query) > 0 {
		q = query[0]
	}
	q = cmn.AddBckToQuery(q, bck)

	resp, err := doHTTPRequestGetResp(ReqParams{BaseParams: baseParams, Path: path, Query: q}, nil)
	if err != nil {
		return
	}

	err = cmn.IterFields(p, func(tag string, field cmn.IterField) (error, bool) {
		return field.SetValue(resp.Header.Get(tag), true /*force*/), false
	}, cmn.IterOpts{OnlyRead: false})
	return
}

// ListBuckets API
//
// provider takes one of Cloud Provider enum names (see cmn/bucket.go). If provider is empty, return all names.
// Otherwise, return cloud or ais bucket names.
func ListBuckets(baseParams BaseParams, queryBcks cmn.QueryBcks) (cmn.BucketNames, error) {
	var (
		bucketNames = cmn.BucketNames{}
		path        = cmn.URLPath(cmn.Version, cmn.Buckets, cmn.AllBuckets)
		query       = cmn.AddBckToQuery(nil, cmn.Bck(queryBcks))
	)

	baseParams.Method = http.MethodGet
	err := DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: path, Query: query}, &bucketNames)
	if err != nil {
		return nil, err
	}
	return bucketNames, nil
}

// GetBucketsSummaries API
//
// Returns bucket summaries for the specified bucket provider (and all bucket summaries for unspecified ("") provider).
func GetBucketsSummaries(baseParams BaseParams, query cmn.QueryBcks, smsg *cmn.SelectMsg) (cmn.BucketsSummaries, error) {
	if smsg == nil {
		smsg = &cmn.SelectMsg{}
	}

	baseParams.Method = http.MethodPost

	reqParams := ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Buckets, query.Name),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Query:      cmn.AddBckToQuery(nil, cmn.Bck(query)),
	}
	var summaries cmn.BucketsSummaries
	if err := waitForAsyncReqComplete(reqParams, cmn.ActSummaryBucket, smsg, &summaries); err != nil {
		return nil, err
	}
	sort.Sort(summaries)
	return summaries, nil
}

// CreateBucket API
//
// CreateBucket sends a HTTP request to a proxy to create an ais bucket with the given name
func CreateBucket(baseParams BaseParams, bck cmn.Bck, ops ...cmn.BucketPropsToUpdate) error {
	if len(ops) > 1 {
		return fmt.Errorf("only a single BucketPropsToUpdate parameter can be accepted")
	}
	var value interface{}
	if len(ops) == 1 {
		value = ops[0]
	}
	baseParams.Method = http.MethodPost
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActCreateLB, Value: value}),
		Query:      cmn.AddBckToQuery(nil, bck),
	})
}

// DestroyBucket API
//
// DestroyBucket sends a HTTP request to a proxy to remove an ais bucket with the given name
func DestroyBucket(baseParams BaseParams, bck cmn.Bck) error {
	baseParams.Method = http.MethodDelete
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActDestroyLB}),
		Query:      cmn.AddBckToQuery(nil, bck),
	})
}

// DoesBucketExist API
//
// DoesBucketExist queries a proxy or target to get a list of all ais buckets,
// returns true if the bucket is present in the list.
func DoesBucketExist(baseParams BaseParams, query cmn.QueryBcks) (bool, error) {
	bcks, err := ListBuckets(baseParams, query)
	if err != nil {
		return false, err
	}
	return bcks.Contains(query), nil
}

// CopyBucket API
//
// CopyBucket creates a new ais bucket newName and
// copies into it contents of the existing oldName bucket
func CopyBucket(baseParams BaseParams, fromBck, toBck cmn.Bck) (xactID string, err error) {
	baseParams.Method = http.MethodPost
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Buckets, fromBck.Name),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActCopyBucket, Name: toBck.Name}),
	}, &xactID)
	return
}

// RenameBucket API
//
// RenameBucket changes the name of a bucket from oldName to newBucketName
func RenameBucket(baseParams BaseParams, oldBck, newBck cmn.Bck) (xactID string, err error) {
	baseParams.Method = http.MethodPost
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Buckets, oldBck.Name),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActRenameLB, Name: newBck.Name}),
	}, &xactID)
	return
}

// DeleteList API
//
// DeleteList sends a HTTP request to remove a list of objects from a bucket
func DeleteList(baseParams BaseParams, bck cmn.Bck, fileslist []string) (string, error) {
	deleteMsg := cmn.ListMsg{ObjNames: fileslist}
	return doListRangeRequest(baseParams, bck, cmn.ActDelete, http.MethodDelete, deleteMsg)
}

// DeleteRange API
//
// DeleteRange sends a HTTP request to remove a range of objects from a bucket
func DeleteRange(baseParams BaseParams, bck cmn.Bck, rng string) (string, error) {
	deleteMsg := cmn.RangeMsg{Template: rng}
	return doListRangeRequest(baseParams, bck, cmn.ActDelete, http.MethodDelete, deleteMsg)
}

// PrefetchList API
//
// PrefetchList sends a HTTP request to prefetch a list of objects from a cloud bucket
func PrefetchList(baseParams BaseParams, bck cmn.Bck, fileslist []string) (string, error) {
	prefetchMsg := cmn.ListMsg{ObjNames: fileslist}
	return doListRangeRequest(baseParams, bck, cmn.ActPrefetch, http.MethodPost, prefetchMsg)
}

// PrefetchRange API
//
// PrefetchRange sends a HTTP request to prefetch a range of objects from a cloud bucket
func PrefetchRange(baseParams BaseParams, bck cmn.Bck, rng string) (string, error) {
	prefetchMsg := cmn.RangeMsg{Template: rng}
	return doListRangeRequest(baseParams, bck, cmn.ActPrefetch, http.MethodPost, prefetchMsg)
}

// EvictList API
//
// EvictList sends a HTTP request to evict a list of objects from a cloud bucket
func EvictList(baseParams BaseParams, bck cmn.Bck, fileslist []string) (string, error) {
	evictMsg := cmn.ListMsg{ObjNames: fileslist}
	return doListRangeRequest(baseParams, bck, cmn.ActEvictObjects, http.MethodDelete, evictMsg)
}

// EvictRange API
//
// EvictRange sends a HTTP request to evict a range of objects from a cloud bucket
func EvictRange(baseParams BaseParams, bck cmn.Bck, rng string) (string, error) {
	evictMsg := cmn.RangeMsg{Template: rng}
	return doListRangeRequest(baseParams, bck, cmn.ActEvictObjects, http.MethodDelete, evictMsg)
}

// EvictCloudBucket API
//
// EvictCloudBucket sends a HTTP request to a proxy to evict an entire cloud bucket from the AIStore
// - the operation results in eliminating all traces of the specified cloud bucket in the AIStore
func EvictCloudBucket(baseParams BaseParams, bck cmn.Bck, query ...url.Values) error {
	var q url.Values
	if len(query) > 0 {
		q = query[0]
	}
	if bck.Provider == "" {
		bck.Provider = cmn.AnyCloud
	}
	baseParams.Method = http.MethodDelete
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActEvictCB}),
		Query:      cmn.AddBckToQuery(q, bck),
	})
}

// Polling:
// 1. The function sends the requests as is (smsg.UUID should be empty) to initiate
//    asynchronous task. The destination returns ID of a newly created task
// 2. Starts polling: request destination with received UUID in a loop while
//    the destination returns StatusAccepted=task is still running
//	  Time between requests is dynamic: it starts at 200ms and increases
//	  by half after every "not-StatusOK" request. It is limited with 10 seconds
// 3. Breaks loop on error
// 4. If the destination returns status code StatusOK, it means the response
//    contains the real data and the function returns the response to the caller
func waitForAsyncReqComplete(reqParams ReqParams, action string, smsg *cmn.SelectMsg, v interface{}) error {
	cmn.Assert(action == cmn.ActListObjects || action == cmn.ActSummaryBucket)
	var (
		initRespMsg  = &cmn.InitTaskRespMsg{}
		buff         = bytes.NewBuffer(nil)
		sleep        = initialPollInterval
		actMsg       = cmn.ActionMsg{Action: action, Value: smsg}
		changeOfTask bool
	)
	if reqParams.Query == nil {
		reqParams.Query = url.Values{}
	}
	reqParams.Query.Set(cmn.URLParamNextPage, "1")
	reqParams.Body = cmn.MustMarshal(actMsg)
	resp, err := doHTTPRequestGetResp(reqParams, buff)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusAccepted {
		if resp.StatusCode == http.StatusOK {
			return errors.New("expected 202 response code on first call, got 200")
		}
		return fmt.Errorf("invalid response code: %d", resp.StatusCode)
	}
	reqParams.Query.Del(cmn.URLParamNextPage)

	// Receiver started async task and returned the initResMsg
	if err := jsoniter.Unmarshal(buff.Bytes(), initRespMsg); err != nil {
		return err
	}
	if smsg.UUID == "" {
		smsg.UUID = initRespMsg.UUID
	}
	if smsg.UUID != "" {
		reqParams.Query.Set(cmn.URLParamUUID, smsg.UUID)
	}
	actMsg = handleAsyncReqAccepted(initRespMsg, action, smsg, reqParams)

	// Poll async task for http.StatusOK completion
	for {
		reqParams.Body = cmn.MustMarshal(actMsg)
		buff.Reset()
		resp, err = doHTTPRequestGetResp(reqParams, buff)
		if err != nil {
			return err
		}
		if !changeOfTask && resp.StatusCode == http.StatusAccepted {
			// NOTE: async task changed on the fly
			if err := jsoniter.Unmarshal(buff.Bytes(), initRespMsg); err != nil {
				return err
			}
			actMsg = handleAsyncReqAccepted(initRespMsg, action, smsg, reqParams)
			changeOfTask = true
		}
		if resp.StatusCode == http.StatusOK {
			if err := jsoniter.Unmarshal(buff.Bytes(), v); err != nil {
				return err
			}
			break
		}
		time.Sleep(sleep)
		if sleep < maxPollInterval {
			sleep += sleep / 2
		}
	}
	return err
}

func handleAsyncReqAccepted(initRespMsg *cmn.InitTaskRespMsg, action string, smsg *cmn.SelectMsg, reqParams ReqParams) (actMsg cmn.ActionMsg) {
	if smsg != nil {
		msg := cmn.SelectMsg{}
		msg = *smsg
		if msg.UUID == "" {
			msg.UUID = initRespMsg.UUID
		}
		actMsg = cmn.ActionMsg{Action: action, Value: &msg}
	}
	reqParams.Query.Set(cmn.URLParamUUID, initRespMsg.UUID)
	return
}

// ListObjects API

// ListObjects returns list of objects in a bucket. numObjects is the
// maximum number of objects returned by ListObjects (0 - return all objects in a bucket)
func ListObjects(baseParams BaseParams, bck cmn.Bck, smsg *cmn.SelectMsg, numObjects uint) (*cmn.BucketList, error) {
	baseParams.Method = http.MethodPost
	var (
		err     error
		q       = cmn.AddBckToQuery(url.Values{}, bck)
		path    = cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name)
		bckList = &cmn.BucketList{
			Entries: make([]*cmn.BucketEntry, 0, cmn.DefaultListPageSize),
		}

		// Temporary page for intermediate results
		tmpPage = &cmn.BucketList{}
	)

	if smsg == nil {
		smsg = &cmn.SelectMsg{}
	}

	// An optimization to read as few objects from bucket as possible.
	// `toRead` is the current number of objects `ListObjects` must read before
	// returning the list. Every cycle the loop reads objects by pages and
	// decreases `toRead` by the number of received objects. When `toRead` gets less
	// than `pageSize`, the loop does the final request with reduced `pageSize`.
	toRead := numObjects
	smsg.UUID = ""

	pageSize := smsg.PageSize
	if pageSize == 0 {
		pageSize = cmn.DefaultListPageSize
	}

	for iter := 1; ; iter++ {
		if toRead != 0 && toRead <= pageSize {
			smsg.PageSize = toRead
		}

		reqParams := ReqParams{
			BaseParams: baseParams,
			Path:       path,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Query:      q,
		}

		page := tmpPage
		if iter == 1 {
			// On first iteration use just `bckList` to prevent additional allocations.
			page = bckList
		} else if iter > 1 {
			cmn.Assert(smsg.UUID != "")
			reqParams.Query.Set(cmn.URLParamUUID, smsg.UUID)
			// On later iterations just allocate temporary page.
			//
			// NOTE: do not try to optimize this code by allocating the page
			// on the second iteration and reusing it - it will not work since
			// the Unmarshaler/Decoder will reuse the entry pointers what will
			// result in duplications and incorrect output.
			page.Entries = make([]*cmn.BucketEntry, 0, pageSize)
		}

		if err := waitForAsyncReqComplete(reqParams, cmn.ActListObjects, smsg, &page); err != nil {
			return nil, err
		}

		// First iteration uses `bckList` directly so there is no need to append.
		if iter > 1 {
			bckList.Entries = append(bckList.Entries, page.Entries...)
			bckList.PageMarker = page.PageMarker
		}

		if page.PageMarker == "" {
			smsg.PageMarker = ""
			break
		}

		// NOTE: toRead == 0 means reading all objects with no limit
		if toRead > 0 {
			if n := int(toRead) - len(page.Entries); n <= 0 {
				break
			} else {
				toRead = uint(n)
			}
		}

		smsg.PageMarker = page.PageMarker
	}

	return bckList, err
}

// ListObjectsPage returns the first page of bucket objects
// On success the function updates msg.PageMarker, so a client can reuse
// the message to fetch the next page
func ListObjectsPage(baseParams BaseParams, bck cmn.Bck, smsg *cmn.SelectMsg) (*cmn.BucketList, error) {
	baseParams.Method = http.MethodPost
	if smsg == nil {
		smsg = &cmn.SelectMsg{}
	}

	reqParams := ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name),
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Query: cmn.AddBckToQuery(url.Values{}, bck),
	}

	page := &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, cmn.DefaultListPageSize)}
	if err := waitForAsyncReqComplete(reqParams, cmn.ActListObjects, smsg, &page); err != nil {
		return nil, err
	}
	smsg.PageMarker = page.PageMarker
	return page, nil
}

// ListObjectsFast returns list of objects in a bucket.
// Build an object list with minimal set of properties: name and size.
// All SelectMsg fields except prefix do not work and are skipped.
// Function always returns the whole list of objects without paging
func ListObjectsFast(baseParams BaseParams, bck cmn.Bck, smsg *cmn.SelectMsg, invalidateCache ...bool) (bckList *cmn.BucketList, err error) {
	if smsg == nil {
		smsg = &cmn.SelectMsg{}
	}

	preallocSize := cmn.DefaultListPageSize
	if smsg.PageSize != 0 {
		preallocSize = smsg.PageSize
	}

	smsg.Fast = true
	smsg.Cached = true

	baseParams.Method = http.MethodPost
	reqParams := ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Query:      cmn.AddBckToQuery(url.Values{}, bck),
	}
	bckList = &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, preallocSize)}
	if err = waitForAsyncReqComplete(reqParams, cmn.ActListObjects, smsg, &bckList); err != nil {
		if len(invalidateCache) == 0 || invalidateCache[0] {
			ListObjectsInvalidateCache(baseParams, bck, smsg)
		}
		return nil, err
	}

	if len(invalidateCache) == 0 || invalidateCache[0] {
		err = ListObjectsInvalidateCache(baseParams, bck, smsg)
	}
	return bckList, err
}

// TODO: remove this function after introducing mechanism detecting bucket changes.
func ListObjectsInvalidateCache(params BaseParams, bck cmn.Bck, selMsg *cmn.SelectMsg) error {
	params.Method = http.MethodPost
	var (
		path = cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name)
		q    = url.Values{}
	)

	q = cmn.AddBckToQuery(q, bck)
	actMsg := cmn.ActionMsg{Action: cmn.ActInvalListCache, Value: selMsg}
	return DoHTTPRequest(ReqParams{
		Query:      q,
		BaseParams: params,
		Path:       path,
		Body:       cmn.MustMarshal(actMsg),
	})
}

// Handles the List/Range operations (delete, prefetch)
func doListRangeRequest(baseParams BaseParams, bck cmn.Bck, action, method string, listRangeMsg interface{}) (xactID string, err error) {
	baseParams.Method = method
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: action, Value: listRangeMsg}),
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Query: cmn.AddBckToQuery(nil, bck),
	}, &xactID)
	return
}

func ECEncodeBucket(baseParams BaseParams, bck cmn.Bck, data, parity int) (xactID string, err error) {
	baseParams.Method = http.MethodPost
	// without `string` conversion it makes base64 from []byte in `Body`
	ecConf := string(cmn.MustMarshal(&cmn.ECConfToUpdate{DataSlices: &data, ParitySlices: &parity}))
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActECEncode, Value: ecConf}),
		Query:      cmn.AddBckToQuery(nil, bck),
	}, &xactID)
	return
}
