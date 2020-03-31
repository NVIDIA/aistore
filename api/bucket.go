// Package api provides RESTful API to AIS object storage
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/cmn"
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
func SetBucketProps(baseParams BaseParams, bck cmn.Bck, props cmn.BucketPropsToUpdate, query ...url.Values) error {
	b := cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActSetBprops, Value: props})
	return patchBucketProps(baseParams, bck, b, query...)
}

// ResetBucketProps API
//
// Reset the properties of a bucket, identified by its name, to the global configuration.
func ResetBucketProps(baseParams BaseParams, bck cmn.Bck, query ...url.Values) error {
	b := cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActResetBprops})
	return patchBucketProps(baseParams, bck, b, query...)
}

func patchBucketProps(baseParams BaseParams, bck cmn.Bck, body []byte, query ...url.Values) error {
	var q url.Values
	if len(query) > 0 {
		q = query[0]
	}
	q = cmn.AddBckToQuery(q, bck)
	baseParams.Method = http.MethodPatch
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name)
	return DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: path, Body: body, Query: q})
}

// HeadBucket API
//
// Returns the properties of a bucket specified by its name.
// Converts the string type fields returned from the HEAD request to their
// corresponding counterparts in the BucketProps struct
func HeadBucket(baseParams BaseParams, bck cmn.Bck, query ...url.Values) (p cmn.BucketProps, err error) {
	var (
		path = cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name)
		q    url.Values
	)
	baseParams.Method = http.MethodHead
	if len(query) > 0 {
		q = query[0]
	}
	q = cmn.AddBckToQuery(q, bck)

	resp, err := doHTTPRequestGetResp(ReqParams{BaseParams: baseParams, Path: path, Query: q}, nil)
	if err != nil {
		return
	}

	err = cmn.IterFields(&p, func(tag string, field cmn.IterField) (error, bool) {
		return field.SetValue(resp.Header.Get(tag), true /*force*/), false
	})
	if err != nil {
		return p, err
	}
	return
}

// GetBucketNames API
//
// provider takes one of Cloud Provider enum names (see cmn/bucket.go). If provider is empty, return all names.
// Otherwise, return cloud or ais bucket names.
func GetBucketNames(baseParams BaseParams, bck cmn.Bck) (*cmn.BucketNames, error) {
	var (
		bucketNames = &cmn.BucketNames{}
		path        = cmn.URLPath(cmn.Version, cmn.Buckets, cmn.AllBuckets)
		query       = cmn.AddBckToQuery(nil, bck)
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
func GetBucketsSummaries(baseParams BaseParams, bck cmn.Bck, msg *cmn.SelectMsg) (cmn.BucketsSummaries, error) {
	if msg == nil {
		msg = &cmn.SelectMsg{}
	}

	baseParams.Method = http.MethodPost

	reqParams := ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Query:      cmn.AddBckToQuery(nil, bck),
	}
	var summaries cmn.BucketsSummaries
	if err := waitForAsyncReqComplete(reqParams, cmn.ActSummaryBucket, msg, &summaries); err != nil {
		return nil, err
	}
	return summaries, nil
}

// CreateBucket API
//
// CreateBucket sends a HTTP request to a proxy to create an ais bucket with the given name
func CreateBucket(baseParams BaseParams, bck cmn.Bck) error {
	baseParams.Method = http.MethodPost
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActCreateLB}),
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
func DoesBucketExist(baseParams BaseParams, bck cmn.Bck) (bool, error) {
	buckets, err := GetBucketNames(baseParams, bck)
	if err != nil {
		return false, err
	}

	exists := cmn.StringInSlice(bck.Name, buckets.AIS)
	return exists, nil
}

// CopyBucket API
//
// CopyBucket creates a new ais bucket newName and
// copies into it contents of the existing oldName bucket
func CopyBucket(baseParams BaseParams, fromBck, toBck cmn.Bck) error {
	baseParams.Method = http.MethodPost
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Buckets, fromBck.Name),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActCopyBucket, Name: toBck.Name}),
	})
}

// RenameBucket API
//
// RenameBucket changes the name of a bucket from oldName to newBucketName
func RenameBucket(baseParams BaseParams, oldBck, newBck cmn.Bck) error {
	baseParams.Method = http.MethodPost
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Buckets, oldBck.Name),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActRenameLB, Name: newBck.Name}),
	})
}

// DeleteList API
//
// DeleteList sends a HTTP request to remove a list of objects from a bucket
func DeleteList(baseParams BaseParams, bck cmn.Bck, fileslist []string) error {
	deleteMsg := cmn.ListMsg{ObjNames: fileslist}
	return doListRangeRequest(baseParams, bck, cmn.ActDelete, http.MethodDelete, deleteMsg)
}

// DeleteRange API
//
// DeleteRange sends a HTTP request to remove a range of objects from a bucket
func DeleteRange(baseParams BaseParams, bck cmn.Bck, rng string) error {
	deleteMsg := cmn.RangeMsg{Template: rng}
	return doListRangeRequest(baseParams, bck, cmn.ActDelete, http.MethodDelete, deleteMsg)
}

// PrefetchList API
//
// PrefetchList sends a HTTP request to prefetch a list of objects from a cloud bucket
func PrefetchList(baseParams BaseParams, bck cmn.Bck, fileslist []string) error {
	prefetchMsg := cmn.ListMsg{ObjNames: fileslist}
	return doListRangeRequest(baseParams, bck, cmn.ActPrefetch, http.MethodPost, prefetchMsg)
}

// PrefetchRange API
//
// PrefetchRange sends a HTTP request to prefetch a range of objects from a cloud bucket
func PrefetchRange(baseParams BaseParams, bck cmn.Bck, rng string) error {
	prefetchMsg := cmn.RangeMsg{Template: rng}
	return doListRangeRequest(baseParams, bck, cmn.ActPrefetch, http.MethodPost, prefetchMsg)
}

// EvictList API
//
// EvictList sends a HTTP request to evict a list of objects from a cloud bucket
func EvictList(baseParams BaseParams, bck cmn.Bck, fileslist []string) error {
	evictMsg := cmn.ListMsg{ObjNames: fileslist}
	return doListRangeRequest(baseParams, bck, cmn.ActEvictObjects, http.MethodDelete, evictMsg)
}

// EvictRange API
//
// EvictRange sends a HTTP request to evict a range of objects from a cloud bucket
func EvictRange(baseParams BaseParams, bck cmn.Bck, rng string) error {
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
		bck.Provider = cmn.Cloud
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
// 1. The function sends the requests as is (msg.taskID should be 0) to initiate
//    asynchronous task. The destination returns ID of a newly created task
// 2. Starts polling: request destination with received taskID in a loop while
//    the destination returns StatusAccepted=task is still running
//	  Time between requests is dynamic: it starts at 200ms and increases
//	  by half after every "not-StatusOK" request. It is limited with 10 seconds
// 3. Breaks loop on error
// 4. If the destination returns status code StatusOK, it means the response
//    contains the real data and the function returns the response to the caller
func waitForAsyncReqComplete(reqParams ReqParams, action string, origMsg *cmn.SelectMsg, v interface{}) error {
	var (
		changeOfTask bool
		id           string

		sleep  = initialPollInterval
		actMsg = cmn.ActionMsg{Action: action, Value: origMsg}
	)
	if reqParams.Query == nil {
		reqParams.Query = url.Values{}
	}
	reqParams.Body = cmn.MustMarshal(actMsg)
	resp, err := doHTTPRequestGetResp(reqParams, &id)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid response code: %d", resp.StatusCode)
	}
	if resp.StatusCode == http.StatusAccepted {
		// receiver started async task and returned the task ID
		actMsg = handleAsyncReqAccepted(id, action, origMsg, reqParams)
	}

	defer reqParams.Query.Del(cmn.URLParamTaskID)

	// poll async task for http.StatusOK completion
	for {
		reqParams.Body = cmn.MustMarshal(actMsg)
		resp, err = doHTTPRequestGetResp(reqParams, v)
		if err != nil {
			return err
		}
		if !changeOfTask && resp.StatusCode == http.StatusAccepted {
			// NOTE: async task changed on the fly
			actMsg = handleAsyncReqAccepted(id, action, origMsg, reqParams)
			changeOfTask = true
		}
		if resp.StatusCode == http.StatusOK {
			break
		}
		time.Sleep(sleep)
		if sleep < maxPollInterval {
			sleep += sleep / 2
		}
	}
	return err
}

func handleAsyncReqAccepted(id, action string, origMsg *cmn.SelectMsg, reqParams ReqParams) (actMsg cmn.ActionMsg) {
	if origMsg != nil {
		msg := cmn.SelectMsg{}
		msg = *origMsg
		msg.TaskID = id
		actMsg = cmn.ActionMsg{Action: action, Value: &msg}
	}
	reqParams.Query.Set(cmn.URLParamTaskID, id)
	return
}

// ListBucket API
//
// ListBucket returns list of objects in a bucket. numObjects is the
// maximum number of objects returned by ListBucket (0 - return all objects in a bucket)
func ListBucket(baseParams BaseParams, bck cmn.Bck, msg *cmn.SelectMsg, numObjects int, query ...url.Values) (*cmn.BucketList, error) {
	baseParams.Method = http.MethodPost
	var (
		q       = url.Values{}
		path    = cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name)
		bckList = &cmn.BucketList{
			Entries: make([]*cmn.BucketEntry, 0, cmn.DefaultListPageSize),
		}

		// Temporary page for intermediate results
		tmpPage = &cmn.BucketList{}
	)

	if len(query) > 0 {
		q = query[0]
	}
	q = cmn.AddBckToQuery(q, bck)

	if msg == nil {
		msg = &cmn.SelectMsg{}
	}

	// An optimization to read as few objects from bucket as possible.
	// `toRead` is the current number of objects `ListBucket` must read before
	// returning the list. Every cycle the loop reads objects by pages and
	// decreases `toRead` by the number of received objects. When `toRead` gets less
	// than `pageSize`, the loop does the final request with reduced `pageSize`.
	toRead := numObjects
	msg.TaskID = ""

	pageSize := msg.PageSize
	if pageSize == 0 {
		pageSize = cmn.DefaultListPageSize
	}

	for iter := 1; ; iter++ {
		if toRead != 0 && toRead <= pageSize {
			msg.PageSize = toRead
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
			// On later iterations just allocate temporary page.
			//
			// NOTE: do not try to optimize this code by allocating the page
			// on the second iteration and reusing it - it will not work since
			// the Unmarshaler/Decoder will reuse the entry pointers what will
			// result in duplications and incorrect output.
			page.Entries = make([]*cmn.BucketEntry, 0, pageSize)
		}

		if err := waitForAsyncReqComplete(reqParams, cmn.ActListObjects, msg, &page); err != nil {
			return nil, err
		}

		// First iteration uses `bckList` directly so there is no need to append.
		if iter > 1 {
			bckList.Entries = append(bckList.Entries, page.Entries...)
			bckList.PageMarker = page.PageMarker
		}

		if page.PageMarker == "" {
			msg.PageMarker = ""
			break
		}

		if toRead != 0 {
			toRead -= len(page.Entries)
			if toRead <= 0 {
				break
			}
		}

		msg.PageMarker = page.PageMarker
	}

	return bckList, nil
}

// ListBucketPage returns the first page of bucket objects
// On success the function updates msg.PageMarker, so a client can reuse
// the message to fetch the next page
func ListBucketPage(baseParams BaseParams, bck cmn.Bck, msg *cmn.SelectMsg, query ...url.Values) (*cmn.BucketList, error) {
	baseParams.Method = http.MethodPost
	if msg == nil {
		msg = &cmn.SelectMsg{}
	}

	q := url.Values{}
	if len(query) > 0 {
		q = query[0]
	}

	reqParams := ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name),
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Query: cmn.AddBckToQuery(q, bck),
	}

	page := &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, cmn.DefaultListPageSize)}
	if err := waitForAsyncReqComplete(reqParams, cmn.ActListObjects, msg, &page); err != nil {
		return nil, err
	}
	msg.PageMarker = page.PageMarker
	return page, nil
}

// ListBucketFast returns list of objects in a bucket.
// Build an object list with minimal set of properties: name and size.
// All SelectMsg fields except prefix do not work and are skipped.
// Function always returns the whole list of objects without paging
func ListBucketFast(baseParams BaseParams, bck cmn.Bck, msg *cmn.SelectMsg, query ...url.Values) (*cmn.BucketList, error) {
	if msg == nil {
		msg = &cmn.SelectMsg{}
	}

	preallocSize := cmn.DefaultListPageSize
	if msg.PageSize != 0 {
		preallocSize = msg.PageSize
	}

	msg.Fast = true
	msg.Cached = true

	var q url.Values
	if len(query) > 0 {
		q = query[0]
	}
	baseParams.Method = http.MethodPost
	reqParams := ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Query:      cmn.AddBckToQuery(q, bck),
	}
	bckList := &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, preallocSize)}
	if err := waitForAsyncReqComplete(reqParams, cmn.ActListObjects, msg, &bckList); err != nil {
		return nil, err
	}
	return bckList, nil
}

// Handles the List/Range operations (delete, prefetch)
func doListRangeRequest(baseParams BaseParams, bck cmn.Bck, action, method string, listRangeMsg interface{}) error {
	baseParams.Method = method
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: action, Value: listRangeMsg}),
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Query: cmn.AddBckToQuery(nil, bck),
	})
}

func ECEncodeBucket(baseParams BaseParams, bck cmn.Bck) error {
	baseParams.Method = http.MethodPost
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActECEncode}),
		Query:      cmn.AddBckToQuery(nil, bck),
	})
}
