// Package api provides RESTful API to AIS object storage
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
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
func SetBucketProps(baseParams BaseParams, bck cmn.Bck, props cmn.BucketPropsToUpdate, query ...url.Values) error {
	var (
		b, err    = jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActSetBprops, Value: props})
		optParams = OptionalParams{}
	)
	if err != nil {
		return err
	}
	if len(query) > 0 {
		optParams.Query = query[0]
	}
	optParams.Query = cmn.AddBckToQuery(optParams.Query, bck)

	baseParams.Method = http.MethodPatch
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name)
	_, err = DoHTTPRequest(baseParams, path, b, optParams)
	return err
}

// ResetBucketProps API
//
// Reset the properties of a bucket, identified by its name, to the global configuration.
func ResetBucketProps(baseParams BaseParams, bck cmn.Bck, query ...url.Values) error {
	var (
		b, err    = jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActResetProps})
		optParams = OptionalParams{}
	)
	if err != nil {
		return err
	}
	if len(query) > 0 {
		optParams.Query = query[0]
	}
	optParams.Query = cmn.AddBckToQuery(optParams.Query, bck)

	baseParams.Method = http.MethodPut
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name)
	_, err = DoHTTPRequest(baseParams, path, b, optParams)
	return err
}

// HeadBucket API
//
// Returns the properties of a bucket specified by its name.
// Converts the string type fields returned from the HEAD request to their
// corresponding counterparts in the BucketProps struct
func HeadBucket(baseParams BaseParams, bck cmn.Bck, query ...url.Values) (p cmn.BucketProps, err error) {
	var (
		path      = cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name)
		optParams = OptionalParams{}
		r         *http.Response
	)
	baseParams.Method = http.MethodHead
	if len(query) > 0 {
		optParams.Query = query[0]
	}
	optParams.Query = cmn.AddBckToQuery(optParams.Query, bck)

	if r, err = doHTTPRequestGetResp(baseParams, path, nil, optParams); err != nil {
		return
	}
	defer r.Body.Close()

	err = cmn.IterFields(&p, func(tag string, field cmn.IterField) (error, bool) {
		return field.SetValue(r.Header.Get(tag), true /*force*/), false
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
		optParams   = OptionalParams{Query: query}
	)

	baseParams.Method = http.MethodGet
	b, err := DoHTTPRequest(baseParams, path, nil, optParams)
	if err != nil {
		return nil, err
	}
	if len(b) != 0 {
		if err = jsoniter.Unmarshal(b, &bucketNames); err != nil {
			return nil, fmt.Errorf("failed to unmarshal bucket names, err: %v - [%s]", err, string(b))
		}
	} else {
		return nil, fmt.Errorf("empty response instead of empty bucket list from %s", baseParams.URL)
	}
	return bucketNames, nil
}

// GetBucketsSummaries API
//
// Returns bucket summaries for the specified bucket provider (and all bucket summaries for unspecified ("") provider).
func GetBucketsSummaries(baseParams BaseParams, bck cmn.Bck, msg *cmn.SelectMsg) (cmn.BucketsSummaries, error) {
	var (
		q    = cmn.AddBckToQuery(nil, bck)
		path = cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name)
	)

	if msg == nil {
		msg = &cmn.SelectMsg{}
	}

	baseParams.Method = http.MethodPost
	b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActSummaryBucket, Value: msg})
	if err != nil {
		return nil, err
	}

	optParams := OptionalParams{
		Header: http.Header{"Content-Type": []string{"application/json"}},
		Query:  q,
	}

	resp, err := waitForAsyncReqComplete(baseParams, cmn.ActSummaryBucket, path, msg, optParams)
	if err != nil {
		return nil, err
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}

	var summaries cmn.BucketsSummaries
	if err = jsoniter.Unmarshal(respBody, &summaries); err != nil {
		return nil, fmt.Errorf("failed to json-unmarshal, err: %v [%s]", err, string(b))
	}
	return summaries, nil
}

// CreateBucket API
//
// CreateBucket sends a HTTP request to a proxy to create an ais bucket with the given name
func CreateBucket(baseParams BaseParams, bck cmn.Bck) error {
	var (
		msg, err = jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActCreateLB})
		path     = cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name)
		query    = cmn.AddBckToQuery(nil, bck)
	)
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPost
	_, err = DoHTTPRequest(baseParams, path, msg, OptionalParams{Query: query})
	return err
}

// DestroyBucket API
//
// DestroyBucket sends a HTTP request to a proxy to remove an ais bucket with the given name
func DestroyBucket(baseParams BaseParams, bck cmn.Bck) error {
	var (
		msg, err = jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActDestroyLB})
		path     = cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name)
		query    = cmn.AddBckToQuery(nil, bck)
	)
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodDelete
	_, err = DoHTTPRequest(baseParams, path, msg, OptionalParams{Query: query})
	return err
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
	b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActCopyBucket, Name: toBck.Name})
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Buckets, fromBck.Name)
	_, err = DoHTTPRequest(baseParams, path, b)
	return err
}

// RenameBucket API
//
// RenameBucket changes the name of a bucket from oldName to newBucketName
func RenameBucket(baseParams BaseParams, oldBck, newBck cmn.Bck) error {
	b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActRenameLB, Name: newBck.Name})
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Buckets, oldBck.Name)
	_, err = DoHTTPRequest(baseParams, path, b)
	return err
}

// DeleteList API
//
// DeleteList sends a HTTP request to remove a list of objects from a bucket
func DeleteList(baseParams BaseParams, bck cmn.Bck, fileslist []string) error {
	deleteMsg := cmn.ListMsg{Objnames: fileslist}
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
	prefetchMsg := cmn.ListMsg{Objnames: fileslist}
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
	evictMsg := cmn.ListMsg{Objnames: fileslist}
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
	var (
		b, err    = jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActEvictCB})
		optParams = OptionalParams{}
	)
	if err != nil {
		return err
	}
	if len(query) > 0 {
		optParams.Query = query[0]
	}
	if bck.Provider == "" {
		bck.Provider = cmn.Cloud
	}
	optParams.Query = cmn.AddBckToQuery(optParams.Query, bck)
	baseParams.Method = http.MethodDelete
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name)
	_, err = DoHTTPRequest(baseParams, path, b, optParams)
	return err
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
func waitForAsyncReqComplete(baseParams BaseParams, action, path string,
	origMsg *cmn.SelectMsg, optParams OptionalParams) (*http.Response, error) {
	var (
		resp         *http.Response
		sleep        = initialPollInterval
		b, err       = jsoniter.Marshal(cmn.ActionMsg{Action: action, Value: origMsg})
		changeOfTask bool
	)
	if err != nil {
		return nil, err
	}
	if optParams.Query == nil {
		optParams.Query = url.Values{}
	}
	if resp, err = doHTTPRequestGetResp(baseParams, path, b, optParams); err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("Invalid response code: %d", resp.StatusCode)
	}
	if resp.StatusCode == http.StatusAccepted {
		// receiver started async task and returned the task ID
		if b, err = handleAsyncReqAccepted(resp, action, origMsg, optParams); err != nil {
			return nil, err
		}
	}

	defer optParams.Query.Del(cmn.URLParamTaskID)

	// poll async task for http.StatusOK completion
	for {
		resp, err = doHTTPRequestGetResp(baseParams, path, b, optParams)
		if err != nil {
			return nil, err
		}
		if !changeOfTask && resp.StatusCode == http.StatusAccepted {
			// NOTE: async task changed on the fly
			if b, err = handleAsyncReqAccepted(resp, action, origMsg, optParams); err != nil {
				return nil, err
			}
			changeOfTask = true
		}
		if resp.StatusCode == http.StatusOK {
			break
		}
		resp.Body.Close()
		time.Sleep(sleep)
		if sleep < maxPollInterval {
			sleep += sleep / 2
		}
	}
	return resp, err
}

func handleAsyncReqAccepted(resp *http.Response, action string, origMsg *cmn.SelectMsg,
	optParams OptionalParams) (b []byte, err error) {
	var (
		id       string
		respBody []byte
	)
	respBody, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return
	}
	id = string(respBody)
	if origMsg != nil {
		msg := cmn.SelectMsg{}
		msg = *origMsg
		msg.TaskID = id
		if b, err = jsoniter.Marshal(cmn.ActionMsg{Action: action, Value: &msg}); err != nil {
			return
		}
	}
	optParams.Query.Set(cmn.URLParamTaskID, id)
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

		optParams := OptionalParams{
			Header: http.Header{"Content-Type": []string{"application/json"}},
			Query:  q,
		}

		resp, err := waitForAsyncReqComplete(baseParams, cmn.ActListObjects, path, msg, optParams)
		if err != nil {
			return nil, err
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

		if err = jsoniter.NewDecoder(resp.Body).Decode(&page); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("failed to json-unmarshal, err: %v", err)
		}
		resp.Body.Close()

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
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name)

	if msg == nil {
		msg = &cmn.SelectMsg{}
	}

	q := url.Values{}
	if len(query) > 0 {
		q = query[0]
	}

	q = cmn.AddBckToQuery(q, bck)
	optParams := OptionalParams{
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Query: q,
	}

	resp, err := waitForAsyncReqComplete(baseParams, cmn.ActListObjects, path, msg, optParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	page := &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, cmn.DefaultListPageSize)}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&page); err != nil {
		return nil, fmt.Errorf("failed to json-unmarshal, err: %v", err)
	}
	msg.PageMarker = page.PageMarker

	return page, nil
}

// ListBucketFast returns list of objects in a bucket.
// Build an object list with minimal set of properties: name and size.
// All SelectMsg fields except prefix do not work and are skipped.
// Function always returns the whole list of objects without paging
func ListBucketFast(baseParams BaseParams, bck cmn.Bck, msg *cmn.SelectMsg, query ...url.Values) (*cmn.BucketList, error) {
	var (
		path = cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name)
		q    url.Values
	)
	if msg == nil {
		msg = &cmn.SelectMsg{}
	}

	preallocSize := cmn.DefaultListPageSize
	if msg.PageSize != 0 {
		preallocSize = msg.PageSize
	}

	msg.Fast = true
	msg.Cached = true

	if len(query) > 0 {
		q = query[0]
	}
	q = cmn.AddBckToQuery(q, bck)

	optParams := OptionalParams{
		Header: http.Header{"Content-Type": []string{"application/json"}},
		Query:  q,
	}
	baseParams.Method = http.MethodPost
	resp, err := waitForAsyncReqComplete(baseParams, cmn.ActListObjects, path, msg, optParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	bckList := &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, preallocSize)}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&bckList); err != nil {
		return nil, fmt.Errorf("failed to json-unmarshal, err: %v", err)
	}

	return bckList, nil
}

// Handles the List/Range operations (delete, prefetch)
func doListRangeRequest(baseParams BaseParams, bck cmn.Bck, action, method string, listrangemsg interface{}) error {
	var (
		actionMsg = cmn.ActionMsg{Action: action, Value: listrangemsg}
		b, err    = jsoniter.Marshal(actionMsg)
		path      = cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name)
		query     = cmn.AddBckToQuery(nil, bck)
	)
	if err != nil {
		return fmt.Errorf("failed to marshal cmn.ActionMsg, err: %v", err)
	}
	optParams := OptionalParams{
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Query: query,
	}
	baseParams.Method = method
	_, err = DoHTTPRequest(baseParams, path, b, optParams)
	return err
}

func ECEncodeBucket(baseParams BaseParams, bck cmn.Bck) error {
	var (
		b, err = jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActECEncode})
		path   = cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name)
		query  = cmn.AddBckToQuery(nil, bck)
	)
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPost
	_, err = DoHTTPRequest(baseParams, path, b, OptionalParams{Query: query})
	return err
}
