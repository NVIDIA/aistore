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
	"strconv"
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
func SetBucketProps(baseParams BaseParams, bucket string, props cmn.BucketPropsToUpdate, query ...url.Values) error {
	b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActSetProps, Value: props})
	if err != nil {
		return err
	}

	optParams := OptionalParams{}
	if len(query) > 0 {
		optParams.Query = query[0]
	}
	baseParams.Method = http.MethodPatch
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
	_, err = DoHTTPRequest(baseParams, path, b, optParams)
	return err
}

// ResetBucketProps API
//
// Reset the properties of a bucket, identified by its name, to the global configuration.
func ResetBucketProps(baseParams BaseParams, bucket string, query ...url.Values) error {
	b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActResetProps})
	if err != nil {
		return err
	}
	optParams := OptionalParams{}
	if len(query) > 0 {
		optParams.Query = query[0]
	}
	baseParams.Method = http.MethodPut
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
	_, err = DoHTTPRequest(baseParams, path, b, optParams)
	return err
}

// HeadBucket API
//
// Returns the properties of a bucket specified by its name.
// Converts the string type fields returned from the HEAD request to their
// corresponding counterparts in the BucketProps struct
func HeadBucket(baseParams BaseParams, bucket string, query ...url.Values) (p cmn.BucketProps, err error) {
	var (
		path      = cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
		optParams = OptionalParams{}
		r         *http.Response
	)
	baseParams.Method = http.MethodHead
	if len(query) > 0 {
		optParams.Query = query[0]
	}
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
// provider takes one of Cloud Provider enum names (see provider.go). If provider is empty, return all names.
// Otherwise, return cloud or ais bucket names.
func GetBucketNames(baseParams BaseParams, provider string) (*cmn.BucketNames, error) {
	bucketNames := &cmn.BucketNames{}
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Buckets, cmn.AllBuckets)
	query := url.Values{cmn.URLParamProvider: []string{provider}}
	optParams := OptionalParams{Query: query}

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
// Cloud provider takes one of "", "cloud", "ais", "gcp", "aws".
// Returns bucket summaries for the specified bucket provider (and all bucket summaries for unspecified ("") provider).
func GetBucketsSummaries(baseParams BaseParams, bucket, provider string,
	msg *cmn.SelectMsg) (summaries cmn.BucketsSummaries, err error) {
	var (
		q    = url.Values{}
		path = cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
	)

	if msg == nil {
		msg = &cmn.SelectMsg{}
	}

	baseParams.Method = http.MethodPost
	q.Add(cmn.URLParamProvider, provider)

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

	if err = jsoniter.Unmarshal(respBody, &summaries); err != nil {
		return nil, fmt.Errorf("failed to json-unmarshal, err: %v [%s]", err, string(b))
	}

	return summaries, nil
}

// CreateBucket API
//
// CreateBucket sends a HTTP request to a proxy to create an ais bucket with the given name
func CreateBucket(baseParams BaseParams, bucket string) error {
	msg, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActCreateLB})
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
	_, err = DoHTTPRequest(baseParams, path, msg)
	return err
}

// DestroyBucket API
//
// DestroyBucket sends a HTTP request to a proxy to remove an ais bucket with the given name
func DestroyBucket(baseParams BaseParams, bucket string) error {
	b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActDestroyLB})
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodDelete
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
	_, err = DoHTTPRequest(baseParams, path, b)
	return err
}

// DoesBucketExist API
//
// DoesBucketExist queries a proxy or target to get a list of all ais buckets,
// returns true if the bucket is present in the list.
func DoesBucketExist(baseParams BaseParams, bucket string) (bool, error) {
	buckets, err := GetBucketNames(baseParams, cmn.AIS)
	if err != nil {
		return false, err
	}

	exists := cmn.StringInSlice(bucket, buckets.AIS)
	return exists, nil
}

// CopyBucket API
//
// CopyBucket creates a new ais bucket newName and
// copies into it contents of the existing oldName bucket
func CopyBucket(baseParams BaseParams, oldName, newName string) error {
	b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActCopyBucket, Name: newName})
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Buckets, oldName)
	_, err = DoHTTPRequest(baseParams, path, b)
	return err
}

// RenameBucket API
//
// RenameBucket changes the name of a bucket from oldName to newBucketName
func RenameBucket(baseParams BaseParams, oldName, newName string) error {
	b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActRenameLB, Name: newName})
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Buckets, oldName)
	_, err = DoHTTPRequest(baseParams, path, b)
	return err
}

// DeleteList API
//
// DeleteList sends a HTTP request to remove a list of objects from a bucket
func DeleteList(baseParams BaseParams, bucket, provider string, fileslist []string, wait bool, deadline time.Duration) error {
	listRangeMsgBase := cmn.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	deleteMsg := cmn.ListMsg{Objnames: fileslist, ListRangeMsgBase: listRangeMsgBase}
	return doListRangeRequest(baseParams, bucket, provider, cmn.ActDelete, http.MethodDelete, deleteMsg)
}

// DeleteRange API
//
// DeleteRange sends a HTTP request to remove a range of objects from a bucket
func DeleteRange(baseParams BaseParams, bucket, provider, prefix, regex, rng string, wait bool, deadline time.Duration) error {
	listRangeMsgBase := cmn.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	deleteMsg := cmn.RangeMsg{Prefix: prefix, Regex: regex, Range: rng, ListRangeMsgBase: listRangeMsgBase}
	return doListRangeRequest(baseParams, bucket, provider, cmn.ActDelete, http.MethodDelete, deleteMsg)
}

// PrefetchList API
//
// PrefetchList sends a HTTP request to prefetch a list of objects from a cloud bucket
func PrefetchList(baseParams BaseParams, bucket, provider string, fileslist []string, wait bool, deadline time.Duration) error {
	listRangeMsgBase := cmn.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	prefetchMsg := cmn.ListMsg{Objnames: fileslist, ListRangeMsgBase: listRangeMsgBase}
	return doListRangeRequest(baseParams, bucket, provider, cmn.ActPrefetch, http.MethodPost, prefetchMsg)
}

// PrefetchRange API
//
// PrefetchRange sends a HTTP request to prefetch a range of objects from a cloud bucket
func PrefetchRange(baseParams BaseParams, bucket, provider, prefix, regex, rng string, wait bool, deadline time.Duration) error {
	prefetchMsgBase := cmn.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	prefetchMsg := cmn.RangeMsg{Prefix: prefix, Regex: regex, Range: rng, ListRangeMsgBase: prefetchMsgBase}
	return doListRangeRequest(baseParams, bucket, provider, cmn.ActPrefetch, http.MethodPost, prefetchMsg)
}

// EvictList API
//
// EvictList sends a HTTP request to evict a list of objects from a cloud bucket
func EvictList(baseParams BaseParams, bucket, provider string, fileslist []string, wait bool, deadline time.Duration) error {
	listRangeMsgBase := cmn.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	evictMsg := cmn.ListMsg{Objnames: fileslist, ListRangeMsgBase: listRangeMsgBase}
	return doListRangeRequest(baseParams, bucket, provider, cmn.ActEvictObjects, http.MethodDelete, evictMsg)
}

// EvictRange API
//
// EvictRange sends a HTTP request to evict a range of objects from a cloud bucket
func EvictRange(baseParams BaseParams, bucket, provider, prefix, regex, rng string, wait bool, deadline time.Duration) error {
	listRangeMsgBase := cmn.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	evictMsg := cmn.RangeMsg{Prefix: prefix, Regex: regex, Range: rng, ListRangeMsgBase: listRangeMsgBase}
	return doListRangeRequest(baseParams, bucket, provider, cmn.ActEvictObjects, http.MethodDelete, evictMsg)
}

// EvictCloudBucket API
//
// EvictCloudBucket sends a HTTP request to a proxy to evict an entire cloud bucket from the AIStore
// - the operation results in eliminating all traces of the specified cloud bucket in the AIStore
func EvictCloudBucket(baseParams BaseParams, bucket string, query ...url.Values) error {
	b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActEvictCB})
	if err != nil {
		return err
	}
	optParams := OptionalParams{}
	if len(query) > 0 {
		optParams.Query = query[0]
	} else {
		optParams.Query = make(url.Values)
	}
	optParams.Query.Add(cmn.URLParamProvider, cmn.Cloud)

	baseParams.Method = http.MethodDelete
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
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
		id       int64
		respBody []byte
	)
	respBody, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return
	}
	if id, err = strconv.ParseInt(string(respBody), 10, 64); err != nil {
		return
	}
	if origMsg != nil {
		msg := cmn.SelectMsg{}
		msg = *origMsg
		msg.TaskID = id
		if b, err = jsoniter.Marshal(cmn.ActionMsg{Action: action, Value: &msg}); err != nil {
			return
		}
	}
	optParams.Query.Set(cmn.URLParamTaskID, strconv.FormatInt(id, 10))
	return
}

// ListBucket API
//
// ListBucket returns list of objects in a bucket. numObjects is the
// maximum number of objects returned by ListBucket (0 - return all objects in a bucket)
func ListBucket(baseParams BaseParams, bucket string, msg *cmn.SelectMsg, numObjects int, query ...url.Values) (*cmn.BucketList, error) {
	baseParams.Method = http.MethodPost
	var (
		q       = url.Values{}
		path    = cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
		bckList = &cmn.BucketList{
			Entries: make([]*cmn.BucketEntry, 0, cmn.DefaultListPageSize),
		}

		// Temporary page for intermediate results
		tmpPage = &cmn.BucketList{}
	)

	if len(query) > 0 {
		q = query[0]
	}

	if msg == nil {
		msg = &cmn.SelectMsg{}
	}

	// An optimization to read as few objects from bucket as possible.
	// `toRead` is the current number of objects `ListBucket` must read before
	// returning the list. Every cycle the loop reads objects by pages and
	// decreases `toRead` by the number of received objects. When `toRead` gets less
	// than `pageSize`, the loop does the final request with reduced `pageSize`.
	toRead := numObjects
	msg.TaskID = 0

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
func ListBucketPage(baseParams BaseParams, bucket string, msg *cmn.SelectMsg, query ...url.Values) (*cmn.BucketList, error) {
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bucket)

	if msg == nil {
		msg = &cmn.SelectMsg{}
	}

	q := url.Values{}
	if len(query) > 0 {
		q = query[0]
	}

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
func ListBucketFast(baseParams BaseParams, bucket string, msg *cmn.SelectMsg, query ...url.Values) (*cmn.BucketList, error) {
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bucket)

	if msg == nil {
		msg = &cmn.SelectMsg{}
	}

	preallocSize := cmn.DefaultListPageSize
	if msg.PageSize != 0 {
		preallocSize = msg.PageSize
	}

	msg.Fast = true
	msg.Cached = true

	q := url.Values{}
	if len(query) > 0 {
		q = query[0]
	}

	optParams := OptionalParams{
		Header: http.Header{"Content-Type": []string{"application/json"}},
		Query:  q,
	}
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
func doListRangeRequest(baseParams BaseParams, bucket, provider, action, method string, listrangemsg interface{}) error {
	actionMsg := cmn.ActionMsg{Action: action, Value: listrangemsg}
	b, err := jsoniter.Marshal(actionMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal cmn.ActionMsg, err: %v", err)
	}
	baseParams.Method = method
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
	query := url.Values{cmn.URLParamProvider: []string{provider}}
	optParams := OptionalParams{
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Query: query,
	}
	_, err = DoHTTPRequest(baseParams, path, b, optParams)
	return err
}

func ECEncodeBucket(baseParams BaseParams, bucket string) error {
	b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActECEncode})
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
	_, err = DoHTTPRequest(baseParams, path, b)
	return err
}
