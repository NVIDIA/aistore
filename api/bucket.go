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

// SetBucketPropsMsg API
//
// Set the properties of a bucket using the bucket name and the entire bucket property structure to be set.
// Validation of the properties passed in is performed by AIStore Proxy.
func SetBucketPropsMsg(baseParams *BaseParams, bucket string, props cmn.BucketProps, query ...url.Values) error {
	if props.Cksum.Type == "" {
		props.Cksum.Type = cmn.PropInherit
	}

	b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActSetProps, Value: props})
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

// SetBucketProps API
//
// Set properties of a bucket using the bucket name, and key value pairs.
// Validation of the properties passed in is performed by AIStore Proxy.
func SetBucketProps(baseParams *BaseParams, bucket string, nvs cmn.SimpleKVs, query ...url.Values) error {
	optParams := OptionalParams{}
	q := url.Values{}
	if len(query) > 0 {
		q = query[0]
	}

	for key, val := range nvs {
		q.Add(key, val)
	}

	optParams.Query = q
	baseParams.Method = http.MethodPut
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bucket, cmn.ActSetProps)
	_, err := DoHTTPRequest(baseParams, path, nil, optParams)
	return err
}

// ResetBucketProps API
//
// Reset the properties of a bucket, identified by its name, to the global configuration.
func ResetBucketProps(baseParams *BaseParams, bucket string, query ...url.Values) error {
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
func HeadBucket(baseParams *BaseParams, bucket string, query ...url.Values) (p *cmn.BucketProps, err error) {
	var (
		path      = cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
		optParams = OptionalParams{}
		r         *http.Response
		n         int64
		u, aattrs uint64
		b         bool
	)
	baseParams.Method = http.MethodHead
	if len(query) > 0 {
		optParams.Query = query[0]
	}
	if r, err = doHTTPRequestGetResp(baseParams, path, nil, optParams); err != nil {
		return
	}
	defer r.Body.Close()

	cksumProps := cmn.CksumConf{
		Type: r.Header.Get(cmn.HeaderBucketChecksumType),
	}
	if b, err = cmn.ParseBool(r.Header.Get(cmn.HeaderBucketValidateColdGet)); err == nil {
		cksumProps.ValidateColdGet = b
	} else {
		return
	}
	if b, err = cmn.ParseBool(r.Header.Get(cmn.HeaderBucketValidateWarmGet)); err == nil {
		cksumProps.ValidateWarmGet = b
	} else {
		return
	}
	if b, err = cmn.ParseBool(r.Header.Get(cmn.HeaderBucketValidateObjMove)); err == nil {
		cksumProps.ValidateObjMove = b
	} else {
		return
	}
	if b, err = cmn.ParseBool(r.Header.Get(cmn.HeaderBucketEnableReadRange)); err == nil {
		cksumProps.EnableReadRange = b
	} else {
		return
	}

	verProps := cmn.VersionConf{}
	if b, err = cmn.ParseBool(r.Header.Get(cmn.HeaderBucketVerEnabled)); err == nil {
		verProps.Enabled = b
	} else {
		return
	}
	if b, err = cmn.ParseBool(r.Header.Get(cmn.HeaderBucketVerValidateWarm)); err == nil {
		verProps.ValidateWarmGet = b
	} else {
		return
	}

	lruProps := cmn.LRUConf{
		DontEvictTimeStr:   r.Header.Get(cmn.HeaderBucketDontEvictTime),
		CapacityUpdTimeStr: r.Header.Get(cmn.HeaderBucketCapUpdTime),
	}
	if u, err = strconv.ParseUint(r.Header.Get(cmn.HeaderBucketLRULowWM), 10, 32); err == nil {
		lruProps.LowWM = int64(u)
	} else {
		return
	}
	if u, err = strconv.ParseUint(r.Header.Get(cmn.HeaderBucketLRUHighWM), 10, 32); err == nil {
		lruProps.HighWM = int64(u)
	} else {
		return
	}
	if b, err = cmn.ParseBool(r.Header.Get(cmn.HeaderBucketLRUEnabled)); err == nil {
		lruProps.Enabled = b
	} else {
		return
	}

	mirrorProps := cmn.MirrorConf{}
	if n, err = strconv.ParseInt(r.Header.Get(cmn.HeaderBucketCopies), 10, 32); err == nil {
		mirrorProps.Copies = n
	} else {
		return
	}
	if b, err = cmn.ParseBool(r.Header.Get(cmn.HeaderBucketMirrorEnabled)); err == nil {
		mirrorProps.Enabled = b
	} else {
		return
	}
	if n, err = strconv.ParseInt(r.Header.Get(cmn.HeaderBucketMirrorThresh), 10, 32); err == nil {
		mirrorProps.UtilThresh = n
	} else {
		return
	}

	ecProps := cmn.ECConf{}
	if b, err = cmn.ParseBool(r.Header.Get(cmn.HeaderBucketECEnabled)); err == nil {
		ecProps.Enabled = b
	} else {
		return
	}
	if n, err = strconv.ParseInt(r.Header.Get(cmn.HeaderBucketECMinSize), 10, 64); err == nil {
		ecProps.ObjSizeLimit = n
	} else {
		return
	}
	if n, err = strconv.ParseInt(r.Header.Get(cmn.HeaderBucketECData), 10, 32); err == nil {
		ecProps.DataSlices = int(n)
	}
	if n, err = strconv.ParseInt(r.Header.Get(cmn.HeaderBucketECParity), 10, 32); err == nil {
		ecProps.ParitySlices = int(n)
	} else {
		return
	}
	tierProps := cmn.TierConf{
		NextTierURL: r.Header.Get(cmn.HeaderNextTierURL),
		ReadPolicy:  r.Header.Get(cmn.HeaderReadPolicy),
		WritePolicy: r.Header.Get(cmn.HeaderWritePolicy),
	}
	if u, err = strconv.ParseUint(r.Header.Get(cmn.HeaderBucketAccessAttrs), 10, 64); err == nil {
		aattrs = u
	} else {
		return
	}
	p = &cmn.BucketProps{
		CloudProvider: r.Header.Get(cmn.HeaderCloudProvider),
		Versioning:    verProps,
		Tiering:       tierProps,
		Cksum:         cksumProps,
		LRU:           lruProps,
		Mirror:        mirrorProps,
		EC:            ecProps,
		AccessAttrs:   aattrs,
	}
	return
}

// GetBucketNames API
//
// bckProvider takes one of "" (empty), "cloud" or "local". If bckProvider is empty, return all bucketnames.
// Otherwise return "cloud" or "local" buckets.
func GetBucketNames(baseParams *BaseParams, bckProvider string) (*cmn.BucketNames, error) {
	bucketNames := &cmn.BucketNames{}
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Buckets, cmn.ListAll)
	query := url.Values{cmn.URLParamBckProvider: []string{bckProvider}}
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

// CreateLocalBucket API
//
// CreateLocalBucket sends a HTTP request to a proxy to create a local bucket with the given name
func CreateLocalBucket(baseParams *BaseParams, bucket string) error {
	msg, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActCreateLB})
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
	_, err = DoHTTPRequest(baseParams, path, msg)
	return err
}

// DestroyLocalBucket API
//
// DestroyLocalBucket sends a HTTP request to a proxy to remove a local bucket with the given name
func DestroyLocalBucket(baseParams *BaseParams, bucket string) error {
	b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActDestroyLB})
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodDelete
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
	_, err = DoHTTPRequest(baseParams, path, b)
	return err
}

// DoesLocalBucketExist API
//
// DoesLocalBucketExist queries a proxy or target to get a list of all local
// buckets, returns true if the bucket exists.
func DoesLocalBucketExist(baseParams *BaseParams, bucket string) (bool, error) {
	buckets, err := GetBucketNames(baseParams, cmn.LocalBs)
	if err != nil {
		return false, err
	}

	exists := cmn.StringInSlice(bucket, buckets.Local)
	return exists, nil
}

// CopyLocalBucket API
//
// CopyLocalBucket creates a new local bucket newName and
// copies into it contents of the existing oldName bucket
func CopyLocalBucket(baseParams *BaseParams, oldName, newName string) error {
	b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActCopyLB, Name: newName})
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Buckets, oldName)
	_, err = DoHTTPRequest(baseParams, path, b)
	return err
}

// RenameLocalBucket API
//
// RenameLocalBucket changes the name of a bucket from oldName to newBucketName
func RenameLocalBucket(baseParams *BaseParams, oldName, newName string) error {
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
func DeleteList(baseParams *BaseParams, bucket, bckProvider string, fileslist []string, wait bool, deadline time.Duration) error {
	listRangeMsgBase := cmn.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	deleteMsg := cmn.ListMsg{Objnames: fileslist, ListRangeMsgBase: listRangeMsgBase}
	return doListRangeRequest(baseParams, bucket, bckProvider, cmn.ActDelete, http.MethodDelete, deleteMsg)
}

// DeleteRange API
//
// DeleteRange sends a HTTP request to remove a range of objects from a bucket
func DeleteRange(baseParams *BaseParams, bucket, bckProvider, prefix, regex, rng string, wait bool, deadline time.Duration) error {
	listRangeMsgBase := cmn.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	deleteMsg := cmn.RangeMsg{Prefix: prefix, Regex: regex, Range: rng, ListRangeMsgBase: listRangeMsgBase}
	return doListRangeRequest(baseParams, bucket, bckProvider, cmn.ActDelete, http.MethodDelete, deleteMsg)
}

// PrefetchList API
//
// PrefetchList sends a HTTP request to prefetch a list of objects from a cloud bucket
func PrefetchList(baseParams *BaseParams, bucket, bckProvider string, fileslist []string, wait bool, deadline time.Duration) error {
	listRangeMsgBase := cmn.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	prefetchMsg := cmn.ListMsg{Objnames: fileslist, ListRangeMsgBase: listRangeMsgBase}
	return doListRangeRequest(baseParams, bucket, bckProvider, cmn.ActPrefetch, http.MethodPost, prefetchMsg)
}

// PrefetchRange API
//
// PrefetchRange sends a HTTP request to prefetch a range of objects from a cloud bucket
func PrefetchRange(baseParams *BaseParams, bucket, bckProvider, prefix, regex, rng string, wait bool, deadline time.Duration) error {
	prefetchMsgBase := cmn.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	prefetchMsg := cmn.RangeMsg{Prefix: prefix, Regex: regex, Range: rng, ListRangeMsgBase: prefetchMsgBase}
	return doListRangeRequest(baseParams, bucket, bckProvider, cmn.ActPrefetch, http.MethodPost, prefetchMsg)
}

// EvictList API
//
// EvictList sends a HTTP request to evict a list of objects from a cloud bucket
func EvictList(baseParams *BaseParams, bucket, bckProvider string, fileslist []string, wait bool, deadline time.Duration) error {
	listRangeMsgBase := cmn.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	evictMsg := cmn.ListMsg{Objnames: fileslist, ListRangeMsgBase: listRangeMsgBase}
	return doListRangeRequest(baseParams, bucket, bckProvider, cmn.ActEvictObjects, http.MethodDelete, evictMsg)
}

// EvictRange API
//
// EvictRange sends a HTTP request to evict a range of objects from a cloud bucket
func EvictRange(baseParams *BaseParams, bucket, bckProvider, prefix, regex, rng string, wait bool, deadline time.Duration) error {
	listRangeMsgBase := cmn.ListRangeMsgBase{Deadline: deadline, Wait: wait}
	evictMsg := cmn.RangeMsg{Prefix: prefix, Regex: regex, Range: rng, ListRangeMsgBase: listRangeMsgBase}
	return doListRangeRequest(baseParams, bucket, bckProvider, cmn.ActEvictObjects, http.MethodDelete, evictMsg)
}

// EvictCloudBucket API
//
// EvictCloudBucket sends a HTTP request to a proxy to evict an entire cloud bucket from the AIStore
// - the operation results in eliminating all traces of the specified cloud bucket in the AIStore
func EvictCloudBucket(baseParams *BaseParams, bucket string, query ...url.Values) error {
	b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActEvictCB})
	if err != nil {
		return err
	}
	optParams := OptionalParams{}
	if len(query) > 0 {
		optParams.Query = query[0]
	}
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
func waitForAsyncReqComplete(baseParams *BaseParams, path string, origMsg *cmn.SelectMsg, optParams OptionalParams) (*http.Response, error) {
	b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActListObjects, Value: origMsg})
	if err != nil {
		return nil, err
	}

	resp, err := doHTTPRequestGetResp(baseParams, path, b, optParams)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("Invalid response code: %d", resp.StatusCode)
	}

	var id int64
	// the destination started async task and returned its ID
	if resp.StatusCode == http.StatusAccepted {
		respBody, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		idstr := string(respBody)
		id, err = strconv.ParseInt(idstr, 10, 64)
		if err != nil {
			return nil, err
		}

		if origMsg != nil {
			msg := cmn.SelectMsg{}
			msg = *origMsg
			msg.TaskID = id
			b, err = jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActListObjects, Value: &msg})
			if err != nil {
				return nil, err
			}
		}
	}

	sleep := initialPollInterval
	optParams.Query.Set(cmn.URLParamTaskID, strconv.FormatInt(id, 10))
	defer optParams.Query.Del(cmn.URLParamTaskID)

	// poll the task status until it completes
	for resp.StatusCode != http.StatusOK {
		resp, err = doHTTPRequestGetResp(baseParams, path, b, optParams)
		if err != nil {
			return nil, err
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

	// now resp should contain the real data returned by the destination
	return resp, err
}

// ListBucket API
//
// ListBucket returns list of objects in a bucket. numObjects is the
// maximum number of objects returned by ListBucket (0 - return all objects in a bucket)
func ListBucket(baseParams *BaseParams, bucket string, msg *cmn.SelectMsg, numObjects int, query ...url.Values) (*cmn.BucketList, error) {
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
	reslist := &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, 1000)}
	q := url.Values{}
	if len(query) > 0 {
		q = query[0]
	}

	// An optimization to read as few objects from bucket as possible.
	// toRead is the current number of objects ListBucket must read before
	// returning the list. Every cycle the loop reads objects by pages and
	// decreases toRead by the number of received objects. When toRead gets less
	// than pageSize, the loop does the final request with reduced pageSize
	toRead := numObjects
	msg.TaskID = 0
	for {
		if toRead != 0 {
			if (msg.PageSize == 0 && toRead < cmn.DefaultPageSize) ||
				(msg.PageSize != 0 && msg.PageSize > toRead) {
				msg.PageSize = toRead
			}
		}

		b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActListObjects, Value: msg})
		if err != nil {
			return nil, err
		}

		optParams := OptionalParams{
			Header: http.Header{"Content-Type": []string{"application/json"}},
			Query:  q,
		}

		resp, err := waitForAsyncReqComplete(baseParams, path, msg, optParams)
		if err != nil {
			return nil, err
		}

		respBody, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}

		page := &cmn.BucketList{}
		page.Entries = make([]*cmn.BucketEntry, 0, 1000)

		if err = jsoniter.Unmarshal(respBody, page); err != nil {
			return nil, fmt.Errorf("failed to json-unmarshal, err: %v [%s]", err, string(b))
		}

		reslist.Entries = append(reslist.Entries, page.Entries...)
		if page.PageMarker == "" {
			msg.PageMarker = ""
			break
		}

		if numObjects != 0 {
			if len(reslist.Entries) >= numObjects {
				break
			}
			toRead -= len(page.Entries)
		}

		msg.PageMarker = page.PageMarker
	}

	return reslist, nil
}

// ListBucketPage returns the first page of bucket objects
// On success the function updates msg.PageMarker, so a client can reuse
// the message to fetch the next page
func ListBucketPage(baseParams *BaseParams, bucket string, msg *cmn.SelectMsg, query ...url.Values) (*cmn.BucketList, error) {
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
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

	resp, err := waitForAsyncReqComplete(baseParams, path, msg, optParams)
	if err != nil {
		return nil, err
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		return nil, err
	}

	reslist := &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, 1000)}
	if err = jsoniter.Unmarshal(respBody, reslist); err != nil {
		return nil, fmt.Errorf("failed to json-unmarshal, err: %v [%s]", err, string(respBody))
	}
	msg.PageMarker = reslist.PageMarker

	return reslist, nil
}

// ListBucketFast returns list of objects in a bucket.
// Build an object list with minimal set of properties: name and size.
// All SelectMsg fields except prefix do not work and are skipped.
// Function always returns the whole list of objects without paging
func ListBucketFast(baseParams *BaseParams, bucket string, msg *cmn.SelectMsg, query ...url.Values) (*cmn.BucketList, error) {
	var (
		b   []byte
		err error
	)
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bucket, cmn.ActListObjects)
	reslist := &cmn.BucketList{}
	if msg != nil {
		b, err = jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActListObjects, Value: msg})
		if err != nil {
			return nil, err
		}
	}
	q := url.Values{}
	if len(query) > 0 {
		q = query[0]
	}

	optParams := OptionalParams{
		Header: http.Header{"Content-Type": []string{"application/json"}},
		Query:  q,
	}
	resp, err := waitForAsyncReqComplete(baseParams, path, msg, optParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if err = jsoniter.Unmarshal(respBody, reslist); err != nil {
		return nil, fmt.Errorf("failed to json-unmarshal, err: %v [%s]", err, string(b))
	}

	return reslist, nil
}

// Handles the List/Range operations (delete, prefetch)
func doListRangeRequest(baseParams *BaseParams, bucket, bckProvider, action, method string, listrangemsg interface{}) error {
	actionMsg := cmn.ActionMsg{Action: action, Value: listrangemsg}
	b, err := jsoniter.Marshal(actionMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal cmn.ActionMsg, err: %v", err)
	}
	baseParams.Method = method
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
	query := url.Values{cmn.URLParamBckProvider: []string{bckProvider}}
	optParams := OptionalParams{
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Query: query,
	}
	_, err = DoHTTPRequest(baseParams, path, b, optParams)
	return err
}
