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

	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
)

// SetBucketProps API
//
// Set the properties of a bucket, using the bucket name and the bucket properties to be set.
// Validation of the properties passed in is performed by AIStore Proxy.
func SetBucketProps(baseParams *BaseParams, bucket string, props cmn.BucketProps, query ...url.Values) error {
	querystr := ""
	if props.Checksum == "" {
		props.Checksum = cmn.ChecksumInherit
	}

	b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActSetProps, Value: props})
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPut
	if len(query) > 0 {
		querystr = "?" + query[0].Encode()
	}
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bucket) + querystr
	_, err = DoHTTPRequest(baseParams, path, b)
	return err
}

// SetBucketProp API
//
// Set a single propertie of a bucket, using the bucket name, and the bucket property's name and value to be set.
// The function converts a property to a string to simplify route selection(it is
// either single string value or entire bucket property structure) and to avoid
// a lot of type reflexlection checks.
// Validation of the properties passed in is performed by AIStore Proxy.
func SetBucketProp(baseParams *BaseParams, bucket, prop string, value interface{}, query ...url.Values) error {
	strValue := fmt.Sprintf("%v", value)
	querystr := ""
	b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActSetProps, Name: prop, Value: strValue})
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPut
	if len(query) > 0 {
		querystr = "?" + query[0].Encode()
	}
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bucket) + querystr
	_, err = DoHTTPRequest(baseParams, path, b)
	return err
}

// ResetBucketProps API
//
// Reset the properties of a bucket, identified by its name, to the global configuration.
func ResetBucketProps(baseParams *BaseParams, bucket string, query ...url.Values) error {
	querystr := ""
	b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActResetProps})
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPut

	if len(query) > 0 {
		querystr = "?" + query[0].Encode()
	}
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bucket) + querystr
	_, err = DoHTTPRequest(baseParams, path, b)
	return err
}

// HeadBucket API
//
// Returns the properties of a bucket specified by its name.
// Converts the string type fields returned from the HEAD request to their
// corresponding counterparts in the BucketProps struct
func HeadBucket(baseParams *BaseParams, bucket string, query ...url.Values) (*cmn.BucketProps, error) {
	var querystr = ""
	if len(query) > 0 {
		querystr = "?" + query[0].Encode()
	}

	r, err := baseParams.Client.Head(baseParams.URL + cmn.URLPath(cmn.Version, cmn.Buckets, bucket) + querystr)

	if err != nil {
		return nil, err
	}

	defer r.Body.Close()

	if r.StatusCode >= http.StatusBadRequest {
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to read response, err: %v", err)
		}
		return nil, fmt.Errorf("HEAD bucket: %s failed, HTTP status code: %d, HTTP response body: %s",
			bucket, r.StatusCode, string(b))
	}

	cksumconf := cmn.CksumConf{
		Checksum: r.Header.Get(cmn.HeaderBucketChecksumType),
	}
	if b, err := strconv.ParseBool(r.Header.Get(cmn.HeaderBucketValidateColdGet)); err == nil {
		cksumconf.ValidateColdGet = b
	}
	if b, err := strconv.ParseBool(r.Header.Get(cmn.HeaderBucketValidateWarmGet)); err == nil {
		cksumconf.ValidateWarmGet = b
	}
	if b, err := strconv.ParseBool(r.Header.Get(cmn.HeaderBucketValidateRange)); err == nil {
		cksumconf.EnableReadRangeChecksum = b
	}

	lruprops := cmn.LRUConf{
		DontEvictTimeStr:   r.Header.Get(cmn.HeaderBucketDontEvictTime),
		CapacityUpdTimeStr: r.Header.Get(cmn.HeaderBucketCapUpdTime),
	}

	if b, err := strconv.ParseUint(r.Header.Get(cmn.HeaderBucketLRULowWM), 10, 32); err == nil {
		lruprops.LowWM = int64(b)
	}
	if b, err := strconv.ParseUint(r.Header.Get(cmn.HeaderBucketLRUHighWM), 10, 32); err == nil {
		lruprops.HighWM = int64(b)
	}

	if b, err := strconv.ParseInt(r.Header.Get(cmn.HeaderBucketAtimeCacheMax), 10, 32); err == nil {
		lruprops.AtimeCacheMax = b
	}

	if b, err := strconv.ParseBool(r.Header.Get(cmn.HeaderBucketLRUEnabled)); err == nil {
		lruprops.LRUEnabled = b
	}

	mirror := cmn.MirrorConf{}
	if b, err := strconv.ParseInt(r.Header.Get(cmn.HeaderBucketCopies), 10, 32); err == nil {
		mirror.Copies = b
	}
	if b, err := strconv.ParseBool(r.Header.Get(cmn.HeaderBucketMirrorEnabled)); err == nil {
		mirror.MirrorEnabled = b
	}
	if n, err := strconv.ParseInt(r.Header.Get(cmn.HeaderBucketMirrorThresh), 10, 32); err == nil {
		mirror.MirrorUtilThresh = n
	}

	ecprops := cmn.ECConf{}
	if b, err := strconv.ParseBool(r.Header.Get(cmn.HeaderBucketECEnabled)); err == nil {
		ecprops.ECEnabled = b
	}
	if n, err := strconv.ParseInt(r.Header.Get(cmn.HeaderBucketECMinSize), 10, 64); err == nil {
		ecprops.ECObjSizeLimit = n
	}
	if n, err := strconv.ParseInt(r.Header.Get(cmn.HeaderBucketECData), 10, 32); err == nil {
		ecprops.DataSlices = int(n)
	}
	if n, err := strconv.ParseInt(r.Header.Get(cmn.HeaderBucketECParity), 10, 32); err == nil {
		ecprops.ParitySlices = int(n)
	}

	return &cmn.BucketProps{
		CloudProvider: r.Header.Get(cmn.HeaderCloudProvider),
		Versioning:    r.Header.Get(cmn.HeaderVersioning),
		NextTierURL:   r.Header.Get(cmn.HeaderNextTierURL),
		ReadPolicy:    r.Header.Get(cmn.HeaderReadPolicy),
		WritePolicy:   r.Header.Get(cmn.HeaderWritePolicy),
		CksumConf:     cksumconf,
		LRUConf:       lruprops,
		MirrorConf:    mirror,
		ECConf:        ecprops,
	}, nil
}

// GetBucketNames API
//
// bucketProvider takes one of "" (empty), "cloud" or "local". If bucketProvider is empty, return all bucketnames.
// Otherwise return "cloud" or "local" buckets.
func GetBucketNames(baseParams *BaseParams, bucketProvider string) (*cmn.BucketNames, error) {
	var bucketNames cmn.BucketNames
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Buckets, "*") +
		fmt.Sprintf("?%s=%s", cmn.URLParamBucketProvider, bucketProvider)
	b, err := DoHTTPRequest(baseParams, path, nil)
	if err != nil {
		return nil, err
	}
	if len(b) != 0 {
		err = jsoniter.Unmarshal(b, &bucketNames)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal bucket names, err: %v - [%s]", err, string(b))
		}
	} else {
		return nil, fmt.Errorf("empty response instead of empty bucket list from %s", baseParams.URL)
	}
	return &bucketNames, nil
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

// EvictCloudBucket API
//
// EvictCloudBucket sends a HTTP request to a proxy to evict a cloud bucket with the given name
func EvictCloudBucket(baseParams *BaseParams, bucket string, query ...url.Values) error {
	var querystr string
	if len(query) > 0 {
		querystr = "?" + query[0].Encode()
	}
	b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActEvictCB})
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodDelete
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bucket) + querystr
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

// ListBucket API
//
// ListBucket returns list of objects in a bucket. numObjects is the
// maximum number of objects returned by ListBucket (0 - return all objects in a bucket)
func ListBucket(baseParams *BaseParams, bucket string, msg *cmn.GetMsg, numObjects int, query ...url.Values) (*cmn.BucketList, error) {
	var querystr = ""
	baseParams.Method = http.MethodPost
	if len(query) > 0 {
		querystr = "?" + query[0].Encode()
	}
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bucket) + querystr
	reslist := &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, 1000)}

	// An optimization to read as few objects from bucket as possible.
	// toRead is the current number of objects ListBucket must read before
	// returning the list. Every cycle the loop reads objects by pages and
	// decreases toRead by the number of received objects. When toRead gets less
	// than pageSize, the loop does the final request with reduced pageSize
	toRead := numObjects
	for {
		if toRead != 0 {
			if (msg.GetPageSize == 0 && toRead < cmn.DefaultPageSize) ||
				(msg.GetPageSize != 0 && msg.GetPageSize > toRead) {
				msg.GetPageSize = toRead
			}
		}

		b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActListObjects, Value: msg})
		if err != nil {
			return nil, err
		}

		optParams := OptionalParams{Header: http.Header{
			"Content-Type": []string{"application/json"},
		}}
		respBody, err := DoHTTPRequest(baseParams, path, b, optParams)
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
			break
		}

		if numObjects != 0 {
			if len(reslist.Entries) >= numObjects {
				break
			}
			toRead -= len(page.Entries)
		}

		msg.GetPageMarker = page.PageMarker
	}

	return reslist, nil
}

// EraseCopies API
//
// EraseCopies starts an extended action (xaction) to reduce redundancy of a given bucket to 1 (single copy)
func EraseCopies(baseParams *BaseParams, bucket string) error {
	b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActEraseCopies})
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
	_, err = DoHTTPRequest(baseParams, path, b)
	return err
}
