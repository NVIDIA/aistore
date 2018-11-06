/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/NVIDIA/dfcpub/cmn"
)

// SetBucketProps API operation for DFC
//
// Set the properties of a bucket, using the bucket name and the bucket properties to be set.
// Validation of the properties passed in is performed by DFC Proxy.
func SetBucketProps(httpClient *http.Client, proxyURL, bucket string, props cmn.BucketProps) error {
	url := proxyURL + cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
	if props.Checksum == "" {
		props.Checksum = cmn.ChecksumInherit
	}

	b, err := json.Marshal(cmn.ActionMsg{Action: cmn.ActSetProps, Value: props})
	if err != nil {
		return err
	}

	_, err = doHTTPRequest(httpClient, http.MethodPut, url, b)
	return err
}

// ResetBucketProps API operation for DFC
//
// Reset the properties of a bucket, identified by its name, to the global configuration.
func ResetBucketProps(httpClient *http.Client, proxyURL, bucket string) error {
	url := proxyURL + cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
	b, err := json.Marshal(cmn.ActionMsg{Action: cmn.ActResetProps})
	if err != nil {
		return err
	}

	_, err = doHTTPRequest(httpClient, http.MethodPut, url, b)
	return err
}

// HeadBucket API operation for DFC
//
// Returns the properties of a bucket specified by its name.
// Converts the string type fields returned from the HEAD request to their
// corresponding counterparts in the BucketProps struct
func HeadBucket(httpClient *http.Client, proxyURL, bucket string) (*cmn.BucketProps, error) {
	r, err := httpClient.Head(proxyURL + cmn.URLPath(cmn.Version, cmn.Buckets, bucket))
	if err != nil {
		return nil, err
	}

	defer r.Body.Close()

	if r.StatusCode >= http.StatusBadRequest {
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, fmt.Errorf(
				"Failed to read response, err: %v", err)
		}
		return nil, fmt.Errorf("HEAD bucket: %s failed, HTTP status code: %d, HTTP response body: %s",
			bucket, r.StatusCode, string(b))
	}

	cksumconf := cmn.CksumConfig{
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

	lruprops := cmn.LRUConfig{
		DontEvictTimeStr:   r.Header.Get(cmn.HeaderBucketDontEvictTime),
		CapacityUpdTimeStr: r.Header.Get(cmn.HeaderBucketCapUpdTime),
	}

	if b, err := strconv.ParseUint(r.Header.Get(cmn.HeaderBucketLRULowWM), 10, 32); err == nil {
		lruprops.LowWM = uint32(b)
	}
	if b, err := strconv.ParseUint(r.Header.Get(cmn.HeaderBucketLRUHighWM), 10, 32); err == nil {
		lruprops.HighWM = uint32(b)
	}

	if b, err := strconv.ParseUint(r.Header.Get(cmn.HeaderBucketAtimeCacheMax), 10, 32); err == nil {
		lruprops.AtimeCacheMax = b
	}

	if b, err := strconv.ParseBool(r.Header.Get(cmn.HeaderBucketLRUEnabled)); err == nil {
		lruprops.LRUEnabled = b
	}

	return &cmn.BucketProps{
		CloudProvider: r.Header.Get(cmn.HeaderCloudProvider),
		Versioning:    r.Header.Get(cmn.HeaderVersioning),
		NextTierURL:   r.Header.Get(cmn.HeaderNextTierURL),
		ReadPolicy:    r.Header.Get(cmn.HeaderReadPolicy),
		WritePolicy:   r.Header.Get(cmn.HeaderWritePolicy),
		CksumConfig:   cksumconf,
		LRUConfig:     lruprops,
	}, nil
}

// GetBucketNames API operation for DFC
//
// If localOnly is false, returns two lists, one for local buckets and one for cloud buckets.
// Otherwise, i.e. localOnly is true, still returns two lists, but the one for cloud buckets is empty
func GetBucketNames(httpClient *http.Client, proxyURL string, localOnly bool) (*cmn.BucketNames, error) {
	var bucketNames cmn.BucketNames
	url := proxyURL + cmn.URLPath(cmn.Version, cmn.Buckets, "*") +
		fmt.Sprintf("?%s=%t", cmn.URLParamLocal, localOnly)
	b, err := doHTTPRequest(httpClient, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if len(b) != 0 {
		err = json.Unmarshal(b, &bucketNames)
		if err != nil {
			return nil, fmt.Errorf("Failed to unmarshal bucket names, err: %v - [%s]", err, string(b))
		}
	} else {
		return nil, fmt.Errorf("Empty response instead of empty bucket list from %s", proxyURL)
	}
	return &bucketNames, nil
}

// CreateLocalBucket API operation for DFC
//
// CreateLocalBucket sends a HTTP request to a proxy to create a local bucket with the given name
func CreateLocalBucket(httpClient *http.Client, proxyURL, bucket string) error {
	msg, err := json.Marshal(cmn.ActionMsg{Action: cmn.ActCreateLB})
	if err != nil {
		return err
	}
	url := proxyURL + cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
	_, err = doHTTPRequest(httpClient, http.MethodPost, url, msg)
	return err
}

// DestroyLocalBucket API operation for DFC
//
// DestroyLocalBucket sends a HTTP request to a proxy to remove a local bucket with the given name
func DestroyLocalBucket(httpClient *http.Client, proxyURL, bucket string) error {
	b, err := json.Marshal(cmn.ActionMsg{Action: cmn.ActDestroyLB})
	if err != nil {
		return err
	}

	url := proxyURL + cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
	_, err = doHTTPRequest(httpClient, http.MethodDelete, url, b)
	return err
}

// RenameLocalBucket API operation for DFC
//
// RenameLocalBucket changes the name of a bucket from oldBucketName to newBucketName
func RenameLocalBucket(httpClient *http.Client, proxyURL, oldBucketName, newBucketName string) error {
	b, err := json.Marshal(cmn.ActionMsg{Action: cmn.ActRenameLB, Name: newBucketName})
	if err != nil {
		return err
	}
	url := proxyURL + cmn.URLPath(cmn.Version, cmn.Buckets, oldBucketName)
	_, err = doHTTPRequest(httpClient, http.MethodPost, url, b)
	return err
}
