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
	"time"

	"github.com/NVIDIA/dfcpub/common"
)

const (
	RWPolicyCloud    = "cloud"
	RWPolicyNextTier = "next_tier"
)

type CksumConfig struct {

	// Checksum: hashing algorithm used to check for object corruption
	// Values: none, xxhash, md5, inherit
	// Value of 'none' disables hash checking
	Checksum string `json:"checksum"`

	// ValidateColdGet determines whether or not the checksum of received object
	// is checked after downloading it from the cloud or next tier
	ValidateColdGet bool `json:"validate_checksum_cold_get"`

	// ValidateWarmGet: if enabled, the object's version (if in Cloud-based bucket)
	// and checksum are checked. If either value fail to match, the object
	// is removed from local storage
	ValidateWarmGet bool `json:"validate_checksum_warm_get"`

	// EnableReadRangeChecksum: Return read range checksum otherwise return entire object checksum
	EnableReadRangeChecksum bool `json:"enable_read_range_checksum"`
}

type LRUConfig struct {

	// LowWM: Self-throttling mechanisms are suspended if disk utilization is below LowWM
	LowWM uint32 `json:"lowwm"`

	// HighWM: Self-throttling mechanisms are fully engaged if disk utilization is above HighWM
	HighWM uint32 `json:"highwm"`

	// AtimeCacheMax represents the maximum number of entries
	AtimeCacheMax uint64 `json:"atime_cache_max"`

	// DontEvictTimeStr denotes the period of time during which eviction of an object
	// is forbidden [atime, atime + DontEvictTime]
	DontEvictTimeStr string `json:"dont_evict_time"`

	// DontEvictTime is the parsed value of DontEvictTimeStr
	DontEvictTime time.Duration `json:"-"`

	// CapacityUpdTimeStr denotes the frequency with which DFC updates filesystem usage
	CapacityUpdTimeStr string `json:"capacity_upd_time"`

	// CapacityUpdTime is the parsed value of CapacityUpdTimeStr
	CapacityUpdTime time.Duration `json:"-"`

	// LRUEnabled: LRU will only run when set to true
	LRUEnabled bool `json:"lru_enabled"`
}

// BucketProps defines the configuration of the bucket with regard to
// its type, checksum, and LRU. These characteristics determine its behaviour
// in response to operations on the bucket itself or the objects inside the bucket.
type BucketProps struct {

	// CloudProvider can be "aws", "gcp", or "dfc".
	// If a bucket is local, CloudProvider must be "dfc".
	// Otherwise, it must be "aws" or "gcp".
	CloudProvider string `json:"cloud_provider,omitempty"`

	// Versioning defines what kind of buckets should use versioning to
	// detect if the object must be redownloaded.
	// Values: "all", "cloud", "local" or "none".
	Versioning string

	// NextTierURL is an absolute URI corresponding to the primary proxy
	// of the next tier configured for the bucket specified
	NextTierURL string `json:"next_tier_url,omitempty"`

	// ReadPolicy determines if a read will be from cloud or next tier
	// specified by NextTierURL. Default: "next_tier"
	ReadPolicy string `json:"read_policy,omitempty"`

	// WritePolicy determines if a write will be to cloud or next tier
	// specified by NextTierURL. Default: "cloud"
	WritePolicy string `json:"write_policy,omitempty"`

	// CksumConfig is the embedded struct of the same name
	CksumConfig `json:"cksum_config"`

	// LRUConfig is the embedded struct of the same name
	LRUConfig `json:"lru_props"`
}

func NewBucketProps(lruconf *LRUConfig) *BucketProps {
	return &BucketProps{
		CksumConfig: CksumConfig{Checksum: ChecksumInherit},
		LRUConfig:   *lruconf,
	}
}

// SetBucketProps API operation for DFC
//
// Set the properties of a bucket, using the bucket name and the bucket properties to be set.
// Validation of the properties passed in is performed by DFC Proxy.
func SetBucketProps(httpClient *http.Client, proxyURL, bucket string, props BucketProps) error {
	url := proxyURL + common.URLPath(Version, Buckets, bucket)
	if props.Checksum == "" {
		props.Checksum = ChecksumInherit
	}

	b, err := json.Marshal(ActionMsg{Action: ActSetProps, Value: props})
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
	url := proxyURL + common.URLPath(Version, Buckets, bucket)
	b, err := json.Marshal(ActionMsg{Action: ActResetProps})
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
func HeadBucket(httpClient *http.Client, proxyURL, bucket string) (*BucketProps, error) {
	r, err := httpClient.Head(proxyURL + common.URLPath(Version, Buckets, bucket))
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

	cksumconf := CksumConfig{
		Checksum: r.Header.Get(HeaderBucketChecksumType),
	}
	if b, err := strconv.ParseBool(r.Header.Get(HeaderBucketValidateColdGet)); err == nil {
		cksumconf.ValidateColdGet = b
	}
	if b, err := strconv.ParseBool(r.Header.Get(HeaderBucketValidateWarmGet)); err == nil {
		cksumconf.ValidateWarmGet = b
	}
	if b, err := strconv.ParseBool(r.Header.Get(HeaderBucketValidateRange)); err == nil {
		cksumconf.EnableReadRangeChecksum = b
	}

	lruprops := LRUConfig{
		DontEvictTimeStr:   r.Header.Get(HeaderBucketDontEvictTime),
		CapacityUpdTimeStr: r.Header.Get(HeaderBucketCapUpdTime),
	}

	if b, err := strconv.ParseUint(r.Header.Get(HeaderBucketLRULowWM), 10, 32); err == nil {
		lruprops.LowWM = uint32(b)
	}
	if b, err := strconv.ParseUint(r.Header.Get(HeaderBucketLRUHighWM), 10, 32); err == nil {
		lruprops.HighWM = uint32(b)
	}

	if b, err := strconv.ParseUint(r.Header.Get(HeaderBucketAtimeCacheMax), 10, 32); err == nil {
		lruprops.AtimeCacheMax = b
	}

	if b, err := strconv.ParseBool(r.Header.Get(HeaderBucketLRUEnabled)); err == nil {
		lruprops.LRUEnabled = b
	}

	return &BucketProps{
		CloudProvider: r.Header.Get(HeaderCloudProvider),
		Versioning:    r.Header.Get(HeaderVersioning),
		NextTierURL:   r.Header.Get(HeaderNextTierURL),
		ReadPolicy:    r.Header.Get(HeaderReadPolicy),
		WritePolicy:   r.Header.Get(HeaderWritePolicy),
		CksumConfig:   cksumconf,
		LRUConfig:     lruprops,
	}, nil
}

// GetBucketNames API operation for DFC
//
// If localOnly is false, returns two lists, one for local buckets and one for cloud buckets.
// Otherwise, i.e. localOnly is true, still returns two lists, but the one for cloud buckets is empty
func GetBucketNames(httpClient *http.Client, proxyURL string, localOnly bool) (*BucketNames, error) {
	var bucketNames BucketNames
	url := proxyURL + common.URLPath(Version, Buckets, "*") +
		fmt.Sprintf("?%s=%t", URLParamLocal, localOnly)
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
	msg, err := json.Marshal(ActionMsg{Action: ActCreateLB})
	if err != nil {
		return err
	}
	url := proxyURL + common.URLPath(Version, Buckets, bucket)
	_, err = doHTTPRequest(httpClient, http.MethodPost, url, msg)
	return err
}

// DestroyLocalBucket API operation for DFC
//
// DestroyLocalBucket sends a HTTP request to a proxy to remove a local bucket with the given name
func DestroyLocalBucket(httpClient *http.Client, proxyURL, bucket string) error {
	b, err := json.Marshal(ActionMsg{Action: ActDestroyLB})
	if err != nil {
		return err
	}

	url := proxyURL + common.URLPath(Version, Buckets, bucket)
	_, err = doHTTPRequest(httpClient, http.MethodDelete, url, b)
	return err
}
