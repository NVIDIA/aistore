/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

import (
	"testing"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/common"
	"github.com/NVIDIA/dfcpub/tutils"
)

func TestResetBucketProps(t *testing.T) {
	var (
		proxyURL     = getPrimaryURL(t, proxyURLRO)
		globalProps  api.BucketProps
		globalConfig = getConfig(proxyURL+common.URLPath(api.Version, api.Daemon), t)
		cksumConfig  = globalConfig["cksum_config"].(map[string]interface{})
		lruConfig    = globalConfig["lru_config"].(map[string]interface{})
	)

	createFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer destroyLocalBucket(t, proxyURL, TestLocalBucketName)

	bucketProps := defaultBucketProps()
	bucketProps.Checksum = api.ChecksumNone
	bucketProps.ValidateWarmGet = true
	bucketProps.EnableReadRangeChecksum = true

	globalProps.CloudProvider = api.ProviderDFC
	globalProps.Checksum = cksumConfig["checksum"].(string)
	globalProps.ValidateColdGet = cksumConfig["validate_checksum_cold_get"].(bool)
	globalProps.ValidateWarmGet = cksumConfig["validate_checksum_warm_get"].(bool)
	globalProps.EnableReadRangeChecksum = cksumConfig["enable_read_range_checksum"].(bool)
	globalProps.LowWM = uint32(lruConfig["lowwm"].(float64))
	globalProps.HighWM = uint32(lruConfig["highwm"].(float64))
	globalProps.AtimeCacheMax = uint64(lruConfig["atime_cache_max"].(float64))
	globalProps.DontEvictTimeStr = lruConfig["dont_evict_time"].(string)
	globalProps.CapacityUpdTimeStr = lruConfig["capacity_upd_time"].(string)
	globalProps.LRUEnabled = lruConfig["lru_enabled"].(bool)
	err := api.SetBucketProps(tutils.HTTPClient, proxyURL, TestLocalBucketName, bucketProps)
	tutils.CheckFatal(err, t)

	p, err := api.HeadBucket(tutils.HTTPClient, proxyURL, TestLocalBucketName)
	tutils.CheckFatal(err, t)

	// check that bucket props do get set
	validateBucketProps(t, bucketProps, *p)
	err = tutils.ResetBucketProps(proxyURL, TestLocalBucketName)
	tutils.CheckFatal(err, t)

	p, err = api.HeadBucket(tutils.HTTPClient, proxyURL, TestLocalBucketName)
	tutils.CheckFatal(err, t)

	// check that bucket props are reset
	validateBucketProps(t, globalProps, *p)
}

func TestSetBucketNextTierURLInvalid(t *testing.T) {
	var (
		proxyURL          = getPrimaryURL(t, proxyURLRO)
		bucketProps       api.BucketProps
		invalidDaemonURLs []string
	)

	createFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer destroyLocalBucket(t, proxyURL, TestLocalBucketName)

	smap, err := tutils.GetClusterMap(proxyURL)
	tutils.CheckFatal(err, t)

	if len(smap.Tmap) < 1 || len(smap.Pmap) < 1 {
		t.Fatal("This test requires there to be at least one target and one proxy in the current cluster")
	}

	// Test Invalid Proxy URLs for NextTierURL property
	for _, proxyInfo := range smap.Pmap {
		invalidDaemonURLs = append(invalidDaemonURLs,
			proxyInfo.InternalNet.DirectURL,
			proxyInfo.PublicNet.DirectURL,
			proxyInfo.ReplNet.DirectURL,
		)
		// Break early to avoid flooding the logs with too many error messages.
		break
	}

	// Test Invalid Target URLs for NextTierURL property
	for _, targetInfo := range smap.Tmap {
		invalidDaemonURLs = append(invalidDaemonURLs,
			targetInfo.InternalNet.DirectURL,
			targetInfo.PublicNet.DirectURL,
			targetInfo.ReplNet.DirectURL,
		)
		// Break early to avoid flooding the logs with too many error messages.
		break
	}

	for _, url := range invalidDaemonURLs {
		bucketProps.NextTierURL = url
		if err = api.SetBucketProps(tutils.HTTPClient, proxyURL, TestLocalBucketName, bucketProps); err == nil {
			t.Fatalf("Setting the bucket's nextTierURL to daemon %q should fail, it is in the current cluster.", url)
		}
	}
}
