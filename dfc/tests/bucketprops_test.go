/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

import (
	"testing"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/tutils"
)

func testBucketProps(t *testing.T) *cmn.BucketProps {
	proxyURL := getPrimaryURL(t, proxyURLRO)
	globalConfig := getConfig(proxyURL+cmn.URLPath(cmn.Version, cmn.Daemon), t)
	lruConfig := globalConfig["lru_config"].(map[string]interface{})

	LRUConf := cmn.LRUConfig{
		LowWM:              uint32(lruConfig["lowwm"].(float64)),
		HighWM:             uint32(lruConfig["highwm"].(float64)),
		AtimeCacheMax:      uint64(lruConfig["atime_cache_max"].(float64)),
		DontEvictTimeStr:   lruConfig["dont_evict_time"].(string),
		CapacityUpdTimeStr: lruConfig["capacity_upd_time"].(string),
		LRUEnabled:         lruConfig["lru_enabled"].(bool),
	}
	return &cmn.BucketProps{
		CksumConfig: cmn.CksumConfig{Checksum: cmn.ChecksumInherit},
		LRUConfig:   LRUConf,
	}
}

func TestResetBucketProps(t *testing.T) {
	var (
		proxyURL     = getPrimaryURL(t, proxyURLRO)
		globalProps  cmn.BucketProps
		globalConfig = getConfig(proxyURL+cmn.URLPath(cmn.Version, cmn.Daemon), t)
		cksumConfig  = globalConfig["cksum_config"].(map[string]interface{})
	)

	createFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer destroyLocalBucket(t, proxyURL, TestLocalBucketName)

	bucketProps := defaultBucketProps()
	bucketProps.Checksum = cmn.ChecksumNone
	bucketProps.ValidateWarmGet = true
	bucketProps.EnableReadRangeChecksum = true

	globalProps.CloudProvider = cmn.ProviderDFC
	globalProps.Checksum = cksumConfig["checksum"].(string)
	globalProps.ValidateColdGet = cksumConfig["validate_checksum_cold_get"].(bool)
	globalProps.ValidateWarmGet = cksumConfig["validate_checksum_warm_get"].(bool)
	globalProps.EnableReadRangeChecksum = cksumConfig["enable_read_range_checksum"].(bool)
	globalProps.LRUConfig = testBucketProps(t).LRUConfig

	err := api.SetBucketProps(tutils.HTTPClient, proxyURL, TestLocalBucketName, bucketProps)
	tutils.CheckFatal(err, t)

	p, err := api.HeadBucket(tutils.HTTPClient, proxyURL, TestLocalBucketName)
	tutils.CheckFatal(err, t)

	// check that bucket props do get set
	validateBucketProps(t, bucketProps, *p)
	err = api.ResetBucketProps(tutils.HTTPClient, proxyURL, TestLocalBucketName)
	tutils.CheckFatal(err, t)

	p, err = api.HeadBucket(tutils.HTTPClient, proxyURL, TestLocalBucketName)
	tutils.CheckFatal(err, t)

	// check that bucket props are reset
	validateBucketProps(t, globalProps, *p)
}

func TestSetBucketNextTierURLInvalid(t *testing.T) {
	var (
		proxyURL          = getPrimaryURL(t, proxyURLRO)
		bucketProps       cmn.BucketProps
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
			proxyInfo.PublicNet.DirectURL,
			proxyInfo.IntraControlNet.DirectURL,
			proxyInfo.IntraDataNet.DirectURL,
		)
		// Break early to avoid flooding the logs with too many error messages.
		break
	}

	// Test Invalid Target URLs for NextTierURL property
	for _, targetInfo := range smap.Tmap {
		invalidDaemonURLs = append(invalidDaemonURLs,
			targetInfo.PublicNet.DirectURL,
			targetInfo.IntraControlNet.DirectURL,
			targetInfo.IntraDataNet.DirectURL,
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
