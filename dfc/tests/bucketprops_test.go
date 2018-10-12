/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

import (
	"testing"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/common"
	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
)

func TestResetBucketProps(t *testing.T) {
	const (
		nextTierURL                        = "http://foo.com"
		validateColdGetTestSetting         = false
		validateWarmGetTestSetting         = true
		enableReadRangeChecksumTestSetting = true

		lowWMTestSetting           = (uint32)(10)
		highWMTestSetting          = (uint32)(50)
		atimeCacheMaxTestSetting   = (uint64)(9999)
		dontEvictTimeTestSetting   = "1m"
		capacityUpdTimeTestSetting = "2m"
		lruEnabledTestSetting      = false
	)
	var (
		proxyURL     = getPrimaryURL(t, proxyURLRO)
		bucketProps  dfc.BucketProps
		globalProps  dfc.BucketProps
		globalConfig = getConfig(proxyURL+common.URLPath(api.Version, api.Daemon), httpclient, t)
		cksumConfig  = globalConfig["cksum_config"].(map[string]interface{})
		lruConfig    = globalConfig["lru_config"].(map[string]interface{})
	)

	createFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer destroyLocalBucket(t, proxyURL, TestLocalBucketName)

	bucketProps.CloudProvider = api.ProviderDFC
	bucketProps.NextTierURL = nextTierURL
	bucketProps.ReadPolicy = dfc.RWPolicyNextTier
	bucketProps.WritePolicy = dfc.RWPolicyNextTier
	bucketProps.CksumConf.Checksum = api.ChecksumNone
	bucketProps.CksumConf.ValidateColdGet = validateColdGetTestSetting
	bucketProps.CksumConf.ValidateWarmGet = validateWarmGetTestSetting
	bucketProps.CksumConf.EnableReadRangeChecksum = enableReadRangeChecksumTestSetting
	bucketProps.LRUProps.LowWM = lowWMTestSetting
	bucketProps.LRUProps.HighWM = highWMTestSetting
	bucketProps.LRUProps.AtimeCacheMax = atimeCacheMaxTestSetting
	bucketProps.LRUProps.DontEvictTimeStr = dontEvictTimeTestSetting
	bucketProps.LRUProps.CapacityUpdTimeStr = capacityUpdTimeTestSetting
	bucketProps.LRUProps.LRUEnabled = lruEnabledTestSetting

	globalProps.CloudProvider = api.ProviderDFC
	globalProps.CksumConf.Checksum = cksumConfig["checksum"].(string)
	globalProps.CksumConf.ValidateColdGet = cksumConfig["validate_checksum_cold_get"].(bool)
	globalProps.CksumConf.ValidateWarmGet = cksumConfig["validate_checksum_warm_get"].(bool)
	globalProps.CksumConf.EnableReadRangeChecksum = cksumConfig["enable_read_range_checksum"].(bool)
	globalProps.LRUProps.LowWM = uint32(lruConfig["lowwm"].(float64))
	globalProps.LRUProps.HighWM = uint32(lruConfig["highwm"].(float64))
	globalProps.LRUProps.AtimeCacheMax = uint64(lruConfig["atime_cache_max"].(float64))
	globalProps.LRUProps.DontEvictTimeStr = lruConfig["dont_evict_time"].(string)
	globalProps.LRUProps.CapacityUpdTimeStr = lruConfig["capacity_upd_time"].(string)
	globalProps.LRUProps.LRUEnabled = lruConfig["lru_enabled"].(bool)

	err := client.SetBucketProps(proxyURL, TestLocalBucketName, bucketProps)
	checkFatal(err, t)

	p, err := client.HeadBucket(proxyURL, TestLocalBucketName)
	checkFatal(err, t)

	// check that bucket props do get set
	validateBucketProps(t, bucketProps, *p)

	err = client.ResetBucketProps(proxyURL, TestLocalBucketName)
	checkFatal(err, t)

	p, err = client.HeadBucket(proxyURL, TestLocalBucketName)
	checkFatal(err, t)

	// check that bucket props are reset
	validateBucketProps(t, globalProps, *p)
}
