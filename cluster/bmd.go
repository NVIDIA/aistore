// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/NVIDIA/aistore/cmn"
)

var (
	// Translates the various query values for URLParamBckProvider for cluster use
	bckProviderMap = map[string]string{
		// Cloud values
		cmn.CloudBs:        cmn.CloudBs,
		cmn.ProviderAmazon: cmn.CloudBs,
		cmn.ProviderGoogle: cmn.CloudBs,

		// Local values
		cmn.LocalBs:     cmn.LocalBs,
		cmn.ProviderAIS: cmn.LocalBs,

		// unset
		"": "",
	}
)

// interface to Get current bucket-metadata instance
// (for implementation, see ais/bucketmeta.go)
type Bowner interface {
	Get() (bmd *BMD)
}

// - BMD represents buckets (that store objects) and associated metadata
// - BMD (instance) can be obtained via Bowner.Get()
// - BMD is immutable and versioned
// - BMD versioning is monotonic and incremental
// Note: Getting a cloud object does not add the cloud bucket to CBmap
type BMD struct {
	LBmap   map[string]*cmn.BucketProps `json:"l_bmap"`  // local cache-only buckets and their props
	CBmap   map[string]*cmn.BucketProps `json:"c_bmap"`  // Cloud-based buckets and their AIStore-only metadata
	Version int64                       `json:"version"` // version - gets incremented on every update
}

func (m *BMD) IsLocal(bucket string) bool {
	_, ok := m.LBmap[bucket]
	return ok
}

func (m *BMD) Get(b string, local bool) (*cmn.BucketProps, bool) {
	mm := m.LBmap
	if !local {
		mm = m.CBmap
	}
	if p, ok := mm[b]; ok {
		return p, true
	}
	return cmn.DefaultBucketProps(), false
}

func (m *BMD) ValidateBucket(bucket, bckProvider string) (isLocal bool, err error) {
	if !validateBucketName(bucket) {
		err = fmt.Errorf("invalid bucket names - it must contain only lowercase letters, numbers, dashes (-), underscores (_), and dots (.)")
		return
	}
	config := cmn.GCO.Get()

	normalizedBckProvider, err := TranslateBckProvider(bckProvider)
	if err != nil {
		return false, err
	}

	bckIsLocal := m.IsLocal(bucket)
	switch normalizedBckProvider {
	case cmn.LocalBs:
		// Check if local bucket does exist
		if !bckIsLocal {
			return false, fmt.Errorf("local bucket %q %s", bucket, cmn.DoesNotExist)
		}
		isLocal = true
	case cmn.CloudBs:
		// Check if user does have the associated cloud
		if !isValidCloudProvider(bckProvider, config.CloudProvider) {
			err = fmt.Errorf("cluster cloud provider %q, mis-match bucket provider %q", config.CloudProvider, bckProvider)
			return
		}
		isLocal = false
	default:
		isLocal = bckIsLocal
	}
	return
}

func TranslateBckProvider(bckProvider string) (string, error) {
	bckProvider = strings.ToLower(bckProvider)
	val, ok := bckProviderMap[bckProvider]
	if !ok {
		return "", fmt.Errorf("invalid value %q for %q", bckProvider, cmn.URLParamBckProvider)
	}
	return val, nil
}

func isValidCloudProvider(bckProvider, cloudProvider string) bool {
	return bckProvider == cloudProvider || bckProvider == cmn.CloudBs
}

func validateBucketName(bucket string) bool {
	if bucket == "" {
		return false
	}
	reg, err := regexp.Compile(`^[\.a-zA-Z0-9_-]*$`)
	cmn.AssertNoErr(err)
	if !reg.MatchString(bucket) {
		return false
	}
	// Reject bucket name containing only dots
	for _, c := range bucket {
		if c != '.' {
			return true
		}
	}
	return false
}
