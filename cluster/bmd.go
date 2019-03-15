// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/cmn"
)

const (
	maxBucketNameLen = 200
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
	if bucket == "*" { // Get bucket names
		return false, nil
	}
	if bucket == "" {
		err = fmt.Errorf("invalid bucket name %q - bucket name is empty", bucket)
		return
	}
	if strings.Contains(bucket, string(filepath.Separator)) {
		err = fmt.Errorf("invalid bucket name %q - contains '/'", bucket)
		return
	}
	if strings.Contains(bucket, " ") {
		err = fmt.Errorf("invalid bucket name %q - contains space", bucket)
		return
	}
	if len(bucket) > maxBucketNameLen {
		err = fmt.Errorf("invalid bucket name %q - length exceeds %d", bucket, maxBucketNameLen)
		return
	}

	bckProvider = strings.ToLower(bckProvider)
	config := cmn.GCO.Get()
	val, ok := bckProviderMap[bckProvider]
	if !ok {
		err = fmt.Errorf("invalid value %q for %q", bckProvider, cmn.URLParamBckProvider)
		return
	}

	bckIsLocal := m.IsLocal(bucket)
	switch val {
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

func isValidCloudProvider(bckProvider, cloudProvider string) bool {
	return bckProvider == cloudProvider || bckProvider == cmn.CloudBs
}
