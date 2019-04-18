// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"
	"regexp"

	"github.com/NVIDIA/aistore/cmn"
)

const (
	BisLocalBit   = uint64(1 << 63)
	bucketNameErr = "must contain lowercase letters, numbers, dashes (-), underscores (_), and dots (.)"
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

func (m *BMD) GenBucketID(local bool) uint64 {
	if !local {
		return uint64(m.Version)
	}
	return uint64(m.Version) | BisLocalBit
}

func (m *BMD) Exists(b string, bckID uint64, local bool) (exists bool) {
	if bckID == 0 {
		if local {
			exists = m.IsLocal(b)
			cmn.Assert(!exists) // local bucket must have unique ID
		} else {
			exists = m.IsCloud(b)
		}
		return
	}
	if local != (bckID&BisLocalBit != 0) {
		return
	}
	var (
		p  *cmn.BucketProps
		mm = m.LBmap
	)
	if !local {
		mm = m.CBmap
	}
	p, exists = mm[b]
	if exists && p.BID != bckID {
		exists = false
	}
	return
}

func (m *BMD) IsLocal(bucket string) bool { _, ok := m.LBmap[bucket]; return ok }
func (m *BMD) IsCloud(bucket string) bool { _, ok := m.CBmap[bucket]; return ok }

func (m *BMD) Bstring(b string, local bool) string {
	var (
		s    = cmn.BckProviderFromLocal(local)
		p, e = m.Get(b, local)
	)
	if !e {
		return fmt.Sprintf("%s(unknown, %s)", b, s)
	}
	return fmt.Sprintf("%s(%x, %s)", b, p.BID, s)
}

func (m *BMD) Get(b string, local bool) (p *cmn.BucketProps, present bool) {
	if local {
		p, present = m.LBmap[b]
		return
	}
	p, present = m.CBmap[b]
	if !present {
		p = cmn.DefaultBucketProps()
	}
	return
}

func (m *BMD) ValidateBucket(bucket, bckProvider string) (isLocal bool, err error) {
	if !validateBucketName(bucket) {
		err = fmt.Errorf("bucket name %s is invalid (%s)", bucket, bucketNameErr)
		return
	}
	config := cmn.GCO.Get()

	normalizedBckProvider, err := cmn.BckProviderFromStr(bckProvider)
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
		if !cmn.IsValidCloudProvider(bckProvider, config.CloudProvider) {
			err = fmt.Errorf("cluster cloud provider %q, mis-match bucket provider %q", config.CloudProvider, bckProvider)
			return
		}
		isLocal = false
	default:
		isLocal = bckIsLocal
	}
	return
}

func validateBucketName(bucket string) bool {
	if bucket == "" {
		return false
	}
	reg := regexp.MustCompile(`^[\.a-zA-Z0-9_-]*$`)
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
