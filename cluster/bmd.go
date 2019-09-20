// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
)

const (
	BisLocalBit = uint64(1 << 63)
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
	LBmap   map[string]*cmn.BucketProps `json:"l_bmap"`  // ais buckets and their props
	CBmap   map[string]*cmn.BucketProps `json:"c_bmap"`  // Cloud-based buckets and their AIStore-only metadata
	Version int64                       `json:"version"` // version - gets incremented on every update
}

func (m *BMD) GenBucketID(isais bool) uint64 {
	if !isais {
		return uint64(m.Version)
	}
	return uint64(m.Version) | BisLocalBit
}

func (m *BMD) Exists(b string, bckID uint64, isais bool) (exists bool) {
	if bckID == 0 {
		if isais {
			exists = m.IsAIS(b)
			// cmn.Assert(!exists)
			if exists {
				glog.Errorf("%s: ais bucket must have ID", m.Bstring(b, isais))
				exists = false
			}
		} else {
			exists = m.IsCloud(b)
		}
		return
	}
	if isais != (bckID&BisLocalBit != 0) {
		return
	}
	var (
		p  *cmn.BucketProps
		mm = m.LBmap
	)
	if !isais {
		mm = m.CBmap
	}
	p, exists = mm[b]
	if exists && p.BID != bckID {
		exists = false
	}
	return
}

func (m *BMD) IsAIS(bucket string) bool   { _, ok := m.LBmap[bucket]; return ok }
func (m *BMD) IsCloud(bucket string) bool { _, ok := m.CBmap[bucket]; return ok }

func (m *BMD) Bstring(b string, isais bool) string {
	var (
		s    = cmn.ProviderFromBool(isais)
		p, e = m.Get(b, isais)
	)
	if !e {
		return fmt.Sprintf("%s(unknown, %s)", b, s)
	}
	return fmt.Sprintf("%s(%x, %s)", b, p.BID, s)
}

func (m *BMD) Get(b string, isais bool) (p *cmn.BucketProps, present bool) {
	if isais {
		p, present = m.LBmap[b]
		return
	}
	p, present = m.CBmap[b]
	if !present {
		p = cmn.DefaultBucketProps(isais)
	}
	return
}

func (m *BMD) ValidateBucket(bucket, provider string) (isLocal bool, err error) {
	if err = cmn.ValidateBucketName(bucket); err != nil {
		return
	}
	normalizedProvider, err := cmn.ProviderFromStr(provider)
	if err != nil {
		return
	}
	var (
		config   = cmn.GCO.Get()
		bckIsAIS = m.IsAIS(bucket)
	)
	switch normalizedProvider {
	case cmn.AIS:
		if !bckIsAIS {
			return false, fmt.Errorf("ais bucket %q %s", bucket, cmn.DoesNotExist)
		}
		isLocal = true
	case cmn.Cloud:
		if provider != config.CloudProvider && provider != cmn.Cloud {
			err = fmt.Errorf("cluster cloud provider %q, mismatch bucket provider %q", config.CloudProvider, provider)
			return
		}
		isLocal = false
	default:
		isLocal = bckIsAIS
	}
	return
}
