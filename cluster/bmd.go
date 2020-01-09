// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"
	"strconv"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
)

const (
	BisLocalBit = uint64(1 << 63)
)

// interface to Get current BMD instance
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
	LBmap   map[string]*cmn.BucketProps `json:"l_bmap"`         // ais buckets and their props
	CBmap   map[string]*cmn.BucketProps `json:"c_bmap"`         // Cloud-based buckets and their AIStore-only metadata
	Version int64                       `json:"version,string"` // version - gets incremented on every update
	Origin  uint64                      `json:"origin,string"`  // (unique) origin stays the same for the lifetime
}

func (m *BMD) String() string {
	if m == nil {
		return "BMD <nil>"
	}
	return "BMD v" + strconv.FormatInt(m.Version, 10)
}

func (m *BMD) StringEx() string {
	if m == nil {
		return "BMD <nil>"
	}
	return fmt.Sprintf("BMD v%d[...%d, ais=%d, cloud=%d]", m.Version, m.Origin%1000, len(m.LBmap), len(m.CBmap))
}

func (m *BMD) GenBucketID(isais bool) uint64 {
	if !isais {
		return uint64(m.Version)
	}
	return uint64(m.Version) | BisLocalBit
}

func (m *BMD) Exists(bck *Bck, bckID uint64) (exists bool) {
	if bckID == 0 {
		if bck.IsAIS() {
			exists = m.IsAIS(bck.Name)
			if exists {
				glog.Errorf("%s: ais bucket must have ID", m.Bstring(bck))
				exists = false
			}
		} else {
			exists = m.IsCloud(bck.Name)
		}
		return
	}
	if bck.IsAIS() != (bckID&BisLocalBit != 0) {
		return
	}
	var (
		p  *cmn.BucketProps
		mm = m.LBmap
	)
	if !bck.IsAIS() {
		mm = m.CBmap
	}
	p, exists = mm[bck.Name]
	if exists && p.BID != bckID {
		exists = false
	}
	return
}

func (m *BMD) IsAIS(bucket string) bool   { _, ok := m.LBmap[bucket]; return ok }
func (m *BMD) IsCloud(bucket string) bool { _, ok := m.CBmap[bucket]; return ok }

func (m *BMD) Bstring(bck *Bck) string {
	_, e := m.Get(bck)
	if !e {
		return fmt.Sprintf("%s(not exists)", bck)
	}
	return fmt.Sprintf("%s(exists)", bck)
}

func (m *BMD) Get(bck *Bck) (p *cmn.BucketProps, present bool) {
	if bck.IsAIS() {
		p, present = m.LBmap[bck.Name]
		return
	}
	p, present = m.CBmap[bck.Name]
	if !present {
		p = cmn.DefaultBucketProps()
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

func (m *BMD) IsECUsed() bool {
	for _, bck := range m.LBmap {
		if bck.EC.Enabled {
			return true
		}
	}
	for _, bck := range m.CBmap {
		if bck.EC.Enabled {
			return true
		}
	}

	return false
}
