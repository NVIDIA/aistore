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

func (m *BMD) ValidateBucket(bucket, bckProvider string) (isLocal bool, err error) {
	if err = cmn.ValidateBucketName(bucket); err != nil {
		return
	}
	normalizedBckProvider, err := cmn.ProviderFromStr(bckProvider)
	if err != nil {
		return
	}
	var (
		config   = cmn.GCO.Get()
		bckIsAIS = m.IsAIS(bucket)
	)
	switch normalizedBckProvider {
	case cmn.AIS:
		if !bckIsAIS {
			return false, fmt.Errorf("ais bucket %q %s", bucket, cmn.DoesNotExist)
		}
		isLocal = true
	case cmn.Cloud:
		if bckProvider != config.CloudProvider && bckProvider != cmn.Cloud {
			err = fmt.Errorf("cluster cloud provider %q, mismatch bucket provider %q", config.CloudProvider, bckProvider)
			return
		}
		isLocal = false
	default:
		isLocal = bckIsAIS
	}
	return
}

//
// access perms
//

func (m *BMD) AllowGET(b string, isais bool, bprops ...*cmn.BucketProps) error {
	return m.allow(b, bprops, "GET", cmn.AccessGET, isais)
}
func (m *BMD) AllowHEAD(b string, isais bool, bprops ...*cmn.BucketProps) error {
	return m.allow(b, bprops, "HEAD", cmn.AccessHEAD, isais)
}
func (m *BMD) AllowPUT(b string, isais bool, bprops ...*cmn.BucketProps) error {
	return m.allow(b, bprops, "PUT", cmn.AccessPUT, isais)
}
func (m *BMD) AllowColdGET(b string, isais bool, bprops ...*cmn.BucketProps) error {
	return m.allow(b, bprops, "cold-GET", cmn.AccessColdGET, isais)
}
func (m *BMD) AllowDELETE(b string, isais bool, bprops ...*cmn.BucketProps) error {
	return m.allow(b, bprops, "DELETE", cmn.AccessDELETE, isais)
}
func (m *BMD) AllowRENAME(b string, isais bool, bprops ...*cmn.BucketProps) error {
	return m.allow(b, bprops, "RENAME", cmn.AccessRENAME, isais)
}

func (m *BMD) allow(b string, bprops []*cmn.BucketProps, oper string, bits uint64, isais bool) (err error) {
	var p *cmn.BucketProps
	if len(bprops) > 0 {
		p = bprops[0]
	} else {
		p, _ = m.Get(b, isais)
		if p == nil { // handle non-existence elsewhere
			return
		}
	}
	if p.AccessAttrs == cmn.AllowAnyAccess {
		return
	}
	if (p.AccessAttrs & bits) != 0 {
		return
	}
	err = cmn.NewBucketAccessDenied(m.Bstring(b, isais), oper, p.AccessAttrs)
	return
}
