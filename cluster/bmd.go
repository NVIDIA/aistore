// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import "github.com/NVIDIA/dfcpub/cmn"

// - BMD represents buckets (that store objects) and associated metadata
// - BMD (instance) can be obtained via Bowner.Get()
// - BMD is immutable and versioned
// - BMD versioning is monotonic and incremental
type BMD struct {
	LBmap   map[string]*cmn.BucketProps `json:"l_bmap"`  // local cache-only buckets and their props
	CBmap   map[string]*cmn.BucketProps `json:"c_bmap"`  // Cloud-based buckets and their DFC-only metadata
	Version int64                       `json:"version"` // version - gets incremented on every update
}

func (m *BMD) IsLocal(bucket string) bool {
	_, ok := m.LBmap[bucket]
	return ok
}

// interface to Get current bucket-metadata instance
// (for implementation, see dfc/bucketmeta.go)
type Bowner interface {
	Get() (bucketmd *BMD)
}
