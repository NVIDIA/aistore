/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package cluster provides local access to cluster-level metadata
package cluster

import "github.com/NVIDIA/dfcpub/cmn"

// - BMD represents buckets (that store objects) and associated metadata
// - BMD (instance) can be obtained via Bowner.Get()
// - BMD is immutable and versioned
// - BMD versioning is monotonic and incremental
type BMD struct {
	LBmap   map[string]cmn.BucketProps `json:"l_bmap"`  // local cache-only buckets and their props
	CBmap   map[string]cmn.BucketProps `json:"c_bmap"`  // Cloud-based buckets and their DFC-only metadata
	Version int64                         `json:"version"` // version - gets incremented on every update
}

// interface to Get current bucket-metadata instance
// (for implementation, see dfc/bucketmeta.go)
type Bowner interface {
	Get() (bucketmd *BMD)
}
