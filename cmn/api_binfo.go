// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

// bucket information: _presence_ (see QparamFltPresence), size, and - in the future -
// usage, capacity and other TBD statistics.
// - compare with BucketSummary;
// - is obtained via HEAD(bucket), where: apc.HdrBucketProps => BucketProps and
// apc.HdrBucketInfo => BucketInfo.
type BucketInfo struct {
	Present bool `json:"present_in_cluster,omitempty"`
}
