/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// FIXME -- FIXME -- FIXME: temporary, in-transit
package api

import (
	"time"
)

const (
	RWPolicyCloud    = "cloud"
	RWPolicyNextTier = "next_tier"
)

type Cksumconfig struct {
	Checksum                string `json:"checksum"`                   // DFC checksum: xxhash:none
	ValidateColdGet         bool   `json:"validate_checksum_cold_get"` // MD5 (ETag) validation upon cold GET
	ValidateWarmGet         bool   `json:"validate_checksum_warm_get"` // MD5 (ETag) validation upon warm GET
	EnableReadRangeChecksum bool   `json:"enable_read_range_checksum"` // Return read range checksum otherwise return entire object checksum
}

type Lruconfig struct {
	LowWM              uint32        `json:"lowwm"`             // capacity usage low watermark
	HighWM             uint32        `json:"highwm"`            // capacity usage high watermark
	AtimeCacheMax      uint64        `json:"atime_cache_max"`   // atime cache - max num entries
	DontEvictTimeStr   string        `json:"dont_evict_time"`   // eviction is not permitted during [atime, atime + dont]
	CapacityUpdTimeStr string        `json:"capacity_upd_time"` // min time to update capacity
	DontEvictTime      time.Duration `json:"-"`                 // omitempty
	CapacityUpdTime    time.Duration `json:"-"`                 // ditto
	LRUEnabled         bool          `json:"lru_enabled"`       // LRU will only run when LRUEnabled is true
}

type BucketProps struct {
	CloudProvider string      `json:"cloud_provider,omitempty"`
	NextTierURL   string      `json:"next_tier_url,omitempty"`
	ReadPolicy    string      `json:"read_policy,omitempty"`
	WritePolicy   string      `json:"write_policy,omitempty"`
	CksumConf     Cksumconfig `json:"cksum_config"`
	LRUProps      Lruconfig   `json:"lru_props"`
}
