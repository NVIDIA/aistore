// Package apc: API control messages and constants
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package apc

type (
	// IndexShardMsg is the control message for ActIndexShard xaction.
	IndexShardMsg struct {
		Prefix     string `json:"prefix,omitempty"`      // only index shards whose name begins with Prefix
		NumWorkers int    `json:"num-workers,omitempty"` // number of concurrent workers; (-1) none; (0) auto-computed (media type + load)
		SkipVerify bool   `json:"skip-verify,omitempty"` // if shard already has an index, trust it without loading+verifying staleness (fast re-run)
	}
	// ShardSummMsg is the control message for ActSummaryShard.
	ShardSummMsg struct {
		UUID   string `json:"uuid,omitempty"`   // server-assigned on the first response; client echoes it back
		Prefix string `json:"prefix,omitempty"` // only include TAR objects whose name begins with Prefix
	}
	// ShardSummResult is the per-bucket local TAR/index coverage summary.
	ShardSummResult struct {
		TarObjs        uint64 `json:"tar-objs,string"`        // local TAR objects found
		TarSize        uint64 `json:"tar-size,string"`        // total size of local TAR objects
		Shards         uint64 `json:"shards,string"`          // local TAR objects with a valid shard index
		ShardSize      uint64 `json:"shard-size,string"`      // total size of valid indexed shards
		ArchivedObjs   uint64 `json:"archived-objs,string"`   // total archived objects across valid indexed shards
		StaleIndexes   uint64 `json:"stale-indexes,string"`   // # TAR objects whose shard index is stale
		InvalidIndexes uint64 `json:"invalid-indexes,string"` // # TAR objects whose shard index failed to load
	}
)

func (r *ShardSummResult) IsEmpty() bool {
	return r.TarObjs == 0
}

func (r *ShardSummResult) Aggregate(from ShardSummResult) {
	r.TarObjs += from.TarObjs
	r.TarSize += from.TarSize
	r.Shards += from.Shards
	r.ShardSize += from.ShardSize
	r.ArchivedObjs += from.ArchivedObjs
	r.StaleIndexes += from.StaleIndexes
	r.InvalidIndexes += from.InvalidIndexes
}
