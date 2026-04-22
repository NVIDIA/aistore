// Package apc: API control messages and constants
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package apc

// IndexShardMsg is the control message for ActIndexShard xaction.
type IndexShardMsg struct {
	Prefix     string `json:"prefix,omitempty"`      // only index shards whose name begins with Prefix
	NumWorkers int    `json:"num-workers,omitempty"` // number of concurrent workers; (-1) none; (0) auto-computed (media type + load)
	SkipVerify bool   `json:"skip-verify,omitempty"` // if shard already has an index, trust it without loading+verifying staleness (fast re-run)
}
