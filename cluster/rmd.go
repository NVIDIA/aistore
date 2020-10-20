// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

type (
	// RMD aka "rebalance metadata" is used to distribute information
	// for the next rebalance.
	RMD struct {
		TargetIDs []string `json:"target_ids,omitempty"`
		Version   int64    `json:"version"`
		Resilver  string   `json:"resilver,omitempty"`
	}
)
