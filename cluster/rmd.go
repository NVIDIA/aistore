// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

type (
	// RMD aka "rebalance metadata" is used to distribute information
	// for the next rebalance.
	RMD struct {
		Ext       any      `json:"ext,omitempty"` // within meta-version extensions
		Resilver  string   `json:"resilver,omitempty"`
		TargetIDs []string `json:"target_ids,omitempty"`
		Version   int64    `json:"version"`
	}
)
