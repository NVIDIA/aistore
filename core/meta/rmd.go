// Package meta: cluster-level metadata
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package meta

type (
	// Rebalance MetaData
	RMD struct {
		Ext       any      `json:"ext,omitempty"` // within meta-version extensions
		CluID     string   `json:"cluster_id"`    // effectively, Smap.UUID
		TargetIDs []string `json:"target_ids,omitempty"`
		Version   int64    `json:"version"`
	}
)
