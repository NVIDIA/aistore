// Package meta: cluster-level metadata
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package meta

type (
	// Rebalance MetaData
	RMD struct {
		Ext       any      `json:"ext,omitempty"` // within meta-version extensions
		CluID     string   `json:"cluster_id"`    // effectively, Smap.UUID
		TargetIDs []string `json:"target_ids,omitempty"`
		Version   int64    `json:"version"`

		// Minimum BMD version required by a bucket placement policy rebalance.
		PlacementBMDVersion int64 `json:"placement_bmd_version,string,omitempty"`
	}
)
