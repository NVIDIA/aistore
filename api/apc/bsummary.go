// Package apc: API messages and constants
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package apc

type (
	// to generate bucket summary (or summaries)
	BsummCtrlMsg struct {
		UUID          string `json:"uuid"`
		Prefix        string `json:"prefix"`
		ObjCached     bool   `json:"cached"`
		BckPresent    bool   `json:"present"`
		DontAddRemote bool   `json:"dont_add_remote"`
	}

	// "summarized" result for a given bucket
	BsummResult struct {
		ObjCount struct {
			Present uint64 `json:"obj_count_present,string"`
			Remote  uint64 `json:"obj_count_remote,string"`
		}
		ObjSize struct {
			Min int64 `json:"obj_min_size"`
			Avg int64 `json:"obj_avg_size"`
			Max int64 `json:"obj_max_size"`
		}
		TotalSize struct {
			OnDisk      uint64 `json:"size_on_disk,string"`          // sum(dir sizes) aka "apparent size"
			PresentObjs uint64 `json:"size_all_present_objs,string"` // sum(cached object sizes)
			RemoteObjs  uint64 `json:"size_all_remote_objs,string"`  // sum(all object sizes in a remote bucket)
			Disks       uint64 `json:"total_disks_size,string"`
		}
		UsedPct      uint64 `json:"used_pct"`
		IsBckPresent bool   `json:"is_present"` // in BMD
	}
)
