// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import "github.com/NVIDIA/aistore/cmn/cos"

type (
	// BsummCtrlMsg is the control message for computing a bucket
	// summary (object count, total size, used percentage, etc.).
	// Depending on the flags below, the summary covers only
	// cached-in-cluster objects or all objects (cached + remote).
	BsummCtrlMsg struct {
		// Stable ID that ties together the pages/streaming chunks of
		// one summary computation. Server-assigned on the first
		// response; the client echoes it back on each subsequent page.
		UUID string `json:"uuid"` // +gen:optional
		// Include only objects whose name starts with this prefix.
		Prefix string `json:"prefix"` // +gen:optional
		// Restrict the summary to objects currently cached in the
		// cluster. When `false`, remote objects are also counted.
		ObjCached bool `json:"cached"` // +gen:optional
		// Require the bucket to already be present in BMD. When
		// `false` and the bucket is remote-not-yet-present, the server
		// may add it on the fly (unless `dont_add_remote` is also set).
		BckPresent bool `json:"present"` // +gen:optional
		// Do not add a remote bucket to BMD as a side effect of
		// summarizing it. Takes precedence over `present`.
		DontAddRemote bool `json:"dont_add_remote"` // +gen:optional
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

func (msg *BsummCtrlMsg) Str(cname string, sb *cos.SB) {
	sb.WriteString(cname)

	sb.WriteString(", flags:")
	first := true
	if msg.ObjCached {
		sb.WriteString("cached")
		first = false
	}
	if msg.BckPresent {
		if !first {
			sb.WriteUint8(',')
		}
		sb.WriteString("bck-present")
		first = false
	}
	if msg.DontAddRemote {
		if !first {
			sb.WriteUint8(',')
		}
		sb.WriteString("don't-add")
	}
}
