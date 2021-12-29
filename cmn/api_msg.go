// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import "github.com/NVIDIA/aistore/cmn/cos"

// ActionMsg is a JSON-formatted control structures used in a majority of API calls
type (
	ActionMsg struct {
		Action string      `json:"action"` // ActShutdown, ActRebalance, and many more (see api_const.go)
		Name   string      `json:"name"`   // action-specific (e.g., bucket name)
		Value  interface{} `json:"value"`  // ditto
	}
	ActValPromote struct {
		Target    string `json:"target"`
		ObjName   string `json:"objname"`
		Recursive bool   `json:"recursive"`
		Overwrite bool   `json:"overwrite"`
		KeepOrig  bool   `json:"keep_original"`
	}
	ActValRmNode struct {
		DaemonID      string `json:"sid"`
		SkipRebalance bool   `json:"skip_rebalance"`
		RmUserData    bool   `json:"rm_user_data"` // remove user data (decommission-only)
		NoShutdown    bool   `json:"no_shutdown"`
	}
)

type (
	JoinNodeResult struct {
		DaemonID    string `json:"daemon_id"`
		RebalanceID string `json:"rebalance_id"`
	}
)

// MountpathList contains two lists:
// * Available - list of local mountpaths available to the storage target
// * WaitingDD - waiting for resilvering completion to be detached or disabled (moved to `Disabled`)
// * Disabled  - list of disabled mountpaths, the mountpaths that generated
//	         IO errors followed by (FSHC) health check, etc.
type (
	MountpathList struct {
		Available []string `json:"available"`
		WaitingDD []string `json:"waiting_dd"`
		Disabled  []string `json:"disabled"`
	}
)

// sysinfo
type (
	CapacityInfo struct {
		Used    uint64  `json:"fs_used,string"`
		Total   uint64  `json:"fs_capacity,string"`
		PctUsed float64 `json:"pct_fs_used"`
	}
	TSysInfo struct {
		cos.SysInfo
		CapacityInfo
	}
	ClusterSysInfo struct {
		Proxy  map[string]*cos.SysInfo `json:"proxy"`
		Target map[string]*TSysInfo    `json:"target"`
	}
	ClusterSysInfoRaw struct {
		Proxy  cos.JSONRawMsgs `json:"proxy"`
		Target cos.JSONRawMsgs `json:"target"`
	}
)
