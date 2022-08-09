// Package apc: API constant
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	jsoniter "github.com/json-iterator/go"
)

// ActionMsg is a JSON-formatted control structures used in a majority of API calls
type (
	ActionMsg struct {
		Value  interface{} `json:"value"`  // action-specific and optional
		Action string      `json:"action"` // ActShutdown, ActRebalance, and many more (see apc/const.go)
		Name   string      `json:"name"`   // action-specific name (e.g., bucket name)
	}
	ActValRmNode struct {
		DaemonID          string `json:"sid"`
		SkipRebalance     bool   `json:"skip_rebalance"`
		RmUserData        bool   `json:"rm_user_data"`        // decommission-only
		KeepInitialConfig bool   `json:"keep_initial_config"` // ditto (to be able to restart a node from scratch)
		NoShutdown        bool   `json:"no_shutdown"`
	}
)

type (
	JoinNodeResult struct {
		DaemonID    string `json:"daemon_id"`
		RebalanceID string `json:"rebalance_id"`
	}
)

// MountpathList contains two lists:
//   - Available - list of local mountpaths available to the storage target
//   - WaitingDD - waiting for resilvering completion to be detached or disabled (moved to `Disabled`)
//   - Disabled  - list of disabled mountpaths, the mountpaths that generated
//     IO errors followed by (FSHC) health check, etc.
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
		cos.MemCPUInfo
		CapacityInfo
	}
	ClusterSysInfo struct {
		Proxy  map[string]*cos.MemCPUInfo `json:"proxy"`
		Target map[string]*TSysInfo       `json:"target"`
	}
	ClusterSysInfoRaw struct {
		Proxy  cos.JSONRawMsgs `json:"proxy"`
		Target cos.JSONRawMsgs `json:"target"`
	}
)

///////////////
// ActionMsg //
///////////////
func (msg *ActionMsg) String() string {
	s := "amsg[" + msg.Action
	if msg.Name != "" {
		s += ", name=" + msg.Name
	}
	if msg.Value == nil {
		return s + "]"
	}
	vs, err := jsoniter.Marshal(msg.Value)
	if err != nil {
		debug.AssertNoErr(err)
		s += "-<json err: " + err.Error() + ">"
		return s + "]"
	}
	s += ", val=" + strings.ReplaceAll(string(vs), ",", ", ") + "]"
	return s
}
