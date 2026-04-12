// Package stats provides methods and functionality to register, track, log,
// and export metrics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
)

type (
	//msgp:ignore copyTracker
	Node struct {
		Snode   *meta.Snode `json:"snode" msg:"n"`
		Tracker copyTracker `json:"tracker" msg:"t"`
		Tcdf    fs.Tcdf     `json:"capacity" msg:"x"`
	}

	// main control structure (carries much of control-plane info)
	NodeStatus struct {
		RebSnap        *core.Snap `json:"rebalance_snap,omitempty" msg:"r,omitempty"`
		Status         string     `json:"status" msg:"s"`
		DeploymentType string     `json:"deployment" msg:"d"`
		Version        string     `json:"ais_version" msg:"v"`  // major.minor.build
		BuildTime      string     `json:"build_time" msg:"b"`   // YYYY-MM-DD HH:MM:SS-TZ
		K8sPodName     string     `json:"k8s_pod_name" msg:"k"` // (via ais-k8s/operator `MY_POD` env var)
		Reserved1      string     `json:"reserved1,omitempty" msg:"q1,omitempty"`
		Reserved2      string     `json:"reserved2,omitempty" msg:"q2,omitempty"`
		Node
		Cluster     cos.NodeStateInfo `json:"cluster" msg:"c"` // (4.5 note: add explicit json tag)
		MemCPUInfo  apc.MemCPUInfo    `json:"sys_info" msg:"y"`
		SmapVersion int64             `json:"smap_version,string" msg:"w"`
		Reserved3   int64             `json:"reserved3,omitempty" msg:"q3,omitempty"`
		Reserved4   int64             `json:"reserved4,omitempty" msg:"q4,omitempty"`
	}
)
