// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
)

// enum: `statsValue` kinds
const (
	// lockless
	KindCounter            = "counter"
	KindTotal              = "total"
	KindSize               = "size"
	KindGauge              = "gauge"
	KindSpecial            = "special"
	KindComputedThroughput = "compbw" // disk read/write throughput
	// compound (+ semantics)
	KindLatency    = "latency"
	KindThroughput = "bw" // e.g. GetThroughput
)

type (
	Tracker interface {
		cos.StatsUpdater

		StartedUp() bool

		IsPrometheus() bool

		IncErr(metric string)

		GetStats() *Node
		GetStatsV322() *NodeV322 // [backward compatibility]

		ResetStats(errorsOnly bool)
		GetMetricNames() cos.StrKVs // (name, kind) pairs

		// for aistore modules, to add their respective metrics
		RegExtMetric(node *meta.Snode, name, kind string, extra *Extra)
	}
)

type (

	// REST API
	Node struct {
		Snode   *meta.Snode `json:"snode"`
		Tracker copyTracker `json:"tracker"`
		Tcdf    fs.Tcdf     `json:"capacity"`
	}
	Cluster struct {
		Proxy  *Node            `json:"proxy"`
		Target map[string]*Node `json:"target"`
	}
	ClusterRaw struct {
		Proxy  *Node           `json:"proxy"`
		Target cos.JSONRawMsgs `json:"target"`
	}

	// (includes stats.Node and more; NOTE: direct API call w/ no proxying)
	NodeStatus struct {
		RebSnap *core.Snap `json:"rebalance_snap,omitempty"`
		// assorted props
		Status         string `json:"status"`
		DeploymentType string `json:"deployment"`
		Version        string `json:"ais_version"`  // major.minor.build
		BuildTime      string `json:"build_time"`   // YYYY-MM-DD HH:MM:SS-TZ
		K8sPodName     string `json:"k8s_pod_name"` // (via ais-k8s/operator `MY_POD` env var)
		Reserved1      string `json:"reserved1,omitempty"`
		Reserved2      string `json:"reserved2,omitempty"`
		Node
		Cluster     cos.NodeStateInfo
		MemCPUInfo  apc.MemCPUInfo `json:"sys_info"`
		SmapVersion int64          `json:"smap_version,string"`
		Reserved3   int64          `json:"reserved3,omitempty"`
		Reserved4   int64          `json:"reserved4,omitempty"`
	}
)

// [backward compatibility]: includes v3.22 cdf* structures
type (
	NodeV322 struct {
		Snode   *meta.Snode      `json:"snode"`
		Tracker copyTracker      `json:"tracker"`
		Tcdf    fs.TargetCDFv322 `json:"capacity"`
	}
	NodeStatusV322 struct {
		RebSnap *core.Snap `json:"rebalance_snap,omitempty"`
		// assorted props
		Status         string `json:"status"`
		DeploymentType string `json:"deployment"`
		Version        string `json:"ais_version"`  // major.minor.build
		BuildTime      string `json:"build_time"`   // YYYY-MM-DD HH:MM:SS-TZ
		K8sPodName     string `json:"k8s_pod_name"` // (via ais-k8s/operator `MY_POD` env var)
		NodeV322
		MemCPUInfo  apc.MemCPUInfo `json:"sys_info"`
		SmapVersion int64          `json:"smap_version,string"`
	}
)

type (
	Extra struct {
		Labels  cos.StrKVs
		StrName string
		Help    string
	}
)

func IsErrMetric(name string) bool {
	return strings.HasPrefix(name, errPrefix) // e.g., "err.get.n"
}

func IsIOErrMetric(name string) bool {
	return strings.HasPrefix(name, ioErrPrefix) // e.g., "err.io.get.n" (see ioErrNames)
}

// compare with base.init() at ais/backend/common
func LatencyToCounter(latency string) string {
	// 1. basics first
	switch latency {
	case GetLatency, GetRedirLatency:
		return GetCount
	case PutLatency, PutRedirLatency:
		return PutCount
	case HeadLatency:
		return HeadCount
	case ListLatency:
		return ListCount
	case AppendLatency:
		return AppendCount
	}
	// 2. filter out
	if !strings.Contains(latency, "get.") && !strings.Contains(latency, "put.") {
		return ""
	}
	// backend first
	if strings.HasSuffix(latency, ".ns.total") {
		for prefix := range apc.Providers {
			if prefix == apc.AIS {
				prefix = apc.RemAIS
			}
			if strings.HasPrefix(latency, prefix) {
				if strings.Contains(latency, ".get.") {
					return prefix + "." + GetCount
				}
				return prefix + "." + PutCount
			}
		}
	}
	return ""
}
