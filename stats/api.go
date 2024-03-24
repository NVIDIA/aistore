// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
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
	KindSize               = "size"
	KindGauge              = "gauge"
	KindSpecial            = "special"
	KindComputedThroughput = "compbw" // disk read/write throughput
	// compound (+ semantics)
	KindLatency    = "latency"
	KindThroughput = "bw" // e.g. GetThroughput
)

const errPrefix = "err." // all error metric names (see `IsErrMetric` below)

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

		RegMetrics(node *meta.Snode) // + init Prometheus, if configured
	}

	// REST API
	Node struct {
		Snode     *meta.Snode  `json:"snode"`
		Tracker   copyTracker  `json:"tracker"`
		TargetCDF fs.TargetCDF `json:"capacity"`
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
		Node
		RebSnap *core.Snap `json:"rebalance_snap,omitempty"`
		// assorted props
		Status         string         `json:"status"`
		DeploymentType string         `json:"deployment"`
		Version        string         `json:"ais_version"`  // major.minor.build
		BuildTime      string         `json:"build_time"`   // YYYY-MM-DD HH:MM:SS-TZ
		K8sPodName     string         `json:"k8s_pod_name"` // (via ais-k8s/operator `MY_POD` env var)
		MemCPUInfo     apc.MemCPUInfo `json:"sys_info"`
		SmapVersion    int64          `json:"smap_version,string"`
	}
)

// [backward compatibility]: includes v3.22 cdf* structures
type (
	NodeV322 struct {
		Snode     *meta.Snode      `json:"snode"`
		Tracker   copyTracker      `json:"tracker"`
		TargetCDF fs.TargetCDFv322 `json:"capacity"`
	}
	NodeStatusV322 struct {
		NodeV322
		RebSnap *core.Snap `json:"rebalance_snap,omitempty"`
		// assorted props
		Status         string         `json:"status"`
		DeploymentType string         `json:"deployment"`
		Version        string         `json:"ais_version"`  // major.minor.build
		BuildTime      string         `json:"build_time"`   // YYYY-MM-DD HH:MM:SS-TZ
		K8sPodName     string         `json:"k8s_pod_name"` // (via ais-k8s/operator `MY_POD` env var)
		MemCPUInfo     apc.MemCPUInfo `json:"sys_info"`
		SmapVersion    int64          `json:"smap_version,string"`
	}
)

func IsErrMetric(name string) bool {
	return strings.HasPrefix(name, errPrefix) // e.g. name = ErrHTTPWriteCount
}
