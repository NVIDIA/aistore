// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
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

const ErrPrefix = "err." // convention to be abided by

type (
	Tracker interface {
		cos.StatsUpdater

		StartedUp() bool
		IsPrometheus() bool

		IncErr(metric string)

		GetWhatStats() *Node
		GetMetricNames() cos.StrKVs // (name, kind) pairs

		RegMetrics(node *cluster.Snode) // + init Prometheus, if configured
	}

	// REST API
	Node struct {
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

	// (includes stats.Node and more)
	DaemonStatus struct {
		Snode *cluster.Snode `json:"snode"`
		// Node
		Tracker   copyTracker  `json:"stats_tracker"`
		TargetCDF fs.TargetCDF `json:"mountpaths_info"`
		// separately, rebalance stats
		RebSnap *cluster.Snap `json:"rebalance_snap,omitempty"`
		// assorted props
		Status         string         `json:"status"`
		DeploymentType string         `json:"deployment"`
		Version        string         `json:"ais_version"`  // major.minor.build
		BuildTime      string         `json:"build_time"`   // YYYY-MM-DD HH:MM:SS-TZ
		K8sPodName     string         `json:"k8s_pod_name"` // (via ais-k8s/operator `MY_POD` env var)
		MemCPUInfo     cos.MemCPUInfo `json:"sys_info"`
		SmapVersion    int64          `json:"smap_version,string"`
	}
)
