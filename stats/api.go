// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats/statsd"
)

// enum: `statsValue` kinds
const (
	KindCounter = "counter"
	KindGauge   = "gauge"
	// + semantics
	KindLatency            = "latency"
	KindThroughput         = "bw"     // currently, only GetThroughput - see NOTE below
	KindComputedThroughput = "compbw" // disk(s) read/write throughputs
	KindSpecial            = "special"
)

type (
	Tracker interface {
		cos.StatsTracker

		StartedUp() bool
		AddErrorHTTP(method string, val int64)

		CoreStats() *CoreStats
		GetWhatStats() *DaemonStats

		GetMetricNames() cos.StrKVs // (name, kind) pairs
		RegMetrics(node *cluster.Snode)

		IsPrometheus() bool
	}
	CoreStats struct {
		Tracker   statsTracker
		promDesc  promDesc
		statsdC   *statsd.Client
		sgl       *memsys.SGL
		statsTime time.Duration
		cmu       sync.RWMutex // ctracker vs Prometheus Collect()
	}

	// REST API
	DaemonStatus struct {
		Snode          *cluster.Snode `json:"snode"`
		Stats          *CoreStats     `json:"daemon_stats"`
		Capacity       fs.MPCap       `json:"capacity"`
		RebSnap        *cluster.Snap  `json:"rebalance_snap,omitempty"`
		Status         string         `json:"status"`
		DeploymentType string         `json:"deployment"`
		Version        string         `json:"ais_version"`  // major.minor.build
		BuildTime      string         `json:"build_time"`   // YYYY-MM-DD HH:MM:SS-TZ
		K8sPodName     string         `json:"k8s_pod_name"` // (via ais-k8s/operator `MY_POD` env var)
		MemCPUInfo     cos.MemCPUInfo `json:"sys_info"`
		SmapVersion    int64          `json:"smap_version,string"`
	}
	DaemonStatusMap map[string]*DaemonStatus // by SID (aka DaemonID)

	DaemonStats struct {
		Tracker copyTracker `json:"tracker"`
		MPCap   fs.MPCap    `json:"capacity"`
	}
	ClusterStats struct {
		Proxy  *DaemonStats            `json:"proxy"`
		Target map[string]*DaemonStats `json:"target"`
	}
	ClusterStatsRaw struct {
		Proxy  *DaemonStats    `json:"proxy"`
		Target cos.JSONRawMsgs `json:"target"`
	}
)
