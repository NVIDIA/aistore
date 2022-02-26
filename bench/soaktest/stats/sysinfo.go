// Package stats keeps track of all the different statistics collected by the report
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
)

const (
	TypeProxy  = "proxy"
	TypeTarget = "target"
)

// Tracks System Info Stats
type SysInfoStat struct {
	cos.SysInfo
	apc.CapacityInfo

	Type      string    `json:"type"` // type (proxy|target)
	DaemonID  string    `json:"daemonid"`
	Timestamp time.Time `json:"timestamp"`
}

func ParseClusterSysInfo(csi *apc.ClusterSysInfo, timestamp time.Time) []*SysInfoStat {
	result := make([]*SysInfoStat, 0)
	for k, v := range csi.Proxy {
		result = append(result, &SysInfoStat{SysInfo: *v, Type: TypeProxy, DaemonID: k, Timestamp: timestamp})
	}
	for k, v := range csi.Target {
		result = append(result,
			&SysInfoStat{
				SysInfo: v.SysInfo, CapacityInfo: v.CapacityInfo,
				Type: TypeTarget, DaemonID: k, Timestamp: timestamp,
			})
	}

	return result
}

func (SysInfoStat) getHeadingsText() map[string]string {
	return map[string]string{
		"timestamp": "Time (excel timestamp)",
		"daemonID":  "DaemonID",
		"role":      "Role",

		"memUsed":    "Memory Used (B)",
		"memAvail":   "Memory Available (B)",
		"pctMemUsed": "% Memory Used",
		"pctCpuUsed": "% CPU Used",

		"capUsed":    "Capacity Used (B)",
		"capAvail":   "Total Capacity (B)",
		"pctCapUsed": "% Capacity Used",
	}
}

func (SysInfoStat) getHeadingsOrder() []string {
	return []string{
		"timestamp", "daemonID", "role",
		"memUsed", "memAvail", "pctMemUsed", "pctCpuUsed",
		"capUsed", "capAvail", "pctCapUsed",
	}
}

func (sis SysInfoStat) getContents() map[string]interface{} {
	contents := map[string]interface{}{
		"timestamp": getTimestamp(sis.Timestamp),
		"daemonID":  sis.DaemonID,
		"role":      sis.Type,

		"memUsed":    sis.MemUsed,
		"memAvail":   sis.MemAvail,
		"pctMemUsed": sis.PctMemUsed,
		"pctCpuUsed": sis.PctCPUUsed,
	}

	if sis.Total > 0 {
		contents["capUsed"] = sis.Used
		contents["capAvail"] = sis.Total
		contents["pctCapUsed"] = sis.PctUsed
	}

	return contents
}
