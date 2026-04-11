// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"fmt"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
)

const (
	colProxy      = "PROXY"
	colTarget     = "TARGET"
	colMemUsed    = "MEM USED(%)"
	colMemAvail   = "MEM AVAIL"
	colCapUsed    = "CAP USED(%)"
	colCapAvail   = "CAP AVAIL"
	colSysCPU     = "SYS CPU(%)"
	colThrottled  = "THROTTLED(%)"
	colLoadAvg    = "LOAD AVERAGE"
	colRebalance  = "REBALANCE"
	colUptime     = "UPTIME"
	colPodName    = "K8s POD"
	colStatus     = "STATUS"
	colVersion    = "VERSION"
	colBuildTime  = "BUILD TIME"
	colStateFlags = "ALERT"
)

// TODO: extend api.GetClusterSysInfo() and api.GetStatsAndStatus to return memsys.Pressure
const (
	memPctUsedHigh    = 80
	memPctUsedExtreme = 90

	memAvailLow = cos.GiB
	memOOM      = 200 * cos.MiB
)

type (
	NodeStatusMap map[string]*stats.NodeStatus // by node ID (SID)

	ClusterTabOpts struct {
		Units      string
		Verbose    bool
		ShowSysCPU bool
		ShowLoad   bool
	}
)

// proxy(ies)
func (h *StatsAndStatusHelper) MakeTabP(smap *meta.Smap, opts ClusterTabOpts) *Table {
	var (
		pods         = h.pods()
		status       = h.onlineStatus()
		versions     = h.versions()
		builds       = h.buildTimes()
		showThrottle = anyThrottled(h.Pmap) // can optimize-out if !opts.ShowSysCPU (pre-4.4)
		cols         = []*header{
			{name: colProxy},
			{name: colMemUsed},
			{name: colMemAvail},
			{name: colSysCPU, hide: !opts.ShowSysCPU},
			{name: colThrottled, hide: !showThrottle},
			{name: colLoadAvg, hide: !opts.ShowLoad},
			{name: colUptime},
			{name: colPodName, hide: len(pods) == 1 && pods[0] == ""},
			{name: colStatus, hide: len(status) == 1 && status[0] == NodeOnline},
			{name: colVersion, hide: len(versions) == 1 && len(builds) == 1},
			{name: colBuildTime, hide: len(versions) == 1 && len(builds) == 1},
			{name: colStateFlags, hide: h.Pmap.allStateFlagsOK()},
		}
		table = newTable(cols...)
	)

	ids := h.Pmap.sortPODs(smap, true)
	for _, sid := range ids {
		ds := h.Pmap[sid]
		if ds.Status != NodeOnline {
			nid, nstatus := fmtStatusSID(ds.Snode.ID(), smap, ds.Status)
			table.addRow(row{
				nid,
				unknownVal, // mem used
				unknownVal, // mem avail
				unknownVal, // sys cpu
				unknownVal, // throttled
				unknownVal, // load avg
				unknownVal, // uptime
				ds.K8sPodName,
				nstatus,
				ds.Version,
				ds.BuildTime,
				unknownVal, // alert
			})
			continue
		}

		memUsed, high, oom := _memUsed(ds.MemCPUInfo.PctMemUsed)
		memAvail, status := _memAvail(int64(ds.MemCPUInfo.MemAvail), opts.Units, ds.Status, high, oom)

		upns := ds.Tracker[stats.Uptime].Value
		uptime := FmtDuration(upns, opts.Units)
		if upns == 0 {
			uptime = unknownVal
		}

		table.addRow(row{
			fmtDaemonID(ds.Snode.ID(), smap, ds.Status),
			memUsed,
			memAvail,
			_sysCPU(&ds.MemCPUInfo),
			_throttled(&ds.MemCPUInfo),
			_load(&ds.MemCPUInfo),
			uptime,
			ds.K8sPodName,
			status,
			ds.Version,
			ds.BuildTime,
			fmtAlerts(ds.Cluster.Flags),
		})
	}
	return table
}

func _memUsed(pctUsed float64) (s string, high, oom bool) {
	s = fmt.Sprintf("%.2f%%", pctUsed)
	switch {
	case pctUsed >= memPctUsedExtreme:
		oom = true
	case pctUsed >= memPctUsedHigh:
		high = true
	}
	return
}

func _memAvail(avail int64, units, status string, high, oom bool) (string, string) {
	s := FmtSize(avail, units, 2)
	switch {
	case avail < memOOM:
		oom = true
	case avail < memAvailLow:
		high = true
	}
	if oom {
		status += fred(" (out of memory)")
	} else if high {
		status += fcyan(" (low on memory)")
	}
	return s, status
}

// target(s)
func (h *StatsAndStatusHelper) MakeTabT(smap *meta.Smap, opts ClusterTabOpts) *Table {
	var (
		pods         = h.pods()
		status       = h.onlineStatus()
		versions     = h.versions()
		builds       = h.buildTimes()
		showThrottle = anyThrottled(h.Tmap) // ditto
		cols         = []*header{
			{name: colTarget},
			{name: colMemUsed},
			{name: colMemAvail},
			{name: colCapUsed},
			{name: colCapAvail},
			{name: colSysCPU, hide: !opts.ShowSysCPU},
			{name: colThrottled, hide: !showThrottle},
			{name: colLoadAvg, hide: !opts.ShowLoad},
			{name: colRebalance, hide: len(h.rebalance()) == 0},
			{name: colUptime},
			{name: colPodName, hide: len(pods) == 1 && pods[0] == ""},
			{name: colStatus, hide: len(status) == 1 && status[0] == NodeOnline},
			{name: colVersion, hide: len(versions) == 1 && len(builds) == 1},
			{name: colBuildTime, hide: len(versions) == 1 && len(builds) == 1},
			{name: colStateFlags, hide: h.Tmap.allStateFlagsOK()},
		}
		table = newTable(cols...)
	)

	ids := h.Tmap.sortPODs(smap, false)
	for _, sid := range ids {
		ds := h.Tmap[sid]
		if ds.Status != NodeOnline {
			nid, nstatus := fmtStatusSID(ds.Snode.ID(), smap, ds.Status)
			table.addRow(row{
				nid,
				unknownVal, // mem used
				unknownVal, // mem avail
				unknownVal, // cap used
				unknownVal, // cap avail
				unknownVal, // sys cpu
				unknownVal, // throttled
				unknownVal, // load avg
				unknownVal, // rebalance
				unknownVal, // uptime
				ds.K8sPodName,
				nstatus,
				ds.Version,
				ds.BuildTime,
				unknownVal, // alert
			})
			continue
		}

		memUsed, high, oom := _memUsed(ds.MemCPUInfo.PctMemUsed)
		memAvail, status := _memAvail(int64(ds.MemCPUInfo.MemAvail), opts.Units, ds.Status, high, oom)

		upns := ds.Tracker[stats.Uptime].Value
		uptime := FmtDuration(upns, opts.Units)
		if upns == 0 {
			uptime = unknownVal
		}

		capUsed := fmt.Sprintf("%d%%", ds.Tcdf.PctAvg)
		v := calcCap(ds)
		capAvail := FmtSize(int64(v), opts.Units, 3)
		if v == 0 {
			capUsed, capAvail = UnknownStatusVal, UnknownStatusVal
		}

		table.addRow(row{
			fmtDaemonID(ds.Snode.ID(), smap, ds.Status),
			memUsed,
			memAvail,
			capUsed,
			capAvail,
			_sysCPU(&ds.MemCPUInfo),
			_throttled(&ds.MemCPUInfo),
			_load(&ds.MemCPUInfo),
			fmtRebStatus(ds.RebSnap),
			uptime,
			ds.K8sPodName,
			status,
			ds.Version,
			ds.BuildTime,
			fmtAlerts(ds.Cluster.Flags),
		})
	}
	return table
}

func _sysCPU(info *apc.MemCPUInfo) string {
	if info.CPUUtil == 0 &&
		info.LoadAvg.One == 0 &&
		info.LoadAvg.Five == 0 &&
		info.LoadAvg.Fifteen == 0 {
		return NotSetVal
	}
	return fmt.Sprintf("%d%%", info.CPUUtil) // not using %3d - tabwriter aligns the column
}

func _throttled(info *apc.MemCPUInfo) string {
	if info.CPUThrottled == 0 {
		return NotSetVal
	}
	s := fmt.Sprintf("%2d%%", info.CPUThrottled)
	return fred(s) // fixed-width and colored
}

func _load(info *apc.MemCPUInfo) string {
	load := fmt.Sprintf("[%.1f %.1f %.1f]", info.LoadAvg.One, info.LoadAvg.Five, info.LoadAvg.Fifteen)
	// older version
	if info.LoadAvg.One == 0 && info.LoadAvg.Five == 0 && info.LoadAvg.Fifteen == 0 {
		load = UnknownStatusVal
	}
	return load
}

func anyThrottled(m NodeStatusMap) bool {
	for _, ds := range m {
		if ds.MemCPUInfo.CPUThrottled != 0 {
			return true
		}
	}
	return false
}
