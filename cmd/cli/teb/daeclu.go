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

// proxy(ies)
func (h *StatsAndStatusHelper) MakeTabP(smap *meta.Smap, units string, verbose bool) *Table {
	var (
		pods         = h.pods()
		status       = h.onlineStatus()
		versions     = h.versions()
		builds       = h.buildTimes()
		showThrottle = h.anyThrottled(false /*targets*/)
		cols         = []*header{
			{name: colProxy},
			{name: colMemUsed},
			{name: colMemAvail},
			{name: colSysCPU},
		}
		table *Table
	)

	// two "conditional" columns
	if showThrottle {
		cols = append(cols, &header{name: colThrottled})
	}
	if verbose {
		cols = append(cols, &header{name: colLoadAvg})
	}

	cols = append(cols,
		&header{name: colUptime},
		&header{name: colPodName, hide: len(pods) == 1 && pods[0] == ""},
		&header{name: colStatus, hide: len(status) == 1 && status[0] == NodeOnline},
		&header{name: colVersion, hide: len(versions) == 1 && len(builds) == 1},
		&header{name: colBuildTime, hide: len(versions) == 1 && len(builds) == 1},
		&header{name: colStateFlags, hide: h.Pmap.allStateFlagsOK()},
	)

	table = newTable(cols...)

	ids := h.Pmap.sortPODs(smap, true)
	for _, sid := range ids {
		ds := h.Pmap[sid]

		if ds.Status != NodeOnline {
			nid, nstatus := fmtStatusSID(ds.Snode.ID(), smap, ds.Status)
			row := []string{
				nid,
				unknownVal,
				unknownVal,
				unknownVal, // sys cpu
			}
			if showThrottle {
				row = append(row, unknownVal)
			}
			if verbose {
				row = append(row, unknownVal) // load avg
			}
			row = append(row,
				unknownVal, // uptime
				ds.K8sPodName,
				nstatus,
				ds.Version,
				ds.BuildTime,
				unknownVal, // alert
			)
			table.addRow(row)
			continue
		}

		memUsed, high, oom := _memUsed(ds.MemCPUInfo.PctMemUsed)
		memAvail, status := _memAvail(int64(ds.MemCPUInfo.MemAvail), units, ds.Status, high, oom)

		upns := ds.Tracker[stats.Uptime].Value
		uptime := FmtDuration(upns, units)
		if upns == 0 {
			uptime = unknownVal
		}

		row := []string{
			fmtDaemonID(ds.Snode.ID(), smap, ds.Status),
			memUsed,
			memAvail,
			_sysCPU(ds.MemCPUInfo),
		}
		if showThrottle {
			row = append(row, _throttled(ds.MemCPUInfo))
		}
		if verbose {
			row = append(row, _load(ds.MemCPUInfo))
		}
		row = append(row,
			uptime,
			ds.K8sPodName,
			status,
			ds.Version,
			ds.BuildTime,
			fmtAlerts(ds.Cluster.Flags),
		)
		table.addRow(row)
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
func (h *StatsAndStatusHelper) MakeTabT(smap *meta.Smap, units string, verbose bool) *Table {
	var (
		pods         = h.pods()
		status       = h.onlineStatus()
		versions     = h.versions()
		builds       = h.buildTimes()
		showThrottle = h.anyThrottled(true /*targets*/)
		cols         = []*header{
			{name: colTarget},
			{name: colMemUsed},
			{name: colMemAvail},
			{name: colCapUsed},
			{name: colCapAvail},
			{name: colSysCPU},
		}
		table *Table
	)

	// two "conditional" columns
	if showThrottle {
		cols = append(cols, &header{name: colThrottled})
	}
	if verbose {
		cols = append(cols, &header{name: colLoadAvg})
	}

	cols = append(cols,
		&header{name: colRebalance, hide: len(h.rebalance()) == 0},
		&header{name: colUptime},
		&header{name: colPodName, hide: len(pods) == 1 && pods[0] == ""},
		&header{name: colStatus, hide: len(status) == 1 && status[0] == NodeOnline},
		&header{name: colVersion, hide: len(versions) == 1 && len(builds) == 1},
		&header{name: colBuildTime, hide: len(versions) == 1 && len(builds) == 1},
		&header{name: colStateFlags, hide: h.Tmap.allStateFlagsOK()},
	)

	table = newTable(cols...)

	ids := h.Tmap.sortPODs(smap, false)
	for _, sid := range ids {
		ds := h.Tmap[sid]

		if ds.Status != NodeOnline {
			nid, nstatus := fmtStatusSID(ds.Snode.ID(), smap, ds.Status)
			row := []string{
				nid,
				unknownVal,
				unknownVal,
				unknownVal,
				unknownVal,
				unknownVal, // sys cpu
			}
			if showThrottle {
				row = append(row, unknownVal)
			}
			if verbose {
				row = append(row, unknownVal) // load avg
			}
			row = append(row,
				unknownVal, // rebalance
				unknownVal, // uptime
				ds.K8sPodName,
				nstatus,
				ds.Version,
				ds.BuildTime,
				unknownVal, // alert
			)
			table.addRow(row)
			continue
		}

		memUsed, high, oom := _memUsed(ds.MemCPUInfo.PctMemUsed)
		memAvail, status := _memAvail(int64(ds.MemCPUInfo.MemAvail), units, ds.Status, high, oom)

		upns := ds.Tracker[stats.Uptime].Value
		uptime := FmtDuration(upns, units)
		if upns == 0 {
			uptime = unknownVal
		}

		capUsed := fmt.Sprintf("%d%%", ds.Tcdf.PctAvg)
		v := calcCap(ds)
		capAvail := FmtSize(int64(v), units, 3)
		if v == 0 {
			capUsed, capAvail = UnknownStatusVal, UnknownStatusVal
		}

		row := []string{
			fmtDaemonID(ds.Snode.ID(), smap, ds.Status),
			memUsed,
			memAvail,
			capUsed,
			capAvail,
			_sysCPU(ds.MemCPUInfo),
		}
		if showThrottle {
			row = append(row, _throttled(ds.MemCPUInfo))
		}
		if verbose {
			row = append(row, _load(ds.MemCPUInfo))
		}
		row = append(row,
			fmtRebStatus(ds.RebSnap),
			uptime,
			ds.K8sPodName,
			status,
			ds.Version,
			ds.BuildTime,
			fmtAlerts(ds.Cluster.Flags),
		)
		table.addRow(row)
	}
	return table
}

func _sysCPU(info apc.MemCPUInfo) string {
	if info.CPUUtil == 0 &&
		info.LoadAvg.One == 0 &&
		info.LoadAvg.Five == 0 &&
		info.LoadAvg.Fifteen == 0 {
		return NotSetVal
	}
	return fmt.Sprintf("%d%%", info.CPUUtil) // not using %3d - tabwriter aligns the column
}

func _throttled(info apc.MemCPUInfo) string {
	if info.CPUThrottled == 0 {
		return NotSetVal
	}
	s := fmt.Sprintf("%2d%%", info.CPUThrottled)
	return fred(s) // fixed-width and colored
}

func _load(info apc.MemCPUInfo) string {
	load := fmt.Sprintf("[%.1f %.1f %.1f]", info.LoadAvg.One, info.LoadAvg.Five, info.LoadAvg.Fifteen)
	// older version
	if info.LoadAvg.One == 0 && info.LoadAvg.Five == 0 && info.LoadAvg.Fifteen == 0 {
		load = UnknownStatusVal
	}
	return load
}

func (h *StatsAndStatusHelper) anyThrottled(targets bool) bool {
	if targets {
		for _, ds := range h.Tmap {
			if ds.MemCPUInfo.CPUThrottled != 0 {
				return true
			}
		}
		return false
	}
	for _, ds := range h.Pmap {
		if ds.MemCPUInfo.CPUThrottled != 0 {
			return true
		}
	}
	return false
}
