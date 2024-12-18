// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
)

const (
	colProxy     = "PROXY"
	colTarget    = "TARGET"
	colMemUsed   = "MEM USED(%)"
	colMemAvail  = "MEM AVAIL"
	colCapUsed   = "CAP USED(%)"
	colCapAvail  = "CAP AVAIL"
	colLoadAvg   = "LOAD AVERAGE"
	colRebalance = "REBALANCE"
	colUptime    = "UPTIME"
	colPodName   = "K8s POD"
	colStatus    = "STATUS"
	colVersion   = "VERSION"
	colBuildTime = "BUILD TIME"

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
func (h *StatsAndStatusHelper) MakeTabP(smap *meta.Smap, units string) *Table {
	var (
		pods     = h.pods()
		status   = h.onlineStatus()
		versions = h.versions()
		builds   = h.buildTimes()
		cols     = []*header{
			{name: colProxy},
			{name: colMemUsed},
			{name: colMemAvail},
			{name: colLoadAvg},
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
			row := []string{
				nid,
				unknownVal,
				unknownVal,
				unknownVal,
				unknownVal,
				ds.K8sPodName,
				nstatus,
				ds.Version,
				ds.BuildTime,
				unknownVal,
			}
			table.addRow(row)
			continue
		}

		var memAvail string
		memUsed, high, oom := _memUsed(ds.MemCPUInfo.PctMemUsed)
		memAvail, ds.Status = _memAvail(int64(ds.MemCPUInfo.MemAvail), units, ds.Status, high, oom)

		load := fmt.Sprintf("[%.1f %.1f %.1f]", ds.MemCPUInfo.LoadAvg.One, ds.MemCPUInfo.LoadAvg.Five, ds.MemCPUInfo.LoadAvg.Fifteen)
		// older version
		if ds.MemCPUInfo.LoadAvg.One == 0 && ds.MemCPUInfo.LoadAvg.Five == 0 && ds.MemCPUInfo.LoadAvg.Fifteen == 0 {
			load = UnknownStatusVal
		}
		upns := ds.Tracker[stats.Uptime].Value
		uptime := FmtDuration(upns, units)
		if upns == 0 {
			uptime = unknownVal
		}
		row := []string{
			fmtDaemonID(ds.Snode.ID(), smap, ds.Status),
			memUsed,
			memAvail,
			load,
			uptime,
			ds.K8sPodName,
			ds.Status,
			ds.Version,
			ds.BuildTime,
			fmtAlerts(ds.Cluster.Flags),
		}
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
func (h *StatsAndStatusHelper) MakeTabT(smap *meta.Smap, units string) *Table {
	var (
		pods     = h.pods()
		status   = h.onlineStatus()
		versions = h.versions()
		builds   = h.buildTimes()
		cols     = []*header{
			{name: colTarget},
			{name: colMemUsed},
			{name: colMemAvail},
			{name: colCapUsed},
			{name: colCapAvail},
			{name: colLoadAvg},
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
			row := []string{
				nid,
				unknownVal,
				unknownVal,
				unknownVal,
				unknownVal,
				unknownVal,
				unknownVal,
				unknownVal,
				ds.K8sPodName,
				nstatus,
				ds.Version,
				ds.BuildTime,
				unknownVal,
			}
			table.addRow(row)
			continue
		}

		var memAvail string
		memUsed, high, oom := _memUsed(ds.MemCPUInfo.PctMemUsed)
		memAvail, ds.Status = _memAvail(int64(ds.MemCPUInfo.MemAvail), units, ds.Status, high, oom)

		upns := ds.Tracker[stats.Uptime].Value
		uptime := FmtDuration(upns, units)
		if upns == 0 {
			uptime = unknownVal
		}
		load := fmt.Sprintf("[%.1f %.1f %.1f]", ds.MemCPUInfo.LoadAvg.One, ds.MemCPUInfo.LoadAvg.Five, ds.MemCPUInfo.LoadAvg.Fifteen)
		// older version
		if ds.MemCPUInfo.LoadAvg.One == 0 && ds.MemCPUInfo.LoadAvg.Five == 0 && ds.MemCPUInfo.LoadAvg.Fifteen == 0 {
			load = UnknownStatusVal
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
			load,
			fmtRebStatus(ds.RebSnap),
			uptime,
			ds.K8sPodName,
			ds.Status,
			ds.Version,
			ds.BuildTime,
			fmtAlerts(ds.Cluster.Flags),
		}
		table.addRow(row)
	}
	return table
}
