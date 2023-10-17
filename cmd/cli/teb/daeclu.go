// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"fmt"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn/debug"
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
	colStatus    = "STATUS"
	colVersion   = "VERSION"
	colBuildTime = "BUILD TIME"
	colPodName   = "K8s POD"
)

func NewDaeStatus(st *stats.NodeStatus, smap *meta.Smap, daeType, units string) *Table {
	switch daeType {
	case apc.Proxy:
		return newTableProxies(StstMap{st.Snode.ID(): st}, smap, units)
	case apc.Target:
		return newTableTargets(StstMap{st.Snode.ID(): st}, smap, units)
	default:
		debug.Assert(false)
		return nil
	}
}

func NewDaeMapStatus(ds *StatsAndStatusHelper, smap *meta.Smap, daeType, units string) *Table {
	switch daeType {
	case apc.Proxy:
		return newTableProxies(ds.Pmap, smap, units)
	case apc.Target:
		return newTableTargets(ds.Tmap, smap, units)
	default:
		debug.Assert(false)
		return nil
	}
}

// proxy(ies)
func newTableProxies(ps StstMap, smap *meta.Smap, units string) *Table {
	var (
		h        = StatsAndStatusHelper{Pmap: ps}
		pods     = h.pods()
		status   = h.onlineStatus()
		versions = h.versions()
		cols     = []*header{
			{name: colProxy},
			{name: colMemUsed},
			{name: colMemAvail},
			{name: colLoadAvg},
			{name: colUptime},
			{name: colPodName, hide: len(pods) == 1 && pods[0] == ""},
			{name: colStatus, hide: len(status) == 1 && status[0] == NodeOnline},
			{name: colVersion, hide: len(versions) == 1 && len(ps) > 1},
			{name: colBuildTime, hide: len(versions) == 1 && len(ps) > 1}, // intended
		}
		table = newTable(cols...)
	)

	ids := ps.sortedSIDs()

	for _, sid := range ids {
		ds := ps[sid]

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
			}
			table.addRow(row)
			continue
		}

		memUsed := fmt.Sprintf("%.2f%%", ds.MemCPUInfo.PctMemUsed)
		memAvail := FmtSize(int64(ds.MemCPUInfo.MemAvail), units, 2)
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
		}
		table.addRow(row)
	}
	return table
}

// target(s)
func newTableTargets(ts StstMap, smap *meta.Smap, units string) *Table {
	var (
		h        = StatsAndStatusHelper{Tmap: ts}
		pods     = h.pods()
		status   = h.onlineStatus()
		versions = h.versions()
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
			{name: colVersion, hide: len(versions) == 1 && len(ts) > 1},
			{name: colBuildTime, hide: len(versions) == 1 && len(ts) > 1}, // intended
		}
		table = newTable(cols...)
	)
	ids := ts.sortedSIDs()

	for _, sid := range ids {
		ds := ts[sid]

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
			}
			table.addRow(row)
			continue
		}

		memUsed := fmt.Sprintf("%.2f%%", ds.MemCPUInfo.PctMemUsed)
		memAvail := FmtSize(int64(ds.MemCPUInfo.MemAvail), units, 2)
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
		capUsed := fmt.Sprintf("%d%%", ds.TargetCDF.PctAvg)
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
		}
		table.addRow(row)
	}
	return table
}
