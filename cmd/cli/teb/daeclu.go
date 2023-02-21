// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"fmt"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
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
	colCPUUsed   = "CPU USED(%)"
	colRebalance = "REBALANCE"
	colUptime    = "UPTIME"
	colStatus    = "STATUS"
	colVersion   = "VERSION"
	colBuildTime = "BUILD TIME"
	colPodName   = "K8s POD"
)

func NewDaeStatus(st *stats.NodeStatus, smap *cluster.Smap, daeType, units string) *Table {
	switch daeType {
	case apc.Proxy:
		return newTableProxies(StStMap{st.Snode.ID(): st}, smap, units)
	case apc.Target:
		return newTableTargets(StStMap{st.Snode.ID(): st}, smap, units)
	default:
		debug.Assert(false)
		return nil
	}
}

func NewDaeMapStatus(ds *StatsAndStatusHelper, smap *cluster.Smap, daeType, units string) *Table {
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
func newTableProxies(ps StStMap, smap *cluster.Smap, units string) *Table {
	var (
		h        = StatsAndStatusHelper{Pmap: ps}
		pods     = h.pods()
		status   = h.onlineStatus()
		versions = h.versions()
		cols     = []*header{
			{name: colProxy},
			{name: colMemUsed},
			{name: colMemAvail},
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
			row := []string{
				fmtDaemonID(ds.Snode.ID(), smap, ds.Status),
				unknownVal,
				unknownVal,
				unknownVal,
				ds.K8sPodName,
				ds.Status,
				ds.Version,
				ds.BuildTime,
			}
			table.addRow(row)
			continue
		}

		memUsed := fmt.Sprintf("%.2f%%", ds.MemCPUInfo.PctMemUsed)
		if ds.MemCPUInfo.PctMemUsed == 0 {
			memUsed = unknownVal
		}
		memAvail := FmtSize(int64(ds.MemCPUInfo.MemAvail), units, 2)
		if ds.MemCPUInfo.MemAvail == 0 {
			memAvail = unknownVal
		}
		upns := ds.Tracker[stats.Uptime].Value
		uptime := fmtDuration(upns, units)
		if upns == 0 {
			uptime = unknownVal
		}
		row := []string{
			fmtDaemonID(ds.Snode.ID(), smap, ds.Status),
			memUsed,
			memAvail,
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
func newTableTargets(ts StStMap, smap *cluster.Smap, units string) *Table {
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
			{name: colCPUUsed},
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
			row := []string{
				fmtDaemonID(ds.Snode.ID(), smap, ds.Status),
				unknownVal,
				unknownVal,
				unknownVal,
				unknownVal,
				unknownVal,
				unknownVal,
				unknownVal,
				ds.K8sPodName,
				ds.Status,
				ds.Version,
				ds.BuildTime,
			}
			table.addRow(row)
			continue
		}

		row := []string{
			fmtDaemonID(ds.Snode.ID(), smap, ds.Status),
			fmt.Sprintf("%.2f%%", ds.MemCPUInfo.PctMemUsed),
			FmtSize(int64(ds.MemCPUInfo.MemAvail), units, 2),
			fmt.Sprintf("%d%%", ds.TargetCDF.PctAvg),
			FmtSize(int64(calcCap(ds)), units, 3),
			fmt.Sprintf("%.2f%%", ds.MemCPUInfo.PctCPUUsed),
			fmtRebStatus(ds.RebSnap),
			fmtDuration(ds.Tracker[stats.Uptime].Value, units),
			ds.K8sPodName,
			ds.Status,
			ds.Version,
			ds.BuildTime,
		}
		table.addRow(row)
	}
	return table
}
