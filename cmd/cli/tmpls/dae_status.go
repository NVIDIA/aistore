// Package tmpls provides the set of templates used to format output for the CLI.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package tmpls

import (
	"fmt"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
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

func NewDaeStatus(st *stats.DaemonStatus, smap *cluster.Smap, daeType string) *Table {
	switch daeType {
	case apc.Proxy:
		return newTableProxies(stats.DaemonStatusMap{st.Snode.ID(): st}, smap)
	case apc.Target:
		return newTableTargets(stats.DaemonStatusMap{st.Snode.ID(): st}, smap)
	default:
		debug.Assert(false)
		return nil
	}
}

func NewDaeMapStatus(ds *DaemonStatusTemplateHelper, smap *cluster.Smap, daeType string) *Table {
	switch daeType {
	case apc.Proxy:
		return newTableProxies(ds.Pmap, smap)
	case apc.Target:
		return newTableTargets(ds.Tmap, smap)
	default:
		debug.Assert(false)
		return nil
	}
}

// proxy(ies)
func newTableProxies(ps stats.DaemonStatusMap, smap *cluster.Smap) *Table {
	var (
		h        = DaemonStatusTemplateHelper{Pmap: ps}
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

	ids := statusMap2SortedNodes(ps)

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
		memAvail := cos.UnsignedB2S(ds.MemCPUInfo.MemAvail, 2)
		if ds.MemCPUInfo.MemAvail == 0 {
			memAvail = unknownVal
		}
		upns := ds.Tracker[stats.Uptime].Value
		uptime := fmtDuration(upns)
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
func newTableTargets(ts stats.DaemonStatusMap, smap *cluster.Smap) *Table {
	var (
		h        = DaemonStatusTemplateHelper{Tmap: ts}
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
	ids := statusMap2SortedNodes(ts)

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
			cos.UnsignedB2S(ds.MemCPUInfo.MemAvail, 2),
			fmt.Sprintf("%.2f%%", calcCapPercentage(ds)),
			cos.UnsignedB2S(calcCap(ds), 3),
			fmt.Sprintf("%.2f%%", ds.MemCPUInfo.PctCPUUsed),
			fmtRebStatus(ds.RebSnap),
			fmtDuration(ds.Tracker[stats.Uptime].Value),
			ds.K8sPodName,
			ds.Status,
			ds.Version,
			ds.BuildTime,
		}
		table.addRow(row)
	}
	return table
}
