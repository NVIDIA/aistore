// Package tmpls provides the set of templates used to format output for the CLI.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package tmpls

import (
	"fmt"

	"github.com/NVIDIA/aistore/api"
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
			{name: colStatus, hide: len(status) == 1 && status[0] == api.StatusOnline && len(ps) > 1},
			{name: colVersion, hide: len(versions) == 1 && len(ps) > 1},
			{name: colBuildTime, hide: len(versions) == 1 && len(ps) > 1}, // intended
		}
		table = newTable(cols...)
	)

	ids := statusMap2SortedNodes(ps)

	for _, sid := range ids {
		status := ps[sid]
		memUsed := fmt.Sprintf("%.2f%%", status.MemCPUInfo.PctMemUsed)
		if status.MemCPUInfo.PctMemUsed == 0 {
			memUsed = unknownVal
		}
		memAvail := cos.UnsignedB2S(status.MemCPUInfo.MemAvail, 2)
		if status.MemCPUInfo.MemAvail == 0 {
			memAvail = unknownVal
		}
		upns := extractStat(status.Stats, "up.ns.time")
		uptime := fmtDuration(upns)
		if upns == 0 {
			uptime = unknownVal
		}
		row := []string{
			fmtDaemonID(status.Snode.ID(), smap),
			memUsed,
			memAvail,
			uptime,
			status.K8sPodName,
			status.Status,
			status.Version,
			status.BuildTime,
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
			{name: colStatus, hide: len(status) == 1 && status[0] == api.StatusOnline && len(ts) > 1},
			{name: colVersion, hide: len(versions) == 1 && len(ts) > 1},
			{name: colBuildTime, hide: len(versions) == 1 && len(ts) > 1}, // intended
		}
		table = newTable(cols...)
	)
	ids := statusMap2SortedNodes(ts)

	for _, sid := range ids {
		status := ts[sid]
		row := []string{
			fmtDaemonID(status.Snode.ID(), smap),
			fmt.Sprintf("%.2f%%", status.MemCPUInfo.PctMemUsed),
			cos.UnsignedB2S(status.MemCPUInfo.MemAvail, 2),
			fmt.Sprintf("%.2f%%", calcCapPercentage(status)),
			cos.UnsignedB2S(calcCap(status), 3),
			fmt.Sprintf("%.2f%%", status.MemCPUInfo.PctCPUUsed),
			fmtRebStatus(status.RebSnap),
			fmtDuration(extractStat(status.Stats, "up.ns.time")),
			status.K8sPodName,
			status.Status,
			status.Version,
			status.BuildTime,
		}
		table.addRow(row)
	}
	return table
}
