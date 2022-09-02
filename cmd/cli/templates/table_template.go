// Package templates provides the set of templates used to format output for the CLI.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package templates

import (
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/stats"
)

const (
	sepa = "\t "

	headerProxy     = "PROXY"
	headerTarget    = "TARGET"
	headerMemUsed   = "MEM USED(%)"
	headerMemAvail  = "MEM AVAIL"
	headerCapUsed   = "CAP USED(%)"
	headerCapAvail  = "CAP AVAIL"
	headerCPUUsed   = "CPU USED(%)"
	headerRebalance = "REBALANCE"
	headerUptime    = "UPTIME"
	headerStatus    = "STATUS"
	headerVersion   = "VERSION"
	headerBuildTime = "BUILD TIME"
	headerPodName   = "K8s POD"
)

type (
	header struct {
		name string
		hide bool
	}

	row []string

	TemplateTable struct {
		headers []*header
		rows    []row
	}

	NodeTableArgs struct {
		Verbose        bool
		HideDeployment bool
	}
)

func newTemplateTable(headers ...*header) *TemplateTable {
	return &TemplateTable{
		headers: headers,
	}
}

func (t *TemplateTable) addRows(rows ...row) error {
	for _, row := range rows {
		if len(row) != len(t.headers) {
			return fmt.Errorf("invalid row: expected %d values, got %d", len(t.headers), len(row))
		}
		t.rows = append(t.rows, rows...)
	}
	return nil
}

func (t *TemplateTable) Template(hideHeader bool) string {
	sb := strings.Builder{}

	if !hideHeader {
		headers := make([]string, 0, len(t.headers))
		for _, header := range t.headers {
			if !header.hide {
				headers = append(headers, header.name)
			}
		}
		sb.WriteString(strings.Join(headers, sepa))
		sb.WriteRune('\n')
	}

	for _, row := range t.rows {
		rowStrings := make([]string, 0, len(row))
		for i, value := range row {
			if !t.headers[i].hide {
				rowStrings = append(rowStrings, value)
			}
		}
		sb.WriteString(strings.Join(rowStrings, sepa))
		sb.WriteRune('\n')
	}

	return sb.String()
}

// Proxies table

func NewProxyTable(proxyStats *stats.DaemonStatus, smap *cluster.Smap) *TemplateTable {
	return newTableProxies(stats.DaemonStatusMap{proxyStats.Snode.ID(): proxyStats}, smap)
}

func NewProxiesTable(ds *DaemonStatusTemplateHelper, smap *cluster.Smap) *TemplateTable {
	return newTableProxies(ds.Pmap, smap)
}

func newTableProxies(ps stats.DaemonStatusMap, smap *cluster.Smap) *TemplateTable {
	var (
		h        = DaemonStatusTemplateHelper{Pmap: ps}
		pods     = h.pods()
		status   = h.onlineStatus()
		versions = h.versions()
		headers  = []*header{
			{name: headerProxy},
			{name: headerMemUsed},
			{name: headerMemAvail},
			{name: headerUptime},
			{name: headerPodName, hide: len(pods) == 1 && pods[0] == ""},
			{name: headerStatus, hide: len(status) == 1 && status[0] == api.StatusOnline && len(ps) > 1},
			{name: headerVersion, hide: len(versions) == 1 && len(ps) > 1},
			{name: headerBuildTime, hide: len(versions) == 1 && len(ps) > 1}, // intended
		}
		table = newTemplateTable(headers...)
	)
	for _, status := range ps {
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
			fmtDaemonID(status.Snode.ID(), *smap),
			memUsed,
			memAvail,
			uptime,
			status.K8sPodName,
			status.Status,
			status.Version,
			status.BuildTime,
		}
		debug.AssertNoErr(table.addRows(row))
	}
	return table
}

// Targets table

func NewTargetTable(targetStats *stats.DaemonStatus) *TemplateTable {
	return newTableTargets(stats.DaemonStatusMap{targetStats.Snode.ID(): targetStats})
}

func NewTargetsTable(ds *DaemonStatusTemplateHelper) *TemplateTable {
	return newTableTargets(ds.Tmap)
}

func newTableTargets(ts stats.DaemonStatusMap) *TemplateTable {
	var (
		h        = DaemonStatusTemplateHelper{Tmap: ts}
		pods     = h.pods()
		status   = h.onlineStatus()
		versions = h.versions()
		headers  = []*header{
			{name: headerTarget},
			{name: headerMemUsed},
			{name: headerMemAvail},
			{name: headerCapUsed},
			{name: headerCapAvail},
			{name: headerCPUUsed},
			{name: headerRebalance, hide: len(h.rebalance()) == 0},
			{name: headerUptime},
			{name: headerPodName, hide: len(pods) == 1 && pods[0] == ""},
			{name: headerStatus, hide: len(status) == 1 && status[0] == api.StatusOnline && len(ts) > 1},
			{name: headerVersion, hide: len(versions) == 1 && len(ts) > 1},
			{name: headerBuildTime, hide: len(versions) == 1 && len(ts) > 1}, // intended
		}
		table = newTemplateTable(headers...)
	)
	for _, status := range ts {
		row := []string{
			status.Snode.ID(),
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
		debug.AssertNoErr(table.addRows(row))
	}
	return table
}
