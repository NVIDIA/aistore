// Package templates provides the set of templates used to format output for the CLI.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package templates

import (
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/stats"
)

const (
	sepa = "\t "

	headerProxy      = "PROXY"
	headerTarget     = "TARGET"
	headerDeployment = "DEPLOYMENT"
	headerMemUsed    = "MEM USED %"
	headerMemAvail   = "MEM AVAIL"
	headerCapUsed    = "CAP USED %"
	headerCapAvail   = "CAP AVAIL"
	headerCPUUsed    = "CPU USED %"
	headerRebalance  = "REBALANCE"
	headerUptime     = "UPTIME"
	headerStatus     = "STATUS"
	headerVersion    = "VERSION"
	headerBuildTime  = "BUILD TIME"
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
	return newTableProxies(map[string]*stats.DaemonStatus{proxyStats.Snode.ID(): proxyStats}, smap, false, false)
}

func NewProxiesTable(ds *DaemonStatusTemplateHelper, smap *cluster.Smap, onlyProxies, verbose bool) *TemplateTable {
	deployments := daemonsDeployments(ds.Pmap)
	if !onlyProxies {
		deployments.Add(daemonsDeployments(ds.Tmap).ToSlice()...)
	}

	hideDeployments := !verbose && len(deployments) <= 1
	return newTableProxies(ds.Pmap, smap, hideDeployments, len(ds.Pmap) > 1 && allNodesOnline(ds.Pmap))
}

func newTableProxies(ps map[string]*stats.DaemonStatus, smap *cluster.Smap, hideDeployments, hideStatus bool) *TemplateTable {
	headers := []*header{
		{name: headerProxy},
		{name: headerDeployment, hide: hideDeployments},
		{name: headerMemUsed},
		{name: headerMemAvail},
		{name: headerUptime},
		{name: headerStatus, hide: hideStatus},
		{name: headerVersion},
		{name: headerBuildTime},
	}

	table := newTemplateTable(headers...)
	for _, status := range ps {
		row := []string{
			fmtDaemonID(status.Snode.ID(), *smap),
			status.DeployedOn,
			fmt.Sprintf("%.2f%%", status.SysInfo.PctMemUsed),
			cos.UnsignedB2S(status.SysInfo.MemAvail, 2),
			fmtDuration(extractStat(status.Stats, "up.ns.time")),
			status.Status,
			status.Version,
			status.BuildTime,
		}
		cos.AssertNoErr(table.addRows(row))
	}
	return table
}

// Targets table

func NewTargetTable(targetStats *stats.DaemonStatus) *TemplateTable {
	return newTableTargets(map[string]*stats.DaemonStatus{targetStats.Snode.ID(): targetStats}, false, false, true)
}

func NewTargetsTable(ds *DaemonStatusTemplateHelper, onlyTargets, verbose bool) *TemplateTable {
	deployments := daemonsDeployments(ds.Tmap)
	if !onlyTargets {
		deployments.Add(daemonsDeployments(ds.Pmap).ToSlice()...)
	}

	hideDeployments := !verbose && len(deployments) <= 1
	return newTableTargets(ds.Tmap, hideDeployments, len(ds.Tmap) > 1 && allNodesOnline(ds.Tmap), !verbose)
}

func newTableTargets(ts map[string]*stats.DaemonStatus, hideDeployments, hideStatus, hideBuildTime bool) *TemplateTable {
	headers := []*header{
		{name: headerTarget},
		{name: headerDeployment, hide: hideDeployments},
		{name: headerMemUsed},
		{name: headerMemAvail},
		{name: headerCapUsed},
		{name: headerCapAvail},
		{name: headerCPUUsed},
		{name: headerRebalance},
		{name: headerUptime},
		{name: headerStatus, hide: hideStatus},
		{name: headerVersion},
		{name: headerBuildTime, hide: hideBuildTime},
	}

	table := newTemplateTable(headers...)
	for _, status := range ts {
		row := []string{
			status.Snode.ID(),
			status.DeployedOn,
			fmt.Sprintf("%.2f%%", status.SysInfo.PctMemUsed),
			cos.UnsignedB2S(status.SysInfo.MemAvail, 2),
			fmt.Sprintf("%.2f%%", calcCapPercentage(status)),
			cos.UnsignedB2S(calcCap(status), 3),
			fmt.Sprintf("%.2f%%", status.SysInfo.PctCPUUsed),
			fmtXactStatus(status.TStatus),
			fmtDuration(extractStat(status.Stats, "up.ns.time")),
			status.Status,
			status.Version,
			status.BuildTime,
		}
		cos.AssertNoErr(table.addRows(row))
	}
	return table
}
