// Package tmpls provides the set of templates used to format output for the CLI.
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package tmpls

import (
	"fmt"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/stats"
)

// table 1: non-zero counters and sizes
const (
	colGET = "GET"
	colPUT = "PUT"
)

// table 2: throughput
const (
	colGETbps    = "GET"
	colDiskRead  = "READ (min, avg, max)"
	colDiskWrite = "WRITE (min, avg, max)"
	colDiskUtil  = "UTIL (min, avg, max)"
)

// table 3: latency
const (
// colGETns = "GET"
)

// table 4:  GET-performance
const (
// include colGET, colGETbps, and colGETns from above
)

// table 5:  Memory, CPU, Capacity, Uptime, Jobs
const (
// include:
// colMemUsed   = "MEM USED(%)"
// colMemAvail  = "MEM AVAIL"
// colCapUsed   = "CAP USED(%)"
// colCapAvail  = "CAP AVAIL"
// colCPUUsed   = "CPU USED(%)"
// colRebalance = "REBALANCE"
// colUptime    = "UPTIME"
)

// TODO -- FIXME: looks like stats.DaemonStatusMap is a superset that includes stats.ClusterStats
// TODO -- FIXME: looks like I need to switch gears to fillNodeStatusMap and
// TODO -- FIXME: likely need a helper to omit a column that contains only zero values

func NewTargetStats(st stats.ClusterStats) *Table {
	var (
		headers = []*header{
			{name: colGET, hide: false},
			{name: colPUT, hide: false},
			{name: colDiskRead, hide: false},
			{name: colDiskWrite, hide: false},
			{name: colDiskUtil, hide: false},
		}
		table = newTable(headers...)
	)
	for tid, tgt := range st.Target {
		kvs := tgt.Tracker
		row := []string{
			cluster.Tname(tid),
			fmt.Sprintf("%d %d", kvs[stats.GetCount], kvs[stats.GetThroughput]),
			fmt.Sprintf("%d", kvs[stats.PutCount]),
		}
		table.addRow(row)
	}
	return table
}
