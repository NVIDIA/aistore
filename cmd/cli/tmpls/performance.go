// Package tmpls provides the set of templates used to format output for the CLI.
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package tmpls

import (
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/stats"
)

// table 1: non-zero counters and sizes
const (
// colGET = "GET"
// colPUT = "PUT"
)

// table 2: throughput
const (
// colGETbps    = "GET"
// colDiskRead  = "READ (min, avg, max)"
// colDiskWrite = "WRITE (min, avg, max)"
// colDiskUtil  = "UTIL (min, avg, max)"
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

// TODO -- FIXME: add helper to omit a column that contains only zero values
// TODO -- FIXME: statsValue.Kind not getting marshaled

func NewCountersTab(st stats.DaemonStatusMap, smap *cluster.Smap) *Table {
	headers := make([]*header, 0, 32)

	// counters' names, dynamically
	for _, ds := range st {
		headers = append(headers, &header{name: colTarget, hide: false})
		for name := range ds.Stats.Tracker {
			// v.Kind stats.KindCounter // TODO -- FIXME
			if strings.HasSuffix(name, ".n") {
				headers = append(headers, &header{name: name, hide: false})
			}
		}
		break
	}

	table := newTable(headers...)

	// rows of values
	for tid, ds := range st {
		row := make([]string, 0, len(headers))
		row = append(row, fmtDaemonID(tid, smap))
		for _, h := range headers[1:] {
			if v, ok := ds.Stats.Tracker[h.name]; ok {
				row = append(row, fmt.Sprintf("%d", v.Value))
			} else {
				debug.Assert(false, h.name)
				row = append(row, unknownVal)
			}
		}
		table.addRow(row)
	}
	return table
}
