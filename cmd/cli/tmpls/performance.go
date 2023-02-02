// Package tmpls provides the set of templates used to format output for the CLI.
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package tmpls

import (
	"fmt"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
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

func NewCountersTab(st stats.DaemonStatusMap, smap *cluster.Smap, metrics cos.StrKVs, showZeroCols bool) *Table {
	cols := make([]*header, 0, 32)

	// 1. dynamically: counter names
	for _, ds := range st {
		cols = append(cols, &header{name: colTarget, hide: false})
		for name := range ds.Stats.Tracker {
			if metrics[name] == stats.KindCounter {
				cols = append(cols, &header{name: name, hide: false})
			}
		}
		break
	}

	// 2. exclude zero columns unless requested
	if !showZeroCols {
		cols = _zerout(cols, st)
	}

	// 3. sort remaining columns
	sort.Slice(cols, func(i, j int) bool {
		return cols[i].name < cols[j].name
	})

	// 4. convert metric to column names
	printedColumns := _rename(cols)

	table := newTable(printedColumns...)

	// 4. rows of values
	for tid, ds := range st {
		row := make([]string, 0, len(cols))
		row = append(row, fmtDaemonID(tid, smap))
		for _, h := range cols[1:] {
			if v, ok := ds.Stats.Tracker[h.name]; ok {
				if strings.HasSuffix(h.name, ".size") {
					row = append(row, cos.B2S(v.Value, 2))
				} else {
					row = append(row, fmt.Sprintf("%d", v.Value))
				}
				continue
			}
			debug.Assert(false, h.name)
			row = append(row, unknownVal)
		}
		table.addRow(row)
	}
	return table
}

//
// utils/helpers
//

// remove all-zeros columns
func _zerout(cols []*header, st stats.DaemonStatusMap) []*header {
	for i := 0; i < len(cols); i++ {
		var found bool
		h := cols[i]
		if h.name == colTarget {
			continue
		}
		for _, ds := range st {
			if v, ok := ds.Stats.Tracker[h.name]; ok && v.Value != 0 {
				found = true
				break
			}
		}
		if !found {
			copy(cols[i:], cols[i+1:])
			cols = cols[:len(cols)-1]
			i--
		}
	}
	return cols
}

// convert metric names to column names
// (for naming conventions, lookup "naming" and "convention" in stats/*)
func _rename(cols []*header) (printedColumns []*header) {
	printedColumns = make([]*header, len(cols))
	for i, h := range cols {
		var (
			name  string
			parts = strings.Split(h.name, ".")
		)
		// first name
		switch parts[0] {
		case "lru":
			copy(parts[0:], parts[1:])
			parts = parts[:len(parts)-1]
			name = strings.ToUpper(parts[0])
		case "lst":
			name = "LIST"
		case "del":
			name = "DELETE"
		case "ren":
			name = "RENAME"
		case "vchange":
			name = "VOL-CHANGE"
		default:
			name = strings.ToUpper(parts[0])
		}
		// middle name
		if len(parts) == 3 {
			name += "-" + strings.ToUpper(parts[1])
		}
		// suffix
		if suffix := parts[len(parts)-1]; suffix == "size" {
			name += "-" + strings.ToUpper(suffix)
		}

		printedColumns[i] = &header{name: name, hide: cols[i].hide}
	}

	return
}
