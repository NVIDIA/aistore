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
	"github.com/fatih/color"
)

var fred = color.New(color.FgHiRed)

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

	// 5. apply color
	for i := range cols {
		if isErrCol(cols[i].name) {
			printedColumns[i].name = fred.Sprintf("%s", printedColumns[i].name)
		}
	}

	// 6. construct empty table
	table := newTable(printedColumns...)

	// 7. finally, add rows
	for tid, ds := range st {
		row := make([]string, 0, len(cols))
		row = append(row, fmtDaemonID(tid, smap))
		for _, h := range cols[1:] {
			if v, ok := ds.Stats.Tracker[h.name]; ok {
				var printedValue string
				if strings.HasSuffix(h.name, ".size") {
					printedValue = cos.B2S(v.Value, 2)
				} else {
					printedValue = fmt.Sprintf("%d", v.Value)
				}
				if isErrCol(h.name) {
					printedValue = fred.Sprintf("%s", printedValue)
				}
				row = append(row, printedValue)
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

func isErrCol(colName string) bool { return strings.HasPrefix(colName, "err") }

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
		case "ver":
			name = "OBJ-VERSION"
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
