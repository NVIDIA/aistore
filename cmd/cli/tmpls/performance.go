// Package tmpls provides the set of templates used to format output for the CLI.
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package tmpls

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/stats"
	"github.com/fatih/color"
)

var fred = color.New(color.FgHiRed)

func NewPerformanceTab(st stats.DaemonStatusMap, smap *cluster.Smap, sid string, metrics cos.StrKVs, regex *regexp.Regexp,
	allCols bool) (*Table, error) {
	// 1. columns
	cols := make([]*header, 0, 32)
	for tid, ds := range st {
		debug.Assert(ds.Status != "", tid+" has no status")
		// skip
		if ds.Status != NodeOnline {
			continue // maintenance mode et al. (see `cli._status`)
		}
		if ds.Stats == nil { // (unlikely)
			debug.Assert(false, tid)
			return nil, fmt.Errorf("missing stats from %s, please try again later", fmtDaemonID(tid, smap))
		}

		// statically
		cols = append(cols, &header{name: colTarget, hide: false})
		// selected by caller
		for name := range ds.Stats.Tracker {
			if _, ok := metrics[name]; ok {
				cols = append(cols, &header{name: name, hide: false})
			}
		}
		// only once
		break
	}

	// 2. exclude zero columns unless (--all) requested
	if !allCols {
		cols = _zerout(cols, st)
	}

	// 3. sort (remaining) columns and shift `err-*` columns to the right
	sort.Slice(cols, func(i, j int) bool {
		switch {
		case isErrCol(cols[i].name) && isErrCol(cols[j].name):
			return cols[i].name < cols[j].name
		case isErrCol(cols[i].name) && !isErrCol(cols[j].name):
			return false
		case !isErrCol(cols[i].name) && isErrCol(cols[j].name):
			return true
		default:
			return cols[i].name < cols[j].name
		}
	})

	// 4. add STATUS iff thetre's at least one node that isn't "online"
	cols = _addStatus(cols, st)

	// 5. convert metric to column names
	printedColumns := _rename(cols, metrics)

	// 6. regex to filter columns, if spec-ed
	if regex != nil {
		cols, printedColumns = _filter(cols, printedColumns, regex)
	}

	// 7. apply color
	for i := range cols {
		if isErrCol(cols[i].name) {
			printedColumns[i].name = fred.Sprintf("\t%s", printedColumns[i].name)
		}
	}

	// 8. sort targets
	tids := statusMap2SortedNodes(st)

	// 9. construct empty table
	table := newTable(printedColumns...)

	// and finally,
	// 10. add rows
	for _, tid := range tids {
		ds := st[tid]
		if sid != "" && sid != tid {
			continue
		}
		row := make([]string, 0, len(cols))
		row = append(row, fmtDaemonID(tid, smap, ds.Status))
		for _, h := range cols[1:] {
			if h.name == colStatus {
				row = append(row, ds.Status)
				continue
			}
			if ds.Status != NodeOnline {
				row = append(row, unknownVal)
				continue
			}

			// format value
			var (
				printedValue string
				v, ok1       = ds.Stats.Tracker[h.name]
				kind, ok2    = metrics[h.name]
			)
			debug.Assert(ok1, h.name)
			debug.Assert(ok2, h.name)
			switch {
			case kind == stats.KindThroughput || kind == stats.KindComputedThroughput:
				printedValue = cos.B2S(v.Value, 2) + "/s"
			case kind == stats.KindLatency:
				printedValue = fmtDuration(v.Value)
			case strings.HasSuffix(h.name, ".size"):
				printedValue = cos.B2S(v.Value, 2)
			default:
				printedValue = fmt.Sprintf("%d", v.Value)
			}
			// add some color
			if isErrCol(h.name) {
				printedValue = fred.Sprintf("\t%s", printedValue)
			}
			row = append(row, printedValue)
		}
		table.addRow(row)
	}
	return table, nil
}

//
// utils/helpers
//

func isErrCol(colName string) bool { return strings.HasPrefix(colName, stats.ErrPrefix) }

// remove all-zeros columns
func _zerout(cols []*header, st stats.DaemonStatusMap) []*header {
	for i := 0; i < len(cols); i++ {
		var found bool
		h := cols[i]
		if h.name == colTarget {
			continue
		}
		for _, ds := range st {
			if ds.Status != NodeOnline {
				continue
			}
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

func _addStatus(cols []*header, st stats.DaemonStatusMap) []*header {
	for _, ds := range st {
		if ds.Status != NodeOnline {
			cols = append(cols, &header{name: colStatus, hide: false})
			break
		}
	}
	return cols
}

// convert metric names to column names
// (for naming conventions, lookup "naming" and "convention" in stats/*)
func _rename(cols []*header, metrics cos.StrKVs) (printedColumns []*header) {
	printedColumns = make([]*header, len(cols))
	for i, h := range cols {
		var (
			name         string
			kind, suffix string
			ok           bool
			parts        = strings.Split(h.name, ".")
		)
		if h.name == colTarget || h.name == colStatus {
			name = h.name
			goto add
		}
		kind, ok = metrics[h.name]
		debug.Assert(ok, h.name)
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
		// middle name (is every name in-between)
		for i := 1; i < len(parts)-1; i++ {
			name += "-" + strings.ToUpper(parts[i])
		}

		// suffix
		suffix = parts[len(parts)-1]
		switch {
		case kind == stats.KindThroughput || kind == stats.KindComputedThroughput:
			name += " (bw)"
		case kind == stats.KindLatency:
			name += " (latency)"
		case suffix == "size":
			name += "-" + strings.ToUpper(suffix)
		}
	add:
		printedColumns[i] = &header{name: name, hide: cols[i].hide}
	}

	return
}

func _filter(cols, printedColumns []*header, regex *regexp.Regexp) ([]*header, []*header) {
	for i := 0; i < len(cols); i++ {
		if cols[i].name == colTarget || cols[i].name == colStatus { // aren't subject to filtering
			continue
		}
		if regex.MatchString(cols[i].name) || regex.MatchString(printedColumns[i].name) {
			continue
		}
		// shift
		copy(cols[i:], cols[i+1:])
		cols = cols[:len(cols)-1]
		copy(printedColumns[i:], printedColumns[i+1:])
		printedColumns = printedColumns[:len(printedColumns)-1]
		i--
	}
	return cols, printedColumns
}
