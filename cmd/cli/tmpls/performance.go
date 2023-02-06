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

func NewCountersTab(st stats.DaemonStatusMap, smap *cluster.Smap, sid string, metrics cos.StrKVs, regex *regexp.Regexp,
	allCols bool) (*Table, error) {
	// 1. dynamically: counter names
	cols := make([]*header, 0, 32)
	for tid, ds := range st {
		if ds.Stats == nil {
			return nil, fmt.Errorf("missing stats from %s, please try again later", fmtDaemonID(tid, smap))
		}

		// statically
		cols = append(cols, &header{name: colTarget, hide: false})

		for name := range ds.Stats.Tracker {
			if metrics[name] == stats.KindCounter {
				cols = append(cols, &header{name: name, hide: false})
			}
		}
		// just once
		break
	}

	// 2. exclude zero columns unless (--all) requested
	if !allCols {
		cols = _zerout(cols, st)
	}

	// 3. sort (remaining) columns and shift `err-*` to the right
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

	// 4. convert metric to column names
	printedColumns := _rename(cols)

	// 5. regex to filter columns, if spec-ed
	if regex != nil {
		cols, printedColumns = _filter(cols, printedColumns, regex)
	}

	// 6. apply color
	for i := range cols {
		if isErrCol(cols[i].name) {
			printedColumns[i].name = fred.Sprintf("\t%s", printedColumns[i].name)
		}
	}

	// 7. sort targets
	targets := make([]string, 0, len(st))
	for tid := range st {
		targets = append(targets, tid)
	}
	sort.Strings(targets)

	// 8. construct empty table
	table := newTable(printedColumns...)

	// finally: 9. add rows
	for _, tid := range targets {
		ds := st[tid]
		if sid != "" && sid != tid {
			continue
		}
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
					printedValue = fred.Sprintf("\t%s", printedValue)
				}
				row = append(row, printedValue)
				continue
			}
			debug.Assert(false, h.name)
			row = append(row, unknownVal)
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
		if h.name == colTarget {
			name = h.name
			goto add
		}
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
		// middle name: take everything in-between
		for i := 1; i < len(parts)-1; i++ {
			name += "-" + strings.ToUpper(parts[i])
		}

		// suffix
		if suffix := parts[len(parts)-1]; suffix == "size" {
			name += "-" + strings.ToUpper(suffix)
		}
	add:
		printedColumns[i] = &header{name: name, hide: cols[i].hide}
	}

	return
}

func _filter(cols, printedColumns []*header, regex *regexp.Regexp) ([]*header, []*header) {
	for i := 0; i < len(cols); i++ {
		if cols[i].name == colTarget {
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
