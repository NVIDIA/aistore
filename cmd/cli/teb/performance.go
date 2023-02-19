// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/stats"
)

type PerfTabCtx struct {
	Smap    *cluster.Smap
	Sid     string         // single target, unless ""
	Metrics cos.StrKVs     // metric (aka stats) names and kinds
	Regex   *regexp.Regexp // filter column names (case-insensitive)
	Units   string         // IEC, SI, raw
	AllCols bool           // show all-zero columns
	AvgSize bool           // compute average size on the fly (and show it), e.g.: `get.size/get.n`
}

func NewPerformanceTab(st StatsAndStatusMap, c *PerfTabCtx) (*Table, error) {
	var n2n cos.StrKVs // name of the KindSize metric => it's cumulative counter counterpart
	if c.AvgSize {
		n2n = make(cos.StrKVs, 2)
	}

	// 1. columns
	cols := make([]*header, 0, 32)
	for tid, ds := range st {
		debug.Assert(ds.Status != "", tid+" has no status")
		// skip
		if ds.Status != NodeOnline {
			continue // maintenance mode et al. (see `cli._status`)
		}
		if ds.Tracker == nil { // (unlikely)
			debug.Assert(false, tid)
			return nil, fmt.Errorf("missing stats from %s, please try again later", fmtDaemonID(tid, c.Smap))
		}

		// statically
		cols = append(cols, &header{name: colTarget, hide: false})
		// for subset of metrics selected by caller
		for name := range ds.Tracker {
			if _, ok := c.Metrics[name]; ok {
				cols = append(cols, &header{name: name, hide: false})
			}
		}
		// only once
		break
	}

	// 2. exclude zero columns unless (--all) requested
	if !c.AllCols {
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

	// 4. add STATUS column unless all nodes are online (`NodeOnline`)
	cols = _addStatus(cols, st)

	// 5. convert metric to column names
	printedColumns := _metricsToColNames(cols, c.Metrics, n2n)

	// 6. regex to filter columns, if spec-ed
	if c.Regex != nil {
		cols, printedColumns = _filter(cols, printedColumns, c.Regex)
	}

	// 7. apply color
	for i := range cols {
		if isErrCol(cols[i].name) {
			printedColumns[i].name = fred("\t%s", printedColumns[i].name)
		}
	}

	// 8. sort targets by IDs
	tids := st.sortedSIDs()

	// 9. construct an empty table
	table := newTable(printedColumns...)

	// 10. finally, add rows
	for _, tid := range tids {
		ds := st[tid]
		if c.Sid != "" && c.Sid != tid {
			continue
		}
		row := make([]string, 0, len(cols))
		row = append(row, fmtDaemonID(tid, c.Smap, ds.Status))
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
			v, ok := ds.Tracker[h.name]
			debug.Assert(ok, h.name)
			kind, ok := c.Metrics[h.name]
			debug.Assert(ok, h.name)
			printedValue := FmtStatValue(h.name, kind, v.Value, c.Units)

			// add some color
			if isErrCol(h.name) {
				printedValue = fred("\t%s", printedValue)
			} else if kind == stats.KindSize && c.AvgSize {
				if v, ok := _compAvgSize(ds, h.name, v.Value, n2n); ok {
					printedValue += "  " + FmtStatValue("", kind, v, c.Units)
				}
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
func _zerout(cols []*header, st StatsAndStatusMap) []*header {
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
			if v, ok := ds.Tracker[h.name]; ok && v.Value != 0 {
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

// (aternatively, could always add, conditionally hide)
func _addStatus(cols []*header, st StatsAndStatusMap) []*header {
	for _, ds := range st {
		if ds.Status != NodeOnline {
			cols = append(cols, &header{name: colStatus, hide: false})
			break
		}
	}
	return cols
}

// convert metric names to column names
// NOTE hard-coded translation constants `.lst` => LIST, et al.
// for naming conventions, see "naming" or "convention" under stats/*
func _metricsToColNames(cols []*header, metrics, n2n cos.StrKVs) (printedColumns []*header) {
	printedColumns = make([]*header, len(cols))
	for colIdx, h := range cols {
		var (
			name  string // resulting column name
			kind  string
			ok    bool
			parts = strings.Split(h.name, ".")
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
		for j := 1; j < len(parts)-1; j++ {
			name += "-" + strings.ToUpper(parts[j])
		}

		// suffix
		switch {
		case kind == stats.KindThroughput || kind == stats.KindComputedThroughput:
			name += "(bw)"
		case kind == stats.KindLatency:
			name += "(latency)"
		case kind == stats.KindSize:
			if n2n != nil && _present(cols, metrics, h.name, n2n) {
				name += "(cumulative, average)"
			} else {
				name += "(size)"
			}
		}
	add:
		printedColumns[colIdx] = &header{name: name, hide: cols[colIdx].hide}
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

//
// averaging (cumulative-size/cumulative-count)
//

func _present(cols []*header, metrics cos.StrKVs, ns string, n2n cos.StrKVs) bool {
	i := strings.LastIndexByte(ns, '.')
	if i < 0 {
		return false
	}
	nc := ns[:i] + ".n"
	for _, h := range cols {
		if h.name == nc && metrics[h.name] == stats.KindCounter {
			n2n[ns] = nc
			return true
		}
	}
	return false
}

// where ns and vs are cumulative size name and value, respectively
func _compAvgSize(ds *stats.NodeStatus, ns string, vs int64, n2n cos.StrKVs) (avg int64, ok bool) {
	var nc string // cumulative count
	if nc, ok = n2n[ns]; !ok {
		return
	}
	ok = false
	if vc, exists := ds.Tracker[nc]; exists {
		avg, ok = cos.DivRound(vs, vc.Value), true
	}
	return
}
