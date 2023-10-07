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

	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/stats"
)

type PerfTabCtx struct {
	Smap      *meta.Smap
	Sid       string           // single target, unless ""
	Metrics   cos.StrKVs       // metric (aka stats) names and kinds
	Regex     *regexp.Regexp   // filter column names (case-insensitive)
	Units     string           // IEC, SI, raw
	Totals    map[string]int64 // metrics to sum up (name => sum(column)), where the name is IN and the sum is OUT
	TotalsHdr string
	AvgSize   bool // compute average size on the fly (and show it), e.g.: `get.size/get.n`
	Idle      bool // currently idle
}

func NewPerformanceTab(st StstMap, c *PerfTabCtx) (*Table, int /*numNZ non-zero metrics OR bad status*/, error) {
	var (
		numNZ int        // num non-zero metrics
		numTs int        // num active targets in `st`
		n2n   cos.StrKVs // name of the KindSize metric => it's cumulative counter counterpart
	)
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
			err := fmt.Errorf("missing stats from %s, please try again later", fmtDaemonID(tid, c.Smap, ""))
			return nil, 0, err
		}

		// statically, once
		if len(cols) == 0 {
			cols = append(cols, &header{name: colTarget, hide: false})
		}
		// for the subset of caller selected metrics
	outer:
		for name := range ds.Tracker {
			if _, ok := c.Metrics[name]; ok {
				for _, col := range cols {
					if col.name == name {
						continue outer // already added
					}
				}
				cols = append(cols, &header{name: name, hide: false})
			}
		}
	}

	// 2. exclude zero columns unless requested specific match or (--all)
	if c.Regex == nil {
		cols = _zerout(cols, st)
	}

	// 3. sort (remaining) columns and shift `err-*` columns to the right
	sort.Slice(cols, func(i, j int) bool {
		switch {
		case stats.IsErrMetric(cols[i].name) && stats.IsErrMetric(cols[j].name):
			return cols[i].name < cols[j].name
		case stats.IsErrMetric(cols[i].name) && !stats.IsErrMetric(cols[j].name):
			return false
		case !stats.IsErrMetric(cols[i].name) && stats.IsErrMetric(cols[j].name):
			return true
		default:
			return cols[i].name < cols[j].name
		}
	})

	// 4. add STATUS column unless all nodes are online (`NodeOnline`)
	cols = _addStatus(cols, st)

	// 5. add regex-specified (ie, user-requested) metrics that are missing
	// ------ api.GetStatsAndStatus() does not return zero counters -------
	if c.Regex != nil {
		cols = _addMissingMatchingMetrics(cols, c.Metrics, c.Regex)
	}

	// 6. convert metric to column names
	printedColumns := _metricsToColNames(cols, c.Metrics, n2n)

	// 7. regex to filter columns, if spec-ed
	if c.Regex != nil {
		cols, printedColumns = _filter(cols, printedColumns, c.Regex)
	}

	// 8. apply color
	for i := range cols {
		if stats.IsErrMetric(cols[i].name) {
			printedColumns[i].name = fred("\t%s", printedColumns[i].name)
		}
	}

	// 9. sort targets by IDs
	tids := st.sortedSIDs()

	// 10. construct an empty table
	table := newTable(printedColumns...)

	// 11. finally, add rows
	for _, tid := range tids {
		ds := st[tid]
		if c.Sid != "" && c.Sid != tid {
			continue
		}
		if ds.Status == NodeOnline {
			numTs++
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
				numNZ++
				continue
			}

			// format value
			v, ok := ds.Tracker[h.name]
			debug.Assert(ok, h.name)
			kind, ok := c.Metrics[h.name]
			debug.Assert(ok, h.name)
			printedValue := FmtStatValue(h.name, kind, v.Value, c.Units)

			if v.Value != 0 {
				numNZ++
			}

			// add some color
			if stats.IsErrMetric(h.name) {
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

	if !c.Idle && (c.Totals == nil || numTs <= 1) { // but always show 'idle'
		return table, numNZ, nil
	}

	// tally up
	var (
		row   = make([]string, 0, len(cols))
		added bool
	)
	row = append(row, c.TotalsHdr)
	for _, h := range cols[1:] {
		if h.name == colStatus {
			row = append(row, "")
			continue
		}
		if c.Idle || numNZ == 0 {
			if !added {
				row = append(row, fgreen("idle"))
				added = true
			} else {
				row = append(row, "")
			}
			continue
		}
		val, ok := c.Totals[h.name]
		if ok {
			kind, ok := c.Metrics[h.name]
			debug.Assert(ok, h.name)
			printedValue := FmtStatValue(h.name, kind, val, c.Units)
			row = append(row, printedValue)
		} else {
			row = append(row, "")
		}
	}
	table.addRow(row)
	return table, numNZ, nil
}

//
// utils/helpers
//

// remove all-zeros columns
func _zerout(cols []*header, st StstMap) []*header {
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
func _addStatus(cols []*header, st StstMap) []*header {
	for _, ds := range st {
		if ds.Status != NodeOnline {
			cols = append(cols, &header{name: colStatus, hide: false})
			break
		}
	}
	return cols
}

func _addMissingMatchingMetrics(cols []*header, metrics cos.StrKVs, regex *regexp.Regexp) []*header {
outer:
	for name := range metrics {
		for _, h := range cols {
			if h.name == name {
				continue outer // already added (non-zero via API call)
			}
		}

		// regex matching (user-visible, "printed") column names
		printedName := _metricToPrintedColName(name, cols, metrics, nil)
		if !regex.MatchString(name) && !regex.MatchString(printedName) {
			lower := strings.ToLower(printedName)
			if !regex.MatchString(lower) {
				continue
			}
		}

		cols = append(cols, &header{name: name, hide: false})
	}
	return cols
}

// convert metric names to column names
// NOTE hard-coded translation constants `.lst` => LIST, et al.
// for naming conventions, see "naming" or "convention" under stats/*
func _metricsToColNames(cols []*header, metrics, n2n cos.StrKVs) (printedColumns []*header) {
	printedColumns = make([]*header, len(cols)) // one to one
	for colIdx, h := range cols {
		var printedName string
		if h.name == colTarget || h.name == colStatus {
			printedName = h.name
		} else {
			printedName = _metricToPrintedColName(h.name, cols, metrics, n2n)
		}
		printedColumns[colIdx] = &header{name: printedName, hide: cols[colIdx].hide}
	}
	return
}

func _metricToPrintedColName(mname string, cols []*header, metrics, n2n cos.StrKVs) (printedName string) {
	kind, ok := metrics[mname]
	debug.Assert(ok, mname)
	parts := strings.Split(mname, ".")

	// first name
	switch parts[0] {
	case "lru":
		copy(parts[0:], parts[1:])
		parts = parts[:len(parts)-1]
		printedName = strings.ToUpper(parts[0])
	case "lst":
		printedName = "LIST"
	case "del":
		printedName = "DELETE"
	case "ren":
		printedName = "RENAME"
	case "ver":
		printedName = "VERSION"
	default:
		printedName = strings.ToUpper(parts[0])
	}

	// middle name (is every name in-between)
	for j := 1; j < len(parts)-1; j++ {
		printedName += "-" + strings.ToUpper(parts[j])
	}

	// suffix
	switch {
	case kind == stats.KindThroughput || kind == stats.KindComputedThroughput:
		printedName += "(bw)"
	case kind == stats.KindLatency:
		printedName += "(latency)"
	case kind == stats.KindSize:
		if n2n != nil && _present(cols, metrics, mname, n2n) {
			printedName += "(cumulative, average)"
		} else {
			printedName += "(size)"
		}
	}
	return
}

func _filter(cols, printedColumns []*header, regex *regexp.Regexp) ([]*header, []*header) {
	for i := 0; i < len(cols); i++ {
		if cols[i].name == colTarget || cols[i].name == colStatus { // aren't subject to filtering
			continue
		}

		lower := strings.ToLower(printedColumns[i].name)
		if regex.MatchString(cols[i].name) || regex.MatchString(printedColumns[i].name) || regex.MatchString(lower) {
			continue // keep
		}

		// shift to filter out
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
