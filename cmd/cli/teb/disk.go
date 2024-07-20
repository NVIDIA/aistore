// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"regexp"
	"strings"

	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
)

const (
	colDisk     = "DISK"
	colRead     = "READ"
	colReadAvg  = "READ(avg size)"
	colWrite    = "WRITE"
	colWriteAvg = "WRITE(avg size)"
	colUtil     = "UTIL(%)"
)

func NewDiskTab(dsh []*DiskStatsHelper, smap *meta.Smap, regex *regexp.Regexp, units, totalsHdr string, withCap bool) *Table {
	// 1. columns
	cols := []*header{
		{name: colTarget},
		{name: colDisk},
		{name: colRead},
		{name: colReadAvg},
		{name: colWrite},
		{name: colWriteAvg},
		{name: colUtil},
	}
	if withCap {
		cols = append(cols, &header{name: colCapUsed}, &header{name: colCapAvail})
	}
	if regex != nil {
		cols = _flt(cols, regex)
	}
	table := newTable(cols...)
	for _, ds := range dsh {
		row := make([]string, 0, len(cols))
		if ds.TargetID == totalsHdr {
			row = append(row, totalsHdr)
		} else {
			row = append(row, fmtDaemonID(ds.TargetID, smap, ""))
		}
		row = append(row, ds.DiskName)

		// disk stats are positional, not named - hence hardcoding

		stat := ds.Stat
		if _idx(cols, colRead) >= 0 {
			row = append(row, FmtStatValue("", stats.KindThroughput, stat.RBps, units))
		}
		if _idx(cols, colReadAvg) >= 0 {
			row = append(row, FmtSize(stat.Ravg, units, 2))
		}
		if _idx(cols, colWrite) >= 0 {
			row = append(row, FmtSize(stat.WBps, units, 2))
		}
		if _idx(cols, colWriteAvg) >= 0 {
			row = append(row, FmtSize(stat.Wavg, units, 2))
		}
		if _idx(cols, colUtil) >= 0 {
			row = append(row, FmtStatValue("", "", stat.Util, units)+"%")
		}

		var haveCap bool
		if withCap {
			// this disk (used%, avail)
			if ds.Tcdf != nil {
				for _, cdf := range ds.Tcdf.Mountpaths {
					// TODO: multi-disk mountpath
					if cdf.Disks[0] != ds.DiskName {
						continue
					}
					used := FmtStatValue("", "", int64(cdf.PctUsed), units) + "%"
					avail := FmtSize(int64(cdf.Avail), units, 2)
					row = append(row, used, avail)
					haveCap = true
					break
				}
			}
		}
		if withCap && !haveCap {
			row = append(row, unknownVal, unknownVal)
		}

		// (alert | total)
		if a, i := fs.HasAlert([]string{ds.DiskName}); i > 0 {
			row[len(row)-1] += fred(" <--- " + a)
		} else if ds.TargetID == totalsHdr { // (cluTotal = "--- Cluster:")
			row[len(row)-1] += fcyan(" ---")
		}

		table.addRow(row)
	}
	return table
}

func _flt(cols []*header, regex *regexp.Regexp) []*header {
	for i := 0; i < len(cols); i++ {
		if cols[i].name == colTarget || cols[i].name == colDisk {
			continue
		}
		lower := strings.ToLower(cols[i].name)
		if regex.MatchString(cols[i].name) || regex.MatchString(lower) {
			continue
		}
		// shift
		copy(cols[i:], cols[i+1:])
		cols = cols[:len(cols)-1]
		i--
	}
	return cols
}

func _idx(cols []*header, name string) int {
	for i := range len(cols) {
		if cols[i].name == name {
			return i
		}
	}
	return -1
}
