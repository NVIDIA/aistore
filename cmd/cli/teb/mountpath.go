// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
)

const (
	// colTarget
	colNumMpaths  = "MOUNTPATHS"
	colUsedAvgMax = "USED(avg%, max%)"
	// colCapAvail
	colDisksFSes = "Disks & File Systems"
	colCapStatus = "CAP STATUS"
)

func NewMpathCapTab(st StstMap, c *PerfTabCtx) *Table {
	// 1. columns
	cols := []*header{
		{name: colTarget},
		{name: colNumMpaths},
		{name: colUsedAvgMax},
		{name: colCapAvail},
		{name: colDisksFSes},
		{name: colCapStatus},
	}
	// filter
	if c.Regex != nil {
		cols = _flt(cols, c.Regex)
		if _idx(cols, colCapStatus) < 0 && _inclStatus(st) {
			cols = append(cols, &header{name: colCapStatus}) // add it back
		}
	}
	table := newTable(cols...)

	// add rows
	tids := st.sortedSIDs()
	for _, tid := range tids {
		ds := st[tid]
		if c.Sid != "" && c.Sid != tid {
			continue
		}
		row := make([]string, 0, len(cols))
		row = append(row, fmtDaemonID(tid, c.Smap, ds.Status))

		if ds.Status != NodeOnline {
			for _, h := range cols[1:] {
				if h.name == colCapStatus {
					row = append(row, fred(ds.Status))
				} else {
					row = append(row, unknownVal)
				}
			}
			continue
		}

		cdf := ds.TargetCDF
		if _idx(cols, colNumMpaths) >= 0 {
			row = append(row, strconv.Itoa(len(cdf.Mountpaths)))
		}
		if _idx(cols, colUsedAvgMax) >= 0 {
			row = append(row, "     "+strconv.Itoa(int(cdf.PctAvg))+"%   "+strconv.Itoa(int(cdf.PctMax))+"%")
		}
		if _idx(cols, colCapAvail) >= 0 {
			avail := _sumupMpathsAvail(cdf, ds.DeploymentType)
			row = append(row, FmtStatValue("", stats.KindSize, int64(avail), c.Units))
		}
		if i := _idx(cols, colDisksFSes); i >= 0 {
			row = append(row, _fmtMpathDisks(cdf.Mountpaths, i))
		}
		if _idx(cols, colCapStatus) >= 0 {
			if cdf.CsErr == "" {
				if len(cdf.Mountpaths) == 1 {
					row = append(row, "good")
				} else {
					row = append(row, fcyan("good"))
				}
			} else {
				row = append(row, fred(cdf.CsErr))
			}
		}
		table.addRow(row)
	}
	return table
}

func _fmtMpathDisks(cdfs map[string]*fs.CDF, idx int) (s string) {
	var next bool
	for _, cdf := range cdfs {
		if next {
			s += "\n" + strings.Repeat("\t", idx) + " "
		}
		s += cdf.FS
		next = true
	}
	return
}

func _inclStatus(st StstMap) bool {
	for _, ds := range st {
		if ds.Status != NodeOnline || ds.TargetCDF.CsErr != "" {
			return true
		}
	}
	return false
}

func _sumupMpathsAvail(cdf fs.TargetCDF, deploymentType string) (avail uint64) {
	for _, c := range cdf.Mountpaths {
		avail += c.Capacity.Avail
		if deploymentType == apc.DeploymentDev {
			return
		}
	}
	return
}
