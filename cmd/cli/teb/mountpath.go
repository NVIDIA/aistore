// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2023=2025, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
)

const (
	// colTarget
	colNumMpaths = "MOUNTPATHS"
	colMountpath = "MOUNTPATH"

	colUsedAvgMax = "USED(min%, avg%, max%)"

	colDisksFS = "Disks & File System"
	colFS      = "File System"

	colCapStatus = "CAP STATUS"
)

func NewMpathCapTab(st StstMap, c *PerfTabCtx, showMpaths bool) *Table {
	var cols []*header

	// 1. columns
	if showMpaths {
		cols = []*header{
			{name: colTarget},
			{name: colMountpath},
			{name: colCapUsed},
			{name: colCapAvail},
			{name: colDisk},
			{name: colFS},
			{name: colCapStatus},
		}
	} else { // show num mountpaths and aggregated used/avail stats
		cols = []*header{
			{name: colTarget},
			{name: colNumMpaths},
			{name: colUsedAvgMax},
			{name: colCapAvail},
			{name: colDisksFS},
			{name: colCapStatus},
		}
	}

	// filter columns
	if c.Regex != nil {
		cols = _flt(cols, c.Regex)
		if _idx(cols, colCapStatus) < 0 && _inclStatus(st) {
			cols = append(cols, &header{name: colCapStatus}) // add it back
		}
	}
	table := newTable(cols...)

	tids := st.sortSIDs()
	for _, tid := range tids {
		ds := st[tid]
		if c.Sid != "" && c.Sid != tid {
			continue
		}
		if ds.Status != NodeOnline {
			row := make([]string, 0, len(cols))
			row = append(row, fmtDaemonID(tid, c.Smap, ds.Status))
			for _, h := range cols[1:] {
				if h.name == colCapStatus {
					row = append(row, fred(ds.Status))
				} else {
					row = append(row, unknownVal)
				}
			}
			table.addRow(row)
			continue
		}

		if showMpaths {
			var (
				i, num = 0, len(ds.Tcdf.Mountpaths)
				mpaths = make([]string, 0, num)
			)
			for mp := range ds.Tcdf.Mountpaths {
				mpaths = append(mpaths, mp)
			}
			sort.Strings(mpaths)
			// add rows: one per mountpath
			for _, mp := range mpaths {
				row := make([]string, 0, len(cols))
				if i == 0 {
					row = append(row, fmtDaemonID(tid, c.Smap, ds.Status))
				} else {
					row = append(row, "")
				}
				cdf := ds.Tcdf.Mountpaths[mp]
				row = mpathRow(c, cols, mp, cdf, row)
				if _idx(cols, colCapStatus) >= 0 {
					if i == 0 {
						row = append(row, _capStatus(ds.Tcdf))
					} else {
						row = append(row, "")
					}
				}
				table.addRow(row)

				i++
			}
		} else {
			// add row
			row := make([]string, 0, len(cols))
			row = append(row, fmtDaemonID(tid, c.Smap, ds.Status))
			row = numMpathsRow(ds, c, cols, row)
			table.addRow(row)
		}
	}
	return table
}

func mpathRow(c *PerfTabCtx, cols []*header, mpath string, cdf *fs.CDF, row []string) []string {
	if _idx(cols, colMountpath) >= 0 {
		debug.Assert(_idx(cols, colNumMpaths) < 0)
		row = append(row, mpath)
	}
	if _idx(cols, colCapUsed) >= 0 {
		row = append(row, strconv.Itoa(int(cdf.PctUsed))+"%")
	}
	if _idx(cols, colCapAvail) >= 0 {
		row = append(row, FmtSize(int64(cdf.Avail), c.Units, 2))
	}
	if _idx(cols, colDisk) >= 0 {
		switch {
		case len(cdf.Disks) > 1:
			row = append(row, fmt.Sprintf("%v", cdf.Disks))
		case len(cdf.Disks) == 1:
			row = append(row, cdf.Disks[0])
		default:
			row = append(row, unknownVal)
		}
	}
	if _idx(cols, colFS) >= 0 {
		row = append(row, cdf.FS.String())
	}
	return row
}

func numMpathsRow(ds *stats.NodeStatus, c *PerfTabCtx, cols []*header, row []string) []string {
	tcdf := ds.Tcdf
	if _idx(cols, colNumMpaths) >= 0 {
		debug.Assert(_idx(cols, colMountpath) < 0)
		row = append(row, strconv.Itoa(len(tcdf.Mountpaths)))
	}
	if _idx(cols, colUsedAvgMax) >= 0 {
		// len("USED(")
		row = append(row, "     "+fmtCapPctMAM(&tcdf, false /*list*/))
	}
	if _idx(cols, colCapAvail) >= 0 {
		avail := _sumupMpathsAvail(tcdf, ds.DeploymentType)
		row = append(row, FmtSize(int64(avail), c.Units, 2))
	}
	if i := _idx(cols, colDisksFS); i >= 0 {
		row = append(row, _fmtMpathDisks(tcdf.Mountpaths, i))
	}
	if _idx(cols, colCapStatus) >= 0 {
		row = append(row, _capStatus(tcdf))
	}
	return row
}

func _capStatus(tcdf fs.Tcdf) (s string) {
	if tcdf.CsErr == "" {
		if len(tcdf.Mountpaths) == 1 {
			s = "good"
		} else {
			s = fcyan("good")
		}
	} else {
		s = fred("Error: ") + tcdf.CsErr
	}
	return
}

func _fmtMpathDisks(cdfs map[string]*fs.CDF, idx int) (s string) {
	var next bool
	for _, cdf := range cdfs {
		if next {
			s += "\n" + strings.Repeat("\t", idx) + " "
		}
		s += cdf.FS.String()
		next = true
	}
	return
}

func _inclStatus(st StstMap) bool {
	for _, ds := range st {
		if ds.Status != NodeOnline || ds.Tcdf.CsErr != "" {
			return true
		}
	}
	return false
}

// compare with "total num disks" (daeclu.go)
func _sumupMpathsAvail(cdf fs.Tcdf, deploymentType string) (avail uint64) {
	for _, c := range cdf.Mountpaths {
		avail += c.Capacity.Avail
		if deploymentType == apc.DeploymentDev {
			return // simplifying HACK that'll be true most of the time
		}
	}
	return
}
