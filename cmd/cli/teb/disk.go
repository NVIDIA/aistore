// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/stats"
)

func NewDiskTab(dsh []DiskStatsHelper, smap *cluster.Smap, units, totalsHeader string) *Table {
	// 1. columns
	cols := []*header{
		{name: colTarget},
		{name: "DISK"},
		{name: "READ"},
		{name: "READ(avg size)"},
		{name: "WRITE"},
		{name: "WRITE(avg size)"},
		{name: "UTIL(%)"},
	}
	table := newTable(cols...)
	for _, ds := range dsh {
		row := make([]string, 0, len(cols))
		if ds.TargetID == totalsHeader {
			row = append(row, totalsHeader)
		} else {
			row = append(row, fmtDaemonID(ds.TargetID, smap))
		}
		row = append(row, ds.DiskName)

		stat := ds.Stat
		row = append(row, FmtStatValue("", stats.KindThroughput, stat.RBps, units),
			FmtStatValue("", stats.KindSize, stat.Ravg, units),
			FmtStatValue("", stats.KindThroughput, stat.WBps, units),
			FmtStatValue("", stats.KindSize, stat.Wavg, units),
			FmtStatValue("", "", stat.Util, units)+"%")
		table.addRow(row)
	}
	return table
}
