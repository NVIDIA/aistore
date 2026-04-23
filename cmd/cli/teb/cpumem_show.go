// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"fmt"

	"github.com/NVIDIA/aistore/core/meta"
)

const (
	colNode       = "NODE"
	colNodeType   = "TYPE"
	colProcCPU    = "PROC CPU(%)"
	colProcRSS    = "PROC RSS"
	colProcSize   = "PROC SIZE"
	colProcShared = "PROC SHARED"
	colProcMem    = "PROC MEM(%)"
	colMemTotal   = "TOTAL"
	colMemUsedAbs = "USED"
	colMemFree    = "FREE"
	colBuffCache  = "BUFF/CACHE"
	colActualUsed = "ACTUAL USED"
	colActualFree = "ACTUAL FREE"
	colSwapUsed   = "SWAP USED"
	colSwapFree   = "SWAP FREE"
	colSwapTotal  = "SWAP TOTAL"
)

// `ais show cluster cpu` - proxy and target tables
// USER/SYSTEM/TOTAL jiffies omitted; PROC CPU(%) subsumes them
// THROTTLED hidden when zero across both maps
func MakeTabCPU(smap *meta.Smap, pmap, tmap NodeStatusMap) (*Table, *Table) {
	showThrottle := _anyThrottled(pmap, tmap)

	make1 := func(m NodeStatusMap, proxies bool) *Table {
		table := newTable(
			&header{name: colNode},
			&header{name: colNodeType},
			&header{name: colProcCPU},
			&header{name: colSysCPU},
			&header{name: colThrottled, hide: !showThrottle},
			&header{name: colLoadAvg},
		)
		for _, sid := range m.sortPODs(smap, proxies) {
			ds := m[sid]
			if ds.Status != NodeOnline {
				nid, _ := fmtStatusSID(ds.Snode.ID(), smap, ds.Status)
				table.addRow(row{nid, ds.Snode.DaeType,
					unknownVal, unknownVal, unknownVal, unknownVal})
				continue
			}
			procCPU := NotSetVal
			if ds.MemCPUInfo.Proc != nil {
				procCPU = fmt.Sprintf("%.1f%%", ds.MemCPUInfo.Proc.CPU.Percent)
			}
			table.addRow(row{
				fmtDaemonID(ds.Snode.ID(), smap, ds.Status),
				ds.Snode.DaeType,
				procCPU,
				_sysCPU(&ds.MemCPUInfo),
				_throttled(&ds.MemCPUInfo),
				_load(&ds.MemCPUInfo),
			})
		}
		return table
	}
	return make1(pmap, true), make1(tmap, false)
}

// `ais show cluster memory` - proxy and target tables
// SWAP columns hidden together when SwapTotal==0 across both maps
// PROC SHARED and BUFF/CACHE hidden when all-zero
func MakeTabMem(smap *meta.Smap, pmap, tmap NodeStatusMap, units string) (*Table, *Table) {
	showShared := _anyProcShared(pmap, tmap)
	showBuffCache := _anyBuffCache(pmap, tmap)
	showSwap := _anySwap(pmap, tmap)

	make1 := func(m NodeStatusMap, proxies bool) *Table {
		table := newTable(
			&header{name: colNode},
			&header{name: colNodeType},
			&header{name: colProcRSS},
			&header{name: colProcSize},
			&header{name: colProcShared, hide: !showShared},
			&header{name: colProcMem},
			&header{name: colMemTotal},
			&header{name: colMemUsedAbs},
			&header{name: colMemFree},
			&header{name: colBuffCache, hide: !showBuffCache},
			&header{name: colActualUsed},
			&header{name: colActualFree},
			&header{name: colSwapUsed, hide: !showSwap},
			&header{name: colSwapFree, hide: !showSwap},
			&header{name: colSwapTotal, hide: !showSwap},
		)
		for _, sid := range m.sortPODs(smap, proxies) {
			ds := m[sid]
			if ds.Status != NodeOnline {
				nid, _ := fmtStatusSID(ds.Snode.ID(), smap, ds.Status)
				table.addRow(row{nid, ds.Snode.DaeType,
					unknownVal, unknownVal, unknownVal, unknownVal,
					unknownVal, unknownVal, unknownVal, unknownVal,
					unknownVal, unknownVal, unknownVal, unknownVal, unknownVal})
				continue
			}
			procRSS, procSize, procShared, procMem := NotSetVal, NotSetVal, NotSetVal, NotSetVal
			if p := ds.MemCPUInfo.Proc; p != nil {
				procRSS = FmtSize(int64(p.Mem.Resident), units, 2)
				procSize = FmtSize(int64(p.Mem.Size), units, 2)
				procShared = FmtSize(int64(p.Mem.Share), units, 2)
				procMem = fmt.Sprintf("%.1f%%", ds.MemCPUInfo.PctMemUsed)
			}
			memTotal, memUsed, memFree := NotSetVal, NotSetVal, NotSetVal
			buffCache, actualUsed, actualFree := NotSetVal, NotSetVal, NotSetVal
			swapUsed, swapFree, swapTotal := NotSetVal, NotSetVal, NotSetVal
			if m := ds.MemCPUInfo.Mem; m != nil {
				memTotal = FmtSize(int64(m.Total), units, 2)
				memUsed = FmtSize(int64(m.Used), units, 2)
				memFree = FmtSize(int64(m.Free), units, 2)
				buffCache = FmtSize(int64(m.BuffCache), units, 2)
				actualUsed = FmtSize(int64(m.ActualUsed), units, 2)
				actualFree = FmtSize(int64(m.ActualFree), units, 2)
				swapUsed = FmtSize(int64(m.SwapUsed), units, 2)
				swapFree = FmtSize(int64(m.SwapFree), units, 2)
				swapTotal = FmtSize(int64(m.SwapTotal), units, 2)
			}
			table.addRow(row{
				fmtDaemonID(ds.Snode.ID(), smap, ds.Status),
				ds.Snode.DaeType,
				procRSS, procSize, procShared, procMem,
				memTotal, memUsed, memFree, buffCache,
				actualUsed, actualFree,
				swapUsed, swapFree, swapTotal,
			})
		}
		return table
	}
	return make1(pmap, true), make1(tmap, false)
}

func _anyThrottled(maps ...NodeStatusMap) bool {
	for _, m := range maps {
		for _, ds := range m {
			if ds.MemCPUInfo.CPUThrottled != 0 {
				return true
			}
		}
	}
	return false
}

func _anyProcShared(maps ...NodeStatusMap) bool {
	for _, m := range maps {
		for _, ds := range m {
			if ds.MemCPUInfo.Proc != nil && ds.MemCPUInfo.Proc.Mem.Share != 0 {
				return true
			}
		}
	}
	return false
}

func _anyBuffCache(maps ...NodeStatusMap) bool {
	for _, m := range maps {
		for _, ds := range m {
			if ds.MemCPUInfo.Mem != nil && ds.MemCPUInfo.Mem.BuffCache != 0 {
				return true
			}
		}
	}
	return false
}

func _anySwap(maps ...NodeStatusMap) bool {
	for _, m := range maps {
		for _, ds := range m {
			if ds.MemCPUInfo.Mem != nil && ds.MemCPUInfo.Mem.SwapTotal != 0 {
				return true
			}
		}
	}
	return false
}
