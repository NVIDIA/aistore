// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"os"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/sys"
)

type MemCPUInfo struct {
	MemUsed    uint64      `json:"mem_used"`
	MemAvail   uint64      `json:"mem_avail"`
	PctMemUsed float64     `json:"pct_mem_used"`
	PctCPUUsed float64     `json:"pct_cpu_used"`
	LoadAvg    sys.LoadAvg `json:"load_avg"`
}

func GetMemCPU() MemCPUInfo {
	var (
		mem sys.MemStat
		err error
	)
	err = mem.Get()
	debug.AssertNoErr(err)

	proc, err := sys.ProcessStats(os.Getpid())
	debug.AssertNoErr(err)

	load, err := sys.LoadAverage()
	debug.AssertNoErr(err)

	return MemCPUInfo{
		MemAvail:   mem.ActualFree,
		MemUsed:    proc.Mem.Resident,
		PctMemUsed: float64(proc.Mem.Resident) * 100 / float64(mem.Total),
		PctCPUUsed: proc.CPU.Percent,
		LoadAvg:    load,
	}
}
