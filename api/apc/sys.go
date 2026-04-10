// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"os"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/sys"
)

// swagger:model
type MemCPUInfo struct {
	MemUsed    uint64      `json:"mem_used"`
	MemAvail   uint64      `json:"mem_avail"`
	PctMemUsed float64     `json:"pct_mem_used"` // process memory
	PctCPUUsed float64     `json:"pct_cpu_used"` // process CPU
	LoadAvg    sys.LoadAvg `json:"load_avg"`
	// added in 4.4
	CPUUtil      int64 `json:"cpu_util"`                // sampled system/container CPU util (%)
	CPUThrottled int64 `json:"cpu_throttled,omitempty"` // %% time container was CPU throttled (cgroup-v2)
	// added in 4.5
	Mem  *sys.MemStat   `json:"mem,omitempty"`
	Proc *sys.ProcStats `json:"proc,omitempty"`
}

func GetMemCPU() MemCPUInfo {
	var mem sys.MemStat
	err := mem.Get()
	debug.AssertNoErr(err)

	proc, ep := sys.ProcessStats(os.Getpid())
	debug.AssertNoErr(ep)

	load, el := sys.LoadAverage()
	debug.AssertNoErr(el)

	utilEMA, throttled, eu := sys.Refresh(mono.NanoTime(), false /*periodic*/)
	debug.AssertNoErr(eu)

	return MemCPUInfo{
		MemAvail:     mem.ActualFree,
		MemUsed:      proc.Mem.Resident,
		PctMemUsed:   float64(proc.Mem.Resident) * 100 / float64(mem.Total),
		PctCPUUsed:   proc.CPU.Percent,
		LoadAvg:      load,
		CPUUtil:      utilEMA,
		CPUThrottled: throttled,
		Mem:          &mem,
		Proc:         &proc,
	}
}
