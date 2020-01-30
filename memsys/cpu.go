// Package memsys provides memory management and Slab allocation
// with io.Reader and io.Writer interfaces on top of a scatter-gather lists
// (of reusable buffers)
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package memsys

import (
	"os"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/sys"
)

// This file provides additional functions to monitor system statistics, such as cpu and disk usage, using the memsys library

func (r *MMSA) FetchSysInfo() cmn.SysInfo {
	sysInfo := cmn.SysInfo{}

	osMem, _ := sys.Mem()
	sysInfo.MemAvail = osMem.Total

	proc, _ := sys.ProcessStats(os.Getpid())
	sysInfo.MemUsed = proc.Mem.Resident
	sysInfo.PctMemUsed = float64(sysInfo.MemUsed) * 100 / float64(sysInfo.MemAvail)
	sysInfo.PctCPUUsed = proc.CPU.Percent

	return sysInfo
}
