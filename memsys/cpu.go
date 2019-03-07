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
	sigar "github.com/cloudfoundry/gosigar"
)

// This file provides additional functions to monitor system statistics, such as cpu and disk usage, using the memsys library

func (r *Mem2) FetchSysInfo() cmn.SysInfo {
	sysInfo := cmn.SysInfo{}

	concSigar := sigar.ConcreteSigar{}
	concMem, _ := concSigar.GetMem()

	sysInfo.MemAvail = concMem.Total

	mem := sigar.ProcMem{}
	mem.Get(os.Getpid())
	sysInfo.MemUsed = mem.Resident
	sysInfo.PctMemUsed = float64(sysInfo.MemUsed) * 100 / float64(sysInfo.MemAvail)

	cpu := sigar.ProcCpu{}
	cpu.Get(os.Getpid())
	sysInfo.PctCPUUsed = cpu.Percent

	return sysInfo
}
