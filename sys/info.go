// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"os"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

func FetchSysInfo() cos.SysInfo {
	mem, err := Mem()
	debug.AssertNoErr(err)
	proc, errP := ProcessStats(os.Getpid())
	debug.AssertNoErr(errP)

	return cos.SysInfo{
		MemAvail:   mem.ActualFree,
		MemUsed:    proc.Mem.Resident,
		PctMemUsed: float64(proc.Mem.Resident) * 100 / float64(mem.Total),
		PctCPUUsed: proc.CPU.Percent,
	}
}
