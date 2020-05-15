// Package sys provides methods to read system information
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"os"

	"github.com/NVIDIA/aistore/cmn"
)

func FetchSysInfo() cmn.SysInfo {
	var (
		osMem, _ = Mem()
		proc, _  = ProcessStats(os.Getpid())
	)
	return cmn.SysInfo{
		MemAvail:   osMem.Total,
		MemUsed:    proc.Mem.Resident,
		PctMemUsed: float64(proc.Mem.Resident) * 100 / float64(osMem.Total),
		PctCPUUsed: proc.CPU.Percent,
	}
}
