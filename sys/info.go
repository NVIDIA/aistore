// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"os"

	"github.com/NVIDIA/aistore/cmn/cos"
)

func FetchSysInfo() cos.SysInfo {
	var (
		osMem, _ = Mem()
		proc, _  = ProcessStats(os.Getpid())
	)
	return cos.SysInfo{
		MemAvail:   osMem.Total,
		MemUsed:    proc.Mem.Resident,
		PctMemUsed: float64(proc.Mem.Resident) * 100 / float64(osMem.Total),
		PctCPUUsed: proc.CPU.Percent,
	}
}
