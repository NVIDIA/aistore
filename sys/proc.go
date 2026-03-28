// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"github.com/NVIDIA/aistore/cmn/debug"
)

// in sys/proc.go and friends prefix "proc" means "process"
// (not to confuse with /proc FS)

type (
	ProcCPUStats struct {
		User     uint64
		System   uint64
		Total    uint64
		LastTime int64
		Percent  float64 // lifetime-average CPU percent since process start
	}

	ProcMemStats struct {
		Size     uint64
		Resident uint64
		Share    uint64
	}

	ProcStats struct {
		CPU ProcCPUStats
		Mem ProcMemStats
	}
)

func ProcessStats(pid int) (ProcStats, error) {
	cpu, err := procCPU(pid)
	if err != nil {
		return ProcStats{}, err
	}

	mem, err := procMem(pid)
	if err != nil {
		return ProcStats{}, err
	}

	return ProcStats{
		CPU: cpu,
		Mem: mem,
	}, nil
}

func ProcFDSize() int {
	n, err := getFDSize()
	debug.AssertNoErr(err)
	return n
}
