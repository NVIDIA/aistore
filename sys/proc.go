// Package sys provides helpers to read system info (CPU, memory, loadavg, processes)
// with support for cgroup v2 and a moving-average CPU estimator.
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
		User     uint64  `msg:"u"`
		System   uint64  `msg:"s"`
		Total    uint64  `msg:"t"`
		LastTime int64   `msg:"l"`
		Percent  float64 `msg:"p"`
	}

	ProcMemStats struct {
		Size     uint64 `msg:"s"`
		Resident uint64 `msg:"r"`
		Share    uint64 `msg:"h"`
	}

	ProcStats struct {
		CPU ProcCPUStats `msg:"c"`
		Mem ProcMemStats `msg:"m"`
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
