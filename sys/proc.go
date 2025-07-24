// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import "github.com/NVIDIA/aistore/cmn/debug"

type (
	ProcCPUStats struct {
		User     uint64
		System   uint64
		Total    uint64
		LastTime int64
		Percent  float64
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
	stats := ProcStats{CPU: cpu}
	if contCPUs > 1 {
		stats.CPU.Percent = float64(stats.CPU.Total) / float64(contCPUs) // TODO: confirm
	}
	mem, err := procMem(pid)
	if err != nil {
		return ProcStats{}, err
	}
	stats.Mem = mem

	return stats, nil
}

func ProcFDSize() int {
	n, err := getFDSize()
	debug.AssertNoErr(err)
	return n
}
