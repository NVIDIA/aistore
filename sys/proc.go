// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"sync"
	"time"
)

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

	procCache struct {
		sync.Mutex
		procs map[int]*ProcStats
	}
)

// the minimum number of milliseconds between two cpu usage calculations
// if a new requests comes earlier, the last calculated cpu usage is returned
const cpuRefreshInterval = 1000

// last values for requested processes to calculate CPU usage
var cache *procCache

// First call to ProcessStats always returns 0% CPU usage because
// the process in not in the cache yet
// Return by value, so the next call for the same pid wouldn't rewrite old data
func ProcessStats(pid int) (ProcStats, error) {
	cache.Lock()
	defer cache.Unlock()
	stats, ok := cache.procs[pid]

	cpu, err := procCPU(pid)
	if err != nil {
		return ProcStats{}, err
	}
	mem, err := procMem(pid)
	if err != nil {
		return ProcStats{}, err
	}
	if ok {
		tm := time.Now().UnixNano() / int64(time.Millisecond)
		timeDiff := tm - stats.CPU.LastTime
		if timeDiff >= cpuRefreshInterval {
			stats.CPU.LastTime = tm
			stats.CPU.Percent = 100.0 * float64(cpu.Total-stats.CPU.Total) / float64(timeDiff)
			stats.CPU.Total = cpu.Total
			stats.CPU.User = cpu.User
			stats.CPU.System = cpu.System
		}
	} else {
		stats = &ProcStats{CPU: cpu}
		stats.CPU.LastTime = time.Now().UnixNano() / int64(time.Millisecond)
		cache.procs[pid] = stats
	}
	stats.Mem = mem

	return *stats, nil
}
