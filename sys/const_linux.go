// Package sys provides helpers to read system info (CPU, memory, loadavg, processes)
// with support for cgroup v2 and a moving-average CPU estimator.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

const (
	// host OS stats
	proc = "/proc/"

	hostProcStat        = proc + "stat"
	hostCPUPressurePath = proc + "pressure/cpu" // PSI bare metal (TODO)

	// to detect container vs hardware
	rootProcess     = proc + "1/cgroup"
	hostLoadAvgPath = proc + "loadavg"
	hostMemPath     = proc + "meminfo"
	// CPU usage by a process
	hostProcessStatCPUPath = proc + "%d/stat"
	// Memory usage by a process
	hostProcessStatMemPath = proc + "%d/statm"

	// Name, Umask, State, other process details
	// Used to get FDSize
	hostProcessInfo = proc + "self/status"
)

// container stats: cgroup v1 (fixed hierarchy with per-controller directories)
const (
	contBase = "/sys/fs/cgroup"

	// path to read all memory info for cgroup
	contMemPath = contBase + "/memory/"
	// path to read all CPU info for cgroup
	contCPUPath = contBase + "/cpu/"
	// memory counters
	contMemUsedPath  = contMemPath + "memory.usage_in_bytes"
	contMemLimitPath = contMemPath + "memory.limit_in_bytes"
	contMemStatPath  = contMemPath + "memory.stat"

	// time for cgroup given by scheduler before throttling cgroup
	contCPULimit = contCPUPath + "cpu.cfs_quota_us"
	// length of a period (quota/period ~= max number of CPU available for cgroup)
	contCPUPeriod = contCPUPath + "cpu.cfs_period_us"

	// cgroup v1: cumulative CPU usage in nanoseconds
	contCPUAcctUsage = contBase + "/cpuacct/cpuacct.usage"
)

// cgroup v2 knob basenames
const (
	cgV2CPUStat = "cpu.stat"
	cgV2CPUMax  = "cpu.max"
	cgV2MemMax  = "memory.max"
	cgV2MemCur  = "memory.current"
	cgV2MemStat = "memory.stat"
)
