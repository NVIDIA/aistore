// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package sys

const (
	// host OS stats
	proc = "/proc/"
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

// container stats
const (
	// path to read all memory info for cgroup
	contMemPath = "/sys/fs/cgroup/memory/"
	// path to read all CPU info for cgroup
	contCPUPath = "/sys/fs/cgroup/cpu/"
	// memory counters
	contMemUsedPath  = contMemPath + "memory.usage_in_bytes"
	contMemLimitPath = contMemPath + "memory.limit_in_bytes"
	contMemStatPath  = contMemPath + "memory.stat"

	// time for cgroup given by scheduler before throttling cgroup
	contCPULimit = contCPUPath + "cpu.cfs_quota_us"
	// length of a period (quota/period ~= max number of CPU available for cgroup)
	contCPUPeriod = contCPUPath + "cpu.cfs_period_us"
)
