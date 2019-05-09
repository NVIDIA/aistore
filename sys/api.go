// Package sys provides methods to read system information
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package sys

const (
	// host OS stats
	proc = "/proc/"
	// to detect container vs hardware
	rootProcess     = proc + "1/cgroup"
	hostLoadAvgPath = proc + "loadavg"
	// TODO: for later use
	// hostMemPath         = proc + "meminfo"
	// hostProcessStatPath = proc + "%s/stat"

	// container stats

	// TODO: for later use
	// path to read all memory info for cgroup
	// contMemPath = "/sys/fs/cgroup/memory/"
	// path to read all CPU info for cgroup
	contCPUPath = "/sys/fs/cgroup/cpu/"

	// time for cgroup given by scheduler before throttling cgroup
	contCPULimit = contCPUPath + "cpu.cfs_quota_us"
	// length of a period (quota/period ~= max number of CPU available for cgroup)
	contCPUPeriod = contCPUPath + "cpu.cfs_period_us"
)
