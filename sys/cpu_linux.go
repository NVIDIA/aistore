// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"errors"
	"io"
	"runtime"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

// NOTE (July 2024):
// Reading /proc/1/cgroup cannot be relied on - does not always produce "docker", etc. strings that indicate "containerization."
// One plausible, albeit still somewhat hacky approach could be detecting filesystem type of the root ("/"),
// as in:
//
// var mi = fs.Mountpath{Path: "/"}
// if mi.resolveFS() == nil {
// 	if mi.FsType == "overlay" {
// 		containerized = true
// 		...
// 	}
// }

// TODO: either amend, OR introduce env var and remove auto-detection altogether
func isContainerized() (yes bool) {
	err := cos.ReadLines(rootProcess, func(line string) error {
		if strings.Contains(line, "docker") || strings.Contains(line, "lxc") || strings.Contains(line, "kube") {
			yes = true
			return io.EOF
		}
		return nil
	})
	if err != nil {
		nlog.Errorf("Failed to read system info: %v", err)
	}
	return
}

// Returns an approximate number of CPUs allocated for the container.
// By default, container runs without limits and its cfs_quota_us is
// negative (-1). When a container starts with limited CPU usage its quota
// is between 0.01 CPU and the number of CPUs on the host machine.
// The function rounds up the calculated number.
func containerNumCPU() (int, error) {
	var quota, period uint64

	quotaInt, err := cos.ReadOneInt64(contCPULimit)
	if err != nil {
		return 0, err
	}
	// negative quota means 'unlimited' - all hardware CPUs are used
	if quotaInt <= 0 {
		return runtime.NumCPU(), nil
	}
	quota = uint64(quotaInt)
	period, err = cos.ReadOneUint64(contCPUPeriod)
	if err != nil {
		return 0, err
	}

	if period == 0 {
		return 0, errors.New("failed to read container CPU info")
	}

	approx := (quota + period - 1) / period
	return int(max(approx, 1)), nil
}

// LoadAverage returns the system load average
func LoadAverage() (avg LoadAvg, err error) {
	avg = LoadAvg{}

	line, err := cos.ReadOneLine(hostLoadAvgPath)
	if err != nil {
		return avg, err
	}

	fields := strings.Fields(line)
	avg.One, err = strconv.ParseFloat(fields[0], 64)
	if err == nil {
		avg.Five, err = strconv.ParseFloat(fields[1], 64)
	}
	if err == nil {
		avg.Fifteen, err = strconv.ParseFloat(fields[2], 64)
	}

	return avg, err
}
