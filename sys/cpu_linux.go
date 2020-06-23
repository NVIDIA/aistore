// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"errors"
	"io"
	"runtime"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
)

const (
	uninitialized = -1
)

var (
	inContainer = atomic.NewInt32(uninitialized) // can be bool, but bool does not support 3-state
)

// Containerized returns true if the application is running
// inside a container(docker/lxc/k8s)
//
// How to detect being inside a container:
// https://stackoverflow.com/questions/20010199/how-to-determine-if-a-process-runs-inside-lxc-docker
func Containerized() bool {
	cont := inContainer.Load()
	if cont != uninitialized {
		return cont != 0
	}

	// check if any resource is mounted as one of a container
	inside := false
	err := cmn.ReadLines(rootProcess, func(line string) error {
		if strings.Contains(line, "docker") ||
			strings.Contains(line, "lxc") ||
			strings.Contains(line, "kube") {
			inside = true
			return io.EOF
		}
		return nil
	})
	if err != nil {
		glog.Errorf("Failed to read system info: %v", err)
		return false
	}

	if inside {
		inContainer.Store(1)
	} else {
		inContainer.Store(0)
	}

	return inside
}

// Returns the approximate number of CPUs allocated for the container.
// By default a container runs without limits and its cfs_quota_us is
// negative (-1). When a container starts with limited CPU usage its quota
// is between 0.01 CPU and the number of CPUs on the host machine.
// The function rounds up the calculated number.
func ContainerNumCPU() (int, error) {
	var quota, period uint64

	quotaInt, err := cmn.ReadOneInt64(contCPULimit)
	if err != nil {
		return 0, err
	}
	// negative quota means 'unlimited' - all hardware CPUs are used
	if quotaInt <= 0 {
		return runtime.NumCPU(), nil
	}
	quota = uint64(quotaInt)
	period, err = cmn.ReadOneUint64(contCPUPeriod)
	if err != nil {
		return 0, err
	}

	if period == 0 {
		return 0, errors.New("failed to read container CPU info")
	}

	approx := (quota + period - 1) / period
	return int(cmn.MaxU64(approx, 1)), nil
}

// LoadAverage returns the system load average
func LoadAverage() (avg LoadAvg, err error) {
	avg = LoadAvg{}

	line, err := cmn.ReadOneLine(hostLoadAvgPath)
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
