// Package sys provides helpers to read system info (CPU, memory, loadavg, processes)
// with support for cgroup v2 and a moving-average CPU estimator.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
)

/////////
// cpu //
/////////

// Set `cgroupVer` _and_ return an approximate number of CPUs available for the container.
// By default, container runs without limits and its cfs_quota_us is (-1).
// When a container starts with limited CPU usage its quota
// is between 0.01 CPU and the number of CPUs on the host machine.
// The function rounds up the calculated number.
func (t *cpu) setNumCgroup() error {
	n2, errV2 := containerNumCPUV2()
	if errV2 == nil {
		cgroupVer = 2
		t.num = n2
		return nil
	}

	n1, errV1 := containerNumCPUV1()
	if errV1 == nil {
		cgroupVer = 1
		t.num = n1
		return nil
	}

	if cos.IsNotExist(errV2) && cos.IsNotExist(errV1) {
		return fmt.Errorf("%s no cgroup v1/v2 files; using runtime.NumCPU() = %d", errPrefixCPU, t.num)
	}
	return fmt.Errorf("%s: failed to read container CPU count: v2 (%q): %v; v1 (%q, %q): %v",
		errPrefixCPU, contCPUV2Max, errV2, contCPULimit, contCPUPeriod, errV1)
}

func (*cpu) read() (sample sample, err error) {
	switch cgroupVer {
	case 2:
		var data []byte
		data, err = os.ReadFile(contCPUV2Stat)
		if err == nil {
			sample.usage, sample.throttledUsec = parseCgroupV2Stat(data)
		}
	case 1:
		// cgroup v1 (obsolete; to be removed)
		sample.usage, err = cos.ReadOneInt64(contCPUAcctUsage)
	default:
		sample.usage, err = readProcStatCPUUsage()
	}
	if err != nil {
		return sample, fmt.Errorf("%s: %w", errPrefixCPU, err)
	}
	return sample, nil
}

//
// stateless readers ----------------------------------------------------------------------
//

func containerNumCPUV2() (int, error) {
	line, err := cos.ReadOneLine(contCPUV2Max)
	if err != nil {
		return 0, err
	}
	fields := strings.Fields(line)
	if len(fields) != 2 {
		return 0, fmt.Errorf("%s: unexpected format (%q)", contCPUV2Max, strings.TrimSpace(line))
	}

	// "max <period>" means no CPU quota
	if fields[0] == "max" {
		return runtime.NumCPU(), nil
	}

	quota, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: parse quota %q: %w", contCPUV2Max, fields[0], err)
	}
	period, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: parse period %q: %w", contCPUV2Max, fields[1], err)
	}
	if period == 0 {
		return 0, fmt.Errorf("%s: zero period", contCPUV2Max)
	}

	approx := (quota + period - 1) / period
	return int(max(approx, 1)), nil
}

// cgroup v1 (obsolete; to be removed)
func containerNumCPUV1() (int, error) {
	quota, err := cos.ReadOneInt64(contCPULimit)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", contCPULimit, err)
	}
	if quota <= 0 {
		return runtime.NumCPU(), nil
	}
	period, err := cos.ReadOneUint64(contCPUPeriod)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", contCPUPeriod, err)
	}
	if period == 0 {
		return 0, fmt.Errorf("failed to read container CPU info: period=0 (v1, %q)", contCPUPeriod)
	}

	approx := (uint64(quota) + period - 1) / period
	return int(max(approx, 1)), nil
}

func parseCgroupV2Stat(data []byte) (usageNs, throttledUsec int64) {
	for line := range strings.SplitSeq(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		switch fields[0] {
		case "usage_usec":
			if v, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
				usageNs = v * 1000 // usec -> ns
			}
		case "throttled_usec":
			if v, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
				throttledUsec = v
			}
		}
	}
	return
}

// return total cumulative "active" (non-idle) CPU time in nanoseconds:
// - parse the aggregate "cpu" line from /proc/stat
// - require at least 9 fields
// - sum up (user, nice, system, irq, softirq, and steal) jiffies
// - explicitly exclude idle and iowait; ignor guest/guest_nice (already included)
func readProcStatCPUUsage() (int64, error) {
	data, err := os.ReadFile(hostProcStat)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", hostProcStat, err)
	}

	line, _, _ := strings.Cut(string(data), "\n")
	if !strings.HasPrefix(line, "cpu ") {
		return 0, fmt.Errorf("unexpected %s format: first line %q", hostProcStat, line)
	}

	fields := strings.Fields(line)
	if len(fields) < 9 {
		return 0, fmt.Errorf("unexpected %s format: num-fields=%d (old kernel?)", hostProcStat, len(fields))
	}

	var (
		totalJiffies int64
		ii           = [...]int{1, 2, 3, 6, 7, 8} // 1:user, 2:nice, 3:system, 6:irq, 7:softirq, 8:steal
	)
	for _, i := range ii {
		v, err := strconv.ParseInt(fields[i], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%s: parse field[%d]=%q: %w", hostProcStat, i, fields[i], err)
		}
		totalJiffies += v
	}
	return totalJiffies * nsPerJiffy, nil
}
