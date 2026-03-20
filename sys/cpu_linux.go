// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

type (
	errLoadAvg struct {
		err error
	}

	cpuSample struct {
		usage         int64 // cumulative CPU time (nanoseconds)
		throttledUsec int64 // cumulative throttled time (usec), cgroup v2 only
		ts            int64 // mono.NanoTime
	}

	// (previous) raw cumulative cpuSample
	cpuTracker struct {
		prev cpuSample
		mu   sync.Mutex
	}
)

var ctracker = &cpuTracker{}

func isContainerized() (yes bool) {
	err := cos.ReadLines(rootProcess, func(line string) error {
		if strings.Contains(line, "docker") || strings.Contains(line, "lxc") || strings.Contains(line, "kube") {
			yes = true
			return io.EOF
		}
		return nil
	})
	if err != nil {
		nlog.Errorln("Failed to read system info:", err)
	}
	return
}

// Returns an approximate number of CPUs allocated for the container.
// By default, container runs without limits and its cfs_quota_us is
// negative (-1). When a container starts with limited CPU usage its quota
// is between 0.01 CPU and the number of CPUs on the host machine.
// The function rounds up the calculated number.

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

func containerNumCPU() (int, error) {
	// try cgroup v2
	if n, err := containerNumCPUV2(); err == nil {
		return n, nil
	} else if !cos.IsNotExist(err) {
		return 0, err
	}

	// cgroup v1
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

//
// load averages, with the following limited usage:
// - fallback when cpuTracker has no prior sample or read error
// - `ais show cluster` and friends
//

func (e *errLoadAvg) Error() string {
	return fmt.Sprint("failed to load averages: ", e.err)
}

func LoadAverage() (avg LoadAvg, _ error) {
	line, err := cos.ReadOneLine(hostLoadAvgPath)
	if err != nil {
		return avg, &errLoadAvg{err}
	}

	fields := strings.Fields(line) // TODO: manual parse to reduce string allocations (can wait)
	if l := len(fields); l < 3 {
		err := fmt.Errorf("unexpected %s format: num-fields=%d", hostLoadAvgPath, l)
		return avg, &errLoadAvg{err}
	}

	avg.One, err = strconv.ParseFloat(fields[0], 64)
	if err == nil {
		avg.Five, err = strconv.ParseFloat(fields[1], 64)
	}
	if err == nil {
		avg.Fifteen, err = strconv.ParseFloat(fields[2], 64)
	}
	if err == nil {
		return avg, nil
	}
	return avg, &errLoadAvg{err}
}

//
// CPU utilization (percentage-based, container-aware)
//

// return:
// - CPU utilization as a percentage since the last call, and
// - percentage of wall-clock time the container was CPU-throttled since the last call
// or:
// - 0 if the prev. sample is old (or first call)
func (t *cpuTracker) get() (util, throttled float64, _ error) {
	cur, err := readCPUUsage()
	if err != nil {
		return 0, 0, err
	}

	var (
		prev cpuSample
	)
	t.mu.Lock()
	prev = t.prev
	t.prev = cur

	if prev.ts == 0 {
		t.mu.Unlock()
		return 0, 0, nil
	}

	wallDt := cur.ts - prev.ts
	if wallDt <= 0 || wallDt > maxSampleAge {
		t.mu.Unlock()
		return 0, 0, nil
	}

	if containerized && cur.throttledUsec > 0 {
		dtUsec := max(cur.throttledUsec-prev.throttledUsec, 0)
		throttled = min(float64(dtUsec)*100_000/float64(wallDt), 100)
	}

	t.mu.Unlock()

	cpuDt := max(cur.usage-prev.usage, 0)
	ncpu := float64(NumCPU())
	util = float64(cpuDt) * 100 / float64(wallDt) / ncpu
	return min(util, 100), throttled, nil
}

//
// stateless readers
//

func readCPUUsage() (cpuSample, error) {
	now := mono.NanoTime()
	if containerized {
		usage, thrt, err := readCgroupCPUUsage()
		return cpuSample{usage: usage, throttledUsec: thrt, ts: now}, err
	}

	usage, err := readProcStatCPUUsage()
	return cpuSample{usage: usage, ts: now}, err
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

// read cgroup CPU usage:
// - v2: from cpu.stat (usage_usec + throttled_usec)
// - v1: from cpuacct.usage (nanoseconds), throttled unknown => 0
func readCgroupCPUUsage() (usageNs, throttledUsec int64, _ error) {
	// try cgroup v2
	data, err := os.ReadFile(contCPUV2Stat)
	if err == nil {
		usageNs, throttledUsec = parseCgroupV2Stat(data)
		if usageNs > 0 {
			return usageNs, throttledUsec, nil
		}
	} else if !cos.IsNotExist(err) {
		return 0, 0, fmt.Errorf("%s: %w", contCPUV2Stat, err)
	}

	// fallback to cgroup v1
	ns, err := cos.ReadOneInt64(contCPUAcctUsage)
	if err != nil {
		return 0, 0, fmt.Errorf("cgroup CPU usage (v1 at %q and v2 at %q): %w", contCPUAcctUsage, contCPUV2Stat, err)
	}
	return ns, 0, nil
}

// aggregate "cpu" line from /proc/stat
// - sum user+nice+system+irq+softirq+steal
// - exclude idle and iowait
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
	if len(fields) < 5 {
		return 0, fmt.Errorf("unexpected %s format: num-fields=%d", hostProcStat, len(fields))
	}

	var totalJiffies int64
	for i, f := range fields[1:] {
		if i == 3 || i == 4 { // idle, iowait
			continue
		}
		v, e := strconv.ParseInt(f, 10, 64)
		if e != nil {
			continue
		}
		totalJiffies += v
	}
	return int64(float64(totalJiffies) * nsPerJiffy), nil
}
