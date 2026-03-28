// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

// best-effort auto-detect running in container
func contDetected() bool {
	if _, err := os.Stat("/.dockerenv"); err == nil {
		return true
	}

	var (
		markers = [...]string{"docker", "containerd", "kubepods", "kube", "lxc", "libpod", "podman"}
		yes     bool
	)
	err := cos.ReadLines(rootProcess, func(line string) error {
		line = strings.ToLower(line)
		for _, s := range markers {
			if strings.Contains(line, s) {
				yes = true
				return io.EOF
			}
		}
		return nil
	})
	if err != nil && err != io.EOF {
		nlog.Warningln("failed to detect containerized runtime:", err)
	}
	return yes
}

/////////
// cpu //
/////////

// Return:
// - an approximate number of CPUs allocated for the container.
// By default, container runs without limits and its cfs_quota_us is
// negative (-1). When a container starts with limited CPU usage its quota
// is between 0.01 CPU and the number of CPUs on the host machine.
// The function rounds up the calculated number.
func (t *cpu) setNum() error {
	n2, errV2 := containerNumCPUV2()
	if errV2 == nil {
		t.okv2 = true
		t.num = n2
		return nil
	}

	n1, errV1 := containerNumCPUV1()
	if errV1 == nil {
		t.okv1 = true
		t.num = n1
		return nil
	}

	if cos.IsNotExist(errV2) && cos.IsNotExist(errV1) {
		return errors.New("containerized: no cgroup CPU controller files found; using runtime.NumCPU()")
	}
	return fmt.Errorf("failed to read container CPU count: v2 (%q): %v; v1 (%q, %q): %v", contCPUV2Max, errV2, contCPULimit, contCPUPeriod, errV1)
}

// Return:
// - CPU utilization as a percentage since the last call, and
// - percentage of wall-clock time the container was CPU-throttled since the last call
// or:
// - 0 if the prev. sample is old (or first call)
func (t *cpu) get() (util, throttled int64, _ error) {
	cur, err := t.read()
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
	if wallDt < minWallIval || wallDt > maxSampleAge {
		t.mu.Unlock()
		return 0, 0, nil
	}

	if isContainerized() && cur.throttledUsec > 0 {
		dtUsec := max(cur.throttledUsec-prev.throttledUsec, 0)
		throttled = min((dtUsec*100_000+wallDt/2)/wallDt, int64(100))
	}

	t.mu.Unlock()

	cpuDt := max(cur.usage-prev.usage, 0)
	denom := wallDt * int64(NumCPU())
	util = (cpuDt*100 + denom/2) / denom
	return min(util, 100), throttled, nil
}

func (t *cpu) read() (sample cpuSample, err error) {
	sample.ts = mono.NanoTime()
	switch {
	case t.okv2:
		var data []byte
		data, err = os.ReadFile(contCPUV2Stat)
		if err == nil {
			sample.usage, sample.throttledUsec = parseCgroupV2Stat(data)
		}
		return sample, err
	case t.okv1:
		sample.usage, err = cos.ReadOneInt64(contCPUAcctUsage)
		return sample, err
	default:
		sample.usage, err = readProcStatCPUUsage()
		return sample, err
	}
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
