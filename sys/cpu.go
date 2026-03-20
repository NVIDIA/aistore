// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/NVIDIA/aistore/cmn/nlog"
)

// 1. Motivation:
// Provide CPU utilization for both bare-metal and containerized deployments.
//
// 2. Utilization:
// We track cumulative CPU time (usage_usec) over a wall-clock
// interval (mono.NanoTime). Utilization is: delta-usage / (delta-time * NumCPU)
//
// 3. Throttling (cgroup v2 only):
// We also monitor 'throttled_usec' from cpu.stat. If the container is being
// throttled, it is CPU-starved by definition - even when utilization appears
// low (e.g., a 4-CPU container on a busy node may show 60% util but still
// get throttled when bursting above its quota).
//
// 4. Hierarchy:
// a) Cgroup v2 (cpu.stat) - Primary source for unified usage and pressure.
// b) Cgroup v1 (cpuacct.usage) - Fallback for older container runtimes.
// c) /proc/stat - Bare-metal fallback using Jiffy-delta math.
// d) /proc/loadavg - Last resort fallback (normalized by NumCPU).
//
// 5. Statefulness:
// The 'cpuTracker' maintains the previous sample to calculate deltas. To avoid
// stale math after long pauses, samples older than 'maxSampleAge' are discarded.

// TODO: hardcoded
const (
	maxSampleAge          = int64(10 * time.Second) // when prev. sample is considered outdated
	throttleExtremeThresh = 10                      // >10% wall-clock throttled => extreme starvation

	// USER_HZ
	// - what-if: kernel gets compiled w/ non-default
	// - relatively low priority: used only for bare-metal fallback when cgroups are unavailable
	nsPerJiffy = 1e9 / 100
)

// CPU utilization thresholds (percentage, 0-100)
// HighLoad < HighLoadWM() < ExtremeLoad
const (
	ExtremeLoad = 92
	HighLoad    = 82
)

type LoadAvg struct {
	One, Five, Fifteen float64
}

// set at startup and never change
var (
	contCPUs      int
	containerized bool
)

func init() {
	contCPUs = runtime.NumCPU()
	if containerized = isContainerized(); containerized {
		if c, err := containerNumCPU(); err == nil {
			contCPUs = c
		} else {
			fmt.Fprintln(os.Stderr, err) // (cannot nlog yet)
		}
	}
}

func Containerized() bool { return containerized }
func NumCPU() int         { return contCPUs }

// number of intra-cluster broadcasting goroutines
func MaxParallelism() int { return max(NumCPU(), 4) }

func GoEnvMaxprocs() {
	if val, exists := os.LookupEnv("GOMEMLIMIT"); exists {
		nlog.Warningln("Go environment: GOMEMLIMIT =", val) // soft memory limit for the runtime (IEC units or raw bytes)
	}
	if val, exists := os.LookupEnv("GOMAXPROCS"); exists {
		nlog.Warningln("Go environment: GOMAXPROCS =", val)
		return
	}

	maxprocs := runtime.GOMAXPROCS(0)
	ncpu := NumCPU()
	if maxprocs > ncpu {
		nlog.Warningf("Reducing GOMAXPROCS (prev = %d) to %d", maxprocs, ncpu)
		runtime.GOMAXPROCS(ncpu)
	}
}

// HighLoadWM: "high-load watermark" as a percentage.
// For 8 CPUs: max(100 - 100/8, 1) = 88 - between HighLoad(82) and ExtremeLoad(92).
// see also: (ExtremeLoad, HighLoad) defaults
func HighLoadWM() int {
	ncpu := NumCPU()
	return max(100-100/ncpu, 1)
}

// MaxLoad returns CPU utilization percentage (0-100).
func MaxLoad() (load float64) {
	util, _, err := ctracker.get()
	if err != nil {
		return maxLoadFallback()
	}
	return util
}

// MaxLoad2 returns CPU utilization percentage and whether the system
// is in an "extreme" CPU-starvation condition.
//
// In containers (cgroup v2): also check throttled_usec - if the container
// is being throttled, that's CPU starvation regardless of utilization percentage.
func MaxLoad2() (load float64, isExtreme bool) {
	util, throttled, err := ctracker.get()
	if err != nil {
		load = maxLoadFallback()
		return load, load >= ExtremeLoad
	}
	if util == 0 {
		return 0, false
	}

	if containerized && throttled > throttleExtremeThresh {
		return max(util, ExtremeLoad), true
	}

	return util, util >= ExtremeLoad
}

// fallback when cpuTracker returns an error:
// convert load average to percentage: (load / NumCPU)%
func maxLoadFallback() float64 {
	avg, err := LoadAverage()
	if err != nil {
		nlog.ErrorDepth(1, err)
		return 100
	}
	load := max(avg.One, avg.Five)
	ncpu := float64(NumCPU())
	pct := load * 100 / ncpu
	return min(pct, 100)
}
