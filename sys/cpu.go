// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

//
// for CPU and memory reporting, cgroup support and future plans, see README.md in this package.
//

// TODO:
// - remove (maxSampleAge, minWallIval); support alpha/(1-alpha) weighted average over fixed array of samples
// - call Init() from cli/app.go
// - callers to optionally pass mono-time => MaxLoad2()
// - sys/mem.go and friends - support cgroups and all the rest of sys/cpu.go functionality

const (
	// when prev. sample is considered outdated
	// see also:
	// - ios.doRefresh()
	// - stats_time
	maxSampleAge = int64(13 * time.Second)

	// - temp workaround against millisecond-range resampling noise
	// - replace with cached/smoothed CPU-load state
	minWallIval = int64(2 * time.Second)

	// >10% wall-clock throttled => extreme starvation
	throttleExtremeThresh = 10

	// assume the usual USER_HZ value
	// as in: C.sysconf(C._SC_CLK_TCK)
	userHZ = 100

	// USER_HZ-derived nanoseconds per tick ("jiffy"); used only for bare-metal fallback
	// when cgroup-based accounting is unavailable
	nsPerJiffy = 1e9 / userHZ
)

// CPU utilization thresholds (percentage, 0-100)
// HighLoad < HighLoadWM() < ExtremeLoad
const (
	ExtremeLoad = 92
	HighLoad    = 82
)

type (
	LoadAvg struct {
		One, Five, Fifteen float64
	}
	errLoadAvg struct {
		err error
	}

	cpuSample struct {
		usage         int64 // cumulative CPU time (nanoseconds)
		throttledUsec int64 // cumulative throttled time (usec), cgroup v2 only
		ts            int64 // mono.NanoTime
	}

	// global cpu state
	cpu struct {
		prev         cpuSample // previous sample
		num          int       // num CPUs
		mu           sync.Mutex
		contDetected bool
		contForced   bool // <-- feat.ForceContainerCPUMem at startup
		okv2         bool // successfully parsed cgroup v2 at init time
		okv1         bool // ditto v1
	}
)

var gcpu cpu

// num CPUs may get adjusted by Init() below
func init() {
	gcpu.num = runtime.NumCPU()
}

// container-aware CPU count; GOMAXPROCS; okv2 if { container && can read cgroup v2 }
// - AIS node (`aisnode`) calls Init() once upon startup
// - external modules that skip it still get a sane NumCPU() - see above
func Init(contForced bool) bool {
	debug.Assert(gcpu.num > 0)
	gcpu.contForced = contForced
	if gcpu.contDetected = contDetected(); gcpu.contDetected || contForced {
		if err := gcpu.setNum(); err != nil {
			fmt.Fprintln(os.Stderr, err) // (cannot nlog yet)
		}
	}

	// - warn GOMEMLIMIT
	// - possibly reduce GOMAXPROCS to container's num CPUs
	if val, exists := os.LookupEnv("GOMEMLIMIT"); exists {
		nlog.Warningln("Go environment: GOMEMLIMIT =", val) // soft memory limit for the runtime (IEC units or raw bytes)
	}
	if val, exists := os.LookupEnv("GOMAXPROCS"); exists {
		nlog.Warningln("Go environment: GOMAXPROCS =", val)
		return gcpu.contDetected
	}

	maxprocs := runtime.GOMAXPROCS(0)
	ncpu := NumCPU() // gcpu.num
	if maxprocs > ncpu {
		nlog.Warningf("Reducing GOMAXPROCS (prev = %d) to %d", maxprocs, ncpu)
		runtime.GOMAXPROCS(ncpu)
	}
	return gcpu.contDetected
}

func isContainerized() bool { return gcpu.contDetected || gcpu.contForced }

func NumCPU() int { return gcpu.num }

// number of intra-cluster broadcasting goroutines
func MaxParallelism() int { return max(NumCPU(), 4) }

// HighLoadWM: "high-load watermark" as a percentage.
// For 8 CPUs: max(100 - 100/8, 1) = 88 - between HighLoad(82) and ExtremeLoad(92).
// see also: (ExtremeLoad, HighLoad) defaults
func HighLoadWM() int64 {
	ncpu := int64(NumCPU())
	return max(100-100/ncpu, 1)
}

// MaxLoad2 returns CPU utilization percentage and whether the system
// is in an "extreme" CPU-starvation condition.
// In containers (cgroup v2): also check throttled_usec - if the container
// is being throttled, that's CPU starvation regardless of utilization percentage.
//
// See also: README.md in this package.
func MaxLoad2() (load int64, isExtreme bool) {
	util, throttled, err := gcpu.get()
	if err != nil {
		load = maxLoadFallback()
		return load, load >= ExtremeLoad
	}
	if util == 0 {
		return 0, false
	}

	if isContainerized() && throttled > throttleExtremeThresh {
		return max(util, ExtremeLoad), true
	}

	return util, util >= ExtremeLoad
}

// fallback when cpuTracker returns an error:
// convert load average to percentage: (load / NumCPU)%
func maxLoadFallback() int64 {
	avg, err := LoadAverage()
	if err != nil {
		nlog.ErrorDepth(1, err)
		return 100
	}
	load := max(avg.One, avg.Five)
	ncpu := float64(NumCPU())
	pct := load * 100 / ncpu
	return min(int64(pct+0.4), 100)
}
