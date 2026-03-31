// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

//
// for CPU and memory reporting, cgroup support and future plans, see README.md in this package.
//

// TODO:
// - callers to optionally pass mono-time => MaxLoad2()

const (
	// cpu samples ring
	MinIvalShort = time.Second
	minIvalLong  = 8 * time.Second
	numSamples   = 8

	// >10% wall-clock throttled => extreme starvation
	throttleExtremeThresh = 10

	// assume the usual USER_HZ value
	// as in: C.sysconf(C._SC_CLK_TCK)
	userHZ = 100

	// USER_HZ-derived nanoseconds per tick ("jiffy"); used only for bare-metal fallback
	// when cgroup-based accounting is unavailable
	nsPerJiffy = 1e9 / userHZ
)

const errPrefixCPU = "sys/cpu"

// CPU utilization thresholds (percentage, 0-100)
// HighLoad < HighLoadWM() < ExtremeLoad
const (
	ExtremeLoad = 95
	HighLoad    = 85
)

type (
	sample struct {
		usage         int64 // cumulative CPU time (nanoseconds)
		throttledUsec int64 // cumulative throttled time (usec), cgroup v2 only
		ts            int64 // mono.NanoTime
	}
	ring struct {
		a   [numSamples]sample
		idx int // current
		mu  sync.Mutex
	}
	// global cpu state (gcpu)
	cpu struct {
		ring      ring
		last      atomic.Int64
		util      atomic.Int64
		throttled atomic.Int64
		// num CPUs
		num int
	}
)

func Refresh(now int64, periodic bool) (util, throttled int64, _ error) {
	if last := gcpu.last.Load(); last > 0 {
		elapsed := max(time.Duration(now-last), 0)
		if elapsed < MinIvalShort || (elapsed < minIvalLong && !periodic) {
			return gcpu.util.Load(), gcpu.throttled.Load(), nil
		}
	}

	cur, err := gcpu.read()
	if err != nil {
		return gcpu.util.Load(), gcpu.throttled.Load(), err
	}
	cur.ts = now

	// atomic update
	gcpu.ring.mu.Lock()
	util, throttled = gcpu.ring.sample(cur)
	gcpu.util.Store(util)
	gcpu.throttled.Store(throttled)
	gcpu.last.Store(now)
	gcpu.ring.mu.Unlock()

	return util, throttled, nil
}

func (r *ring) sample(cur sample) (util, throttled int64) {
	// samples previous and current
	prev := r.a[r.idx]
	r.idx = cos.Ternary(r.idx >= numSamples-1, 0, r.idx+1)
	r.a[r.idx] = cur

	if prev.ts == 0 {
		return 0, 0
	}

	// compute
	wallDt := cur.ts - prev.ts
	if isContainerized() && cur.throttledUsec > 0 {
		dtUsec := max(cur.throttledUsec-prev.throttledUsec, 0)
		throttled = min((dtUsec*100_000+wallDt/2)/wallDt, 100)
	}
	cpuDt := max(cur.usage-prev.usage, 0)
	denom := wallDt * int64(NumCPU())
	util = (cpuDt*100 + denom/2) / denom
	return min(util, 100), throttled
}

func CPU(periodic bool) (load int64, isExtreme bool) {
	util, throttled, err := Refresh(mono.NanoTime(), periodic)
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
