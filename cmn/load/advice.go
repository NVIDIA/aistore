// Package load provides 5-dimensional node-pressure readings and per-dimension grading.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package load

import "time"

type Advice struct {
	Sleep  time.Duration // recommended sleep at check points
	Batch  int64         // ops between checks (use ShouldCheck helper)
	Grades uint64        // packed grade vector for caller inspection
}

// helper: caller to check/throttle
func ShouldCheck(n, batch int64) bool { return n&batch == batch }

// provide throttling recommendation based on current system load
// (notice priority order: memory > goroutines > CPU > disk)
func Throttle(refresh uint64) Advice {
	loadVec := Refresh(refresh)
	adv := Advice{
		Sleep:  0,
		Batch:  throttleBatch, // default: check every 256 ops
		Grades: loadVec,
	}

	// 1) memory pressure
	switch memOf(loadVec) {
	case Critical:
		adv.Sleep = Throttle100ms
		adv.Batch = throMicroBatch
		return adv // short-circuit on OOM
	case High:
		adv.Sleep = Throttle10ms
		adv.Batch = throMiniBatch
	case Moderate:
		adv.Batch = throMiniBatch
	}

	// 2) goroutines
	switch gorOf(loadVec) {
	case Critical:
		adv.Sleep = max(adv.Sleep, Throttle10ms)
		adv.Batch = min(adv.Batch, throMicroBatch)
	case High:
		adv.Sleep = max(adv.Sleep, Throttle1ms)
		adv.Batch = min(adv.Batch, throMiniBatch)
	}

	// 3) CPU load
	if cpuOf(loadVec) >= High {
		adv.Sleep = max(adv.Sleep, Throttle1ms)
		adv.Batch = min(adv.Batch, throMiniBatch)
	}

	// TODO 4) Disk utilization (via existing ThrottlePct)
	return adv
}
