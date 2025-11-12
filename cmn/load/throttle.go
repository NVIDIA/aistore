// Package load provides 5-dimensional node-pressure readings and per-dimension grading.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package load

import (
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/sys"
)

// common throttling constants

const MaxThrottlePct = 75 // vs ratio pct returned by ThrottlePct() below

const (
	Throttle1ms   = time.Millisecond
	Throttle10ms  = 10 * time.Millisecond
	Throttle100ms = 100 * time.Millisecond

	throttleBatch  = 0x1f
	throMiniBatch  = 0x1f >> 1
	throMicroBatch = 0x1f >> 2

	throttleWalk = 0xff
)

// return:
// - max disk utilization across mountpaths
// - max (1 minute, 5 minute) load average
func ThrottlePct() (int, int64, float64) {
	var (
		load     = sys.MaxLoad()
		util     = fs.GetMaxUtil()
		highLoad = sys.HighLoadWM()
	)
	if load >= float64(highLoad) {
		return 100, util, load
	}
	ru := cos.RatioPct(100, 2, util)
	rl := cos.RatioPct(int64(10*highLoad), 1, int64(10*load))
	return int(max(ru, rl)), util, load
}

// inline helpers
func IsThrottleDflt(n int64) bool  { return n&throttleBatch == throttleBatch }
func IsThrottleMini(n int64) bool  { return n&throMiniBatch == throMiniBatch }
func IsThrottleMicro(n int64) bool { return n&throMicroBatch == throMicroBatch }

func IsThrottleWalk(n int64) bool { return n&throttleWalk == throttleWalk }
