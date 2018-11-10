/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc

import (
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
)

// tunable defaults
const (
	initThrottleSleep  = time.Millisecond
	maxThrottleSleep   = time.Second
	fsCapCheckDuration = time.Second * 10
)

const (
	onDiskUtil = uint64(1) << iota
	onFSUsed
)

// implements cluster.Throttler
var _ cluster.Throttler = &throttleContext{}

type throttleContext struct {
	// runtime
	sleep         time.Duration
	nextUtilCheck time.Time
	nextCapCheck  time.Time
	prevUtilPct   float32
	prevFSUsedPct uint64
	// init-time
	capUsedHigh  int64
	diskUtilLow  int64
	diskUtilHigh int64
	period       time.Duration
	path         string
	fs           string
	flag         uint64
}

func (u *throttleContext) Throttle() {
	u.recompute()
	if u.sleep > 0 {
		time.Sleep(u.sleep)
	}
}

// recompute sleep time
func (u *throttleContext) recompute() {
	var (
		ok  bool
		now = time.Now() // FIXME: this may cost if the caller's coming here every ms or so..
	)
	if (u.flag & onFSUsed) != 0 {
		usedFSPercentage := u.prevFSUsedPct
		if now.After(u.nextCapCheck) {
			usedFSPercentage, ok = getFSUsedPercentage(u.path)
			u.nextCapCheck = now.Add(fsCapCheckDuration)
			if !ok {
				glog.Errorf("Unable to retrieve used capacity for fs %s", u.fs)
				u.sleep = 0
				return
			}
			u.prevFSUsedPct = usedFSPercentage
		}
		if usedFSPercentage >= uint64(u.capUsedHigh) {
			u.sleep = 0
			return
		}
	}
	if (u.flag & onDiskUtil) != 0 {
		iostatr := getiostatrunner()
		curUtilPct := u.prevUtilPct

		if now.After(u.nextUtilCheck) {
			curUtilPct, ok = iostatr.maxUtilFS(u.fs)
			u.nextUtilCheck = now.Add(u.period)
			if !ok {
				curUtilPct = u.prevUtilPct
				glog.Errorf("Unable to retrieve disk utilization for fs %s", u.fs)
			}
		}

		if curUtilPct > float32(u.diskUtilHigh) {
			if u.sleep < initThrottleSleep {
				u.sleep = initThrottleSleep
			} else {
				u.sleep *= 2
			}
		} else if curUtilPct < float32(u.diskUtilLow) {
			u.sleep = 0
		} else {
			if u.sleep < initThrottleSleep {
				u.sleep = initThrottleSleep
			}
			multiplier := (curUtilPct - float32(u.diskUtilLow)) / float32(u.diskUtilHigh-u.diskUtilLow)
			u.sleep = u.sleep + time.Duration(multiplier*float32(u.sleep))
		}
		if u.sleep > maxThrottleSleep {
			u.sleep = maxThrottleSleep
		}
		u.prevUtilPct = curUtilPct
	}
}
