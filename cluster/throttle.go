/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package cluster provides common interfaces and local access to cluster-level metadata
package cluster

import (
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/ios"
)

// tunable defaults
const (
	initThrottleSleep  = time.Millisecond
	maxThrottleSleep   = time.Second
	fsCapCheckDuration = time.Second * 10
)

const (
	OnDiskUtil = uint64(1) << iota
	OnFSUsed
)

type (
	Throttler interface {
		Sleep()
	}
	Throttle struct {
		// runtime
		sleep         time.Duration
		nextUtilCheck time.Time
		nextCapCheck  time.Time
		prevUtilPct   float32
		prevFSUsedPct uint64
		// init-time
		Config  *cmn.Config
		Riostat *ios.IostatRunner
		Path    string
		FS      string
		Flag    uint64
	}
)

var _ Throttler = &Throttle{}

func (u *Throttle) Sleep() {
	u.recompute()
	if u.sleep > 0 {
		time.Sleep(u.sleep)
	}
}

// recompute sleep time
func (u *Throttle) recompute() {
	var (
		ok  bool
		now = time.Now() // FIXME: this may cost if the caller's coming here every ms or so..
	)
	if (u.Flag & OnFSUsed) != 0 {
		usedFSPercentage := u.prevFSUsedPct
		if now.After(u.nextCapCheck) {
			usedFSPercentage, ok = ios.GetFSUsedPercentage(u.Path)
			u.nextCapCheck = now.Add(fsCapCheckDuration)
			if !ok {
				glog.Errorf("Unable to retrieve used capacity for FS %s", u.FS)
				u.sleep = 0
				return
			}
			u.prevFSUsedPct = usedFSPercentage
		}
		if usedFSPercentage >= uint64(u.Config.LRU.HighWM) {
			u.sleep = 0
			return
		}
	}
	if (u.Flag & OnDiskUtil) != 0 {
		curUtilPct := u.prevUtilPct

		if now.After(u.nextUtilCheck) {
			curUtilPct, ok = u.Riostat.MaxUtilFS(u.FS)
			u.nextUtilCheck = now.Add(u.Config.Periodic.StatsTime)
			if !ok {
				curUtilPct = u.prevUtilPct
				glog.Errorf("Unable to retrieve disk utilization for FS %s", u.FS)
			}
		}

		if curUtilPct > float32(u.Config.Xaction.DiskUtilHighWM) {
			if u.sleep < initThrottleSleep {
				u.sleep = initThrottleSleep
			} else {
				u.sleep *= 2
			}
		} else if curUtilPct < float32(u.Config.Xaction.DiskUtilLowWM) {
			u.sleep = 0
		} else {
			if u.sleep < initThrottleSleep {
				u.sleep = initThrottleSleep
			}
			x := float32(u.Config.Xaction.DiskUtilHighWM - u.Config.Xaction.DiskUtilLowWM)
			multiplier := (curUtilPct - float32(u.Config.Xaction.DiskUtilLowWM)) / x
			u.sleep = u.sleep + time.Duration(multiplier*float32(u.sleep))
		}
		if u.sleep > maxThrottleSleep {
			u.sleep = maxThrottleSleep
		}
		u.prevUtilPct = curUtilPct
	}
}
