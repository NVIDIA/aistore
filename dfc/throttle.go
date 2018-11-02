/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc

import (
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

const (
	initThrottleSleep  = time.Millisecond
	maxThrottleSleep   = time.Second
	fsCapCheckDuration = time.Second * 10
)

const (
	onDiskUtil = uint64(1) << iota
	onFSUsed
)

type throttleContext struct {
	sleep         time.Duration
	nextUtilCheck time.Time
	nextCapCheck  time.Time
	prevUtilPct   float32
	prevFSUsedPct uint64
}

type throttleParams struct {
	throttle uint64 // flag field that indicates on what to perform throttling
	fs       string
	fqn      string
}

func (thrctx *throttleContext) throttle(params *throttleParams) {
	// compute sleeping time and update context
	thrctx.computeThrottle(params)

	if thrctx.sleep > 0 {
		if glog.V(4) {
			glog.Infof("%s: sleeping %v", params.fqn, thrctx.sleep)
		}
		time.Sleep(thrctx.sleep)
	}
}

func (thrctx *throttleContext) computeThrottle(params *throttleParams) {
	var (
		ok  bool
		now = time.Now()
	)

	if (params.throttle & onFSUsed) != 0 {
		usedFSPercentage := thrctx.prevFSUsedPct
		if now.After(thrctx.nextCapCheck) {
			usedFSPercentage, ok = getFSUsedPercentage(params.fqn)
			thrctx.nextCapCheck = now.Add(fsCapCheckDuration)
			if !ok {
				glog.Errorf("Unable to retrieve used capacity for fs %s", params.fs)
				thrctx.sleep = 0
				return
			}
			thrctx.prevFSUsedPct = usedFSPercentage
		}

		if usedFSPercentage >= uint64(ctx.config.LRU.HighWM) {
			thrctx.sleep = 0
			return
		}
	}

	if (params.throttle & onDiskUtil) != 0 {
		iostatr := getiostatrunner()
		curUtilPct := thrctx.prevUtilPct

		if now.After(thrctx.nextUtilCheck) {
			curUtilPct, ok = iostatr.maxUtilFS(params.fs)
			thrctx.nextUtilCheck = now.Add(ctx.config.Periodic.StatsTime)
			if !ok {
				curUtilPct = thrctx.prevUtilPct
				glog.Errorf("Unable to retrieve disk utilization for fs %s", params.fs)
			}
		}

		if curUtilPct > float32(ctx.config.Xaction.DiskUtilHighWM) {
			if thrctx.sleep < initThrottleSleep {
				thrctx.sleep = initThrottleSleep
			} else {
				thrctx.sleep *= 2
			}
		} else if curUtilPct < float32(ctx.config.Xaction.DiskUtilLowWM) {
			thrctx.sleep = 0
		} else {
			if thrctx.sleep < initThrottleSleep {
				thrctx.sleep = initThrottleSleep
			}
			multiplier := (curUtilPct - float32(ctx.config.Xaction.DiskUtilLowWM)) /
				float32(ctx.config.Xaction.DiskUtilHighWM-ctx.config.Xaction.DiskUtilLowWM)
			thrctx.sleep = thrctx.sleep + time.Duration(multiplier*float32(thrctx.sleep))
		}
		if thrctx.sleep > maxThrottleSleep {
			thrctx.sleep = maxThrottleSleep
		}
		thrctx.prevUtilPct = curUtilPct
	}
}
