// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import "time"

// common throttling constants
const (
	Throttle1ms   = time.Millisecond
	Throttle10ms  = 10 * time.Millisecond
	Throttle100ms = 100 * time.Millisecond

	throttleBatch = 0x1f      // a.k.a. unit or period
	throMiniBatch = 0x1f >> 1 // ditto
)

func IsThrottle(n int64) bool     { return n&throttleBatch == throttleBatch }
func IsMiniThrottle(n int64) bool { return n&throMiniBatch == throMiniBatch }
