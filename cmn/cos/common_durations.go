// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"time"

	"github.com/NVIDIA/aistore/cmn/debug"
)

// Common sleep intervals for polling loops and retry-style backoffs.
// Not intended for high-frequency spinning or network-level timeouts.
// See also: hk/common_durations
const (
	PollSleepShort  = 100 * time.Millisecond
	PollSleepMedium = 250 * time.Millisecond
	PollSleepLong   = 500 * time.Millisecond
)

// wait duration => probing frequency
func ProbingFrequency(dur time.Duration) time.Duration {
	sleep := min(dur>>3, time.Second)
	sleep = max(dur>>6, sleep)
	return max(sleep, PollSleepShort)
}

// constrain duration `d` to the closed interval [mind, maxd]
// (see also: ClampInt)
func ClampDuration(d, mind, maxd time.Duration) time.Duration {
	debug.Assert(mind <= maxd, mind, " vs ", maxd)
	if d < mind {
		return mind
	}
	return min(d, maxd)
}
