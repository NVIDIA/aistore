// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"testing"
	"time"
)

const (
	daemonID     = "12345"
	maxKeepalive = 4 * time.Second
)

func TestTimeoutStatsForDaemon(t *testing.T) {
	k := &keepalive{
		tt:           &timeoutTracker{timeoutStats: make(map[string]*timeoutStats)},
		maxKeepalive: int64(maxKeepalive),
	}
	ts := k.timeoutStats(daemonID)
	if ts == nil {
		t.Fatal("timeoutStats should not be nil")
	}
	timeout := time.Duration(ts.timeout)
	srtt := time.Duration(ts.srtt)
	rttvar := time.Duration(ts.rttvar)
	if timeout != maxKeepalive {
		t.Errorf("initial timeout should be: %v, got: %v", maxKeepalive, timeout)
	}
	if srtt != maxKeepalive {
		t.Errorf("initial srtt should be: %v, got: %v", maxKeepalive, srtt)
	}
	if rttvar != maxKeepalive/2 {
		t.Errorf("initial rttvar should be: %v, got: %v", maxKeepalive/2, rttvar)
	}
}

func TestUpdateTimeoutForDaemon(t *testing.T) {
	{
		k := &keepalive{
			tt:           &timeoutTracker{timeoutStats: make(map[string]*timeoutStats)},
			maxKeepalive: int64(maxKeepalive),
		}
		initial := k.timeoutStats(daemonID)
		nextRTT := time.Duration(initial.srtt * 3 / 4)
		nextTimeout := k.updateTimeoutFor(daemonID, nextRTT)
		if nextTimeout <= nextRTT {
			t.Errorf("updated timeout: %v should be greater than most recent RTT: %v", nextTimeout, nextRTT)
		} else if nextTimeout > maxKeepalive {
			t.Errorf("updated timeout: %v should be lesser than or equal to max keepalive time: %v",
				nextTimeout, maxKeepalive)
		}
	}
	{
		k := &keepalive{
			tt:           &timeoutTracker{timeoutStats: make(map[string]*timeoutStats)},
			maxKeepalive: int64(maxKeepalive),
		}
		initial := k.timeoutStats(daemonID)
		nextRTT := time.Duration(initial.srtt + initial.srtt/10)
		nextTimeout := k.updateTimeoutFor(daemonID, nextRTT)
		if nextTimeout != maxKeepalive {
			t.Errorf("updated timeout: %v should be equal to the max keepalive timeout: %v",
				nextTimeout, maxKeepalive)
		}
	}
	{
		k := &keepalive{
			tt:           &timeoutTracker{timeoutStats: make(map[string]*timeoutStats)},
			maxKeepalive: int64(maxKeepalive),
		}
		for i := 0; i < 100; i++ {
			initial := k.timeoutStats(daemonID)
			nextRTT := time.Duration(initial.srtt / 4)
			nextTimeout := k.updateTimeoutFor(daemonID, nextRTT)
			// Eventually, the `nextTimeout` must converge and stop at `maxKeepalive/2`.
			if i > 25 && nextTimeout != maxKeepalive/2 {
				t.Errorf("updated timeout: %v should be equal to the min keepalive timeout: %v",
					nextTimeout, maxKeepalive/2)
			}
		}
	}
}

func TestHB(t *testing.T) {
	hb := newHBTracker(time.Millisecond * 10)

	if !hb.TimedOut("unknown server") {
		t.Fatal("None existing server should return timed out")
	}

	id1 := "1"
	hb.HeardFrom(id1, false)
	time.Sleep(time.Millisecond * 1)

	if hb.TimedOut(id1) {
		t.Fatal("Expecting no time out")
	}

	time.Sleep(time.Millisecond * 10)

	if !hb.TimedOut(id1) {
		t.Fatal("Expecting time out")
	}

	hb.HeardFrom(id1, false)
	time.Sleep(time.Millisecond * 11)
	hb.HeardFrom(id1, false)
	if hb.TimedOut(id1) {
		t.Fatal("Expecting no time out")
	}

	time.Sleep(time.Millisecond * 10)
	id2 := "2"
	hb.HeardFrom(id2, false)

	if hb.TimedOut(id2) {
		t.Fatal("Expecting no time out")
	}

	if !hb.TimedOut(id1) {
		t.Fatal("Expecting time out")
	}
}
