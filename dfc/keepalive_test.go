/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

package dfc

import (
	"testing"
	"time"
)

const (
	daemonID         = "12345"
	maxKeepaliveTime = 4 * time.Second
)

func TestTimeoutStatsForDaemon(t *testing.T) {
	k := &keepalive{
		tt:               &timeoutTracker{timeoutStatsMap: make(map[string]*timeoutStats)},
		maxKeepaliveTime: float64(maxKeepaliveTime.Nanoseconds()),
	}
	ts := k.timeoutStatsForDaemon(daemonID)
	if ts == nil {
		t.Fatal("timeoutStats should not be nil")
	}
	timeout := time.Duration(ts.timeout)
	srtt := time.Duration(ts.srtt)
	rttvar := time.Duration(ts.rttvar)
	if timeout != maxKeepaliveTime {
		t.Errorf("initial timeout should be: %v, got: %v", maxKeepaliveTime, timeout)
	}
	if srtt != maxKeepaliveTime {
		t.Errorf("initial srtt should be: %v, got: %v", maxKeepaliveTime, srtt)
	}
	if rttvar != maxKeepaliveTime/2 {
		t.Errorf("initial rttvar should be: %v, got: %v", maxKeepaliveTime/2, rttvar)
	}
}

func TestUpdateTimeoutForDaemon(t *testing.T) {
	{
		k := &keepalive{
			tt:               &timeoutTracker{timeoutStatsMap: make(map[string]*timeoutStats)},
			maxKeepaliveTime: float64(maxKeepaliveTime.Nanoseconds()),
		}
		initial := k.timeoutStatsForDaemon(daemonID)
		nextRTT := time.Duration(initial.srtt * 0.75)
		nextTimeout := k.updateTimeoutForDaemon(daemonID, nextRTT)
		if nextTimeout <= nextRTT {
			t.Errorf("updated timeout: %v should be greater than most recent RTT: %v", nextTimeout, nextRTT)
		} else if nextTimeout > maxKeepaliveTime {
			t.Errorf("updated timeout: %v should be lesser than or equal to max keepalive time: %v",
				nextTimeout, maxKeepaliveTime)
		}
	}
	{
		k := &keepalive{
			tt:               &timeoutTracker{timeoutStatsMap: make(map[string]*timeoutStats)},
			maxKeepaliveTime: float64(maxKeepaliveTime.Nanoseconds()),
		}
		initial := k.timeoutStatsForDaemon(daemonID)
		nextRTT := time.Duration(initial.srtt * 1.1)
		nextTimeout := k.updateTimeoutForDaemon(daemonID, nextRTT)
		if nextTimeout != maxKeepaliveTime {
			t.Errorf("updated timeout: %v should be equal to the max keepalive timeout: %v",
				nextTimeout, maxKeepaliveTime)
		}
	}
	{
		k := &keepalive{
			tt:               &timeoutTracker{timeoutStatsMap: make(map[string]*timeoutStats)},
			maxKeepaliveTime: minKeepaliveTime}
		for i := 0; i < 100; i++ {
			initial := k.timeoutStatsForDaemon(daemonID)
			nextRTT := time.Duration(initial.srtt * 0.25)
			nextTimeout := k.updateTimeoutForDaemon(daemonID, nextRTT)
			if nextTimeout != time.Duration(minKeepaliveTime) {
				t.Errorf("updated timeout: %v should be equal to the min keepalive timeout: %v",
					nextTimeout, time.Duration(minKeepaliveTime))
			}
		}
	}

}
