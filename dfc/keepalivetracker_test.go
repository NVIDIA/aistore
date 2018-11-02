/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dfc

import (
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/dfc/statsd"
)

func TestKeepaliveTrackerHeartBeat(t *testing.T) {
	hb := newHeartBeatTracker(time.Millisecond*10, &statsd.Client{})

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
