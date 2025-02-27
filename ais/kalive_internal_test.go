// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"testing"
	"time"
)

func TestHB(t *testing.T) {
	hb := newHB(time.Millisecond * 10)

	if !hb.TimedOut("unknown server") {
		t.Fatal("None existing server should return timed out")
	}

	id1 := "1"
	hb.HeardFrom(id1, 0 /*now*/)
	time.Sleep(time.Millisecond * 1)

	if hb.TimedOut(id1) {
		t.Fatal("Expecting no timeout")
	}

	time.Sleep(time.Millisecond * 10)

	if !hb.TimedOut(id1) {
		t.Fatal("Expecting timeout")
	}

	hb.HeardFrom(id1, 0 /*now*/)
	time.Sleep(time.Millisecond * 11)
	hb.HeardFrom(id1, 0 /*now*/)
	if hb.TimedOut(id1) {
		t.Fatal("Expecting no timeout")
	}

	time.Sleep(time.Millisecond * 10)
	id2 := "2"
	hb.HeardFrom(id2, 0 /*now*/)

	if hb.TimedOut(id2) {
		t.Fatal("Expecting no timeout")
	}

	if !hb.TimedOut(id1) {
		t.Fatal("Expecting timeout")
	}
}
