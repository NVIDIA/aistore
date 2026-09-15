// Package xact_test tests xaction wait conditions.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xact_test

import (
	"testing"
	"time"

	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/xact"
)

func TestSnapsFinishedWaitsForReportedInstances(t *testing.T) {
	const xid = "xaction-1"
	start, end := time.Now(), time.Now().Add(time.Second)
	snaps := xact.MultiSnap{
		"t1": {{ID: xid, StartTime: start, EndTime: end, IdleX: true}},
		"t2": {{ID: xid, StartTime: start}},
	}
	cond := (&xact.ArgsMsg{ID: xid}).Finished()

	done, _, err := cond(snaps)
	if err != nil || done {
		t.Fatalf("expected one running target: done=%t, err=%v", done, err)
	}

	// An idle xaction can still accept work: idle is not finished.
	snaps["t2"][0].IdleX = true
	done, _, err = cond(snaps)
	if err != nil || done {
		t.Fatalf("expected one idle but unfinished target: done=%t, err=%v", done, err)
	}

	snaps["t2"][0].EndTime = end
	done, _, err = cond(snaps)
	if err != nil || !done {
		t.Fatalf("expected all reported instances finished: done=%t, err=%v", done, err)
	}
}

func TestSnapsFinishedTerminalState(t *testing.T) {
	const xid = "xaction-1"
	start := time.Now()
	for _, tc := range []struct {
		name  string
		snaps xact.MultiSnap
		done  bool
	}{
		{name: "not visible"},
		{name: "unrelated ID", snaps: xact.MultiSnap{"t1": {{ID: "xaction-2", StartTime: start, EndTime: start}}}},
		{name: "not started", snaps: xact.MultiSnap{"t1": {{ID: xid}}}},
		{name: "finished without idle flag", snaps: xact.MultiSnap{"t1": {{ID: xid, StartTime: start, EndTime: start}}}, done: true},
		{name: "finished with empty target", snaps: xact.MultiSnap{
			"t1": {{ID: xid, StartTime: start, EndTime: start}},
			"t2": nil,
		}, done: true},
		{name: "finished with unrelated target", snaps: xact.MultiSnap{
			"t1": {{ID: xid, StartTime: start, EndTime: start}},
			"t2": {{ID: "xaction-2", StartTime: start}},
		}, done: true},
		{name: "abort overrides running", snaps: xact.MultiSnap{
			"t1": {{ID: xid, StartTime: start}},
			"t2": {{ID: xid, StartTime: start, AbortedX: true}},
		}, done: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			done, reset, err := (&xact.ArgsMsg{ID: xid}).Finished()(tc.snaps)
			if err != nil || reset || done != tc.done {
				t.Fatalf("done=%t, want=%t, reset=%t, err=%v", done, tc.done, reset, err)
			}
		})
	}
}

func TestAggregateStatePreservesIdle(t *testing.T) {
	snap := &core.Snap{ID: "xaction-1", StartTime: time.Now(), IdleX: true}
	snaps := xact.MultiSnap{"t1": {snap}}
	for _, xid := range []string{snap.ID, ""} {
		aborted, running, notstarted := snaps.AggregateState(xid)
		if aborted || running || notstarted {
			t.Fatalf("ID=%q: expected idle state, got aborted=%t, running=%t, notstarted=%t", xid, aborted, running, notstarted)
		}
	}
}
