// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// global waiting tunables
const (
	DefWaitTimeShort = time.Minute        // zero `ArgsMsg.Timeout` defaults to
	DefWaitTimeLong  = 7 * 24 * time.Hour // when `ArgsMsg.Timeout` is negative
	MaxProbingFreq   = 30 * time.Second   // as the name implies
	MinPollTime      = 2 * time.Second    // ditto
	MaxPollTime      = 2 * time.Minute    // can grow up to

	// number of consecutive 'idle' xaction states, with possible numeric
	// values translating as follows:
	// 1: fully rely on xact.IsIdle() logic with no extra checks whatsoever
	// 2: one additional IsIdle() call after MinPollTime
	// 3: two additional IsIdle() calls spaced at MinPollTime interval, and so on.
	numConsecutiveIdle = 2

	// stability window for kind-only wait-for-finished
	// (see snapsFinished below)
	numConsecutiveEmpty = 3
)

// ArgsMsg.Flags
// note: for simplicity, keeping all custom x-flags in one place and one global enum for now
const (
	FlagZeroSize = 1 << iota // usage: x-cleanup (apc.ActStoreCleanup) to remove zero size objects
	FlagLatestVer
	FlagSync
	FlagKeepMisplaced // usage: x-cleanup to _not_ remove (ie, keep) misplaced objects
)

type (
	// either xaction ID or Kind must be specified
	// is getting passed via ActMsg.Value w/ MorphMarshal extraction
	ArgsMsg struct {
		ID          string        // xaction UUID
		Kind        string        // xaction kind _or_ name (see `xact.Table`)
		DaemonID    string        // node that runs this xaction
		Bck         cmn.Bck       // bucket
		Buckets     []cmn.Bck     // list of buckets (e.g., copy-bucket, lru-evict, etc.)
		Timeout     time.Duration // max time to wait
		Flags       uint32        `json:"flags,omitempty"` // enum (FlagZeroSize, ...) bitwise
		Force       bool          // force
		OnlyRunning bool          // only for running xactions
	}
)

func (args *ArgsMsg) String() string {
	var sb cos.SB
	sb.Init(128)
	sb.WriteString("xa-")
	sb.WriteString(args.Kind)
	sb.WriteUint8('[')
	if args.ID != "" {
		sb.WriteString(args.ID)
	}
	sb.WriteUint8(']')
	if !args.Bck.IsEmpty() {
		sb.WriteUint8('-')
		sb.WriteString(args.Bck.String())
	}
	if args.Timeout > 0 {
		sb.WriteUint8('-')
		sb.WriteString(args.Timeout.String())
	}
	if args.DaemonID != "" {
		sb.WriteString("-node[")
		sb.WriteString(args.DaemonID)
		sb.WriteUint8(']')
	}
	if args.Flags > 0 {
		sb.WriteString("-0x")
		sb.WriteString(strconv.FormatUint(uint64(args.Flags), 16))
	}
	return sb.String()
}

//
// SnapsCond: condition functions for snaps-based polling (api.WaitForSnaps)
//

// SnapsCond is the condition function signature for snaps-based polling.
// Returns:
//   - done: whether to stop waiting
//   - reset: whether to reset polling sleep back to MinPollTime
//   - err: error to propagate (stops polling immediately)
type SnapsCond func(MultiSnap) (done, reset bool, err error)

// --- Finished condition ---
//
// Notice the distinction between Finished and NotRunning (below)
//
// - args.ID != "": wait for that UUID to become visible and reach a terminal state
//   (finished OR aborted).
// - kind-only wait (args.ID == ""): require seeing at least one matching xaction first,
//   then complete when no matching *running* xactions are visible anymore (stable empty snaps).
//   This relies on args.OnlyRunning=true (set below) to avoid stale completed jobs.

type snapsFinished struct {
	id    string
	seen  bool
	empty int
}

func (c *snapsFinished) check(snaps MultiSnap) (bool, bool, error) {
	if c.id != "" {
		// specific UUID: wait until observed and terminal (finished OR aborted)
		for _, tsnaps := range snaps {
			for _, snap := range tsnaps {
				if snap.ID == c.id {
					return snap.IsFinished() || snap.IsAborted(), false, nil
				}
			}
		}
		// keep polling
		return false, false, nil
	}

	// kind-only: require seeing at least one running xaction, then stable "emptiness"
	if !snaps.hasUUIDs() {
		if !c.seen {
			return false, false, nil
		}
		c.empty++
		return c.empty >= numConsecutiveEmpty, true, nil // "reset" => probe sooner after progress
	}

	c.seen = true
	c.empty = 0
	return false, false, nil
}

func (args *ArgsMsg) Finished() SnapsCond {
	if args.ID == "" {
		args.OnlyRunning = true
	}
	return (&snapsFinished{id: args.ID}).check
}

// --- NotRunning condition ---
// Succeed immediately if nothing is running.
// Unlike Finished, this has no "seen" tracking - use for pre-condition checks
// ("make sure nothing's running before I start") rather than post-action waits
// ("started and completed").

type snapsNotRunning struct {
	id string
}

func (c *snapsNotRunning) check(snaps MultiSnap) (bool, bool, error) {
	_, running, _ := snaps.AggregateState(c.id)
	return !running, false, nil
}

func (args *ArgsMsg) NotRunning() SnapsCond {
	return (&snapsNotRunning{id: args.ID}).check
}

// --- Started condition ---
// Wait until the xaction is visible/running.
// NOTE: waiting for a job to start, especially cluster-wide, is inherently racy -
// it depends on per-target workload, server hardware, network, etc. Use with caution!

type snapsStarted struct {
	id string
}

func (c *snapsStarted) check(snaps MultiSnap) (bool, bool, error) {
	if c.id != "" {
		_, _, notstarted := snaps.AggregateState(c.id)
		return !notstarted, true, nil
	}
	// kind-only: wait until something is running
	_, snap, err := snaps.RunningTarget("")
	if err != nil {
		return true, false, err // ambiguity: multiple running UUIDs
	}
	return snap != nil, true, nil
}

func (args *ArgsMsg) Started() SnapsCond {
	return (&snapsStarted{id: args.ID}).check
}

// --- Idle condition ---
// Wait until the xaction becomes idle for numConsecutiveIdle consecutive polls.
//
// Background:
// - Some xactions "idle before finishing" - they remain running but have no work
//   items to process for a period of time
//   (e.g., apc.ActECPut, apc.ActPutCopies, apc.ActGetBatch, and more).
//   An idle xaction is still *running* - it just has no work items at the moment.
//   For such xactions, "finished" may be delayed long after useful work is done.
// Therefore:
//   - Use WaitForSnaps(..., args.Idle()) rather than a "finished" condition.
//   - See xact.IdlesBeforeFinishing(kind) to check if a given xaction kind _idles_.

type snapsIdle struct {
	id      string
	cnt     int
	delayed bool
}

func (c *snapsIdle) check(snaps MultiSnap) (bool, bool, error) {
	aborted, running, notstarted := snaps.AggregateState(c.id)
	if aborted {
		return true, false, nil
	}
	if running {
		c.cnt = 0
		return false, false, nil
	}
	if notstarted && c.cnt == 0 {
		// preserve legacy behavior: avoid mistaking "not yet visible" for "idle"
		if !c.delayed {
			time.Sleep(min(2*MinPollTime, 4*time.Second))
			c.delayed = true
		}
		return false, false, nil
	}
	// idle
	c.cnt++
	return c.cnt >= numConsecutiveIdle, true, nil
}

func (args *ArgsMsg) Idle() SnapsCond {
	args.OnlyRunning = true
	return (&snapsIdle{id: args.ID}).check
}
