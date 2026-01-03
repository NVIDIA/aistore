// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/xact"
)

//
// Xactions: distributed batch jobs (see docs/overview.md and docs/batch.md for details)
//
// Waiting/Getting/Querying API-wise, there are two *data models* you can poll:
//
// (1) IC-notifying xactions ("status"-based)
// ------------------------------------------
//   Under the hood: GetOneXactionStatus (IC proxy aggregated status)
//   Typical examples: rebalance, ETL (cluster-controlled)
//
// (2) All xactions including those that do not notify IC ("snaps"-based)
// --------------------------------------------------
//   Under the hood: QueryXactionSnaps (per-target snapshots)
//   Typical examples: resilver, blob-download, dsort, warm-up, etc.
//
// Selection rules:
//   - args.Kind is a filter: querying with a kind returns only that kind (server-side);
//   - args.ID is a filter: querying with UUID returns only that UUID (server-side);
//   - kind-only waits are valid (may match multiple UUIDs).
//

//
// built-in conditions
//

// return:
//   - done: whether to stop waiting
//   - reset: whether to reset polling sleep back to xact.MinPollTime
type SnapsCond func(xact.MultiSnap) (done, reset bool)

// return:
//   - done: whether to stop waiting
//   - reset: whether to reset polling sleep back to xact.MinPollTime (rarely needed)
type StatusCond func(*nl.Status) (done, reset bool)

// Wait until the selected xaction(s) are no longer running.
// If id is empty, it applies to all UUIDs present in the returned snaps (kind-filtered server-side).
func SnapsFinished(id string) SnapsCond {
	// when waiting kind-only, avoid finishing immediately on an empty snapshot
	// (e.g., transient visibility, or "not yet visible" early in the wait)
	var seen bool
	return func(snaps xact.MultiSnap) (bool, bool) {
		if id == "" {
			uuids := snaps.GetUUIDs()
			if len(uuids) == 0 {
				if !seen {
					return false, false
				}
			} else {
				seen = true
			}
		}
		aborted, running, _ := snaps.AggregateState(id)
		return aborted || !running, false
	}
}

// Wait until the selected xaction becomes idle for
// xact.NumConsecutiveIdle polls. Use this for xactions that "idle before finishing".
func SnapsConsecutiveIdle(id string) SnapsCond {
	var (
		cnt     int
		delayed bool
	)
	return func(snaps xact.MultiSnap) (done, reset bool) {
		aborted, running, notstarted := snaps.AggregateState(id)
		if aborted {
			return true, false
		}
		if running {
			cnt = 0
			return false, true // progress: probe sooner
		}
		if notstarted && cnt == 0 {
			// preserve legacy behavior: avoid mistaking "not yet visible" for "idle"
			if !delayed {
				time.Sleep(min(2*xact.MinPollTime, 4*time.Second))
				delayed = true
			}
			return false, false
		}
		// idle
		cnt++
		return cnt >= xact.NumConsecutiveIdle, false
	}
}

func StatusFinished(st *nl.Status) (bool, bool) { return st.IsFinished(), false }

//
// Snaps-based API
//

// Poll QueryXactionSnaps until cond returns done=true.
// If cond is nil, it waits until selected xaction(s) are no longer running (finished or aborted).
//
// If args.ID is provided, the wait is for that UUID.
// If args.ID is empty (kind-only), the wait is for "no longer running" across UUIDs present
// in the returned snaps (server-side kind filter applies).
func WaitForSnaps(bp BaseParams, args *xact.ArgsMsg, cond SnapsCond) (xact.MultiSnap, error) {
	if err := _validateXargs(args); err != nil {
		return nil, err
	}
	if cond == nil {
		cond = SnapsFinished(args.ID)
	}
	return pollSnaps(bp, args, cond)
}

// Query just once (no polling)
func GetSnaps(bp BaseParams, args *xact.ArgsMsg) (xact.MultiSnap, error) {
	if err := _validateXargs(args); err != nil {
		return nil, err
	}
	return QueryXactionSnaps(bp, args)
}

// Wait until it can observe a selected xaction as *running* and returns its UUID.
// This is the "no extra network round-trip" helper for flows where the UUID is not known upfront.
// - If args.ID is set: waits until that UUID becomes visible (not "notstarted") and returns it.
// - If args.ID is empty: waits until any matching xaction is running, returns its UUID.
// NOTE:
// Waiting for a job to start, especially cluster-wide, is inherently racy - it depends on per-target
// workload, server hardware, network, etc. Use with caution!
func WaitForSnapsStarted(bp BaseParams, args *xact.ArgsMsg) (xid string, snaps xact.MultiSnap, err error) {
	if err := _validateXargs(args); err != nil {
		return "", nil, err
	}
	var runErr error
	cond := func(ms xact.MultiSnap) (bool, bool) {
		if args.ID != "" {
			_, _, notstarted := ms.AggregateState(args.ID)
			if notstarted {
				return false, false
			}
			xid = args.ID
			return true, true
		}
		// discover a single running UUID (preferred) and surface ambiguity
		_, snap, err := ms.RunningTarget("")
		if err != nil {
			runErr = err
			return true, false // stop polling; caller will return runErr
		}
		if snap != nil {
			xid = snap.ID
			return true, true
		}
		return false, false
	}

	snaps, err = pollSnaps(bp, args, cond)
	if err == nil && runErr != nil {
		return "", snaps, runErr
	}
	return xid, snaps, err
}

//
// Status-based API
//

// Poll GetOneXactionStatus until cond returns done=true.
// If cond is nil, it waits until status.IsFinished().
//
// Note: kind-only waits are supported (args.ID empty). Server-side filtering applies.
func WaitForStatus(bp BaseParams, args *xact.ArgsMsg, cond StatusCond) (*nl.Status, error) {
	if err := _validateXargs(args); err != nil {
		return nil, err
	}
	if cond == nil {
		cond = StatusFinished
	}
	return pollStatus(bp, args, cond)
}

// Query IC once (no polling)
func GetStatus(bp BaseParams, args *xact.ArgsMsg) (*nl.Status, error) {
	if err := _validateXargs(args); err != nil {
		return nil, err
	}
	return GetOneXactionStatus(bp, args)
}

//
// Internal polling (shared backoff logic)
//

func pollSnaps(bp BaseParams, args *xact.ArgsMsg, cond SnapsCond) (xact.MultiSnap, error) {
	return pollAny("api.wait-snaps",
		args,
		func() (xact.MultiSnap, error) { return QueryXactionSnaps(bp, args) },
		cond)
}

func pollStatus(bp BaseParams, args *xact.ArgsMsg, cond StatusCond) (*nl.Status, error) {
	return pollAny("api.wait-status",
		args,
		func() (*nl.Status, error) { return GetOneXactionStatus(bp, args) },
		cond)
}

// common polling loop used by both snaps- and status-based waiters
func pollAny[T any](prefix string, args *xact.ArgsMsg, apicb func() (T, error), cond func(T) (done, reset bool)) (T, error) {
	var (
		total, maxSleep = _times(args)
		sleep           = xact.MinPollTime
		begin           = mono.NanoTime()

		lastGood T
		hasGood  bool
	)

	for {
		val, err := apicb()
		if err != nil {
			if !_isRetriable(err) {
				if hasGood {
					return lastGood, err
				}
				return val, err
			}
		} else {
			lastGood = val
			hasGood = true
			done, reset := cond(val)
			if done {
				return val, nil
			}
			if reset {
				sleep = xact.MinPollTime
			}
		}

		time.Sleep(sleep)
		sleep = min(maxSleep, sleep+sleep/2)

		if mono.Since(begin) >= total {
			if hasGood {
				return lastGood, fmt.Errorf("%s: timed out (%v) waiting for %s", prefix, total, args.String())
			}
			var zero T
			return zero, fmt.Errorf("%s: timed out (%v) waiting for %s", prefix, total, args.String())
		}
	}
}
