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
// Snaps-based API ----------------------------------------------------------------
//

// Poll QueryXactionSnaps() until `cond` returns done=true; if `cond` is nil, default to args.Finished()
func WaitForSnaps(bp BaseParams, args *xact.ArgsMsg, cond xact.SnapsCond) (xact.MultiSnap, error) {
	if err := _validateXargs(args); err != nil {
		return nil, err
	}

	// some built-in conditions (args.Finished/args.Idle) may temporarily change args.OnlyRunning
	orig := args.OnlyRunning
	defer func() { args.OnlyRunning = orig }()

	if cond == nil {
		cond = args.Finished()
	}

	return pollSnaps(bp, args, cond)
}

// Query once (no polling).
func GetSnaps(bp BaseParams, args *xact.ArgsMsg) (xact.MultiSnap, error) {
	if err := _validateXargs(args); err != nil {
		return nil, err
	}
	return QueryXactionSnaps(bp, args)
}

// Wait for a given on-demand xaction to become idle.
func WaitForSnapsIdle(bp BaseParams, args *xact.ArgsMsg) error {
	_, err := WaitForSnaps(bp, args, args.Idle())
	return err
}

//
// Status-based API ---------------------------------------------------------------
//

// Condition function signature for status-based polling.
// Returns:
//   - done: whether to stop waiting
//   - reset: whether to reset polling sleep back to xact.MinPollTime (rarely needed)
//   - err: error to propagate (stops polling immediately)
type StatusCond func(*nl.Status) (done, reset bool, err error)

// --- Finished condition ---

func statusFinished(st *nl.Status) (bool, bool, error) {
	return st.IsFinished(), false, nil
}

// StatusFinished waits until status.IsFinished() returns true.
func StatusFinished() StatusCond {
	return statusFinished
}

//
// Status-based wait/get functions
//

// WaitForStatus polls GetOneXactionStatus until cond returns done=true.
// If cond is nil, it waits until status.IsFinished().
func WaitForStatus(bp BaseParams, args *xact.ArgsMsg, cond StatusCond) (*nl.Status, error) {
	if err := _validateXargs(args); err != nil {
		return nil, err
	}
	if cond == nil {
		cond = StatusFinished()
	}
	return pollStatus(bp, args, cond)
}

// GetStatus queries IC once (no polling).
func GetStatus(bp BaseParams, args *xact.ArgsMsg) (*nl.Status, error) {
	if err := _validateXargs(args); err != nil {
		return nil, err
	}
	return GetOneXactionStatus(bp, args)
}

//
// Internal polling (shared backoff logic)
//

func pollSnaps(bp BaseParams, args *xact.ArgsMsg, cond xact.SnapsCond) (xact.MultiSnap, error) {
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

// pollAny is the common polling loop used by both snaps- and status-based waiters.
func pollAny[T any](prefix string, args *xact.ArgsMsg, apicb func() (T, error), cond func(T) (bool, bool, error)) (T, error) {
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

			done, reset, condErr := cond(val)
			if condErr != nil {
				return val, condErr
			}
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
