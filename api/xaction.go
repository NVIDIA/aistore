// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/xact"
)

// Xaction client API
//
// Introduction
// ============
// Xactions (short for 'extended actions'): distributed batch jobs (see docs/overview.md and docs/batch.md for details).
// Xactions can be started, stopped (aborted), queried, and waited on.
//
// This package provides client-side entrypoints used by CLI, SDKs, and tests. The actual xaction control structures
// and semantics live in the core `xact` package.
//
// Quick map
// =========
//   - Query snapshots (registry / per-node view):
//       QueryXactionSnaps(bp, args) -> xact.MultiSnap
//       WaitForSnaps(bp, args, cond) polls QueryXactionSnaps until cond is satisfied.
//
//   - Query status (IC / cluster-wide view):
//       GetOneXactionStatus / WaitForStatus (polls IC) -> *nl.Status
//
//   - Conditions ("started", "finished", "idle"):
//       Implemented in `xact` as methods on xact.ArgsMsg:
//         args.Started(), args.Finished(), args.NotRunning(), args.Idle()
//
// Notes
// =====
//   - Information Center (IC) is a group of up to 3 (or configured) AIStore gateways that
//     always include the primary (see docs/ic.md).
//
//   - Some xactions do not report status to the IC. For those, prefer snaps-based waits
//     (WaitForSnaps + args.* conditions).
//
//   - Some xactions "idle before finishing" - they remain running but have no work
//     items to process for a period of time
//     (e.g., apc.ActECPut, apc.ActPutCopies, apc.ActGetBatch, and more).
//     An idle xaction is still *running* - it just has no work items at the moment.
//     For such xactions, "finished" may be delayed long after useful work is done.
//     Therefore:
//     - Use WaitForSnaps(..., args.Idle()) rather than a "finished" condition.
//     - See xact.IdlesBeforeFinishing(kind) to check if a given xaction kind _idles_.
//
// See also: xact.ArgsMsg, xact.MultiSnap, xact.IdlesBeforeFinishing.
// Related documentation: docs/overview.md, docs/batch.md, docs/cli.md

func StartXaction(bp BaseParams, args *xact.ArgsMsg, extra string /* e.g. blob-downloader objname */) (xid string, err error) {
	if !xact.Table[args.Kind].Startable {
		return "", fmt.Errorf("xaction %q is not startable", args.Kind)
	}
	q := qalloc()
	args.Bck.SetQuery(q)
	if args.Force {
		q.Set(apc.QparamForce, "true")
	}
	msg := apc.ActMsg{Action: apc.ActXactStart, Value: args, Name: extra}

	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}
	_, err = reqParams.doReqStr(&xid)

	FreeRp(reqParams)
	qfree(q)
	return xid, err
}

// a.k.a. stop
func AbortXaction(bp BaseParams, args *xact.ArgsMsg) (err error) {
	var (
		q   = qalloc()
		msg = apc.ActMsg{Action: apc.ActXactStop, Value: args}
	)

	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		args.Bck.SetQuery(q)
		reqParams.Query = q
	}
	err = reqParams.DoRequest()

	FreeRp(reqParams)
	qfree(q)
	return err
}

//
// querying and waiting
//

// returns a slice of canonical xaction names, as in: `xact.Cname()`
// e.g.: put-copies[D-ViE6HEL_j] list[H96Y7bhR2s] copy-bck[matRQMRes] put-copies[pOibtHExY]
// TODO: return idle xactions separately
func GetAllRunningXactions(bp BaseParams, kindOrName string) (out []string, err error) {
	var (
		msg       = xact.QueryMsg{Kind: kindOrName}
		q         = qalloc()
		reqParams = AllocRp()
	)
	q.Set(apc.QparamWhat, apc.WhatAllRunningXacts)
	bp.Method = http.MethodGet
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}

	_, err = reqParams.DoReqAny(&out)

	FreeRp(reqParams)
	qfree(q)
	return out, err
}

// QueryXactionSnaps gets all xaction snaps based on the specified selection.
// NOTE: args.Kind can be either xaction kind or name - here and elsewhere
func QueryXactionSnaps(bp BaseParams, args *xact.ArgsMsg) (xs xact.MultiSnap, err error) {
	var (
		msg = xact.QueryMsg{ID: args.ID, Kind: args.Kind, Bck: args.Bck}
		q   = qalloc()
	)
	if args.OnlyRunning {
		msg.OnlyRunning = apc.Ptr(true)
	}
	q.Set(apc.QparamWhat, apc.WhatQueryXactStats)
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}

	_, err = reqParams.DoReqAny(&xs)

	FreeRp(reqParams)
	qfree(q)
	return xs, err
}

// GetOneXactionStatus queries one of the IC (proxy) members for status
// of the `args`-identified xaction.
// NOTE:
// - is used internally by the WaitForXactionIC() helper function (to wait on xaction)
// - returns a single matching xaction or none;
// - when the `args` filter "covers" multiple xactions the returned status corresponds to
// any matching xaction that's currently running, or - if nothing's running -
// the one that's finished most recently,
// if exists
func GetOneXactionStatus(bp BaseParams, args *xact.ArgsMsg) (status *nl.Status, err error) {
	status = &nl.Status{}
	q := qalloc()
	q.Set(apc.QparamWhat, apc.WhatOneXactStatus)

	err = getxst(status, q, bp, args)

	qfree(q)
	return status, err
}

// same as above, except that it returns _all_ matching xactions
func GetAllXactionStatus(bp BaseParams, args *xact.ArgsMsg) (matching nl.StatusVec, err error) {
	q := qalloc()
	q.Set(apc.QparamWhat, apc.WhatAllXactStatus)
	if args.Force {
		// (force just-in-time)
		// for each args-selected xaction:
		// check if any of the targets delayed updating the corresponding status,
		// and query those targets directly
		q.Set(apc.QparamForce, "true")
	}

	err = getxst(&matching, q, bp, args)

	qfree(q)
	return matching, err
}

func getxst(out any, q url.Values, bp BaseParams, args *xact.ArgsMsg) (err error) {
	bp.Method = http.MethodGet
	msg := xact.QueryMsg{ID: args.ID, Kind: args.Kind, Bck: args.Bck}
	if args.OnlyRunning {
		msg.OnlyRunning = apc.Ptr(true)
	}
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}
	_, err = reqParams.DoReqAny(out)
	FreeRp(reqParams)
	return
}

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
// See related xact.IdlesBeforeFinishing()
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

// Query IC once (no polling).
func GetStatus(bp BaseParams, args *xact.ArgsMsg) (*nl.Status, error) {
	if err := _validateXargs(args); err != nil {
		return nil, err
	}
	return GetOneXactionStatus(bp, args)
}

// Usage: xactions that report status back to IC (e.g. rebalance).
func WaitForXactionIC(bp BaseParams, args *xact.ArgsMsg) (out *nl.Status, err error) {
	out, err = WaitForStatus(bp, args, nil)
	if err == nil || !args.OnlyRunning || args.ID != "" || args.Kind == "" {
		return
	}
	// (404 + only-running + by-kind) == ok
	herr, ok := err.(*cmn.ErrHTTP)
	if ok && herr.Status == http.StatusNotFound {
		err = nil
	}
	return
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

// pollAny is the common polling loop used by both snaps- and status-based waiters
func pollAny[T any](prefix string, args *xact.ArgsMsg, apicb func() (T, error), cond func(T) (bool, bool, error)) (T, error) {
	debug.Assert(cond != nil)
	var (
		total, maxSleep = _times(args)
		sleep           = xact.MinPollTime
		begin           = mono.NanoTime()

		lastGood T
		lastErr  error
		hasGood  bool
	)

	for {
		val, err := apicb()
		if err != nil {
			lastErr = err
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
			switch {
			case condErr != nil:
				return val, condErr
			case done:
				return val, nil
			case reset:
				sleep = xact.MinPollTime
			}
		}

		time.Sleep(sleep)
		sleep = min(maxSleep, sleep+sleep/2)

		if mono.Since(begin) >= total {
			var (
				add string
				s   = args.String()
			)
			if lastErr != nil {
				add = fmt.Sprintf(" (last error: %v)", lastErr)
			}
			if hasGood {
				return lastGood, fmt.Errorf("%s: timed out (%v) waiting for %s%s", prefix, total, s, add)
			}
			var zero T
			return zero, fmt.Errorf("%s: timed out (%v) waiting for %s%s", prefix, total, s, add)
		}
	}
}

//
// helpers
//

// `args` filter must contain at least kind or UUID
func _validateXargs(args *xact.ArgsMsg) error {
	if args.Kind == "" && args.ID == "" {
		return fmt.Errorf("api.xaction: missing selector (expecting kind and/or UUID): %s", args.String())
	}
	if args.Kind != "" {
		if err := xact.CheckValidKind(args.Kind); err != nil {
			return err
		}
	}
	if args.ID != "" {
		if err := xact.CheckValidUUID(args.ID); err != nil {
			return err
		}
	}
	return nil
}

func _isRetriable(err error) bool {
	return cos.IsErrRetriableConn(err) || cmn.IsStatusServiceUnavailable(err)
}

func _times(args *xact.ArgsMsg) (time.Duration, time.Duration) {
	total := args.Timeout
	switch {
	case args.Timeout == 0:
		total = xact.DefWaitTimeShort
	case args.Timeout < 0:
		total = xact.DefWaitTimeLong
	}
	return total, min(xact.MaxProbingFreq, cos.ProbingFrequency(total))
}
