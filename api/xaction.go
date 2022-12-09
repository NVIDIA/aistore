// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/xact"
)

// tunables
const (
	xactDefWaitTimeShort = time.Minute
	xactDefWaitTimeLong  = 7 * 24 * time.Hour
	xactMaxProbingFreq   = 30 * time.Second
	xactMaxPollTime      = 2 * time.Minute
	xactMinPollTime      = 2 * time.Second
	numConsecutiveIdle   = 3 // number of consecutive 'idle' states
)

type (
	XactMultiSnap map[string][]*xact.SnapExt // by target ID (tid)

	// either xaction ID or Kind must be specified
	XactReqArgs struct {
		ID   string // xaction UUID
		Kind string // xaction kind _or_ name (see `xact.Table`)

		// optional parameters to further narrow down or filter-out xactions in question
		DaemonID    string        // node that runs this xaction
		Bck         cmn.Bck       // bucket
		Buckets     []cmn.Bck     // list of buckets (e.g., copy-bucket, lru-evict, etc.)
		Timeout     time.Duration // max time to wait and other "non-filters"
		Force       bool          // force
		OnlyRunning bool          // look only for running xactions
	}
)

// StartXaction starts a given xact.
func StartXaction(bp BaseParams, args XactReqArgs) (id string, err error) {
	if !xact.Table[args.Kind].Startable {
		return id, fmt.Errorf("cannot start \"kind=%s\" xaction", args.Kind)
	}
	xactMsg := xact.QueryMsg{Kind: args.Kind, Bck: args.Bck, DaemonID: args.DaemonID}

	// TODO -- FIXME: remove
	if strings.Contains(args.Kind, "lru") {
		ext := &xact.QueryMsgLRU{}
		if args.Buckets != nil {
			xactMsg.Buckets = args.Buckets
			ext.Force = args.Force
		}
		xactMsg.Ext = ext
	} else if strings.Contains(args.Kind, "cleanup") && args.Buckets != nil {
		xactMsg.Buckets = args.Buckets
	}

	msg := apc.ActionMsg{Action: apc.ActXactStart, Value: xactMsg}
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = args.Bck.AddToQuery(nil)
	}
	err = reqParams.DoReqResp(&id)
	FreeRp(reqParams)
	return id, err
}

// AbortXaction aborts a given xact.
func AbortXaction(bp BaseParams, args XactReqArgs) error {
	msg := apc.ActionMsg{
		Action: apc.ActXactStop,
		Value:  xact.QueryMsg{ID: args.ID, Kind: args.Kind, Bck: args.Bck},
	}
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = args.Bck.AddToQuery(nil)
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

//
// querying and waiting
//

// returns unique ':'-separated kind/ID pairs (strings)
// e.g.: [put-copies:D-ViE6HEL_j list:H96Y7bhR2s copy-bck:matRQMRes put-copies:pOibtHExY]
func GetAllRunningXactions(bp BaseParams, kindOrName string) (out []string, err error) {
	msg := xact.QueryMsg{Kind: kindOrName}
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.GetWhatAllRunningXacts}}
	}
	err = reqParams.DoReqResp(&out)
	FreeRp(reqParams)
	return
}

// QueryXactionSnaps gets all xaction snaps based on the specified selection.
// NOTE: args.Kind can be either xaction kind or name - here and elsewhere
func QueryXactionSnaps(bp BaseParams, args XactReqArgs) (xs XactMultiSnap, err error) {
	msg := xact.QueryMsg{ID: args.ID, Kind: args.Kind, Bck: args.Bck}
	if args.OnlyRunning {
		msg.OnlyRunning = Bool(true)
	}
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.GetWhatQueryXactStats}}
	}
	err = reqParams.DoReqResp(&xs)
	FreeRp(reqParams)
	return
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
func GetOneXactionStatus(bp BaseParams, args XactReqArgs) (status *nl.NotifStatus, err error) {
	status = &nl.NotifStatus{}
	q := url.Values{apc.QparamWhat: []string{apc.GetWhatOneXactStatus}}
	err = getxst(status, q, bp, args)
	return
}

// same as above, except that it returns _all_ matching xactions
func GetAllXactionStatus(bp BaseParams, args XactReqArgs, force bool) (matching nl.NotifStatusVec, err error) {
	q := url.Values{apc.QparamWhat: []string{apc.GetWhatAllXactStatus}}
	if force {
		// (force just-in-time)
		// for each args-selected xaction:
		// check if any of the targets delayed updating the corresponding status,
		// and query those targets directly
		q.Set(apc.QparamForce, "true")
	}
	err = getxst(&matching, q, bp, args)
	return
}

func getxst(v any, q url.Values, bp BaseParams, args XactReqArgs) (err error) {
	bp.Method = http.MethodGet
	msg := xact.QueryMsg{ID: args.ID, Kind: args.Kind, Bck: args.Bck}
	if args.OnlyRunning {
		msg.OnlyRunning = Bool(true)
	}
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}
	err = reqParams.DoReqResp(v)
	FreeRp(reqParams)
	return
}

func initPollingTimes(args XactReqArgs) (time.Duration, time.Duration) {
	total := args.Timeout
	switch {
	case args.Timeout == 0:
		total = xactDefWaitTimeShort
	case args.Timeout < 0:
		total = xactDefWaitTimeLong
	}
	return total, cos.MinDuration(xactMaxProbingFreq, cos.ProbingFrequency(total))
}

func backoffPoll(dur time.Duration) time.Duration {
	dur += dur / 2
	return cos.MinDuration(xactMaxPollTime, dur)
}

// WaitForXactionIC waits for a given xaction to complete.
// Use it only for global xactions
// (those that execute on all targets and report their status to IC, e.g. rebalance).
func WaitForXactionIC(bp BaseParams, args XactReqArgs) (status *nl.NotifStatus, err error) {
	return waitX(bp, args, nil)
}

// WaitForXactionNode waits for a given xaction to complete.
// Use for xaction which can be launched on a single node and do not report their
// statuses(e.g resilver) to IC or to check specific xaction states (e.g Idle).
func WaitForXactionNode(bp BaseParams, args XactReqArgs, fn func(XactMultiSnap) bool) error {
	debug.Assert(args.Kind != "" || xact.IsValidUUID(args.ID))
	_, err := waitX(bp, args, fn)
	return err
}

func waitX(bp BaseParams, args XactReqArgs, fn func(XactMultiSnap) bool) (status *nl.NotifStatus, err error) {
	var (
		elapsed      time.Duration
		begin        = mono.NanoTime()
		total, sleep = initPollingTimes(args)
		ctx, cancel  = context.WithTimeout(context.Background(), total)
	)
	defer cancel()
	for {
		var done bool
		if fn == nil {
			status, err = GetOneXactionStatus(bp, args)
			done = err == nil && status.Finished() && elapsed >= xactMinPollTime
		} else {
			var snaps XactMultiSnap
			snaps, err = QueryXactionSnaps(bp, args)
			done = err == nil && fn(snaps)
		}
		canRetry := err == nil || cos.IsRetriableConnErr(err) || cmn.IsStatusServiceUnavailable(err)
		if done || !canRetry /*fail*/ {
			return
		}
		time.Sleep(sleep)
		elapsed = mono.Since(begin)
		sleep = backoffPoll(sleep)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			break
		}
	}
}

// WaitForXactionIdle waits for a given on-demand xaction to be idle.
func WaitForXactionIdle(bp BaseParams, args XactReqArgs) error {
	var idles int
	args.OnlyRunning = true
	check := func(snaps XactMultiSnap) bool {
		found, idle := snaps.isAllIdle(args.ID)
		if idle || !found { // TODO -- FIXME: !found may translate as "hasn't started yet"
			idles++
			return idles >= numConsecutiveIdle
		}
		idles = 0
		return false
	}
	return WaitForXactionNode(bp, args, check)
}

// Wait for bucket summary:
//  1. The function sends the requests as is (lsmsg.UUID should be empty) to initiate
//     asynchronous task. The destination returns ID of a newly created task
//  2. Starts polling: request destination with received UUID in a loop while
//     the destination returns StatusAccepted=task is still running
//     Time between requests is dynamic: it starts at 200ms and increases
//     by half after every "not-StatusOK" request. It is limited with 10 seconds
//  3. Breaks loop on error
//  4. If the destination returns status code StatusOK, it means the response
//     contains the real data and the function returns the response to the caller
func (reqParams *ReqParams) waitBsumm(msg *cmn.BsummCtrlMsg, v *cmn.AllBsummResults) error {
	var (
		uuid   string
		sleep  = xactMinPollTime
		actMsg = apc.ActionMsg{Action: apc.ActSummaryBck, Value: msg}
		body   = cos.MustMarshal(actMsg)
	)
	if reqParams.Query == nil {
		reqParams.Query = url.Values{}
	}
	reqParams.Body = body
	resp, err := reqParams.doResp(&uuid)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusAccepted {
		if resp.StatusCode == http.StatusOK {
			return errors.New("expected 202 response code on first call, got 200")
		}
		return fmt.Errorf("invalid response code: %d", resp.StatusCode)
	}
	if msg.UUID == "" {
		msg.UUID = uuid
		body = cos.MustMarshal(actMsg)
	}

	// Poll async task for http.StatusOK completion
	for {
		reqParams.Body = body
		resp, err = reqParams.doResp(v)
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusOK {
			break
		}
		time.Sleep(sleep)
		if sleep < xactMaxProbingFreq {
			sleep += sleep / 2
		}
	}
	return err
}

///////////////////
// XactMultiSnap //
///////////////////

// NOTE: when xaction UUID is not specified: require the same kind _and_
// a single running uuid (otherwise, IsAborted() et al. can only produce ambiguous results)
func (xs XactMultiSnap) checkEmptyID(xactID string) error {
	var kind, uuid string
	if xactID != "" {
		debug.Assert(xact.IsValidUUID(xactID), xactID)
		return nil
	}
	for _, snaps := range xs {
		for _, xsnap := range snaps {
			if kind == "" {
				kind = xsnap.Kind
			} else if kind != xsnap.Kind {
				return fmt.Errorf("invalid multi-snap Kind: %q vs %q", kind, xsnap.Kind)
			}
			if xsnap.Running() {
				if uuid == "" {
					uuid = xsnap.ID
				} else if uuid != xsnap.ID {
					return fmt.Errorf("invalid multi-snap UUID: %q vs %q", uuid, xsnap.ID)
				}
			}
		}
	}
	return nil
}

func (xs XactMultiSnap) GetUUIDs() []string {
	uuids := make(cos.StrSet, 2)
	for _, snaps := range xs {
		for _, xsnap := range snaps {
			uuids[xsnap.ID] = struct{}{}
		}
	}
	return uuids.ToSlice()
}

func (xs XactMultiSnap) RunningTarget(xactID string) (string /*tid*/, *xact.SnapExt, error) {
	if err := xs.checkEmptyID(xactID); err != nil {
		return "", nil, err
	}
	for tid, snaps := range xs {
		for _, xsnap := range snaps {
			if (xactID == xsnap.ID || xactID == "") && xsnap.Running() {
				return tid, xsnap, nil
			}
		}
	}
	return "", nil, nil
}

func (xs XactMultiSnap) IsAborted(xactID string) (bool, error) {
	if err := xs.checkEmptyID(xactID); err != nil {
		return false, err
	}
	for _, snaps := range xs {
		for _, xsnap := range snaps {
			if (xactID == xsnap.ID || xactID == "") && xsnap.IsAborted() {
				return true, nil
			}
		}
	}
	return false, nil
}

func (xs XactMultiSnap) isAllIdle(xactID string) (found, idle bool) {
	if xactID != "" {
		debug.Assert(xact.IsValidUUID(xactID), xactID)
		return xs.isOneIdle(xactID)
	}
	uuids := xs.GetUUIDs()
	idle = true
	for _, xactID = range uuids {
		f, i := xs.isOneIdle(xactID)
		found = found || f
		idle = idle && i
	}
	return
}

func (xs XactMultiSnap) isOneIdle(xactID string) (found, idle bool) {
	for _, snaps := range xs {
		for _, xsnap := range snaps {
			if xactID == xsnap.ID {
				found = true
				if xsnap.Started() && !xsnap.IsAborted() && !xsnap.Idle() {
					return true, false
				}
			}
		}
	}
	idle = true // read: not-idle not found
	return
}

func (xs XactMultiSnap) ObjCounts(xactID string) (locObjs, outObjs, inObjs int64) {
	if xactID == "" {
		uuids := xs.GetUUIDs()
		debug.Assert(len(uuids) == 1, uuids)
		xactID = uuids[0]
	}
	for _, snaps := range xs {
		for _, xsnap := range snaps {
			if xactID == xsnap.ID {
				locObjs += xsnap.Stats.Objs
				outObjs += xsnap.Stats.OutObjs
				inObjs += xsnap.Stats.InObjs
			}
		}
	}
	return
}

func (xs XactMultiSnap) ByteCounts(xactID string) (locBytes, outBytes, inBytes int64) {
	if xactID == "" {
		uuids := xs.GetUUIDs()
		debug.Assert(len(uuids) == 1, uuids)
		xactID = uuids[0]
	}
	for _, snaps := range xs {
		for _, xsnap := range snaps {
			if xactID == xsnap.ID {
				locBytes += xsnap.Stats.Bytes
				outBytes += xsnap.Stats.OutBytes
				inBytes += xsnap.Stats.InBytes
			}
		}
	}
	return
}

func (xs XactMultiSnap) TotalRunningTime(xactID string) (time.Duration, error) {
	debug.Assert(xact.IsValidUUID(xactID), xactID)
	var (
		start, end     time.Time
		found, running bool
	)
	for _, snaps := range xs {
		for _, xsnap := range snaps {
			if xactID == xsnap.ID {
				found = true
				running = running || xsnap.Running()
				if !xsnap.StartTime.IsZero() {
					if start.IsZero() || xsnap.StartTime.Before(start) {
						start = xsnap.StartTime
					}
				}
				if !xsnap.EndTime.IsZero() && xsnap.EndTime.After(end) {
					end = xsnap.EndTime
				}
			}
		}
	}
	if !found {
		return 0, errors.New("xaction [" + xactID + "] not found")
	}
	if running {
		end = time.Now()
	}
	return end.Sub(start), nil
}
