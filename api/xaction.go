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
	NodesXactSnap      map[string]*xact.SnapExt
	NodesXactMultiSnap map[string][]*xact.SnapExt
	XactSnapTestFunc   func(NodesXactMultiSnap) bool

	XactStatsHelper interface {
		Running() bool
		Finished() bool
		Aborted() bool
		ObjCount() int64
	}

	XactReqArgs struct {
		// either xaction ID or Kind _must_ be specified
		ID   string // xaction UUID
		Kind string // xaction kind, see `xact.Table`)
		// optional parameters to further narrow down or filter out xactions in question
		DaemonID string    // node that runs this xaction
		Bck      cmn.Bck   // bucket
		Buckets  []cmn.Bck // list of buckets (e.g., copy-bucket, lru-evict, etc.)
		// max time to wait and other "non-filters"
		Timeout time.Duration
		Force   bool // force
		// more filters
		OnlyRunning bool // look only for running xactions
	}
)

// StartXaction starts a given xact.
func StartXaction(baseParams BaseParams, args XactReqArgs) (id string, err error) {
	if !xact.Table[args.Kind].Startable {
		return id, fmt.Errorf("cannot start \"kind=%s\" xaction", args.Kind)
	}
	xactMsg := xact.QueryMsg{Kind: args.Kind, Bck: args.Bck, DaemonID: args.DaemonID}
	if args.Kind == apc.ActLRU {
		ext := &xact.QueryMsgLRU{}
		if args.Buckets != nil {
			xactMsg.Buckets = args.Buckets
			ext.Force = args.Force
		}
		xactMsg.Ext = ext
	} else if args.Kind == apc.ActStoreCleanup && args.Buckets != nil {
		xactMsg.Buckets = args.Buckets
	}

	msg := apc.ActionMsg{Action: apc.ActXactStart, Value: xactMsg}
	baseParams.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = args.Bck.AddToQuery(nil)
	}
	err = reqParams.DoHTTPReqResp(&id)
	FreeRp(reqParams)
	return id, err
}

// AbortXaction aborts a given xact.
func AbortXaction(baseParams BaseParams, args XactReqArgs) error {
	msg := apc.ActionMsg{
		Action: apc.ActXactStop,
		Value:  xact.QueryMsg{ID: args.ID, Kind: args.Kind, Bck: args.Bck},
	}
	baseParams.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = args.Bck.AddToQuery(nil)
	}
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

// GetXactionSnapsByID gets all xaction snaps for a given xaction id.
func GetXactionSnapsByID(baseParams BaseParams, xactID string) (nxs NodesXactSnap, err error) {
	xs, err := QueryXactionSnaps(baseParams, XactReqArgs{ID: xactID})
	if err != nil {
		return
	}
	nxs = xs.nodesXactSnap(xactID)
	return
}

// QueryXactionSnaps gets all xaction snaps based on the specified selection.
func QueryXactionSnaps(baseParams BaseParams, args XactReqArgs) (xs NodesXactMultiSnap, err error) {
	msg := xact.QueryMsg{ID: args.ID, Kind: args.Kind, Bck: args.Bck}
	if args.OnlyRunning {
		msg.OnlyRunning = Bool(true)
	}
	baseParams.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.GetWhatQueryXactStats}}
	}
	err = reqParams.DoHTTPReqResp(&xs)
	FreeRp(reqParams)
	return xs, err
}

// GetXactionStatus retrieves the status of the xact.
func GetXactionStatus(baseParams BaseParams, args XactReqArgs) (status *nl.NotifStatus, err error) {
	baseParams.Method = http.MethodGet
	msg := xact.QueryMsg{ID: args.ID, Kind: args.Kind, Bck: args.Bck}
	if args.OnlyRunning {
		msg.OnlyRunning = Bool(true)
	}
	status = &nl.NotifStatus{}
	reqParams := AllocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.GetWhatXactStatus}}
	}
	err = reqParams.DoHTTPReqResp(status)
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

func _waitForXaction(baseParams BaseParams, args XactReqArgs, condFn ...XactSnapTestFunc) (status *nl.NotifStatus, err error) {
	var (
		elapsed      time.Duration
		begin        = mono.NanoTime()
		total, sleep = initPollingTimes(args)
		ctx, cancel  = context.WithTimeout(context.Background(), total)
	)
	defer cancel()
	for {
		var done bool
		if len(condFn) == 0 {
			status, err = GetXactionStatus(baseParams, args)
			done = err == nil && status.Finished() && elapsed >= xactMinPollTime
		} else {
			var (
				snaps NodesXactMultiSnap
				fn    = condFn[0]
			)
			snaps, err = QueryXactionSnaps(baseParams, args)
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

// WaitForXactionIC waits for a given xaction to complete.
// Use it only for global xactions
// (those that execute on all targets and report their status to IC, e.g. rebalance).
func WaitForXactionIC(baseParams BaseParams, args XactReqArgs) (status *nl.NotifStatus, err error) {
	return _waitForXaction(baseParams, args)
}

// WaitForXactionNode waits for a given xaction to complete.
// Use for xaction which can be launched on a single node and do not report their
// statuses(e.g resilver) to IC or to check specific xaction states (e.g Idle).
func WaitForXactionNode(baseParams BaseParams, args XactReqArgs, fn XactSnapTestFunc) error {
	debug.Assert(args.Kind != "")
	_, err := _waitForXaction(baseParams, args, fn)
	return err
}

// WaitForXactionIdle waits for a given on-demand xaction to be idle.
func WaitForXactionIdle(baseParams BaseParams, args XactReqArgs) error {
	idles := 0
	args.OnlyRunning = true
	check := func(snaps NodesXactMultiSnap) bool {
		if snaps.Idle() {
			idles++
			return idles >= numConsecutiveIdle
		}
		idles = 0
		return false
	}
	return WaitForXactionNode(baseParams, args, check)
}

///////////////////
// NodesXactSnap //
///////////////////

func (nxs NodesXactSnap) running() bool {
	for _, snap := range nxs {
		if snap.Running() {
			return true
		}
	}
	return false
}

func (nxs NodesXactSnap) Finished() bool { return !nxs.running() }

func (nxs NodesXactSnap) IsAborted() bool {
	for _, snap := range nxs {
		if snap.IsAborted() {
			return true
		}
	}
	return false
}

func (nxs NodesXactSnap) ObjCounts() (locObjs, outObjs, inObjs int64) {
	for _, snap := range nxs {
		locObjs += snap.Stats.Objs
		outObjs += snap.Stats.OutObjs
		inObjs += snap.Stats.InObjs
	}
	return
}

func (nxs NodesXactSnap) ByteCounts() (locBytes, outBytes, inBytes int64) {
	for _, snap := range nxs {
		locBytes += snap.Stats.Bytes
		outBytes += snap.Stats.OutBytes
		inBytes += snap.Stats.InBytes
	}
	return
}

func (nxs NodesXactSnap) TotalRunningTime() time.Duration {
	var (
		start   = time.Now()
		end     time.Time
		running bool
	)
	for _, snap := range nxs {
		running = running || snap.Running()
		if snap.StartTime.Before(start) {
			start = snap.StartTime
		}
		if snap.EndTime.After(end) {
			end = snap.EndTime
		}
	}
	if running {
		end = time.Now()
	}
	return end.Sub(start)
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
func (reqParams *ReqParams) waitSummary(msg *apc.BckSummMsg, v interface{}) error {
	var (
		uuid   string
		action = apc.ActSummaryBck
		sleep  = xactMinPollTime
		actMsg = apc.ActionMsg{Action: action, Value: msg}
	)
	if reqParams.Query == nil {
		reqParams.Query = url.Values{}
	}
	reqParams.Body = cos.MustMarshal(actMsg)
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
	}

	// Poll async task for http.StatusOK completion
	for {
		reqParams.Body = cos.MustMarshal(actMsg)
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

////////////////////////
// NodesXactMultiSnap //
////////////////////////

func (xs NodesXactMultiSnap) Running() (tid string, xsnap *xact.SnapExt) {
	var snaps []*xact.SnapExt
	for tid, snaps = range xs {
		for _, xsnap = range snaps {
			if xsnap.Running() {
				return
			}
		}
	}
	return "", nil
}

func (xs NodesXactMultiSnap) ObjCounts() (locObjs, outObjs, inObjs int64) {
	for _, targetStats := range xs {
		for _, snap := range targetStats {
			locObjs += snap.Stats.Objs
			outObjs += snap.Stats.OutObjs
			inObjs += snap.Stats.InObjs
		}
	}
	return
}

func (xs NodesXactMultiSnap) ByteCounts() (locBytes, outBytes, inBytes int64) {
	for _, targetStats := range xs {
		for _, snap := range targetStats {
			locBytes += snap.Stats.Bytes
			outBytes += snap.Stats.OutBytes
			inBytes += snap.Stats.InBytes
		}
	}
	return
}

func (xs NodesXactMultiSnap) nodesXactSnap(xactID string) (nxs NodesXactSnap) {
	nxs = make(NodesXactSnap)
	for tid, snaps := range xs {
		for _, xsnap := range snaps {
			if xsnap.ID == xactID {
				nxs[tid] = xsnap
				break
			}
		}
	}
	return
}

func (xs NodesXactMultiSnap) Idle() bool {
	for _, xs := range xs {
		for _, xsnap := range xs {
			if !xsnap.Idle() {
				return false
			}
		}
	}
	return true
}
