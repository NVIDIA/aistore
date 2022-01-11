// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/xact"
)

const (
	XactPollTime       = time.Second
	numConsecutiveIdle = 3 // number of consecutive 'idle' states
)

type (
	NodesXactSnap      map[string]*xact.SnapExt
	NodesXactMultiSnap map[string][]*xact.SnapExt

	XactStatsHelper interface {
		Running() bool
		Finished() bool
		Aborted() bool
		ObjCount() int64
	}

	XactReqArgs struct {
		ID          string
		Kind        string    // Xaction kind, see: cmn.Table
		Node        string    // Optional
		Bck         cmn.Bck   // Optional bucket
		Buckets     []cmn.Bck // Optional: Xaction on list of buckets
		Timeout     time.Duration
		Force       bool // Optional: force LRU
		OnlyRunning bool // Read only active xactions
	}
)

// StartXaction starts a given xact.
func StartXaction(baseParams BaseParams, args XactReqArgs) (id string, err error) {
	if !xact.Table[args.Kind].Startable {
		return id, fmt.Errorf("cannot start \"kind=%s\" xaction", args.Kind)
	}

	xactMsg := xact.QueryMsg{
		Kind: args.Kind,
		Bck:  args.Bck,
		Node: args.Node,
	}

	if args.Kind == cmn.ActLRU {
		ext := &xact.QueryMsgLRU{}
		if args.Buckets != nil {
			xactMsg.Buckets = args.Buckets
			ext.Force = args.Force
		}
		xactMsg.Ext = ext
	} else if args.Kind == cmn.ActStoreCleanup && args.Buckets != nil {
		xactMsg.Buckets = args.Buckets
	}

	msg := cmn.ActionMsg{
		Action: cmn.ActXactStart,
		Value:  xactMsg,
	}

	baseParams.Method = http.MethodPut
	err = DoHTTPReqResp(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Body:       cos.MustMarshal(msg),
		Header:     http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}},
		Query:      cmn.AddBckToQuery(nil, args.Bck),
	}, &id)
	return id, err
}

// AbortXaction aborts a given xact.
func AbortXaction(baseParams BaseParams, args XactReqArgs) error {
	msg := cmn.ActionMsg{
		Action: cmn.ActXactStop,
		Value:  xact.QueryMsg{ID: args.ID, Kind: args.Kind, Bck: args.Bck},
	}
	baseParams.Method = http.MethodPut
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Body:       cos.MustMarshal(msg),
		Header:     http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}},
		Query:      cmn.AddBckToQuery(nil, args.Bck),
	})
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
	err = DoHTTPReqResp(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Body:       cos.MustMarshal(msg),
		Header:     http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}},
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatQueryXactStats}},
	}, &xs)
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
	err = DoHTTPReqResp(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathCluster.S,
		Body:       cos.MustMarshal(msg),
		Header:     http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}},
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatStatus}},
	}, status)
	return
}

// WaitForXaction waits for a given xaction to complete.
func WaitForXaction(baseParams BaseParams, args XactReqArgs, sleeps ...time.Duration) (status *nl.NotifStatus, err error) {
	var (
		ctx = context.Background()
		// TODO: remove `sleeps` arg and calculate `sleep` based on args.Timeout
		sleep = XactPollTime
	)
	if len(sleeps) > 0 {
		sleep = sleeps[0]
	}
	if args.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, args.Timeout)
		defer cancel()
	}
	for {
		status, err = GetXactionStatus(baseParams, args)
		canRetry := err == nil || cos.IsRetriableConnErr(err) || cmn.IsStatusServiceUnavailable(err)
		finished := err == nil && status.Finished()
		if !canRetry || finished {
			return
		}
		time.Sleep(sleep)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			break
		}
	}
}

// WaitForXactionIdle waits for a given on-demand xaction to be idle.
func WaitForXactionIdle(baseParams BaseParams, args XactReqArgs) error {
	var (
		ctx   = context.Background()
		idles int
	)
	if args.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, args.Timeout)
		defer cancel()
	}
	args.OnlyRunning = true
	for {
		snaps, err := QueryXactionSnaps(baseParams, args)
		if err != nil {
			return err
		}
		if snaps.Idle() {
			if idles == numConsecutiveIdle {
				return nil
			}
			idles++
		} else {
			idles = 0
		}
		time.Sleep(XactPollTime)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			break
		}
	}
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

func (xs NodesXactMultiSnap) ObjCount() (count int64) {
	for _, targetStats := range xs {
		for _, snap := range targetStats {
			count += snap.Stats.Objs
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
