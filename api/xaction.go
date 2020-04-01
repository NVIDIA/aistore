// Package api provides RESTful API to AIS object storage
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
)

const (
	xactRetryInterval = time.Second
)

type (
	NodesXactStats map[string][]*stats.BaseXactStatsExt

	XactReqArgs struct {
		ID      string
		Kind    string  // Xaction kind, see: cmn.XactType
		Bck     cmn.Bck // Optional bucket
		Latest  bool    // Determines if we should get latest or all xactions
		Timeout time.Duration
	}
)

func (xs NodesXactStats) Running() bool {
	for _, targetStats := range xs {
		for _, xaction := range targetStats {
			if xaction.Running() {
				return true
			}
		}
	}
	return false
}

func (xs NodesXactStats) Finished() bool { return !xs.Running() }

func (xs NodesXactStats) Aborted() bool {
	for _, targetStats := range xs {
		for _, xaction := range targetStats {
			if xaction.Aborted() {
				return true
			}
		}
	}
	return false
}

func (xs NodesXactStats) ObjCount() (count int64) {
	for _, targetStats := range xs {
		for _, xaction := range targetStats {
			count += xaction.ObjCount()
		}
	}
	return
}

// StartXaction API
//
// StartXaction starts a given xaction.
func StartXaction(baseParams BaseParams, args XactReqArgs) (id string, err error) {
	msg := cmn.ActionMsg{
		Action: cmn.ActXactStart,
		Name:   args.Kind,
		Value:  args.Bck.Name,
	}
	baseParams.Method = http.MethodPut
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Cluster),
		Body:       cmn.MustMarshal(msg),
		Query:      cmn.AddBckToQuery(nil, args.Bck),
	}, &id)
	return id, err
}

// AbortXaction API
//
// AbortXaction aborts a given xaction.
func AbortXaction(baseParams BaseParams, args XactReqArgs) error {
	msg := cmn.ActionMsg{
		Action: cmn.ActXactStop,
		Name:   args.Kind,
		Value:  args.Bck.Name,
	}
	baseParams.Method = http.MethodPut
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Cluster),
		Body:       cmn.MustMarshal(msg),
		Query:      cmn.AddBckToQuery(nil, args.Bck),
	})
}

// GetXactionStats API
//
// GetXactionStats gets all xaction stats for given kind and bucket (optional).
func GetXactionStats(baseParams BaseParams, args XactReqArgs) (xactStats NodesXactStats, err error) {
	// TODO: msg.Action field is unused and shouldn't be used (as well as the entire action message)
	// TODO: #668
	msg := cmn.ActionMsg{
		Name: args.Kind,
		Value: cmn.XactionExtMsg{
			ID:  args.ID,
			Bck: args.Bck,
			All: !args.Latest,
		},
	}
	baseParams.Method = http.MethodGet
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Cluster),
		Body:       cmn.MustMarshal(msg),
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatXactStats}},
	}, &xactStats)
	return xactStats, err
}

// WaitForXaction API
//
// WaitForXaction waits for a given xaction to complete.
func WaitForXaction(baseParams BaseParams, args XactReqArgs) error {
	ctx := context.Background()
	if args.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, args.Timeout)
		defer cancel()
	}

	for {
		xactStats, err := GetXactionStats(baseParams, args)
		if err != nil {
			return err
		}
		if xactStats.Finished() {
			break
		}
		time.Sleep(xactRetryInterval)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			break
		}
	}

	return nil
}

// WaitForXaction API
//
// WaitForXactionToStart waits for a given xaction to start.
func WaitForXactionToStart(baseParams BaseParams, args XactReqArgs) error {
	ctx := context.Background()
	if args.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, args.Timeout)
		defer cancel()
	}

	for {
		xactStats, err := GetXactionStats(baseParams, args)
		if err != nil {
			return err
		}
		if xactStats.Running() {
			break
		}
		if len(xactStats) > 0 && xactStats.Finished() {
			break
		}

		time.Sleep(xactRetryInterval)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			break
		}
	}
	return nil
}

// MakeNCopies API
//
// MakeNCopies starts an extended action (xaction) to bring a given bucket to a certain redundancy level (num copies)
func MakeNCopies(baseParams BaseParams, bck cmn.Bck, copies int) error {
	baseParams.Method = http.MethodPost
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActMakeNCopies, Value: copies}),
	})
}
