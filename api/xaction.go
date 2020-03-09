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
	jsoniter "github.com/json-iterator/go"
)

const (
	xactRetryInterval = time.Second
)

type (
	NodesXactStats map[string][]*stats.BaseXactStatsExt

	XactReqArgs struct {
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

func execXactionAction(baseParams BaseParams, args XactReqArgs, action string) error {
	var (
		optParams []OptionalParams
		actMsg    = &cmn.ActionMsg{
			Action: action,
			Name:   args.Kind,
			Value:  args.Bck.Name,
		}
		path     = cmn.URLPath(cmn.Version, cmn.Cluster)
		msg, err = jsoniter.Marshal(actMsg)
		query    = cmn.AddBckToQuery(nil, args.Bck)
	)
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPut
	optParams = []OptionalParams{{Query: query}}
	_, err = DoHTTPRequest(baseParams, path, msg, optParams...)
	return err
}

// StartXaction API
//
// StartXaction starts a given xaction.
func StartXaction(baseParams BaseParams, args XactReqArgs) error {
	return execXactionAction(baseParams, args, cmn.ActXactStart)
}

// AbortXaction API
//
// AbortXaction aborts a given xaction.
func AbortXaction(baseParams BaseParams, args XactReqArgs) error {
	return execXactionAction(baseParams, args, cmn.ActXactStop)
}

// GetXactionStats API
//
// GetXactionStats gets all xaction stats for given kind and bucket (optional).
func GetXactionStats(baseParams BaseParams, args XactReqArgs) (NodesXactStats, error) {
	var (
		resp      *http.Response
		xactStats = make(NodesXactStats)
	)

	optParams := OptionalParams{}
	actMsg := &cmn.ActionMsg{
		Action: cmn.ActXactStats,
		Name:   args.Kind,
		Value: cmn.XactionExtMsg{
			Bck: args.Bck,
			All: !args.Latest,
		},
	}
	msg, err := jsoniter.Marshal(actMsg)
	if err != nil {
		return nil, err
	}
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Cluster)
	optParams.Query = url.Values{cmn.URLParamWhat: []string{cmn.GetWhatXaction}}
	if resp, err = doHTTPRequestGetResp(baseParams, path, msg, optParams); err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	decoder := jsoniter.NewDecoder(resp.Body)
	if err := decoder.Decode(&xactStats); err != nil {
		return nil, err
	}
	return xactStats, nil
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

func IsErr404(err error) (yes bool) {
	if errHTTP, ok := err.(*cmn.HTTPError); ok {
		yes = errHTTP.Status == http.StatusNotFound
	}
	return
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
	b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActMakeNCopies, Value: copies})
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name)
	_, err = DoHTTPRequest(baseParams, path, b)
	return err
}
