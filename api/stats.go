// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"io"
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
	jsoniter "github.com/json-iterator/go"
)

// How to compute throughputs:
//
// - AIS supports several enumerated "kinds" of metrics (for complete enum, see stats/api.go).
// - By convention, metrics that have `KindThroughput` kind are named with ".bps" ("bytes per second") suffix.
// - ".bps" metrics reported by api.GetClusterStats and api.GetDaemonStats are, in fact, cumulative byte numbers.
// - It is the client's responsibility to compute the actual throughputs as only the client knows _when_ exactly
//   the same ".bps" metric was queried the previous time.
//
// See also:
// - api.GetClusterStats
// - api.GetDaemonStats
// - api.GetStatsAndStatus
// - stats/api.go

//
// cluster ----------------------
//

func GetClusterStats(bp BaseParams) (res stats.Cluster, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.WhatNodeStats}}
	}

	var rawStats stats.ClusterRaw
	_, err = reqParams.DoReqAny(&rawStats)
	FreeRp(reqParams)
	if err != nil {
		return
	}

	res.Proxy = rawStats.Proxy
	res.Target = make(map[string]*stats.Node, len(rawStats.Target))
	for tid := range rawStats.Target {
		var ts stats.Node
		if err := jsoniter.Unmarshal(rawStats.Target[tid], &ts); err == nil {
			res.Target[tid] = &ts
		}
	}
	return
}

//
// node ----------------------
//

func anyStats(bp BaseParams, sid, what string, out any) (err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.S // NOTE: reverse, via p.reverseHandler
		reqParams.Query = url.Values{apc.QparamWhat: []string{what}}
		reqParams.Header = http.Header{apc.HdrNodeID: []string{sid}}
	}
	_, err = reqParams.DoReqAny(out)
	FreeRp(reqParams)
	return err
}

// NOTE: direct call used only in tests (remove?)
func GetDaemonStats(bp BaseParams, node *meta.Snode) (ds *stats.Node, err error) {
	ds = &stats.Node{}
	err = anyStats(bp, node.ID(), apc.WhatNodeStats, ds)
	return ds, err
}

// returns both node's stats (as above) and extended status
func GetStatsAndStatus(bp BaseParams, node *meta.Snode) (ds *stats.NodeStatus, err error) {
	ds = &stats.NodeStatus{}
	err = anyStats(bp, node.ID(), apc.WhatNodeStatsAndStatus, ds)
	return ds, err
}

func GetStatsAndStatusV322(bp BaseParams, node *meta.Snode) (ds *stats.NodeStatusV322, err error) {
	ds = &stats.NodeStatusV322{}
	err = anyStats(bp, node.ID(), apc.WhatNodeStatsAndStatusV322, ds)
	return ds, err
}

func GetAnyStats(bp BaseParams, sid, what string) (out []byte, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.S // NOTE: reverse, via p.reverseHandler
		reqParams.Query = url.Values{apc.QparamWhat: []string{what}}
		reqParams.Header = http.Header{apc.HdrNodeID: []string{sid}}
	}
	resp, err := reqParams.do()
	if err != nil {
		return nil, err
	}
	out, err = io.ReadAll(resp.Body)
	cos.DrainReader(resp.Body)
	resp.Body.Close()
	FreeRp(reqParams)
	return out, err
}

//
// reset (cluster | node) stats _or_ only error counters ------------
//

func ResetClusterStats(bp BaseParams, errorsOnly bool) (err error) {
	return _putCluster(bp, apc.ActMsg{Action: apc.ActResetStats, Value: errorsOnly})
}

func ResetDaemonStats(bp BaseParams, node *meta.Snode, errorsOnly bool) error {
	return _putDaemon(bp, node.ID(), apc.ActMsg{Action: apc.ActResetStats, Value: errorsOnly})
}
