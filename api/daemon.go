// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/stats"
)

const (
	StatusOnline   = "online"
	StatusOffline  = "offline"
	StatusTimedOut = "timed out"
)

type GetLogInput struct {
	Writer   io.Writer
	Severity string // one of: {cmn.LogInfo, ...}
}

// GetMountpaths given the direct public URL of the target, returns the target's mountpaths or error.
func GetMountpaths(baseParams BaseParams, node *cluster.Snode) (mpl *cmn.MountpathList, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPReqResp(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathReverseDaemon.S,
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatMountpaths}},
		Header: http.Header{
			cmn.HdrNodeID:  []string{node.ID()},
			cmn.HdrNodeURL: []string{node.URL(cmn.NetworkPublic)},
		},
	}, &mpl)
	return mpl, err
}

// TODO: rewrite tests that come here with `force`
func AttachMountpath(baseParams BaseParams, node *cluster.Snode, mountpath string, force bool) error {
	baseParams.Method = http.MethodPut
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathReverseDaemon.Join(cmn.Mountpaths),
		Body:       cos.MustMarshal(cmn.ActionMsg{Action: cmn.ActMountpathAttach, Value: mountpath}),
		Header: http.Header{
			cmn.HdrNodeID:      []string{node.ID()},
			cmn.HdrNodeURL:     []string{node.URL(cmn.NetworkPublic)},
			cmn.HdrContentType: []string{cmn.ContentJSON},
		},
		Query: url.Values{cmn.URLParamForce: []string{strconv.FormatBool(force)}},
	})
}

func EnableMountpath(baseParams BaseParams, node *cluster.Snode, mountpath string) error {
	baseParams.Method = http.MethodPost
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathReverseDaemon.Join(cmn.Mountpaths),
		Body:       cos.MustMarshal(cmn.ActionMsg{Action: cmn.ActMountpathEnable, Value: mountpath}),
		Header: http.Header{
			cmn.HdrNodeID:      []string{node.ID()},
			cmn.HdrNodeURL:     []string{node.URL(cmn.NetworkPublic)},
			cmn.HdrContentType: []string{cmn.ContentJSON},
		},
	})
}

func DetachMountpath(baseParams BaseParams, node *cluster.Snode, mountpath string, dontResilver bool) error {
	var q url.Values
	if dontResilver {
		q = url.Values{cmn.URLParamDontResilver: []string{"true"}}
	}
	baseParams.Method = http.MethodDelete
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathReverseDaemon.Join(cmn.Mountpaths),
		Body:       cos.MustMarshal(cmn.ActionMsg{Action: cmn.ActMountpathDetach, Value: mountpath}),
		Header: http.Header{
			cmn.HdrNodeID:      []string{node.ID()},
			cmn.HdrContentType: []string{cmn.ContentJSON},
		},
		Query: q,
	})
}

func DisableMountpath(baseParams BaseParams, node *cluster.Snode, mountpath string, dontResilver bool) error {
	var q url.Values
	if dontResilver {
		q = url.Values{cmn.URLParamDontResilver: []string{"true"}}
	}
	baseParams.Method = http.MethodPost
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathReverseDaemon.Join(cmn.Mountpaths),
		Body:       cos.MustMarshal(cmn.ActionMsg{Action: cmn.ActMountpathDisable, Value: mountpath}),
		Header: http.Header{
			cmn.HdrNodeID:      []string{node.ID()},
			cmn.HdrContentType: []string{cmn.ContentJSON},
		},
		Query: q,
	})
}

// GetDaemonConfig returns the configuration of a specific daemon in a cluster.
func GetDaemonConfig(baseParams BaseParams, node *cluster.Snode) (config *cmn.Config, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPReqResp(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathReverseDaemon.S,
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatConfig}},
		Header:     http.Header{cmn.HdrNodeID: []string{node.ID()}},
	}, &config)
	if err != nil {
		return nil, err
	}
	// FIXME: transform backend structures on the client side
	// - as a side effect, config.Backend validation populates non-JSON structs that client can utilize;
	// - secondly, HDFS networking, etc.
	// TODO: revise and remove
	_ = config.Backend.Validate()
	return config, nil
}

func GetDaemonStats(baseParams BaseParams, node *cluster.Snode) (ds *stats.DaemonStats, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPReqResp(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathReverseDaemon.S,
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatStats}},
		Header: http.Header{
			cmn.HdrNodeID: []string{node.ID()},
		},
	}, &ds)
	return ds, err
}

// GetDaemonLog returns log of a specific daemon in a cluster.
func GetDaemonLog(baseParams BaseParams, node *cluster.Snode, args GetLogInput) (err error) {
	w := args.Writer
	q := url.Values{}
	q.Set(cmn.URLParamWhat, cmn.GetWhatLog)
	if args.Severity != "" {
		q.Set(cmn.URLParamSev, args.Severity)
	}
	baseParams.Method = http.MethodGet
	err = DoHTTPReqResp(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathReverseDaemon.S,
		Query:      q,
		Header:     http.Header{cmn.HdrNodeID: []string{node.ID()}},
	}, w)
	return
}

// GetDaemonStatus returns information about specific node in a cluster.
func GetDaemonStatus(baseParams BaseParams, node *cluster.Snode) (daeInfo *stats.DaemonStatus, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPReqResp(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathReverseDaemon.S,
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatDaemonStatus}},
		Header:     http.Header{cmn.HdrNodeID: []string{node.ID()}},
	}, &daeInfo)
	if err == nil {
		daeInfo.Status = StatusOnline
	} else {
		daeInfo = &stats.DaemonStatus{Snode: node, Status: StatusOffline}
		if errors.Is(err, context.DeadlineExceeded) {
			daeInfo.Status = StatusTimedOut
		} else if httpErr := cmn.Err2HTTPErr(err); httpErr != nil {
			daeInfo.Status = fmt.Sprintf("error: %d", httpErr.Status)
		}
	}
	return daeInfo, err
}

// SetDaemonConfig, given key value pairs, sets the configuration accordingly for a specific node.
func SetDaemonConfig(baseParams BaseParams, nodeID string, nvs cos.SimpleKVs, transient ...bool) error {
	baseParams.Method = http.MethodPut
	query := url.Values{}
	for key, val := range nvs {
		query.Add(key, val)
	}
	if len(transient) > 0 {
		query.Add(cmn.ActTransient, strconv.FormatBool(transient[0]))
	}
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathReverseDaemon.Join(cmn.ActSetConfig),
		Query:      query,
		Header:     http.Header{cmn.HdrNodeID: []string{nodeID}},
	})
}

// ResetDaemonConfig resets the configuration for a specific node to the cluster configuration.
func ResetDaemonConfig(baseParams BaseParams, nodeID string) error {
	baseParams.Method = http.MethodPut
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathReverseDaemon.S,
		Body:       cos.MustMarshal(cmn.ActionMsg{Action: cmn.ActResetConfig}),
		Header: http.Header{
			cmn.HdrNodeID:      []string{nodeID},
			cmn.HdrContentType: []string{cmn.ContentJSON},
		},
	})
}
