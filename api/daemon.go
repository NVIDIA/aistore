// Package api provides RESTful API to AIS object storage
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
)

const (
	StatusOnline   = "online"
	StatusOffline  = "offline"
	StatusTimedOut = "timed out"
)

// GetMountpaths given the direct public URL of the target, returns its
// mountpaths or error.
func GetMountpaths(baseParams BaseParams, node *cluster.Snode) (mpl *cmn.MountpathList, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Reverse, cmn.Daemon),
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatMountpaths}},
		Header: http.Header{
			cmn.HeaderNodeID:  []string{node.ID()},
			cmn.HeaderNodeURL: []string{node.URL(cmn.NetworkPublic)},
		},
	}, &mpl)
	return mpl, err
}

func AddMountpath(baseParams BaseParams, node *cluster.Snode, mountpath string) error {
	baseParams.Method = http.MethodPut
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Reverse, cmn.Daemon, cmn.Mountpaths),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActMountpathAdd, Value: mountpath}),
		Header: http.Header{
			cmn.HeaderNodeID:  []string{node.ID()},
			cmn.HeaderNodeURL: []string{node.URL(cmn.NetworkPublic)},
		},
	})
}

func RemoveMountpath(baseParams BaseParams, nodeID, mountpath string) error {
	baseParams.Method = http.MethodDelete
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Reverse, cmn.Daemon, cmn.Mountpaths),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActMountpathRemove, Value: mountpath}),
		Header:     http.Header{cmn.HeaderNodeID: []string{nodeID}},
	})
}

func EnableMountpath(baseParams BaseParams, node *cluster.Snode, mountpath string) error {
	baseParams.Method = http.MethodPost
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Reverse, cmn.Daemon, cmn.Mountpaths),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActMountpathEnable, Value: mountpath}),
		Header: http.Header{
			cmn.HeaderNodeID:  []string{node.ID()},
			cmn.HeaderNodeURL: []string{node.URL(cmn.NetworkPublic)},
		},
	})
}

func DisableMountpath(baseParams BaseParams, nodeID, mountpath string) error {
	baseParams.Method = http.MethodPost
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Reverse, cmn.Daemon, cmn.Mountpaths),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActMountpathDisable, Value: mountpath}),
		Header:     http.Header{cmn.HeaderNodeID: []string{nodeID}},
	})
}

// GetDaemonConfig returns the configuration of a specific daemon in a cluster.
func GetDaemonConfig(baseParams BaseParams, nodeID string) (config *cmn.Config, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Reverse, cmn.Daemon),
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatConfig}},
		Header:     http.Header{cmn.HeaderNodeID: []string{nodeID}},
	}, &config)
	if err != nil {
		return nil, err
	}
	err = config.Validate()
	return config, err
}

// GetDaemonStatus returns the info of a specific node in the cluster.
func GetDaemonStatus(baseParams BaseParams, node *cluster.Snode) (daeInfo *stats.DaemonStatus, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Reverse, cmn.Daemon),
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatDaemonStatus}},
		Header:     http.Header{cmn.HeaderNodeID: []string{node.ID()}},
	}, &daeInfo)
	if err == nil {
		daeInfo.Status = StatusOnline
	} else {
		httpErr := &cmn.HTTPError{}
		daeInfo = &stats.DaemonStatus{Snode: node, Status: StatusOffline}
		if errors.Is(err, context.DeadlineExceeded) {
			daeInfo.Status = StatusTimedOut
		} else if errors.As(err, &httpErr) {
			daeInfo.Status = fmt.Sprintf("error: %d", httpErr.Status)
		}
	}
	return daeInfo, err
}

// SetDaemonConfig given key value pairs sets the configuration accordingly for a specific node.
func SetDaemonConfig(baseParams BaseParams, nodeID string, nvs cmn.SimpleKVs) error {
	baseParams.Method = http.MethodPut
	query := url.Values{}
	for key, val := range nvs {
		query.Add(key, val)
	}
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Reverse, cmn.Daemon, cmn.ActSetConfig),
		Query:      query,
		Header:     http.Header{cmn.HeaderNodeID: []string{nodeID}},
	})
}
