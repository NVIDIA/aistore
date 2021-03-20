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

// GetMountpaths given the direct public URL of the target, returns its
// mountpaths or error.
func GetMountpaths(baseParams BaseParams, node *cluster.Snode) (mpl *cmn.MountpathList, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPRequest(ReqParams{
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

func AddMountpath(baseParams BaseParams, node *cluster.Snode, mountpath string) error {
	baseParams.Method = http.MethodPut
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathReverseDaemon.Join(cmn.Mountpaths),
		Body:       cos.MustMarshal(cmn.ActionMsg{Action: cmn.ActMountpathAdd, Value: mountpath}),
		Header: http.Header{
			cmn.HdrNodeID:  []string{node.ID()},
			cmn.HdrNodeURL: []string{node.URL(cmn.NetworkPublic)},
		},
	})
}

func RemoveMountpath(baseParams BaseParams, nodeID, mountpath string) error {
	baseParams.Method = http.MethodDelete
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathReverseDaemon.Join(cmn.Mountpaths),
		Body:       cos.MustMarshal(cmn.ActionMsg{Action: cmn.ActMountpathRemove, Value: mountpath}),
		Header:     http.Header{cmn.HdrNodeID: []string{nodeID}},
	})
}

func EnableMountpath(baseParams BaseParams, node *cluster.Snode, mountpath string) error {
	baseParams.Method = http.MethodPost
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathReverseDaemon.Join(cmn.Mountpaths),
		Body:       cos.MustMarshal(cmn.ActionMsg{Action: cmn.ActMountpathEnable, Value: mountpath}),
		Header: http.Header{
			cmn.HdrNodeID:  []string{node.ID()},
			cmn.HdrNodeURL: []string{node.URL(cmn.NetworkPublic)},
		},
	})
}

func DisableMountpath(baseParams BaseParams, nodeID, mountpath string) error {
	baseParams.Method = http.MethodPost
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathReverseDaemon.Join(cmn.Mountpaths),
		Body:       cos.MustMarshal(cmn.ActionMsg{Action: cmn.ActMountpathDisable, Value: mountpath}),
		Header:     http.Header{cmn.HdrNodeID: []string{nodeID}},
	})
}

// GetDaemonConfig returns the configuration of a specific daemon in a cluster.
func GetDaemonConfig(baseParams BaseParams, node *cluster.Snode) (config *cmn.Config, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathReverseDaemon.S,
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatConfig}},
		Header:     http.Header{cmn.HdrNodeID: []string{node.ID()}},
	}, &config)
	if err != nil {
		return nil, err
	}
	// Validate either as proxy or target.
	config.SetRole(node.Type())
	err = config.Validate()
	return config, err
}

// GetDaemonStatus returns information about specific node in a cluster.
func GetDaemonStatus(baseParams BaseParams, node *cluster.Snode) (daeInfo *stats.DaemonStatus, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPRequest(ReqParams{
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
		Header:     http.Header{cmn.HdrNodeID: []string{nodeID}},
	})
}
