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

	"github.com/NVIDIA/aistore/api/apc"
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
func GetMountpaths(baseParams BaseParams, node *cluster.Snode) (mpl *apc.MountpathList, err error) {
	baseParams.Method = http.MethodGet
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathReverseDaemon.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.GetWhatMountpaths}}
		reqParams.Header = http.Header{
			apc.HdrNodeID:  []string{node.ID()},
			apc.HdrNodeURL: []string{node.URL(cmn.NetPublic)},
		}
	}
	err = reqParams.DoHTTPReqResp(&mpl)
	freeRp(reqParams)
	return mpl, err
}

// TODO: rewrite tests that come here with `force`
func AttachMountpath(baseParams BaseParams, node *cluster.Snode, mountpath string, force bool) error {
	baseParams.Method = http.MethodPut
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathReverseDaemon.Join(apc.Mountpaths)
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActMountpathAttach, Value: mountpath})
		reqParams.Header = http.Header{
			apc.HdrNodeID:      []string{node.ID()},
			apc.HdrNodeURL:     []string{node.URL(cmn.NetPublic)},
			cmn.HdrContentType: []string{cmn.ContentJSON},
		}
		reqParams.Query = url.Values{apc.QparamForce: []string{strconv.FormatBool(force)}}
	}
	err := reqParams.DoHTTPRequest()
	freeRp(reqParams)
	return err
}

func EnableMountpath(baseParams BaseParams, node *cluster.Snode, mountpath string) error {
	baseParams.Method = http.MethodPost
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathReverseDaemon.Join(apc.Mountpaths)
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActMountpathEnable, Value: mountpath})
		reqParams.Header = http.Header{
			apc.HdrNodeID:      []string{node.ID()},
			apc.HdrNodeURL:     []string{node.URL(cmn.NetPublic)},
			cmn.HdrContentType: []string{cmn.ContentJSON},
		}
	}
	err := reqParams.DoHTTPRequest()
	freeRp(reqParams)
	return err
}

func DetachMountpath(baseParams BaseParams, node *cluster.Snode, mountpath string, dontResilver bool) error {
	var q url.Values
	if dontResilver {
		q = url.Values{apc.QparamDontResilver: []string{"true"}}
	}
	baseParams.Method = http.MethodDelete
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathReverseDaemon.Join(apc.Mountpaths)
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActMountpathDetach, Value: mountpath})
		reqParams.Header = http.Header{
			apc.HdrNodeID:      []string{node.ID()},
			cmn.HdrContentType: []string{cmn.ContentJSON},
		}
		reqParams.Query = q
	}
	err := reqParams.DoHTTPRequest()
	freeRp(reqParams)
	return err
}

func DisableMountpath(baseParams BaseParams, node *cluster.Snode, mountpath string, dontResilver bool) error {
	var q url.Values
	if dontResilver {
		q = url.Values{apc.QparamDontResilver: []string{"true"}}
	}
	baseParams.Method = http.MethodPost
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathReverseDaemon.Join(apc.Mountpaths)
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActMountpathDisable, Value: mountpath})
		reqParams.Header = http.Header{
			apc.HdrNodeID:      []string{node.ID()},
			cmn.HdrContentType: []string{cmn.ContentJSON},
		}
		reqParams.Query = q
	}
	err := reqParams.DoHTTPRequest()
	freeRp(reqParams)
	return err
}

// GetDaemonConfig returns the configuration of a specific daemon in a cluster.
func GetDaemonConfig(baseParams BaseParams, node *cluster.Snode) (config *cmn.Config, err error) {
	baseParams.Method = http.MethodGet
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathReverseDaemon.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.GetWhatConfig}}
		reqParams.Header = http.Header{apc.HdrNodeID: []string{node.ID()}}
	}
	err = reqParams.DoHTTPReqResp(&config)
	freeRp(reqParams)
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
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathReverseDaemon.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.GetWhatStats}}
		reqParams.Header = http.Header{apc.HdrNodeID: []string{node.ID()}}
	}
	err = reqParams.DoHTTPReqResp(&ds)
	freeRp(reqParams)
	return ds, err
}

// GetDaemonLog returns log of a specific daemon in a cluster.
func GetDaemonLog(baseParams BaseParams, node *cluster.Snode, args GetLogInput) error {
	w := args.Writer
	q := url.Values{}
	q.Set(apc.QparamWhat, apc.GetWhatLog)
	if args.Severity != "" {
		q.Set(apc.QparamSev, args.Severity)
	}
	baseParams.Method = http.MethodGet
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathReverseDaemon.S
		reqParams.Query = q
		reqParams.Header = http.Header{apc.HdrNodeID: []string{node.ID()}}
	}
	err := reqParams.DoHTTPReqResp(w)
	freeRp(reqParams)
	return err
}

// GetDaemonStatus returns information about specific node in a cluster.
func GetDaemonStatus(baseParams BaseParams, node *cluster.Snode) (daeInfo *stats.DaemonStatus, err error) {
	baseParams.Method = http.MethodGet
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathReverseDaemon.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.GetWhatDaemonStatus}}
		reqParams.Header = http.Header{apc.HdrNodeID: []string{node.ID()}}
	}
	err = reqParams.DoHTTPReqResp(&daeInfo)
	freeRp(reqParams)
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
		query.Add(apc.ActTransient, strconv.FormatBool(transient[0]))
	}
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathReverseDaemon.Join(apc.ActSetConfig)
		reqParams.Query = query
		reqParams.Header = http.Header{apc.HdrNodeID: []string{nodeID}}
	}
	err := reqParams.DoHTTPRequest()
	freeRp(reqParams)
	return err
}

// ResetDaemonConfig resets the configuration for a specific node to the cluster configuration.
func ResetDaemonConfig(baseParams BaseParams, nodeID string) error {
	baseParams.Method = http.MethodPut
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathReverseDaemon.S
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActResetConfig})
		reqParams.Header = http.Header{
			apc.HdrNodeID:      []string{nodeID},
			cmn.HdrContentType: []string{cmn.ContentJSON},
		}
	}
	err := reqParams.DoHTTPRequest()
	freeRp(reqParams)
	return err
}
