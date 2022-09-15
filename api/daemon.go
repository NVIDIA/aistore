// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
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
func GetMountpaths(bp BaseParams, node *cluster.Snode) (mpl *apc.MountpathList, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.GetWhatMountpaths}}
		reqParams.Header = http.Header{
			apc.HdrNodeID:  []string{node.ID()},
			apc.HdrNodeURL: []string{node.URL(cmn.NetPublic)},
		}
	}
	err = reqParams.DoHTTPReqResp(&mpl)
	FreeRp(reqParams)
	return mpl, err
}

// TODO: rewrite tests that come here with `force`
func AttachMountpath(bp BaseParams, node *cluster.Snode, mountpath string, force bool) error {
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.Join(apc.Mountpaths)
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActMountpathAttach, Value: mountpath})
		reqParams.Header = http.Header{
			apc.HdrNodeID:      []string{node.ID()},
			apc.HdrNodeURL:     []string{node.URL(cmn.NetPublic)},
			cos.HdrContentType: []string{cos.ContentJSON},
		}
		reqParams.Query = url.Values{apc.QparamForce: []string{strconv.FormatBool(force)}}
	}
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

func EnableMountpath(bp BaseParams, node *cluster.Snode, mountpath string) error {
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.Join(apc.Mountpaths)
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActMountpathEnable, Value: mountpath})
		reqParams.Header = http.Header{
			apc.HdrNodeID:      []string{node.ID()},
			apc.HdrNodeURL:     []string{node.URL(cmn.NetPublic)},
			cos.HdrContentType: []string{cos.ContentJSON},
		}
	}
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

func DetachMountpath(bp BaseParams, node *cluster.Snode, mountpath string, dontResilver bool) error {
	var q url.Values
	if dontResilver {
		q = url.Values{apc.QparamDontResilver: []string{"true"}}
	}
	bp.Method = http.MethodDelete
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.Join(apc.Mountpaths)
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActMountpathDetach, Value: mountpath})
		reqParams.Header = http.Header{
			apc.HdrNodeID:      []string{node.ID()},
			cos.HdrContentType: []string{cos.ContentJSON},
		}
		reqParams.Query = q
	}
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

func DisableMountpath(bp BaseParams, node *cluster.Snode, mountpath string, dontResilver bool) error {
	var q url.Values
	if dontResilver {
		q = url.Values{apc.QparamDontResilver: []string{"true"}}
	}
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.Join(apc.Mountpaths)
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActMountpathDisable, Value: mountpath})
		reqParams.Header = http.Header{
			apc.HdrNodeID:      []string{node.ID()},
			cos.HdrContentType: []string{cos.ContentJSON},
		}
		reqParams.Query = q
	}
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

// GetDaemonConfig returns the configuration of a specific daemon in a cluster.
func GetDaemonConfig(bp BaseParams, node *cluster.Snode) (config *cmn.Config, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.GetWhatConfig}}
		reqParams.Header = http.Header{apc.HdrNodeID: []string{node.ID()}}
	}
	err = reqParams.DoHTTPReqResp(&config)
	FreeRp(reqParams)
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

func GetDaemonStats(bp BaseParams, node *cluster.Snode) (ds *stats.DaemonStats, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.GetWhatStats}}
		reqParams.Header = http.Header{apc.HdrNodeID: []string{node.ID()}}
	}
	err = reqParams.DoHTTPReqResp(&ds)
	FreeRp(reqParams)
	return ds, err
}

// GetDaemonLog returns log of a specific daemon in a cluster.
func GetDaemonLog(bp BaseParams, node *cluster.Snode, args GetLogInput) error {
	w := args.Writer
	q := url.Values{}
	q.Set(apc.QparamWhat, apc.GetWhatLog)
	if args.Severity != "" {
		q.Set(apc.QparamSev, args.Severity)
	}
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.S
		reqParams.Query = q
		reqParams.Header = http.Header{apc.HdrNodeID: []string{node.ID()}}
	}
	err := reqParams.DoHTTPReqResp(w)
	FreeRp(reqParams)
	return err
}

// GetDaemonStatus returns information about specific node in a cluster.
func GetDaemonStatus(bp BaseParams, node *cluster.Snode) (daeInfo *stats.DaemonStatus, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.GetWhatDaemonStatus}}
		reqParams.Header = http.Header{apc.HdrNodeID: []string{node.ID()}}
	}
	err = reqParams.DoHTTPReqResp(&daeInfo)
	FreeRp(reqParams)
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
func SetDaemonConfig(bp BaseParams, nodeID string, nvs cos.SimpleKVs, transient ...bool) error {
	bp.Method = http.MethodPut
	query := url.Values{}
	for key, val := range nvs {
		query.Add(key, val)
	}
	if len(transient) > 0 {
		query.Add(apc.ActTransient, strconv.FormatBool(transient[0]))
	}
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.Join(apc.ActSetConfig)
		reqParams.Query = query
		reqParams.Header = http.Header{apc.HdrNodeID: []string{nodeID}}
	}
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

// ResetDaemonConfig resets the configuration for a specific node to the cluster configuration.
func ResetDaemonConfig(bp BaseParams, nodeID string) error {
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.S
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActResetConfig})
		reqParams.Header = http.Header{
			apc.HdrNodeID:      []string{nodeID},
			cos.HdrContentType: []string{cos.ContentJSON},
		}
	}
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}
