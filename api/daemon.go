// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
)

type GetLogInput struct {
	Writer   io.Writer
	Severity string // one of: {cmn.LogInfo, ...}
	Offset   int64
	All      bool
}

// GetMountpaths given the direct public URL of the target, returns the target's mountpaths or error.
func GetMountpaths(bp BaseParams, node *meta.Snode) (mpl *apc.MountpathList, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.WhatMountpaths}}
		reqParams.Header = http.Header{
			apc.HdrNodeID: []string{node.ID()},
		}
	}
	_, err = reqParams.DoReqAny(&mpl)
	FreeRp(reqParams)
	return mpl, err
}

func AttachMountpath(bp BaseParams, node *meta.Snode, mountpath string, label ...cos.MountpathLabel) error {
	var q url.Values
	if len(label) > 0 {
		if lb := string(label[0]); lb != "" {
			q = url.Values{apc.QparamMpathLabel: []string{lb}}
		}
	}
	bp.Method = http.MethodPut
	return _actMpath(bp, node, mountpath, apc.ActMountpathAttach, q)
}

func EnableMountpath(bp BaseParams, node *meta.Snode, mountpath string) error {
	bp.Method = http.MethodPost
	return _actMpath(bp, node, mountpath, apc.ActMountpathEnable, nil)
}

func DetachMountpath(bp BaseParams, node *meta.Snode, mountpath string, dontResilver bool) error {
	var q url.Values
	if dontResilver {
		q = url.Values{apc.QparamDontResilver: []string{"true"}}
	}
	bp.Method = http.MethodDelete
	return _actMpath(bp, node, mountpath, apc.ActMountpathDetach, q)
}

func DisableMountpath(bp BaseParams, node *meta.Snode, mountpath string, dontResilver bool) error {
	var q url.Values
	if dontResilver {
		q = url.Values{apc.QparamDontResilver: []string{"true"}}
	}
	bp.Method = http.MethodPost
	return _actMpath(bp, node, mountpath, apc.ActMountpathDisable, q)
}

func RescanMountpath(bp BaseParams, node *meta.Snode, mountpath string, dontResilver bool) error {
	var q url.Values
	if dontResilver {
		q = url.Values{apc.QparamDontResilver: []string{"true"}}
	}
	bp.Method = http.MethodPost
	return _actMpath(bp, node, mountpath, apc.ActMountpathRescan, q)
}

func FshcMountpath(bp BaseParams, node *meta.Snode, mountpath string) error {
	bp.Method = http.MethodPost
	return _actMpath(bp, node, mountpath, apc.ActMountpathFSHC, nil)
}

func _actMpath(bp BaseParams, node *meta.Snode, mountpath, action string, q url.Values) error {
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.Join(apc.Mountpaths)
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: action, Value: mountpath})
		reqParams.Header = http.Header{
			apc.HdrNodeID:      []string{node.ID()},
			cos.HdrContentType: []string{cos.ContentJSON},
		}
		reqParams.Query = q
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

// GetDaemonConfig returns the configuration of a specific daemon in a cluster.
// (compare with `api.GetClusterConfig`)
func GetDaemonConfig(bp BaseParams, node *meta.Snode) (config *cmn.Config, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.WhatNodeConfig}}
		reqParams.Header = http.Header{apc.HdrNodeID: []string{node.ID()}}
	}
	_, err = reqParams.DoReqAny(&config)
	FreeRp(reqParams)
	return config, err
}

// names _and_ kinds, i.e. (name, kind) pairs
func GetMetricNames(bp BaseParams, node *meta.Snode) (kvs cos.StrKVs, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.WhatMetricNames}}
		reqParams.Header = http.Header{apc.HdrNodeID: []string{node.ID()}}
	}
	_, err = reqParams.DoReqAny(&kvs)
	FreeRp(reqParams)
	return
}

// Returns log of a specific node in a cluster.
func GetDaemonLog(bp BaseParams, node *meta.Snode, args GetLogInput) (int64, error) {
	w := args.Writer
	q := make(url.Values, 3)
	q.Set(apc.QparamWhat, apc.WhatLog)
	if args.Severity != "" {
		q.Set(apc.QparamLogSev, args.Severity)
	}
	if args.Offset != 0 {
		q.Set(apc.QparamLogOff, strconv.FormatInt(args.Offset, 10))
	}
	if args.All {
		q.Set(apc.QparamAllLogs, "true")
	}
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.S
		reqParams.Query = q
		reqParams.Header = http.Header{apc.HdrNodeID: []string{node.ID()}}
	}
	wrap, err := reqParams.doWriter(w)
	FreeRp(reqParams)
	if err == nil {
		return wrap.n, nil
	}
	return 0, err
}

// SetDaemonConfig, given key value pairs, sets the configuration accordingly for a specific node.
func SetDaemonConfig(bp BaseParams, nodeID string, nvs cos.StrKVs, transient ...bool) error {
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
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

// reset node's configuration to cluster defaults
func ResetDaemonConfig(bp BaseParams, nodeID string) error {
	return _putDaemon(bp, nodeID, apc.ActMsg{Action: apc.ActResetConfig})
}

func RotateLogs(bp BaseParams, nodeID string) error {
	return _putDaemon(bp, nodeID, apc.ActMsg{Action: apc.ActRotateLogs})
}

func _putDaemon(bp BaseParams, nodeID string, msg apc.ActMsg) error {
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{
			apc.HdrNodeID:      []string{nodeID},
			cos.HdrContentType: []string{cos.ContentJSON},
		}
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}
