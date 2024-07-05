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
	"github.com/NVIDIA/aistore/ios"
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
		reqParams.Path = apc.URLPathReverseDae.S // NOTE: reverse, via p.reverseHandler
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.WhatMountpaths}}
		reqParams.Header = http.Header{
			apc.HdrNodeID:  []string{node.ID()},
			apc.HdrNodeURL: []string{node.URL(cmn.NetPublic)},
		}
	}
	_, err = reqParams.DoReqAny(&mpl)
	FreeRp(reqParams)
	return mpl, err
}

func AttachMountpath(bp BaseParams, node *meta.Snode, mountpath string, label ...ios.Label) error {
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.Join(apc.Mountpaths) // NOTE: reverse, via p.reverseHandler
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActMountpathAttach, Value: mountpath})
		reqParams.Header = http.Header{
			apc.HdrNodeID:      []string{node.ID()},
			apc.HdrNodeURL:     []string{node.URL(cmn.NetPublic)},
			cos.HdrContentType: []string{cos.ContentJSON},
		}
		if len(label) > 0 {
			if lb := string(label[0]); lb != "" {
				reqParams.Query = url.Values{apc.QparamMpathLabel: []string{lb}}
			}
		}
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

func EnableMountpath(bp BaseParams, node *meta.Snode, mountpath string) error {
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.Join(apc.Mountpaths) // NOTE: reverse, via p.reverseHandler
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActMountpathEnable, Value: mountpath})
		reqParams.Header = http.Header{
			apc.HdrNodeID:      []string{node.ID()},
			apc.HdrNodeURL:     []string{node.URL(cmn.NetPublic)},
			cos.HdrContentType: []string{cos.ContentJSON},
		}
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

func DetachMountpath(bp BaseParams, node *meta.Snode, mountpath string, dontResilver bool) error {
	var q url.Values
	if dontResilver {
		q = url.Values{apc.QparamDontResilver: []string{"true"}}
	}
	bp.Method = http.MethodDelete
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.Join(apc.Mountpaths) // NOTE: reverse, via p.reverseHandler
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActMountpathDetach, Value: mountpath})
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

func DisableMountpath(bp BaseParams, node *meta.Snode, mountpath string, dontResilver bool) error {
	var q url.Values
	if dontResilver {
		q = url.Values{apc.QparamDontResilver: []string{"true"}}
	}
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.Join(apc.Mountpaths) // NOTE: reverse, via p.reverseHandler
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActMountpathDisable, Value: mountpath})
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
		reqParams.Path = apc.URLPathReverseDae.S // NOTE: reverse, via p.reverseHandler
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.WhatNodeConfig}}
		reqParams.Header = http.Header{apc.HdrNodeID: []string{node.ID()}}
	}
	_, err = reqParams.DoReqAny(&config)
	FreeRp(reqParams)
	if err != nil {
		return nil, err
	}
	// FIXME: transform backend structures on the client side
	// as a side effect, config.Backend validation populates non-JSON structs that client can utilize;
	_ = config.Backend.Validate()
	return config, nil
}

// names _and_ kinds, i.e. (name, kind) pairs
func GetMetricNames(bp BaseParams, node *meta.Snode) (kvs cos.StrKVs, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.S // NOTE: reverse, via p.reverseHandler
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
		reqParams.Path = apc.URLPathReverseDae.S // NOTE: reverse, via p.reverseHandler
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
		reqParams.Path = apc.URLPathReverseDae.Join(apc.ActSetConfig) // NOTE: reverse, via p.reverseHandler
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
		reqParams.Path = apc.URLPathReverseDae.S // NOTE: reverse, via p.reverseHandler
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
