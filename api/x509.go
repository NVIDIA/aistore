// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
)

func LoadX509Cert(bp BaseParams, nodeID ...string) error {
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		if len(nodeID) > 0 {
			reqParams.Path = apc.URLPathCluX509.Join(nodeID[0]) // the node
		} else {
			reqParams.Path = apc.URLPathCluX509.S // all nodes
		}
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

func GetX509Info(bp BaseParams, nodeID ...string) (info cos.StrKVs, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.WhatCertificate}}
		if len(nodeID) > 0 {
			reqParams.Path = apc.URLPathReverseDae.S
			reqParams.Header = http.Header{
				apc.HdrNodeID: []string{nodeID[0]},
			}
		} else {
			reqParams.Path = apc.URLPathDae.S
		}
	}
	_, err = reqParams.DoReqAny(&info)
	FreeRp(reqParams)
	return
}
