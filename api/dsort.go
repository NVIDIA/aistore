// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/dsort"
)

func StartDSort(bp BaseParams, rs dsort.RequestSpec) (string, error) {
	var id string
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathdSort.S
		reqParams.Body = cos.MustMarshal(rs)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	err := reqParams.DoReqResp(&id)
	FreeRp(reqParams)
	return id, err
}

func AbortDSort(bp BaseParams, managerUUID string) error {
	bp.Method = http.MethodDelete
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathdSortAbort.S
		reqParams.Query = url.Values{apc.QparamUUID: []string{managerUUID}}
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

func MetricsDSort(bp BaseParams, managerUUID string) (metrics map[string]*dsort.Metrics, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathdSort.S
		reqParams.Query = url.Values{apc.QparamUUID: []string{managerUUID}}
	}
	err = reqParams.DoReqResp(&metrics)
	FreeRp(reqParams)
	return metrics, err
}

func RemoveDSort(bp BaseParams, managerUUID string) error {
	bp.Method = http.MethodDelete
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathdSort.S
		reqParams.Query = url.Values{apc.QparamUUID: []string{managerUUID}}
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

func ListDSort(bp BaseParams, regex string, onlyActive bool) (jobInfos []*dsort.JobInfo, err error) {
	q := make(url.Values, 2)
	q.Set(apc.QparamRegex, regex)
	if onlyActive {
		q.Set(apc.QparamOnlyActive, "true")
	}
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathdSort.S
		reqParams.Query = q
	}
	err = reqParams.DoReqResp(&jobInfos)
	FreeRp(reqParams)
	return
}
