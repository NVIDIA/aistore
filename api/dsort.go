// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/dsort"
)

func StartDSort(baseParams BaseParams, rs dsort.RequestSpec) (string, error) {
	var id string
	baseParams.Method = http.MethodPost
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathdSort.S
		reqParams.Body = cos.MustMarshal(rs)
		reqParams.Header = http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}}
	}
	err := reqParams.DoHTTPReqResp(&id)
	freeRp(reqParams)
	return id, err
}

func AbortDSort(baseParams BaseParams, managerUUID string) error {
	baseParams.Method = http.MethodDelete
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathdSortAbort.S
		reqParams.Query = url.Values{apc.QparamUUID: []string{managerUUID}}
	}
	err := reqParams.DoHTTPRequest()
	freeRp(reqParams)
	return err
}

func MetricsDSort(baseParams BaseParams, managerUUID string) (metrics map[string]*dsort.Metrics, err error) {
	baseParams.Method = http.MethodGet
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathdSort.S
		reqParams.Query = url.Values{apc.QparamUUID: []string{managerUUID}}
	}
	err = reqParams.DoHTTPReqResp(&metrics)
	freeRp(reqParams)
	return metrics, err
}

func RemoveDSort(baseParams BaseParams, managerUUID string) error {
	baseParams.Method = http.MethodDelete
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathdSort.S
		reqParams.Query = url.Values{apc.QparamUUID: []string{managerUUID}}
	}
	err := reqParams.DoHTTPRequest()
	freeRp(reqParams)
	return err
}

func ListDSort(baseParams BaseParams, regex string) (jobsInfos []*dsort.JobInfo, err error) {
	baseParams.Method = http.MethodGet
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathdSort.S
		reqParams.Query = url.Values{apc.QparamRegex: []string{regex}}
	}
	err = reqParams.DoHTTPReqResp(&jobsInfos)
	freeRp(reqParams)
	return jobsInfos, err
}
