// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/dsort"
)

func StartDSort(baseParams BaseParams, rs dsort.RequestSpec) (string, error) {
	var id string
	baseParams.Method = http.MethodPost
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathdSort.S,
		Body:       cos.MustMarshal(rs),
		Header:     http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}},
	}
	err := reqParams.DoHTTPReqResp(&id)
	return id, err
}

func AbortDSort(baseParams BaseParams, managerUUID string) error {
	baseParams.Method = http.MethodDelete
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathdSortAbort.S,
		Query:      url.Values{cmn.URLParamUUID: []string{managerUUID}},
	}
	return reqParams.DoHTTPRequest()
}

func MetricsDSort(baseParams BaseParams, managerUUID string) (metrics map[string]*dsort.Metrics, err error) {
	baseParams.Method = http.MethodGet
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathdSort.S,
		Query:      url.Values{cmn.URLParamUUID: []string{managerUUID}},
	}
	err = reqParams.DoHTTPReqResp(&metrics)
	return metrics, err
}

func RemoveDSort(baseParams BaseParams, managerUUID string) error {
	baseParams.Method = http.MethodDelete
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathdSort.S,
		Query:      url.Values{cmn.URLParamUUID: []string{managerUUID}},
	}
	return reqParams.DoHTTPRequest()
}

func ListDSort(baseParams BaseParams, regex string) (jobsInfos []*dsort.JobInfo, err error) {
	baseParams.Method = http.MethodGet
	reqParams := &ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathdSort.S,
		Query:      url.Values{cmn.URLParamRegex: []string{regex}},
	}
	err = reqParams.DoHTTPReqResp(&jobsInfos)
	return jobsInfos, err
}
