// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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
	err := DoHTTPReqResp(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathdSort.S,
		Body:       cos.MustMarshal(rs),
		Header:     http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}},
	}, &id)
	return id, err
}

func AbortDSort(baseParams BaseParams, managerUUID string) error {
	baseParams.Method = http.MethodDelete
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathdSortAbort.S,
		Query:      url.Values{cmn.URLParamUUID: []string{managerUUID}},
	})
}

func MetricsDSort(baseParams BaseParams, managerUUID string) (metrics map[string]*dsort.Metrics, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPReqResp(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathdSort.S,
		Query:      url.Values{cmn.URLParamUUID: []string{managerUUID}},
	}, &metrics)
	return metrics, err
}

func RemoveDSort(baseParams BaseParams, managerUUID string) error {
	baseParams.Method = http.MethodDelete
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathdSort.S,
		Query:      url.Values{cmn.URLParamUUID: []string{managerUUID}},
	})
}

func ListDSort(baseParams BaseParams, regex string) (jobsInfos []*dsort.JobInfo, err error) {
	baseParams.Method = http.MethodGet

	err = DoHTTPReqResp(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathdSort.S,
		Query:      url.Values{cmn.URLParamRegex: []string{regex}},
	}, &jobsInfos)
	return jobsInfos, err
}
