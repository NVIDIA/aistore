// Package api provides RESTful API to AIS object storage
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort"
	jsoniter "github.com/json-iterator/go"
)

func StartDSort(baseParams BaseParams, rs dsort.RequestSpec) (string, error) {
	msg, err := jsoniter.Marshal(rs)
	if err != nil {
		return "", err
	}

	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Sort)
	body, err := DoHTTPRequest(baseParams, path, msg)
	if err != nil {
		return "", err
	}

	return string(body), err
}

func AbortDSort(baseParams BaseParams, managerUUID string) error {
	baseParams.Method = http.MethodDelete
	path := cmn.URLPath(cmn.Version, cmn.Sort, cmn.Abort)
	query := url.Values{cmn.URLParamID: []string{managerUUID}}
	optParams := OptionalParams{Query: query}
	_, err := DoHTTPRequest(baseParams, path, nil, optParams)
	return err
}

func MetricsDSort(baseParams BaseParams, managerUUID string) (map[string]*dsort.Metrics, error) {
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Sort)
	query := url.Values{cmn.URLParamID: []string{managerUUID}}
	optParams := OptionalParams{Query: query}
	body, err := DoHTTPRequest(baseParams, path, nil, optParams)
	if err != nil {
		return nil, err
	}

	var metrics map[string]*dsort.Metrics
	err = jsoniter.Unmarshal(body, &metrics)
	return metrics, err
}

func RemoveDSort(baseParams BaseParams, managerUUID string) error {
	baseParams.Method = http.MethodDelete
	path := cmn.URLPath(cmn.Version, cmn.Sort)
	query := url.Values{cmn.URLParamID: []string{managerUUID}}
	optParams := OptionalParams{Query: query}
	_, err := DoHTTPRequest(baseParams, path, nil, optParams)
	return err
}

func ListDSort(baseParams BaseParams, regex string) ([]*dsort.JobInfo, error) {
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Sort)
	query := url.Values{}
	query.Add(cmn.URLParamRegex, regex)
	optParams := OptionalParams{
		Query: query,
	}

	body, err := DoHTTPRequest(baseParams, path, nil, optParams)
	if err != nil {
		return nil, err
	}

	var jobsInfos []*dsort.JobInfo
	err = jsoniter.Unmarshal(body, &jobsInfos)
	return jobsInfos, err
}
