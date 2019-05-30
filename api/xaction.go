// Package api provides RESTful API to AIS object storage
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
	jsoniter "github.com/json-iterator/go"
)

// MakeXactGetRequest API
//
// MakeXactGetRequest gets the response of the Xaction Query
// Action can be one of: start, stop, stats
// Kind will be one of the xactions
func MakeXactGetRequest(baseParams *BaseParams, kind, action, bucket string, all bool) (map[string][]*stats.BaseXactStatsExt, error) {
	var (
		resp      *http.Response
		xactStats = make(map[string][]*stats.BaseXactStatsExt)
	)
	optParams := OptionalParams{}
	actMsg := &cmn.ActionMsg{
		Action: action,
		Name:   kind,
		Value: cmn.XactionExtMsg{
			Bucket: bucket,
			All:    all,
		},
	}
	msg, err := jsoniter.Marshal(actMsg)
	if err != nil {
		return nil, err
	}

	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Cluster)
	optParams.Query = url.Values{cmn.URLParamWhat: []string{cmn.GetWhatXaction}}
	if resp, err = doHTTPRequestGetResp(baseParams, path, msg, optParams); err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if action == cmn.ActXactStats {
		bod, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(bod, &xactStats); err != nil {
			return nil, err
		}
		return xactStats, nil
	}

	return nil, nil
}

// MakeNCopies API
//
// MakeNCopies starts an extended action (xaction) to bring a given bucket to a certain redundancy level (num copies)
func MakeNCopies(baseParams *BaseParams, bucket string, copies int) error {
	b, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActMakeNCopies, Value: copies})
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
	_, err = DoHTTPRequest(baseParams, path, b)
	return err
}
