// Package api provides RESTful API to AIS object storage
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
)

// GetXactionResponse API
//
// GetXactionResponse gets the response of the Xaction Query
// Action can be one of: start, stop, stats
// Kind will be one of the xactions
// 1. if kind is empty and bucket is empty, request will be executed on all xactions
// 2. if kind is empty and bucket is not empty, request will be executed on all bucket's xactions
// 3. if kind is name of global xaction, bucket is disregarded
func GetXactionResponse(baseParams *BaseParams, kind, action, bucket string) (b []byte, err error) {
	optParams := OptionalParams{}
	actMsg := &cmn.ActionMsg{
		Action: action,
		Name:   kind,
		Value: cmn.XactionExtMsg{
			Bucket: bucket,
		},
	}
	msg, err := jsoniter.Marshal(actMsg)
	if err != nil {
		return nil, err
	}

	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Cluster)
	optParams.Query = url.Values{cmn.URLParamWhat: []string{cmn.GetWhatXaction}}
	if b, err = DoHTTPRequest(baseParams, path, msg, optParams); err != nil {
		return nil, err
	}

	return b, nil
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
