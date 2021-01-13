// Package api provides RESTful API to AIS object storage
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"io"
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/etl"
)

func ETLInit(baseParams BaseParams, spec []byte) (id string, err error) {
	baseParams.Method = http.MethodPost
	err = DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: cmn.URLPathETLInit.S, Body: spec}, &id)
	return id, err
}

func ETLBuild(baseParams BaseParams, msg etl.BuildMsg) (id string, err error) {
	baseParams.Method = http.MethodPost
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathETLBuild.S,
		Body:       cmn.MustMarshal(msg),
	}, &id)
	return id, err
}

func ETLList(baseParams BaseParams) (list []etl.Info, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: cmn.URLPathETLList.S}, &list)
	return list, err
}

func ETLLogs(baseParams BaseParams, id string, targetID ...string) (logs etl.PodsLogsMsg, err error) {
	baseParams.Method = http.MethodGet
	path := cmn.URLPathETLLogs.Join(id)
	if len(targetID) > 0 && targetID[0] != "" {
		path = cmn.JoinWords(path, targetID[0])
	}
	err = DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: path}, &logs)
	return logs, err
}

func ETLHealth(params BaseParams, id string) (healths etl.PodsHealthMsg, err error) {
	params.Method = http.MethodGet
	path := cmn.URLPathETLHealth.Join(id)
	err = DoHTTPRequest(ReqParams{BaseParams: params, Path: path}, &healths)
	return healths, err
}

func ETLStop(baseParams BaseParams, id string) (err error) {
	baseParams.Method = http.MethodDelete
	err = DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: cmn.URLPathETLStop.Join(id)})
	return err
}

func ETLObject(baseParams BaseParams, id string, bck cmn.Bck, objName string, w io.Writer) (err error) {
	_, err = GetObject(baseParams, bck, objName, GetObjectInput{
		Writer: w,
		Query:  url.Values{cmn.URLParamUUID: []string{id}},
	})
	return
}

func ETLBucket(baseParams BaseParams, fromBck, toBck cmn.Bck, bckMsg *cmn.Bck2BckMsg) (xactID string, err error) {
	if err = cmn.ValidateBckName(toBck.Name); err != nil {
		return
	}
	baseParams.Method = http.MethodPost
	q := cmn.AddBckUnameToQuery(nil, fromBck, cmn.URLParamBucket) // aka cmn.URLParamBucketFrom
	_ = cmn.AddBckUnameToQuery(q, toBck, cmn.URLParamBucketTo)
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathBuckets.Join(fromBck.Name),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActETLBck, Value: bckMsg}),
		Query:      q,
	}, &xactID)
	return
}
