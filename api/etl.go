// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"io"
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/etl"
)

func ETLInitSpec(baseParams BaseParams, podspec []byte /*yaml*/) (id string, err error) {
	baseParams.Method = http.MethodPost
	err = DoHTTPReqResp(ReqParams{BaseParams: baseParams, Path: cmn.URLPathETLInitSpec.S, Body: podspec}, &id)
	return id, err
}

func ETLInitCode(baseParams BaseParams, msg etl.InitCodeMsg) (id string, err error) {
	baseParams.Method = http.MethodPost
	err = DoHTTPReqResp(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathETLInitCode.S,
		Body:       cos.MustMarshal(msg),
		Header:     http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}},
	}, &id)
	return id, err
}

func ETLList(baseParams BaseParams) (list []etl.Info, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPReqResp(ReqParams{BaseParams: baseParams, Path: cmn.URLPathETLList.S}, &list)
	return list, err
}

func ETLLogs(baseParams BaseParams, id string, targetID ...string) (logs etl.PodsLogsMsg, err error) {
	baseParams.Method = http.MethodGet
	path := cmn.URLPathETLLogs.Join(id)
	if len(targetID) > 0 && targetID[0] != "" {
		path = cos.JoinWords(path, targetID[0])
	}
	err = DoHTTPReqResp(ReqParams{BaseParams: baseParams, Path: path}, &logs)
	return logs, err
}

func ETLHealth(params BaseParams, id string) (healths etl.PodsHealthMsg, err error) {
	params.Method = http.MethodGet
	path := cmn.URLPathETLHealth.Join(id)
	err = DoHTTPReqResp(ReqParams{BaseParams: params, Path: path}, &healths)
	return healths, err
}

func ETLStop(baseParams BaseParams, id string) (err error) {
	baseParams.Method = http.MethodDelete
	err = DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: cmn.URLPathETLStop.Join(id)})
	return err
}

// TODO: "if query has UUID then the request is ETL" is not good enough. Add ETL-specific
//       query param and change the examples/docs (!4455)
func ETLObject(baseParams BaseParams, id string, bck cmn.Bck, objName string, w io.Writer) (err error) {
	_, err = GetObject(baseParams, bck, objName, GetObjectInput{
		Writer: w,
		Query:  url.Values{cmn.URLParamUUID: []string{id}},
	})
	return
}

func ETLBucket(baseParams BaseParams, fromBck, toBck cmn.Bck, bckMsg *cmn.TCBMsg) (xactID string, err error) {
	if err = toBck.Validate(); err != nil {
		return
	}
	baseParams.Method = http.MethodPost
	q := cmn.AddBckToQuery(nil, fromBck)
	_ = cmn.AddBckUnameToQuery(q, toBck, cmn.URLParamBucketTo)
	err = DoHTTPReqResp(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathBuckets.Join(fromBck.Name),
		Body:       cos.MustMarshal(cmn.ActionMsg{Action: cmn.ActETLBck, Value: bckMsg}),
		Header:     http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}},
		Query:      q,
	}, &xactID)
	return
}
