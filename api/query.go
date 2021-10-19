// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"net/http"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/query"
)

func InitQuery(baseParams BaseParams, objectsTemplate string, bck cmn.Bck, filter *query.FilterMsg, workersCnts ...uint) (string, error) {
	var (
		outerSelectMsg = query.OuterSelectMsg{Template: objectsTemplate}
		fromMsg        = query.FromMsg{Bck: bck}
		qMsg           = query.DefMsg{
			OuterSelect: outerSelectMsg,
			From:        fromMsg,
			Where:       query.WhereMsg{Filter: filter},
		}

		workersCnt uint
		handle     string
	)

	baseParams.Method = http.MethodPost
	if len(workersCnts) > 0 {
		workersCnt = workersCnts[0]
	}

	initMsg := query.InitMsg{QueryMsg: qMsg, WorkersCnt: workersCnt}

	err := DoHTTPReqResp(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathQueryInit.S,
		Body:       cos.MustMarshal(initMsg),
		Header:     http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}},
	}, &handle)
	return handle, err
}

func NextQueryResults(baseParams BaseParams, handle string, size uint) ([]*cmn.BucketEntry, error) {
	var objectsNames []*cmn.BucketEntry

	baseParams.Method = http.MethodGet
	err := DoHTTPReqResp(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathQueryNext.S,
		Body:       cos.MustMarshal(query.NextMsg{Handle: handle, Size: size}),
		Header:     http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}},
	}, &objectsNames)

	return objectsNames, err
}

func QueryWorkerTarget(baseParams BaseParams, handle string, workerID uint) (daemonID string, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPReqResp(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathQueryWorker.S,
		Body:       cos.MustMarshal(query.NextMsg{Handle: handle, WorkerID: workerID}),
		Header:     http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}},
	}, &daemonID)
	return
}
