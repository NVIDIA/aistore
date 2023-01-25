// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ext/etl"
)

// Initiate custom ETL workload by executing one of the documented `etl.InitMsg`
// message types.
// The API call results in deploying multiple ETL containers (K8s pods):
// one container per storage target.
// Returns xaction ID if successful, an error otherwise.
func ETLInit(bp BaseParams, msg etl.InitMsg) (xactID string, err error) {
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathETL.S
		reqParams.Body = cos.MustMarshal(msg)
	}
	_, err = reqParams.doReqStr(&xactID)
	FreeRp(reqParams)
	return
}

func ETLList(bp BaseParams) (list []etl.Info, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathETL.S
	}
	_, err = reqParams.DoReqAny(&list)
	FreeRp(reqParams)
	return
}

func ETLGetInitMsg(params BaseParams, etlName string) (etl.InitMsg, error) {
	params.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = params
		reqParams.Path = apc.URLPathETL.Join(etlName)
	}
	r, err := reqParams.doReader()
	FreeRp(reqParams)
	if err != nil {
		return nil, err
	}
	defer cos.Close(r)

	b, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read response, err: %w", err)
	}
	return etl.UnmarshalInitMsg(b)
}

func ETLLogs(bp BaseParams, etlName string, targetID ...string) (logs etl.PodsLogsMsg, err error) {
	bp.Method = http.MethodGet
	var path string
	if len(targetID) > 0 && targetID[0] != "" {
		path = apc.URLPathETL.Join(etlName, apc.ETLLogs, targetID[0])
	} else {
		path = apc.URLPathETL.Join(etlName, apc.ETLLogs)
	}
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = path
	}
	_, err = reqParams.DoReqAny(&logs)
	FreeRp(reqParams)
	return
}

func ETLMetrics(params BaseParams, etlName string) (healths etl.PodsMetricsMsg, err error) {
	params.Method = http.MethodGet
	path := apc.URLPathETL.Join(etlName, apc.ETLMetrics)
	reqParams := AllocRp()
	{
		reqParams.BaseParams = params
		reqParams.Path = path
	}
	_, err = reqParams.DoReqAny(&healths)
	FreeRp(reqParams)
	return
}

func ETLHealth(params BaseParams, etlName string) (healths etl.PodsHealthMsg, err error) {
	params.Method = http.MethodGet
	path := apc.URLPathETL.Join(etlName, apc.ETLHealth)
	reqParams := AllocRp()
	{
		reqParams.BaseParams = params
		reqParams.Path = path
	}
	_, err = reqParams.DoReqAny(&healths)
	FreeRp(reqParams)
	return
}

func ETLDelete(bp BaseParams, etlName string) (err error) {
	bp.Method = http.MethodDelete
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathETL.Join(etlName)
	}
	err = reqParams.DoRequest()
	FreeRp(reqParams)
	return
}

func ETLStop(bp BaseParams, etlName string) (err error) {
	return etlPostAction(bp, etlName, apc.ETLStop)
}

func ETLStart(bp BaseParams, etlName string) (err error) {
	return etlPostAction(bp, etlName, apc.ETLStart)
}

func etlPostAction(bp BaseParams, etlName, action string) (err error) {
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathETL.Join(etlName, action)
	}
	err = reqParams.DoRequest()
	FreeRp(reqParams)
	return
}

// TODO: add ETL-specific query param and change the examples/docs (!4455)
func ETLObject(bp BaseParams, etlName string, bck cmn.Bck, objName string, w io.Writer) (err error) {
	_, err = GetObject(bp, bck, objName, &GetArgs{
		Writer: w,
		Query:  url.Values{apc.QparamETLName: []string{etlName}},
	})
	return
}

func ETLBucket(bp BaseParams, fromBck, toBck cmn.Bck, bckMsg *apc.TCBMsg) (xactID string, err error) {
	if err = toBck.Validate(); err != nil {
		return
	}
	bp.Method = http.MethodPost
	q := fromBck.AddToQuery(nil)
	_ = toBck.AddUnameToQuery(q, apc.QparamBckTo)
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathBuckets.Join(fromBck.Name)
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActETLBck, Value: bckMsg})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}
	_, err = reqParams.doReqStr(&xactID)
	FreeRp(reqParams)
	return
}
