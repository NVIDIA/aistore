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

func ETLInit(bp BaseParams, msg etl.InitMsg) (id string, err error) {
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathETL.S
		reqParams.Body = cos.MustMarshal(msg)
	}
	err = reqParams.DoReqResp(&id)
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
	err = reqParams.DoReqResp(&list)
	FreeRp(reqParams)
	return
}

func ETLGetInitMsg(params BaseParams, id string) (etl.InitMsg, error) {
	params.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = params
		reqParams.Path = apc.URLPathETL.Join(id)
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

func ETLLogs(bp BaseParams, id string, targetID ...string) (logs etl.PodsLogsMsg, err error) {
	bp.Method = http.MethodGet
	var path string
	if len(targetID) > 0 && targetID[0] != "" {
		path = apc.URLPathETL.Join(id, apc.ETLLogs, targetID[0])
	} else {
		path = apc.URLPathETL.Join(id, apc.ETLLogs)
	}
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = path
	}
	err = reqParams.DoReqResp(&logs)
	FreeRp(reqParams)
	return
}

func ETLHealth(params BaseParams, id string) (healths etl.PodsHealthMsg, err error) {
	params.Method = http.MethodGet
	path := apc.URLPathETL.Join(id, apc.ETLHealth)
	reqParams := AllocRp()
	{
		reqParams.BaseParams = params
		reqParams.Path = path
	}
	err = reqParams.DoReqResp(&healths)
	FreeRp(reqParams)
	return
}

func ETLDelete(bp BaseParams, id string) (err error) {
	bp.Method = http.MethodDelete
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathETL.Join(id)
	}
	err = reqParams.DoRequest()
	FreeRp(reqParams)
	return
}

func ETLStop(bp BaseParams, id string) (err error) {
	return etlPostAction(bp, id, apc.ETLStop)
}

func ETLStart(bp BaseParams, id string) (err error) {
	return etlPostAction(bp, id, apc.ETLStart)
}

func etlPostAction(bp BaseParams, id, action string) (err error) {
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathETL.Join(id, action)
	}
	err = reqParams.DoRequest()
	FreeRp(reqParams)
	return
}

// TODO: add ETL-specific query param and change the examples/docs (!4455)
func ETLObject(bp BaseParams, id string, bck cmn.Bck, objName string, w io.Writer) (err error) {
	_, err = GetObject(bp, bck, objName, GetObjectInput{
		Writer: w,
		Query:  url.Values{apc.QparamUUID: []string{id}},
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
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActETLBck, Value: bckMsg})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}
	err = reqParams.DoReqResp(&xactID)
	FreeRp(reqParams)
	return
}
