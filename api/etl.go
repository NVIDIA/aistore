// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ext/etl"
)

type ETLObjArgs struct {
	// TransformArgs holds the arguments to be used in ETL inline transform,
	// which will be sent as `apc.QparamETLArgs` query parameter in the request.
	// Optional, can be omitted (nil).
	TransformArgs any

	// ETLName specifies the running ETL instance to be used in inline transform.
	ETLName string
}

// Initiate custom ETL workload by executing one of the documented `etl.InitMsg`
// message types.
// The API call results in deploying multiple ETL containers (K8s pods):
// one container per storage target.
// Returns xaction ID if successful, an error otherwise.
func ETLInit(bp BaseParams, msg etl.InitMsg) (xid string, err error) {
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathETL.S
		reqParams.Body = cos.MustMarshal(msg)
	}
	_, err = reqParams.doReqStr(&xid)
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
	r, size, err := reqParams.doReader()
	FreeRp(reqParams)
	if err != nil {
		return nil, err
	}
	defer cos.Close(r)

	b, err := cos.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	debug.Assert(size < 0 || size == int64(len(b)), size, " != ", len(b))
	return etl.UnmarshalInitMsg(b)
}

func ETLLogs(bp BaseParams, etlName string, targetID ...string) (logs etl.LogsByTarget, err error) {
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

func ETLMetrics(params BaseParams, etlName string) (healths etl.CPUMemByTarget, err error) {
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

func ETLHealth(params BaseParams, etlName string) (healths etl.HealthByTarget, err error) {
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

func ETLObject(bp BaseParams, args *ETLObjArgs, bck cmn.Bck, objName string, w io.Writer) (oah ObjAttrs, err error) {
	query := url.Values{apc.QparamETLName: []string{args.ETLName}}
	if args.TransformArgs != nil {
		targs, err := cos.ConvertToString(args.TransformArgs)
		if err != nil {
			return oah, err
		}
		query.Add(apc.QparamETLTransformArgs, targs)
	}

	return GetObject(bp, bck, objName, &GetArgs{Writer: w, Query: query})
}

// NOTE: for ETLBucket(), see api/bucket
