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
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.ETL, cmn.ETLInit),
		Body:       spec,
	}, &id)
	return id, err
}

func ETLBuild(baseParams BaseParams, msg etl.BuildMsg) (id string, err error) {
	baseParams.Method = http.MethodPost
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.ETL, cmn.ETLBuild),
		Body:       cmn.MustMarshal(msg),
	}, &id)
	return id, err
}

func ETLList(baseParams BaseParams) (list []etl.Info, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.ETL, cmn.ETLList),
	}, &list)
	return list, err
}

func ETLLogs(baseParams BaseParams, id string, targetID ...string) (logs etl.PodsLogsMsg, err error) {
	baseParams.Method = http.MethodGet
	path := cmn.JoinWords(cmn.Version, cmn.ETL, cmn.ETLLogs, id)
	if len(targetID) > 0 && targetID[0] != "" {
		path = cmn.JoinWords(path, targetID[0])
	}
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       path,
	}, &logs)
	return logs, err
}

func ETLStop(baseParams BaseParams, id string) (err error) {
	baseParams.Method = http.MethodDelete
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.ETL, cmn.ETLStop, id),
	})
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
	baseParams.Method = http.MethodPost
	bckMsg.BckTo = toBck
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Buckets, fromBck.Name),
		Body:       cmn.MustMarshal(cmn.ActionMsg{Action: cmn.ActETLBucket, Name: toBck.Name, Value: bckMsg}),
	}, &xactID)
	return
}
