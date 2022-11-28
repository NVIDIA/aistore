// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"net/http"
	"sort"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ext/dload"
)

func DownloadSingle(bp BaseParams, description string,
	bck cmn.Bck, objName, link string, intervals ...time.Duration) (string, error) {
	dlBody := dload.SingleBody{
		SingleObj: dload.SingleObj{
			ObjName: objName,
			Link:    link,
		},
	}
	if len(intervals) > 0 {
		dlBody.ProgressInterval = intervals[0].String()
	}
	dlBody.Bck = bck
	dlBody.Description = description
	return DownloadWithParam(bp, dload.TypeSingle, &dlBody)
}

func DownloadRange(bp BaseParams, description string, bck cmn.Bck, template string, intervals ...time.Duration) (string, error) {
	dlBody := dload.RangeBody{Template: template}
	if len(intervals) > 0 {
		dlBody.ProgressInterval = intervals[0].String()
	}
	dlBody.Bck = bck
	dlBody.Description = description
	return DownloadWithParam(bp, dload.TypeRange, dlBody)
}

func DownloadWithParam(bp BaseParams, dlt dload.Type, body any) (id string, err error) {
	bp.Method = http.MethodPost
	msg := cos.MustMarshal(body)
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathDownload.S
		reqParams.Body = cos.MustMarshal(dload.Body{Type: dlt, RawMessage: msg})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	id, err = reqParams.doDlDownloadRequest()
	FreeRp(reqParams)
	return
}

func DownloadMulti(bp BaseParams, description string, bck cmn.Bck, msg any, intervals ...time.Duration) (string, error) {
	dlBody := dload.MultiBody{}
	if len(intervals) > 0 {
		dlBody.ProgressInterval = intervals[0].String()
	}
	dlBody.Bck = bck
	dlBody.Description = description
	dlBody.ObjectsPayload = msg
	return DownloadWithParam(bp, dload.TypeMulti, dlBody)
}

func DownloadBackend(bp BaseParams, descr string, bck cmn.Bck, prefix, suffix string, ivals ...time.Duration) (string, error) {
	dlBody := dload.BackendBody{Prefix: prefix, Suffix: suffix}
	if len(ivals) > 0 {
		dlBody.ProgressInterval = ivals[0].String()
	}
	dlBody.Bck = bck
	dlBody.Description = descr
	return DownloadWithParam(bp, dload.TypeBackend, dlBody)
}

func DownloadStatus(bp BaseParams, id string, onlyActive bool) (resp *dload.StatusResp, err error) {
	dlBody := dload.AdminBody{ID: id, OnlyActive: onlyActive}
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathDownload.S
		reqParams.Body = cos.MustMarshal(dlBody)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	resp = &dload.StatusResp{}
	err = reqParams.DoReqResp(resp)
	FreeRp(reqParams)
	return
}

func DownloadGetList(bp BaseParams, regex string, onlyActive bool) (dlList dload.JobInfos, err error) {
	dlBody := dload.AdminBody{Regex: regex, OnlyActive: onlyActive}
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathDownload.S
		reqParams.Body = cos.MustMarshal(dlBody)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	err = reqParams.DoReqResp(&dlList)
	FreeRp(reqParams)
	sort.Sort(dlList)
	return
}

func AbortDownload(bp BaseParams, id string) error {
	dlBody := dload.AdminBody{ID: id}
	bp.Method = http.MethodDelete
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathDownloadAbort.S
		reqParams.Body = cos.MustMarshal(dlBody)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

func RemoveDownload(bp BaseParams, id string) error {
	dlBody := dload.AdminBody{ID: id}
	bp.Method = http.MethodDelete
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathDownloadRemove.S
		reqParams.Body = cos.MustMarshal(dlBody)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

func (reqParams *ReqParams) doDlDownloadRequest() (string, error) {
	var resp dload.DlPostResp
	err := reqParams.DoReqResp(&resp)
	return resp.ID, err
}
