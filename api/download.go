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
	"github.com/NVIDIA/aistore/dloader"
)

func DownloadSingle(bp BaseParams, description string,
	bck cmn.Bck, objName, link string, intervals ...time.Duration) (string, error) {
	dlBody := dloader.SingleBody{
		SingleObj: dloader.SingleObj{
			ObjName: objName,
			Link:    link,
		},
	}
	if len(intervals) > 0 {
		dlBody.ProgressInterval = intervals[0].String()
	}
	dlBody.Bck = bck
	dlBody.Description = description
	return DownloadWithParam(bp, dloader.TypeSingle, &dlBody)
}

func DownloadRange(bp BaseParams, description string, bck cmn.Bck, template string, intervals ...time.Duration) (string, error) {
	dlBody := dloader.RangeBody{Template: template}
	if len(intervals) > 0 {
		dlBody.ProgressInterval = intervals[0].String()
	}
	dlBody.Bck = bck
	dlBody.Description = description
	return DownloadWithParam(bp, dloader.TypeRange, dlBody)
}

func DownloadWithParam(bp BaseParams, dlt dloader.Type, body any) (id string, err error) {
	bp.Method = http.MethodPost
	msg := cos.MustMarshal(body)
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathDownload.S
		reqParams.Body = cos.MustMarshal(dloader.Body{Type: dlt, RawMessage: msg})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	id, err = reqParams.doDlDownloadRequest()
	FreeRp(reqParams)
	return
}

func DownloadMulti(bp BaseParams, description string, bck cmn.Bck, msg any, intervals ...time.Duration) (string, error) {
	dlBody := dloader.MultiBody{}
	if len(intervals) > 0 {
		dlBody.ProgressInterval = intervals[0].String()
	}
	dlBody.Bck = bck
	dlBody.Description = description
	dlBody.ObjectsPayload = msg
	return DownloadWithParam(bp, dloader.TypeMulti, dlBody)
}

func DownloadBackend(bp BaseParams, description string, bck cmn.Bck, prefix, suffix string,
	intervals ...time.Duration) (string, error) {
	dlBody := dloader.BackendBody{Prefix: prefix, Suffix: suffix}
	if len(intervals) > 0 {
		dlBody.ProgressInterval = intervals[0].String()
	}
	dlBody.Bck = bck
	dlBody.Description = description
	return DownloadWithParam(bp, dloader.TypeBackend, dlBody)
}

func DownloadStatus(bp BaseParams, id string, onlyActive ...bool) (resp *dloader.StatusResp, err error) {
	dlBody := dloader.AdminBody{ID: id}
	if len(onlyActive) > 0 {
		// only active downloaders - skip finished and aborted, omit errors
		dlBody.OnlyActive = onlyActive[0]
	}
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathDownload.S
		reqParams.Body = cos.MustMarshal(dlBody)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	resp = &dloader.StatusResp{}
	err = reqParams.DoReqResp(resp)
	FreeRp(reqParams)
	return
}

func DownloadGetList(bp BaseParams, regex string) (dlList dloader.JobInfos, err error) {
	dlBody := dloader.AdminBody{Regex: regex}
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
	dlBody := dloader.AdminBody{ID: id}
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
	dlBody := dloader.AdminBody{ID: id}
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
	var resp dloader.DlPostResp
	err := reqParams.DoReqResp(&resp)
	return resp.ID, err
}
