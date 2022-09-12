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
	"github.com/NVIDIA/aistore/downloader"
)

func DownloadSingle(bp BaseParams, description string,
	bck cmn.Bck, objName, link string, intervals ...time.Duration) (string, error) {
	dlBody := downloader.DlSingleBody{
		DlSingleObj: downloader.DlSingleObj{
			ObjName: objName,
			Link:    link,
		},
	}
	if len(intervals) > 0 {
		dlBody.ProgressInterval = intervals[0].String()
	}
	dlBody.Bck = bck
	dlBody.Description = description
	return DownloadWithParam(bp, downloader.DlTypeSingle, &dlBody)
}

func DownloadRange(bp BaseParams, description string, bck cmn.Bck, template string, intervals ...time.Duration) (string, error) {
	dlBody := downloader.DlRangeBody{Template: template}
	if len(intervals) > 0 {
		dlBody.ProgressInterval = intervals[0].String()
	}
	dlBody.Bck = bck
	dlBody.Description = description
	return DownloadWithParam(bp, downloader.DlTypeRange, dlBody)
}

func DownloadWithParam(bp BaseParams, dlt downloader.DlType, body interface{}) (id string, err error) {
	bp.Method = http.MethodPost
	msg := cos.MustMarshal(body)
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathDownload.S
		reqParams.Body = cos.MustMarshal(downloader.DlBody{Type: dlt, RawMessage: msg})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	id, err = reqParams.doDlDownloadRequest()
	FreeRp(reqParams)
	return
}

func DownloadMulti(bp BaseParams, description string, bck cmn.Bck, msg interface{}, intervals ...time.Duration) (string, error) {
	dlBody := downloader.DlMultiBody{}
	if len(intervals) > 0 {
		dlBody.ProgressInterval = intervals[0].String()
	}
	dlBody.Bck = bck
	dlBody.Description = description
	dlBody.ObjectsPayload = msg
	return DownloadWithParam(bp, downloader.DlTypeMulti, dlBody)
}

func DownloadBackend(bp BaseParams, description string, bck cmn.Bck, prefix, suffix string,
	intervals ...time.Duration) (string, error) {
	dlBody := downloader.DlBackendBody{Prefix: prefix, Suffix: suffix}
	if len(intervals) > 0 {
		dlBody.ProgressInterval = intervals[0].String()
	}
	dlBody.Bck = bck
	dlBody.Description = description
	return DownloadWithParam(bp, downloader.DlTypeBackend, dlBody)
}

func DownloadStatus(bp BaseParams, id string, onlyActiveTasks ...bool) (resp downloader.DlStatusResp, err error) {
	dlBody := downloader.DlAdminBody{ID: id}
	if len(onlyActiveTasks) > 0 {
		// Status of only active downloader tasks. Skip details of finished/errored tasks
		dlBody.OnlyActiveTasks = onlyActiveTasks[0]
	}
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathDownload.S
		reqParams.Body = cos.MustMarshal(dlBody)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	resp, err = reqParams.doDlStatusRequest()
	FreeRp(reqParams)
	return
}

func DownloadGetList(bp BaseParams, regex string) (dlList downloader.DlJobInfos, err error) {
	dlBody := downloader.DlAdminBody{Regex: regex}
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathDownload.S
		reqParams.Body = cos.MustMarshal(dlBody)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	err = reqParams.DoHTTPReqResp(&dlList)
	FreeRp(reqParams)
	sort.Sort(dlList)
	return
}

func AbortDownload(bp BaseParams, id string) error {
	dlBody := downloader.DlAdminBody{ID: id}
	bp.Method = http.MethodDelete
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathDownloadAbort.S
		reqParams.Body = cos.MustMarshal(dlBody)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

func RemoveDownload(bp BaseParams, id string) error {
	dlBody := downloader.DlAdminBody{ID: id}
	bp.Method = http.MethodDelete
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathDownloadRemove.S
		reqParams.Body = cos.MustMarshal(dlBody)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

func (reqParams *ReqParams) doDlDownloadRequest() (string, error) {
	var resp downloader.DlPostResp
	err := reqParams.DoHTTPReqResp(&resp)
	return resp.ID, err
}

func (reqParams *ReqParams) doDlStatusRequest() (resp downloader.DlStatusResp, err error) {
	err = reqParams.DoHTTPReqResp(&resp)
	return resp, err
}
