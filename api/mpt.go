// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"net/http"
	"strconv"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

type (
	PutPartArgs struct {
		PutArgs
		UploadID   string // QparamMptUploadID
		PartNumber int    // QparamMptPartNo
	}
)

// CreateMultipartUpload creates a new multipart upload.
func CreateMultipartUpload(bp BaseParams, bck cmn.Bck, objName string) (uploadID string, err error) {
	q := qalloc()
	q = bck.AddToQuery(q)
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name, objName)
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActMptUpload})
		reqParams.Query = q
	}
	_, err = reqParams.doReqStr(&uploadID)

	FreeRp(reqParams)
	qfree(q)
	return uploadID, err
}

// UploadPart uploads a part of a multipart upload.
// - uploadID: the ID of the multipart upload to upload the part to
// - partNumber: the part number to upload
func UploadPart(args *PutPartArgs) error {
	q := qalloc()
	q.Set(apc.QparamMptUploadID, args.UploadID)
	q.Set(apc.QparamMptPartNo, strconv.Itoa(args.PartNumber))
	q = args.Bck.AddToQuery(q)

	reqArgs := cmn.AllocHra()
	{
		reqArgs.Method = http.MethodPut
		reqArgs.Base = args.BaseParams.URL
		reqArgs.Path = apc.URLPathObjects.Join(args.Bck.Name, args.ObjName)
		reqArgs.Query = q
		reqArgs.BodyR = args.Reader
		reqArgs.Header = args.Header
	}
	_, err := DoWithRetry(args.BaseParams.Client, args.put, reqArgs) //nolint:bodyclose // is closed inside
	cmn.FreeHra(reqArgs)
	qfree(q)
	return err
}

// CompleteMultipartUpload completes a multipart upload.
// - uploadID: the ID of the multipart upload to complete
// - partNumbers: the part numbers to complete
func CompleteMultipartUpload(bp BaseParams, bck cmn.Bck, objName, uploadID string, partNumbers []int) error {
	q := qalloc()
	q.Set(apc.QparamMptUploadID, uploadID)
	q = bck.AddToQuery(q)
	bp.Method = http.MethodPost

	completeMptUpload := make([]apc.MptCompletedPart, len(partNumbers))
	for i, partNumber := range partNumbers {
		completeMptUpload[i].PartNumber = partNumber
	}

	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name, objName)
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActMptComplete, Value: completeMptUpload})
		reqParams.Query = q
	}

	err := reqParams.DoRequest()
	FreeRp(reqParams)
	qfree(q)

	return err
}
