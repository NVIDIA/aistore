// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

func BlobDownload(bp BaseParams, bck cmn.Bck, objName string, msg *apc.BlobMsg) (xid string, err error) {
	var (
		actMsg = apc.ActMsg{Action: apc.ActBlobDl, Value: msg, Name: objName}
		q      = qalloc()
	)
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name)
		reqParams.Body = cos.MustMarshal(actMsg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		bck.SetQuery(q)
		reqParams.Query = q
	}
	_, err = reqParams.doReqStr(&xid)

	FreeRp(reqParams)
	qfree(q)
	return xid, err
}
