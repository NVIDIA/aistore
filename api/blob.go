// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

func BlobDownload(bp BaseParams, bck cmn.Bck, objName string, msg *apc.BlobMsg) (xid string, err error) {
	actMsg := apc.ActMsg{Action: apc.ActBlobDl, Value: msg, Name: objName}
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name)
		reqParams.Body = cos.MustMarshal(actMsg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = bck.NewQuery()
	}
	_, err = reqParams.doReqStr(&xid)
	FreeRp(reqParams)
	return xid, err
}
