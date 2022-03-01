// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"io"
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
)

// s3/<bucket-name>/<object-name>
func GetObjectS3(baseParams BaseParams, bck cmn.Bck, objectName string, options ...GetObjectInput) (int64, error) {
	var (
		q   url.Values
		hdr http.Header
		w   = io.Discard
	)
	if len(options) != 0 {
		w, q, hdr = getObjectOptParams(options[0])
	}
	q = bck.AddToQuery(q)
	baseParams.Method = http.MethodGet
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathS3.Join(bck.Name, objectName)
		reqParams.Query = q
		reqParams.Header = hdr
	}
	resp, err := reqParams.doResp(w)
	freeRp(reqParams)
	if err != nil {
		return 0, err
	}
	return resp.n, nil
}
