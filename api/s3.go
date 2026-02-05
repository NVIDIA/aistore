// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
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
func GetObjectS3(bp BaseParams, bck cmn.Bck, objectName string, args ...GetArgs) (int64, error) {
	var (
		q   url.Values
		hdr http.Header
		w   = io.Discard
	)
	if len(args) != 0 {
		w, q, hdr = args[0].ret()
	}
	q = bck.AddToQuery(q)
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathS3.Join(bck.Name, objectName)
		reqParams.Query = q
		reqParams.Header = hdr
	}
	wresp, err := reqParams.doWriter(w)
	FreeRp(reqParams)
	if err != nil {
		return 0, err
	}
	return wresp.n, nil
}
