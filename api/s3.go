// Package api provides RESTful API to AIS object storage
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/ais/s3compat"
	"github.com/NVIDIA/aistore/cmn"
)

// s3/bckName/objName[!tf]
func GetObjectS3(baseParams BaseParams, bck cmn.Bck, objectName string, options ...GetObjectInput) (n int64, err error) {
	var (
		w = ioutil.Discard
		q url.Values
	)
	if len(options) != 0 {
		w, q = getObjectOptParams(options[0])
	}
	q = cmn.AddBckToQuery(q, bck)
	baseParams.Method = http.MethodGet
	resp, err := doHTTPRequestGetResp(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(s3compat.Root, bck.Name, objectName),
		Query:      q,
	}, w)
	if err != nil {
		return 0, err
	}
	return resp.n, nil
}
