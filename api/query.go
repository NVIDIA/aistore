// Package api provides RESTful API to AIS object storage
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"net/http"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/query"
)

func InitQuery(baseParams BaseParams, objectsTemplate string, bck cmn.Bck, filter *query.FilterMsg) (string, error) {
	baseParams.Method = http.MethodPost
	outerSelectMsg := query.OuterSelectMsg{Template: objectsTemplate}
	fromMsg := query.FromMsg{Bck: bck}
	qMsg := query.DefMsg{
		OuterSelect: outerSelectMsg,
		From:        fromMsg,
		Where:       query.WhereMsg{Filter: filter},
	}
	initMsg := query.InitMsg{QueryMsg: qMsg}

	var handle string
	err := DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Query, cmn.Init),
		Body:       cmn.MustMarshal(initMsg),
	}, &handle)
	return handle, err
}

func NextQueryResults(baseParams BaseParams, handle string, size uint) ([]*cmn.BucketEntry, error) {
	var objectsNames []*cmn.BucketEntry
	baseParams.Method = http.MethodGet
	err := DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Query, cmn.Next),
		Body:       cmn.MustMarshal(query.NextMsg{Handle: handle, Size: size}),
	}, &objectsNames)

	return objectsNames, err
}
