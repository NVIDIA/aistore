// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// Create native bucket inventory (NBI) for a given remote bucket; return job ID.
// - inventory name (cinvMsg.Name) is optional; when omitted, the job ID is used instead
// - inventory name: letters, numbers, dashes (-), underscores (_), and dots (.) except '..'; len <= cos.MaxNameLength
// - currently, one inventory per bucket; use cinvMsg.Force to replace existing
// - requires admin permission
func CreateNBI(bp BaseParams, bck cmn.Bck, cinvMsg *apc.CreateNBIMsg) (string, error) {
	q := qalloc()
	bck.SetQuery(q)
	bp.Method = http.MethodPost
	jbody := cos.MustMarshal(apc.ActMsg{Action: apc.ActCreateNBI, Value: cinvMsg})
	return doBckAct(bp, bck, jbody, q)
}

// Destroy bucket inventory.
// - invName: inventory to destroy; empty string destroys all inventories of the bucket
// - invName must be a valid inventory name (see CreateNBI); otherwise, return an error
// - requires admin permission
func DestroyNBI(bp BaseParams, bck cmn.Bck, invName string /*optional*/) error {
	q := qalloc()

	bp.Method = http.MethodDelete
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathBuckets.Join(bck.Name)
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActDestroyNBI, Name: invName})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		bck.SetQuery(q)
		reqParams.Query = q
	}
	err := reqParams.DoRequest()

	FreeRp(reqParams)
	qfree(q)
	return err
}

// Show bucket inventory (or inventories).
// - bck: a specific bucket, or a query (empty bucket name) to match multiple buckets by provider and namespace
// - invName: inventory to show; empty string shows all
// - requires bucket HEAD permission (specific bucket) or list-buckets permission (multiple buckets)
// - return info keyed by inventory object name (NBIInfo.ObjName); empty when nothing matches
func GetNBI(bp BaseParams, bck cmn.Bck, invName string) (apc.NBIInfoMap, error) {
	q := qalloc()
	bck.SetQuery(q)
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathBuckets.Join(bck.Name)
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActShowNBI, Name: invName})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}
	var info apc.NBIInfoMap
	_, err := reqParams.DoReqAny(&info)
	FreeRp(reqParams)
	qfree(q)
	return info, err
}
