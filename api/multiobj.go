// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"fmt"
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// CreateArchMultiObj allows to archive multiple objects.
// The option to append multiple objects to an existing archive is also supported.
// The source and the destination buckets are defined as `fromBck` and `toBck`, respectively
// (not necessarily distinct)
// For supported archiving formats, see `cos.ArchExtensions`.
// NOTE: compare with `api.AppendToArch`
func CreateArchMultiObj(bp BaseParams, fromBck cmn.Bck, msg cmn.ArchiveMsg) (string, error) {
	return doListRangeRequest(bp, fromBck, apc.ActArchive, msg)
}

func CopyMultiObj(bp BaseParams, fromBck cmn.Bck, msg cmn.TCObjsMsg) (xactID string, err error) {
	return doListRangeRequest(bp, fromBck, apc.ActCopyObjects, msg)
}

func ETLMultiObj(bp BaseParams, fromBck cmn.Bck, msg cmn.TCObjsMsg) (xactID string, err error) {
	return doListRangeRequest(bp, fromBck, apc.ActETLObjects, msg)
}

// DeleteList sends request to remove a list of objects from a bucket.
func DeleteList(bp BaseParams, bck cmn.Bck, filesList []string) (string, error) {
	deleteMsg := cmn.SelectObjsMsg{ObjNames: filesList}
	return doListRangeRequest(bp, bck, apc.ActDeleteObjects, deleteMsg)
}

// DeleteRange sends request to remove a range of objects from a bucket.
func DeleteRange(bp BaseParams, bck cmn.Bck, rng string) (string, error) {
	deleteMsg := cmn.SelectObjsMsg{Template: rng}
	return doListRangeRequest(bp, bck, apc.ActDeleteObjects, deleteMsg)
}

// PrefetchList sends request to prefetch a list of objects from a remote bucket.
func PrefetchList(bp BaseParams, bck cmn.Bck, fileslist []string) (string, error) {
	prefetchMsg := cmn.SelectObjsMsg{ObjNames: fileslist}
	return doListRangeRequest(bp, bck, apc.ActPrefetchObjects, prefetchMsg)
}

// PrefetchRange sends request to prefetch a range of objects from a remote bucket.
func PrefetchRange(bp BaseParams, bck cmn.Bck, rng string) (string, error) {
	prefetchMsg := cmn.SelectObjsMsg{Template: rng}
	return doListRangeRequest(bp, bck, apc.ActPrefetchObjects, prefetchMsg)
}

// EvictList sends request to evict a list of objects from a remote bucket.
func EvictList(bp BaseParams, bck cmn.Bck, fileslist []string) (string, error) {
	evictMsg := cmn.SelectObjsMsg{ObjNames: fileslist}
	return doListRangeRequest(bp, bck, apc.ActEvictObjects, evictMsg)
}

// EvictRange sends request to evict a range of objects from a remote bucket.
func EvictRange(bp BaseParams, bck cmn.Bck, rng string) (string, error) {
	evictMsg := cmn.SelectObjsMsg{Template: rng}
	return doListRangeRequest(bp, bck, apc.ActEvictObjects, evictMsg)
}

// Handles multi-object (delete, prefetch, evict) operations
// as well as (archive, copy and ETL) transactions
func doListRangeRequest(bp BaseParams, bck cmn.Bck, action string, msg any) (xactID string, err error) {
	q := bck.AddToQuery(nil)
	switch action {
	case apc.ActDeleteObjects, apc.ActEvictObjects:
		bp.Method = http.MethodDelete
	case apc.ActPrefetchObjects, apc.ActCopyObjects, apc.ActETLObjects:
		bp.Method = http.MethodPost
	case apc.ActArchive:
		bp.Method = http.MethodPut
	default:
		err = fmt.Errorf("invalid action %q", action)
		return
	}
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathBuckets.Join(bck.Name)
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: action, Value: msg})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}
	err = reqParams.DoReqResp(&xactID)
	FreeRp(reqParams)
	return
}
