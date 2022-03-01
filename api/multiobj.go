// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
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
func CreateArchMultiObj(baseParams BaseParams, fromBck cmn.Bck, msg cmn.ArchiveMsg) (string, error) {
	return doListRangeRequest(baseParams, fromBck, apc.ActArchive, msg)
}

func CopyMultiObj(baseParams BaseParams, fromBck cmn.Bck, msg cmn.TCObjsMsg) (xactID string, err error) {
	return doListRangeRequest(baseParams, fromBck, apc.ActCopyObjects, msg)
}

func ETLMultiObj(baseParams BaseParams, fromBck cmn.Bck, msg cmn.TCObjsMsg) (xactID string, err error) {
	return doListRangeRequest(baseParams, fromBck, apc.ActETLObjects, msg)
}

// DeleteList sends request to remove a list of objects from a bucket.
func DeleteList(baseParams BaseParams, bck cmn.Bck, filesList []string) (string, error) {
	deleteMsg := cmn.ListRangeMsg{ObjNames: filesList}
	return doListRangeRequest(baseParams, bck, apc.ActDeleteObjects, deleteMsg)
}

// DeleteRange sends request to remove a range of objects from a bucket.
func DeleteRange(baseParams BaseParams, bck cmn.Bck, rng string) (string, error) {
	deleteMsg := cmn.ListRangeMsg{Template: rng}
	return doListRangeRequest(baseParams, bck, apc.ActDeleteObjects, deleteMsg)
}

// PrefetchList sends request to prefetch a list of objects from a remote bucket.
func PrefetchList(baseParams BaseParams, bck cmn.Bck, fileslist []string) (string, error) {
	prefetchMsg := cmn.ListRangeMsg{ObjNames: fileslist}
	return doListRangeRequest(baseParams, bck, apc.ActPrefetchObjects, prefetchMsg)
}

// PrefetchRange sends request to prefetch a range of objects from a remote bucket.
func PrefetchRange(baseParams BaseParams, bck cmn.Bck, rng string) (string, error) {
	prefetchMsg := cmn.ListRangeMsg{Template: rng}
	return doListRangeRequest(baseParams, bck, apc.ActPrefetchObjects, prefetchMsg)
}

// EvictList sends request to evict a list of objects from a remote bucket.
func EvictList(baseParams BaseParams, bck cmn.Bck, fileslist []string) (string, error) {
	evictMsg := cmn.ListRangeMsg{ObjNames: fileslist}
	return doListRangeRequest(baseParams, bck, apc.ActEvictObjects, evictMsg)
}

// EvictRange sends request to evict a range of objects from a remote bucket.
func EvictRange(baseParams BaseParams, bck cmn.Bck, rng string) (string, error) {
	evictMsg := cmn.ListRangeMsg{Template: rng}
	return doListRangeRequest(baseParams, bck, apc.ActEvictObjects, evictMsg)
}

// Handles multi-object (delete, prefetch, evict) operations
// as well as (archive, copy and ETL) transactions
func doListRangeRequest(baseParams BaseParams, bck cmn.Bck, action string, msg interface{}) (xactID string, err error) {
	q := bck.AddToQuery(nil)
	switch action {
	case apc.ActDeleteObjects, apc.ActEvictObjects:
		baseParams.Method = http.MethodDelete
	case apc.ActPrefetchObjects, apc.ActCopyObjects, apc.ActETLObjects:
		baseParams.Method = http.MethodPost
	case apc.ActArchive:
		baseParams.Method = http.MethodPut
	default:
		err = fmt.Errorf("invalid action %q", action)
		return
	}
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathBuckets.Join(bck.Name)
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: action, Value: msg})
		reqParams.Header = http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}}
		reqParams.Query = q
	}
	err = reqParams.DoHTTPReqResp(&xactID)
	freeRp(reqParams)
	return
}
