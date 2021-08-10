// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"fmt"
	"net/http"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// CreateArchMultiObj sends HTTP request to archive multiple objects.
// The source and the destination buckets are defined as `fromBck` and `toBck`, respectively
// (not necessarily distinct)
// For supported archiving formats, see cos.ArchExtensions.
func CreateArchMultiObj(baseParams BaseParams, fromBck cmn.Bck, msg cmn.ArchiveMsg) (string, error) {
	return doListRangeRequest(baseParams, fromBck, cmn.ActArchive, msg)
}

// DeleteList sends HTTP request to remove a list of objects from a bucket.
func DeleteList(baseParams BaseParams, bck cmn.Bck, filesList []string) (string, error) {
	deleteMsg := cmn.ListRangeMsg{ObjNames: filesList}
	return doListRangeRequest(baseParams, bck, cmn.ActDeleteObjects, deleteMsg)
}

// DeleteRange sends HTTP request to remove a range of objects from a bucket.
func DeleteRange(baseParams BaseParams, bck cmn.Bck, rng string) (string, error) {
	deleteMsg := cmn.ListRangeMsg{Template: rng}
	return doListRangeRequest(baseParams, bck, cmn.ActDeleteObjects, deleteMsg)
}

// PrefetchList sends HTTP request to prefetch a list of objects from a remote bucket.
func PrefetchList(baseParams BaseParams, bck cmn.Bck, fileslist []string) (string, error) {
	prefetchMsg := cmn.ListRangeMsg{ObjNames: fileslist}
	return doListRangeRequest(baseParams, bck, cmn.ActPrefetchObjects, prefetchMsg)
}

// PrefetchRange sends HTTP request to prefetch a range of objects from a remote bucket.
func PrefetchRange(baseParams BaseParams, bck cmn.Bck, rng string) (string, error) {
	prefetchMsg := cmn.ListRangeMsg{Template: rng}
	return doListRangeRequest(baseParams, bck, cmn.ActPrefetchObjects, prefetchMsg)
}

// EvictList sends HTTP request to evict a list of objects from a remote bucket.
func EvictList(baseParams BaseParams, bck cmn.Bck, fileslist []string) (string, error) {
	evictMsg := cmn.ListRangeMsg{ObjNames: fileslist}
	return doListRangeRequest(baseParams, bck, cmn.ActEvictObjects, evictMsg)
}

// EvictRange sends HTTP request to evict a range of objects from a remote bucket.
func EvictRange(baseParams BaseParams, bck cmn.Bck, rng string) (string, error) {
	evictMsg := cmn.ListRangeMsg{Template: rng}
	return doListRangeRequest(baseParams, bck, cmn.ActEvictObjects, evictMsg)
}

func CopyObjectsRange(baseParams BaseParams, fromBck, toBck cmn.Bck, rng string) (xactID string, err error) {
	cpyRangeMsg := cmn.TCObjsMsg{ListRangeMsg: cmn.ListRangeMsg{Template: rng}, TCBMsg: cmn.TCBMsg{}}
	return transCpyListRange(baseParams, cmn.ActCopyObjects, fromBck, toBck, cpyRangeMsg)
}

func transCpyListRange(baseParams BaseParams, kind string, fromBck, toBck cmn.Bck, msg cmn.TCObjsMsg) (xactID string, err error) {
	baseParams.Method = http.MethodPost
	q := cmn.AddBckToQuery(nil, fromBck)
	_ = cmn.AddBckUnameToQuery(q, toBck, cmn.URLParamBucketTo)
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathBuckets.Join(fromBck.Name),
		Body:       cos.MustMarshal(cmn.ActionMsg{Action: kind, Value: msg}),
		Header:     http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}},
		Query:      q,
	}, &xactID)
	return
}

// Handles operations on multiple objects (delete, prefetch, evict, archive)
func doListRangeRequest(baseParams BaseParams, bck cmn.Bck, action string, msg interface{}) (xactID string, err error) {
	switch action {
	case cmn.ActDeleteObjects, cmn.ActEvictObjects:
		baseParams.Method = http.MethodDelete
	case cmn.ActPrefetchObjects:
		baseParams.Method = http.MethodPost
	case cmn.ActArchive:
		baseParams.Method = http.MethodPut
	default:
		err = fmt.Errorf("invalid action %q", action)
		return
	}
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathBuckets.Join(bck.Name),
		Body:       cos.MustMarshal(cmn.ActionMsg{Action: action, Value: msg}),
		Header:     http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}},
		Query:      cmn.AddBckToQuery(nil, bck),
	}, &xactID)
	return
}
