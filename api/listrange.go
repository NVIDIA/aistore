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

// ArchiveMsg sends HTTP request to archive source objects as a tar (tgz/tar.gz, zip)
// at the destination (for supported archiving formats, see cos.ArchExtensions)
func ArchiveList(baseParams BaseParams, fromBck, toBck cmn.Bck, archName string, filesList []string) (string, error) {
	archiveMsg := cmn.ArchiveMsg{
		ListMsg:  cmn.ListMsg{ObjNames: filesList},
		ToBck:    toBck,
		ArchName: archName,
	}
	return doListRangeRequest(baseParams, fromBck, cmn.ActArchive, archiveMsg)
}

func ArchiveRange(baseParams BaseParams, fromBck, toBck cmn.Bck, archName, rng string) (string, error) {
	archiveMsg := cmn.ArchiveMsg{
		RangeMsg: cmn.RangeMsg{Template: rng},
		ToBck:    toBck,
		ArchName: archName,
	}
	return doListRangeRequest(baseParams, fromBck, cmn.ActArchive, archiveMsg)
}

// DeleteList sends HTTP request to remove a list of objects from a bucket.
func DeleteList(baseParams BaseParams, bck cmn.Bck, filesList []string) (string, error) {
	deleteMsg := cmn.ListMsg{ObjNames: filesList}
	return doListRangeRequest(baseParams, bck, cmn.ActDelete, deleteMsg)
}

// DeleteRange sends HTTP request to remove a range of objects from a bucket.
func DeleteRange(baseParams BaseParams, bck cmn.Bck, rng string) (string, error) {
	deleteMsg := cmn.RangeMsg{Template: rng}
	return doListRangeRequest(baseParams, bck, cmn.ActDelete, deleteMsg)
}

// PrefetchList sends HTTP request to prefetch a list of objects from a remote bucket.
func PrefetchList(baseParams BaseParams, bck cmn.Bck, fileslist []string) (string, error) {
	prefetchMsg := cmn.ListMsg{ObjNames: fileslist}
	return doListRangeRequest(baseParams, bck, cmn.ActPrefetch, prefetchMsg)
}

// PrefetchRange sends HTTP request to prefetch a range of objects from a remote bucket.
func PrefetchRange(baseParams BaseParams, bck cmn.Bck, rng string) (string, error) {
	prefetchMsg := cmn.RangeMsg{Template: rng}
	return doListRangeRequest(baseParams, bck, cmn.ActPrefetch, prefetchMsg)
}

// EvictList sends HTTP request to evict a list of objects from a remote bucket.
func EvictList(baseParams BaseParams, bck cmn.Bck, fileslist []string) (string, error) {
	evictMsg := cmn.ListMsg{ObjNames: fileslist}
	return doListRangeRequest(baseParams, bck, cmn.ActEvictObjects, evictMsg)
}

// EvictRange sends HTTP request to evict a range of objects from a remote bucket.
func EvictRange(baseParams BaseParams, bck cmn.Bck, rng string) (string, error) {
	evictMsg := cmn.RangeMsg{Template: rng}
	return doListRangeRequest(baseParams, bck, cmn.ActEvictObjects, evictMsg)
}

// Handles operations on multiple objects (delete, prefetch, evict, archive)
func doListRangeRequest(baseParams BaseParams, bck cmn.Bck, action string, msg interface{}) (xactID string, err error) {
	switch action {
	case cmn.ActDelete, cmn.ActEvictObjects:
		baseParams.Method = http.MethodDelete
	case cmn.ActPrefetch:
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
