// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package api

import "github.com/NVIDIA/aistore/cmn"

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
