// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"net/http"
	"net/url"
	"strconv"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// Archive multiple objects from the specified source bucket.
// The option to append multiple objects to an existing archive is also supported.
// The source and the destination buckets are defined as `bckFrom` and `bckTo`, respectively
// (not necessarily distinct)
// For supported archiving formats, see `archive.FileExtensions`.
// See also: api.AppendToArch
func ArchiveMultiObj(bp BaseParams, bckFrom cmn.Bck, msg cmn.ArchiveMsg) (string, error) {
	bp.Method = http.MethodPut
	q := bckFrom.AddToQuery(nil)
	return dolr(bp, bckFrom, apc.ActArchive, msg, q)
}

// `fltPresence` applies exclusively to remote `bckFrom` (is ignored if the source is ais://)
// and is one of: { apc.FltExists, apc.FltPresent, ... } - for complete enum, see api/apc/query.go

func CopyMultiObj(bp BaseParams, bckFrom cmn.Bck, msg cmn.TCObjsMsg, fltPresence ...int) (xid string, err error) {
	bp.Method = http.MethodPost
	q := bckFrom.AddToQuery(nil)
	if len(fltPresence) > 0 {
		q.Set(apc.QparamFltPresence, strconv.Itoa(fltPresence[0]))
	}
	return dolr(bp, bckFrom, apc.ActCopyObjects, msg, q)
}

func ETLMultiObj(bp BaseParams, bckFrom cmn.Bck, msg cmn.TCObjsMsg, fltPresence ...int) (xid string, err error) {
	bp.Method = http.MethodPost
	q := bckFrom.AddToQuery(nil)
	if len(fltPresence) > 0 {
		q.Set(apc.QparamFltPresence, strconv.Itoa(fltPresence[0]))
	}
	return dolr(bp, bckFrom, apc.ActETLObjects, msg, q)
}

// DeleteList sends request to remove a list of objects from a bucket.
func DeleteList(bp BaseParams, bck cmn.Bck, filesList []string) (string, error) {
	bp.Method = http.MethodDelete
	q := bck.AddToQuery(nil)
	msg := apc.ListRange{ObjNames: filesList}
	return dolr(bp, bck, apc.ActDeleteObjects, msg, q)
}

// DeleteRange sends request to remove a range of objects from a bucket.
func DeleteRange(bp BaseParams, bck cmn.Bck, rng string) (string, error) {
	bp.Method = http.MethodDelete
	q := bck.AddToQuery(nil)
	msg := apc.ListRange{Template: rng}
	return dolr(bp, bck, apc.ActDeleteObjects, msg, q)
}

// EvictList sends request to evict a list of objects from a remote bucket.
func EvictList(bp BaseParams, bck cmn.Bck, fileslist []string) (string, error) {
	bp.Method = http.MethodDelete
	q := bck.AddToQuery(nil)
	msg := apc.ListRange{ObjNames: fileslist}
	return dolr(bp, bck, apc.ActEvictObjects, msg, q)
}

// EvictRange sends request to evict a range of objects from a remote bucket.
func EvictRange(bp BaseParams, bck cmn.Bck, rng string) (string, error) {
	bp.Method = http.MethodDelete
	q := bck.AddToQuery(nil)
	msg := apc.ListRange{Template: rng}
	return dolr(bp, bck, apc.ActEvictObjects, msg, q)
}

// PrefetchList sends request to prefetch a list of objects from a remote bucket.
func PrefetchList(bp BaseParams, bck cmn.Bck, fileslist []string) (string, error) {
	bp.Method = http.MethodPost
	q := bck.AddToQuery(nil)
	msg := apc.ListRange{ObjNames: fileslist}
	return dolr(bp, bck, apc.ActPrefetchObjects, msg, q)
}

// PrefetchRange sends request to prefetch a range of objects from a remote bucket.
func PrefetchRange(bp BaseParams, bck cmn.Bck, rng string) (string, error) {
	bp.Method = http.MethodPost
	q := bck.AddToQuery(nil)
	msg := apc.ListRange{Template: rng}
	return dolr(bp, bck, apc.ActPrefetchObjects, msg, q)
}

// multi-object list-range (delete, prefetch, evict, archive, copy, and etl)
func dolr(bp BaseParams, bck cmn.Bck, action string, msg any, q url.Values) (xid string, err error) {
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathBuckets.Join(bck.Name)
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: action, Value: msg})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}
	_, err = reqParams.doReqStr(&xid)
	FreeRp(reqParams)
	return
}
