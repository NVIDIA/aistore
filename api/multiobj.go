// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
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

//
// In this file: APIs to start multi-object xactions
// See also: xaction.go (generic part)
//

// Archive multiple objects from the specified source bucket.
// The option to append multiple objects to an existing archive is also supported.
// The source and the destination buckets are defined as `bckFrom` and `bckTo`, respectively
// (not necessarily distinct)
// For supported archiving formats, see `archive.FileExtensions`.
// See also: api.PutApndArch
func ArchiveMultiObj(bp BaseParams, bckFrom cmn.Bck, msg *cmn.ArchiveBckMsg) (string, error) {
	bp.Method = http.MethodPut
	q := qalloc()
	bckFrom.SetQuery(q)
	return dolr(bp, bckFrom, apc.ActArchive, msg, q)
}

// `fltPresence` applies exclusively to remote `bckFrom` (is ignored if the source is ais://)
// and is one of: { apc.FltExists, apc.FltPresent, ... } - for complete enum, see api/apc/query.go

func CopyMultiObj(bp BaseParams, bckFrom cmn.Bck, msg *cmn.TCOMsg, fltPresence ...int) (xid string, err error) {
	bp.Method = http.MethodPost
	q := qalloc()
	bckFrom.SetQuery(q)
	if len(fltPresence) > 0 {
		q.Set(apc.QparamFltPresence, strconv.Itoa(fltPresence[0]))
	}
	return dolr(bp, bckFrom, apc.ActCopyObjects, msg, q)
}

func ETLMultiObj(bp BaseParams, bckFrom cmn.Bck, msg *cmn.TCOMsg, fltPresence ...int) (xid string, err error) {
	bp.Method = http.MethodPost
	q := qalloc()
	bckFrom.SetQuery(q)
	if len(fltPresence) > 0 {
		q.Set(apc.QparamFltPresence, strconv.Itoa(fltPresence[0]))
	}
	return dolr(bp, bckFrom, apc.ActETLObjects, msg, q)
}

func DeleteMultiObj(bp BaseParams, bck cmn.Bck, objNames []string, template string) (string, error) {
	bp.Method = http.MethodDelete
	q := qalloc()
	bck.SetQuery(q)
	msg := apc.ListRange{ObjNames: objNames, Template: template}
	return dolr(bp, bck, apc.ActDeleteObjects, msg, q)
}

func EvictMultiObj(bp BaseParams, bck cmn.Bck, objNames []string, template string) (string, error) {
	bp.Method = http.MethodDelete
	q := qalloc()
	bck.SetQuery(q)
	msg := apc.ListRange{ObjNames: objNames, Template: template}
	return dolr(bp, bck, apc.ActEvictObjects, msg, q)
}

func Prefetch(bp BaseParams, bck cmn.Bck, msg apc.PrefetchMsg) (string, error) {
	bp.Method = http.MethodPost
	q := qalloc()
	bck.SetQuery(q)
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
	qfree(q)
	return xid, err
}
