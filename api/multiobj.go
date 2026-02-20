// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"net/http"
	"strconv"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

//
// APIs to execute multi-object xactions (jobs)
// Background: all multi-object xactions have `apc.ListRange` in their respective control messages
// See also: api/xaction.go that includes common APIs to start and stop any xaction.
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
	jbody := cos.MustMarshal(apc.ActMsg{Action: apc.ActArchive, Value: msg})
	return doBckAct(bp, bckFrom, jbody, q)
}

// `fltPresence` applies exclusively to remote `bckFrom` (is ignored if the source is ais://)
// and is one of: { apc.FltExists, apc.FltPresent, ... } - for complete enum, see api/apc/query.go

func CopyMultiObj(bp BaseParams, bckFrom cmn.Bck, msg *cmn.TCOMsg, fltPresence ...int) (string, error) {
	bp.Method = http.MethodPost
	q := qalloc()
	bckFrom.SetQuery(q)
	if len(fltPresence) > 0 {
		q.Set(apc.QparamFltPresence, strconv.Itoa(fltPresence[0]))
	}
	jbody := cos.MustMarshal(apc.ActMsg{Action: apc.ActCopyObjects, Value: msg})
	return doBckAct(bp, bckFrom, jbody, q)
}

func ETLMultiObj(bp BaseParams, bckFrom cmn.Bck, msg *cmn.TCOMsg, fltPresence ...int) (string, error) {
	bp.Method = http.MethodPost
	q := qalloc()
	bckFrom.SetQuery(q)
	if len(fltPresence) > 0 {
		q.Set(apc.QparamFltPresence, strconv.Itoa(fltPresence[0]))
	}
	jbody := cos.MustMarshal(apc.ActMsg{Action: apc.ActETLObjects, Value: msg})
	return doBckAct(bp, bckFrom, jbody, q)
}

func DeleteMultiObj(bp BaseParams, bck cmn.Bck, msg *apc.EvdMsg) (string, error) {
	bp.Method = http.MethodDelete
	q := qalloc()
	bck.SetQuery(q)
	jbody := cos.MustMarshal(apc.ActMsg{Action: apc.ActDeleteObjects, Value: msg})
	return doBckAct(bp, bck, jbody, q)
}

func EvictMultiObj(bp BaseParams, bck cmn.Bck, msg *apc.EvdMsg) (string, error) {
	bp.Method = http.MethodDelete
	q := qalloc()
	bck.SetQuery(q)
	jbody := cos.MustMarshal(apc.ActMsg{Action: apc.ActEvictObjects, Value: msg})
	return doBckAct(bp, bck, jbody, q)
}

func Prefetch(bp BaseParams, bck cmn.Bck, msg *apc.PrefetchMsg) (string, error) {
	bp.Method = http.MethodPost
	q := qalloc()
	bck.SetQuery(q)
	jbody := cos.MustMarshal(apc.ActMsg{Action: apc.ActPrefetchObjects, Value: msg})
	return doBckAct(bp, bck, jbody, q)
}
