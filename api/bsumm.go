// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/xact"
	jsoniter "github.com/json-iterator/go"
)

const fmtErrStatus = "expecting status (ok, partial, accepted), or error - got %d"

type (
	BsummCB func(*cmn.AllBsummResults, bool)

	BsummArgs struct {
		Callback  BsummCB
		CallAfter time.Duration
		DontWait  bool
	}
	BinfoArgs struct {
		BsummArgs
		UUID          string
		Prefix        string
		FltPresence   int
		Summarize     bool
		WithRemote    bool
		DontAddRemote bool
	}
)

// Bucket information - a runtime addendum to `BucketProps`.
// In addition to `cmn.Bprops` properties (which are user configurable), bucket runtime info:
// - includes usage, capacity, other statistics
// - is obtained via GetBucketInfo() API
// - and delivered via apc.HdrBucketInfo header (compare with GetBucketSummary)
// The API uses http.MethodHead and can be considered an extension of HeadBucket (above)
func GetBucketInfo(bp BaseParams, bck cmn.Bck, args *BinfoArgs) (string, *cmn.Bprops, *cmn.BsummResult, error) {
	q := make(url.Values, 4)
	q = bck.AddToQuery(q)
	q.Set(apc.QparamFltPresence, strconv.Itoa(args.FltPresence))
	if args.DontAddRemote {
		q.Set(apc.QparamDontAddRemote, "true")
	}
	if args.Summarize {
		if args.WithRemote {
			q.Set(apc.QparamBinfoWithOrWithoutRemote, "true")
		} else {
			q.Set(apc.QparamBinfoWithOrWithoutRemote, "false")
		}
	}
	bp.Method = http.MethodHead
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		if args.Prefix != "" {
			reqParams.Path = apc.URLPathBuckets.Join(bck.Name, args.Prefix)
		} else {
			reqParams.Path = apc.URLPathBuckets.Join(bck.Name)
		}
		reqParams.Query = q
	}
	xid, p, info, err := _binfo(reqParams, bck, args)
	FreeRp(reqParams)
	return xid, p, info, err
}

// compare w/ _bsumm
// TODO: _binfoDontWait w/ ref
func _binfo(reqParams *ReqParams, bck cmn.Bck, args *BinfoArgs) (xid string, p *cmn.Bprops, info *cmn.BsummResult, err error) {
	var (
		hdr          http.Header
		status       int
		start, after int64
		sleep        = xact.MinPollTime
		news         = args.UUID == ""
	)
	if !news {
		reqParams.Query.Set(apc.QparamUUID, xid)
	}
	if hdr, status, err = reqParams.doReqHdr(); err != nil {
		err = hdr2msg(bck, status, err)
		return
	}

	hdrProps := hdr.Get(apc.HdrBucketProps)
	if hdrProps != "" {
		p = &cmn.Bprops{}
		if err = jsoniter.Unmarshal([]byte(hdrProps), p); err != nil {
			return
		}
	}
	xid = hdr.Get(apc.HdrXactionID)
	if xid == "" {
		debug.Assert(status == http.StatusOK && !args.Summarize, status, " ", args.Summarize)
		return
	}
	debug.Assert(news || xid == args.UUID)
	if args.DontWait {
		if (status == http.StatusOK || status == http.StatusPartialContent) && args.Summarize {
			if hdrSumm := hdr.Get(apc.HdrBucketSumm); hdrSumm != "" {
				info = &cmn.BsummResult{}
				err = jsoniter.Unmarshal([]byte(hdrSumm), info)
			}
		}
		return
	}
	if status != http.StatusAccepted {
		err = _invalidStatus(status)
		return
	}
	if args.Callback != nil {
		start = mono.NanoTime()
		after = start + args.CallAfter.Nanoseconds()
	}

	if news {
		reqParams.Query.Set(apc.QparamUUID, xid)
	}
	time.Sleep(sleep / 2)
	for i := 0; ; i++ {
		if hdr, status, err = reqParams.doReqHdr(); err != nil {
			err = hdr2msg(bck, status, err)
			return
		}

		hdrSumm := hdr.Get(apc.HdrBucketSumm)
		if hdrSumm != "" {
			info = &cmn.BsummResult{}
			err = jsoniter.Unmarshal([]byte(hdrSumm), info)
		}
		if err != nil {
			return // unlikely
		}
		debug.Assertf(hdr.Get(apc.HdrXactionID) == xid, "%q vs %q", hdr.Get(apc.HdrXactionID), xid)

		// callback w/ partial results
		if args.Callback != nil && (status == http.StatusPartialContent || status == http.StatusOK) {
			if after == start || mono.NanoTime() >= after {
				res := cmn.AllBsummResults{info}
				args.Callback(&res, status == http.StatusOK)
			}
		}
		if status == http.StatusOK {
			return
		}
		if status != http.StatusPartialContent && status != http.StatusAccepted {
			err = _invalidStatus(status)
			return
		}

		time.Sleep(sleep)
		// inc. sleep time if there's nothing at all
		if i == 8 && status != http.StatusPartialContent {
			sleep *= 2
		} else if i == 16 && status != http.StatusPartialContent {
			sleep *= 2
		}
	}
}

// GetBucketSummary returns bucket capacity ulitization percentages, sizes (total and min/max/average),
// and the numbers of objects, both _in_ the cluster and remote
// GetBucketSummary supports a single specified bucket or multiple buckets, as per `cmn.QueryBcks` query.
// (e.g., GetBucketSummary with an empty bucket query will return "summary" info for all buckets)
func GetBucketSummary(bp BaseParams, qbck cmn.QueryBcks, msg *apc.BsummCtrlMsg, args BsummArgs) (xid string,
	res cmn.AllBsummResults, err error) {
	if msg == nil {
		msg = &apc.BsummCtrlMsg{ObjCached: true, BckPresent: true}
	}
	bp.Method = http.MethodGet

	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathBuckets.Join(qbck.Name)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = qbck.NewQuery()
	}
	if args.DontWait {
		debug.Assert(args.Callback == nil)
		xid, err = _bsummDontWait(reqParams, msg, &res)
	} else {
		xid, err = _bsumm(reqParams, msg, &res, args)
	}
	if err == nil {
		sort.Sort(res)
	}
	FreeRp(reqParams)
	return
}

// Wait/poll bucket-summary:
// - initiate `apc.ActSummaryBck` (msg.UUID == "").
// - poll for status != ok
// - handle status: ok, accepted, partial-content
// See also:
// - _binfo
// - _bsummDontWait
func _bsumm(reqParams *ReqParams, msg *apc.BsummCtrlMsg, res *cmn.AllBsummResults, args BsummArgs) (xid string, _ error) {
	var (
		start, after int64
		sleep        = xact.MinPollTime
		actMsg       = apc.ActMsg{Action: apc.ActSummaryBck, Value: msg}
	)
	debug.Assert(msg.UUID == "")
	if args.Callback != nil {
		start = mono.NanoTime()
		after = start + args.CallAfter.Nanoseconds()
	}
	reqParams.Body = cos.MustMarshal(actMsg)
	status, err := reqParams.doReqStr(&xid)
	if err != nil {
		return xid, err
	}
	if status != http.StatusAccepted {
		return xid, _invalidStatus(status)
	}
	if msg.UUID == "" {
		msg.UUID = xid
		reqParams.Body = cos.MustMarshal(actMsg)
	}

	time.Sleep(sleep / 2)
	for i := 0; ; i++ {
		status, err = reqParams.DoReqAny(res)
		if err != nil {
			return xid, err
		}
		// callback w/ partial results
		if args.Callback != nil && (status == http.StatusPartialContent || status == http.StatusOK) {
			if after == start || mono.NanoTime() >= after {
				args.Callback(res, status == http.StatusOK)
			}
		}
		if status == http.StatusOK {
			return xid, nil
		}
		if status != http.StatusPartialContent && status != http.StatusAccepted {
			return xid, _invalidStatus(status)
		}

		time.Sleep(sleep)
		// inc. sleep time if there's nothing at all
		if i == 8 && status != http.StatusPartialContent {
			sleep *= 2
		} else if i == 16 && status != http.StatusPartialContent {
			sleep *= 2
		}
	}
}

func _bsummDontWait(reqParams *ReqParams, msg *apc.BsummCtrlMsg, res *cmn.AllBsummResults) (xid string, err error) {
	var (
		actMsg = apc.ActMsg{Action: apc.ActSummaryBck, Value: msg}
		status int
		news   = msg.UUID == ""
	)
	reqParams.Body = cos.MustMarshal(actMsg)
	if news {
		status, err = reqParams.doReqStr(&xid)
		if err == nil {
			if status != http.StatusAccepted {
				err = _invalidStatus(status)
			}
		}
		return
	}

	status, err = reqParams.DoReqAny(res)
	if err != nil {
		return xid, err
	}
	switch status {
	case http.StatusOK:
	case http.StatusPartialContent, http.StatusAccepted:
		err = &cmn.ErrHTTP{Message: http.StatusText(status), Status: status} // indicate
	default:
		err = _invalidStatus(status)
	}
	return
}

func _invalidStatus(status int) error {
	return &cmn.ErrHTTP{
		Message: fmt.Sprintf(fmtErrStatus, status),
		Status:  status,
	}
}
