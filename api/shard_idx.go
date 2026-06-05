// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"fmt"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

type (
	ShardSummCB func(*apc.ShardSummResult, bool)

	ShardSummArgs struct {
		Callback  ShardSummCB
		CallAfter time.Duration
		DontWait  bool
	}
)

// GetBucketShardSummary summarizes local TAR objects and their shard-index coverage.
func GetBucketShardSummary(bp BaseParams, bck cmn.Bck, msg *apc.ShardSummMsg, args ShardSummArgs) (xid string,
	res *apc.ShardSummResult, err error) {
	if msg == nil {
		msg = &apc.ShardSummMsg{}
	}
	if args.DontWait && args.Callback != nil {
		return "", nil, fmt.Errorf("%s: callback cannot be used with dont-wait", apc.ActSummaryShard)
	}
	if !args.DontWait && msg.UUID != "" {
		return "", nil, fmt.Errorf("%s: UUID %q requires dont-wait", apc.ActSummaryShard, msg.UUID)
	}
	res = &apc.ShardSummResult{}

	q := qalloc()
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathBuckets.Join(bck.Name)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		bck.SetQuery(q)
		reqParams.Query = q
	}
	if args.DontWait {
		xid, err = _shardSummDontWait(reqParams, msg, res)
	} else {
		xid, err = _shardSumm(reqParams, msg, res, args)
	}
	FreeRp(reqParams)
	qfree(q)
	return xid, res, err
}

func _shardSumm(reqParams *ReqParams, msg *apc.ShardSummMsg, res *apc.ShardSummResult, args ShardSummArgs) (xid string, _ error) {
	actMsg := apc.ActMsg{Action: apc.ActSummaryShard, Value: msg}
	xid, err := _summStart(reqParams, actMsg)
	if err != nil {
		return xid, err
	}
	msg.UUID = xid
	reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActSummaryShard, Value: msg})
	return xid, _summPoll(reqParams, res, args.CallAfter, args.Callback)
}

func _shardSummDontWait(reqParams *ReqParams, msg *apc.ShardSummMsg, res *apc.ShardSummResult) (xid string, err error) {
	return _summDontWait(reqParams, apc.ActMsg{Action: apc.ActSummaryShard, Value: msg}, msg.UUID, res)
}
