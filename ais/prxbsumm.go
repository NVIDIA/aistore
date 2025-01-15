// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/xact"
)

// in this source:
// - bsummact  <= api.GetBucketSummary(query-bcks, ActMsg)
// - bsummhead <= api.GetBucketInfo(bck, QparamBinfoWithOrWithoutRemote)

func (p *proxy) bsummact(w http.ResponseWriter, r *http.Request, qbck *cmn.QueryBcks, msg *apc.BsummCtrlMsg) {
	news := msg.UUID == ""
	debug.Assert(msg.UUID == "" || cos.IsValidUUID(msg.UUID), msg.UUID)

	// start new
	if news {
		err := p.bsummNew(qbck, msg)
		if err != nil {
			p.writeErr(w, r, err)
		} else {
			w.WriteHeader(http.StatusAccepted)
			writeXid(w, msg.UUID)
		}
		return
	}

	// or, query partial or final results
	summaries, status, err := p.bsummCollect(qbck, msg)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	w.WriteHeader(status)
	p.writeJSON(w, r, summaries, "bucket-summary")
}

func (p *proxy) bsummNew(qbck *cmn.QueryBcks, msg *apc.BsummCtrlMsg) (err error) {
	q := qbck.NewQuery()

	msg.UUID = cos.GenUUID()
	actMsgExt := p.newAmsgActVal(apc.ActSummaryBck, msg)

	args := allocBcArgs()
	args.req = cmn.HreqArgs{
		Method: http.MethodGet,
		Path:   apc.URLPathBuckets.Join(qbck.Name, apc.ActBegin), // compare w/ txn
		Query:  q,
		Body:   cos.MustMarshal(actMsgExt),
	}
	// not using default control-plane timeout -
	// returning only _after_ all targets start running this new job
	// (see Run() in nsumm.go)
	args.timeout = apc.DefaultTimeout

	args.smap = p.owner.smap.get()
	if cnt := args.smap.CountActiveTs(); cnt < 1 {
		return cmn.NewErrNoNodes(apc.Target, args.smap.CountTargets())
	}
	results := p.bcastGroup(args)
	for _, res := range results {
		if res.err != nil {
			if res.details == "" || res.details == dfltDetail {
				res.details = xact.Cname(apc.ActSummaryBck, msg.UUID)
			}
			err = res.toErr()
			break
		}
	}
	freeBcastRes(results)
	return err
}

func (p *proxy) bsummCollect(qbck *cmn.QueryBcks, msg *apc.BsummCtrlMsg) (_ cmn.AllBsummResults, status int, err error) {
	var (
		q         = make(url.Values, 4)
		actMsgExt = p.newAmsgActVal(apc.ActSummaryBck, msg)
		args      = allocBcArgs()
	)
	args.req = cmn.HreqArgs{
		Method: http.MethodGet,
		Path:   apc.URLPathBuckets.Join(qbck.Name, apc.ActQuery),
		Body:   cos.MustMarshal(actMsgExt),
	}
	args.smap = p.owner.smap.get()
	if cnt := args.smap.CountActiveTs(); cnt < 1 {
		return nil, 0, cmn.NewErrNoNodes(apc.Target, args.smap.CountTargets())
	}
	qbck.AddToQuery(q)
	q.Set(apc.QparamSilent, "true")
	args.req.Query = q
	args.cresv = cresjGeneric[cmn.AllBsummResults]{}

	results := p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err != nil {
			if res.details == "" || res.details == dfltDetail {
				res.details = xact.Cname(apc.ActSummaryBck, msg.UUID)
			}
			err = res.toErr()
			freeBcastRes(results)
			return nil, 0, err
		}
	}

	var (
		summaries   = make(cmn.AllBsummResults, 0, 8)
		dsize       = make(map[string]uint64, len(results))
		numAccepted int
		numPartial  int
	)
	for _, res := range results {
		if res.status == http.StatusAccepted {
			numAccepted++
			continue
		}
		if res.status == http.StatusPartialContent {
			numPartial++
		}
		tbsumm, tid := res.v.(*cmn.AllBsummResults), res.si.ID()
		for _, summ := range *tbsumm {
			dsize[tid] = summ.TotalSize.Disks
			summaries = summaries.Aggregate(summ)
		}
	}
	summaries.Finalize(dsize, cmn.Rom.TestingEnv())
	freeBcastRes(results)

	switch {
	case numPartial == 0 && numAccepted == 0:
		status = http.StatusOK
	case numPartial == 0:
		status = http.StatusAccepted
	default:
		status = http.StatusPartialContent
	}
	return summaries, status, nil
}

// fully reuse bsummact impl.
func (p *proxy) bsummhead(bck *meta.Bck, msg *apc.BsummCtrlMsg) (info *cmn.BsummResult, status int, err error) {
	var (
		summaries cmn.AllBsummResults
		qbck      = (*cmn.QueryBcks)(bck) // adapt
	)
	if msg.UUID == "" {
		if err = p.bsummNew(qbck, msg); err == nil {
			status = http.StatusAccepted
		}
		return info, status, err
	}
	summaries, status, err = p.bsummCollect(qbck, msg)
	if err == nil && (status == http.StatusOK || status == http.StatusPartialContent) {
		info = summaries[0]
	}
	return info, status, err
}
