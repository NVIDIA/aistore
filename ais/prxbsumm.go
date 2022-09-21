// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"net/url"
	"runtime"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

func (p *proxy) bucketSummary(w http.ResponseWriter, r *http.Request, qbck *cmn.QueryBcks, amsg *apc.ActionMsg, dpq *dpq) {
	var msg apc.BckSummMsg
	if err := cos.MorphMarshal(amsg.Value, &msg); err != nil {
		p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, amsg.Action, amsg.Value, err)
		return
	}
	debug.Assert(msg.UUID == "" || cos.IsValidUUID(msg.UUID))
	if qbck.IsBucket() {
		bck := (*cluster.Bck)(qbck)
		bckArgs := bckInitArgs{p: p, w: w, r: r, msg: amsg, perms: apc.AceBckHEAD, bck: bck, dpq: dpq}
		bckArgs.createAIS = false
		if _, err := bckArgs.initAndTry(qbck.Name); err != nil {
			return
		}
	}
	summaries, err := p.bsummDo(qbck, &msg)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	if summaries == nil {
		debug.Assert(cos.IsValidUUID(msg.UUID))
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(msg.UUID))
	} else {
		p.writeJSON(w, r, summaries, "bucket-summary")
	}
}

// 4 (four) steps
func (p *proxy) bsummDo(qbck *cmn.QueryBcks, msg *apc.BckSummMsg) (summaries cmn.BckSummaries, err error) {
	var (
		q      = make(url.Values, 4)
		config = cmn.GCO.Get()
		smap   = p.owner.smap.get()
		aisMsg = p.newAmsgActVal(apc.ActSummaryBck, msg)
		isNew  bool
	)
	// 1. init
	if msg.UUID == "" {
		isNew = true
		msg.UUID = cos.GenUUID() // generate UUID
		q.Set(apc.QparamTaskAction, apc.TaskStart)
	} else {
		// an extra handshake to synchronize on 'all done'
		q.Set(apc.QparamTaskAction, apc.TaskStatus)
	}
	q = qbck.AddToQuery(q)

	// 2. request
	args := allocBcArgs()
	args.req = cmn.HreqArgs{
		Method: http.MethodGet,
		Path:   apc.URLPathBuckets.Join(qbck.Name),
		Query:  q,
		Body:   cos.MustMarshal(aisMsg),
	}
	args.smap = smap
	results := p.bcastGroup(args)

	// 3. check status
	if isNew {
		runtime.Gosched()
	}
	allDone, err := p.bsummCheckRes(msg.UUID, results)
	if err != nil || !allDone {
		return nil, err
	}

	// 4. all targets ready - call to collect the results
	q.Set(apc.QparamTaskAction, apc.TaskResult)
	q.Set(apc.QparamSilent, "true")
	args.req.Query = q
	args.cresv = cresBsumm{} // -> cmn.BckSummaries
	results = p.bcastGroup(args)
	freeBcArgs(args)

	// 5. summarize
	summaries = make(cmn.BckSummaries, 0, 8)
	dsize := make(map[string]uint64, len(results))
	for _, res := range results {
		if res.err != nil {
			err = res.toErr()
			freeBcastRes(results)
			return nil, err
		}
		tgtsumm, tid := res.v.(*cmn.BckSummaries), res.si.ID()
		for _, summ := range *tgtsumm {
			dsize[tid] = summ.TotalDisksSize
			summaries = summaries.Aggregate(summ)
		}
	}
	summaries.Finalize(dsize, config.TestingEnv())
	freeBcastRes(results)
	return summaries, nil
}

func (p *proxy) bsummCheckRes(uuid string, results sliceResults) (bool /*all done*/, error) {
	var numDone, numAck int
	defer freeBcastRes(results)
	for _, res := range results {
		if res.status == http.StatusNotFound {
			continue
		}
		if res.err != nil {
			return false, res.err
		}
		switch {
		case res.status == http.StatusOK:
			numDone++
		case res.status == http.StatusAccepted:
			numAck++
		default:
			err := fmt.Errorf("%s: unexpected x-summary[%s] from %s: status(%d), details(%q)", p, uuid,
				res.si, res.status, res.details)
			debug.AssertNoErr(err)
			return false, err
		}
	}
	return numDone == len(results), nil
}

// TODO -- FIXME: add query param to wait so-much time
func (p *proxy) bsummDoWait(bck *cluster.Bck, info *cmn.BucketInfo) error {
	var (
		max   = cmn.Timeout.MaxKeepalive()
		sleep = cos.ProbingFrequency(max)
		qbck  = (*cmn.QueryBcks)(bck)
		msg   = &apc.BckSummMsg{Cached: true, Fast: true}
	)
	if _, err := p.bsummDo(qbck, msg); err != nil {
		return err
	}
	debug.Assert(cos.IsValidUUID(msg.UUID))
	for total := time.Duration(0); total < max; total += sleep {
		time.Sleep(sleep)
		summaries, err := p.bsummDo(qbck, msg)
		if err != nil {
			return err
		}
		if summaries == nil {
			continue
		}
		info.BckSumm = *summaries[0]
		return nil
	}
	glog.Warningf("%s: timed-out waiting for %s x-summary[%s]", p, bck, msg.UUID)
	return nil
}
