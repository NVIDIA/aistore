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
	var msg cmn.BsummCtrlMsg
	if err := cos.MorphMarshal(amsg.Value, &msg); err != nil {
		p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, amsg.Action, amsg.Value, err)
		return
	}
	debug.Assert(msg.UUID == "" || cos.IsValidUUID(msg.UUID), msg.UUID)
	if qbck.IsBucket() {
		bck := (*cluster.Bck)(qbck)
		bckArgs := bckInitArgs{p: p, w: w, r: r, msg: amsg, perms: apc.AceBckHEAD, bck: bck, dpq: dpq}
		bckArgs.createAIS = false
		bckArgs.dontHeadRemote = msg.BckPresent
		if _, err := bckArgs.initAndTry(); err != nil {
			return
		}
	}
	orig, retried := msg.UUID, false
retry:
	summaries, tsi, numNotFound, err := p.bsummDo(qbck, &msg)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	// result
	if summaries != nil {
		p.writeJSON(w, r, summaries, "bucket-summary")
		return
	}
	// all accepted
	if numNotFound == 0 {
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(msg.UUID))
		return
	}

	// "not started"
	if orig != "" && !retried {
		runtime.Gosched()
		glog.Errorf("%s: checking on the x-%s[%s] status - retrying once...", p, apc.ActSummaryBck, msg.UUID)
		time.Sleep(cos.MaxDuration(cmn.Timeout.CplaneOperation(), time.Second))
		retried = true
		goto retry
	}
	var s string
	if tsi != nil {
		s = fmt.Sprintf(", targets: [%s ...]", tsi.StringEx())
	}
	err = fmt.Errorf("%s: %d instance%s of x-%s[%s] apparently failed to start%s",
		p, numNotFound, cos.Plural(numNotFound), apc.ActSummaryBck, msg.UUID, s)
	p.writeErr(w, r, err)
}

// steps
func (p *proxy) bsummDo(qbck *cmn.QueryBcks, msg *cmn.BsummCtrlMsg) (cmn.AllBsummResults, *cluster.Snode, int, error) {
	var (
		q      = make(url.Values, 4)
		config = cmn.GCO.Get()
		smap   = p.owner.smap.get()
		aisMsg = p.newAmsgActVal(apc.ActSummaryBck, msg)
	)
	// 1. init
	if msg.UUID == "" {
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
	tsi, numDone, numAck, err := p.bsummCheckRes(msg.UUID, results)
	if err != nil {
		return nil, tsi, 0, err
	}
	if numDone != len(results) {
		numNotFound := len(results) - (numDone + numAck)
		if numNotFound > 0 {
			var s string
			if tsi != nil {
				s = fmt.Sprintf(", failed targets: [%s ...]", tsi.StringEx())
			}
			glog.Errorf("%s: not found %d instance%s of x-%s[%s]%s",
				p, numNotFound, cos.Plural(numNotFound), apc.ActSummaryBck, msg.UUID, s)
		}
		return nil, tsi, numNotFound, nil
	}

	// 4. all targets ready - call to collect the results
	q.Set(apc.QparamTaskAction, apc.TaskResult)
	q.Set(apc.QparamSilent, "true")
	args.req.Query = q
	args.cresv = cresBsumm{} // -> cmn.AllBsummResults
	results = p.bcastGroup(args)
	freeBcArgs(args)

	// 5. summarize
	summaries := make(cmn.AllBsummResults, 0, 8)
	dsize := make(map[string]uint64, len(results))
	for _, res := range results {
		if res.err != nil {
			err = res.toErr()
			tsi = res.si
			freeBcastRes(results)
			return nil, tsi, 0, err
		}
		tgtsumm, tid := res.v.(*cmn.AllBsummResults), res.si.ID()
		for _, summ := range *tgtsumm {
			dsize[tid] = summ.TotalSize.Disks
			summaries = summaries.Aggregate(summ)
		}
	}
	summaries.Finalize(dsize, config.TestingEnv())
	freeBcastRes(results)
	return summaries, nil, 0, nil
}

func (p *proxy) bsummCheckRes(uuid string, results sliceResults) (tsi *cluster.Snode, numDone, numAck int, err error) {
	defer freeBcastRes(results)
	for _, res := range results {
		if res.status == http.StatusNotFound {
			tsi = res.si
			continue
		}
		if res.err != nil {
			err = res.err
			tsi = res.si
			return
		}
		switch {
		case res.status == http.StatusOK:
			numDone++
		case res.status == http.StatusAccepted:
			numAck++
		default:
			err = fmt.Errorf("%s: unexpected x-%s[%s] from %s: status(%d), details(%q)", p, apc.ActSummaryBck, uuid,
				res.si, res.status, res.details)
			debug.AssertNoErr(err)
			return
		}
	}
	return
}

// NOTE: always executes the _fast_ version of the bucket summary
func (p *proxy) bsummDoWait(bck *cluster.Bck, out *cmn.BsummResult, fltPresence int) error {
	var (
		max   = cmn.Timeout.MaxKeepalive()
		sleep = cos.ProbingFrequency(max)
		qbck  = (*cmn.QueryBcks)(bck)
		msg   = &cmn.BsummCtrlMsg{ObjCached: true, BckPresent: apc.IsFltPresent(fltPresence), Fast: true}
	)
	if _, _, _, err := p.bsummDo(qbck, msg); err != nil {
		return err
	}
	debug.Assert(cos.IsValidUUID(msg.UUID))
	for total := time.Duration(0); total < max; total += sleep {
		time.Sleep(sleep)
		summaries, tsi, numNotFound, err := p.bsummDo(qbck, msg)
		if err != nil {
			glog.Errorf("%s: x-%s[%s]: %s returned err: %v", p, apc.ActSummaryBck, msg.UUID, tsi, err)
			return err
		}
		if summaries == nil {
			if numNotFound > 0 {
				var s string
				if tsi != nil {
					s = fmt.Sprintf(", failed targets: [%s ...]", tsi.StringEx())
				}
				glog.Errorf("%s: not found %d instance%s of x-%s[%s]%s",
					p, numNotFound, cos.Plural(numNotFound), apc.ActSummaryBck, msg.UUID, s)
			}
			continue
		}
		*out = *summaries[0]
		return nil
	}
	glog.Warningf("%s: timed-out waiting for %s x-%s[%s]", p, bck, apc.ActSummaryBck, msg.UUID)
	return nil
}
