// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
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
// - bsummAct  <= api.GetBucketSummary(query-bcks, ActMsg)
// - bsummHead <= api.GetBucketInfo(bck, QparamBinfoWithOrWithoutRemote)
// Summary xactions share proxy start and query handlers (`summNew` and `summCollect`);
// each action keeps its own message parsing and final result aggregation logic.

type summCtx[T any] struct {
	p    *proxy
	qbck *cmn.QueryBcks
	amsg *apc.ActMsg
	uuid string

	smap *smapX
	nat  int
}

func (p *proxy) bsummAct(w http.ResponseWriter, r *http.Request, qbck *cmn.QueryBcks, actMsg *apc.ActMsg, msg *apc.BsummCtrlMsg) {
	isNew := msg.UUID == ""
	if !isNew && !cos.IsValidUUID(msg.UUID) {
		p.writeErrf(w, r, "%s: invalid UUID %q", apc.ActSummaryBck, msg.UUID)
		return
	}

	// start new
	if isNew {
		msg.UUID = cos.GenUUID()
		actMsg.Value = msg
		ctx, err := newSummCtx[cmn.AllBsummResults](p, qbck, actMsg, msg.UUID)
		if err == nil {
			err = ctx.summNew()
		}
		if err != nil {
			p.writeErr(w, r, err)
		} else {
			w.WriteHeader(http.StatusAccepted)
			writeXid(w, msg.UUID)
		}
		return
	}

	// or, query partial or final results
	actMsg.Value = msg
	ctx, err := newSummCtx[cmn.AllBsummResults](p, qbck, actMsg, msg.UUID)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	summaries, status, err := p.bsummCollect(ctx)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	w.WriteHeader(status)
	p.writeJSON(w, r, summaries, "bucket-summary")
}

// bsummCollect keeps bucket-specific aggregation on top of summCollect.
// TODO: make it a summCtx[cmn.AllBsummResults] method once Go 1.27 lands.
func (*proxy) bsummCollect(ctx *summCtx[cmn.AllBsummResults]) (_ cmn.AllBsummResults, status int, err error) {
	results, status, err := ctx.summCollect()
	if err != nil {
		return nil, 0, err
	}
	if status == http.StatusAccepted {
		return cmn.AllBsummResults{}, status, nil
	}
	var (
		summaries = make(cmn.AllBsummResults, 0, 8)
		dsize     = make(map[string]uint64, len(results))
	)
	for tid, tbsumm := range results {
		for _, summ := range tbsumm {
			dsize[tid] = summ.TotalSize.Disks
			summaries = summaries.Aggregate(summ)
		}
	}
	summaries.Finalize(dsize, cmn.Rom.TestingEnv())
	return summaries, status, nil
}

// fully reuse bsummAct impl.
func (p *proxy) bsummHead(bck *meta.Bck, msg *apc.BsummCtrlMsg) (info *cmn.BsummResult, status int, err error) {
	var (
		summaries cmn.AllBsummResults
		actMsg    = &apc.ActMsg{Action: apc.ActSummaryBck, Value: msg}
		qbck      = (*cmn.QueryBcks)(bck) // adapt
	)
	if msg.UUID == "" {
		msg.UUID = cos.GenUUID()
		actMsg.Value = msg
		ctx, e := newSummCtx[cmn.AllBsummResults](p, qbck, actMsg, msg.UUID)
		if e != nil {
			return info, status, e
		}
		if err = ctx.summNew(); err == nil {
			status = http.StatusAccepted
		}
		return info, status, err
	}
	ctx, err := newSummCtx[cmn.AllBsummResults](p, qbck, actMsg, msg.UUID)
	if err != nil {
		return info, status, err
	}
	summaries, status, err = p.bsummCollect(ctx)
	if err == nil && (status == http.StatusOK || status == http.StatusPartialContent) {
		debug.Assert(len(summaries) != 0, bck.Cname(msg.Prefix))
		if len(summaries) != 0 {
			info = summaries[0]
		}
	}
	return info, status, err
}

/////////////
// summCtx //
/////////////

func newSummCtx[T any](p *proxy, qbck *cmn.QueryBcks, amsg *apc.ActMsg, uuid string) (*summCtx[T], error) {
	smap := p.owner.smap.get()
	nat := smap.CountActiveTs()
	if nat < 1 {
		return nil, cmn.NewErrNoNodes(apc.Target, smap.CountTargets())
	}
	return &summCtx[T]{
		p:    p,
		qbck: qbck,
		amsg: amsg,
		uuid: uuid,
		smap: smap,
		nat:  nat,
	}, nil
}

// summNew broadcasts Begin2PC for a prepared summary ActMsg.
func (c *summCtx[T]) summNew() (err error) {
	q := make(url.Values, 1)
	c.qbck.SetQuery(q)

	actMsgExt := c.p.newAmsg(c.amsg, nil)

	args := allocBcArgs()
	args.req = cmn.HreqArgs{
		Method: http.MethodGet,
		Path:   apc.URLPathBuckets.Join(c.qbck.Name, apc.Begin2PC), // compare w/ txn
		Query:  q,
		Body:   cos.MustMarshal(actMsgExt),
	}
	// not using default control-plane timeout -
	// returning only _after_ all targets start running this new job
	// (see Run() in nsumm.go)
	args.timeout = apc.DefaultTimeout

	args.smap = c.smap
	results := c.p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err != nil {
			if res.details == "" || res.details == dfltDetail {
				res.details = xact.Cname(c.amsg.Action, c.uuid)
			}
			err = res.toErr()
			break
		}
	}
	freeBcastRes(results)
	if err != nil {
		return err
	}
	if err = c.smap.CheckSameTargets(&c.p.owner.smap.get().Smap, xact.Cname(c.amsg.Action, c.uuid)); err != nil {
		return err
	}
	return err
}

// summCollect broadcasts Query2PC and returns typed per-target snapshots.
func (c *summCtx[T]) summCollect() (_ map[string]T, status int, err error) {
	var (
		q         = make(url.Values, 4)
		actMsgExt = c.p.newAmsg(c.amsg, nil)
		args      = allocBcArgs()
	)
	args.req = cmn.HreqArgs{
		Method: http.MethodGet,
		Path:   apc.URLPathBuckets.Join(c.qbck.Name, apc.Query2PC),
		Body:   cos.MustMarshal(actMsgExt),
	}
	args.smap = c.smap
	c.qbck.AddToQuery(q)
	q.Set(apc.QparamSilent, "true")
	args.req.Query = q
	args.cresv = cresjGeneric[T]{}

	results := c.p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err != nil {
			if res.details == "" || res.details == dfltDetail {
				res.details = xact.Cname(c.amsg.Action, c.uuid)
			}
			err = res.toErr()
			freeBcastRes(results)
			return nil, 0, err
		}
	}

	var (
		out         = make(map[string]T, c.nat)
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
		v, ok := res.v.(*T)
		if !ok || v == nil {
			freeBcastRes(results)
			return nil, 0, fmt.Errorf("%s: invalid response from %s: %T", xact.Cname(c.amsg.Action, c.uuid), res.si, res.v)
		}
		out[res.si.ID()] = *v
	}
	freeBcastRes(results)

	return out, summStatus(numAccepted, numPartial, len(out)), nil
}

func summStatus(numAccepted, numPartial, numResults int) int {
	switch {
	case numResults == 0:
		return http.StatusAccepted
	case numAccepted > 0 || numPartial > 0:
		return http.StatusPartialContent
	default:
		return http.StatusOK
	}
}
