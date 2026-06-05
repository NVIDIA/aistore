// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
)

func (p *proxy) shardSummAct(w http.ResponseWriter, r *http.Request, bck *meta.Bck, actMsg *apc.ActMsg, msg *apc.ShardSummMsg) {
	isNew := msg.UUID == ""
	if !isNew && !cos.IsValidUUID(msg.UUID) {
		p.writeErrf(w, r, "%s: invalid UUID %q", apc.ActSummaryShard, msg.UUID)
		return
	}

	// start new
	if isNew {
		msg.UUID = cos.GenUUID()
		actMsg.Value = msg
		qbck := (*cmn.QueryBcks)(bck)
		ctx, err := newSummCtx[apc.ShardSummResult](p, qbck, actMsg, msg.UUID)
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
	out, status, err := p.shardSummCollect(bck, actMsg, msg)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	w.WriteHeader(status)
	p.writeJSON(w, r, out, "shard-summary")
}

func (p *proxy) shardSummCollect(bck *meta.Bck, actMsg *apc.ActMsg, msg *apc.ShardSummMsg) (_ *apc.ShardSummResult, status int, err error) {
	qbck := (*cmn.QueryBcks)(bck)
	ctx, err := newSummCtx[apc.ShardSummResult](p, qbck, actMsg, msg.UUID)
	if err != nil {
		return nil, 0, err
	}
	results, status, err := ctx.summCollect()
	if err != nil {
		return nil, 0, err
	}
	out := &apc.ShardSummResult{}
	if status == http.StatusAccepted {
		return out, status, nil
	}
	// Target walkers already skip mirror copies and EC chunks.
	for _, res := range results {
		out.Aggregate(res)
	}
	return out, status, nil
}
