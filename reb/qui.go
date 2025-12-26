// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"time"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
)

//
// quiesce prior to closing streams (fin-streams stage)
//

const logIval = time.Minute

type qui struct {
	rargs *rebArgs
	reb   *Reb
	i     time.Duration // to log every logIval
}

func (q *qui) quicb(total time.Duration) core.QuiRes {
	xreb := q.rargs.xreb
	if xreb.IsAborted() || xreb.IsDone() {
		return core.QuiInactiveCB
	}

	//
	// a) at least 2*max-keepalive _prior_ to counting towards config.Transport.QuiesceTime.D()
	//
	_lastrx := q.reb.lastrx.Load()
	timeout := max(q.rargs.config.Timeout.MaxKeepalive.D()<<1, 8*time.Second)
	if _lastrx != 0 && mono.Since(_lastrx) < timeout {
		if i := total / logIval; i > q.i {
			q.i = i
			locStage := q.reb.stages.stage.Load()
			nlog.Infoln(q.rargs.logHdr, "keep receiving in", stages[locStage], "stage")
		}
		return core.QuiActive
	}

	//
	// b) secondly and separately, all other targets must finish sending
	//
	locStage := q.reb.stages.stage.Load()
	debug.Assert(locStage >= rebStageFin || xreb.IsAborted(), locStage, " vs ", rebStageFin)
	for _, tsi := range q.rargs.smap.Tmap {
		status, _ := q.reb.checkStage(tsi, q.rargs, locStage)
		if status != nil && status.Running && status.Stage < rebStageFin {
			if i := total / logIval; i > q.i {
				q.i = i
				nlog.Infoln(q.rargs.logHdr, "in", stages[locStage], "waiting for:", tsi.StringEx(), stages[status.Stage])
			}
			return core.QuiActiveDontBump
		}
	}

	return core.QuiInactiveCB
}
