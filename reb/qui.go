// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
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

const logIval = 10 * time.Second

type qui struct {
	rargs *rebArgs
	reb   *Reb
	i     time.Duration // to log every logIval
}

func (q *qui) quicb(total time.Duration) core.QuiRes {
	xctn := q.reb.xctn()
	if xctn == nil || xctn.IsAborted() || xctn.Finished() {
		return core.QuiInactiveCB
	}

	//
	// a) at least 2*max-keepalive _prior_ to counting towards config.Transport.QuiesceTime.D()
	//
	lastrx := q.reb.lastrx.Load()
	timout := max(q.rargs.config.Timeout.MaxKeepalive.D()<<1, 8*time.Second)
	if lastrx != 0 && mono.Since(lastrx) < timout {
		return core.QuiActive
	}

	//
	// b) secondly and separately, all other targets must finish sending
	//
	locStage := q.reb.stages.stage.Load()
	debug.Assert(locStage >= rebStageFin || xctn.IsAborted(), locStage, " vs ", rebStageFin)
	for _, tsi := range q.rargs.smap.Tmap {
		status, ok := q.reb.checkStage(tsi, q.rargs, locStage)
		if ok && status.Running && status.Stage < rebStageFin {
			if i := total / logIval; i > q.i {
				q.i = i
				nlog.Infof("%s g[%d]: waiting for %s(stage %s) to quiesce", core.T, q.reb.RebID(), tsi.StringEx(), stages[status.Stage])
			}
			return core.QuiActiveDontBump
		}
	}

	return core.QuiInactiveCB
}
