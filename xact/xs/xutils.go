// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"errors"
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
)

var (
	errRecvAbort = errors.New("remote target abort") // to avoid duplicated broadcast
)

func newErrRecvAbort(r core.Xact, hdr *transport.ObjHdr) error {
	return fmt.Errorf("%s: %w [%s: %s]", r.Name(), errRecvAbort, meta.Tname(hdr.SID), hdr.ObjName /*emsg*/)
}

func isErrRecvAbort(err error) bool {
	return errors.Is(err, errRecvAbort)
}

func rgetstats(bp core.Backend, vlabs map[string]string, size, started int64) {
	tstats := core.T.StatsUpdater()
	delta := mono.SinceNano(started)
	tstats.IncWith(bp.MetricName(stats.GetCount), vlabs)
	tstats.AddWith(
		cos.NamedVal64{Name: bp.MetricName(stats.GetLatencyTotal), Value: delta, VarLabs: vlabs},
		cos.NamedVal64{Name: bp.MetricName(stats.GetSize), Value: size, VarLabs: vlabs},
	)
}

////////////
// tcrate //
////////////

// TODO:
// - support RateLimitConf.Verbs, here and elsewhere
// - `nat` with no respect to num-workers, ditto

type tcrate struct {
	src *cos.BurstRateLim
	dst *cos.BurstRateLim
}

func (rate *tcrate) init(src, dst *meta.Bck, nat int) {
	var err error
	rate.src, err = src.NewFrontendRateLim(nat)
	if err == nil {
		if dst.Props != nil { // destination may not exist
			rate.dst, err = dst.NewFrontendRateLim(nat)
			if err != nil {
				rate.src = nil
				rate.dst = nil
			}
		}
	}
}

func (rate *tcrate) acquire() {
	if rate.src != nil {
		rate.src.RetryAcquire(time.Second)
	}
	if rate.dst != nil {
		rate.dst.RetryAcquire(time.Second)
	}
}

func abortOpcode(r core.Xact, opcode int) error {
	err := fmt.Errorf("invalid header opcode (%d)", opcode)
	debug.AssertNoErr(err)
	r.Abort(err)
	return err
}
