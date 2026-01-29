// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/load"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/cmn/oom"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
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

//
// num-workers parallelism
//

// workers (tcb and tco, the latter via lrit)
const (
	nwpBurstMult = 48 // shared work channel burst multiplier (channel size = burst * num-workers)
	nwpBurstMax  = 4096
)
const (
	nwpNone = -1 // no workers at all - iterated LOMs get executed by the (iterating) goroutine
	nwpMin  = 2  // throttled
	nwpDflt = 0  // (number of mountpaths)
)

// clamp the requested number of workers based on node load
// usage: all list-range type jobs and tcb
// - xname is xaction name
// - n is the requested number of workers
func clampNumWorkers(xname string, n, numMpaths int) (int, error) {
	const memExtremeMsg = "extreme memory pressure"
	var (
		ngr     = runtime.NumGoroutine()
		ngrLoad = load.Gor(ngr)
	)
	// 1. goroutine
	if ngrLoad == load.Critical {
		nlog.Warningln(xname, stats.NgrPrompt, ngr)
		return nwpNone, nil
	}

	n = min(sys.MaxParallelism()+4, n)

	// yellow alert (high)
	if ngrLoad == load.High {
		nlog.Warningln(xname, stats.NgrPrompt, ngr)
		n = min(n, numMpaths)
	}

	// 2. memory pressure
	memLoad := load.Mem()
	switch memLoad {
	case load.Critical:
		oom.FreeToOS(true)
		if !cmn.Rom.TestingEnv() {
			return 0, errors.New(xname + ": " + memExtremeMsg + " - not starting")
		}
		return nwpNone, nil
	case load.High:
		if ngrLoad == load.High {
			return nwpNone, nil
		}
		n = min(nwpMin+1, n)
	}

	// 3. CPU load averages
	cpuLoad := load.CPU()
	if cpuLoad >= load.High {
		if lv, wm := sys.MaxLoad(), sys.HighLoadWM(); lv >= float64(wm) {
			nlog.Warningln(xname, "high load [", lv, wm, "]")
		}
		if ngrLoad == load.High {
			return nwpNone, nil
		}
		n = min(nwpMin, n)
	}

	return n, nil
}

func abortOpcode(r core.Xact, opcode int) error {
	err := fmt.Errorf("invalid header opcode (%d)", opcode)
	debug.AssertNoErr(err)
	r.Abort(err)
	return err
}
