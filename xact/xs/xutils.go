// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/cmn/oom"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
)

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
	rate.src = src.NewFrontendRateLim(nat)
	if dst.Props != nil { // destination may not exist
		rate.dst = dst.NewFrontendRateLim(nat)
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

const (
	minTcoWorkChSize  = 256
	minArchWorkChSize = 256
)

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

// strict rules
func throttleNwp(xname string, n, l int) (int, error) {
	// 1. alert 'too many gorutines'
	tstats := core.T.StatsUpdater()
	flags := cos.NodeStateFlags(tstats.Get(cos.NodeAlerts))
	if flags.IsSet(cos.NumGoroutines) {
		nlog.Warningln(xname, "too many gorutines:", runtime.NumGoroutine(), "[ num-cpu:", sys.NumCPU(), "]")
		n = min(n, l)
	}

	// 2. number of available cores(*)
	n = min(sys.MaxParallelism()+4, n)

	// 3. factor-in memory pressure
	var (
		mm       = core.T.PageMM()
		pressure = mm.Pressure()
	)
	switch pressure {
	case memsys.OOM, memsys.PressureExtreme:
		oom.FreeToOS(true)
		if !cmn.Rom.TestingEnv() {
			return 0, errors.New(xname + ": " + memsys.FmtErrExtreme + " - not starting")
		}
	case memsys.PressureHigh:
		if flags.IsSet(cos.NumGoroutines) {
			return nwpNone, nil
		}
		n = min(nwpMin+1, n)
		nlog.Warningln(xname, "num-workers =", n)
	}

	// 4. finally, take into account load averages
	var (
		load     = sys.MaxLoad()
		highLoad = sys.HighLoadWM()
	)
	if load >= float64(highLoad) {
		nlog.Warningln(xname, "high load [", load, highLoad, "]")
		if flags.IsSet(cos.NumGoroutines) {
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

//
// recvAbortErr - to avoid duplicated broadcast
//

type recvAbortErr struct {
	err error
}

func (e *recvAbortErr) Error() string { return e.err.Error() }
