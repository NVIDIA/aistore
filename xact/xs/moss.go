// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	ratomic "sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/load"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/cmn/work"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"

	jsoniter "github.com/json-iterator/go"
)

// -----------------------------------------------------------------------
// For background and usage, refer to:
// 1. GetBatch: Multi-Object Retrieval API, at https://github.com/NVIDIA/aistore/blob/main/docs/get_batch.md
// 2. Monitoring GetBatch Performance,      at https://github.com/NVIDIA/aistore/blob/main/docs/monitoring-get-batch.md
// ----------------------------------------------------------------------
// Flow:
//  1. client -> ais/ml -> DT (phase 1)
//  2. redirected client -> ais/ml -> DT (phase 2)
//  3. ais/ml proxy -> senders (phase 3)
//     run asynchronously; each active target sends OpcodeStartedWI => DT
//      then pushes its local entries
//
// DT assembly does the heavy lifting:
//    local entries      -> wi.next()
//    remote entries     -> recv.m[] / flushRx()
//    missing/soft errs  -> addMissing(); optional GFN recovery
// ----------------------------------------------------------------------
// Soft errors: any execution path that invokes addMissing() is classified as a _soft_ error,
//    irrespective of its original cause; these include:
//    a) missing data (404) - object or archived file doesn't exist;
//    b) remote stream-break (`ErrSBR`) - a temporary failure of a long-lived;
//       peer-to-peer connection that gets converted into a special missing entry
//    c) timeout waiting for peer to "push" next data item (object or archived file),
//       with subsequent recovery via direct target-to-target GET dubbed GFN (get-from-neighbor).
// Per-request max-number-soft-errors and GFN are separately
// configurable (https://github.com/NVIDIA/aistore/blob/main/docs/get_batch.md)
// -----------------------------------------------------------------------
// TODO: range read; throttle senders
// -----------------------------------------------------------------------

// hardcoded tunables
const (
	mossIdleTime = xact.IdleDefault // xact.Demand idle-ness

	// when work item is subject to GC
	mossAbandonTime = max(mossIdleTime, 5*time.Minute) // max age (see gcAbandoned)

	// max time inRx may remain non-zero before cleanup forces
	// sized for large-batch workloads (e.g. 1G+ objects under contention)
	inRxTimeout = min(90*time.Second, mossAbandonTime>>1)
	// quiesce window when finishing/aborting
	noRxWindow = 100 * time.Millisecond

	// senders to announce themselves
	sendersStartupWarn = 30 * time.Second
	sendersStartupFail = max(mossAbandonTime>>2, sendersStartupWarn*3)

	// log
	sparseLog       = time.Minute // low-level stats: total processed files, objects, etc.
	sparseAdvice    = time.Minute // initial interval to check memory, CPU, etc.
	sparseLogWIMask = 0xff        // verbose mode: log an already assembled work item every 256 requests

	// via OpcodeAbortWI from DT
	gcWIsAbortedByDT = time.Minute

	// small-size worker pool per x-moss instance - unless under heavy load
	// - created at init time
	// - stays enabled for the entire x-moss lifetime
	bewarmSize = cos.MiB

	// asm() size Rx poll
	pollDiv = 8

	// additional admission control
	pendingHighWm   = 96                  // high-watermark to start throttling
	pendingCritWm   = 256                 // critical-watermark to return 429
	pendingThrottle = cos.DfltRateMinBtwn // 10ms (see cos/rate_limit.go)

	// response controller (streaming mode only)
	// purpose: detect a stuck client (extremely slow reader, dead connection, broken TLS session)
	streamingWriteTimeout = max(30*time.Second, cmn.GetBatchWaitMin<<2) // max(30s, 4s) - see pollIval
	streamingFlushIval    = 5 * time.Second
)

type (
	mossFactory struct {
		xctn *XactMoss
		xreg.RenewBase
		designated bool
	}
)

type (
	rxentry struct {
		sgl        *memsys.SGL
		mopaque    *mossOpaque
		bucket     string
		nameInArch string
		local      bool
	}

	// per work item stats that all get summed-up => global Prometheus
	moss struct {
		obj struct {
			size int64
			cnt  int64
		}
		fil struct {
			size int64
			cnt  int64
		}
		wait int64 // Rx wait: time spent waiting to receive entries from peers
		ngfn int   // total num GFN requests by this wi
		errn int   // GFN failures; converted to missing-entry when ContinueOnErr=true
	}

	// work item
	basewi struct {
		aw     archive.Writer
		resp   *apc.MossResp
		sgl    *memsys.SGL // multipart (buffered) only
		timer  *time.Timer
		r      *XactMoss
		config *cmn.Config
		smap   *meta.Smap
		req    *apc.MossReq
		wctrl  *http.ResponseController
		wid    string   // work item ID
		sid    string   // current sender ID
		recv   struct { // Tx/Rx shared state
			ch   chan int
			m    []rxentry
			next int
			mtx  sync.Mutex
		}
		startup struct { // (since senders run async via phase 3)
			nat  int64         // number of active targets including self
			refc atomic.Int64  // number of ACKs received
			warn time.Duration // last time warned
		}
		stats   moss         // internal stats (-> CtlMsg)
		started int64        // (mono)
		soft    int          // number of soft errors must be <= maxSoftErrs; see definition above
		idx     int          // current position in outgoing batch
		clean   atomic.Bool  // true: wi.cleanup() in progress or done
		owned   atomic.Int32 // ownership: asm() path vs gc; Inc/DecPending() symmetry
		awfin   atomic.Bool  // true: is closed; false: need to close (outgoing) archive writer

		// Rx atomic state vs: (abandoned) | (Abort) | late arrival on a completed work item (with 404s)
		inRx      atomic.Int32 // ref-count
		lastRx    atomic.Int64 // (mono)
		abandoned ratomic.Pointer[errAbandonedWI]

		// asm() path: last progress (but gets initialized to wi.started in PrepRx prior to asm())
		lastAsm atomic.Int64 // (mono)
	}
	buffwi struct {
		*basewi
	}
	streamwi struct {
		*basewi
	}
)

type (
	XactMoss struct {
		gmm         *memsys.MMSA
		smm         *memsys.MMSA
		bewarm      *work.Pool
		pending     sync.Map // [wid => *basewi]
		abortedWIDs sync.Map // wid => error from DT
		xact.DemandBase
		activeWG     sync.WaitGroup // active asm()
		lastLog      atomic.Int64   // last log timestamp (sparse)
		advNextCheck atomic.Int64
		pendingCnt   atomic.Int64
		lastAbrtWID  atomic.Int64 // mon-time(abortedWIDs.Store)
	}
)

// interface guard
var (
	_ xreg.Renewable     = (*mossFactory)(nil)
	_ core.Xact          = (*XactMoss)(nil)
	_ transport.Receiver = (*XactMoss)(nil)
)

var (
	errMossRecvStopped = errors.New("x-moss Rx stopped")
	errMossXactStopped = errors.New("x-moss stopped")
)

func (*mossFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	p := &mossFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}}
	designated, ok := args.Custom.(bool)
	debug.Assert(ok)
	p.designated = designated
	return p
}

func (p *mossFactory) Start() error {
	debug.Assert(cos.IsValidUUID(p.Args.UUID), p.Args.UUID)

	p.xctn = newMoss(p)

	return nil
}

// exercise full control over "reuse the one that is running now" decision
// compare w/ xreg.usePrev() default logic
func (p *mossFactory) WhenPrevIsRunning(prev xreg.Renewable) (xreg.WPR, error) {
	if prev.UUID() == p.UUID() {
		return xreg.WprUse, nil
	}

	xprev := prev.Get()
	r, ok := xprev.(*XactMoss)
	if !ok || !cos.IsValidUUID(xprev.ID()) {
		// (unlikely)
		nlog.Errorln("unexpected xprev: [", ok, xprev.Name(), xprev.ID(), xprev.Kind(), "]")
		debug.Assert(false)
		return xreg.WprKeepAndStartNew, nil
	}

	var (
		prevBck = r.Bck()
		currBck = p.Bck
	)
	if prevBck != nil && prevBck.IsEmpty() { // normalize (x-base has cmn.Bck in its state)
		prevBck = nil
	}
	if currBck != nil && currBck.IsEmpty() { // (unlikely)
		debug.Assert(false)
		currBck = nil
	}

	switch {
	case prevBck == nil && currBck == nil:
		// reuse when no default bucket whatsoever
	case (prevBck == nil) != (currBck == nil):
		return xreg.WprKeepAndStartNew, nil
	default:
		if !prevBck.Equal(currBck, true /*same BID*/, true) {
			return xreg.WprKeepAndStartNew, nil
		}
		// reuse
	}

	if cmn.Rom.V(5, cos.ModXs) {
		nlog.Infoln(core.T.String(), "DT prev:", r.Cname(), "curr:", p.UUID(), "- using prev...")
	}
	// reset DemandBase.last timestamp to prevent idle timeout between now and Assemble()
	r.DemandBase.IncPending()
	r.DemandBase.DecPending()
	return xreg.WprUse, nil
}

func newMoss(p *mossFactory) *XactMoss {
	r := &XactMoss{
		gmm: core.T.PageMM(),
		smm: core.T.ByteMM(),
	}
	r.DemandBase.Init(p.UUID(), p.Kind(), p.Bck, mossIdleTime, r.fini)

	// best-effort `bewarm` for pagecache warming-up
	s := "without"
	v := "[]"
	config := cmn.GCO.Get()
	if num := config.GetBatch.WarmupWorkers(); num > 0 {
		adv := newAdvice(config)
		if adv.Load < load.High {
			r.bewarm = work.New(num, num /*work-chan cap*/, r.bewarmFQN, r.ChanAbort())
			s = "with"
			v = adv.String()
		}
	}
	r.advNextCheck.Store(mono.NanoTime())

	hk.Reg(r.hkName(), r.housekeep, mossAbandonTime)

	nlog.Infoln(core.T.String(), "run:", r.Name(), s, "\"bewarm\"", v)
	return r
}

//
// secondary HK: GC path
//

func (r *XactMoss) hkName() string { return r.ID() + "/aband" + hk.NameSuffix }

func (r *XactMoss) housekeep(now int64) time.Duration {
	if r.IsAborted() || r.IsDone() {
		return hk.UnregInterval
	}

	r.gcAbortedByDT(now)

	// abandoned work items
	if r.pendingCnt.Load() > 0 {
		gc := mossGC{
			r:      r,
			now:    now,
			cutoff: now - mossAbandonTime.Nanoseconds(),
		}
		gc.do()
	}
	return mossAbandonTime >> 1
}

func (*XactMoss) Run(*sync.WaitGroup) { debug.Assert(false) }

//
// DemandBase idle logic and asm() wait-group
//

func (r *XactMoss) incPending() {
	r.DemandBase.IncPending()
	r.activeWG.Add(1)
}

func (r *XactMoss) decPending() {
	r.DemandBase.DecPending()
	r.activeWG.Done()
}

// base abort + more
// (do NOT call from the asm() path - activeWG must be done)
func (r *XactMoss) Abort(err error) bool {
	if !r.DemandBase.Abort(err) {
		return false
	}
	// stop receiving
	bundle.SDM.UnregRecv(r.ID())

	hk.UnregIf(r.hkName(), func(int64) time.Duration { return 0 })

	// unreg idle callback prior to quiesce
	r.DemandBase.Stop()

	nlog.Infoln(r.Name(), "aborting:", err)

	r.activeWG.Wait()
	r._abortQuiesce()

	r.pending.Range(r.cleanup)
	r.pending.Clear()
	r.pendingCnt.Store(0)

	r.abortedWIDs.Clear()

	r.bewarmStop()
	return true
}

// best-effort: quiesce prior to cleanups
func (r *XactMoss) _abortQuiesce() {
	const pollIval = cos.PollSleepShort // 100ms
	var (
		total   time.Duration
		maxWait = max(cmn.Rom.MaxKeepalive(), 8*time.Second)
	)
	for r.Pending() > 0 && total < maxWait {
		time.Sleep(pollIval)
		total += pollIval
	}
	if p := r.Pending(); p > 0 {
		nlog.Warningln(r.Name(), "abort: still", p, "demand-pending after", maxWait, "- proceeding anyway")
	}
}

// (Abort, fini) => pending (sync.Map's) callback to cleanup all pending work items
func (*XactMoss) cleanup(_, value any) bool {
	wi := value.(*basewi)
	wi.cleanup(true /*xdone*/)
	return true
}

// graceful termination via (<-- xact.Demand <-- hk)
func (r *XactMoss) fini(int64) (d time.Duration) {
	switch {
	case r.IsAborted() || r.IsDone():
		return hk.UnregInterval
	case r.Pending() > 0:
		return mossIdleTime
	default:
		if !r.SetStopping() {
			return hk.UnregInterval
		}

		hk.UnregIf(r.hkName(), func(int64) time.Duration { return 0 })
		// stop receiving
		bundle.SDM.UnregRecv(r.ID())

		msg := r.CtlMsg()
		nlog.Infoln(r.Cname(), "idle expired [", msg, "]")

		r.pending.Range(r.cleanup)
		r.pending.Clear()
		r.pendingCnt.Store(0)

		r.abortedWIDs.Clear()

		r.bewarmStop()
		r.Finish()
		return hk.UnregInterval
	}
}

func (r *XactMoss) bewarmStop() {
	if r.bewarm == nil {
		return
	}
	r.bewarm.Stop()
	if n := r.bewarm.NumDone(); n > 0 {
		nlog.Infoln(r.Cname(), "bewarm count:", n)
	}
	r.bewarm.Wait()
}

// On "running under pressure" -----
// 1. Memory is the hard resource constraint.
//    DT allocates SGLs while assembling outgoing (TAR) archive, so mem-critical
//    conditions must reject new work items (http 429) - once RAM is exhausted
//    we cannot safely proceed.
// 2. Disk is a soft constraint.
//    Even at 95–99% disk utilization DT can still make forward progress.
//    Instead of rejecting, we slow down either in PrepRx()
//    (rare, only when overall load == Critical) or inside asm() where
//    long-running assembly naturally amplifies disk pressure.
// 3. load.Advice refreshing logic is decoupled:
//    - PrepRx refreshes infrequently (advNextCheck).
//    - asm() refreshes as prescribed (adv.ShouldCheck(i)) ---------------

// (phase 1)
func (r *XactMoss) PrepRx(req *apc.MossReq, config *cmn.Config, smap *meta.Smap, wid string, nat int, usingPrev bool) error {
	now := mono.NanoTime()

	// admission control
	throttled, err429 := r.admit(config, wid, now)
	if err429 != nil {
		return err429
	}
	if throttled {
		now = mono.NanoTime()
	}

	// DT's work item
	var (
		resp = &apc.MossResp{UUID: r.ID()}
		wi   = allocMossWi()
	)
	{
		wi.r = r
		wi.config = cmn.GCO.Get()
		wi.smap = smap
		wi.req = req
		wi.resp = resp
		wi.wid = wid
		wi.started = now
		wi.lastAsm.Store(now)
	}

	// receiving
	if nat > 1 {
		// insert self to properly demux => receive data (to assemble)
		if usingPrev {
			bundle.SDM.UseRecv(r)
		} else {
			bundle.SDM.RegRecv(r)
		}
		// Rx state
		l := len(req.In)
		if cap(wi.recv.m) < l {
			wi.recv.m = make([]rxentry, len(req.In))
		} else {
			wi.recv.m = wi.recv.m[:l]
		}
		wi.recv.ch = make(chan int, len(req.In)<<1) // extra cap

		interval := cos.Ternary(cmn.Rom.V(4, cos.ModXs), sparseLog/2, sparseLog)
		if since := time.Duration(wi.started - r.lastLog.Load()); since > interval {
			nlog.Infoln(r.Cname(), "ctlmsg (", r.CtlMsg(), ")")
			r.lastLog.Store(wi.started)
		}

		wi.startup.nat = int64(nat)

		wi.timer = time.NewTimer(time.Hour)
		wi.timer.Stop()
	}

	if a, loaded := r.pending.LoadOrStore(wid, wi); loaded {
		old := a.(*basewi)
		age := mono.Since(old.started)
		err := fmt.Errorf("%s: wid=%q already exists for %v (duplicate?)", r.Name(), wid, age)
		if age >= mossAbandonTime {
			nlog.Errorln(core.T.String(), err)
		}
		return err
	}

	r.pendingCnt.Inc()
	wi.awfin.Store(true)

	return nil
}

// DT admission control:
// - pendingCritWm is enforced immediately => 429
// - load-based decisions are refreshed periodically via load.Advice:
//   - mem-critical  OR  disk-critical    => 429
//   - mem-high      AND pending-high-wm  => 429
//   - mem-high      OR  pending-high-wm  => throttle
func (r *XactMoss) admit(config *cmn.Config, wid string, now int64) (throttled bool, _ error) {
	var (
		tstats   = core.T.StatsUpdater()
		pdt      = r.pendingCnt.Load()
		critical = pdt >= pendingCritWm
	)
	if critical {
		tstats.Inc(stats.ErrGetBatchCount)
		err := fmt.Errorf("%s: wid=%q rejected: pending=%d", r.Name(), wid, pdt)
		return false, cmn.NewErrTooManyRequests(err, http.StatusTooManyRequests)
	}

	// refresh load.Advice periodically
	next := r.advNextCheck.Load()
	if now < next {
		return false, nil
	}
	// benign race: temporarily set far-future value to minimize duplicate adv.Refresh()
	r.advNextCheck.Store(now + int64(time.Hour))

	adv := newAdvice(config)
	ival := advIval(adv.Load)
	r.advNextCheck.Store(now + int64(ival))

	critical = adv.MemLoad() == load.Critical || (adv.DskLoad() == load.Critical && !cmn.Rom.TestingEnv()) ||
		(adv.MemLoad() >= load.High && pdt >= pendingHighWm)
	if critical {
		tstats.Inc(stats.ErrGetBatchCount)
		err := fmt.Errorf("%s: wid=%q rejected due to resource pressure (%s, pending=%d)", r.Name(), wid, adv.String(), pdt)
		return false, cmn.NewErrTooManyRequests(err, http.StatusTooManyRequests)
	}

	sleep := adv.Sleep
	if (sleep > 0 && adv.MemLoad() >= load.High) || pdt >= pendingHighWm {
		sleep = cos.NonZero(sleep, pendingThrottle)
		if cmn.Rom.V(4, cos.ModXs) || time.Duration(now-r.lastLog.Load()) > sparseLog {
			nlog.Warningln(r.Name(), "pending:", pdt, "resource pressure:", adv.String(), "throttle:", sleep)

			// shared between two log sites: the throttle warning (in admit) and the CtlMsg snapshot
			// (split if need be)
			r.lastLog.Store(now)
		}
		time.Sleep(sleep)
		tstats.Add(stats.GetBatchThrottleTotal, int64(sleep))
		throttled = true
	}
	return throttled, nil
}

func (r *XactMoss) RecvCtrl(wid, sid, opcode string, body []byte) error {
	switch opcode {
	case xact.OpcodeStartedWI:
		debug.Assert(wid != xact.NoneWID)
		debug.Assert(len(body) == 0)
		wi := r._pinwi(wid, sid, true)
		if wi == nil {
			return nil
		}
		defer wi.unpin()
		wi.startup.refc.Inc()
	case xact.OpcodeAbortXact:
		var err error
		if len(body) > 0 {
			errCause := string(body)
			err = r.NewErrRecvAbortXact(sid, errCause)
		}
		r.Abort(err)
	case xact.OpcodeAbortWI:
		debug.Assert(wid != xact.NoneWID)
		var errCause string
		if len(body) > 0 {
			errCause = string(body)
		}
		err := r.NewErrRecvAbortWI(sid, wid, errCause)
		r.abortedWIDs.Store(wid, err)
		r.lastAbrtWID.Store(mono.NanoTime())
	default:
		return fmt.Errorf("%s: unknown ctrl opcode %q", r.Name(), opcode)
	}
	return nil
}

// (phase 2)
// assemble requested data (local and remote); emit resulting archive
// a note on error handling:
// - buffered mode: full error propagation, real HTTP status, client sees the error
// - streaming: truncated TAR on the wire, user sees the error via CLI 'ais show job' ex post facto
func (r *XactMoss) Assemble(req *apc.MossReq, w http.ResponseWriter, wid string) error {
	a, loaded := r.pending.Load(wid)
	if !loaded {
		if err := r.AbortErr(); err != nil {
			return err
		}
		err := cos.NewErrNotFoundFmt(r, "wid=%q (prep-rx not done?)", wid)
		debug.Assert(r.IsDone() || r.IsAborted() || err == nil)
		return err
	}
	wi := a.(*basewi)

	if !wi.owned.CAS(wiownNone, wiownAsm) {
		if err := wi.errAbandoned(); err != nil {
			return err
		}
		err := fmt.Errorf("%s: wid=%q claimed but not abandoned", r.Name(), wid)
		debug.AssertNoErr(err)
		return err
	}

	if r.IsAborted() || r.IsDone() {
		wi.owned.CAS(wiownAsm, wiownNone) // Abort/fini owns final cleanup
		return nil
	}

	// asm() path owns wi; DecPending below
	r.incPending()
	defer r.decPending()
	if r.IsAborted() || r.IsDone() {
		wi.owned.CAS(wiownAsm, wiownNone) // ditto
		return nil
	}

	debug.Assert(wid == wi.wid)
	err := r.asm(req, w, wi)
	if err != nil {
		if r.shouldBcastAbortWI(wi) {
			r.BcastCtrl(wi.smap, wi.wid, xact.OpcodeAbortWI, err)
		}
		tstats := core.T.StatsUpdater()
		tstats.Inc(stats.ErrGetBatchCount)
		nerr := tstats.Get(stats.ErrGetBatchCount)
		if cmn.Rom.SuperV(4, cos.ModXs) || nerr < 5 || (nerr < 100 && nerr%10 == 0) || (nerr&sparseLogWIMask) == 0 {
			nlog.Warningln(r.Cname(), "err:", err, "[", wi.ctlMsg(), "] streaming:", req.StreamingGet)
		}
	} else if cmn.Rom.V(5, cos.ModXs) {
		tstats := core.T.StatsUpdater()
		nreq := tstats.Get(stats.GetBatchCount)
		if cmn.Rom.SuperV(5, cos.ModXs) || (nreq&sparseLogWIMask) == 0 {
			nlog.Infoln(r.Cname(), "[", wi.ctlMsg(), "] streaming:", req.StreamingGet)
		}
	}

	ok := wi.cleanup(false /*xdone*/)
	debug.Assert(wi.owned.Load() == wiownAsm)

	if ok {
		freeMossWi(wi)
	} else {
		owned := wi.owned.CAS(wiownAsm, wiownNone)
		debug.Assert(owned)
	}
	return err
}

func (r *XactMoss) shouldBcastAbortWI(wi *basewi) bool {
	if r.IsAborted() || r.IsDone() {
		return false
	}
	nat := int(wi.startup.nat) // number of active targets in wi.smap (write-once in PrepRx, read via pending sync.Map)
	if nat <= 1 {
		return false
	}

	var (
		pdt = r.pendingCnt.Load()
		rem = len(wi.req.In) - wi.idx
	)
	switch {
	case pdt >= pendingCritWm:
		return rem > 0
	case pdt >= pendingHighWm:
		return rem >= nat-1
	default:
		return rem >= nat*4
	}
}

func (r *XactMoss) asm(req *apc.MossReq, w http.ResponseWriter, basewi *basewi) error {
	opts := archive.Opts{TarFormat: tar.FormatUnknown} // default tar format

	// streaming
	if req.StreamingGet {
		wi := streamwi{basewi: basewi}
		wi.aw = archive.NewWriter(req.OutputFormat, w, nil /*checksum*/, &opts)
		wi.awfin.Store(false)
		return wi.asm(w)
	}

	// buffered
	var (
		sgl = r.gmm.NewSGL(0)
		wi  = buffwi{basewi: basewi}
	)
	wi.sgl = sgl
	wi.resp.Out = make([]apc.MossOut, 0, len(req.In))
	wi.aw = archive.NewWriter(req.OutputFormat, sgl, nil /*checksum*/, &opts)
	wi.awfin.Store(false)
	return wi.asm(w)
}

// send all requested local data => DT (`tsi`)
// (phase 3)
func (r *XactMoss) Send(req *apc.MossReq, smap *meta.Smap, dt *meta.Snode /*DT*/, wid string) error {
	// announce oneself
	if err := r.SendCtrl(dt, wid, xact.OpcodeStartedWI, nil /*body*/); err != nil {
		return err
	}

	// note:
	// - throttle sender as well: init load.Advice and check periodically inside the loop
	// - IncPending/DecPending is done by the caller

	defer r.abortedWIDs.Delete(wid)
	for i := range req.In {
		if r.IsAborted() || r.IsDone() {
			return nil
		}
		if e, ok := r.abortedWIDs.LoadAndDelete(wid); ok {
			err := e.(error)
			debug.Assertf(xact.IsErrRecvAbortWI(err), "%v (%T)", e, e)
			return err
		}
		in := &req.In[i]
		lom, tsi, err := r._lom(in, smap)
		if err != nil {
			return err
		}
		if tsi != nil {
			continue // other target must have it
		}

		var (
			nameInArch = in.NameInRespArch(lom.Bck().Name, req.OnlyObjName)
			handedOff  bool
		)
		// success: unlock via transport completion
		// error: unlock below unless SDM took ownership
		lom.Lock(false)

		if in.ArchPath == "" {
			handedOff, err = r._sendreg(dt, lom, wid, nameInArch, in, i)
		} else {
			handedOff, err = r._sendarch(dt, lom, wid, nameInArch, in, i)
		}
		if err != nil {
			if !handedOff {
				lom.Unlock(false)
			}
			return err
		}
	}

	if cmn.Rom.SuperV(5, cos.ModXs) {
		nlog.Infoln(r.Cname(), core.T.String(), "done Send", wid)
	}
	return nil
}

type (
	anySentCmpl struct {
		lom    *core.LOM
		lh     *core.LomHandle
		opaque []byte
	}
)

// called under r-lock
func (r *XactMoss) _sendreg(tsi *meta.Snode, lom *core.LOM, wid, nameInArch string, in *apc.MossIn, index int) (bool, error) {
	var (
		lh      cos.LomReader
		roc     cos.ReadOpenCloser
		oah     cos.OAH
		mopaque = &mossOpaque{wid: wid, oname: lom.ObjName, index: int32(index)}
	)

	err := lom.Load(false /*cache it*/, true)
	if err == nil {
		if lh, err = lom.Open(); err != nil {
			return false, fmt.Errorf("%s: failed to open already loaded %s under r-lock: %w", r.Name(), lom.Cname(), err)
		}
	}

	if err != nil {
		mopaque.missing = true
		mopaque.emsg = err.Error()
		nameInArch = apc.MossMissingDir + cos.PathSeparator + nameInArch
		oah = &cmn.ObjAttrs{}
	} else {
		oah = lom.ObjAttrs()
		off, length, errRng := in.CheckRange(lom.Lsize()) // note: (0,0) => full object
		if errRng != nil {
			cos.Close(lh)
			return false, errRng
		}
		if length != lom.Lsize() {
			oah = &cmn.ObjAttrs{Size: length} // range length
		}
		roc, err = lom.NewSectionHandle(lh, off, length) // owns lh (mono + chunked)
		if err != nil {
			return false, err
		}
	}

	opaque := r.packOpaque(mopaque)
	o := transport.AllocSend()
	hdr := &o.Hdr
	{
		hdr.Bck.Copy(lom.Bucket())
		hdr.ObjName = nameInArch
		hdr.ObjAttrs.CopyFrom(oah, true /*skip cksum*/)
		hdr.Demux = r.ID()
		hdr.Opaque = opaque
	}

	// roc owns lh (closed via transport.Stream completion); sent-callback unlocks + frees
	o.SentCB = r.anySent
	o.CmplArg = &anySentCmpl{lom: lom, opaque: opaque}

	return true /*handedOff*/, bundle.SDM.Send(o, roc, tsi, r)
}

func (r *XactMoss) anySent(hdr *transport.ObjHdr, _ io.ReadCloser, arg any, err error) {
	ctx, ok := arg.(*anySentCmpl)
	debug.Assert(ok)
	debug.Assert(ctx.lom.IsLocked() == apc.LockRead)

	if ctx.lh != nil {
		cos.Close(ctx.lh)
	}
	ctx.lom.Unlock(false)
	r.smm.Free(ctx.opaque)

	if err == nil && hdr.ObjAttrs.Size > 0 {
		// generic (xaction-level) counters do not contain file and range stats -
		// using `OutObjs` to count num transmissions
		r.OutObjsAdd(1, hdr.ObjAttrs.Size)
	}
}

// called under r-lock
func (r *XactMoss) _sendarch(tsi *meta.Snode, lom *core.LOM, wid, nameInArch string, in *apc.MossIn, index int) (bool, error) {
	var (
		roc     cos.ReadOpenCloser
		oah     cos.SimpleOAH
		mopaque = &mossOpaque{
			wid:   wid,
			oname: lom.ObjName + "/" + in.ArchPath,
			index: int32(index),
		}
	)
	nameInArch += cos.PathSeparator + in.ArchPath

	lh, err := lom.NewHandle(false /*loaded*/)
	if err != nil {
		mopaque.missing = true
		mopaque.emsg = err.Error()
		nameInArch = apc.MossMissingDir + cos.PathSeparator + nameInArch
	} else {
		// Note: This will use the shard index fast path if available.
		csl, err := lom.NewArchpathReader(lh, in.ArchPath, "" /*mime*/)
		if err != nil {
			nameInArch = apc.MossMissingDir + cos.PathSeparator + nameInArch
			mopaque.missing = true
			mopaque.emsg = err.Error()
			cos.Close(lh)
			lh = nil
		} else {
			size := csl.Size()
			off, length, errRng := in.CheckRange(size)
			if errRng != nil {
				cos.Close(csl)
				cos.Close(lh)
				return false, errRng
			}
			// range reader over archived file
			if length != size {
				csl, err = archive.RangeReader(csl, off, length)
				if err != nil {
					cos.Close(lh)
					return false, err
				}
				size = length
			}
			roc = cos.NopOpener(csl)
			oah.Size = size
		}
	}

	opaque := r.packOpaque(mopaque)
	o := transport.AllocSend()
	hdr := &o.Hdr
	{
		hdr.Bck.Copy(lom.Bucket())
		hdr.ObjName = nameInArch
		hdr.ObjAttrs.Size = oah.Size
		hdr.Demux = r.ID()
		hdr.Opaque = opaque
	}
	o.SentCB = r.anySent
	o.CmplArg = &anySentCmpl{lom: lom, lh: lh, opaque: opaque}

	return true /*handedOff*/, bundle.SDM.Send(o, roc, tsi, r)
}

func (r *XactMoss) packOpaque(data *mossOpaque) []byte {
	size := data.PackedSize()
	// Round up to the nearest multiple for memory pool allocation
	lr := (size + memsys.SmallSlabIncStep - 1) / memsys.SmallSlabIncStep * memsys.SmallSlabIncStep
	slab, err := r.smm.GetSlab(int64(lr))
	debug.AssertNoErr(err)
	buf := slab.Alloc()

	packer := cos.NewPacker(buf, size)
	packer.WriteAny(data)
	return packer.Bytes()
}

func (r *XactMoss) unpackOpaque(opaque []byte) (*mossOpaque, error) {
	unpacker := cos.NewUnpacker(opaque)
	mopaque := &mossOpaque{}
	if err := mopaque.Unpack(unpacker); err != nil {
		return nil, fmt.Errorf("%s: failed to unpack opaque data: %w", r.Name(), err)
	}
	return mopaque, nil
}

// demux -> wi.recv()
// [convention:] received hdr.ObjName is `nameInArch` (ie., filename in resulting TAR)
func (r *XactMoss) RecvObj(hdr *transport.ObjHdr, reader io.Reader, err error) error {
	defer transport.DrainAndFreeReader(reader)
	if err != nil {
		return err
	}
	if r.IsAborted() || r.IsDone() {
		return nil
	}

	// not expecting transport control opcodes (utilizing t2tctrl instead)
	if hdr.Opcode != 0 {
		return abortOpcode(r, hdr.Opcode)
	}

	// data
	if err := r._recvObj(hdr, reader); err != nil {
		smap := core.T.Sowner().Get()
		r.BcastCtrl(smap, "" /*wid*/, xact.OpcodeAbortXact, err)
		r.Abort(err)
		return err
	}
	if hdr.ObjAttrs.Size > 0 {
		r.InObjsAdd(1, hdr.ObjAttrs.Size)
	}
	return nil
}

// callers must wi.unpin()
func (r *XactMoss) _pinwi(wid, sid string, warn bool) *basewi {
	a, loaded := r.pending.Load(wid)
	if !loaded {
		if warn || cmn.Rom.V(4, cos.ModXs) {
			nlog.Warningln(r.Name(), "stale or unknown wid", wid, "from", meta.Tname(sid), "[ warn:", warn, "]")
		}
		return nil
	}
	wi := a.(*basewi)
	debug.Assertf(wi.wid == wid, "%q vs %q", wi.wid, wid)

	if wi.clean.Load() || wi.errAbandoned() != nil {
		return nil
	}
	wi.inRx.Inc()
	if wi.clean.Load() || wi.errAbandoned() != nil {
		wi.inRx.Dec()
		return nil
	}

	// vs cleanup's quiesce (marks Rx activity)
	wi.lastRx.Store(mono.NanoTime())
	return wi
}

func (wi *basewi) unpin() { wi.inRx.Dec() }

// (note: ObjHdr and its fields must be consumed synchronously)
func (r *XactMoss) _recvObj(hdr *transport.ObjHdr, reader io.Reader) error {
	mopaque, err := r.unpackOpaque(hdr.Opaque)
	if err != nil {
		return err
	}

	wi := r._pinwi(mopaque.wid, hdr.SID, false /*warn*/)
	if wi == nil {
		return nil
	}
	defer wi.unpin()

	debug.Assert(wi.receiving())
	return wi.recvObj(int(mopaque.index), hdr, reader, mopaque)
}

func (*mossFactory) Kind() string     { return apc.ActGetBatch }
func (p *mossFactory) Get() core.Xact { return p.xctn }
func (r *XactMoss) Snap() *core.Snap  { return r.Base.NewSnap(r) }

func (r *XactMoss) _lom(in *apc.MossIn, smap *meta.Smap) (lom *core.LOM, tsi *meta.Snode, err error) {
	bck, err := r._bucket(in)
	if err != nil {
		return nil, nil, err
	}

	lom = &core.LOM{ObjName: in.ObjName}
	if err := lom.InitBck(bck); err != nil {
		return nil, nil, err
	}

	var local bool
	tsi, local, err = lom.HrwTarget(smap)
	if local {
		tsi = nil
	}
	return
}

// per-object override, if specified
func (r *XactMoss) _bucket(in *apc.MossIn) (*meta.Bck, error) {
	if bck, err := meta.BckFromUBP(in.Uname, in.Bucket, in.Provider); bck != nil || err != nil {
		return bck, err
	}
	// default
	bck := r.Bck()
	if bck != nil {
		return bck, nil
	}
	if in.ArchPath == "" {
		return nil, fmt.Errorf("%s: missing bucket specification for object %q", r.Name(), in.ObjName)
	}
	return nil, fmt.Errorf("%s: missing bucket specification for archived file %s/%s", r.Name(), in.ObjName, in.ArchPath)
}

func (*XactMoss) bewarmFQN(fqn string) {
	if f, err := os.Open(fqn); err == nil {
		io.CopyN(io.Discard, f, bewarmSize)
		f.Close()
	}
}

// CtlMsg
// - this xaction's just-in-time pending counts and bewarm ("job:" prefix)
// - this target's kind=get-batch counters ("node:" prefix)
const (
	ctlMaxLen = 256
)

func (r *XactMoss) CtlMsg() string {
	var sb cos.SB
	sb.Init(ctlMaxLen)
	r._ctlMsg(&sb)
	return sb.String()
}

func (r *XactMoss) _ctlMsg(sb *cos.SB) {
	tstats := core.T.StatsUpdater()
	nreq := tstats.Get(stats.GetBatchCount)
	nerr := tstats.Get(stats.ErrGetBatchCount)
	if nreq == 0 && nerr == 0 {
		return
	}

	// [this target]
	sb.WriteString(core.T.String())
	sb.WriteString(":[")
	sb.WriteString("reqs:")
	sb.WriteString(strconv.FormatInt(nreq, 10))
	if nerr > 0 {
		sb.WriteString(" errs:")
		sb.WriteString(strconv.FormatInt(nerr, 10))
	}
	if ocnt := tstats.Get(stats.GetBatchObjCount); ocnt > 0 {
		sb.WriteString(" objs:(")
		sb.WriteString(strconv.FormatInt(ocnt, 10))
		sb.WriteUint8(',')
		sb.WriteString(cos.IEC(tstats.Get(stats.GetBatchObjSize), 2))
		sb.WriteUint8(')')
	}
	if fcnt := tstats.Get(stats.GetBatchFileCount); fcnt > 0 {
		sb.WriteString(" files:(")
		sb.WriteString(strconv.FormatInt(fcnt, 10))
		sb.WriteUint8(',')
		sb.WriteString(cos.IEC(tstats.Get(stats.GetBatchFileSize), 2))
		sb.WriteUint8(')')
	}
	if wait := tstats.Get(stats.GetBatchRxWaitTotal); wait > 0 {
		sb.WriteString(" avg-wait:")
		sb.WriteString((time.Duration(wait / nreq)).String())
	}
	sb.WriteUint8(']')

	// [this xaction]
	pdemand, pdt := r.Pending(), r.pendingCnt.Load()
	hasJob := pdemand != 0 || pdt != 0 || r.bewarm != nil
	if !hasJob {
		return
	}
	sb.WriteString(" job:[")
	if pdemand != 0 || pdt != 0 {
		sb.WriteString("pending:(")
		sb.WriteString(strconv.FormatInt(pdemand, 10))
		sb.WriteUint8(',')
		sb.WriteString(strconv.FormatInt(pdt, 10))
		sb.WriteUint8(')')
	}
	if r.bewarm != nil {
		if pdemand != 0 || pdt != 0 {
			sb.WriteUint8(' ')
		}
		sb.WriteString("bewarm:(")
		sb.WriteString(strconv.Itoa(r.bewarm.NumWorkers()))
		sb.WriteUint8(',')
		sb.WriteString(strconv.FormatInt(r.bewarm.NumDone(), 10))
		sb.WriteUint8(')')
	}
	sb.WriteUint8(']')
}

////////////
// basewi //
////////////

func (wi *basewi) ctlMsg() string {
	var sb cos.SB
	sb.Init(ctlMaxLen)
	wi._ctlMsg(&sb)
	return sb.String()
}

func (wi *basewi) _ctlMsg(sb *cos.SB) {
	sb.WriteString("work:wid=\"")
	sb.WriteString(wi.wid)
	sb.WriteUint8('"')

	if c := wi.stats.obj.cnt; c > 0 {
		sb.WriteString(" objs:[")
		sb.WriteString(strconv.FormatInt(c, 10))
		sb.WriteUint8(' ')
		sb.WriteString(cos.IEC(wi.stats.obj.size, 2))
		sb.WriteUint8(']')
	}
	if c := wi.stats.fil.cnt; c > 0 {
		sb.WriteString(" files:[")
		sb.WriteString(strconv.FormatInt(c, 10))
		sb.WriteUint8(' ')
		sb.WriteString(cos.IEC(wi.stats.fil.size, 2))
		sb.WriteUint8(']')
	}
	if n := wi.inRx.Load(); n > 0 {
		sb.WriteString(" in-rx:")
		sb.WriteString(strconv.FormatInt(int64(n), 10))
	}
	if n := wi.stats.ngfn; n > 0 {
		sb.WriteString(" gfn:")
		sb.WriteString(strconv.Itoa(n))
	}
	if n := wi.stats.errn; n > 0 {
		sb.WriteString(" gfn-err:")
		sb.WriteString(strconv.Itoa(n))
	}
	if wi.started > 0 {
		sb.WriteString(" age:")
		sb.WriteString(mono.Since(wi.started).String())
	}
}

func (wi *basewi) receiving() bool { return wi.recv.m != nil }

func (wi *basewi) cleanup(xdone bool) bool {
	if !wi.clean.CAS(false, true) {
		return false
	}

	r := wi.r
	if !wi.quiesce(xdone) {
		// TODO: unlikely(rxentry SGLs, rxStuck state) :TODO
		if !xdone {
			if _, loaded := r.pending.LoadAndDelete(wi.wid); loaded {
				r.pendingCnt.Dec()
			}
		}
		return false
	}

	if !xdone { // when xdone == true: full cleanup via parent's Range(r.cleanup)
		_, loaded := r.pending.LoadAndDelete(wi.wid)
		debug.Assertf(loaded, "wid=%q not found", wi.wid)
		if loaded {
			r.pendingCnt.Dec()
		}
	}

	tstats := core.T.StatsUpdater()
	tstats.Inc(stats.GetBatchCount)
	tstats.Add(stats.GetBatchObjCount, wi.stats.obj.cnt)
	tstats.Add(stats.GetBatchObjSize, wi.stats.obj.size)
	tstats.Add(stats.GetBatchFileCount, wi.stats.fil.cnt)
	tstats.Add(stats.GetBatchFileSize, wi.stats.fil.size)
	if wi.soft > 0 {
		tstats.Add(stats.GetBatchSoftErrCount, int64(wi.soft))
	}
	if wi.stats.wait > 0 {
		tstats.Add(stats.GetBatchRxWaitTotal, wi.stats.wait)
	}
	if wi.stats.ngfn > 0 {
		tstats.Add(stats.GetBatchCountGFN, int64(wi.stats.ngfn))
	}
	if wi.stats.errn > 0 {
		tstats.Add(stats.GetBatchErrCountGFN, int64(wi.stats.errn))
	}

	// TODO: core.Xact to count archived files separately
	wi.r.ObjsAdd(int(wi.stats.obj.cnt+wi.stats.fil.cnt), wi.stats.obj.size+wi.stats.fil.size)

	if wi.aw != nil && wi.awfin.CAS(false, true) {
		err := wi.aw.Fini()
		wi.aw = nil
		if err != nil {
			if cmn.Rom.V(5, cos.ModXs) {
				nlog.Warningln(r.Name(), core.T.String(), "cleanup: err fini()", wi.wid, err)
			}
		}
	}

	if wi.resp != nil {
		clear(wi.resp.Out)
		wi.resp.Out = nil
	}

	if !wi.receiving() {
		return true
	}

	if !wi.req.StreamingGet && wi.sgl != nil { // wi.sgl nil upon early term (e.g. invalid bucket)
		wi.sgl.Free()
		wi.sgl = nil
	}

	wi.recv.mtx.Lock()
	for i := range wi.recv.m {
		entry := &wi.recv.m[i]
		if entry.sgl != nil {
			entry.sgl.Free()
			entry.sgl = nil
		}
	}
	clear(wi.recv.m)
	wi.recv.m = nil
	wi.recv.mtx.Unlock()

	// finally
	if wi.timer != nil {
		if !wi.timer.Stop() {
			select {
			case <-wi.timer.C:
			default:
			}
		}
		wi.timer = nil
	}

	return true
}

//
// work-item quiescence
//

// wait for inRx to drain and remain zero for noRxWindow
// brief mode is an exception: best-effort, no stability gate
func (wi *basewi) quiesce(brief bool) bool {
	if brief {
		return wi._quiesce(true)
	}

	started := mono.NanoTime()

	for {
		if !wi._quiesce(false) {
			return false // inner timed out
		}

		now := mono.NanoTime()
		if time.Duration(now-wi.lastRx.Load()) >= noRxWindow {
			return true // ok
		}
		if dur := time.Duration(now - started); dur >= inRxTimeout {
			err := fmt.Errorf("wi.quiesce timed out (stable) [%s, %q, %v > %v timeout]", wi.r.Name(), wi.wid, dur, inRxTimeout)
			wi.r.AddErr(err, 0)
			return false
		}
		time.Sleep(noRxWindow / 4)
	}
}

func (wi *basewi) _quiesce(brief bool) bool {
	const numSpins = 10

	r := wi.r
	var started int64
	for spins := 0; wi.inRx.Load() > 0; spins++ {
		if brief {
			if spins >= numSpins {
				err := fmt.Errorf("wi.quiesce: inRx still busy on brief cleanup [%s, %q, spins=%d > %d limit]",
					r.Name(), wi.wid, spins, numSpins)
				r.AddErr(err, 0)
				return false
			}
			time.Sleep(min(time.Second, time.Millisecond*time.Duration(spins)))
			continue
		}
		switch {
		case spins < numSpins:
			runtime.Gosched()
		case spins == numSpins:
			started = mono.NanoTime()
			fallthrough
		case spins < 10*numSpins:
			time.Sleep(min(time.Second, time.Millisecond*time.Duration(spins)))
		default:
			debug.Assert(started != 0)
			if dur := mono.Since(started); dur >= inRxTimeout {
				err := fmt.Errorf("wi.quiesce timed out waiting for inRx to drain [%s, %q, %v >= %v timeout]",
					r.Name(), wi.wid, dur, inRxTimeout)
				r.AddErr(err, 0)
				return false
			}
			time.Sleep(time.Second)
		}
	}

	return true // ok
}

// receive work item at [index]
// (note: ObjHdr and its fields must be consumed synchronously)
func (wi *basewi) recvObj(index int, hdr *transport.ObjHdr, reader io.Reader, mopaque *mossOpaque) (err error) {
	var (
		sgl  *memsys.SGL
		size int64
		r    = wi.r
	)
	if hdr.IsHeaderOnly() {
		debug.Assert(hdr.ObjAttrs.Size == 0, hdr.ObjName, " size: ", hdr.ObjAttrs.Size)
		goto add
	}

	sgl = r.gmm.NewSGL(0)
	size, err = io.Copy(sgl, reader)

	if err != nil {
		sgl.Free()
		err = fmt.Errorf("%s %s failed to receive %s from %s: %w", r.Name(), core.T.String(), hdr.ObjName, meta.Tname(hdr.SID), err)
		nlog.Warningln(err)
		return err
	}
	debug.Assert(size == sgl.Len(), size, " vs ", sgl.Len())

add:
	var added, freegl bool

	wi.recv.mtx.Lock()
	added, freegl, err = wi._recvObj(index, hdr, mopaque, sgl)
	wi.recv.mtx.Unlock()

	if freegl {
		sgl.Free()
	}
	if added {
		wi.recv.ch <- index
	}
	if err == nil {
		if cmn.Rom.SuperV(5, cos.ModXs) {
			nlog.Infoln(r.Cname(), core.T.String(), "Rx [ wid:", wi.wid, "index:", index, "oname:", hdr.ObjName, "size:", size, "]")
		}
		return nil
	}

	if cmn.Rom.V(4, cos.ModXs) {
		nlog.Warningln(r.Name(), core.T.String(), "Rx error [ wid:", wi.wid, "index:", index, "oname:", hdr.ObjName, "err:", err, "]")
	}
	return err
}

// under receive mutex
func (wi *basewi) _recvObj(index int, hdr *transport.ObjHdr, mopaque *mossOpaque, sgl *memsys.SGL) (added, freegl bool, _ error) {
	r := wi.r
	if index < 0 || index >= len(wi.recv.m) {
		if r.IsAborted() || r.IsDone() {
			return false, sgl != nil, nil
		}
		return false, sgl != nil, fmt.Errorf("%s %s out-of-bounds index %d (recv'd len=%d, wid=%q)",
			r.Name(), core.T.String(), index, len(wi.recv.m), wi.wid)
	}

	// two rare conditions
	entry := &wi.recv.m[index]
	if entry.isLocal() {
		// gfn
		return false, sgl != nil, nil
	}
	if !entry.isEmpty() {
		nlog.Warningf("%s %s duplicate recv idx=%d from %s — dropping", r.Name(), core.T.String(), index, meta.Tname(hdr.SID))
		return false, sgl != nil, nil
	}

	wi.recv.m[index] = rxentry{
		sgl:        sgl,
		bucket:     cos.StrDup(hdr.Bck.Name),
		nameInArch: cos.StrDup(hdr.ObjName),
		mopaque:    mopaque,
	}
	return true, false, nil
}

func (wi *basewi) waitFlushRx(i int) (int, error) {
	var (
		elapsed        time.Duration
		recvDrainBurst = cos.ClampInt(8, 1, cap(wi.recv.ch)>>1)
		timeout        = wi.config.GetBatch.MaxWait.D()
		pollIval       = cos.ClampDuration(timeout/pollDiv, cmn.GetBatchWaitMin, streamingWriteTimeout>>1)
		lastDeadline   int64
	)
	const refreshIval = streamingWriteTimeout / 3
	for {
		if err := wi.termErr(); err != nil {
			return 0, err
		}
		// refresh response-controller (streaming) deadline
		if wi.wctrl != nil {
			if now := mono.NanoTime(); now-lastDeadline >= int64(refreshIval) {
				erd := wi.wctrl.SetWriteDeadline(time.Now().Add(streamingWriteTimeout))
				debug.AssertNoErr(erd)
				lastDeadline = now
			}
		}

		wi.recv.mtx.Lock()
		err := wi.flushRx()
		next := wi.recv.next
		wi.recv.mtx.Unlock()

		if err != nil {
			return 0, err
		}
		if next > i {
			wi.stats.wait += elapsed.Nanoseconds()
			return next, nil
		}

		var received bool
	inner:
		for range recvDrainBurst {
			select {
			case _, ok := <-wi.recv.ch:
				if !ok {
					return 0, errMossRecvStopped
				}
				received = true
			default:
				break inner
			}
		}
		if received {
			continue
		}

		if err := bundle.SDM.CanSend(wi.sid); err != nil {
			return 0, wi.newErrHole(0, err)
		}

		rem := min(pollIval, timeout-elapsed)
		debug.Assert(rem > 0)
		switch err := wi.waitAnyRx(rem); err {
		case nil:
			continue
		case errMossRecvStopped, errMossXactStopped:
			return 0, err
		default:
			debug.Assert(err == context.DeadlineExceeded)
		}
		if err := wi.termErr(); err != nil {
			return 0, err
		}

		// TODO: not a true wall-clock elapsed time
		elapsed += pollIval
		// timeout
		if elapsed >= timeout {
			return 0, wi.newErrHole(timeout, nil)
		}
	}
}

// in parallel with RecvObj() inserting new entries..
func (wi *basewi) waitAnyRx(timeout time.Duration) error {
	wi.timer.Reset(timeout)
	select {
	case _, ok := <-wi.recv.ch:
		if !ok {
			return errMossRecvStopped
		}
		if !wi.timer.Stop() {
			select {
			case <-wi.timer.C:
			default:
			}
		}
		return nil
	case <-wi.timer.C:
		return context.DeadlineExceeded
	case <-wi.r.ChanAbort():
		wi.timer.Stop()
		return errMossXactStopped
	}
}

// to indicate a missing entry at a given index position
// (either timeout or network error during retrieval)
func (wi *basewi) newErrHole(total time.Duration, err error) error {
	index := wi.recv.next
	if err == nil {
		s := fmt.Sprintf("%s: timed out waiting for missing entry at index %d [ %s, wid=%q, total-wait=%v ]",
			wi.r.Name(), index, core.T.String(), wi.wid, total)
		return &errHole{s}
	}
	s := fmt.Sprintf("%s: network error while retrieving entry at index %d [ %s, wid=%q, err=%v ]",
		wi.r.Name(), index, core.T.String(), wi.wid, err)
	return &errHole{s}
}

func (wi *basewi) next(i int) (int, error) {
	var (
		r  = wi.r
		in = &wi.req.In[i]
	)
	lom, tsi, err := r._lom(in, wi.smap)
	if err != nil {
		return 0, err
	}

	if tsi != nil {
		debug.Assertf(wi.receiving(),
			"%s: unexpected non-local %s when _not_ receiving [%s, %s]", r.Name(), lom.Cname(), wi.wid, wi.smap)

		// set current sender
		wi.sid = tsi.ID()

		// prior to starting to wait on Rx:
		// best-effort non-blocking (look-ahead + `bewarm`)
		if r.bewarm != nil && !r.bewarm.IsBusy() {
			for j := i + 1; j < len(wi.req.In); j++ {
				inJ := &wi.req.In[j]
				lomJ, tsiJ, errJ := r._lom(inJ, wi.smap)
				if errJ == nil && tsiJ == nil {
					// note: breaking regardless
					// of whether TrySubmit returns true or false
					r.bewarm.TrySubmit(lomJ.FQN)
					break
				}
			}
		}

		var nextIdx int
		nextIdx, err = wi.waitFlushRx(i)

		// handle(err)
		switch {
		case err == nil:
			return nextIdx, nil
		case transport.IsErrSBR(err):
			if !wi.req.ContinueOnErr {
				return 0, err
			}
			out, nameInArch := wi._out(lom, in)
			if e := wi.addMissingIn(err, in, out, nameInArch); e != nil {
				return 0, e
			}
			return i + 1, nil
		case !isErrHole(err):
			return nextIdx, err
		default:
			if wi.config.GetBatch.IsDisabledGFN() || wi.stats.ngfn >= wi.config.GetBatch.MaxGFN {
				return nextIdx, err
			}
		}
	}

	out, nameInArch := wi._out(lom, in)
	if isErrHole(err) {
		if errClu := wi._checkSameTargets(); errClu != nil {
			return 0, errClu
		}

		wi.stats.ngfn++
		err = wi.gfn(lom, tsi, in, out, nameInArch) // direct GFN
	} else {
		err = wi.write(lom, in, out, nameInArch)
	}
	if err != nil {
		return 0, err
	}

	if wi.req.StreamingGet {
		// unlike waitFlushRx(), the local path does not re-set streamingWriteTimeout
		// while processing; long no-progress gaps here are treated as a stuck
		// client
		if err := wi.aw.Flush(); err != nil {
			return 0, err
		}
	} else {
		wi.resp.Out = append(wi.resp.Out, *out)
	}

	if wi.receiving() {
		wi.recv.mtx.Lock()
		entry := &wi.recv.m[i]
		entry.local = true
		debug.Assert(wi.recv.next <= i, wi.recv.next, " vs ", i) // must be contiguous
		wi.recv.mtx.Unlock()
	}
	return i + 1, nil
}

func (wi *basewi) _checkSameTargets() error {
	smapCurr := core.T.Sowner().Get()
	return wi.smap.CheckSameTargets(smapCurr, wi.r.Name())
}

func (wi *basewi) _out(lom *core.LOM, in *apc.MossIn) (*apc.MossOut, string) {
	bck := lom.Bck()
	out := apc.MossOut{
		Bucket:   bck.Name,
		Provider: bck.Provider,
		ObjName:  in.ObjName,
		ArchPath: in.ArchPath,
		Opaque:   in.Opaque,
	}
	nameInArch := in.NameInRespArch(bck.Name, wi.req.OnlyObjName)
	return &out, nameInArch
}

func (wi *basewi) gfn(lom *core.LOM, tsi *meta.Snode, in *apc.MossIn, out *apc.MossOut, nameInArch string) error {
	debug.Assert(tsi != nil)
	params := &core.GfnParams{
		Lom:      lom,
		Tsi:      tsi,
		ArchPath: in.ArchPath,
		Size:     wi.avgSize(),
		// give it a double-MaxWait, but do not inherit the generic data-client long timeout
		Timeout: wi.config.GetBatch.MaxWait.D() << 1,
	}

	resp, err := core.T.GetFromNeighbor(params) //nolint:bodyclose // closed below

	if err != nil {
		if cmn.Rom.V(4, cos.ModXs) {
			nlog.Warningln("GFN failure:", wi.r.Name(), wi.wid, nameInArch, err)
		}
		wi.stats.errn++
		if errClu := wi._checkSameTargets(); errClu != nil {
			return errClu
		}
		if !wi.req.ContinueOnErr {
			return err // instead of the original errHole
		}
		if !cmn.IsErrHTTPNotFound(err) {
			return err
		}
		return wi.addMissingIn(err, in, out, nameInArch)
	}

	defer cos.Close(resp.Body)

	if cmn.Rom.V(4, cos.ModXs) {
		nlog.Infoln(wi.r.Cname(), wi.wid, "GFN ok:", nameInArch)
	}
	if in.ArchPath == "" {
		oah := cos.SimpleOAH{Size: resp.ContentLength}
		err = wi._txreg(oah, resp.Body, out, nameInArch)
	} else {
		debug.Assert(resp.ContentLength >= 0, "GFN(arch): negative Content-Length for ", lom.Cname()+"/"+in.ArchPath)
		nameInArch = _withArchpath(nameInArch, in.ArchPath)
		err = wi._txarch(resp.Body, out, nameInArch, resp.ContentLength)
	}
	if err != nil {
		cos.DrainReader(resp.Body)
		return err
	}

	if resp.ContentLength > 0 {
		wi.updStats(in, resp.ContentLength)
	}
	return nil
}

func (wi *basewi) avgSize() (size int64) {
	cnt := len(wi.resp.Out)
	if cnt == 0 {
		return 0
	}
	for i := range wi.resp.Out {
		size += wi.resp.Out[i].Size
	}
	return size / int64(cnt)
}

func (wi *basewi) updStats(in *apc.MossIn, size int64) {
	if in.ArchPath == "" {
		wi.stats.obj.cnt++
		wi.stats.obj.size += size
	} else {
		wi.stats.fil.cnt++
		wi.stats.fil.size += size
	}
}

func (wi *basewi) write(lom *core.LOM, in *apc.MossIn, out *apc.MossOut, nameInArch string) error {
	lom.Lock(false)
	lh, rcs, size, err := wi._write(lom, in, out, nameInArch)
	lom.Unlock(false)

	if rcs != nil {
		cos.Close(rcs)
	}
	if lh != nil {
		cos.Close(lh)
	}

	if err == nil && size > 0 {
		wi.updStats(in, size)
	}
	return err
}

// (under rlock)
func (wi *basewi) _write(lom *core.LOM, in *apc.MossIn, out *apc.MossOut, nameInArch string) (cos.LomReader, cos.ReadCloseSizer, int64, error) {
	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		if cos.IsNotExist(err) && wi.req.ContinueOnErr {
			err = wi.addMissing(err, nameInArch, out)
		}
		return nil, nil, 0, err
	}

	lh, err := lom.Open()
	if err != nil {
		if cos.IsNotExist(err) && wi.req.ContinueOnErr {
			err = wi.addMissing(err, nameInArch, out)
		}
		return nil, nil, 0, err
	}

	var (
		reader io.Reader = lh
		oah    cos.OAH   = lom
		rcs    cos.ReadCloseSizer
		size   = lom.Lsize() // (may change below)
	)

	// archived file
	if in.ArchPath != "" {
		nameInArch = _withArchpath(nameInArch, in.ArchPath)

		// note: use shard index (fast path) if available
		rcs, err = lom.NewArchpathReader(lh, in.ArchPath, "" /*mime*/)
		if err != nil {
			if cos.IsNotExist(err) && wi.req.ContinueOnErr {
				err = wi.addMissing(err, nameInArch, out)
			}
			return lh, nil, 0, err
		}

		size = rcs.Size()
		reader = rcs
		oah = cos.SimpleOAH{Size: size}
	}

	off, length, errRng := in.CheckRange(size)
	if errRng != nil {
		return lh, rcs, 0, errRng
	}

	// range
	if length != size {
		size = length
		oah = cos.SimpleOAH{Size: size}

		if in.ArchPath == "" {
			// over plain object
			reader, err = lom.NewSectionReader(lh, off, size)
		} else {
			// over archived file
			var rcs2 cos.ReadCloseSizer
			rcs2, err = archive.RangeReader(rcs, off, size)
			if err == nil {
				reader, rcs = rcs2, rcs2
			} else {
				rcs = nil // already closed
			}
		}
		if err != nil {
			return lh, rcs, size, err
		}
	}

	err = wi._txreg(oah, reader, out, nameInArch)
	return lh, rcs, size, err
}

func (wi *basewi) _txreg(oah cos.OAH, reader io.Reader, out *apc.MossOut, nameInArch string) error {
	if err := wi.aw.Write(nameInArch, oah, reader); err != nil {
		return err
	}
	out.Size = oah.Lsize()
	return nil
}

func (wi *basewi) _txarch(reader io.Reader, out *apc.MossOut, nameInArch string, size int64) error {
	oah := cos.SimpleOAH{Size: size}
	if err := wi.aw.Write(nameInArch, &oah, reader); err != nil {
		return err
	}
	out.Size = oah.Size
	return nil
}

func (wi *basewi) addMissingIn(err error, in *apc.MossIn, out *apc.MossOut, nameInArch string) error {
	if in.ArchPath != "" {
		nameInArch = _withArchpath(nameInArch, in.ArchPath)
	}
	return wi.addMissing(err, nameInArch, out)
}

func _withArchpath(nameInArch, archpath string) string {
	if archpath[0] == '/' {
		return nameInArch + archpath
	}
	return nameInArch + cos.PathSeparator + archpath
}

func (wi *basewi) addMissing(err error, nameInArch string, out *apc.MossOut) error {
	var (
		missingName = apc.MossMissingDir + cos.PathSeparator + nameInArch
		oah         = cos.SimpleOAH{Size: 0}
		roc         = nopROC{}
	)
	if err := wi.aw.Write(missingName, oah, roc); err != nil {
		return err
	}

	out.ErrMsg = err.Error()

	// soft err-s include a), b), and c) GFN
	// see cmn/config comment
	wi.soft++
	return nil
}

func (wi *basewi) _errSoft(limit int) error {
	return fmt.Errorf("%s: wid=%q exceeded soft-errors limit: %d (max: %d)", wi.r.Name(), wi.wid, wi.soft, limit)
}

func (wi *basewi) errAbandoned() error {
	err := wi.abandoned.Load()
	if err == nil {
		return nil // to return error(nil)
	}
	return err // errAbandonedWI
}

func (wi *basewi) termErr() error {
	if wi.r.IsAborted() || wi.r.IsDone() {
		return errMossXactStopped
	}
	return wi.errAbandoned()
}

func (wi *basewi) asm() error {
	var (
		adv       = newAdvice(wi.config)
		l         = len(wi.req.In)
		started   = mono.NanoTime()
		nextFlush = started + int64(streamingFlushIval)
		sendersOk bool
	)
	for i := 0; i < l; {
		if err := wi.termErr(); err != nil {
			if err == errMossXactStopped {
				err = nil
			}
			return err
		}
		if limit := wi.config.GetBatch.MaxSoftErrs; wi.soft > limit {
			return wi._errSoft(limit)
		}
		j, err := wi.next(i)
		if err != nil {
			if err == errMossXactStopped {
				return nil
			}
			return err
		}
		wi.idx = j

		now := mono.NanoTime()
		wi.lastAsm.Store(now)

		if wi.wctrl != nil {
			if now >= nextFlush {
				// note:
				// set streaming write deadline; waitFlushRx()
				// refreshes it while blocked waiting for peer data
				erd := wi.wctrl.SetWriteDeadline(time.Now().Add(streamingWriteTimeout))
				debug.AssertNoErr(erd)
				if err := wi.wctrl.Flush(); err != nil {
					return err
				}
				nextFlush = now + int64(streamingFlushIval)
			}
		}

		if !sendersOk {
			sendersOk, err = wi._allStarted(time.Duration(now - started))
			if err != nil {
				return err // (req.ContinueOnErr does not apply)
			}
		}

		debug.Assert(j > i && j <= l, i, " vs ", j, " vs ", l)
		i = j

		if adv.ShouldCheck(int64(i)) {
			adv.Refresh()
			if adv.DskLoad() >= load.High {
				debug.Assert(adv.Sleep > 0)
				time.Sleep(adv.Sleep)
			}
		}
	}
	return nil
}

// all expected senders must announce themselves within the sendersStartupFail interval
func (wi *basewi) _allStarted(since time.Duration) (ok bool, err error) {
	const fmtErr = "%s: %d of %d senders did not ack start within %v"
	var (
		cnt      = wi.startup.refc.Load()
		expected = wi.startup.nat - 1
	)
	switch {
	case expected <= 0:
		return true, nil // not receiving
	case cnt >= expected:
		if cnt > expected {
			nlog.Warningln(wi.r.Name(), "extra startup acks:", cnt, "expected:", expected)
		}
		return true, nil // ok
	case since < sendersStartupWarn:
		// do nothing
	case since >= sendersStartupFail:
		return false, fmt.Errorf(fmtErr, wi.r.Name(), expected-cnt, expected, sendersStartupFail)
	case since-wi.startup.warn >= sendersStartupWarn:
		nlog.Warningf(fmtErr, wi.r.Name(), expected-cnt, expected, since)
		wi.startup.warn = since
	}
	return false, nil
}

// drains recv.m[] in strict input order
// is called under `recv.mtx` lock

func (entry *rxentry) isLocal() bool { return entry.local }
func (entry *rxentry) isEmpty() bool { return entry.nameInArch == "" }

func (wi *basewi) flushRx() error {
	for l := len(wi.recv.m); wi.recv.next < l; {
		var (
			err   error
			size  int64
			index = wi.recv.next
			entry = &wi.recv.m[index]
			in    = &wi.req.In[index]
		)
		if entry.isLocal() {
			wi.recv.next++
			continue
		}
		if entry.isEmpty() {
			debug.Assert(entry.mopaque == nil)
			return nil
		}
		if entry.mopaque.missing {
			wi.soft++
			if limit := wi.config.GetBatch.MaxSoftErrs; wi.soft > limit {
				return wi._errSoft(limit)
			}
		}

		wi.recv.mtx.Unlock() //--------------

		switch {
		case entry.mopaque.missing:
			debug.Assert(strings.HasPrefix(entry.nameInArch, apc.MossMissingDir+"/"), entry.nameInArch)
			err = wi.aw.Write(entry.nameInArch, cos.SimpleOAH{Size: 0}, nopROC{})
		default:
			debug.Assert(entry.mopaque.emsg == "", entry.mopaque.emsg)
			size = entry.sgl.Len()
			oah := cos.SimpleOAH{Size: size}
			err = wi.aw.Write(entry.nameInArch, oah, entry.sgl)
		}
		if err == nil && wi.req.StreamingGet {
			err = wi.aw.Flush()
		}
		wi.recv.mtx.Lock() //--------------

		if err != nil {
			return err
		}

		if !wi.req.StreamingGet {
			out := apc.MossOut{
				Bucket:   entry.bucket,
				Provider: in.Provider, // (just copying)
				ObjName:  entry.mopaque.oname,
				Size:     size,
				Opaque:   in.Opaque, // as is (see apc/ml definition)
				ErrMsg:   entry.mopaque.emsg,
			}
			wi.resp.Out = append(wi.resp.Out, out)
		}

		// this sgl is done - can free it early (see related wi.cleanup())
		if entry.sgl != nil {
			entry.sgl.Free()
			entry.sgl = nil
		}

		// this "hole" is plugged
		wi.recv.next++
		if size > 0 {
			wi.updStats(in, size)
		}
	}
	return nil
}

////////////
// buffwi //
////////////

func (wi *buffwi) asm(w http.ResponseWriter) error {
	debug.Assert(!wi.req.StreamingGet)
	if err := wi.basewi.asm(); err != nil {
		return err
	}

	// flush and close aw
	if wi.awfin.CAS(false, true) {
		err := wi.aw.Fini()
		wi.aw = nil
		if err != nil {
			return err
		}
	}

	// write multipart response
	mpw := multipart.NewWriter(w)
	w.Header().Set(cos.HdrContentType, "multipart/mixed; boundary="+mpw.Boundary())

	_, erw := wi.multipart(mpw, wi.resp)
	if err := mpw.Close(); err != nil && erw == nil {
		erw = err
	}
	if erw != nil {
		nlog.Warningln(wi.r.Name(), cmn.ErrGetTxBenign, "[", erw, "]")
		return cmn.ErrGetTxBenign
	}

	wi.sgl.Reset()
	return nil
}

func (wi *buffwi) multipart(mpw *multipart.Writer, resp *apc.MossResp) (int64, error) {
	// part 1: JSON metadata
	part1, err := mpw.CreateFormField(apc.MossMetaPart)
	if err != nil {
		return 0, err
	}
	if err := jsoniter.NewEncoder(part1).Encode(resp); err != nil {
		return 0, err
	}

	// part 2: archive (e.g. TAR) data
	part2, err := mpw.CreateFormFile(apc.MossDataPart, wi.r.Cname())
	if err != nil {
		return 0, err
	}
	return io.Copy(part2, wi.sgl)
}

//////////////
// streamwi //
//////////////

func (wi *streamwi) asm(w http.ResponseWriter) error {
	debug.Assert(wi.req.StreamingGet)

	wi.wctrl = http.NewResponseController(w)
	w.Header().Set(cos.HdrContentType, archive.ContentTypeFromExt(wi.req.OutputFormat))

	erd := wi.wctrl.SetWriteDeadline(time.Now().Add(streamingWriteTimeout))
	debug.AssertNoErr(erd)
	if err := wi.wctrl.Flush(); err != nil {
		nlog.Warningln(wi.r.Name(), "client gone before first write:", err)
		return cmn.ErrGetTxBenign
	}

	if err := wi.basewi.asm(); err != nil {
		return cmn.ErrGetTxBenign
	}

	// flush and close aw
	if wi.awfin.CAS(false, true) {
		err := wi.aw.Fini()
		wi.aw = nil
		if err != nil {
			nlog.Warningln(wi.r.Name(), cmn.ErrGetTxBenign, "[", err, "]")
			return cmn.ErrGetTxBenign
		}
	}

	return nil
}

////////////
// nopROC //
////////////

type nopROC struct{}

// interface guard
var (
	_ cos.ReadOpenCloser = (*nopROC)(nil)
)

func (nopROC) Read([]byte) (int, error)          { return 0, io.EOF }
func (nopROC) Open() (cos.ReadOpenCloser, error) { return &nopROC{}, nil }
func (nopROC) Close() error                      { return nil }

/////////////////
// mossOpaque (not to confuse with apc.MossIn/Out Opaque)
/////////////////

type mossOpaque struct {
	wid     string
	oname   string
	emsg    string
	index   int32
	missing bool
}

// interface guard
var (
	_ cos.Packer   = (*mossOpaque)(nil)
	_ cos.Unpacker = (*mossOpaque)(nil)
)

func (o *mossOpaque) Pack(packer *cos.BytePack) {
	packer.WriteString(o.wid)
	packer.WriteString(o.oname)
	packer.WriteString(o.emsg)
	packer.WriteInt32(o.index)
	packer.WriteBool(o.missing)
}

func (o *mossOpaque) PackedSize() int {
	return cos.PackedStrLen(o.wid) + cos.PackedStrLen(o.oname) + cos.PackedStrLen(o.emsg) + cos.SizeofI32 + 1
}

func (o *mossOpaque) Unpack(unpacker *cos.ByteUnpack) (err error) {
	if o.wid, err = unpacker.ReadString(); err != nil {
		return err
	}
	if o.oname, err = unpacker.ReadString(); err != nil {
		return err
	}
	if o.emsg, err = unpacker.ReadString(); err != nil {
		return err
	}
	if o.index, err = unpacker.ReadInt32(); err != nil {
		return err
	}
	o.missing, err = unpacker.ReadBool()
	return err
}

/////////////
// errHole //
/////////////

type errHole struct {
	msg string
}

func (e *errHole) Error() string { return e.msg }

func isErrHole(err error) bool {
	_, ok := err.(*errHole)
	return ok
}

////////////////////
// errAbandonedWI //
////////////////////

type errAbandonedWI struct {
	wid string
	age time.Duration
}

func (e *errAbandonedWI) Error() string {
	return fmt.Sprintf("wi %q abandoned (age %v)", e.wid, e.age)
}

func newErrAbandonedWI(wid string, age time.Duration) *errAbandonedWI {
	return &errAbandonedWI{wid: wid, age: age}
}

//
// load.Advice helpers
//

func newAdvice(config *cmn.Config) (adv load.Advice) {
	adv.Init(load.FlMem|load.FlCla|load.FlDsk, &load.Extra{Cfg: &config.Disk, RW: true})
	adv.Refresh()
	return
}

func advIval(l load.Load) time.Duration {
	switch l {
	case load.Critical:
		return sparseAdvice >> 3
	case load.High:
		return sparseAdvice >> 2
	default:
		return sparseAdvice
	}
}

//
// GC abandoned work items
//

const (
	gcAbandonedMaxIters = 16 // to limit the time spent in HK callback
)

const (
	wiownNone int32 = iota
	wiownAsm
	wiownGC
)

type (
	mossGC struct {
		r      *XactMoss
		now    int64
		cutoff int64
		iters  int
		warn1  bool
		warn2  bool
	}
)

func (gc *mossGC) do() {
	debug.Assert(gc.r != nil)
	gc.r.pending.Range(gc.visit)
}

func (gc *mossGC) recycled(wi *basewi) bool {
	return wi.wid == "" || wi.started == 0 || wi.lastAsm.Load() > gc.cutoff // skip it
}

func (gc *mossGC) visit(_, value any) bool {
	gc.iters++
	cont := gc.iters < gcAbandonedMaxIters

	wi := value.(*basewi)
	if gc.recycled(wi) {
		return cont
	}

	age := time.Duration(gc.now - wi.lastAsm.Load())
	if gc.mark(wi, age) {
		return cont
	}
	return gc.cleanup(wi, age, cont)
}

// pass 1: mark abandoned, stop new Rx/asm admissions
func (gc *mossGC) mark(wi *basewi, age time.Duration) bool {
	if wi.errAbandoned() != nil {
		return false
	}

	err := newErrAbandonedWI(wi.wid, age)
	if !wi.abandoned.CompareAndSwap(nil, err) {
		return false
	}
	if gc.recycled(wi) {
		wi.abandoned.CompareAndSwap(err, nil) // undo
		return false
	}

	r := gc.r
	r.AddErr(err)
	if !gc.warn1 || cmn.Rom.V(4, cos.ModXs) {
		nlog.Warningln(r.Name(), err)
		gc.warn1 = true
	}
	return true
}

// pass 2: cleanup
func (gc *mossGC) cleanup(wi *basewi, age time.Duration, cont bool) bool {
	r := gc.r

	if r.IsAborted() || r.IsDone() {
		return false // stop #1
	}
	if gc.recycled(wi) {
		return cont
	}

	// in-flight Rx => stuck Rx
	if wi.inRx.Load() > 0 {
		go r._fatal(wi.wid, "Rx", age)
		return false // stop #2
	}

	if !wi.owned.CAS(wiownNone, wiownGC) {
		if wi.clean.Load() {
			return cont
		}
		// asm still owns it => stuck asm
		go r._fatal(wi.wid, "asm", age)
		return false // stop #3
	}

	// gc claims ownership (no IncPending was done, so no DecPending owed)
	if gc.recycled(wi) {
		wi.owned.CAS(wiownGC, wiownNone)
		return cont
	}
	if !wi.cleanup(false) {
		// benign: asm cleaned/recycled after gc.Range observed it
		wi.owned.CAS(wiownGC, wiownNone)
		return cont
	}
	if !gc.warn2 || cmn.Rom.V(4, cos.ModXs) {
		nlog.Warningln(r.Name(), "GC cleaned abandoned wi", wi.wid, "age:", age)
		gc.warn2 = true
	}
	freeMossWi(wi)
	return cont
}

func (r *XactMoss) _fatal(wid, reason string, age time.Duration) {
	err := fmt.Errorf("wedged %s for wi %q, age %v", reason, wid, age)
	if r.Abort(err) {
		nlog.Errorln(r.Name(), err)
	}
}

// via xact.OpcodeAbortWI from DT
func (r *XactMoss) gcAbortedByDT(now int64) {
	last := r.lastAbrtWID.Load()
	if last == 0 || time.Duration(now-last) < gcWIsAbortedByDT {
		return
	}

	const iniCap = 8
	keys := make([]string, 0, iniCap)
	r.abortedWIDs.Range(func(k, _ any) bool {
		keys = append(keys, k.(string))
		return true
	})
	if !r.lastAbrtWID.CAS(last, 0) {
		return
	}
	for _, k := range keys {
		r.abortedWIDs.Delete(k)
	}
}
