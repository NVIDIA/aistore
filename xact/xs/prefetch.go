// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// TODO:
// - blob downloading (when msg.BlobThreshold > 0):
//   - configurable num concurrent x-blob
//   - configurable chunk-size and num-workers
//   - max-threshold that forces blob-downloading for, say, 5G objects and larger

type (
	prfFactory struct {
		xctn *prefetch
		msg  *apc.PrefetchMsg
		xreg.RenewBase
	}
	pebl struct {
		parent  *prefetch
		pending []core.Xact
		load    atomic.Int64
		n       atomic.Int32
		mu      sync.Mutex
	}
	prfStats struct {
		// cold-GET
		coldN    atomic.Int64 // cold-GET completions
		coldSize atomic.Int64 // --/-- size
		// pending
		blobN    atomic.Int64 // blob-downloader children accepted/spawned
		blobSize atomic.Int64 // bytes --/--
		blobRej  atomic.Int64 // // blob-downloader admission rejects (TooManyRequests)
		// pending
		peblSize atomic.Int64 // sum of oa.Size for currently pending blob-downloader children
	}
	prefetch struct {
		ctx    context.Context
		bp     core.Backend
		config *cmn.Config
		msg    *apc.PrefetchMsg
		xlabs  map[string]string
		brl    *cos.BurstRateLim
		pebl   pebl
		stats  prfStats
		lrit
		xact.Base
		latestVer bool
	}
)

// interface guard
var (
	_ core.Xact      = (*prefetch)(nil)
	_ xreg.Renewable = (*prfFactory)(nil)
	_ lrwi           = (*prefetch)(nil)
)

func (*prfFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	msg := args.Custom.(*apc.PrefetchMsg)
	debug.Assert(!msg.IsList() || !msg.HasTemplate())
	np := &prfFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, msg: msg}
	return np
}

func (p *prfFactory) Start() (err error) {
	if p.msg.BlobNumWorkers < xact.NwpNone {
		return fmt.Errorf("invalid blob-num-workers=%d: expecting (-1..N) range", p.msg.BlobNumWorkers)
	}
	if p.msg.BlobThreshold > 0 && p.msg.BlobThreshold < minBlobDlPrefetch {
		a, b := cos.IEC(p.msg.BlobThreshold, 0), cos.IEC(minBlobDlPrefetch, 0)
		nlog.Warningln("blob-threshold (", a, ") is too small, must be at least", b, "- updating...")
		p.msg.BlobThreshold = minBlobDlPrefetch
	}

	b := p.Bck
	if err := b.Init(core.T.Bowner()); err != nil {
		return err
	}
	if !b.IsCloud() && !b.IsRemoteAIS() {
		return fmt.Errorf("can only prefetch Cloud and remote AIS buckets (have %s)", b.Cname(""))
	}
	p.xctn, err = newPrefetch(&p.Args, p.Kind(), b, p.msg)
	return err
}

func (*prfFactory) Kind() string     { return apc.ActPrefetchObjects }
func (p *prfFactory) Get() core.Xact { return p.xctn }

func (*prfFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprKeepAndStartNew, nil
}

func newPrefetch(xargs *xreg.Args, kind string, bck *meta.Bck, msg *apc.PrefetchMsg) (r *prefetch, err error) {
	var lsflags uint64
	r = &prefetch{config: cmn.GCO.Get(), msg: msg}

	smap := core.T.Sowner().Get()
	nat := smap.CountActiveTs()
	r.brl, err = bck.NewFrontendRateLim(nat) // TODO: support RateLimitConf.Verbs - here and elsewhere; `nat` vs num-workers
	if err != nil {
		return nil, err
	}

	if msg.NonRecurs {
		lsflags = apc.LsNoRecursion
	}
	err = r.lrit.init(r, &msg.ListRange, bck, lsflags, msg.NumWorkers, 0 /*burst*/)
	if err != nil {
		return nil, err
	}
	r.InitBase(xargs.UUID, kind, bck)
	r.latestVer = bck.VersionConf().ValidateWarmGet || msg.LatestVer

	r.bp = core.T.Backend(bck)
	r.xlabs = map[string]string{
		stats.VlabBucket: bck.Cname(""),
		stats.VlabXkind:  r.Kind(),
	}
	r.ctx = xact.NewCtxVlabs(r.xlabs)

	if r.msg.BlobThreshold > 0 {
		r.pebl.init(r)
	}
	return r, nil
}

func (r *prefetch) Run(wg *sync.WaitGroup) {
	nlog.Infoln(r.Name())

	wg.Done()

	err := r.lrit.run(r, core.T.Sowner().Get(), false /*prealloc buf*/)
	if err != nil {
		r.AddErr(err, 5, cos.ModXs) // duplicated?
	}
	r.lrit.wait()

	// pending blob-downloads
	if r.pebl.num() > 0 {
		if r.IsAborted() {
			r.pebl.abort(r.AbortErr())
		} else {
			r.pebl.wait()
		}
	}

	r.Finish()
}

// NOTE ref 6735188: _not_ setting negative atime, flushing lom metadata
func (r *prefetch) do(lom *core.LOM, lrit *lrit, _ []byte) {
	var (
		err     error
		oa      *cmn.ObjAttrs
		size    int64
		ecode   int
		deleted bool
	)

	lom.Lock(false)
	oa, deleted, err = lom.LoadLatest(r.latestVer || r.msg.BlobThreshold > 0) // shortcut to find size
	lom.Unlock(false)

	// handle assorted returns
	switch {
	case deleted: // remotely
		debug.Assert(r.latestVer && err != nil)
		if lrit.lrp != lrpList {
			return // deleted or not found remotely, prefix or range
		}
		r.AddErr(err, 5, cos.ModXs)
		r.errStats()
		return
	case oa != nil:
		// not latest
		size = oa.Size
	case err == nil:
		return // nothing to do
	case !cmn.IsErrObjNought(err):
		r.AddErr(err, 5, cos.ModXs)
		r.errStats()
		return
	}

	// apply frontend rate-limit, if any
	if r.brl != nil {
		r.brl.RetryAcquire(time.Second)
	}
	if r.msg.BlobThreshold > 0 && size >= r.msg.BlobThreshold && !r.pebl.busy() {
		ecode, err = r.blobdl(lom, oa)
	} else {
		if r.msg.BlobThreshold == 0 && size > cos.GiB {
			r._whinge(lom, size)
		}
		ecode, err = r.getCold(lom)
	}

	if err == nil {
		if cmn.Rom.V(5, cos.ModXs) {
			nlog.Infoln(r.Name(), lom.Cname())
		}
		return
	}

	lom.UncacheDel()
	switch {
	case cos.IsNotExist(err, ecode) || cmn.IsErrBusy(err) || err == cmn.ErrSkip:
		if lrit.lrp == lrpList {
			r.AddErr(err, 5, cos.ModXs)
		}
	case cos.IsErrOOS(err):
		r.Abort(err)
		r.errStats()
	default:
		r.AddErr(err, 5, cos.ModXs)
		r.errStats()
	}
}

func (r *prefetch) _whinge(lom *core.LOM, size int64) {
	var sb cos.SB
	sb.Init(ctlMsgBufSize)
	sb.WriteString(r.Name())
	sb.WriteString(": prefetching large size ")
	sb.WriteString(cos.IEC(size, 1))
	sb.WriteString(" with blob-downloading disabled [")
	sb.WriteString(lom.Cname())
	sb.WriteUint8(']')
	if size >= 5*cos.GiB {
		nlog.Errorln(sb.String())
	} else {
		nlog.Warningln(sb.String())
	}
}

// OwtGetPrefetchLock: minimal locking, optimistic concurrency
// - light-weight alternative to t.GetCold impl.
// - rate limited via ais/rlbackend, if defined
func (r *prefetch) getCold(lom *core.LOM) (ecode int, err error) {
	started := mono.NanoTime()

	// either a) LoadLatest => UncacheDel (not-latest) or b) not-found
	debug.Assert(lom.GetCustomMD() == nil, lom.Cname())

	if ecode, err = r.bp.GetObj(r.ctx, lom, cmn.OwtGetPrefetchLock, nil /*origReq*/); err != nil {
		return ecode, err
	}

	// RGET stats (compare with ais/tgtimpl namesake)
	size := lom.Lsize()
	rgetstats(r.bp, r.xlabs, size, started)

	// own stats
	r.ObjsAdd(1, size)
	r.stats.coldN.Inc()
	r.stats.coldSize.Add(size)
	r.coldStats(size, started)

	return 0, nil
}

func (r *prefetch) Snap() (snap *core.Snap) {
	snap = r.Base.NewSnap(r)
	snap.Pack(0, len(r.lrit.nwp.workers), r.lrit.nwp.chanFull.Load())
	return
}

//
// async, via blob-downloader --------------------------
//

func (r *prefetch) blobdl(lom *core.LOM, oa *cmn.ObjAttrs) (int, error) {
	// pass user preferences through; blobFactory.Start tunes them once
	params := &core.BlobParams{
		Lom: core.AllocLOM(lom.ObjName),
		Msg: &apc.BlobMsg{
			ChunkSize:  r.msg.BlobChunkSize,
			NumWorkers: r.msg.BlobNumWorkers,
		},
		Parent: xact.Cname("prefetch", r.ID()),
	}
	if err := params.Lom.InitBck(lom.Bck()); err != nil {
		return 0, err
	}
	xctn, err := core.T.GetColdBlob(params, oa)

	// error handling
	switch {
	case err == nil:
		// do nothing
	case cmn.IsErrTooManyRequests(err):
		// fall back to regular cold GET if blob download is rejected due to resource pressure
		r.stats.blobRej.Inc()
		r.blobRejStats()
		nlog.Warningln(r.Name(), ": blob download rejected due to resource pressure, falling back to regular cold GET, error: ", err)
		return r.getCold(lom)
	default:
		return 0, err
	}

	// account for the spawn
	r.stats.blobN.Inc()
	r.stats.blobSize.Add(oa.Size)
	r.stats.peblSize.Add(oa.Size)

	notif := &xact.NotifXact{
		Base: nl.Base{
			When: core.UponTerm,
			F:    r.pebl.done,
		},
	}
	notif.Xact = xctn
	xctn.AddNotif(notif)

	if xctn.IsDone() {
		r.stats.peblSize.Add(-oa.Size)
		r.ObjsAdd(1, oa.Size)
		r.blobStats(oa.Size)
		return 0, nil
	}
	r.pebl.add(xctn)
	return 0, nil
}

//////////
// pebl (pending blob downloads)
//////////

// max concurrent blob downloads by a given prefetch job
// works in conjunction with the current load average - see pebl.busy
const maxPebls = 32

const (
	peblSleep   = 4 * time.Second
	peblTimeout = 32 * time.Minute // must be >> 16s and be divisible by 16
)

func (pebl *pebl) init(parent *prefetch) {
	pebl.parent = parent
	pebl.pending = make([]core.Xact, 0, maxPebls)
}

func (pebl *pebl) add(xctn core.Xact) {
	pebl.mu.Lock()
	pebl.n.Inc()
	pebl.pending = append(pebl.pending, xctn)
	pebl.mu.Unlock()
}

func (pebl *pebl) done(nmsg core.Notif, err error, aborted bool) {
	var (
		xblob *XactBlobDl
		msg   = nmsg.ToNotifMsg(aborted)
		n     int32
	)
	pebl.mu.Lock()
	for _, xctn := range pebl.pending {
		// this one is "done" - remove from pending
		if xctn.ID() == msg.UUID {
			var ok bool
			xblob, ok = xctn.(*XactBlobDl)
			debug.Assert(ok)
			continue
		}
		// finished - remove as well
		if xctn.IsDone() {
			continue
		}
		// keep
		pebl.pending[n] = xctn
		n++
	}
	pebl.pending = pebl.pending[:n]
	pebl.n.Store(n)

	pebl.mu.Unlock()

	load, isExtreme := sys.CPU(false /*periodic*/)
	if isExtreme {
		load = 100
	}
	pebl.load.Store(load) // pebl.busy()

	if xblob == nil {
		return
	}

	// log and stats
	xname := pebl.parent.Name()
	switch {
	case aborted || err != nil:
		nlog.Warningln(xname, "::", xblob.String(), "[", msg.String(), err, "]")
		pebl.parent.AddErr(err)
		pebl.parent.stats.peblSize.Add(-xblob.Size())
		pebl.parent.errStats()
	default:
		pebl.parent.ObjsAdd(1, xblob.Size())
		pebl.parent.stats.peblSize.Add(-xblob.Size())
		pebl.parent.blobStats(xblob.Size())
		if xblob.Size() >= cos.GiB/2 || cmn.Rom.V(4, cos.ModXs) {
			if n > 0 {
				nlog.Infoln(xname, "::", xblob.String(), "( num-pending", strconv.Itoa(int(n)), ")")
			} else {
				nlog.Infoln(xname, "::", xblob.String())
			}
		}
	}
}

// when all non-blob prefetching already done
func (pebl *pebl) wait() {
	const waiting = "still waiting for blob downloads:"
	var (
		total time.Duration
		sleep = peblSleep
		xname = pebl.parent.Name()
	)
	for {
		time.Sleep(sleep)
		n := pebl.num()
		if n <= 0 {
			return
		}
		total += sleep
		switch total {
		case 15 * sleep:
			nlog.Warningln(xname, waiting, pebl.str())
			sleep <<= 1
		case 30 * sleep:
			nlog.Warningln(xname, waiting, pebl.str())
			sleep <<= 1
		case peblTimeout:
			err := fmt.Errorf("%d blob download%s timed-out: %s", n, cos.Plural(int(n)), pebl.str())
			nlog.Warningln(xname, err)
			pebl.parent.AddErr(err)
			return
		}
	}
}

func (pebl *pebl) abort(err error) {
	pebl.mu.Lock()
	for _, xctn := range pebl.pending {
		xctn.Abort(err)
	}
	pebl.mu.Unlock()
}

func (pebl *pebl) num() int32 { return pebl.n.Load() }

func (pebl *pebl) busy() bool {
	return pebl.n.Load() > maxPebls || pebl.load.Load() >= sys.HighLoadWM()
}

func (pebl *pebl) str() string {
	var sb cos.SB

	pebl.mu.Lock()
	n := int(pebl.num())
	sb.Init(max(ctlMsgBufSize, n*64))

	sb.WriteUint8('[')

	for i, xctn := range pebl.pending {
		sb.WriteString(xctn.Name())
		if i < n-1 {
			sb.WriteString("; ")
		}
	}
	pebl.mu.Unlock()

	sb.WriteUint8(']')
	return sb.String()
}

//
// Prometheus
//

func (r *prefetch) coldStats(size, started int64) {
	tstats := core.T.StatsUpdater()
	delta := mono.SinceNano(started)
	tstats.IncWith(stats.PrefetchColdCount, r.xlabs)
	tstats.AddWith(
		cos.NamedVal64{Name: stats.PrefetchColdSize, Value: size, VarLabs: r.xlabs},
		cos.NamedVal64{Name: stats.PrefetchColdLatencyTotal, Value: delta, VarLabs: r.xlabs},
	)
	// blob latency lives in XactBlobDl
}

func (r *prefetch) blobStats(size int64) {
	tstats := core.T.StatsUpdater()
	tstats.IncWith(stats.PrefetchBlobCount, r.xlabs)
	tstats.AddWith(
		cos.NamedVal64{Name: stats.PrefetchBlobSize, Value: size, VarLabs: r.xlabs},
	)
}

func (r *prefetch) blobRejStats() {
	core.T.StatsUpdater().IncWith(stats.PrefetchBlobRejCount, r.xlabs)
}

func (r *prefetch) errStats() {
	core.T.StatsUpdater().IncWith(stats.ErrPrefetchCount, r.xlabs)
}

//
// CtlMsg
//

func (r *prefetch) CtlMsg() string {
	if r.msg == nil {
		return ""
	}

	var sb cos.SB
	sb.Init(ctlMsgBufSize)
	r._ctlMsg(&sb)
	return sb.String()
}

func (r *prefetch) _ctlMsg(sb *cos.SB) {
	// [node]
	sb.WriteString(core.T.String())
	sb.WriteString(": cfg(")
	sb.WriteString(r.msg.Str(r.lrit.lrp == lrpPrefix))
	sb.WriteUint8(')')

	// [this job]
	r._ctlMsgJob(sb)

	// [lifetime]
	r._ctlMsgNode(sb)
}

func (r *prefetch) _ctlMsgJob(sb *cos.SB) {
	var (
		coldN   = r.stats.coldN.Load()
		blobN   = r.stats.blobN.Load()
		blobRej = r.stats.blobRej.Load()
		peblN   = r.pebl.num()
	)
	if coldN == 0 && blobN == 0 && blobRej == 0 && peblN == 0 {
		return
	}

	sb.WriteString(" job:[")
	first := true
	sep := func() {
		if !first {
			sb.WriteUint8(' ')
		}
		first = false
	}

	if coldN > 0 {
		sep()
		sb.WriteString("cold:(")
		sb.WriteString(strconv.FormatInt(coldN, 10))
		sb.WriteUint8(',')
		sb.WriteString(cos.IEC(r.stats.coldSize.Load(), 2))
		sb.WriteUint8(')')
	}
	if blobN > 0 || blobRej > 0 {
		sep()
		sb.WriteString("blob-started:(")
		sb.WriteString(strconv.FormatInt(blobN, 10))
		sb.WriteUint8(',')
		sb.WriteString(cos.IEC(r.stats.blobSize.Load(), 2))
		if blobRej > 0 {
			sb.WriteString(" rejected:")
			sb.WriteString(strconv.FormatInt(blobRej, 10))
		}
		sb.WriteUint8(')')
	}
	if peblN > 0 {
		sep()
		sb.WriteString("pending:(")
		sb.WriteString(strconv.FormatInt(int64(peblN), 10))
		sb.WriteUint8(',')
		sb.WriteString(cos.IEC(r.stats.peblSize.Load(), 2))
		sb.WriteUint8(')')
	}
	sb.WriteUint8(']')
}

func (*prefetch) _ctlMsgNode(sb *cos.SB) {
	tstats := core.T.StatsUpdater()

	var (
		coldN    = tstats.Get(stats.PrefetchColdCount)
		coldSize = tstats.Get(stats.PrefetchColdSize)
		coldLat  = tstats.Get(stats.PrefetchColdLatencyTotal)

		blobN    = tstats.Get(stats.PrefetchBlobCount)
		blobSize = tstats.Get(stats.PrefetchBlobSize)
	)

	if coldN == 0 && blobN == 0 {
		return
	}

	first := true
	sep := func() {
		if !first {
			sb.WriteUint8(' ')
		}
		first = false
	}

	sb.WriteString(" lifetime:[")
	if coldN > 0 {
		sep()
		sb.WriteString("cold:(")
		sb.WriteString(strconv.FormatInt(coldN, 10))
		sb.WriteUint8(',')
		sb.WriteString(cos.IEC(coldSize, 2))
		if coldLat > 0 {
			avg := time.Duration(coldLat / coldN).Truncate(time.Millisecond)
			sb.WriteString(" avg-lat:")
			sb.WriteString(avg.String())
		}
		sb.WriteUint8(')')
	}
	if blobN > 0 {
		sep()
		sb.WriteString("blob-done:(")
		sb.WriteString(strconv.FormatInt(blobN, 10))
		sb.WriteUint8(',')
		sb.WriteString(cos.IEC(blobSize, 2))
		sb.WriteUint8(')')
	}
	sb.WriteUint8(']')
}
