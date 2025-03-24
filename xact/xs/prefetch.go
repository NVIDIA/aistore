// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
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
		xreg.RenewBase
		xctn *prefetch
		msg  *apc.PrefetchMsg
	}
	pebl struct {
		parent  *prefetch
		pending []core.Xact
		load    atomic.Uint64
		n       atomic.Int32
		mu      sync.Mutex
	}
	prefetch struct {
		ctx    context.Context
		bp     core.Backend
		config *cmn.Config
		msg    *apc.PrefetchMsg
		vlabs  map[string]string
		brl    *cos.BurstRateLim
		pebl   pebl
		lrit
		xact.Base
		latestVer bool
	}
)

func (*prfFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	msg := args.Custom.(*apc.PrefetchMsg)
	debug.Assert(!msg.IsList() || !msg.HasTemplate())
	np := &prfFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, msg: msg}
	return np
}

func (p *prfFactory) Start() (err error) {
	if p.msg.BlobThreshold > 0 && p.msg.BlobThreshold < minBlobDlPrefetch {
		a, b := cos.ToSizeIEC(p.msg.BlobThreshold, 0), cos.ToSizeIEC(minBlobDlPrefetch, 0)
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
	r = &prefetch{config: cmn.GCO.Get(), msg: msg}

	err = r.lrit.init(r, &msg.ListRange, bck, msg.NumWorkers)
	if err != nil {
		return nil, err
	}
	r.InitBase(xargs.UUID, kind, msg.Str(r.lrp == lrpPrefix), bck)
	r.latestVer = bck.VersionConf().ValidateWarmGet || msg.LatestVer

	smap := core.T.Sowner().Get()
	nat := smap.CountActiveTs()
	r.brl = bck.NewFrontendRateLim(nat) // TODO: support RateLimitConf.Verbs - here and elsewhere; `nat` vs num-workers

	r.bp = core.T.Backend(bck)
	r.vlabs = map[string]string{
		stats.VlabBucket: bck.Cname(""),
		stats.VlabXkind:  r.Kind(),
	}
	r.ctx = xact.NewCtxVlabs(r.vlabs)

	if r.msg.BlobThreshold > 0 {
		r.pebl.init(r)
	}
	return r, nil
}

func (r *prefetch) Run(wg *sync.WaitGroup) {
	nlog.Infoln(r.Name())

	wg.Done()

	err := r.lrit.run(r, core.T.Sowner().Get())
	if err != nil {
		r.AddErr(err, 5, cos.SmoduleXs) // duplicated?
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
func (r *prefetch) do(lom *core.LOM, lrit *lrit) {
	var (
		err   error
		size  int64
		ecode int
	)

	lom.Lock(false)
	oa, deleted, err := lom.LoadLatest(r.latestVer || r.msg.BlobThreshold > 0) // NOTE: shortcut to find size
	lom.Unlock(false)

	// handle assorted returns
	switch {
	case deleted: // remotely
		debug.Assert(r.latestVer && err != nil)
		if lrit.lrp != lrpList {
			return // deleted or not found remotely, prefix or range
		}
		goto eret
	case oa != nil:
		// not latest
		size = oa.Size
	case err == nil:
		return // nothing to do
	case !cmn.IsErrObjNought(err):
		goto eret
	}

	// apply frontend rate-limit, if any
	if r.brl != nil {
		r.brl.RetryAcquire(time.Second)
	}
	if r.msg.BlobThreshold > 0 && size >= r.msg.BlobThreshold && !r.pebl.busy() {
		err = r.blobdl(lom, oa)
	} else {
		if r.msg.BlobThreshold == 0 && size > cos.GiB {
			r._whinge(lom, size)
		}
		ecode, err = r.getCold(lom)
	}

	switch {
	case err == nil:
		if cmn.Rom.FastV(5, cos.SmoduleXs) {
			nlog.Infoln(r.Name(), lom.Cname())
		}
	case cos.IsNotExist(err, ecode) || cmn.IsErrBusy(err) || err == cmn.ErrSkip:
		if lrit.lrp == lrpList {
			r.AddErr(err, 5, cos.SmoduleXs)
		}
	case cos.IsErrOOS(err):
		r.Abort(err)
	default:
		r.AddErr(err, 5, cos.SmoduleXs)
	}
	return

eret:
	r.AddErr(err, 5, cos.SmoduleXs)
}

func (r *prefetch) _whinge(lom *core.LOM, size int64) {
	var sb strings.Builder
	sb.Grow(256)
	sb.WriteString(r.Name())
	sb.WriteString(": prefetching large size ")
	sb.WriteString(cos.ToSizeIEC(size, 1))
	sb.WriteString(" with blob-downloading disabled [")
	sb.WriteString(lom.Cname())
	sb.WriteByte(']')
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
	if ecode, err = r.bp.GetObj(r.ctx, lom, cmn.OwtGetPrefetchLock, nil /*origReq*/); err != nil {
		return ecode, err
	}

	// RGET stats (compare with ais/tgtimpl namesake)
	rgetstats(r.bp, r.vlabs, lom.Lsize(), started)

	// own stats
	r.ObjsAdd(1, lom.Lsize())

	return 0, nil
}

func (r *prefetch) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	return
}

//
// async, via blob-downloader --------------------------
//

func (r *prefetch) blobdl(lom *core.LOM, oa *cmn.ObjAttrs) error {
	params := &core.BlobParams{
		Lom: core.AllocLOM(lom.ObjName),
		Msg: &apc.BlobMsg{},
	}
	if err := params.Lom.InitBck(lom.Bucket()); err != nil {
		return err
	}
	notif := &xact.NotifXact{
		Base: nl.Base{
			When: core.UponTerm,
			F:    r.pebl.done,
		},
	}
	xctn, err := core.T.GetColdBlob(params, oa)
	if err != nil {
		return err
	}
	notif.Xact = xctn
	xctn.AddNotif(notif)

	if xctn.Finished() {
		return nil
	}
	r.pebl.add(xctn)
	return nil
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
		if xctn.Finished() {
			continue
		}
		// keep
		pebl.pending[n] = xctn
		n++
	}
	pebl.pending = pebl.pending[:n]
	pebl.n.Store(n)

	pebl.mu.Unlock()

	pebl.load.Store(uint64(math.Ceil(sys.MaxLoad()))) // pebl.busy()

	if xblob == nil {
		return
	}

	// log
	xname := pebl.parent.Name()
	switch {
	case aborted || err != nil:
		nlog.Warningln(xname, "::", xblob.String(), "[", msg.String(), err, "]")
	default:
		if xblob.Size() >= cos.GiB/2 || cmn.Rom.FastV(4, cos.SmoduleXs) {
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
	return pebl.n.Load() > maxPebls || pebl.load.Load() >= uint64(sys.HighLoadWM())
}

func (pebl *pebl) str() string {
	var sb strings.Builder

	pebl.mu.Lock()
	n := int(pebl.num())
	sb.Grow(max(256, n*64))

	sb.WriteByte('[')

	for i, xctn := range pebl.pending {
		sb.WriteString(xctn.Name())
		if i < n-1 {
			sb.WriteString("; ")
		}
	}
	pebl.mu.Unlock()

	sb.WriteByte(']')
	return sb.String()
}
