// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
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
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/lpi"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact/xreg"

	"github.com/tinylib/msgp/msgp"
)

const (
	iniPageCap = min(1024, apc.MaxPageSizeGlobal)
	maxPageCap = 4 * apc.MaxPageSizeGlobal
)

// `on-demand` per list-objects request
type (
	lsoFactory struct {
		msg *apc.LsoMsg
		hdr http.Header
		streamingF
	}
	lsoStats struct {
		nreq   atomic.Int64 // doPage requests
		npages atomic.Int64 // successful page responses
		ents   atomic.Int64 // total returned entries
		errs   atomic.Int64 // failed page responses
		totLat atomic.Int64 // total successful Do() latency, nanos
		maxLat atomic.Int64 // max successful Do() latency, nanos
	}
	LsoXact struct {
		nbi *nbiCtx // native bucket inventory

		msg       *apc.LsoMsg       // first message
		msgCh     chan *apc.LsoMsg  // next messages
		respCh    chan *LsoRsp      // responses - next pages
		remtCh    chan *LsoRsp      // remote paging by the responsible target
		nextToken string            // next continuation token -> next pages
		token     string            // continuation token -> last responded page
		stopCh    cos.StopCh        // to stop xaction
		vlabs     map[string]string // -> Prometheus
		stats     lsoStats          // -> CtlMsg (`ais show job`) observability
		smap      *meta.Smap
		walk      struct {
			bp           core.Backend     // t.Backend(bck)
			pageCh       chan *cmn.LsoEnt // channel to accumulate listed object entries
			stopCh       *cos.StopCh      // to abort bucket walk
			wi           *walkInfo        // walking context and state
			lastDir      string           // last seen directory name (for dedup - heap output is sorted, so duplicates are adjacent)
			wg           sync.WaitGroup   // wait until this walk finishes
			done         bool             // done walking (indication)
			wor          bool             // wantOnlyRemote
			dontPopulate bool             // when listing remote obj-s: don't include local MD (in re: LsDonAddRemote)
			this         bool             // r.msg.SID == core.T.SID(): true when this target does remote paging
			last         bool             // last remote page
			remote       bool             // list remote
			ctlVerbose   bool             // this target prints static request details in CtlMsg
		}
		lpis lpi.Lpis
		page cmn.LsoEntries // current page (contents)
		adv  load.Advice
		streamingX
		lensgl int64
		// list remote
	}
	LsoRsp struct {
		Err    error
		Lst    *cmn.LsoRes
		Status int
	}
)

const assertContProgress = "continuation token must make forward progress"

const (
	pageChSize     = 128
	remtPageChSize = 16
)

var (
	errLsoStopped = errors.New("stopped")
	ErrGone       = errors.New("gone")
)

// interface guard
var (
	_ core.Xact      = (*LsoXact)(nil)
	_ xreg.Renewable = (*lsoFactory)(nil)
)

////////////////
// lsoFactory //
////////////////

func (*lsoFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	custom := args.Custom.(*xreg.LsoArgs)
	p := &lsoFactory{
		streamingF: streamingF{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, kind: apc.ActList},
		msg:        custom.Msg,
		hdr:        custom.Hdr,
	}
	return p
}

func (p *lsoFactory) Start() error {
	if err := cos.ValidatePrefix("bad list-objects request", p.msg.Prefix); err != nil {
		return err
	}
	r := &LsoXact{
		streamingX: streamingX{p: &p.streamingF, config: cmn.GCO.Get()},
		msg:        p.msg,
		msgCh:      make(chan *apc.LsoMsg), // unbuffered
		respCh:     make(chan *LsoRsp),     // ditto: one caller-requested page at a time
	}

	r.stopCh.Init()

	// idle timeout vs delayed next-page request
	// see also: resetIdle()
	r.DemandBase.Init(p.UUID(), apc.ActList, p.Bck, r.config.Timeout.MaxHostBusy.D())

	bck := r.Bck()

	// is set by the first message, never changes
	r.walk.wor = r.msg.WantOnlyRemoteProps()
	r.walk.remote = !r.msg.IsFlagSet(apc.LsCached) && !r.msg.IsFlagSet(apc.LsNBI) && bck.IsRemote()

	// true iff the bucket was not added - not initialized
	r.walk.dontPopulate = r.walk.wor && bck.Props == nil
	debug.Assert(!r.walk.dontPopulate || p.msg.IsFlagSet(apc.LsDontAddRemote))

	r.smap = core.T.Sowner().Get()
	tsi, err := r.smap.HrwTargetTask(r.ID())
	if err != nil {
		return err
	}
	// select one target to show the CtlMsg's static part
	r.walk.ctlVerbose = tsi.ID() == core.T.SID()

	if r.walk.remote {
		// R-flow
		r.walk.this = r.msg.SID == core.T.SID()
		if !r.walk.wor {
			if nat := r.smap.CountActiveTs(); nat > 1 {
				// streams
				if err := p.beginStreams(r); err != nil {
					return err
				}
			}
		}
		if r.walk.this {
			r.page = make(cmn.LsoEntries, 0, iniPageCap) // reuse across all remote pages (may grow in cap)
		}

		// engage local page iterator (lpi)
		if r.msg.IsFlagSet(apc.LsDiff) {
			r.lpis.Init(bck.Bucket(), r.msg.Prefix, core.T.Sowner().Get())
		}
	} else {
		// A-flow
		debug.Assert(r.msg.SID == "")
		r.page = make(cmn.LsoEntries, 0, iniPageCap) // reuse across all pages (may grow in cap)

		if r.msg.IsFlagSet(apc.LsNBI) {
			invName := p.hdr.Get(apc.HdrInvName)
			debug.Assert(invName != "") // checked (or set) by target
			r.nbi = &nbiCtx{bck: bck}
			if err := r.nbi.init(invName); err != nil {
				return err
			}
		}
	}

	r.adv.Init(
		load.FlMem|load.FlCla,
		&load.Extra{
			RW: false,
		},
	)
	// and maybe throttle right away
	if r.adv.Sleep > 0 {
		time.Sleep(r.adv.Sleep)
	}

	_ = r.CtlMsg()
	r.vlabs = map[string]string{stats.VlabBucket: bck.Cname("")}
	p.xctn = r

	return nil
}

func (p *lsoFactory) beginStreams(r *LsoXact) error {
	if !r.walk.this {
		r.remtCh = make(chan *LsoRsp, remtPageChSize) // <= by selected target (selected to page remote bucket)
	}

	// TODO -- FIXME: list-objects must have its own config section
	extra := bundle.Extra{
		XactConf:         cmn.XactConf{SbundleMult: 1, Compression: apc.CompressNever},
		Config:           r.config,
		SkipGenericStats: true,
	}

	trname := "lso-" + p.UUID()
	p.dm = bundle.NewDM(trname, r.recv, cmn.OwtPut, extra)

	if err := p.dm.RegRecv(); err != nil {
		if p.msg.ContinuationToken != "" {
			err = fmt.Errorf("%s: late continuation [%s,%s], DM: %v", core.T,
				p.msg.UUID, p.msg.ContinuationToken, err)
		}
		nlog.Errorln(err)
		return err
	}
	p.dm.SetXact(r)
	p.dm.Open()
	return nil
}

/////////////
// LsoXact //
/////////////

func (r *LsoXact) CtlMsg() string {
	if r.msg == nil {
		return ""
	}

	var sb cos.SB
	sb.Init(ctlMsgBufSize)

	sb.WriteString(core.T.String())
	sb.WriteUint8(':')

	if r.walk.ctlVerbose {
		sb.WriteUint8(' ')
		r.msg.Str(r.p.Bck.Cname(r.msg.Prefix), &sb)
		if r.nextToken != "" {
			sb.WriteString(apc.LsoCtlMsgPaging)
		}
		if r.walk.remote {
			sb.WriteString(apc.LsoCtlMsgRemote)
		}
	}

	s := &r.stats

	nreq := s.nreq.Load()
	if nreq == 0 {
		return sb.String()
	}

	// internal stats
	sb.WriteString(" job:[")
	sb.WriteString("reqs:")
	sb.WriteString(strconv.FormatInt(nreq, 10))

	npages := s.npages.Load()
	if npages != 0 {
		sb.WriteString(" pages:")
		sb.WriteString(strconv.FormatInt(npages, 10))

		ents := s.ents.Load()
		sb.WriteString(" entries:")
		sb.WriteString(strconv.FormatInt(ents, 10))

		totLat := s.totLat.Load()
		avgLat := totLat / npages
		sb.WriteString(" avg-lat:")
		sb.WriteString(time.Duration(avgLat).Truncate(time.Microsecond).String())

		maxLat := s.maxLat.Load()
		sb.WriteString(" max-lat:")
		sb.WriteString(time.Duration(maxLat).Truncate(time.Microsecond).String())
	}
	errs := s.errs.Load()
	if errs != 0 {
		sb.WriteString(" errs:")
		sb.WriteString(strconv.FormatInt(errs, 10))
	}
	sb.WriteUint8(']')

	return sb.String()
}

func (r *LsoXact) Run(wg *sync.WaitGroup) {
	wg.Done()

	if !r.walk.remote {
		r.initWalk()
	} else {
		r.walk.bp = core.T.Backend(r.p.Bck)
	}
loop:
	for {
		select {
		case msg := <-r.msgCh:
			// Copy only the values that can change between calls
			debug.Assert(r.msg.UUID == msg.UUID && r.msg.Prefix == msg.Prefix && r.msg.Flags == msg.Flags)
			r.msg.ContinuationToken = msg.ContinuationToken
			r.msg.PageSize = msg.PageSize

			// cannot change
			debug.Assert(r.msg.SID == msg.SID, r.msg.SID, " vs ", msg.SID)
			debug.Assert(r.walk.wor == msg.WantOnlyRemoteProps(), r.CtlMsg())

			r.IncPending()
			resp := r.doPage()
			r.DecPending()
			if r.IsAborted() {
				break loop
			}
			if resp.Err == nil {
				// report heterogeneous stats (x-list is an exception)
				r.ObjsAdd(len(resp.Lst.Entries), 0)
			}
			r.respCh <- resp
		case <-r.IdleTimer():
			break loop
		case <-r.ChanAbort():
			break loop
		}
	}

	// TODO confirm: once started (lsoFactory.Start), always runs and executes stop (to properly cleanup)
	r.stop()
}

func (r *LsoXact) stop() {
	r.stopCh.Close()
	if r.walk.remote {
		if r.DemandBase.IsDone() {
			// must be aborted
			if !r.walk.wor {
				r.p.dm.Close(r.Err())
				r.p.dm.UnregRecv()
			}
		} else {
			r.DemandBase.Stop()
			r.Finish()
			r.lastmsg()

			if !r.walk.wor {
				r.p.dm.Close(r.Err())

				if r.walk.this {
					debug.Assert(r.remtCh == nil)
					cos.DrainAnyChan(r.msgCh)
					r.p.dm.UnregRecv()
				} else if r.p.dm != nil {
					// postpone unreg
					hk.Reg(r.ID()+hk.NameSuffix, r.fcleanup, r.config.Timeout.MaxKeepalive.D())
				}
			}
		}
	} else {
		r.DemandBase.Stop()
		r.walk.stopCh.Close()
		r.walk.wg.Wait()
		r.lastmsg()
		r.Finish()
	}

	clear(r.page)
	r.page = nil

	if r.nbi != nil {
		r.nbi.cleanup()
		r.nbi = nil
	}
}

func (r *LsoXact) lastmsg() {
	select {
	case <-r.msgCh:
		r.respCh <- &LsoRsp{Err: ErrGone}
	default:
		break
	}
	close(r.respCh)
}

// upon listing last page
func (r *LsoXact) resetIdle() {
	r.DemandBase.Reset(max(r.config.Timeout.MaxKeepalive.D(), 2*time.Second))
}

func (r *LsoXact) fcleanup(int64) (d time.Duration) {
	if cnt := r.wiCnt.Load(); cnt > 0 {
		d = max(cmn.Rom.MaxKeepalive(), 2*time.Second)
	} else {
		d = hk.UnregInterval
		if r.remtCh != nil {
			cos.DrainAnyChan(r.remtCh)
		}
		cos.DrainAnyChan(r.msgCh)
		r.p.dm.UnregRecv()
	}
	return d
}

// skip on-demand idleness check
func (r *LsoXact) Abort(err error) (ok bool) {
	if ok = r.Base.Abort(err); ok {
		r.Finish()
	}
	return
}

// Start `fs.WalkBck`, so that by the time we read the next page `r.pageCh` is already populated.
func (r *LsoXact) initWalk() {
	r.walk.pageCh = make(chan *cmn.LsoEnt, pageChSize)
	r.walk.done = false
	r.walk.stopCh = cos.NewStopCh()
	r.walk.lastDir = "" // reset directory dedup state
	r.walk.wg.Add(1)

	go r.doWalk(r.msg.Clone())
	runtime.Gosched()
}

func (r *LsoXact) Do(msg *apc.LsoMsg) *LsoRsp {
	debug.Assert(r.vlabs != nil)
	begin := mono.NanoTime()
	select {
	case r.msgCh <- msg:
		resp := <-r.respCh
		if resp == nil {
			return nil
		}
		if resp.Err != nil {
			r.stats.errs.Inc()
			return resp
		}
		delta := mono.SinceNano(begin)

		// prom metrics
		tstats := core.T.StatsUpdater()
		tstats.IncWith(stats.ListCount, r.vlabs)
		tstats.AddWith(
			cos.NamedVal64{Name: stats.ListLatency, Value: delta, VarLabs: r.vlabs},
		)
		// internal (via CtlMsg)
		r.stats.addResp(resp, delta)

		return resp
	case <-r.stopCh.Listen():
		return &LsoRsp{Err: ErrGone}
	}
}

func (r *LsoXact) doPage() *LsoRsp {
	// throttle
	nreq := r.stats.nreq.Inc()
	if r.adv.ShouldCheck(nreq) {
		r.adv.Refresh()
		if r.adv.Sleep > 0 {
			time.Sleep(r.adv.Sleep)
		}
	}

	// repeated request for same page
	if r.msg.ContinuationToken != "" && r.msg.ContinuationToken == r.token {
		page := &cmn.LsoRes{UUID: r.msg.UUID, Entries: r.page, ContinuationToken: r.nextToken}
		return &LsoRsp{Lst: page, Status: http.StatusOK}
	}

	debug.Assert(!r.walk.remote || r.nbi == nil)
	switch {
	case r.walk.remote:
		return r.doPageR()
	case r.nbi != nil:
		return r.doPageNBI()
	default:
		return r.doPageA()
	}
}

func (r *LsoXact) doPageR() *LsoRsp {
	if r.msg.ContinuationToken == "" || r.msg.ContinuationToken != r.token {
		// can't extract the next-to-list object name from the remotely generated
		// continuation token, keeping and returning the entire last page
		r.token = r.msg.ContinuationToken
		if err := r.nextPageR(); err != nil {
			return &LsoRsp{Status: http.StatusInternalServerError, Err: err}
		}
		// must make forward progress
		debug.Assertf(r.nextToken == "" || r.nextToken != r.token, "%s: next=%q, prev=%q",
			assertContProgress, r.nextToken, r.token)
	}
	page := &cmn.LsoRes{UUID: r.msg.UUID, Entries: r.page, ContinuationToken: r.nextToken}

	if r.msg.IsFlagSet(apc.LsDiff) && r.lpis.Enabled() {
		r.lpis.Do(r.page, page, r.Name(), r.walk.last)
	}
	return &LsoRsp{Lst: page, Status: http.StatusOK}
}

func (r *LsoXact) doPageA() *LsoRsp {
	r.nextPageA()

	var (
		cnt  = r.msg.PageSize
		idx  = r.findToken(r.msg.ContinuationToken)
		lst  = r.page[idx:]
		page *cmn.LsoRes
	)
	debug.Assert(int64(len(lst)) >= cnt || r.walk.done)
	if int64(len(lst)) >= cnt {
		entries := lst[:cnt]
		page = &cmn.LsoRes{UUID: r.msg.UUID, Entries: entries, ContinuationToken: entries[cnt-1].Name}
	} else {
		page = &cmn.LsoRes{UUID: r.msg.UUID, Entries: lst}
	}

	// NOTE: do NOT assign (r.page = returned page.Entries) here.
	// In the A-flow, r.page is the full walk cache accumulated by nextPageA.
	// The returned `page` is a slice window into it - overwriting r.page
	// with that window would destroy the cache and break subsequent
	// pagination (shiftLastPage, findToken), esp. for small page sizes.
	//
	// (doPageNBI uses a different paging semantics and is handled separately)
	return &LsoRsp{Lst: page, Status: http.StatusOK}
}

func (r *LsoXact) doPageNBI() *LsoRsp {
	entries := r.page[:0]
	lst := &cmn.LsoRes{UUID: r.msg.UUID, Entries: entries}
	err := r.nbi.nextPage(r.msg, lst)
	if err != nil {
		return &LsoRsp{Lst: lst, Status: http.StatusInternalServerError, Err: err}
	}
	r.page = lst.Entries
	return &LsoRsp{Lst: lst, Status: http.StatusOK}
}

// return index of the first object in the page that follows the continuation `token`, as in:
// - page[:idx] <= token
// - page[idx:] > token
func (r *LsoXact) findToken(token string) int {
	if r.token == token && r.walk.remote {
		return 0
	}
	return sort.Search(len(r.page), func(i int) bool {
		return !cmn.TokenGreaterEQ(token, r.page[i].Name)
	})
}

func (r *LsoXact) havePage(token string, cnt int64) bool {
	debug.Assert(!r.walk.done)
	idx := r.findToken(token) // corresponds to r.page[idx:]
	return idx+int(cnt) <= len(r.page)
}

func (r *LsoXact) nextPageR() (err error) {
	var (
		page *cmn.LsoRes
		npg  = newNpgCtx(r.p.Bck, r.msg, r.walk.bp)
		tsi  = r.smap.GetActiveNode(r.msg.SID)
	)
	if tsi == nil {
		err = fmt.Errorf("%s: designated (\"paging\") %s is down or inactive, %s", r, meta.Tname(r.msg.SID), r.smap)
		goto ex
	}

	if !r.walk.wor {
		smapCurr := core.T.Sowner().Get()
		if err := r.smap.CheckSameTargets(smapCurr, r.Name()); err != nil {
			r.Abort(err)
			return err
		}
	}

	r.wiCnt.Inc()

	if r.walk.this {
		page, err = r.thisPageR(npg)
	} else {
		debug.Assert(!r.msg.WantOnlyRemoteProps() && /*same*/ !r.walk.wor)
		select {
		case rsp := <-r.remtCh:
			switch {
			case rsp == nil:
				err = ErrGone
			case rsp.Err != nil:
				err = rsp.Err
			default:
				page = rsp.Lst
				err = npg.filterAddLmeta(page)
				r.walk.last = page.ContinuationToken == ""
			}
		case <-r.stopCh.Listen():
			err = ErrGone
		}
	}

	r.wiCnt.Dec()
ex:
	if err != nil {
		r.nextToken = ""
		r.AddErr(err)
		return err
	}
	if page.ContinuationToken == "" {
		r.walk.done = true
		r.resetIdle()
	}
	r.page = page.Entries
	r.nextToken = page.ContinuationToken

	return err
}

func (r *LsoXact) thisPageR(npg *npgCtx) (page *cmn.LsoRes, err error) {
	if cap(r.page) > maxPageCap {
		r.page = make(cmn.LsoEntries, 0, apc.MaxPageSizeGlobal)
	}

	var (
		aborted bool
		entries = r.page[:0] // reusing the same (backing) slice between remote pages
	)
	page, err = npg.nextPageR(entries)
	if err != nil {
		goto rerr
	}
	r.walk.last = page.ContinuationToken == ""

	if r.walk.wor {
		return page, nil
	}
	if aborted = r.IsAborted(); aborted {
		goto rerr
	}

	// bcast page
	if err = r.bcast(page); err != nil {
		goto rerr
	}
	// populate
	if !r.walk.dontPopulate {
		if err = npg.filterAddLmeta(page); err != nil {
			goto rerr
		}
	}

	return page, nil

rerr:
	if aborted && err == nil {
		err = r.AbortErr()
	}
	r.sendTerm(r.msg.UUID, nil, err)
	return page, err
}

func (r *LsoXact) bcast(page *cmn.LsoRes) (err error) {
	if r.p.dm == nil { // single target
		return nil
	}
	var (
		mm        = core.T.PageMM()
		siz       = max(r.lensgl, memsys.DefaultBufSize)
		buf, slab = mm.AllocSize(siz)
		sgl       = mm.NewSGL(siz, slab.Size())
		mw        = msgp.NewWriterBuf(sgl, buf)
	)
	if err = page.EncodeMsg(mw); err == nil {
		err = mw.Flush()
	}
	slab.Free(buf)
	r.lensgl = sgl.Len()
	if err != nil {
		sgl.Free()
		return err
	}

	o := transport.AllocSend()
	{
		o.Hdr.Bck = r.p.Bck.Clone()
		o.Hdr.ObjName = r.Name()
		o.Hdr.Opaque = cos.UnsafeB(r.p.UUID())
		o.Hdr.ObjAttrs.Size = sgl.Len()
	}
	o.SentCB, o.CmplArg = r.sentCb, sgl // cleanup
	o.Reader = sgl
	roc := memsys.NewReader(sgl)
	r.p.dm.Bcast(o, roc)
	return nil
}

func (r *LsoXact) sentCb(hdr *transport.ObjHdr, _ io.ReadCloser, arg any, err error) {
	if err == nil {
		// using generic out-counter to count broadcast pages
		r.OutObjsAdd(1, hdr.ObjAttrs.Size)
	} else if cmn.Rom.V(4, cos.ModXs) || !cos.IsErrRetriableConn(err) {
		nlog.Infof("Warning: %s: failed to send [%+v]: %v", core.T, hdr, err)
	}
	sgl, ok := arg.(*memsys.SGL)
	debug.Assertf(ok, "%T", arg)
	sgl.Free()
}

func (r *LsoXact) _clrPage(from, to int) {
	for i := from; i < to; i++ {
		r.page[i] = nil
	}
}

func (r *LsoXact) nextPageA() {
	if r.token > r.msg.ContinuationToken {
		// restart traversing the bucket (TODO: cache more and try to scroll back)
		r.walk.stopCh.Close()
		r.walk.wg.Wait()
		r.initWalk()
		r._clrPage(0, len(r.page))
		r.page = r.page[:0]
	} else {
		// filter cached page by token first (regardless of walk.done)
		r.shiftLastPage(r.msg.ContinuationToken)
	}

	r.token = r.msg.ContinuationToken

	// if (a) done walking or (b) already have enough, stop
	if r.walk.done || r.havePage(r.token, r.msg.PageSize) {
		return
	}

	for cnt := int64(0); cnt < r.msg.PageSize; {
		entry, ok := <-r.walk.pageCh
		if !ok {
			r.walk.done = true
			r.resetIdle()
			break
		}
		// [convention] dirnames always have trailing slash, and vice versa
		// see also: j.opts.IncludeDirs
		debug.Func(func() {
			ok := entry.IsAnyFlagSet(apc.EntryIsDir)
			debug.Assert(ok == cos.IsLastB(entry.Name, '/'), entry.Name)
		})

		// skip until requested continuation token
		if cmn.TokenGreaterEQ(r.token, entry.Name) {
			continue
		}
		cnt++
		r.page = append(r.page, entry)
	}
}

// Removes entries that were already sent to clients.
// Is used only for AIS buckets and (cached == true) requests.
func (r *LsoXact) shiftLastPage(token string) {
	if token == "" || len(r.page) == 0 {
		return
	}
	j := r.findToken(token)
	// the page is "after" the token - keep it all
	if j == 0 {
		return
	}
	l := len(r.page)

	// (all sent)
	if j == l {
		r._clrPage(0, l)
		r.page = r.page[:0]
		return
	}

	// otherwise, shift the not-yet-transmitted entries and fix the slice
	copy(r.page[0:], r.page[j:])
	r._clrPage(l-j, l)
	r.page = r.page[:l-j]
}

// (compare w/ nextPageA() via lrit)
func (r *LsoXact) doWalk(msg *apc.LsoMsg) {
	r.walk.wi = newWalkInfo(msg, r.LomAdd)
	opts := &fs.WalkBckOpts{
		ValidateCb: r.validateCb,
		WalkOpts: fs.WalkOpts{
			CTs:         []string{fs.ObjCT},
			Callback:    r.cb,
			Prefix:      msg.Prefix,
			Sorted:      true,
			IncludeDirs: msg.IsFlagSet(apc.LsNoRecursion), // include directories in heap for non-recursive listing
		},
	}
	opts.WalkOpts.Bck.Copy(r.Bck().Bucket())
	if err := fs.WalkBck(opts); err != nil {
		if err != filepath.SkipDir && err != errLsoStopped {
			r.AddErr(err, 0)
		}
	}
	close(r.walk.pageCh)
	r.walk.wg.Done()
}

func (r *LsoXact) validateCb(fqn string, de fs.DirEntry) error {
	if !de.IsDir() {
		return nil
	}
	ct, err := r.walk.wi.processDir(fqn)
	if err != nil || ct == nil {
		return err
	}
	if !r.walk.wi.msg.IsFlagSet(apc.LsNoRecursion) {
		return nil
	}

	// no recursion: check nesting level to decide whether to skip walking into this directory
	// (the directory entry itself will be filtered in r.cb using the same CheckDirNoRecurs logic)
	_, errN := cmn.CheckDirNoRecurs(r.walk.wi.msg.Prefix, ct.ObjectName())
	return errN
}

func (r *LsoXact) cb(fqn string, de fs.DirEntry) error {
	// Handle directory entries (from non-recursive listing with IncludeDirs=true)
	if de.IsDir() {
		ct, err := core.NewCTFromFQN(fqn, nil)
		if err != nil {
			return nil // skip on error
		}
		dirName := ct.ObjectName()

		// Filter: only add directories that should be listed as entries
		addDirEntry, _ := cmn.CheckDirNoRecurs(r.walk.wi.msg.Prefix, dirName)
		if !addDirEntry {
			return nil // not a direct child of prefix, skip
		}

		// Use trailing slash for directory names to distinguish from files with same name
		// This ensures lexicographical sorting works correctly: "aaa/bbb" < "aaa/bbb/"
		debug.Assert(!cos.IsLastB(dirName, filepath.Separator))
		dirName += cos.PathSeparator

		// Deduplicate: heap output is sorted, so duplicates from different mountpaths are adjacent
		if dirName == r.walk.lastDir {
			return nil // skip duplicate
		}
		r.walk.lastDir = dirName

		entry := &cmn.LsoEnt{Name: dirName, Flags: apc.EntryIsDir}
		select {
		case r.walk.pageCh <- entry:
		case <-r.walk.stopCh.Listen():
			return errLsoStopped
		}
		return nil
	}

	// Handle file entries
	entry, err := r.walk.wi.callback(fqn, de)
	if err != nil || entry == nil {
		return err
	}
	msg := r.walk.wi.lsmsg()
	if entry.Name <= msg.StartAfter {
		return nil
	}

	select {
	case r.walk.pageCh <- entry:
		/* do nothing */
	case <-r.walk.stopCh.Listen():
		return errLsoStopped
	}

	if !msg.IsFlagSet(apc.LsArchDir) {
		return nil
	}

	// ls arch
	// looking only at the file extension - not reading ("detecting") file magic (TODO: add lsmsg flag)
	archList, err := archive.List(fqn)
	if err != nil {
		if archive.IsErrUnknownFileExt(err) {
			// skip and keep going
			err = nil
		}
		return err
	}
	entry.Flags |= apc.EntryIsArchive // the parent archive
	for _, archEntry := range archList {
		e := &cmn.LsoEnt{
			Name: path.Join(entry.Name, archEntry.Name),
			Size: archEntry.Size,

			// inherit parent's flags except apc.EntryIsArchive
			Flags: (entry.Flags &^ apc.EntryIsArchive) | apc.EntryInArch,
		}
		select {
		case r.walk.pageCh <- e:
			/* do nothing */
		case <-r.walk.stopCh.Listen():
			return errLsoStopped
		}
	}
	return nil
}

func (r *LsoXact) Snap() *core.Snap { return r.Base.NewSnap(r) }

//
// streaming receive: remote pages
//

// (note: ObjHdr and its fields must be consumed synchronously)
func (r *LsoXact) recv(hdr *transport.ObjHdr, objReader io.Reader, err error) error {
	debug.Assert(r.walk.remote)

	if hdr.Opcode == transport.OpcAbort {
		errCause := hdr.ObjName // (see streamingX.sendTerm)
		err = r.NewErrRecvAbortXact(hdr.SID, errCause)
		r.Abort(err)
	}
	if err != nil && !cos.IsOkEOF(err) {
		nlog.Errorln(core.T.String(), r.String(), len(r.remtCh), err)
		r.remtCh <- &LsoRsp{Status: http.StatusInternalServerError, Err: err}
		return err
	}

	debug.Assert(hdr.Opcode == 0)
	r.IncPending()
	buf, slab := core.T.PageMM().AllocSize(cmn.MsgpLsoBufSize)

	err = r._recv(hdr, objReader, buf)

	slab.Free(buf)
	r.DecPending()
	transport.DrainAndFreeReader(objReader)
	return err
}

func (r *LsoXact) _recv(hdr *transport.ObjHdr, objReader io.Reader, buf []byte) (err error) {
	var (
		page = &cmn.LsoRes{}
		mr   = msgp.NewReaderBuf(objReader, buf)
	)
	err = page.DecodeMsg(mr)
	if err == nil {
		r.remtCh <- &LsoRsp{Lst: page, Status: http.StatusOK}
		// using generic in-counter to count received pages
		r.InObjsAdd(1, hdr.ObjAttrs.Size)
	} else {
		nlog.Errorf("%s: failed to recv [%s: %s] num=%d from %s (%s, %s): %v",
			core.T, page.UUID, page.ContinuationToken, len(page.Entries),
			hdr.SID, hdr.Bck.Cname(""), string(hdr.Opaque), err)
		r.remtCh <- &LsoRsp{Status: http.StatusInternalServerError, Err: err}
	}
	return
}

//////////////
// lsoStats //
//////////////

func (s *lsoStats) addResp(resp *LsoRsp, delta int64) {
	s.npages.Add(1)
	s.ents.Add(int64(len(resp.Lst.Entries)))
	s.totLat.Add(delta)
	maxLat := s.maxLat.Load()
	if delta > maxLat {
		s.maxLat.CAS(maxLat, delta)
	}
}
