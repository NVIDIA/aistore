// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
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
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/lpi"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact/xreg"

	"github.com/tinylib/msgp/msgp"
)

// `on-demand` per list-objects request
type (
	lsoFactory struct {
		msg *apc.LsoMsg
		hdr http.Header
		streamingF
	}
	LsoXact struct {
		lpis      lpi.Lpis
		msg       *apc.LsoMsg      // first message
		msgCh     chan *apc.LsoMsg // next messages
		respCh    chan *LsoRsp     // responses - next pages
		remtCh    chan *LsoRsp     // remote paging by the responsible target
		ctx       *core.LsoInvCtx  // bucket inventory
		nextToken string           // next continuation token -> next pages
		token     string           // continuation token -> last responded page
		stopCh    cos.StopCh       // to stop xaction
		page      cmn.LsoEntries   // current page (contents)
		walk      struct {
			pageCh       chan *cmn.LsoEnt // channel to accumulate listed object entries
			stopCh       *cos.StopCh      // to abort bucket walk
			wi           *walkInfo        // walking context and state
			bp           core.Backend     // t.Backend(bck)
			wg           sync.WaitGroup   // wait until this walk finishes
			done         bool             // done walking (indication)
			wor          bool             // wantOnlyRemote
			dontPopulate bool             // when listing remote obj-s: don't include local MD (in re: LsDonAddRemote)
			this         bool             // r.msg.SID == core.T.SID(): true when this target does remote paging
			last         bool             // last remote page
			remote       bool             // list remote
		}
		streamingX
		lensgl int64 // channel to accumulate listed object entries
	}
	LsoRsp struct {
		Err    error
		Lst    *cmn.LsoRes
		Status int
	}
)

const (
	pageChSize     = 128
	remtPageChSize = 16
)

var (
	errStopped = errors.New("stopped")
	ErrGone    = errors.New("gone")
)

// interface guard
var (
	_ core.Xact      = (*LsoXact)(nil)
	_ xreg.Renewable = (*lsoFactory)(nil)
)

// common helper
func lsoIsRemote(bck *meta.Bck, cachedOnly bool) bool { return !cachedOnly && bck.IsRemote() }

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

	r.page = allocLsoEntries()
	r.stopCh.Init()

	// idle timeout vs delayed next-page request
	// see also: resetIdle()
	r.DemandBase.Init(p.UUID(), apc.ActList, p.msg.Str(p.Bck.Cname(p.msg.Prefix)) /*ctlmsg*/, p.Bck, r.config.Timeout.MaxHostBusy.D())

	// is set by the first message, never changes
	r.walk.wor = r.msg.WantOnlyRemoteProps()
	r.walk.this = r.msg.SID == core.T.SID()

	// true iff the bucket was not added - not initialized
	r.walk.dontPopulate = r.walk.wor && p.Bck.Props == nil
	debug.Assert(!r.walk.dontPopulate || p.msg.IsFlagSet(apc.LsDontAddRemote))

	if lsoIsRemote(r.p.Bck, r.msg.IsFlagSet(apc.LsCached)) {
		// begin streams
		if !r.walk.wor {
			nt := core.T.Sowner().Get().CountActiveTs()
			if nt > 1 {
				// streams
				if err := p.beginStreams(r); err != nil {
					return err
				}
			}
		}
		// alternative flow _this_ target will execute:
		// - nextpage =>
		// -     backend.GetBucketInv() =>
		// -        while { backend.ListObjectsInv }
		if cos.IsParseBool(p.hdr.Get(apc.HdrInventory)) && r.walk.this {
			r.ctx = &core.LsoInvCtx{Name: p.hdr.Get(apc.HdrInvName), ID: p.hdr.Get(apc.HdrInvID)}
		}

		// engage local page iterator (lpi)
		if r.msg.IsFlagSet(apc.LsDiff) {
			r.lpis.Init(r.Bck().Bucket(), r.msg.Prefix, core.T.Sowner().Get())
		}
	}

	p.xctn = r
	return nil
}

func (p *lsoFactory) beginStreams(r *LsoXact) error {
	if !r.walk.this {
		r.remtCh = make(chan *LsoRsp, remtPageChSize) // <= by selected target (selected to page remote bucket)
	}
	trname := "lso-" + p.UUID()
	dmxtra := bundle.Extra{Multiplier: 1, Config: r.config}
	p.dm = bundle.NewDM(trname, r.recv, cmn.OwtPut, dmxtra)

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

func (r *LsoXact) Run(wg *sync.WaitGroup) {
	wg.Done()

	r.walk.remote = lsoIsRemote(r.p.Bck, r.msg.IsFlagSet(apc.LsCached))
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
			debug.Assert(r.walk.wor == msg.WantOnlyRemoteProps(), msg.Str(r.p.Bck.Cname("")))
			debug.Assert(r.walk.remote == lsoIsRemote(r.p.Bck, msg.IsFlagSet(apc.LsCached)), msg.Str(r.p.Bck.Cname("")))

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

	r.stop()
}

func (r *LsoXact) stop() {
	r.stopCh.Close()
	if lsoIsRemote(r.p.Bck, r.msg.IsFlagSet(apc.LsCached)) {
		if r.DemandBase.Finished() {
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
					close(r.msgCh)
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

	if r.page != nil {
		freeLsoEntries(r.page)
		r.page = nil
	}
	if r.ctx != nil {
		if r.ctx.Lom != nil {
			cos.Close(r.ctx.Lmfh)
			r.ctx.Lom.Unlock(false) // NOTE: see GetBucketInv() "returns" comment in aws.go
			core.FreeLOM(r.ctx.Lom)
			r.ctx.Lom = nil
		}
		if r.ctx.SGL != nil {
			if r.ctx.SGL.Len() > 0 {
				nlog.Errorln(r.String(), "non-paginated leftover upon exit (bytes)", r.ctx.SGL.Len())
			}
			r.ctx.SGL.Free()
			r.ctx.SGL = nil
		}
		r.ctx = nil
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
			close(r.remtCh)
		}
		close(r.msgCh)
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
	r.walk.wg.Add(1)

	go r.doWalk(r.msg.Clone())
	runtime.Gosched()
}

func (r *LsoXact) Do(msg *apc.LsoMsg) *LsoRsp {
	// The guarantee here is that we either put something on the channel and our
	// request will be processed (since the `msgCh` is unbuffered) or we receive
	// message that the xaction has been stopped.
	select {
	case r.msgCh <- msg:
		return <-r.respCh
	case <-r.stopCh.Listen():
		return &LsoRsp{Err: ErrGone}
	}
}

func (r *LsoXact) doPage() *LsoRsp {
	if r.walk.remote {
		if r.msg.ContinuationToken == "" || r.msg.ContinuationToken != r.token {
			// can't extract the next-to-list object name from the remotely generated
			// continuation token, keeping and returning the entire last page
			r.token = r.msg.ContinuationToken
			if err := r.nextPageR(); err != nil {
				return &LsoRsp{Status: http.StatusInternalServerError, Err: err}
			}
		}
		page := &cmn.LsoRes{UUID: r.msg.UUID, Entries: r.page, ContinuationToken: r.nextToken}

		if r.msg.IsFlagSet(apc.LsDiff) {
			r.lpis.Do(r.page, page, r.Name(), r.walk.last)
		}

		return &LsoRsp{Lst: page, Status: http.StatusOK}
	}

	if r.msg.ContinuationToken == "" || r.msg.ContinuationToken != r.token {
		r.nextPageA()
	}
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
	return &LsoRsp{Lst: page, Status: http.StatusOK}
}

// `ais show job` will report the sum of non-replicated obj numbers and
// sum of obj sizes - for all visited objects
// Returns the index of the first object in the page that follows the continuation `token`
func (r *LsoXact) findToken(token string) int {
	if r.token == token && lsoIsRemote(r.p.Bck, r.msg.IsFlagSet(apc.LsCached)) {
		return 0
	}
	return sort.Search(len(r.page), func(i int) bool { // TODO: revisit
		return !cmn.TokenGreaterEQ(token, r.page[i].Name)
	})
}

func (r *LsoXact) havePage(token string, cnt int64) bool {
	if r.walk.done {
		return true
	}
	idx := r.findToken(token)
	return idx+int(cnt) < len(r.page)
}

func (r *LsoXact) nextPageR() (err error) {
	var (
		page *cmn.LsoRes
		npg  = newNpgCtx(r.p.Bck, r.msg, r.LomAdd, r.ctx, r.walk.bp)
		smap = core.T.Sowner().Get()
		tsi  = smap.GetActiveNode(r.msg.SID)
	)
	if tsi == nil {
		err = fmt.Errorf("%s: designated (\"paging\") %s is down or inactive, %s", r, meta.Tname(r.msg.SID), smap)
		goto ex
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
	freeLsoEntries(r.page)
	r.page = page.Entries
	r.nextToken = page.ContinuationToken

	return err
}

func (r *LsoXact) thisPageR(npg *npgCtx) (page *cmn.LsoRes, err error) {
	var (
		aborted  bool
		nentries = allocLsoEntries()
	)
	page, err = npg.nextPageR(nentries)
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
	o.Callback, o.CmplArg = r.sentCb, sgl // cleanup
	o.Reader = sgl
	roc := memsys.NewReader(sgl)
	r.p.dm.Bcast(o, roc)
	return nil
}

func (r *LsoXact) sentCb(hdr *transport.ObjHdr, _ io.ReadCloser, arg any, err error) {
	if err == nil {
		// using generic out-counter to count broadcast pages
		r.OutObjsAdd(1, hdr.ObjAttrs.Size)
	} else if cmn.Rom.FastV(4, cos.SmoduleXs) || !cos.IsRetriableConnErr(err) {
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
		if r.walk.done {
			return
		}
		r.shiftLastPage(r.msg.ContinuationToken)
	}
	r.token = r.msg.ContinuationToken

	if r.havePage(r.token, r.msg.PageSize) {
		return
	}
	for cnt := int64(0); cnt < r.msg.PageSize; {
		obj, ok := <-r.walk.pageCh
		if !ok {
			r.walk.done = true
			r.resetIdle()
			break
		}
		// Skip until the requested continuation token (TODO: revisit)
		if cmn.TokenGreaterEQ(r.token, obj.Name) {
			continue
		}
		cnt++
		r.page = append(r.page, obj)
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
		WalkOpts:   fs.WalkOpts{CTs: []string{fs.ObjectType}, Callback: r.cb, Prefix: msg.Prefix, Sorted: true},
	}
	opts.WalkOpts.Bck.Copy(r.Bck().Bucket())
	if err := fs.WalkBck(opts); err != nil {
		if err != filepath.SkipDir && err != errStopped {
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

	// no recursion:
	// - check the level of nesting
	// - possibly add virt dir entry
	addDirEntry, errN := cmn.CheckDirNoRecurs(r.walk.wi.msg.Prefix, ct.ObjectName())
	if addDirEntry {
		entry := &cmn.LsoEnt{Name: ct.ObjectName(), Flags: apc.EntryIsDir}
		select {
		case r.walk.pageCh <- entry:
		case <-r.walk.stopCh.Listen():
			return errStopped
		}
	}
	return errN // filepath.SkipDir or nil
}

func (r *LsoXact) cb(fqn string, de fs.DirEntry) error {
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
		return errStopped
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
			Name:  path.Join(entry.Name, archEntry.Name),
			Flags: entry.Flags | apc.EntryInArch,
			Size:  archEntry.Size,
		}
		select {
		case r.walk.pageCh <- e:
			/* do nothing */
		case <-r.walk.stopCh.Listen():
			return errStopped
		}
	}
	return nil
}

func (r *LsoXact) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	return
}

//
// streaming receive: remote pages
//

// (note: ObjHdr and its fields must be consumed synchronously)
func (r *LsoXact) recv(hdr *transport.ObjHdr, objReader io.Reader, err error) error {
	debug.Assert(lsoIsRemote(r.p.Bck, r.msg.IsFlagSet(apc.LsCached)))

	if hdr.Opcode == opAbort {
		// TODO: consider r.Abort(err); today it'll idle for a while
		// see:  streamingX.sendTerm
		err = newErrRecvAbort(r, hdr)
	}
	if err != nil && !cos.IsEOF(err) {
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
