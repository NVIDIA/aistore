// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/cmn/work"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"

	jsoniter "github.com/json-iterator/go"
)

/*
client →  any proxy →  randomly selected designated-target (DT (= T0))
                   │
                   ├── basewi.next()      ←  local data-items
                   │
                   ├── shared-dm.recvd[i] = ...     ←  data-items from other targets via recv()
                   │
                   └── stream TAR         ←  in-order via flushRx()

senders (T1..Tn)
    └── shared-DM.Send(req, smap, DT)
           └── for locality(data-item) == self:
                    shared-dm.Send(data-item, hdr.Opaque = [wid, oname, emsg, index, missing]) → DT

where data-item = (object | archived file) // range read TBD
*/

// Shared-DM registration lifecycle: xaction registers once with shared-dm transport
// when first work item needs receiving (PrepRx w/ receiving=true), then
// unregisters once at xaction end (Abort/fini). No per-work-item ref-counting
// needed since UnregRecv is permissive no-op when not registered.

// TODO:
// - stats: (files, objects) separately; Prometheus metrics
// - soft errors other than not-found; unified error formatting
// - mem-pool: basewi; apc.MossReq; apc.MossResp
// ---------- (can wait) ------------------
// - speed-up via selecting multiple files from a given archive
// - range read
// - finer-grained wi abort

const (
	sparseLog = 30 * time.Second
)

// bwarm is a small-size worker pool per x-moss instance unless under heavy load:
// - created at init time
// - stays enabled for the entire lifetime
const (
	bewarmSize = cos.MiB
)

// limit the number of GFN requests by a work item (wi)
const (
	gfnMaxCount = 2
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
	basewi struct {
		recv struct {
			ch   chan int
			m    []rxentry
			next int
			mtx  *sync.Mutex
		}
		aw      archive.Writer
		r       *XactMoss
		smap    *meta.Smap
		req     *apc.MossReq
		resp    *apc.MossResp
		sgl     *memsys.SGL // multipart (buffered) only
		wid     string      // work item ID
		started int64       // (mono)
		waitRx  int64       // total wait-for-receive time
		size    int64       // (cumulative)
		ocnt    int         // number of processed MossReq objects
		fcnt    int         // --/-- files
		gfncnt  int         // <= gfnMaxCount
		clean   atomic.Bool
		awfin   atomic.Bool
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
		gmm     *memsys.MMSA
		smm     *memsys.MMSA
		config  *cmn.Config
		bewarm  *work.Pool
		pending sync.Map // [wid => *basewi]
		xact.DemandBase
		activeWG sync.WaitGroup // when pending
		size     atomic.Int64   // cumulative across all work items (plain objects, archived files)
		ocnt     atomic.Int64   // total objects: sum(wi.ocnt)
		fcnt     atomic.Int64   // total files: sum(wi.fcnt)
		waitRx   atomic.Int64   // total wait-for-receive time (= sum(work-items))
		numWis   atomic.Int64   // num get-batch requests
		lastLog  atomic.Int64   // last log timestamp (sparse)
		gfn      struct {       // counts
			ok   atomic.Int32
			fail atomic.Int32
		}
		// ctlmsg builder (reused)
		ctl struct {
			sb cos.Sbuilder
			mu sync.Mutex
		}
	}
)

type (
	_archSentCmpl struct {
		lom *core.LOM
		lh  cos.LomReader
		buf []byte
	}
)

const (
	mossIdleTime = xact.IdleDefault
)

// interface guard
var (
	_ xreg.Renewable     = (*mossFactory)(nil)
	_ core.Xact          = (*XactMoss)(nil)
	_ transport.Receiver = (*XactMoss)(nil)
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

	switch {
	case prevBck == nil && currBck == nil:
		// reuse when no default bucket whatsoever
	case (prevBck == nil) != (currBck == nil):
		return xreg.WprKeepAndStartNew, nil
	default:
		if !prevBck.Equal(currBck, true, true) {
			return xreg.WprKeepAndStartNew, nil
		}
		// reuse
	}

	if cmn.Rom.V(5, cos.ModXs) {
		nlog.Infoln(core.T.String(), "DT prev:", r.Name(), "curr:", p.UUID(), "- using prev...")
	}
	// reset DemandBase.last timestamp to prevent idle timeout between now and Assemble()
	r.DemandBase.IncPending()
	r.DemandBase.DecPending()
	return xreg.WprUse, nil
}

func newMoss(p *mossFactory) *XactMoss {
	r := &XactMoss{
		gmm:    memsys.PageMM(),
		smm:    memsys.ByteMM(),
		config: cmn.GCO.Get(),
	}
	r.DemandBase.Init(p.UUID(), p.Kind(), "" /*ctlmsg*/, p.Bck, mossIdleTime, r.fini)

	// best-effort `bewarm` for pagecache warming-up
	s := "without"
	if num := r.config.GetBatch.WarmupWorkers(); num > 0 {
		load, isExtreme := sys.MaxLoad2()
		if !isExtreme {
			ncpu := sys.NumCPU()
			highcpu := ncpu * sys.HighLoad / 100
			if load < float64(highcpu) {
				r.bewarm = work.New(num, num /*work-chan cap*/, r.bewarmFQN, r.ChanAbort())
				s = "with"
			}
		}
	}

	nlog.Infoln(core.T.String(), "run:", r.Name(), s, "\"bewarm\"")
	return r
}

func (*XactMoss) Run(*sync.WaitGroup) { debug.Assert(false) }

func (r *XactMoss) IncPending() {
	r.DemandBase.IncPending()
	r.activeWG.Add(1)
}

func (r *XactMoss) DecPending() {
	r.DemandBase.DecPending()
	r.activeWG.Done()
}

func (r *XactMoss) BcastAbort(err error) {
	if isErrRecvAbort(err) {
		return
	}
	o := transport.AllocSend()
	o.Hdr.Opcode = opAbort
	o.Hdr.Demux = r.ID()
	o.Hdr.ObjName = err.Error()
	e := bundle.SDM.Bcast(o, nil /*roc*/) // receive via sntl.rxAbort
	if cmn.Rom.V(4, cos.ModXs) {
		nlog.Infoln(r.Name(), core.T.String(), "bcast abort [", err, e, "]")
	}
}

func (r *XactMoss) Abort(err error) bool {
	if !r.DemandBase.Abort(err) {
		return false
	}

	nlog.Infoln(r.Name(), "aborting:", err)

	// make sure all asm() and Send() exited
	r.activeWG.Wait()

	r.pending.Range(r.cleanup)

	r.DemandBase.Stop()

	// see "Shared-DM registration lifecycle" note above
	bundle.SDM.UnregRecv(r.ID())

	r.bewarmStop()
	return true
}

// pending (sync.Map's) callback to cleanup all pending work items
func (r *XactMoss) cleanup(key, value any) bool {
	wi := value.(*basewi)
	if wi.cleanup() {
		r.pending.Delete(key)
	}
	return true
}

func (r *XactMoss) gcAbandoned(now int64, maxIters int) {
	var (
		iters  int
		tout   = max(mossIdleTime, time.Minute)
		cutoff = now - tout.Nanoseconds()
	)
	debug.Assert(maxIters > 0)
	r.pending.Range(func(k, v any) bool {
		iters++
		cont := iters < maxIters
		wi := v.(*basewi)
		// old enough
		if wi.started != 0 && wi.started < cutoff {
			if wi.cleanup() {
				// not r.DecPending here (these wi-s never incremented)
				r.pending.Delete(k)
				if cmn.Rom.V(4, cos.ModXs) {
					nlog.Infof("%s: GC abandoned wi %q", r.Name(), wi.wid)
				}
			}
		}
		return cont
	})
}

// terminate via (<-- xact.Demand <-- hk)
func (r *XactMoss) fini(now int64) (d time.Duration) {
	if cmn.Rom.V(5, cos.ModXs) {
		if ok, fail := r.gfn.ok.Load(), r.gfn.fail.Load(); ok+fail > 0 {
			nlog.Infoln(r.Name(), "GFN: [", ok, fail, "]")
		}
	}

	// cleanup abandoned wi-s
	r.gcAbandoned(now, 32 /*max-iters*/)

	switch {
	case r.IsAborted() || r.Finished():
		return hk.UnregInterval
	case r.Pending() > 0:
		return mossIdleTime
	default:
		ocnt := r.ocnt.Load()
		fcnt := r.fcnt.Load()
		size := r.size.Load()
		if size == 0 {
			nlog.Infoln(r.Name(), "idle expired [ stats: none ]")
		} else {
			nlog.Infoln(r.Name(), "idle expired [ objs:", ocnt, "files:", fcnt, "size:", size, "]")
		}
		r.pending.Range(r.cleanup)

		// see "Shared-DM registration lifecycle" note above
		bundle.SDM.UnregRecv(r.ID())

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
		nlog.Infoln(r.Name(), "bewarm count:", n)
	}
	r.bewarm.Wait()
}

// (phase 1)
func (r *XactMoss) PrepRx(req *apc.MossReq, smap *meta.Smap, wid string, receiving, usingPrev bool) error {
	var (
		resp = &apc.MossResp{UUID: r.ID()}
		wi   = basewi{r: r, smap: smap, req: req, resp: resp, wid: wid}
	)

	// for receiving (DT) and GC (all)
	wi.started = mono.NanoTime()

	if receiving {
		if usingPrev {
			bundle.SDM.UseRecv(r)
		} else {
			bundle.SDM.RegRecv(r)
		}
		// Rx state
		wi.recv.m = make([]rxentry, len(req.In))    // preallocate
		wi.recv.ch = make(chan int, len(req.In)<<1) // extra cap
		wi.recv.mtx = &sync.Mutex{}

		r.updateCtlMsg(wi.started)
	}

	if a, loaded := r.pending.LoadOrStore(wid, &wi); loaded {
		old := a.(*basewi)
		age := mono.Since(old.started)
		err := fmt.Errorf("%s: work item %q already exists for %v (duplicate WID generated?)", r.Name(), wid, age)
		if age > mossIdleTime {
			nlog.Errorln(core.T.String(), err)
		}
		return err
	}
	wi.awfin.Store(true)

	return nil
}

// gather other requested data (local and remote); emit resulting archive
// (phase 3)
func (r *XactMoss) Assemble(req *apc.MossReq, w http.ResponseWriter, wid string) error {
	a, loaded := r.pending.Load(wid)
	if !loaded {
		err := fmt.Errorf("%s: work item %q not found (prep-rx not done?)", r.Name(), wid)
		nlog.Errorln(core.T.String(), err)
		return err
	}
	wi := a.(*basewi)
	debug.Assert(wid == wi.wid)

	if wi.clean.Load() {
		return fmt.Errorf("%s: work item %q is no longer available (stale)", r.Name(), wid)
	}

	r.IncPending()
	err := r.asm(req, w, wi)
	wi.cleanup()

	r.DecPending()
	r.updateCtlMsg(0)
	return err
}

func (r *XactMoss) asm(req *apc.MossReq, w http.ResponseWriter, basewi *basewi) error {
	opts := archive.Opts{TarFormat: tar.FormatUnknown} // default tar format

	// streaming
	if req.StreamingGet {
		wi := streamwi{basewi: basewi}
		wi.aw = archive.NewWriter(req.OutputFormat, w, nil /*checksum*/, &opts)
		wi.awfin.Store(false)
		err := wi.asm(w)
		if cmn.Rom.V(5, cos.ModXs) {
			nlog.Infoln(r.Name(), core.T.String(), "done streaming Assemble", basewi.wid, "err", err)
		}
		return err
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
	err := wi.asm(w)

	if cmn.Rom.V(5, cos.ModXs) {
		nlog.Infoln(r.Name(), core.T.String(), "done multipart Assemble", basewi.wid, "err", err)
	}
	return err
}

// send all requested local data => DT (tsi)
// (phase 2)

func (r *XactMoss) Send(req *apc.MossReq, smap *meta.Smap, dt *meta.Snode /*DT*/, wid string, usingPrev bool) error {
	// with a single purpose: to receive opAbort
	if usingPrev {
		bundle.SDM.UseRecv(r)
	} else {
		bundle.SDM.RegRecv(r)
	}

	// send all
	r.IncPending()
	defer r.DecPending()

	for i := range req.In {
		if r.IsAborted() || r.Finished() {
			return nil
		}
		in := &req.In[i]
		if err := _assertNoRange(in); err != nil {
			return err
		}
		lom, tsi, err := r._lom(in, smap)
		if err != nil {
			return err
		}
		if tsi != nil {
			continue // other target must have it
		}

		var (
			nameInArch = in.NameInRespArch(lom.Bck().Name, req.OnlyObjName)
		)
		lom.Lock(false) // (always unlocked by _sendreg/_sendarch)

		if in.ArchPath == "" {
			err = r._sendreg(dt, lom, wid, nameInArch, i)
		} else {
			err = r._sendarch(dt, lom, wid, nameInArch, in.ArchPath, i)
		}
		if err != nil {
			return err
		}
	}

	if cmn.Rom.V(5, cos.ModXs) {
		nlog.Infoln(r.Name(), core.T.String(), "done Send", wid)
	}
	return nil
}

func (r *XactMoss) _sendreg(tsi *meta.Snode, lom *core.LOM, wid, nameInArch string, index int) error {
	var (
		oah      = lom.ObjAttrs()
		roc, err = lom.NewDeferROC(false /*loaded*/)
	)
	mopaque := &mossOpaque{
		WID:   wid,
		Oname: lom.ObjName,
		Index: int32(index),
	}
	if err != nil {
		mopaque.Missing = true
		mopaque.Emsg = err.Error()
		nameInArch = apc.MossMissingDir + cos.PathSeparator + nameInArch
		oah = &cmn.ObjAttrs{}
		roc = nil
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

	o.SentCB, o.CmplArg = r.regSent, opaque
	return bundle.SDM.Send(o, roc, tsi, r)
}

func (r *XactMoss) regSent(hdr *transport.ObjHdr, _ io.ReadCloser, arg any, _ error) {
	opaque, ok := arg.([]byte)
	debug.Assert(ok)
	r.smm.Free(opaque)

	if hdr.ObjAttrs.Size > 0 {
		r.OutObjsAdd(1, hdr.ObjAttrs.Size)
	}
}

func (r *XactMoss) _sendarch(tsi *meta.Snode, lom *core.LOM, wid, nameInArch, archpath string, index int) error {
	var (
		roc     cos.ReadOpenCloser
		oah     cos.SimpleOAH
		mopaque = &mossOpaque{
			WID:   wid,
			Oname: lom.ObjName + "/" + archpath,
			Index: int32(index),
		}
	)
	nameInArch += cos.PathSeparator + archpath

	lh, err := lom.NewHandle(false /*loaded*/)
	if err != nil {
		mopaque.Missing = true
		mopaque.Emsg = err.Error()
		nameInArch = apc.MossMissingDir + cos.PathSeparator + nameInArch
	} else {
		csl, err := lom.NewArchpathReader(lh, archpath, "" /*mime*/)
		if err != nil {
			nameInArch = apc.MossMissingDir + cos.PathSeparator + nameInArch
			mopaque.Missing = true
			mopaque.Emsg = err.Error()
			cos.Close(lh)
			lh = nil
		} else {
			// csl is cos.ReadCloseSizer; see transport/bundle/shared_dm for InitSDM
			roc = cos.NopOpener(csl)
			oah.Size = csl.Size()
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
	o.SentCB = r.archSent
	o.CmplArg = &_archSentCmpl{lom, lh, opaque}

	return bundle.SDM.Send(o, roc, tsi, r)
}

func (r *XactMoss) archSent(hdr *transport.ObjHdr, _ io.ReadCloser, arg any, _ error) {
	ctx, ok := arg.(*_archSentCmpl)
	debug.Assert(ok)
	debug.Assert(ctx.lom.IsLocked() == apc.LockRead)

	if lh, ok := ctx.lh.(*core.LomHandle); ok && lh != nil {
		cos.Close(lh)
	}
	ctx.lom.Unlock(false)
	r.smm.Free(ctx.buf)

	if hdr.ObjAttrs.Size > 0 {
		r.OutObjsAdd(1, hdr.ObjAttrs.Size) // counting archived file as an "object"
	}
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
// note convention: received hdr.ObjName is `nameInArch` (ie., filename in resulting TAR)
func (r *XactMoss) RecvObj(hdr *transport.ObjHdr, reader io.Reader, err error) error {
	defer transport.DrainAndFreeReader(reader)
	if err != nil {
		// TODO: handle transport.AsErrSBR(err); not enough info to abort one WID :TODO
		return err
	}
	if r.IsAborted() || r.Finished() {
		return nil
	}

	// control
	if hdr.Opcode != 0 {
		switch hdr.Opcode {
		case opAbort:
			sntl := &sentinel{r: r}
			sntl.rxAbort(hdr)
			return nil
		default:
			return abortOpcode(r, hdr.Opcode)
		}
	}

	// data
	if err := r._recvObj(hdr, reader, err); err != nil {
		nlog.Errorln(r.Name(), core.T.String(), "RecvObj:", err)
		r.BcastAbort(err)
		r.Abort(err)
		return err
	}
	if hdr.ObjAttrs.Size > 0 {
		r.InObjsAdd(1, hdr.ObjAttrs.Size)
	}
	return nil
}

// (note: ObjHdr and its fields must be consumed synchronously)
func (r *XactMoss) _recvObj(hdr *transport.ObjHdr, reader io.Reader, err error) error {
	if err != nil {
		return err
	}
	mopaque, err := r.unpackOpaque(hdr.Opaque)
	if err != nil {
		return err
	}
	a, loaded := r.pending.Load(mopaque.WID)
	if !loaded {
		// stale or unknown WID: drop quietly
		if cmn.Rom.V(4, cos.ModXs) {
			nlog.Infof("%s: wi %q not pending - dropping", r.Name(), mopaque.WID)
		}
		return nil
	}
	wi := a.(*basewi)

	// already cleaned-up via gcAbandoned()? drop as well
	if wi.clean.Load() {
		if cmn.Rom.V(4, cos.ModXs) {
			nlog.Infof("%s: wi %q is already clean/done - dropping", r.Name(), wi.wid)
		}
		return nil
	}

	debug.Assert(mopaque.WID == wi.wid)
	debug.Assert(wi.receiving())
	return wi.recvObj(int(mopaque.Index), hdr, reader, mopaque)
}

func (*mossFactory) Kind() string     { return apc.ActGetBatch }
func (p *mossFactory) Get() core.Xact { return p.xctn }

func (r *XactMoss) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)
	snap.IdleX = r.IsIdle()
	return
}

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
func (r *XactMoss) _bucket(in *apc.MossIn) (*cmn.Bck, error) {
	// default
	bck := r.Bck().Bucket()

	// uname override
	if in.Uname != "" {
		b, _, err := meta.ParseUname(in.Uname, false)
		if err != nil {
			return nil, err
		}
		return b.Bucket(), nil
	}

	// (bucket, provider) override
	if in.Bucket != "" {
		np, err := cmn.NormalizeProvider(in.Provider)
		if err != nil {
			return nil, err
		}
		return &cmn.Bck{Name: in.Bucket, Provider: np}, nil
	}

	if bck == nil {
		if in.ArchPath == "" {
			return nil, fmt.Errorf("%s: missing bucket specification for object %q", r.Name(), in.ObjName)
		}
		return nil, fmt.Errorf("%s: missing bucket specification for archived file %s/%s", r.Name(), in.ObjName, in.ArchPath)
	}
	return bck, nil
}

func (*XactMoss) bewarmFQN(fqn string) {
	if f, err := os.Open(fqn); err == nil {
		io.CopyN(io.Discard, f, bewarmSize)
		f.Close()
	}
}

// single writer: DT only
func (r *XactMoss) updateCtlMsg(now int64) {
	ocnt, fcnt := r.ocnt.Load(), r.fcnt.Load()
	if ocnt+fcnt == 0 {
		return
	}

	// build under a short critical section
	// (e.g.: pending:2 objs:576730 files:391284 size:8.70GiB waitrx.avg:2.3ms bewarm:on)

	r.ctl.mu.Lock()
	r.ctl.sb.Reset(min(128, r.ctl.sb.Len()))
	if pending := r.Pending(); pending > 0 {
		r.ctl.sb.WriteString("pending:")
		r.ctl.sb.WriteString(strconv.FormatInt(pending, 10))
	}
	if ocnt > 0 {
		r.ctl.sb.WriteString(" objs:")
		r.ctl.sb.WriteString(strconv.FormatInt(ocnt, 10))
	}
	if fcnt > 0 {
		r.ctl.sb.WriteString(" files:")
		r.ctl.sb.WriteString(strconv.FormatInt(fcnt, 10))
	}

	r.ctl.sb.WriteString(" size:")
	r.ctl.sb.WriteString(cos.IEC(r.size.Load(), 2))

	r.ctl.sb.WriteString(" reqs:")
	r.ctl.sb.WriteString(strconv.FormatInt(r.numWis.Load(), 10))

	if waitRx := r.waitRx.Load(); waitRx > 0 {
		avg := time.Duration(r.waitRx.Load()) / time.Duration(ocnt+fcnt)
		r.ctl.sb.WriteString(" avg-wait:")
		r.ctl.sb.WriteString(avg.String())
	}

	r.ctl.sb.WriteString(" bewarm:")
	bewarm := cos.Ternary(r.bewarm != nil, "on", "off")
	r.ctl.sb.WriteString(bewarm)

	msg := r.ctl.sb.String()
	r.SetCtlMsg(msg)
	r.ctl.mu.Unlock()

	if now == 0 {
		now = mono.NanoTime()
	}
	if time.Duration(now-r.lastLog.Load()) > sparseLog {
		nlog.Infoln(r.Name(), "stats: [", msg, "]")
		r.lastLog.Store(now)
	}
}

////////////
// basewi //
////////////

func (wi *basewi) receiving() bool { return wi.recv.m != nil }

func (wi *basewi) cleanup() bool {
	if !wi.clean.CAS(false, true) {
		return false
	}
	r := wi.r

	// update x-moss stats
	r.ocnt.Add(int64(wi.ocnt))
	r.fcnt.Add(int64(wi.fcnt))
	r.size.Add(wi.size)
	r.waitRx.Add(wi.waitRx)
	r.numWis.Inc()

	if wi.awfin.CAS(false, true) {
		err := wi.aw.Fini()
		wi.aw = nil
		if err != nil {
			if cmn.Rom.V(5, cos.ModXs) {
				nlog.Warningln(r.Name(), core.T.String(), "cleanup: err fini()", wi.wid, err)
			}
		}
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
	return true
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
		if cmn.Rom.V(5, cos.ModXs) {
			nlog.Infoln(r.Name(), core.T.String(), "Rx [ wid:", wi.wid, "index:", index, "oname:", hdr.ObjName, "size:", size, "]")
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
		if r.IsAborted() || r.Finished() {
			return false, sgl != nil, nil
		}
		return false, sgl != nil, fmt.Errorf("%s %s out-of-bounds index %d (recv'd len=%d, wid=%s)", r.Name(), core.T.String(), index, len(wi.recv.m), wi.wid)
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
		start          = mono.NanoTime()
		recvDrainBurst = cos.ClampInt(8, 1, cap(wi.recv.ch)>>1)
		timeout        = wi.r.config.GetBatch.MaxWait.D()
	)
	for {
		wi.recv.mtx.Lock()
		err := wi.flushRx()
		next := wi.recv.next
		wi.recv.mtx.Unlock()

		if err != nil {
			return 0, err
		}
		if next > i {
			wi.waitRx += elapsed.Nanoseconds()
			return next, nil
		}

		var received bool
	inner:
		for range recvDrainBurst {
			select {
			case _, ok := <-wi.recv.ch:
				if !ok {
					return 0, errStopped
				}
				received = true
			default:
				break inner
			}
		}
		if received {
			continue
		}

		if elapsed >= timeout {
			return 0, wi.newErrHole(timeout)
		}
		if err := wi.waitAnyRx(timeout - elapsed); err != nil {
			if err == context.DeadlineExceeded {
				return 0, wi.newErrHole(timeout)
			}
			return 0, err
		}
		elapsed = mono.Since(start)
	}
}

// in parallel with RecvObj() inserting new entries..
func (wi *basewi) waitAnyRx(timeout time.Duration) error {
	// fast path
	select {
	case _, ok := <-wi.recv.ch:
		if !ok {
			return errStopped
		}
		return nil
	case <-wi.r.ChanAbort():
		return errStopped
	default:
	}

	// wait
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-wi.recv.ch:
			if !ok {
				return errStopped
			}
			return nil

		case <-timer.C:
			return context.DeadlineExceeded

		case <-wi.r.ChanAbort():
			return errStopped
		}
	}
}

func (wi *basewi) newErrHole(total time.Duration) error {
	index := wi.recv.next // Safe to read without lock here
	s := fmt.Sprintf("%s: timed out waiting for %d \"hole\" to fill [ %s, wid=%s, total-wait=%v ]",
		wi.r.Name(), index, core.T.String(), wi.wid, total)
	return &errHole{s}
}

func (wi *basewi) next(i int) (int, error) {
	var (
		r  = wi.r
		in = &wi.req.In[i]
	)
	if err := _assertNoRange(in); err != nil {
		return 0, err
	}

	lom, tsi, err := r._lom(in, wi.smap)
	if err != nil {
		return 0, err
	}

	if tsi != nil {
		debug.Assertf(wi.receiving(),
			"%s: unexpected non-local %s when _not_ receiving [%s, %s]", r.Name(), lom.Cname(), wi.wid, wi.smap)

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
		if err == nil || !isErrHole(err) || wi.gfncnt >= gfnMaxCount {
			return nextIdx, err
		}
	}

	bck := lom.Bck()
	out := apc.MossOut{
		Bucket:   bck.Name,
		Provider: bck.Provider,
		ObjName:  in.ObjName,
		ArchPath: in.ArchPath,
		Opaque:   in.Opaque,
	}
	nameInArch := in.NameInRespArch(bck.Name, wi.req.OnlyObjName)

	if isErrHole(err) {
		wi.gfncnt++
		err = wi.gfn(lom, tsi, in, &out, nameInArch, err)
	} else {
		err = wi.write(lom, in, &out, nameInArch)
	}
	if err != nil {
		return 0, err
	}

	if wi.req.StreamingGet {
		if err := wi.aw.Flush(); err != nil {
			return 0, err
		}
	} else {
		wi.resp.Out = append(wi.resp.Out, out)
	}

	if in.ArchPath == "" {
		wi.ocnt++
	} else {
		wi.fcnt++
	}
	wi.size += out.Size

	if wi.receiving() {
		wi.recv.mtx.Lock()
		entry := &wi.recv.m[i]
		entry.local = true
		debug.Assert(wi.recv.next <= i, wi.recv.next, " vs ", i) // must be contiguous
		wi.recv.mtx.Unlock()
	}
	return i + 1, nil
}

func (wi *basewi) gfn(lom *core.LOM, tsi *meta.Snode, in *apc.MossIn, out *apc.MossOut, nameInArch string, errHole error) error {
	debug.Assert(tsi != nil)
	params := &core.GfnParams{
		Lom:      lom,
		Tsi:      tsi,
		ArchPath: in.ArchPath,
		Size:     wi.avgSize(),
	}

	resp, err := core.T.GetFromNeighbor(params) //nolint:bodyclose // closed below

	if err != nil {
		nlog.Warningln(wi.r.Name(), wi.wid, "GFN fail:", nameInArch, err)
		wi.r.gfn.fail.Inc()
		if wi.req.ContinueOnErr {
			return wi.write(lom, in, out, nameInArch)
		}
		return errHole // remains orig err
	}

	nlog.Infoln(wi.r.Name(), wi.wid, "GFN ok:", nameInArch)
	wi.r.gfn.ok.Inc()
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
	}
	cos.Close(resp.Body)
	return err
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

func (wi *basewi) write(lom *core.LOM, in *apc.MossIn, out *apc.MossOut, nameInArch string) error {
	lom.Lock(false)
	err := wi._write(lom, in, out, nameInArch)
	lom.Unlock(false)
	return err
}

// (under rlock)
func (wi *basewi) _write(lom *core.LOM, in *apc.MossIn, out *apc.MossOut, nameInArch string) error {
	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		if cos.IsNotExist(err) && wi.req.ContinueOnErr {
			err = wi.addMissing(err, nameInArch, out)
		}
		return err
	}

	lmfh, err := lom.Open()
	if err != nil {
		if cos.IsNotExist(err) && wi.req.ContinueOnErr {
			err = wi.addMissing(err, nameInArch, out)
		}
		return err
	}

	switch {
	case in.ArchPath != "":
		nameInArch = _withArchpath(nameInArch, in.ArchPath)
		var csl cos.ReadCloseSizer
		csl, err = lom.NewArchpathReader(lmfh, in.ArchPath, "" /*mime*/)
		if err != nil {
			if cos.IsNotExist(err) && wi.req.ContinueOnErr {
				err = wi.addMissing(err, nameInArch, out)
			}
		} else {
			err = wi._txarch(csl, out, nameInArch, csl.Size())
			csl.Close()
		}
	default:
		err = wi._txreg(lom, lmfh, out, nameInArch)
	}
	cos.Close(lmfh)
	return err
}

func _withArchpath(nameInArch, archpath string) string {
	if archpath[0] == '/' {
		return nameInArch + archpath
	}
	return nameInArch + cos.PathSeparator + archpath
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
	return nil
}

func (wi *basewi) asm() error {
	l := len(wi.req.In)
	for i := 0; i < l; {
		if wi.r.IsAborted() || wi.r.Finished() {
			return nil
		}
		j, err := wi.next(i)
		if err != nil {
			return err
		}
		debug.Assert(j > i && j <= l, i, " vs ", j, " vs ", l)
		i = j
	}
	return nil
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

		wi.recv.mtx.Unlock() //--------------

		switch {
		case entry.mopaque.Missing:
			debug.Assert(strings.HasPrefix(entry.nameInArch, apc.MossMissingDir+"/"), entry.nameInArch)
			err = wi.aw.Write(entry.nameInArch, cos.SimpleOAH{Size: 0}, nopROC{})
		default:
			debug.Assert(entry.mopaque.Emsg == "", entry.mopaque.Emsg)
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
				ObjName:  entry.mopaque.Oname,
				Size:     size,
				Opaque:   in.Opaque, // as is (see apc/ml definition)
				ErrMsg:   entry.mopaque.Emsg,
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
		if in.ArchPath == "" {
			wi.ocnt++
		} else {
			wi.fcnt++
		}
		wi.size += size
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

	written, erw := wi.multipart(mpw, wi.resp)
	if err := mpw.Close(); err != nil && erw == nil {
		erw = err
	}
	if erw != nil {
		nlog.Warningln(wi.r.Name(), cmn.ErrGetTxBenign, "[", erw, "]")
		return cmn.ErrGetTxBenign
	}

	wi.sgl.Reset()

	// TODO: consider adding archived-files counts and sizes => base.Xact
	wi.r.ObjsAdd(wi.ocnt+wi.fcnt, wi.size)

	if cmn.Rom.V(5, cos.ModXs) {
		nlog.Infoln(wi.r.Name(), "done buffered: [ count:", len(wi.resp.Out), "written:", written, "format:", wi.req.OutputFormat, "]")
	}
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
	w.Header().Set(cos.HdrContentType, archive.ContentTypeFromExt(wi.req.OutputFormat))
	if err := wi.basewi.asm(); err != nil {
		nlog.Warningln(wi.r.Name(), cmn.ErrGetTxBenign, "[", err, "]")
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

	// TODO: ditto
	wi.r.ObjsAdd(wi.ocnt+wi.fcnt, wi.size)

	return nil
}

func _assertNoRange(in *apc.MossIn) (err error) {
	if in.Length != 0 {
		err = cmn.NewErrNotImpl("range read", "moss")
	}
	return
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

// //////////////
// mossOpaque (not to confuse with apc.MossIn/Out Opaque)
// //////////////

type mossOpaque struct {
	WID     string
	Oname   string
	Emsg    string
	Index   int32
	Missing bool
}

// interface guard
var (
	_ cos.Packer   = (*mossOpaque)(nil)
	_ cos.Unpacker = (*mossOpaque)(nil)
)

func (o *mossOpaque) Pack(packer *cos.BytePack) {
	packer.WriteString(o.WID)
	packer.WriteString(o.Oname)
	packer.WriteString(o.Emsg)
	packer.WriteInt32(o.Index)
	packer.WriteBool(o.Missing)
}

func (o *mossOpaque) PackedSize() int {
	return cos.PackedStrLen(o.WID) + cos.PackedStrLen(o.Oname) + cos.PackedStrLen(o.Emsg) + cos.SizeofI32 + 1
}

func (o *mossOpaque) Unpack(unpacker *cos.ByteUnpack) (err error) {
	if o.WID, err = unpacker.ReadString(); err != nil {
		return err
	}
	if o.Oname, err = unpacker.ReadString(); err != nil {
		return err
	}
	if o.Emsg, err = unpacker.ReadString(); err != nil {
		return err
	}
	if o.Index, err = unpacker.ReadInt32(); err != nil {
		return err
	}
	o.Missing, err = unpacker.ReadBool()
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
