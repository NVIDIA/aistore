// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"archive/tar"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"

	jsoniter "github.com/json-iterator/go"
)

/*
client → proxy → designated-target (TD)
                   │
                   ├── basewi.next()      ← local HRW entries
                   │
                   ├── shared-dm.recvd[i] = ...     ← entries from other targets via recv()
                   │
                   └── stream TAR         ← in-order via flushRx()

other-targets (T1..Tn)
    └── Send(req, smap, TD)
           └── for i where HRW(i) == self:
                    shared-dm.Send(obj_i, hdr.Opaque = TD.UUID + i) → TD
*/

// Shared-DM registration lifecycle: xaction registers once with shared-dm transport
// when first work item needs receiving (PrepRx w/ receiving=true), then
// unregisters once at xaction end (Abort/fini). No per-work-item ref-counting
// needed since UnregRecv is permissive no-op when not registered.

// TODO:
// - test: inject/simulate random aborts
// - ctlmsg
// - soft errors other than not-found; unified error formatting
// - mem-pool: basewi; apc.MossReq; apc.MossResp
// - range read (can wait)
// - finer grained abort: one work item (can wait)

// tunables
const (
	iniwait = time.Second
	maxwait = time.Minute
)

type (
	mossFactory struct {
		xreg.RenewBase
		xctn       *XactMoss
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
		aw   archive.Writer
		r    *XactMoss
		smap *meta.Smap
		req  *apc.MossReq
		resp *apc.MossResp
		sgl  *memsys.SGL // multipart (buffered) only
		wid  string      // work item ID
		size int64
		cnt  int
		// Rx
		recv struct {
			ch   chan int
			m    []rxentry
			next int
			mtx  *sync.Mutex
		}
		clean atomic.Bool
		awfin atomic.Bool
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
		xact.DemandBase
		gmm      *memsys.MMSA
		smm      *memsys.MMSA
		pending  sync.Map // [wid => *basewi]
		activeWG sync.WaitGroup
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
	if cmn.Rom.FastV(5, cos.SmoduleXs) {
		nlog.Infoln(core.T.String(), "factory.Start:", p.xctn.String())
	}
	return nil
}

func (p *mossFactory) WhenPrevIsRunning(prev xreg.Renewable) (xreg.WPR, error) {
	if prev.UUID() == p.UUID() {
		return xreg.WprUse, nil
	}
	if p.designated {
		if cmn.Rom.FastV(5, cos.SmoduleXs) {
			nlog.Infoln(core.T.String(), "DT prev:", prev.UUID(), "curr:", p.UUID(), "- using prev...")
		}
		return xreg.WprUse, nil
	}
	return xreg.WprKeepAndStartNew, nil
}

func newMoss(p *mossFactory) *XactMoss {
	r := &XactMoss{
		gmm: memsys.PageMM(),
		smm: memsys.ByteMM(),
	}
	r.DemandBase.Init(p.UUID(), p.Kind(), "" /*ctlmsg*/, p.Bck, mossIdleTime, r.fini)

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
	o.Hdr.ObjName = err.Error()
	e := bundle.SDM.Bcast(o, nil /*roc*/) // receive via sntl.rxAbort
	if cmn.Rom.FastV(4, cos.SmoduleXs) {
		nlog.Infoln(r.Name(), core.T.String(), "bcast abort [", err, e, "]")
	}
}

func (r *XactMoss) Abort(err error) bool {
	if !r.DemandBase.Abort(err) {
		return false
	}

	// make sure all asm() and Send() exited
	r.activeWG.Wait()

	r.pending.Range(r.cleanup)

	r.DemandBase.Stop()

	// see "Shared-DM registration lifecycle" note above
	bundle.SDM.UnregRecv(r.ID())

	return true
}

// a callback to cleanup all pending work items
func (r *XactMoss) cleanup(key, value any) bool {
	wi := value.(*basewi)
	wi.cleanup()
	r.pending.Delete(key)
	return true
}

// terminate via (<-- xact.Demand <-- hk)
func (r *XactMoss) fini(int64) (d time.Duration) {
	switch {
	case r.IsAborted() || r.Finished():
		return hk.UnregInterval
	case r.Pending() > 0:
		return mossIdleTime
	default:
		nlog.Infoln(r.Name(), "idle expired - finishing")

		r.pending.Range(r.cleanup)

		// see "Shared-DM registration lifecycle" note above
		bundle.SDM.UnregRecv(r.ID())
		r.Finish()
		return hk.UnregInterval
	}
}

// (phase 1)
func (r *XactMoss) PrepRx(req *apc.MossReq, smap *meta.Smap, wid string, receiving bool) error {
	var (
		resp = &apc.MossResp{UUID: r.ID()}
		wi   = basewi{r: r, smap: smap, req: req, resp: resp, wid: wid}
	)
	if receiving {
		// see "Shared-DM registration lifecycle" note above
		bundle.SDM.RegRecv(r)

		// Rx state
		wi.recv.m = make([]rxentry, len(req.In))    // preallocate
		wi.recv.ch = make(chan int, len(req.In)<<1) // extra cap
		wi.recv.mtx = &sync.Mutex{}
	}

	if _, loaded := r.pending.LoadOrStore(wid, &wi); loaded {
		err := fmt.Errorf("%s: work item %q already exists (duplicate prep-rx?)", r.Name(), wid)
		debug.AssertNoErr(err)
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
		debug.AssertNoErr(err)
		return err
	}
	wi := a.(*basewi)
	debug.Assert(wid == wi.wid)

	r.IncPending()
	err := r.asm(req, w, wi)
	r.DecPending()

	wi.cleanup()

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
		if cmn.Rom.FastV(5, cos.SmoduleXs) {
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
	sgl.Free()

	if cmn.Rom.FastV(5, cos.SmoduleXs) {
		nlog.Infoln(r.Name(), core.T.String(), "done multipart Assemble", basewi.wid, "err", err)
	}
	return err
}

// send all requested local data => DT (tsi)
// (phase 2)

func (r *XactMoss) Send(req *apc.MossReq, smap *meta.Smap, tsi *meta.Snode, wid string) error {
	// to receive opAbort
	bundle.SDM.RegRecv(r)

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
		lom, local, err := r._lom(in, smap)
		if err != nil {
			return err
		}
		if !local {
			continue // skip
		}

		var (
			nameInArch = in.NameInRespArch(lom.Bck().Name, req.OnlyObjName)
		)
		lom.Lock(false)

		if in.ArchPath == "" {
			err = r._sendreg(tsi, lom, wid, nameInArch, i)
		} else {
			err = r._sendarch(tsi, lom, wid, nameInArch, in.ArchPath, i)
		}
		if err != nil {
			return err
		}
	}

	if cmn.Rom.FastV(5, cos.SmoduleXs) {
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
		nameInArch = apc.MossMissingDir + "/" + nameInArch
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

	o.Callback, o.CmplArg = r.regSent, opaque
	return bundle.SDM.Send(o, roc, tsi, r)
}

// TODO -- FIXME: stats: add objSentCB to count OutObjsAdd(1, hdr.ObjAttrs.Size)
func (r *XactMoss) regSent(_ *transport.ObjHdr, _ io.ReadCloser, arg any, _ error) {
	opaque, ok := arg.([]byte)
	debug.Assert(ok)
	r.smm.Free(opaque)
}

func (r *XactMoss) _sendarch(tsi *meta.Snode, lom *core.LOM, wid, nameInArch, archpath string, index int) error {
	var (
		roc     cos.ReadOpenCloser
		oah     cos.SimpleOAH
		lh, err = lom.NewHandle(false /*loaded*/)
	)
	mopaque := &mossOpaque{
		WID:   wid,
		Oname: lom.ObjName + "/" + archpath,
		Index: int32(index),
	}
	nameInArch += "/" + archpath
	if err != nil {
		mopaque.Missing = true
		mopaque.Emsg = err.Error()
		nameInArch = apc.MossMissingDir + "/" + nameInArch
	} else {
		csl, err := lom.NewArchpathReader(lh, archpath, "" /*mime*/)
		if err != nil {
			nameInArch = apc.MossMissingDir + "/" + nameInArch
			mopaque.Missing = true
			mopaque.Emsg = err.Error()
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
	o.Callback = r.archSent
	o.CmplArg = &struct {
		lom *core.LOM
		buf []byte
	}{lom, opaque}

	return bundle.SDM.Send(o, roc, tsi, r)
}

// TODO -- FIXME: stats: add objSentCB to count OutObjsAdd(1, hdr.ObjAttrs.Size)
func (r *XactMoss) archSent(_ *transport.ObjHdr, _ io.ReadCloser, arg any, _ error) {
	ctx, ok := arg.(*struct {
		lom *core.LOM
		buf []byte
	})
	debug.Assert(ok)
	debug.Assert(ctx.lom.IsLocked() == apc.LockRead)
	ctx.lom.Unlock(false)
	r.smm.Free(ctx.buf)
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
	data := &mossOpaque{}
	if err := unpacker.ReadAny(data); err != nil {
		return nil, fmt.Errorf("%s: failed to unpack opaque data: %w", r.Name(), err)
	}
	return data, nil
}

// demux -> wi.recv()
// note convention: received hdr.ObjName is `nameInArch` (ie., filename in resulting TAR)
func (r *XactMoss) RecvObj(hdr *transport.ObjHdr, reader io.Reader, err error) error {
	if err != nil {
		nlog.Errorln(err)
		return err
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
		return fmt.Errorf("work item %q not found", mopaque.WID)
	}
	wi := a.(*basewi)

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

func (r *XactMoss) _lom(in *apc.MossIn, smap *meta.Smap) (lom *core.LOM, local bool, err error) {
	bck, err := r._bucket(in)
	if err != nil {
		return nil, false, err
	}

	lom = &core.LOM{ObjName: in.ObjName}
	if err := lom.InitBck(bck); err != nil {
		return nil, false, err
	}

	_, local, err = lom.HrwTarget(smap)
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
		bck = &cmn.Bck{Name: in.Bucket, Provider: np}
	}

	return bck, nil
} ////////////
// basewi //
////////////

func (wi *basewi) receiving() bool { return wi.recv.m != nil }

func (wi *basewi) cleanup() {
	if !wi.clean.CAS(false, true) {
		return
	}
	r := wi.r
	if wi.awfin.CAS(false, true) {
		err := wi.aw.Fini()
		wi.aw = nil
		if err != nil {
			if cmn.Rom.FastV(5, cos.SmoduleXs) {
				nlog.Warningln(r.Name(), core.T.String(), "cleanup: err fini()", wi.wid, err)
			}
		}
	}
	if !wi.receiving() {
		return
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

	if cmn.Rom.FastV(5, cos.SmoduleXs) {
		nlog.Infoln(r.Name(), core.T.String(), "cleanup: done", wi.wid)
	}
}

// handle receive for this work item
// (note: ObjHdr and its fields must be consumed synchronously)
func (wi *basewi) recvObj(index int, hdr *transport.ObjHdr, reader io.Reader, mopaque *mossOpaque) (err error) {
	if index < 0 || index >= len(wi.recv.m) {
		err := fmt.Errorf("%s: out-of-bounds index %d (recv'd len=%d, wid=%s)",
			wi.r.Name(), index, len(wi.recv.m), wi.wid)
		debug.AssertNoErr(err)
		return err
	}
	var (
		sgl  *memsys.SGL
		size int64
	)
	if hdr.IsHeaderOnly() {
		debug.Assert(hdr.ObjAttrs.Size == 0, hdr.ObjName, " size: ", hdr.ObjAttrs.Size)
		goto add
	}

	sgl = wi.r.gmm.NewSGL(0)
	size, err = io.Copy(sgl, reader)
	if err != nil {
		sgl.Free()
		err = fmt.Errorf("failed to receive %s: %w", hdr.ObjName, err)
		nlog.Warningln(err)
		return err
	}
	debug.Assert(size == sgl.Len(), size, " vs ", sgl.Len())

add:
	wi.recv.mtx.Lock()
	entry := &wi.recv.m[index]
	debug.Assertf(entry.sgl == nil && entry.nameInArch == "", "duplicated receive[%d]: %q vs %q", index, hdr.ObjName, entry.nameInArch) // TODO -- FIXME: + abort
	wi.recv.m[index] = rxentry{
		sgl:        sgl,
		bucket:     cos.StrDup(hdr.Bck.Name),
		nameInArch: cos.StrDup(hdr.ObjName),
		mopaque:    mopaque,
	}
	wi.recv.mtx.Unlock()

	wi.recv.ch <- index

	if cmn.Rom.FastV(5, cos.SmoduleXs) {
		nlog.Infoln(wi.r.Name(), core.T.String(), "Rx [ wid:", wi.wid, "index:", index, "oname:", hdr.ObjName, "size:", size, "]")
	}
	return nil
}

func (wi *basewi) waitFlushRx(i int) (int, error) {
	for {
		err := wi.waitAnyRx(iniwait)
		if err != nil {
			return 0, err
		}

		var j int
		wi.recv.mtx.Lock()
		err = wi.flushRx()
		j = wi.recv.next
		wi.recv.mtx.Unlock()

		if err != nil {
			return 0, err
		}
		if j <= i {
			continue // hole `i` remains
		}
		return j, nil
	}
}

func (wi *basewi) waitAnyRx(sleep time.Duration) error {
	var (
		total time.Duration
		l     = len(wi.recv.m)
		timer = time.NewTimer(sleep)
	)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			var (
				entry     *rxentry
				index     int
				entryDone bool
			)
			total += sleep
			if cmn.Rom.FastV(5, cos.SmoduleXs) {
				nlog.Infoln(wi.r.Name(), core.T.String(), ">>>>> waitRx tick", wi.wid, total)
			}

			wi.recv.mtx.Lock() // -----------
			index = wi.recv.next
			if index < l {
				entry = &wi.recv.m[index]
				entryDone = entry.isLocal() || !entry.isEmpty()
			} else {
				entryDone = true
			}
			wi.recv.mtx.Unlock() // -----------

			if cmn.Rom.FastV(5, cos.SmoduleXs) {
				if entry != nil {
					nlog.Infoln("\t\t>>>>> next:", wi.wid, index, entry.nameInArch, entry.local)
				} else {
					nlog.Infoln("\t\t>>>>> done:", wi.wid, index, len(wi.recv.m), entryDone)
				}
			}
			if entryDone {
				return nil
			}

			// TODO -- FIXME: [resilience] timeout must trigger a "pointed" retry (re-send)
			if total > maxwait {
				return fmt.Errorf("%s: timed out waiting for %d \"hole\" to fill [ %s, wid=%s, total-wait=%v ]",
					wi.r.Name(), index, core.T.String(), wi.wid, total)
			}

			if n := int64(total / iniwait); n == 8 || n == 16 || n == 24 {
				sleep = min(sleep<<1, maxwait>>3)
			}
			timer.Reset(sleep)

		case _, ok := <-wi.recv.ch:
			if !ok {
				return errStopped
			}
			return nil

		case <-wi.r.ChanAbort():
			return errStopped
		}
	}
}

func (wi *basewi) next(i int) (int, error) {
	var (
		r  = wi.r
		in = &wi.req.In[i]
	)
	if err := _assertNoRange(in); err != nil {
		return 0, err
	}

	lom, local, err := r._lom(in, wi.smap)
	if err != nil {
		return 0, err
	}

	if !local {
		if !wi.receiving() {
			err := fmt.Errorf("%s: unexpected non-local %s when _not_ receiving [%s, %s]", wi.r.Name(), lom.Cname(), wi.wid, wi.smap)
			debug.AssertNoErr(err)
			return 0, err
		}

		return wi.waitFlushRx(i)
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
	err = wi.write(lom, in.ArchPath, &out, nameInArch, wi.req.ContinueOnErr)
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
	wi.cnt++
	wi.size += out.Size

	if cmn.Rom.FastV(5, cos.SmoduleXs) {
		nlog.Infoln(wi.r.Name(), "archived cnt:", wi.cnt, "[", nameInArch, cos.ToSizeIEC(out.Size, 2), "]")
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

func (wi *basewi) write(lom *core.LOM, archpath string, out *apc.MossOut, nameInArch string, contOnErr bool) error {
	lom.Lock(false)
	err := wi._write(lom, archpath, out, nameInArch, contOnErr)
	lom.Unlock(false)
	return err
}

// (under rlock)
func (wi *basewi) _write(lom *core.LOM, archpath string, out *apc.MossOut, nameInArch string, contOnErr bool) error {
	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		if cos.IsNotExist(err, 0) && contOnErr {
			err = wi.addMissing(err, nameInArch, out)
		}
		return err
	}

	lmfh, err := lom.Open()
	if err != nil {
		if cos.IsNotExist(err, 0) && contOnErr {
			err = wi.addMissing(err, nameInArch, out)
		}
		return err
	}

	switch {
	case archpath != "":
		err = wi._txarch(lom, lmfh, out, nameInArch, archpath, contOnErr)
	default:
		err = wi._txreg(lom, lmfh, out, nameInArch)
	}
	cos.Close(lmfh)
	return err
}

func (wi *basewi) _txreg(lom *core.LOM, lmfh cos.LomReader, out *apc.MossOut, nameInArch string) error {
	if err := wi.aw.Write(nameInArch, lom, lmfh); err != nil {
		return err
	}
	out.Size = lom.Lsize()
	return nil
}

// (compare w/ goi._txarch and r._sendarch above)
func (wi *basewi) _txarch(lom *core.LOM, lmfh cos.LomReader, out *apc.MossOut, nameInArch, archpath string, contOnErr bool) error {
	nameInArch += "/" + archpath

	csl, err := lom.NewArchpathReader(lmfh, archpath, "" /*mime*/)
	if err != nil {
		if cos.IsNotExist(err, 0) && contOnErr {
			return wi.addMissing(err, nameInArch, out)
		}
		return err
	}

	oah := cos.SimpleOAH{Size: csl.Size()}
	err = wi.aw.Write(nameInArch, &oah, csl)
	csl.Close()

	if err != nil {
		return err
	}
	out.Size = oah.Size
	return nil
}

func (wi *basewi) addMissing(err error, nameInArch string, out *apc.MossOut) error {
	var (
		missingName = apc.MossMissingDir + "/" + nameInArch
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
		wi.recv.next++ // this "hole" is done

		wi.cnt++
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
	wi.r.ObjsAdd(wi.cnt, wi.size)

	if cmn.Rom.FastV(5, cos.SmoduleXs) {
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
	w.Header().Set(cos.HdrContentType, _ctype(wi.req.OutputFormat))
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

	wi.r.ObjsAdd(wi.cnt, wi.size)

	if cmn.Rom.FastV(5, cos.SmoduleXs) {
		nlog.Infoln(wi.r.Name(), "done streaming: [ count:", wi.cnt, "written:", wi.size, "format:", wi.req.OutputFormat, "]")
	}
	return nil
}

func _ctype(outputFormat string) string {
	switch outputFormat {
	case "" /*default*/, archive.ExtTar:
		return cos.ContentTar // not IANA-registered
	case archive.ExtTgz, archive.ExtTarGz:
		return cos.ContentGzip // not registered; widely used for .tar.gz and .tgz
	case archive.ExtTarLz4:
		return "application/x-lz4" // not registered but consistent with .lz4; alternative: "application/octet-stream"
	case archive.ExtZip:
		return cos.ContentZip // IANA-registered
	default:
		debug.Assert(false, outputFormat)
		return cos.ContentBinary
	}
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
