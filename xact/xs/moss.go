// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"archive/tar"
	"errors"
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

// TODO -- FIXME:
// - ctlmsg
// - soft errors other than not-found
// - error handling in general and across the board; mossErr{wrapped-err}
// - stopping sentinels

// TODO:
// - two cleanup()s
// - write checksum
// - range read (_assertNoRange; and separately, read range archpath)

type (
	mossFactory struct {
		xreg.RenewBase
		xctn       *XactMoss
		designated bool
	}
)

type (
	rxdata struct {
		bucket     string
		oname      string
		nameInArch string
		sgl        *memsys.SGL
		local      bool
		missing    bool
	}
	basewi struct {
		aw   archive.Writer
		r    *XactMoss
		smap *meta.Smap
		resp *apc.MossResp
		sgl  *memsys.SGL // multipart (buffered) only
		wid  string      // work item ID
		size int64
		cnt  int
		// Rx
		recv struct {
			ch   chan int
			m    []rxdata
			next int
			mtx  *sync.Mutex
		}
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
		gmm     *memsys.MMSA
		smm     *memsys.MMSA
		pending map[string]*basewi
		pmtx    sync.RWMutex // TODO: optimize
	}
)

const (
	mossIdleTime = xact.IdleDefault

	iniCapPending = 64
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
		gmm:     memsys.PageMM(),
		smm:     memsys.ByteMM(),
		pending: make(map[string]*basewi, iniCapPending),
	}
	r.DemandBase.Init(p.UUID(), p.Kind(), "" /*ctlmsg*/, p.Bck, mossIdleTime, r.fini)
	return r
}

func (*XactMoss) Run(*sync.WaitGroup) { debug.Assert(false) }

func (r *XactMoss) Abort(err error) bool {
	if !r.DemandBase.Abort(err) {
		return false
	}

	r.pmtx.Lock()
	for _, wi := range r.pending {
		wi.cleanup()
	}
	clear(r.pending)
	r.pmtx.Unlock()

	r.DemandBase.Stop()
	bundle.SDM.UnregRecv(r.ID())
	r.Finish()
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
		nlog.Infoln(r.Name(), "idle expired, finishing")

		// Cleanup all pending work items
		r.pmtx.Lock()
		for _, wi := range r.pending {
			wi.cleanup()
		}
		clear(r.pending)
		r.pmtx.Unlock()

		// Unregister from transport if needed
		smap := core.T.Sowner().Get()
		if nat := smap.CountActiveTs(); nat > 1 {
			bundle.SDM.UnregRecv(r.ID())
		}

		r.DemandBase.Stop() // NOTE: stops timer and calls Unreg
		bundle.SDM.UnregRecv(r.ID())
		r.Finish()
		return hk.UnregInterval
	}
}

// TODO -- FIXME: cleanup basewi if failed after PrepRx but prior to Assemble
// (phase 1)
func (r *XactMoss) PrepRx(req *apc.MossReq, smap *meta.Smap, wid string, receiving bool) error {
	if receiving {
		bundle.SDM.RegRecv(r)
	}

	var (
		resp   = &apc.MossResp{UUID: r.ID()}
		basewi = basewi{r: r, smap: smap, resp: resp, wid: wid}
	)
	basewi.recv.m = make([]rxdata, len(req.In))
	basewi.recv.ch = make(chan int, len(req.In)<<1) // NOTE: extra cap
	basewi.recv.mtx = &sync.Mutex{}

	r.pmtx.Lock()
	if _, ok := r.pending[wid]; ok {
		r.pmtx.Unlock()
		err := fmt.Errorf("%s: work item %q already exists", r.Name(), wid)
		debug.AssertNoErr(err)
		return err
	}
	r.pending[wid] = &basewi
	r.pmtx.Unlock()

	return nil
}

// gather other requested data (local and remote); emit resulting archive
// (phase 3)
func (r *XactMoss) Assemble(req *apc.MossReq, w http.ResponseWriter, wid string) error {
	r.pmtx.Lock()
	wi, ok := r.pending[wid]
	r.pmtx.Unlock()
	if !ok {
		err := fmt.Errorf("%s: work item %q not found (prep-rx not done?)", r.Name(), wid)
		debug.AssertNoErr(err)
		return err // TODO: sentinels
	}
	debug.Assert(wid == wi.wid)

	r.IncPending()
	err := r.asm(req, w, wi)
	wi.cleanup()
	r.DecPending()

	return err
}

func (r *XactMoss) asm(req *apc.MossReq, w http.ResponseWriter, basewi *basewi) error {
	opts := archive.Opts{TarFormat: tar.FormatUnknown} // default tar format

	// streaming
	if req.StreamingGet {
		wi := streamwi{basewi: basewi}
		wi.aw = archive.NewWriter(req.OutputFormat, w, nil /*checksum*/, &opts)
		err := wi.asm(req, w)
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
	err := wi.asm(req, w)
	sgl.Free()

	if cmn.Rom.FastV(5, cos.SmoduleXs) {
		nlog.Infoln(r.Name(), core.T.String(), "done multipart Assemble", basewi.wid, "err", err)
	}
	return err
}

// send all requested local data => DT (tsi)
// (phase 2)
func (r *XactMoss) Send(req *apc.MossReq, smap *meta.Smap, tsi *meta.Snode, wid string) (err error) {
	r.IncPending()

	for i := range req.In {
		in := &req.In[i]
		if err := _assertNoRange(in); err != nil {
			return err
		}
		lom, local, err := r._lom(in, smap)
		if err != nil {
			// TODO -- FIXME: this method cannot return errors - must always send or abort-all
			nlog.Errorln(r.Name(), core.T.String(), "FATAL >>>>>>>>>>>>>>>>> err:", err) // DEBUG
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
			r._sendreg(tsi, lom, wid, nameInArch, i)
		} else {
			r._sendarch(tsi, lom, wid, nameInArch, in.ArchPath, i)
		}
	}

	if cmn.Rom.FastV(5, cos.SmoduleXs) {
		nlog.Infoln(r.Name(), core.T.String(), "done Send", wid)
	}

	r.DecPending()
	return err
}

// TODO -- FIXME: this method cannot return errors - must always send, possibly zero-size + error message
func (r *XactMoss) _sendreg(tsi *meta.Snode, lom *core.LOM, wid, nameInArch string, index int) {
	var (
		oah      = lom.ObjAttrs()
		roc, err = lom.NewDeferROC(false /*loaded*/)
	)
	opaqueData := &mossOpaque{
		WID:     wid,
		OName:   lom.ObjName,
		Index:   int32(index),
		Missing: err != nil,
	}
	if err != nil {
		nameInArch = apc.MossMissingDir + "/" + nameInArch
		oah = &cmn.ObjAttrs{}
		roc = nil
	}

	opaque := r.packOpaque(opaqueData)
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
	err = bundle.SDM.Send(o, roc, tsi, r)
	debug.AssertNoErr(err) // DEBUG -- TODO -- FIXME: unify abort-all
}

// TODO -- FIXME: stats: add objSentCB to count OutObjsAdd(1, hdr.ObjAttrs.Size)
func (r *XactMoss) regSent(_ *transport.ObjHdr, _ io.ReadCloser, arg any, _ error) {
	opaque, ok := arg.([]byte)
	debug.Assert(ok)
	r.smm.Free(opaque)
}

// TODO -- FIXME: this method cannot return errors - must always send, possibly zero-size + error message
func (r *XactMoss) _sendarch(tsi *meta.Snode, lom *core.LOM, wid, nameInArch, archpath string, index int) error {
	var (
		roc     cos.ReadOpenCloser
		oah     cos.SimpleOAH
		lh, err = lom.NewHandle(false /*loaded*/)
	)
	opaqueData := &mossOpaque{
		WID:     wid,
		OName:   lom.ObjName + "/" + archpath,
		Index:   int32(index),
		Missing: err != nil,
	}

	nameInArch += "/" + archpath
	if err != nil {
		nameInArch = apc.MossMissingDir + "/" + nameInArch
	} else {
		csl, err := lom.NewArchpathReader(lh, archpath, "" /*mime*/)
		if err != nil {
			nameInArch = apc.MossMissingDir + "/" + nameInArch
			opaqueData.Missing = true
		} else {
			// csl is cos.ReadCloseSizer; see transport/bundle/shared_dm for InitSDM
			roc = cos.NopOpener(csl)
			oah.Size = csl.Size()
		}
	}

	opaque := r.packOpaque(opaqueData)
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

	err = bundle.SDM.Send(o, roc, tsi, r)
	debug.AssertNoErr(err) // DEBUG is legal
	return err
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
// note that received hdr.ObjName is `nameInArch` (ie., filename in resulting TAR)
func (r *XactMoss) RecvObj(hdr *transport.ObjHdr, reader io.Reader, err error) (erm error) {
	erm = r._recvObj(hdr, reader, err)
	if erm != nil {
		nlog.Errorln(r.Name(), core.T.String(), "RecvObj:", erm)
		r.Abort(err)
	}
	return erm
}

// (note: ObjHdr and its fields must be consumed synchronously)
func (r *XactMoss) _recvObj(hdr *transport.ObjHdr, reader io.Reader, err error) error {
	if err != nil {
		return err
	}
	opaqueData, err := r.unpackOpaque(hdr.Opaque)
	if err != nil {
		return err
	}
	r.pmtx.RLock()
	wi := r.pending[opaqueData.WID]
	r.pmtx.RUnlock()
	if wi == nil {
		return fmt.Errorf("WID %q not found", opaqueData.WID)
	}
	return wi.recvObj(int(opaqueData.Index), hdr, reader, opaqueData.OName, opaqueData.Missing)
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
	r := wi.r
	if wi.aw != nil {
		err := wi.aw.Fini()
		wi.aw = nil
		if err != nil {
			if cmn.Rom.FastV(5, cos.SmoduleXs) {
				nlog.Warningln(r.Name(), core.T.String(), "cleanup: err fini()", wi.wid, err)
			}
		}
	}
	if wi.recv.m == nil {
		return
	}

	r.pmtx.Lock()
	delete(r.pending, wi.wid)
	r.pmtx.Unlock()

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
func (wi *basewi) recvObj(index int, hdr *transport.ObjHdr, reader io.Reader, oname string, missing bool) (err error) {
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
	debug.Assertf(entry.sgl == nil && entry.nameInArch == "", "duplicated receive[%d]: %q vs %q", index, hdr.ObjName, entry.nameInArch)
	wi.recv.m[index] = rxdata{
		bucket:     hdr.Bck.Name,
		oname:      oname,
		nameInArch: hdr.ObjName,
		sgl:        sgl,
		missing:    missing,
	}
	wi.recv.mtx.Unlock()

	wi.recv.ch <- index

	if cmn.Rom.FastV(5, cos.SmoduleXs) {
		nlog.Infoln(wi.r.Name(), core.T.String(), "Rx [ wid:", wi.wid, "index:", index, "oname:", hdr.ObjName, "size:", size, "]")
	}
	return nil
}

// wait for _any_ receive (out of order or in)
func (wi *basewi) waitRx() error {
	ticker := time.NewTicker(time.Second) // TODO -- FIXME: expo-backoff or else
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if cmn.Rom.FastV(5, cos.SmoduleXs) {
				nlog.Infoln(wi.r.Name(), core.T.String(), ">>>>> waitRx tick", wi.wid)
			}

			var (
				entry     *rxdata
				i         int
				entryDone bool
			)
			wi.recv.mtx.Lock()
			i = wi.recv.next
			if i < len(wi.recv.m) {
				entry = &wi.recv.m[i]
				entryDone = entry.isLocal() || !entry.isEmpty()
			} else {
				entryDone = true
			}
			wi.recv.mtx.Unlock()

			if entry != nil && cmn.Rom.FastV(5, cos.SmoduleXs) {
				nlog.Infoln("\t\t>>>>> next:", i, entry.nameInArch, entry.local)
			}
			if entryDone {
				return nil
			}
		case _, ok := <-wi.recv.ch:
			if !ok {
				return errStopped
			}
			return nil
			// TODO -- FIXME: sentinels, to prevent a hang: case <- (remote aborted?)
		case <-wi.r.ChanAbort():
			return errStopped
		}
	}
}

func (wi *basewi) next(req *apc.MossReq, i int, streaming bool) (int, error) {
	var (
		r  = wi.r
		in = &req.In[i]
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
			err := errors.New(core.T.String() + " unexpected non-local " + lom.Cname() + " when _not_ receiving")
			debug.AssertNoErr(err)
			return 0, err
		}

		// TODO -- FIXME: [resilience] timeout must trigger a "pointed" retry (re-send)
	wait:
		if err := wi.waitRx(); err != nil {
			return 0, err
		}

		wi.recv.mtx.Lock()
		err := wi.flushRx(streaming)
		j := wi.recv.next
		wi.recv.mtx.Unlock()

		if err != nil {
			return 0, err
		}
		if j <= i {
			goto wait // hole `i` remains
		}
		return j, nil
	}

	bck := lom.Bck()
	out := apc.MossOut{
		ObjName: in.ObjName, ArchPath: in.ArchPath, Bucket: bck.Name, Provider: bck.Provider,
		Opaque: in.Opaque,
	}
	nameInArch := in.NameInRespArch(bck.Name, req.OnlyObjName)
	err = wi.write(lom, in.ArchPath, &out, nameInArch, req.ContinueOnErr)
	if err != nil {
		return 0, err
	}
	if req.StreamingGet {
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
	wi.recv.mtx.Lock()
	entry := &wi.recv.m[i]
	entry.local = true
	debug.Assert(wi.recv.next <= i, wi.recv.next, " vs ", i) // must be contiguous
	wi.recv.mtx.Unlock()
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

func (wi *basewi) asm(req *apc.MossReq, streaming bool) error {
	var (
		r = wi.r
		l = len(req.In)
	)
	for i := 0; i < l; {
		if err := r.AbortErr(); err != nil {
			return err
		}
		j, err := wi.next(req, i, streaming)
		if err != nil {
			r.Abort(err)
			return err
		}
		debug.Assert(j > i && j <= l, i, " vs ", j, " vs ", l)
		i = j
	}
	return nil
}

// drains recv.m[] in strict input order
// is called under `recv.mtx` lock

func (entry *rxdata) isLocal() bool { return entry.local }
func (entry *rxdata) isEmpty() bool { return entry.nameInArch == "" }

func (wi *basewi) flushRx(streaming bool) error {
	for l := len(wi.recv.m); wi.recv.next < l; {
		var (
			err   error
			size  int64
			entry = &wi.recv.m[wi.recv.next]
		)
		if entry.isLocal() {
			wi.recv.next++
			continue
		}
		if entry.isEmpty() {
			return nil
		}

		wi.recv.mtx.Unlock() //--------------

		if entry.missing {
			debug.Assert(strings.HasPrefix(entry.nameInArch, apc.MossMissingDir+"/"), entry.nameInArch)
			err = wi.aw.Write(entry.nameInArch, cos.SimpleOAH{Size: 0}, nopROC{})
		} else {
			size = entry.sgl.Len()
			oah := cos.SimpleOAH{Size: size}
			err = wi.aw.Write(entry.nameInArch, oah, entry.sgl)
		}
		if err == nil && streaming {
			err = wi.aw.Flush()
		}
		wi.recv.mtx.Lock() //--------------

		if err != nil {
			return err
		}

		if !streaming {
			var emsg string
			if entry.missing {
				emsg = entry.oname + " does not exist" // TODO: pack error message as well
			}
			wi.resp.Out = append(wi.resp.Out, apc.MossOut{Bucket: entry.bucket, ObjName: entry.oname, Size: size, ErrMsg: emsg})
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

func (wi *buffwi) asm(req *apc.MossReq, w http.ResponseWriter) error {
	if err := wi.basewi.asm(req, false); err != nil {
		return err
	}

	// flush and close aw
	err := wi.aw.Fini()
	wi.aw = nil
	if err != nil {
		return err
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
		nlog.Infoln(wi.r.Name(), "done buffered: [ count:", len(wi.resp.Out), "written:", written, "format:", req.OutputFormat, "]")
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

func (wi *streamwi) asm(req *apc.MossReq, w http.ResponseWriter) error {
	w.Header().Set(cos.HdrContentType, _ctype(req.OutputFormat))
	if err := wi.basewi.asm(req, true); err != nil {
		nlog.Warningln(wi.r.Name(), cmn.ErrGetTxBenign, "[", err, "]")
		return cmn.ErrGetTxBenign
	}

	// flush and close aw
	err := wi.aw.Fini()
	wi.aw = nil
	if err != nil {
		nlog.Warningln(wi.r.Name(), cmn.ErrGetTxBenign, "[", err, "]")
		return cmn.ErrGetTxBenign
	}

	wi.r.ObjsAdd(wi.cnt, wi.size)

	if cmn.Rom.FastV(5, cos.SmoduleXs) {
		nlog.Infoln(wi.r.Name(), "done streaming: [ count:", wi.cnt, "written:", wi.size, "format:", req.OutputFormat, "]")
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
// mossOpaque //
// //////////////

type mossOpaque struct {
	WID     string
	OName   string
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
	packer.WriteString(o.OName)
	packer.WriteInt32(o.Index)
	packer.WriteBool(o.Missing)
}

func (o *mossOpaque) PackedSize() int {
	return cos.PackedStrLen(o.WID) + cos.PackedStrLen(o.OName) + cos.SizeofI32 + 1
}

// Implement cos.Unpacker interface
func (o *mossOpaque) Unpack(unpacker *cos.ByteUnpack) error {
	var err error
	if o.WID, err = unpacker.ReadString(); err != nil {
		return err
	}
	if o.OName, err = unpacker.ReadString(); err != nil {
		return err
	}
	if o.Index, err = unpacker.ReadInt32(); err != nil {
		return err
	}
	if o.Missing, err = unpacker.ReadBool(); err != nil {
		return err
	}
	return nil
}
