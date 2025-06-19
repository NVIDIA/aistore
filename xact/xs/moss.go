// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"archive/tar"
	"encoding/binary"
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
		xctn *XactMoss
	}
)

type (
	rxdata struct {
		name      string
		sgl       *memsys.SGL
		isMissing bool
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
		basewi
	}
	streamwi struct {
		basewi
	}
)

type (
	XactMoss struct {
		xact.DemandBase
		mm      *memsys.MMSA
		pending map[string]*basewi
		pmtx    sync.RWMutex // TODO: optimize
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
	return &mossFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}}
}

func (p *mossFactory) Start() error {
	debug.Assert(cos.IsValidUUID(p.Args.UUID), p.Args.UUID)
	req, ok := p.Args.Custom.(*apc.MossReq)
	debug.Assert(ok)

	_ = req // TODO: validate?

	p.xctn = newMoss(p)
	return nil
}

func (*mossFactory) Kind() string     { return apc.ActGetBatch }
func (p *mossFactory) Get() core.Xact { return p.xctn }

func (*mossFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprUse, nil
}

func newMoss(p *mossFactory) *XactMoss {
	r := &XactMoss{
		mm:      memsys.PageMM(),
		pending: make(map[string]*basewi, 64), // TODO: initial
	}
	r.DemandBase.Init(p.UUID(), p.Kind(), "" /*ctlmsg*/, p.Bck, mossIdleTime, r.fini)
	return r
}

func (r *XactMoss) Run(wg *sync.WaitGroup) {
	nlog.Infoln(r.Name(), "starting")

	// Check if we'll be receiving (any targets other than self)
	smap := core.T.Sowner().Get()
	if nat := smap.CountActiveTs(); nat > 1 {
		if err := bundle.SDM.Open(); err != nil {
			r.AddErr(err, 5, cos.SmoduleXs)
			return
		}
		bundle.SDM.RegRecv(r)
	}

	wg.Done()
}

func (r *XactMoss) Abort(err error) bool {
	if !r.DemandBase.Abort(err) {
		return false
	}

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

	r.DemandBase.Stop()
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

		r.DemandBase.Stop()
		r.Finish()
		return hk.UnregInterval
	}
}

// handle req; gather other target's data; emit resulting TAR, et. al formats
func (r *XactMoss) Assemble(req *apc.MossReq, w http.ResponseWriter, wid string) error {
	var (
		resp   = &apc.MossResp{UUID: r.ID()}
		opts   = archive.Opts{TarFormat: tar.FormatUnknown} // default tar format
		basewi = basewi{
			r:    r,
			smap: core.T.Sowner().Get(),
			resp: resp,
			wid:  wid,
		}
	)

	// initialize Rx state iff there are senders
	smap := basewi.smap
	if nat := smap.CountActiveTs(); nat > 1 {
		basewi.recv.m = make([]rxdata, len(req.In))
		basewi.recv.ch = make(chan int, len(req.In))
		basewi.recv.mtx = &sync.Mutex{}

		r.pmtx.Lock()

		if _, ok := r.pending[wid]; ok {
			r.pmtx.Unlock()
			err := fmt.Errorf("%s: work item %q already exists", r.Name(), wid)
			debug.AssertNoErr(err)
			return err // TODO: sentinels
		}

		r.pending[wid] = &basewi
		r.pmtx.Unlock()
	}

	r.IncPending()
	defer basewi.cleanup()

	// streaming
	if req.StreamingGet {
		wi := streamwi{basewi: basewi}
		wi.aw = archive.NewWriter(req.OutputFormat, w, nil /*checksum*/, &opts)
		return wi.asm(req, w)
	}

	// buffered
	var (
		sgl = r.mm.NewSGL(0)
		wi  = buffwi{basewi: basewi}
	)
	wi.sgl = sgl
	wi.resp.Out = make([]apc.MossOut, 0, len(req.In))
	wi.aw = archive.NewWriter(req.OutputFormat, sgl, nil /*checksum*/, &opts)
	err := wi.asm(req, w)
	sgl.Free()
	return err
}

// send all requested local data => tsi
func (r *XactMoss) Send(req *apc.MossReq, smap *meta.Smap, tsi *meta.Snode, wid string) (err error) {
	r.IncPending()

	for i := range req.In {
		err = r._send(req, smap, tsi, wid, i)
		if err != nil {
			r.Abort(err)
			break
		}
	}

	r.DecPending()
	return err
}

func (r *XactMoss) _send(req *apc.MossReq, smap *meta.Smap, tsi *meta.Snode, wid string, i int) error {
	in := &req.In[i]
	if err := _assertNoRange(in); err != nil {
		return err
	}

	lom, local, err := r._lom(in, smap)
	if err != nil {
		return err
	}
	if !local {
		return nil // skip
	}

	bck := lom.Bck()
	nameInArch := in.NameInRespArch(bck.Name, req.OnlyObjName)

	lom.Lock(false)
	roc, err := lom.NewDeferROC(false /*loaded*/)
	if err != nil {
		if !req.ContinueOnErr || !cos.IsNotExist(err, 0) {
			return err
		}
		nameInArch = apc.MossMissingDir + "/" + nameInArch
		roc = nopROC{}
	}

	o := transport.AllocSend()
	hdr := &o.Hdr
	{
		hdr.Bck = *lom.Bucket()
		hdr.ObjName = nameInArch
		hdr.ObjAttrs.CopyFrom(lom.ObjAttrs(), false /*skip cksum*/)
		hdr.Opaque = r._makeOpaque(i, wid)
	}
	bundle.SDM.Send(o, roc, tsi)
	return nil
}

// layout:
// [ --- xid lstring --- | --- wid lstring --- | --- index uint32 --- ]
// (where lstring is length-prefixed string)
func (r *XactMoss) _makeOpaque(index int, wid string) (b []byte) {
	var (
		xid    = r.ID()
		lx, lw = len(xid), len(wid)
		off    int
	)
	// TODO -- FIXME: use T.ByteMM(); consider cos/bytepack
	b = make([]byte, cos.SizeofI16+lx+cos.SizeofI16+lw+cos.SizeofI32)

	binary.BigEndian.PutUint16(b, uint16(lx))
	off += cos.SizeofI16
	copy(b[off:], xid)
	off += lx
	binary.BigEndian.PutUint16(b, uint16(lw))
	off += cos.SizeofI16
	copy(b[off:], wid)
	off += lw
	binary.BigEndian.PutUint32(b[off:], uint32(index))
	return b
}

// see layout above
func (r *XactMoss) _parseOpaque(opaque []byte) (wid string, index int, _ error) {
	var (
		off int
		lo  = len(opaque)
	)
	if lo < cos.SizeofI16+cos.SizeofI16+cos.SizeofI32 {
		return "", 0, fmt.Errorf("%s: opaque data too short: %d bytes", r.Name(), lo)
	}

	// xid
	lx := int(binary.BigEndian.Uint16(opaque))
	off += cos.SizeofI16
	if off+lx > lo {
		return "", 0, fmt.Errorf("%s: invalid xaction ID length: %d", r.Name(), lx)
	}
	xid := string(opaque[off : off+lx])
	if xid != r.ID() {
		return "", 0, fmt.Errorf("%s: xaction routing mismatch: %q vs %q", r.Name(), xid, r.ID())
	}
	off += lx

	// wid
	if off+cos.SizeofI16 > lo {
		return "", 0, fmt.Errorf("%s: opaque data truncated at WID: %d", r.Name(), lo)
	}
	lw := int(binary.BigEndian.Uint16(opaque[off:]))
	off += cos.SizeofI16
	if off+lw > lo {
		return "", 0, fmt.Errorf("invalid WID length: %d", lw)
	}
	wid = string(opaque[off : off+lw])
	off += lw

	// index
	if off+cos.SizeofI32 > lo {
		return "", 0, fmt.Errorf("%s: opaque data truncated at MossIn index: [%d, %s]", r.Name(), lo, wid)
	}
	index = int(binary.BigEndian.Uint32(opaque[off:]))

	debug.Assert(lo == off+cos.SizeofI32, lo, " vs ", off+cos.SizeofI32)
	return wid, index, nil
}

// demux -> wi.recv()
// note that received hdr.ObjName is `nameInArch` (ie., filename in resulting TAR)
func (r *XactMoss) RecvObj(hdr *transport.ObjHdr, reader io.Reader, err error) error {
	if err != nil {
		r.Abort(err)
		return err
	}

	wid, index, err := r._parseOpaque(hdr.Opaque)
	if err != nil {
		r.Abort(err)
		return err
	}

	// find work item
	r.pmtx.RLock()
	wi := r.pending[wid]
	r.pmtx.RUnlock()

	if wi == nil {
		err = fmt.Errorf("WID %q not found", wid)
		nlog.Warningln(err)
		return err
	}

	return wi.recvObj(index, hdr, reader)
}

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
		wi.aw.Fini()
	}

	if wi.recv.m != nil {
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
	}

	r.DecPending()
}

// handle receive for this work item
func (wi *basewi) recvObj(index int, hdr *transport.ObjHdr, reader io.Reader) error {
	if index < 0 || index >= len(wi.recv.m) {
		err := fmt.Errorf("%s: out-of-bounds index %d (recv'd len=%d, wid=%s)",
			wi.r.Name(), index, len(wi.recv.m), wi.wid)
		debug.AssertNoErr(err)
		return err
	}

	var (
		written int64
		sgl     = wi.r.mm.NewSGL(0)
	)
	written, err := io.Copy(sgl, reader)
	if err != nil {
		sgl.Free()
		err = fmt.Errorf("failed to receive %s: %w", hdr.ObjName, err)
		nlog.Warningln(err)
		return err
	}
	debug.Assert(written == sgl.Len(), written, " vs ", sgl.Len())

	isMissing := strings.HasPrefix(hdr.ObjName, apc.MossMissingDir+"/")

	wi.recv.mtx.Lock()
	entry := &wi.recv.m[index]
	debug.Assertf(entry.sgl == nil && entry.name == "", "duplicated receive[%d]: %q vs %q", index, hdr.ObjName, entry.name)
	wi.recv.m[index] = rxdata{
		name:      hdr.ObjName,
		sgl:       sgl,
		isMissing: isMissing,
	}
	wi.recv.mtx.Unlock()

	wi.recv.ch <- index

	if cmn.Rom.FastV(5, cos.SmoduleXs) {
		nlog.Infof("%s: received[%d] %q (%dB)", wi.r.Name(), index, hdr.ObjName, sgl.Len())
	}
	return nil
}

// Wait for any object to be received
func (wi *basewi) waitRx() error {
	select {
	case _, ok := <-wi.recv.ch:
		if !ok {
			return errStopped
		}
		return nil
	case <-wi.r.ChanAbort():
		return errStopped
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
			return 0, errors.New("unexpected non-local " + lom.Cname() + " when _not_ receiving")
		}

		// Wait for any received object, then try to flush
	wait:
		if err := wi.waitRx(); err != nil {
			return 0, err
		}

		wi.recv.mtx.Lock()
		if wi.recv.next < i {
			wi.recv.next = i
		}
		err := wi.flushRx(streaming)
		j := wi.recv.next
		wi.recv.mtx.Unlock()

		if err != nil {
			return 0, err
		}
		if j <= i {
			goto wait // Object i not yet received/processed
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
		if req.StreamingGet {
			nlog.Warningln(wi.r.Name(), cmn.ErrGetTxBenign, "[", err, "]")
			err = cmn.ErrGetTxBenign
		}
		return 0, err
	}
	if req.StreamingGet {
		if err := wi.aw.Flush(); err != nil {
			nlog.Warningln(wi.r.Name(), cmn.ErrGetTxBenign, "[", err, "]")
			return 0, cmn.ErrGetTxBenign
		}
	} else {
		wi.resp.Out = append(wi.resp.Out, out)
	}
	wi.cnt++
	wi.size += out.Size

	if cmn.Rom.FastV(5, cos.SmoduleXs) {
		nlog.Infoln(wi.r.Name(), "archived cnt:", wi.cnt, "[", nameInArch, cos.ToSizeIEC(out.Size, 2), "]")
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

// (compare w/ goi._txarch)
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
func (wi *basewi) flushRx(streaming bool) (err error) {
	for wi.recv.next < len(wi.recv.m) {
		entry := &wi.recv.m[wi.recv.next]
		if entry.sgl == nil && !entry.isMissing {
			break // haven't received this one yet — stop
		}

		var size int64
		if entry.isMissing {
			err = wi.aw.Write(entry.name, cos.SimpleOAH{Size: 0}, nopROC{})
		} else {
			size = entry.sgl.Len()
			oah := cos.SimpleOAH{Size: size}
			err = wi.aw.Write(entry.name, oah, entry.sgl) // --> http.ResponseWriter
		}
		if err != nil {
			return err
		}
		if !streaming {
			out := apc.MossOut{ObjName: entry.name}
			if entry.isMissing {
				out.ErrMsg = "moss: missing object (recv)" // TODO: specific err
			} else {
				out.Size = size
			}
			wi.resp.Out = append(wi.resp.Out, out)
		}

		// GC to release memory
		if entry.sgl != nil {
			entry.sgl.Free()
			entry.sgl = nil
		}
		wi.recv.m[wi.recv.next] = rxdata{}
		wi.recv.next++

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
		return erw
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
	written, err := io.Copy(part2, wi.sgl)
	if err != nil {
		nlog.Warningln(wi.r.Name(), cmn.ErrGetTxBenign, "[", err, "]")
		return 0, cmn.ErrGetTxBenign
	}
	return written, nil
}

//////////////
// streamwi //
//////////////

func (wi *streamwi) asm(req *apc.MossReq, w http.ResponseWriter) error {
	w.Header().Set(cos.HdrContentType, _ctype(req.OutputFormat))
	if err := wi.basewi.asm(req, true); err != nil {
		return err
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
