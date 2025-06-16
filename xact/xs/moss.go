// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"archive/tar"
	"encoding/binary"
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
                   ├── shared-dm.recvd[i] = ...     ← entries from remote targets via recv()
                   │
                   └── stream TAR         ← in-order via flushMaybe()

other-targets (T1..Tn)
    └── Send(req, smap, TD)
           └── for i where HRW(i) == self:
                    shared-dm.Send(obj_i, hdr.Opaque = TD.UUID + i) → TD
*/

// TODO -- FIXME:
// - recv() and generally, multi-target
// - read from shards
// - enable ais/test/moss tests - rm skipf
// - ctlmsg
// - soft errors other than not-found
// - error handling in general and across the board; mossErr{wrapped-err}
// - sentinels

// TODO:
// - write checksum
// - range read (and separately, read range archpath)

type (
	mossFactory struct {
		xreg.RenewBase
		xctn *XactMoss
	}
	mossInRecv struct {
		name      string
		sgl       *memsys.SGL
		isMissing bool
	}
	mossWorkItem interface {
		flushMaybe() error
	}
	XactMoss struct {
		xact.DemandBase
		mm *memsys.MMSA
		wi mossWorkItem // NOTE: one per
		// runtime
		recvd    []mossInRecv
		nextRecv int
		mu       sync.Mutex
	}
)

type (
	basewi struct {
		aw   archive.Writer
		r    *XactMoss
		smap *meta.Smap
		resp *apc.MossResp
		cnt  int
		size int64
	}
	buffwi struct {
		basewi
		sgl *memsys.SGL
	}
	streamwi struct {
		basewi
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
	p.xctn = newMoss(p, req)
	return nil
}

func (*mossFactory) Kind() string     { return apc.ActGetBatch }
func (p *mossFactory) Get() core.Xact { return p.xctn }

func (*mossFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprUse, nil
}

func newMoss(p *mossFactory, req *apc.MossReq) *XactMoss {
	r := &XactMoss{mm: memsys.PageMM()}
	r.DemandBase.Init(p.UUID(), p.Kind(), "" /*ctlmsg*/, p.Bck, mossIdleTime, r.fini)

	// NOTE: recv() readiness - preallocate all required empty slots
	smap := core.T.Sowner().Get()
	if nat := smap.CountActiveTs(); nat > 1 {
		r.recvd = make([]mossInRecv, len(req.In)) // usingSharedDM
	}
	return r
}

func (r *XactMoss) Run(wg *sync.WaitGroup) {
	nlog.Infoln(r.Name(), "starting")

	if r.usingSharedDM() {
		if err := bundle.SDM.Open(); err != nil {
			r.AddErr(err, 5, cos.SmoduleXs)
			return
		}
	}

	wg.Done()

	if r.usingSharedDM() {
		bundle.SDM.RegRecv(r)
	}
}

func (r *XactMoss) Abort(err error) bool {
	if !r.DemandBase.Abort(err) {
		return false
	}

	if r.usingSharedDM() {
		bundle.SDM.UnregRecv(r.ID())
	}
	r.DemandBase.Stop()
	r.cleanup()
	r.Finish()
	return true
}

func (r *XactMoss) cleanup() {
	if r.recvd == nil {
		return
	}
	r.mu.Lock()
	for i := range r.recvd {
		entry := &r.recvd[i]
		if entry.sgl != nil {
			entry.sgl.Free()
			entry.sgl = nil
		}
	}
	clear(r.recvd)
	r.recvd = nil
	r.mu.Unlock()
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
		if r.usingSharedDM() {
			bundle.SDM.UnregRecv(r.ID())
		}
		r.DemandBase.Stop()
		r.cleanup()
		r.Finish()
		return hk.UnregInterval
	}
}

// handle req; gather other target's data; emit resulting TAR, et. al formats
func (r *XactMoss) Assemble(req *apc.MossReq, w http.ResponseWriter) error {
	var (
		resp = &apc.MossResp{
			UUID: r.ID(),
		}
		opts   = archive.Opts{TarFormat: tar.FormatUnknown} // default tar format (here and elsewhere)
		basewi = basewi{
			r:    r,
			smap: core.T.Sowner().Get(),
			resp: resp,
		}
	)
	r.IncPending()
	defer basewi.cleanup()

	// streaming
	if req.StreamingGet {
		wi := streamwi{basewi: basewi}
		wi.aw = archive.NewWriter(req.OutputFormat, w, nil /*checksum*/, &opts)
		r.wi = &wi
		return wi.do(req, w)
	}

	// buffered
	var (
		sgl = r.mm.NewSGL(0)
		wi  = buffwi{basewi: basewi, sgl: sgl}
	)
	r.wi = &wi
	wi.resp.Out = make([]apc.MossOut, 0, len(req.In))
	wi.aw = archive.NewWriter(req.OutputFormat, sgl, nil /*checksum*/, &opts)
	err := wi.do(req, w)
	sgl.Free()
	return err
}

// send all requested local data => tsi
func (r *XactMoss) Send(req *apc.MossReq, smap *meta.Smap, tsi *meta.Snode) error {
	r.IncPending()
	defer r.DecPending()

	for i := range req.In {
		if err := r.AbortErr(); err != nil {
			return err
		}
		in := &req.In[i]
		if in.Length != 0 {
			return cmn.NewErrNotImpl("range read", "moss")
		}
		// source bucket (per-object) override
		bck, err := r._bucket(in)
		if err != nil {
			r.Abort(err)
			return err
		}
		lom := core.LOM{ObjName: in.ObjName}

		// TODO -- FIXME: (dup; impl; pool)

		if err := lom.InitBck(bck); err != nil {
			return err
		}
		_, local, err := lom.HrwTarget(smap)
		if err != nil {
			r.Abort(err)
			return err
		}
		if !local {
			continue // skip
		}

		nameInArch := req.NameInRespArch(bck.Name, i)
		lom.Lock(false)
		roc, err := lom.NewDeferROC(false /*loaded*/)
		if err != nil {
			if !req.ContinueOnErr || !cos.IsNotExist(err, 0) {
				r.Abort(err)
				return err
			}
			nameInArch = apc.MossMissingDir + "/" + nameInArch
			roc = nopROC{}
		}

		o := transport.AllocSend()
		hdr := &o.Hdr
		{
			hdr.Bck = *bck
			hdr.ObjName = nameInArch
			hdr.ObjAttrs.CopyFrom(lom.ObjAttrs(), false /*skip cksum*/)
			hdr.Opaque = r.opaque(i)
		}
		bundle.SDM.Send(o, roc, tsi)
	}

	return nil
}

// TODO(xid-demux): remove; see transport/bundle/shared_dm
func (r *XactMoss) opaque(i int) (b []byte) {
	var (
		xid = r.ID()
		l   = len(xid)
	)
	b = make([]byte, l+cos.SizeofI32)
	copy(b, xid)
	binary.BigEndian.PutUint32(b[l:], uint32(i))
	return b
}

// as transport.Receiver
// note that hdr.ObjName is `nameInArch` resulting from  `req.NameInRespArch()` that takes into account `req.OnlyObjName = (true|false)`
func (r *XactMoss) RecvObj(hdr *transport.ObjHdr, reader io.Reader, err error) error {
	if err != nil {
		nlog.Errorln(r.Name(), "recv error:", err)
		return err
	}

	debug.Assert(len(hdr.Opaque) == cos.SizeofI32)
	index := int(binary.BigEndian.Uint32(hdr.Opaque))
	if index < 0 || index >= len(r.recvd) {
		err = fmt.Errorf("out-of-bounds index %d (recv'd len=%d)", index, len(r.recvd))
		debug.AssertNoErr(err)
		return err
	}

	var (
		written int64
		sgl     = r.mm.NewSGL(0)
	)
	written, err = io.Copy(sgl, reader)
	if err != nil {
		sgl.Free()
		err = fmt.Errorf("failed to receive %s: %w", hdr.ObjName, err)
		nlog.Warningln(err)
		return err
	}
	debug.Assert(written == sgl.Len(), written, " vs ", sgl.Len())

	isMissing := strings.HasPrefix(hdr.ObjName, apc.MossMissingDir+"/") // TODO -- FIXME: must be a better way

	r.mu.Lock()
	entry := &r.recvd[index]
	debug.Assertf(entry.sgl == nil && entry.name == "", "duplicated receive[%d]: %q vs %q", index, hdr.ObjName, entry.name)
	r.recvd[index] = mossInRecv{
		name:      hdr.ObjName,
		sgl:       sgl,
		isMissing: isMissing,
	}
	r.mu.Unlock()

	if err := r.wi.flushMaybe(); err != nil {
		r.Abort(err)
		return err
	}

	if cmn.Rom.FastV(4, cos.SmoduleXs) {
		nlog.Infof("%s: received[%d] %q (%dB)", r.Name(), index, hdr.ObjName, sgl.Len())
	}
	return nil
}

func (r *XactMoss) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)
	snap.IdleX = r.IsIdle()
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
}

func (r *XactMoss) usingSharedDM() bool { return r.recvd != nil }

////////////
// basewi //
////////////

func (wi *basewi) next(req *apc.MossReq, i int) error {
	in := &req.In[i]
	if in.Length != 0 {
		return cmn.NewErrNotImpl("range read", "moss")
	}

	// source bucket (per-object) override
	bck, err := wi.r._bucket(in)
	if err != nil {
		return err
	}
	nameInArch := req.NameInRespArch(bck.Name, i)

	// write next
	var (
		out = apc.MossOut{
			ObjName: in.ObjName, ArchPath: in.ArchPath, Bucket: bck.Name, Provider: bck.Provider,
			Opaque: in.Opaque,
		}
		lom = core.AllocLOM(in.ObjName)
	)
	err = wi.write(bck, lom, in, &out, nameInArch, req.ContinueOnErr)
	core.FreeLOM(lom)
	if err != nil {
		return err
	}
	if !req.StreamingGet {
		wi.resp.Out = append(wi.resp.Out, out)
	}
	wi.cnt++
	wi.size += out.Size

	if cmn.Rom.FastV(5, cos.SmoduleXs) {
		nlog.Infoln(wi.r.Name(), "archived cnt:", wi.cnt, "[", nameInArch, cos.ToSizeIEC(out.Size, 2), "]")
	}
	return nil
}

func (wi *basewi) cleanup() {
	if wi.aw != nil {
		wi.aw.Fini()
	}
	wi.r.DecPending()
}

func (wi *basewi) write(bck *cmn.Bck, lom *core.LOM, in *apc.MossIn, out *apc.MossOut, nameInArch string, contOnErr bool) error {
	if err := lom.InitBck(bck); err != nil {
		return err
	}
	_, local, err := lom.HrwTarget(wi.smap)
	if err != nil {
		return err
	}
	if !local {
		return nil // skip
	}
	lom.Lock(false)
	err = wi._write(lom, in, out, nameInArch, contOnErr)
	lom.Unlock(false)
	return err
}

// (under rlock)
func (wi *basewi) _write(lom *core.LOM, in *apc.MossIn, out *apc.MossOut, nameInArch string, contOnErr bool) error {
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
	case in.ArchPath != "":
		err = wi._txarch(lom, lmfh, out, nameInArch, in.ArchPath, contOnErr)
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

////////////
// buffwi //
////////////

func (wi *buffwi) do(req *apc.MossReq, w http.ResponseWriter) error {
	for i := range req.In {
		if err := wi.r.AbortErr(); err != nil {
			return err
		}
		if err := wi.basewi.next(req, i); err != nil {
			return err
		}
	}

	// flush and close aw
	err := wi.aw.Fini()
	wi.aw = nil
	if err != nil {
		return err
	}

	// write multipart response
	// note: set response headers BEFORE writing
	// format: multipart/mixed; boundary="<boundary>" as per standard lib's mime.ParseMediaType()
	// (see api/client.go for the client-side parsing part)
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
	// the part's filename is available on the client side via part2.Header.Get("Content-Disposition")
	// otherwise ignored
	part2, err := mpw.CreateFormFile(apc.MossDataPart, wi.r.Cname())
	if err != nil {
		return 0, err
	}
	return io.Copy(part2, wi.sgl)
}

// drains recvd[] in strict input order
func (*buffwi) flushMaybe() error {
	debug.Assert(false, "not implemented yet") // TODO -- FIXME
	return nil
}

//////////////
// streamwi //
//////////////

func (wi *streamwi) do(req *apc.MossReq, w http.ResponseWriter) error {
	w.Header().Set(cos.HdrContentType, _ctype(req.OutputFormat))
	for i := range req.In {
		if err := wi.r.AbortErr(); err != nil {
			return err
		}
		if err := wi.basewi.next(req, i); err != nil {
			return err
		}
		if err := wi.aw.Flush(); err != nil {
			return err
		}
	}

	// flush and close aw
	err := wi.aw.Fini()
	wi.aw = nil
	if err != nil {
		return err
	}

	wi.r.ObjsAdd(wi.cnt, wi.size)

	if cmn.Rom.FastV(5, cos.SmoduleXs) {
		nlog.Infoln(wi.r.Name(), "done streaming: [ count:", len(wi.resp.Out), "written:", wi.size, "format:", req.OutputFormat, "]")
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

// drains recvd[] in strict input order
func (wi *streamwi) flushMaybe() (err error) {
	r := wi.r
	r.mu.Lock()
	defer r.mu.Unlock()

	for r.nextRecv < len(r.recvd) {
		entry := &r.recvd[r.nextRecv]
		if entry.sgl == nil && !entry.isMissing {
			break // haven't received this one yet — stop
		}

		if entry.isMissing {
			err = wi.aw.Write(entry.name, cos.SimpleOAH{Size: 0}, nopROC{})
		} else {
			oah := cos.SimpleOAH{Size: entry.sgl.Len()}
			err = wi.aw.Write(entry.name, oah, entry.sgl)
		}
		if err != nil {
			r.Abort(err)
			return err
		}

		// GC to release memory
		r.recvd[r.nextRecv] = mossInRecv{}
		r.nextRecv++

		wi.cnt++
		wi.size += entry.sgl.Len()
		entry.sgl.Free()
		entry.sgl = nil
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
