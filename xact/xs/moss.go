// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api"
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

// TODO -- FIXME:
// - recv() and generally, multi-target
// - read from shards
// - enable ais/test/moss tests - rm skipf
// - ctlmsg
// - soft errors other than not-found
// - error handling in general and across the board; mossErr{wrapped-err}

// TODO:
// - write checksum
// - range read

type (
	mossFactory struct {
		xreg.RenewBase
		xctn *XactMoss
	}
	XactMoss struct {
		xact.DemandBase
	}
)

type (
	basewi struct {
		aw   archive.Writer
		r    *XactMoss
		smap *meta.Smap
		resp *api.MossResp
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
	_ core.Xact      = (*XactMoss)(nil)
	_ xreg.Renewable = (*mossFactory)(nil)
)

func (*mossFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	return &mossFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}}
}

func (p *mossFactory) Start() error {
	debug.Assert(cos.IsValidUUID(p.Args.UUID), p.Args.UUID)
	p.xctn = newMoss(p)
	return nil
}

func (*mossFactory) Kind() string     { return apc.ActGetBatch }
func (p *mossFactory) Get() core.Xact { return p.xctn }

func (*mossFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprUse, nil
}

func newMoss(p *mossFactory) *XactMoss {
	r := &XactMoss{}
	r.DemandBase.Init(p.UUID(), p.Kind(), "" /*ctlmsg*/, p.Bck, mossIdleTime, r.fini)
	return r
}

func (r *XactMoss) Run(wg *sync.WaitGroup) {
	nlog.Infoln(r.Name(), "starting")

	if err := bundle.SDM.Open(); err != nil {
		r.AddErr(err, 5, cos.SmoduleXs)
		return
	}

	wg.Done()

	bundle.SDM.RegRecv(r.ID(), r.recv)
}

func (r *XactMoss) Abort(err error) bool {
	if !r.DemandBase.Abort(err) {
		return false
	}

	bundle.SDM.UnregRecv(r.ID())
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
		bundle.SDM.UnregRecv(r.ID())
		r.DemandBase.Stop()
		r.Finish()
		return hk.UnregInterval
	}
}

// process api.GetBatch request and write multipart response
func (r *XactMoss) Do(req *api.MossReq, w http.ResponseWriter) error {
	var (
		resp = &api.MossResp{
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
		return wi.do(req, w)
	}

	// buffered
	var (
		mm  = core.T.PageMM()
		sgl = mm.NewSGL(0)
		wi  = buffwi{basewi: basewi, sgl: sgl}
	)
	wi.resp.Out = make([]api.MossOut, 0, len(req.In))
	wi.aw = archive.NewWriter(req.OutputFormat, sgl, nil /*checksum*/, &opts)
	err := wi.do(req, w)
	sgl.Free()
	return err
}

func (r *XactMoss) recv(hdr *transport.ObjHdr, reader io.Reader, err error) error {
	if err != nil {
		nlog.Errorln(r.Name(), "recv error:", err)
		return err
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		nlog.Errorln(r.Name(), "failed to read data:", err)
		return err
	}

	if cmn.Rom.FastV(5, cos.SmoduleXs) {
		nlog.Infoln(r.Name(), "received:", hdr.Bck.Cname(hdr.ObjName), "size:", len(data))
	}
	return nil
}

func (r *XactMoss) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)
	snap.IdleX = r.IsIdle()
	return
}

////////////
// basewi //
////////////

func (wi *basewi) next(req *api.MossReq, i int) error {
	in := &req.In[i]
	if in.Length != 0 {
		return cmn.NewErrNotImpl("range read", "moss")
	}

	// source bucket (per-object) override
	bck, err := wi.bucket(in)
	if err != nil {
		return err
	}
	nameInArch := req.NameInRespArch(bck, i)

	// write next
	var (
		out = api.MossOut{
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

// per-object override, if specified
func (wi *basewi) bucket(in *api.MossIn) (*cmn.Bck, error) {
	// default
	bck := wi.r.Bck().Bucket()

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

func (wi *basewi) write(bck *cmn.Bck, lom *core.LOM, in *api.MossIn, out *api.MossOut, nameInArch string, contOnErr bool) error {
	if err := lom.InitBck(bck); err != nil {
		return err
	}
	_, local, err := lom.HrwTarget(wi.smap)
	if err != nil {
		return err
	}
	if !local {
		return cmn.NewErrNotImpl("multi-target", "moss")
	}

	if err := lom.Load(false /*cache it*/, false /*locked*/); err != nil {
		if os.IsNotExist(err) && contOnErr {
			err = wi.addMissing(err, nameInArch, out)
		}
		return err
	}

	var lmfh cos.LomReader
	lmfh, err = lom.Open()
	if err != nil {
		if cos.IsNotExist(err, 0) && contOnErr {
			err = wi.addMissing(err, nameInArch, out)
		}
		return err
	}

	switch {
	case in.ArchPath != "":
		if in.Length != 0 {
			cos.Close(lmfh)
			err := fmt.Errorf("%s: cannot read range from an archived file [%d, %s, %s]", wi.r.Name(), in.Length, lom.Cname(), in.ArchPath)
			return cmn.NewErrUnsuppErr(err)
		}
		err = wi._txarch(lom, lmfh, out, nameInArch, in.ArchPath, contOnErr)
	default:
		err = wi._txreg(lom, lmfh, out, nameInArch)
	}

	cos.Close(lmfh)
	return err
}

func (wi *basewi) _txreg(lom *core.LOM, lmfh cos.LomReader, out *api.MossOut, nameInArch string) error {
	if err := wi.aw.Write(nameInArch, lom, lmfh); err != nil {
		return err
	}
	out.Size = lom.Lsize()
	return nil
}

// (compare w/ goi._txarch)
func (wi *basewi) _txarch(lom *core.LOM, lmfh cos.LomReader, out *api.MossOut, nameInArch, archpath string, contOnErr bool) error {
	nameInArch += "/" + archpath

	csl, err := lom.NewArchpathReader(lmfh, archpath, "" /*mime*/)
	if err != nil {
		if cos.IsNotExist(err, 0) && contOnErr {
			return wi.addMissing(err, archpath, out)
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

func (wi *basewi) addMissing(err error, nameInArch string, out *api.MossOut) error {
	var (
		missingName = api.MissingFilesDirectory + "/" + nameInArch
		oah         = cos.SimpleOAH{Size: 0}
		emptyReader = bytes.NewReader(nil)
	)
	if err := wi.aw.Write(missingName, oah, emptyReader); err != nil {
		return err
	}
	out.ErrMsg = err.Error()
	return nil
}

////////////
// buffwi //
////////////

func (wi *buffwi) do(req *api.MossReq, w http.ResponseWriter) error {
	for i := range req.In {
		if err := wi.r.AbortErr(); err != nil {
			return err
		}
		if err := wi.basewi.next(req, i); err != nil {
			return err
		}
	}

	// flush and close aw
	erc := wi.aw.Fini()
	wi.aw = nil
	if erc != nil {
		return erc
	}

	// write multipart response
	// note: set response headers BEFORE writing
	// format: multipart/mixed; boundary="<boundary>" as per standard lib's mime.ParseMediaType()
	// (see api/client.go for the client-side parsing part)
	mpw := multipart.NewWriter(w)
	w.Header().Set(cos.HdrContentType, "multipart/mixed; boundary="+mpw.Boundary())

	written, erw := wi.multipart(mpw, req.OutputFormat, wi.resp)
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

func (wi *buffwi) multipart(mpw *multipart.Writer, outputFormat string, resp *api.MossResp) (int64, error) {
	// part 1: JSON
	part1, err := mpw.CreateFormField(api.MossMetadataField)
	if err != nil {
		return 0, err
	}
	if err := jsoniter.NewEncoder(part1).Encode(resp); err != nil {
		return 0, err
	}

	// part 2: archive
	part2, err := mpw.CreateFormFile(api.MossArchiveField, api.MossArchivePrefix+outputFormat)
	if err != nil {
		return 0, err
	}
	return io.Copy(part2, wi.sgl)
}

//////////////
// streamwi //
//////////////

func (wi *streamwi) do(req *api.MossReq, w http.ResponseWriter) error {
	w.Header().Set(cos.HdrContentType, _ctype(req.OutputFormat))
	for i := range req.In {
		if err := wi.r.AbortErr(); err != nil {
			return err
		}
		if err := wi.basewi.next(req, i); err != nil {
			return err
		}
	}

	// flush and close aw
	erc := wi.aw.Fini()
	wi.aw = nil
	if erc != nil {
		return erc
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
