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
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"

	jsoniter "github.com/json-iterator/go"
)

// TODO -- FIXME:
// - recv()
// - ctlmsg
// - range read
// - soft errors other than not-found
// - out.ErrMsg = "not found"

type (
	mossFactory struct {
		xreg.RenewBase
		xctn *XactMoss
	}
	XactMoss struct {
		xact.DemandBase
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
	xact.GoRunW(p.xctn)
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
	nlog.InfoDepth(1, r.Name(), "starting")

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

// Do processes the moss request and writes the multipart response
// TODO -- FIXME: split and refactor defer func()
func (r *XactMoss) Do(req *api.MossReq, w http.ResponseWriter) error {
	// 1. Validate request
	if len(req.In) == 0 {
		return fmt.Errorf("%s: empty object list", r.Name())
	}

	// 2. Determine output format (default TAR)
	mime := req.OutputFormat
	if mime == "" {
		mime = archive.ExtTar // default to TAR
	}

	// 3. Prepare response metadata
	resp := &api.MossResp{
		Out:  make([]api.MossOut, 0, len(req.In)),
		UUID: r.ID(),
	}

	// 4. Create archive writer
	var (
		smap = core.T.Sowner().Get()
		mm   = core.T.PageMM()
		sgl  = mm.NewSGL(0)
		opts = archive.Opts{TarFormat: tar.FormatGNU}
		aw   = archive.NewWriter(mime, sgl, nil /*checksum*/, &opts)
	)
	r.IncPending()

	// cleanup
	defer func() {
		if aw != nil {
			aw.Fini()
		}
		sgl.Free()
		r.DecPending()
	}()

	// 5. Process each object in order
	for idx, objIn := range req.In {
		if err := r.AbortErr(); err != nil {
			return err
		}

		debug.Assert(objIn.Length == 0, "range read not implemented yet")

		out := api.MossOut{
			ObjName:  objIn.ObjName,
			Bucket:   objIn.Bucket,
			Provider: objIn.Provider,
			Opaque:   string(objIn.Opaque),
		}

		// Determine bucket (use per-object override if specified)
		bck := r.Bck()
		if objIn.Bucket != "" {
			// Parse bucket override
			bck = &meta.Bck{Name: objIn.Bucket}
			if objIn.Provider != "" {
				bck.Provider = objIn.Provider
			}
		}
		out.Bucket = bck.Name
		out.Provider = bck.Provider

		// Get object
		lom := core.AllocLOM(objIn.ObjName)
		defer core.FreeLOM(lom)

		if err := lom.InitBck(bck.Bucket()); err != nil {
			if req.ContinueOnErr {
				out.ErrMsg = err.Error()
				resp.Out = append(resp.Out, out)
				continue
			}
			return err
		}
		_, local, err := lom.HrwTarget(smap)
		if err != nil {
			return err
		}
		if !local {
			continue
		}

		if err := lom.Load(false /*cache it*/, false /*locked*/); err != nil {
			if os.IsNotExist(err) {
				if req.ContinueOnErr {
					// Add missing file entry with __404__ prefix
					missingName := api.MissingFilesDirectory + "/" + bck.Name + "/" + objIn.ObjName
					oah := cos.SimpleOAH{Size: 0}
					if err := aw.Write(missingName, oah, nil); err != nil {
						return err
					}
					out.ErrMsg = "not found"
					out.Size = 0
					resp.Out = append(resp.Out, out)
					continue
				}
			}
			return err
		}

		// Open object for reading
		fh, err := lom.Open()
		if err != nil {
			if req.ContinueOnErr {
				out.ErrMsg = err.Error()
				resp.Out = append(resp.Out, out)
				continue
			}
			return err
		}
		defer fh.Close()

		// Write to archive
		nameInArch := bck.Name + "/" + objIn.ObjName
		if err := aw.Write(nameInArch, lom, fh); err != nil {
			return err
		}

		// Update metadata
		out.Size = lom.Lsize()
		resp.Out = append(resp.Out, out)

		// Update progress
		r.ObjsAdd(1, out.Size)

		if cmn.Rom.FastV(5, cos.SmoduleXs) {
			nlog.Infoln(r.Name(), "archived cnt:", idx+1, "[", nameInArch, cos.ToSizeIEC(out.Size, 2), "]")
		}
	}

	// 6. Finalize archive
	aw.Fini()
	aw = nil

	// 7. Write multipart response
	boundary := cos.GenUUID()
	mpw := multipart.NewWriter(w)
	mpw.SetBoundary(boundary)

	defer func() {
		if mpw != nil {
			mpw.Close()
		}
	}()

	// 8. Set response headers BEFORE writing
	// This MUST be set before any writes to properly format the header
	// Format: multipart/mixed; boundary="<boundary>" as per standard lib's mime.ParseMediaType()
	w.Header().Set(cos.HdrContentType, fmt.Sprintf("%s; %s=\"%s\"",
		api.MossMultipartPrefix+"mixed", api.MossBoundaryParam, boundary))

	// Part 1: JSON metadata
	metaPart, err := mpw.CreateFormField(api.MossMetadataField)
	if err != nil {
		return err
	}

	if err := jsoniter.NewEncoder(metaPart).Encode(resp); err != nil {
		return err
	}

	// Part 2: Archive
	archivePart, err := mpw.CreateFormFile(api.MossArchiveField, api.MossArchivePrefix+mime)
	if err != nil {
		return err
	}

	// Copy from SGL to the multipart writer
	if _, err := io.Copy(archivePart, sgl); err != nil {
		return err
	}

	// Ensure all parts are written before closing
	err = mpw.Close()
	mpw = nil
	if err != nil {
		return err
	}

	if cmn.Rom.FastV(5, cos.SmoduleXs) {
		nlog.Infoln(r.Name(), "done: [", len(resp.Out), "objects ->", mime, "archive ]")
	}
	return nil
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
