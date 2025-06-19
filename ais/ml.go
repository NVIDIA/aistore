// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/NVIDIA/aistore/xact/xs"
)

// TODO -- FIXME:
// - t.httpmlget
// - use parseReq and dpq

//
// proxy -----------------------------------------------------------------
//

func (p *proxy) mlHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		p.httpmlget(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodGet)
	}
}

// GET /v1/ml/moss/bucket-name
func (p *proxy) httpmlget(w http.ResponseWriter, r *http.Request) {
	// 1. parse/validate
	apiItems, err := p.parseURL(w, r, apc.URLPathML.L, 2, true)
	if err != nil {
		return
	}
	if err := p.checkAccess(w, r, nil, apc.AceGET); err != nil {
		return
	}
	if len(apiItems) > 2 || apiItems[0] != apc.Moss {
		p.writeErrURL(w, r)
		return
	}

	// 2. bucket
	var (
		q      = r.URL.Query()
		bucket = apiItems[1]
	)
	bckArgs := allocBctx()
	{
		bckArgs.p = p
		bckArgs.w = w
		bckArgs.r = r
		bckArgs.query = q
		bckArgs.perms = apc.AceGET
		bckArgs.createAIS = false
	}
	bckArgs.bck, err = newBckFromQ(bucket, q, nil)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	bck, errN := bckArgs.initAndTry()
	freeBctx(bckArgs)
	if errN != nil {
		return
	}

	// 3. select random "mossexec" target
	var (
		smap      = p.owner.smap.get()
		tsi, errT = smap.HrwTargetTask(cos.GenTie())
	)
	if errT != nil {
		p.writeErr(w, r, errT)
		return
	}

	// read api.MossReq but not unmarshal it
	body, errB := cmn.ReadBytes(r)
	if errB != nil {
		p.writeErr(w, r, errB)
		return
	}

	// 4. bcast to all targets except the designated one

	wid := cos.GenYAID(p.SID())
	if nat := smap.CountActiveTs(); nat > 1 {
		args := allocBcArgs()
		{
			q.Set(apc.QparamTID, tsi.ID())

			args.req = cmn.HreqArgs{Method: r.Method, Path: apc.URLPathML.Join(bucket, wid), Query: q, Body: body}
			args.smap = smap
			args.async = true

			// Build selected nodes list excluding designated target
			nodes := args.selected[:0]
			for _, si := range smap.Tmap {
				if si.ID() != tsi.ID() && !si.InMaintOrDecomm() {
					nodes = append(nodes, si)
				}
			}
			args.selected = nodes
			args.nodeCount = len(nodes)
		}
		_ = p.bcastSelected(args)
		freeBcArgs(args)

		if cmn.Rom.FastV(5, cos.SmoduleAIS) {
			nlog.Infoln(r.Method, apc.Moss, bck.Cname(""), "bcast", len(args.selected))
		}
	}

	// 5. update r.URL.Path (to include apc.MossExec) and redirect
	r.URL.Path = cos.JoinWords(apc.Version, apc.ML, apc.MossExec, bucket, wid)
	redirectURL := p.redirectURL(r, tsi, time.Now(), cmn.NetIntraControl)

	if cmn.Rom.FastV(5, cos.SmoduleAIS) {
		nlog.Infoln(r.Method, apc.Moss, bck.Cname(""), "=> redirect to", tsi.String(), "at", redirectURL)
	}
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}

//
// target ---------------------------------------------------------------------------------
//

func (t *target) mlHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		apiItems, e := t.parseURL(w, r, apc.URLPathML.L, 2, true)
		if e != nil {
			return
		}
		if len(apiItems) > 3 {
			t.writeErrURL(w, r)
			return
		}

		// bucket
		var (
			q        = r.URL.Query() // TODO: dpq
			bucket   = apiItems[1]
			wid      = apiItems[2]
			bck, err = newBckFromQ(bucket, q, nil)
		)
		if err != nil {
			t.writeErr(w, r, err)
			return
		}

		// req: read, unmarshal, and validate // TODO: mem-pool
		var (
			req = &apc.MossReq{}
		)
		if err := cmn.ReadJSON(w, r, req); err != nil {
			return
		}
		if len(req.In) == 0 {
			t.writeErr(w, r, errors.New(apc.MossExec+" : empty input"))
			return
		}
		if req.OutputFormat == "" {
			req.OutputFormat = archive.ExtTar // default
		} else {
			f, err := archive.Mime(req.OutputFormat, "" /*filename*/) // normalize
			if err != nil {
				t.writeErr(w, r, err)
				return
			}
			req.OutputFormat = f
		}

		// xid: generate target-local cluster-unique BEID
		var (
			div        = uint64(xact.IdleDefault)
			beid, _, _ = xreg.GenBEID(div, bck.MakeUname(apc.Moss))
		)
		if beid == "" {
			beid = cos.GenUUID()
		}

		// start x-moss
		rns := xreg.RenewBucketXact(apc.ActGetBatch, bck, xreg.Args{UUID: beid, Custom: req})
		if rns.Err != nil {
			t.writeErr(w, r, rns.Err)
			return
		}
		xctn := rns.Entry.Get()
		if !rns.IsRunning() {
			// run it
			xact.GoRunW(xctn)
		}
		xmoss, ok := xctn.(*xs.XactMoss)
		debug.Assert(ok, xctn.Name())

		switch apiItems[0] {
		case apc.Moss:
			// bcast path
			tid := q.Get(apc.QparamTID)
			t.mossend(w, r, xmoss, req, tid, wid)
		case apc.MossExec:
			// redirect path
			t.mossasm(w, r, xmoss, req, wid)
		default:
			t.writeErrURL(w, r)
		}
	default:
		cmn.WriteErr405(w, r, http.MethodGet)
	}
}

// GET /v1/ml/mossexec/bucket-name (redirect path)
func (t *target) mossasm(w http.ResponseWriter, r *http.Request, xmoss *xs.XactMoss, req *apc.MossReq, wid string) {
	if err := xmoss.Assemble(req, w, wid); err != nil {
		if err == cmn.ErrGetTxBenign {
			if cmn.Rom.FastV(5, cos.SmoduleAIS) {
				nlog.Warningln(err)
			}
		} else {
			t.writeErr(w, r, err)
		}
	}
}

// GET /v1/ml/moss/bucket-name (broadcast path)
func (t *target) mossend(w http.ResponseWriter, r *http.Request, xmoss *xs.XactMoss, req *apc.MossReq, tid, wid string) {
	smap := t.owner.smap.get()
	tsi := smap.GetTarget(tid)
	if tsi == nil {
		t.writeErr(w, r, &errNodeNotFound{t.si, smap, "failed to mossend", tid})
		return
	}
	if err := xmoss.Send(req, &smap.Smap, tsi, wid); err != nil {
		t.writeErr(w, r, err)
	}
}
