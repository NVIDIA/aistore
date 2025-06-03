// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
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
	q := r.URL.Query()
	bucket := apiItems[1]
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

	// 3. generate xid and designate random target
	xid := cos.GenUUID()
	smap := p.owner.smap.get()
	tsi, errT := smap.HrwTargetTask(xid)
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
	if nat := smap.CountActiveTs(); nat > 1 {
		args := allocBcArgs()
		{
			q.Set(apc.QparamTID, tsi.ID())

			args.req = cmn.HreqArgs{Method: r.Method, Path: apc.URLPathML.S, Query: q, Body: body}
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
		p.bcastSelected(args)
		freeBcArgs(args)
	}

	// 5. update r.URL.Path (to include apc.MossExec) and redirect
	r.URL.Path = cos.JoinWords(apc.Version, apc.ML, apc.MossExec, bucket)
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
		apiItems, err := t.parseURL(w, r, apc.URLPathML.L, 2, true)
		if err != nil {
			return
		}
		switch apiItems[0] {
		case apc.Moss:
			// bcast path
			t.httpmlget(w, r, apiItems)
		case apc.MossExec:
			// redirect path
			t.mossexec(w, r, apiItems)
		default:
			t.writeErrURL(w, r)
		}
	default:
		cmn.WriteErr405(w, r, http.MethodGet)
	}
}

// GET /v1/ml/mossexec/bucket-name (redirect from proxy)
func (t *target) mossexec(w http.ResponseWriter, r *http.Request, apiItems []string) {
	// 1. validate
	if len(apiItems) > 2 {
		t.writeErrURL(w, r)
		return
	}

	// 2. parse bucket
	q := r.URL.Query()
	bucket := apiItems[1]
	bck, err := newBckFromQ(bucket, q, nil)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}

	// 3. read and unmarshal MossReq
	req := &api.MossReq{}
	if err := cmn.ReadJSON(w, r, req); err != nil {
		return
	}

	// 4. generate target-local xaction ID using BEID mechanism
	div := uint64(xact.IdleDefault)
	beid, _, _ := xreg.GenBEID(div, bck.MakeUname(apc.Moss))
	if beid == "" {
		beid = cos.GenUUID()
	}

	// 5. start moss xaction
	rns := xreg.RenewBucketXact(apc.ActGetBatch, bck, xreg.Args{UUID: beid})
	if rns.Err != nil {
		t.writeErr(w, r, rns.Err)
		return
	}
	xctn := rns.Entry.Get()

	// run it
	if !rns.IsRunning() {
		xact.GoRunW(xctn)
	}

	xmoss := xctn.(*xs.XactMoss)
	if err := xmoss.Do(req, w); err != nil {
		t.writeErr(w, r, err)
	}
}

// GET /v1/ml/moss/bucket-name (broadcast path - for future multi-target support)
func (t *target) httpmlget(w http.ResponseWriter, r *http.Request, apiItems []string) {
	// 1. perform steps 1 through 5 above
	// 2. traverse; send api.MossOut parts to designated target
	// 3. return 200 or 204

	t.writeErr(w, r, cmn.NewErrUnsupp("multi-target moss, bucket", apiItems[1]))
}
