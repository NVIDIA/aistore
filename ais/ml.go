// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/NVIDIA/aistore/xact/xs"
)

// -----------------------------------------------------------------------
// For background and usage, please refer to:
// 1. GetBatch: Multi-Object Retrieval API, at https://github.com/NVIDIA/aistore/blob/main/docs/get_batch.md
// 2. Monitoring GetBatch Performance,      at https://github.com/NVIDIA/aistore/blob/main/docs/monitoring-get-batch.md
// -----------------------------------------------------------------------

// ---------------------- Control Flow -----------------------------------
// phase 1 (POST): Client → Proxy → DT
//   DT: PrepRx(receiving=true) → SDM.RegRecv() → Return XID
// phase 2 (POST): Proxy → Senders
//   Senders: PrepRx(receiving=false) → SDM.Open() → Send() to DT
// phase 3 (GET): Client → DT (redirected)
//   DT: Assemble() → Use pre-existing basewi state
// -----------------------------------------------------------------------

func (p *proxy) mlHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		p.httpmlget(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodGet)
	}
}

const (
	tmosspathNumItems = 5
)

const (
	placeholderXID = "noxid"
)

func tmosspath(bucket, xid, wid string, nat int) string {
	s := strconv.Itoa(nat)
	// when parsed will contain tmosspathNumItems = 5 if bucket name provided
	// otherwise 4 items
	if bucket == "" {
		return apc.URLPathML.Join(apc.Moss, xid, wid, s)
	}
	return apc.URLPathML.Join(apc.Moss, bucket, xid, wid, s)
}

// GET /v1/ml/moss/bucket-name
// +gen:endpoint GET /v1/ml/moss/{bucket}[apc.QparamTID=string] model=[apc.MossReq]
// +gen:payload apc.MossReq={"in":[{"objname":"<object-name>","bucket":"<bucket-name>","provider":"<provider>"},{"objname":"<object-name>","start":<start-offset>,"length":<length>}],"mime":"<mime-type>","coer":<continue-on-error>,"onob":<only-object-name>,"strm":<stream-output>}
// Machine Learning endpoint for batch processing of objects using MOSS (Multi-Object Streaming Service)
func (p *proxy) httpmlget(w http.ResponseWriter, r *http.Request) {
	// parse/validate
	items, err := p.parseURL(w, r, apc.URLPathML.L, 1, true)
	if err != nil {
		return
	}
	if err := p.checkAccess(w, r, nil, apc.AceGET); err != nil {
		return
	}
	if len(items) > 2 || items[0] != apc.Moss {
		p.writeErrURL(w, r)
		return
	}

	var (
		q      url.Values
		bucket string
	)
	if len(items) == 2 {
		bucket = items[1]
		q = r.URL.Query()
		bckArgs := allocBctx()
		{
			bckArgs.p = p
			bckArgs.w = w
			bckArgs.r = r
			bckArgs.query = q
			bckArgs.perms = apc.AceGET
			bckArgs.createAIS = false
		}
		if bckArgs.bck, err = newBckFromQ(bucket, q, nil); err != nil {
			p.writeErr(w, r, err)
			return
		}
		_, err := bckArgs.initAndTry()
		freeBctx(bckArgs)
		if err != nil {
			return
		}
	}

	// DT
	var (
		smap      = p.owner.smap.get()
		nat       = smap.CountActiveTs()
		tsi, errT = smap.HrwTargetTask(cos.GenTie())
	)
	if errT != nil {
		p.writeErr(w, r, errT)
		return
	}

	body, errB := cmn.ReadBytes(r) // read api.MossReq but not unmarshal it
	if errB != nil {
		p.writeErr(w, r, errB)
		return
	}

	if q == nil {
		q = url.Values{apc.QparamTID: []string{tsi.ID()}}
	} else {
		q.Set(apc.QparamTID, tsi.ID())
	}

	// phase 1: call DT
	var (
		wid  = cos.GenYAID(p.SID())
		xid  = placeholderXID
		hreq = cmn.HreqArgs{
			Method: http.MethodPost,
			Path:   tmosspath(bucket, xid, wid, nat),
			Query:  q,
			Body:   body,
		}
	)
	cargs := allocCargs()
	{
		cargs.si = tsi
		cargs.req = hreq
	}
	res := p.call(cargs, smap)
	err = res.err
	xid = res.header.Get(apc.HdrXactionID)
	freeCargs(cargs)
	freeCR(res)

	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	if !cos.IsValidUUID(xid) {
		err := fmt.Errorf("x-moss: invalid xid %q at phase 1", xid)
		debug.AssertNoErr(err)
		p.writeErr(w, r, err)
		return
	}

	path := tmosspath(bucket, xid, wid, nat)
	hreq.Path = path
	if cmn.Rom.V(5, cos.ModAIS) {
		nlog.Infoln(p.String(), apc.Moss, "DT", tsi.String(), "xid", xid, "wid", wid, "[", hreq.Path, hreq.Method, "]")
	}

	// toggle SDM (fast-kalive => primary)
	if !smap.isPrimary(p.si) {
		p.dm.nonpSetActive(mono.NanoTime())
	}

	// phase 2: async broadcast -> all except DT
	if nat > 1 {
		args := allocBcArgs()
		{
			args.req = hreq
			args.smap = smap
			args.network = cmn.NetIntraControl
			args.async = true
		}
		nodes := args.selected[:0]
		for _, si := range smap.Tmap {
			if si.ID() != tsi.ID() && !si.InMaintOrDecomm() {
				nodes = append(nodes, si)
			}
		}
		args.selected = nodes
		args.nodeCount = len(nodes)

		_ = p.bcastSelected(args) // async
		freeBcArgs(args)
	}

	// phase 3: redirect user's GET => DT
	r.URL.Path = path
	redirectURL := p.redirectURL(r, tsi, time.Now(), cmn.NetIntraControl)

	if cmn.Rom.V(5, cos.ModAIS) {
		nlog.Infoln(r.Method, items, "=> redirect to", tsi.String(), "at", redirectURL)
	}
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}

//
// target ---------------------------------------------------------------------------------
//

type mossCtx struct {
	t   *target
	req *apc.MossReq
	bck *meta.Bck
	tid string
	xid string
	wid string
	nat int
}

func (t *target) mlHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	// phase 1: DT to initialize Rx (see `designated`)
	// phase 2: senders to open SDM and start sending
	case http.MethodPost:
		var ctx mossCtx
		if err := t.mossparse(w, r, &ctx); err != nil {
			return
		}
		ctx.t = t

		var (
			smap = t.owner.smap.get()
			nat  = smap.CountActiveTs()
		)
		if ecode, err := t.ensureSameSmap(r.Header, smap); err != nil {
			t.writeErr(w, r, err, ecode)
			return
		}
		if nat != ctx.nat {
			t.writeErrf(w, r, "x-moss: expecting %d targets, have %d", nat, ctx.nat)
			return
		}

		tsi := smap.GetTarget(ctx.tid)
		if tsi == nil {
			t.writeErr(w, r, &errNodeNotFound{t.si, smap, "x-moss", ctx.tid})
			return
		}

		designated := ctx.tid == t.SID()
		if designated {
			ctx.phase1(w, r, smap, nat)
		} else {
			ctx.phase2(w, r, smap, tsi, nat)
		}

	// phase 3: redirect; start an assembly
	case http.MethodGet:
		var ctx mossCtx
		if err := t.mossparse(w, r, &ctx); err != nil {
			return
		}
		ctx.t = t

		debug.Assert(cos.IsValidUUID(ctx.xid), ctx.xid)
		xctn := xreg.GetActiveXact(ctx.xid)
		if xctn == nil {
			ecode := http.StatusConflict
			if xctn, _ = xreg.GetXact(ctx.xid); xctn == nil {
				ecode = http.StatusNotFound
			}
			t.writeErr(w, r, fmt.Errorf("x-moss: xid %q not active", ctx.xid), ecode)
			return
		}
		xmoss, ok := xctn.(*xs.XactMoss)
		debug.Assert(ok, xctn.Name())

		if err := xmoss.Assemble(ctx.req, w, ctx.wid); err != nil {
			// NOTE: not aborting x-moss on a single wid failure
			if err == cmn.ErrGetTxBenign {
				xmoss.AddErr(fmt.Errorf("assemble wid=%s: %v", ctx.wid, err), 4)
			} else {
				xmoss.AddErr(fmt.Errorf("assemble wid=%s: %v", ctx.wid, err), 4, cos.ModXs)
				t.writeErr(w, r, err, 0, Silent)
			}
		}
	default:
		cmn.WriteErr405(w, r, http.MethodGet)
	}
}

// Phase 1: DT renews x-moss and initializes Rx
func (ctx *mossCtx) phase1(w http.ResponseWriter, r *http.Request, smap *smapX, nat int) {
	t := ctx.t

	if ctx.xid != placeholderXID {
		err := fmt.Errorf("x-moss: expected '%s', got %q", placeholderXID, ctx.xid)
		debug.AssertNoErr(err)
		t.writeErr(w, r, err)
		return
	}

	//
	// start new or keep using prev x-moss
	//
	xid := cos.GenUUID()
	rns := xreg.RenewGetBatch(ctx.bck, xid, true /*designated*/)
	if rns.Err != nil {
		t.writeErr(w, r, rns.Err)
		return
	}
	var (
		xctn      = rns.Entry.Get()
		usingPrev bool
	)
	if xid != xctn.ID() && xctn.ID() != "" {
		if !rns.IsRunning() {
			// (unlikely, esp. given xreg's retry-once)
			err := fmt.Errorf("x-moss renewal (temp) conflict: [%s, %t, %v]", xctn.ID(), xctn.IsAborted(), xctn.EndTime())
			t.writeErr(w, r, err, http.StatusConflict)
			return
		}
		usingPrev = true
	}
	xid = xctn.ID()

	//
	// prepare new get-batch for assembly
	//
	if cmn.Rom.V(5, cos.ModAIS) {
		nlog.Infoln(t.String(), "designated = true, renewed:", xctn.Name(), "was running:", rns.IsRunning())
	}
	xmoss, ok := xctn.(*xs.XactMoss)
	debug.Assert(ok, xctn.Name())

	receiving := nat > 1 // multi-target cluster: setup Rx
	if receiving {
		// open SDM
		if err := bundle.SDM.Open(); err != nil {
			xmoss.Abort(err)
			t.writeErr(w, r, err)
			return
		}
	}
	if err := xmoss.PrepRx(ctx.req, &smap.Smap, ctx.wid, receiving, usingPrev); err != nil {
		var ecode int
		if !cmn.IsErrTooManyRequests(err) {
			xmoss.Abort(err)
		} else {
			ecode = http.StatusTooManyRequests
		}
		t.writeErr(w, r, err, ecode)
		return
	}

	// to have a fresh full idle window for the eventual phase3 GET
	xmoss.IncPending()
	xmoss.DecPending()

	// return xid (proxy will send it to phase 2 senders)
	w.Header().Set(apc.HdrXactionID, xid)
}

// Phase 2: Senders open SDM and start sending
func (ctx *mossCtx) phase2(w http.ResponseWriter, r *http.Request, smap *smapX, tsi *meta.Snode, nat int) {
	debug.Assert(nat > 1)
	t := ctx.t

	// expecting valid xid from phase 1
	if !cos.IsValidUUID(ctx.xid) {
		err := fmt.Errorf("x-moss: invalid xid %q at phase 2 (non-DT)", ctx.xid)
		debug.AssertNoErr(err)
		t.writeErr(w, r, err)
		return
	}

	// renew x-moss by ID
	rns := xreg.RenewGetBatch(ctx.bck, ctx.xid, false /*designated*/)
	if rns.Err != nil {
		t.writeErr(w, r, rns.Err)
		return
	}
	var (
		xctn      = rns.Entry.Get()
		usingPrev = rns.IsRunning()
	)
	if cmn.Rom.V(5, cos.ModAIS) {
		nlog.Infoln(t.String(), "designated = false, renewed:", xctn.Name(), "was running:", usingPrev)
	}

	xmoss, ok := xctn.(*xs.XactMoss)
	debug.Assert(ok, xctn.Name())

	// open SDM and start sending
	if err := bundle.SDM.Open(); err != nil {
		t.writeErr(w, r, err)
		return
	}
	if err := xmoss.Send(ctx.req, &smap.Smap, tsi, ctx.wid, usingPrev); err != nil {
		//
		// not aborting x-moss on a single wid failure; silent log
		//
		xmoss.AddErr(fmt.Errorf("send wid=%s: %v", ctx.wid, err), 4, cos.ModXs)
		t.writeErr(w, r, err, 0, Silent)
	}
}

// parse tmosspath()
func (t *target) mossparse(w http.ResponseWriter, r *http.Request, ctx *mossCtx) (err error) {
	var (
		items []string
	)
	if items, err = t.parseURL(w, r, apc.URLPathML.L, 4, true); err != nil {
		return err
	}
	if cmn.Rom.V(5, cos.ModAIS) {
		nlog.Infoln(t.String(), "mossparse", r.Method, "items", items)
	}
	if len(items) > tmosspathNumItems {
		t.writeErrURL(w, r)
		return cmn.ErrSkip // merely a non-nil bail signal
	}
	debug.Assert(items[0] == apc.Moss, items[0])

	// tmosspathNumItems = 5 items with bucket via api.GetBatch(), 4 otherwise
	var (
		bucket string
		shift  = 1
	)
	if len(items) == tmosspathNumItems {
		bucket = items[shift]
		shift++
	}
	ctx.xid = items[shift]
	shift++
	ctx.wid = items[shift]
	shift++
	ctx.nat, err = strconv.Atoi(items[shift])
	if err != nil {
		t.writeErrURL(w, r)
		return err
	}
	debug.Assert(ctx.nat > 0 && ctx.nat < 10_000, ctx.nat)

	q := r.URL.Query() // TODO: dpq
	ctx.tid = q.Get(apc.QparamTID)
	if bucket != "" {
		ctx.bck, err = newBckFromQ(bucket, q, nil)
		if err != nil {
			t.writeErr(w, r, err)
			return err
		}
	}

	ctx.req = &apc.MossReq{}
	if err := cmn.ReadJSON(w, r, ctx.req); err != nil {
		return err
	}
	if len(ctx.req.In) == 0 {
		err := errors.New(apc.Moss + ": empty input") // TODO: unify errs
		t.writeErr(w, r, err)
		return err
	}
	if ctx.req.OutputFormat == "" {
		ctx.req.OutputFormat = archive.ExtTar // default
	} else {
		f, err := archive.Mime(ctx.req.OutputFormat, "" /*filename*/) // normalize
		if err != nil {
			t.writeErr(w, r, err)
			return err
		}
		ctx.req.OutputFormat = f
	}
	if cmn.Rom.V(5, cos.ModAIS) {
		nlog.Infoln(t.String(), "mossparse", "ctx [", ctx.bck.String(), ctx.tid, ctx.xid, ctx.wid, "]")
	}
	return nil
}
