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
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/NVIDIA/aistore/xact/xs"
)

// -----------------------------------------------------------------------
// Control Flow (where DT: designated target, senders: all the rest)
//
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
	xid = res.header.Get(apc.HdrXactionID)
	freeCargs(cargs)
	freeCR(res)
	if err := res.err; err != nil {
		p.writeErr(w, r, err)
		return
	}
	if !cos.IsValidUUID(xid) {
		err := fmt.Errorf("moss: invalid xid %q at phase 1", xid)
		debug.AssertNoErr(err)
		p.writeErr(w, r, err)
		return
	}

	path := tmosspath(bucket, xid, wid, nat)
	hreq.Path = path
	if cmn.Rom.V(5, cos.ModAIS) {
		nlog.Infoln(p.String(), apc.Moss, "DT", tsi.String(), "xid", xid, "wid", wid, "[", hreq.Path, hreq.Method, "]")
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
		ctx, err := t.mossparse(w, r)
		if err != nil {
			return
		}

		var (
			smap = t.owner.smap.get()
			nat  = smap.CountActiveTs()
		)
		if ecode, err := t.ensureSameSmap(r.Header, smap); err != nil {
			t.writeErr(w, r, err, ecode)
			return
		}
		if nat != ctx.nat {
			t.writeErrf(w, r, "moss: expecting %d targets, have %d", nat, ctx.nat)
			return
		}

		tsi := smap.GetTarget(ctx.tid)
		if tsi == nil {
			t.writeErr(w, r, &errNodeNotFound{t.si, smap, "moss", ctx.tid}) // TODO: unify errs
			return
		}

		// renew x-moss or find/reuse existing one
		var (
			xid        = ctx.xid
			designated = ctx.tid == t.SID()
		)
		if designated {
			// phase 1.
			if xid != placeholderXID {
				err := fmt.Errorf("moss: expected '%s', got %q", placeholderXID, xid)
				debug.AssertNoErr(err)
				t.writeErr(w, r, err)
				return
			}
			xid = cos.GenUUID()
		} else {
			// phase 2.
			debug.Assert(nat > 1, "not expecting POST -> non-DT when single-node ", nat) // (ctx.nat checked above)
			if !cos.IsValidUUID(xid) {
				// (unlikely)
				err := fmt.Errorf("moss: invalid xid %q at phase 2 (non-DT)", xid)
				debug.AssertNoErr(err)
				t.writeErr(w, r, err)
				return
			}
		}
		rns := xreg.RenewGetBatch(ctx.bck, xid, designated)
		if rns.Err != nil {
			t.writeErr(w, r, rns.Err)
			return
		}
		xctn := rns.Entry.Get()
		if xid != xctn.ID() {
			// (unlikely)
			err := fmt.Errorf("moss: expecting xid %q, got %q [DT=%t, %s, %s]", xid, xctn.ID(), designated, ctx.tid, t.SID())
			debug.AssertNoErr(err)
			xctn.Abort(err)
			t.writeErr(w, r, err)
			return
		}
		if cmn.Rom.V(5, cos.ModAIS) {
			nlog.Infoln(t.String(), "x-moss renewed:", xctn.Name(), "was running:", rns.IsRunning())
		}

		xmoss, ok := xctn.(*xs.XactMoss)
		debug.Assert(ok, xctn.Name())

		if err := bundle.SDM.Open(); err != nil {
			t.writeErr(w, r, err)
			return
		}
		if designated {
			if err := xmoss.PrepRx(ctx.req, &smap.Smap, ctx.wid, nat > 1 /*receiving*/); err != nil {
				xmoss.Abort(err)
				t.writeErr(w, r, err)
				return
			}
			w.Header().Set(apc.HdrXactionID, xid)
		} else {
			if err := xmoss.Send(ctx.req, &smap.Smap, tsi, ctx.wid); err != nil {
				xmoss.Abort(err)
				xmoss.BcastAbort(err)
				t.writeErr(w, r, err)
				return
			}
		}

	// phase 3: redirect
	case http.MethodGet:
		ctx, err := t.mossparse(w, r)
		if err != nil {
			return
		}

		debug.Assert(cos.IsValidUUID(ctx.xid), ctx.xid)
		xctn := xreg.GetActiveXact(ctx.xid)
		if xctn == nil {
			err := fmt.Errorf("%s: x-moss %q must be active", t, ctx.xid)
			debug.AssertNoErr(err)
			t.writeErr(w, r, err) // TODO -- FIXME: abort-all
			return
		}
		xmoss, ok := xctn.(*xs.XactMoss)
		debug.Assert(ok, xctn.Name())

		if err := xmoss.Assemble(ctx.req, w, ctx.wid); err != nil {
			bundle.SDM.UnregRecv(xmoss.ID()) // drop all Rx earlier

			xmoss.BcastAbort(err)
			xmoss.Abort(err)
			if err == cmn.ErrGetTxBenign {
				if cmn.Rom.V(5, cos.ModAIS) {
					nlog.Warningln(err)
				}
			} else {
				t.writeErr(w, r, err)
			}
		}
	default:
		cmn.WriteErr405(w, r, http.MethodGet)
	}
}

// parse tmosspath()
func (t *target) mossparse(w http.ResponseWriter, r *http.Request) (ctx mossCtx, err error) {
	var (
		items []string
	)
	if items, err = t.parseURL(w, r, apc.URLPathML.L, 4, true); err != nil {
		return ctx, err
	}
	if cmn.Rom.V(5, cos.ModAIS) {
		nlog.Infoln(t.String(), "mossparse", r.Method, "items", items)
	}
	if len(items) > tmosspathNumItems {
		t.writeErrURL(w, r)
		return ctx, err
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
		return ctx, err
	}
	debug.Assert(ctx.nat > 0 && ctx.nat < 10_000, ctx.nat)

	q := r.URL.Query() // TODO: dpq
	ctx.tid = q.Get(apc.QparamTID)
	if bucket != "" {
		ctx.bck, err = newBckFromQ(bucket, q, nil)
		if err != nil {
			t.writeErr(w, r, err)
			return ctx, err
		}
	}

	ctx.req = &apc.MossReq{}
	if err := cmn.ReadJSON(w, r, ctx.req); err != nil {
		return ctx, err
	}
	if len(ctx.req.In) == 0 {
		t.writeErr(w, r, errors.New(apc.Moss+": empty input")) // TODO: unify errs
		return ctx, err
	}
	if ctx.req.OutputFormat == "" {
		ctx.req.OutputFormat = archive.ExtTar // default
	} else {
		f, err := archive.Mime(ctx.req.OutputFormat, "" /*filename*/) // normalize
		if err != nil {
			t.writeErr(w, r, err)
			return ctx, err
		}
		ctx.req.OutputFormat = f
	}
	if cmn.Rom.V(5, cos.ModAIS) {
		nlog.Infoln(t.String(), "mossparse", "ctx [", ctx.bck.String(), ctx.tid, ctx.xid, ctx.wid, "]")
	}
	return ctx, nil
}
