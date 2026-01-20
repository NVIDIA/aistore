//go:build dsort

// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/kvdb"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ext/dsort"

	jsoniter "github.com/json-iterator/go"
)

//
// proxy
//

func (p *proxy) regDsort(handlers []networkHandler) []networkHandler {
	return append(handlers, networkHandler{r: apc.Sort, h: p.dsortHandler, net: accessNetPublic})
}

// Start, monitor, abort, or remove distributed sort (dsort) jobs
func (p *proxy) dsortHandler(w http.ResponseWriter, r *http.Request) {
	if !p.cluStartedWithRetry() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if err := p.checkAccess(w, r, nil, apc.AceAdmin); err != nil {
		return
	}
	apiItems, err := cmn.ParseURL(r.URL.Path, apc.URLPathdSort.L, 0, true)
	if err != nil {
		p.writeErrURL(w, r)
		return
	}

	switch r.Method {
	case http.MethodPost:
		// - validate request, check input_bck and output_bck
		// - start dsort
		body, err := cos.ReadAllN(r.Body, r.ContentLength)
		if err != nil {
			p.writeErrStatusf(w, r, http.StatusInternalServerError, "failed to receive dsort request: %v", err)
			return
		}
		rs := &dsort.RequestSpec{}
		if err := jsoniter.Unmarshal(body, rs); err != nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, p, "dsort request", cos.BHead(body), err)
			p.writeErr(w, r, err)
			return
		}
		parsc, err := rs.ParseCtx()
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		bck := meta.CloneBck(&parsc.InputBck)
		args := bctx{p: p, w: w, r: r, bck: bck, perms: apc.AceObjLIST | apc.AceGET}
		if _, err = args.initAndTry(); err != nil {
			return
		}
		if !parsc.OutputBck.Equal(&parsc.InputBck) {
			bckTo := meta.CloneBck(&parsc.OutputBck)
			bckTo, ecode, err := p.initBckTo(w, r, nil /*query*/, bckTo)
			if err != nil {
				return
			}
			if ecode == http.StatusNotFound {
				if err := p.checkAccess(w, r, nil, apc.AceCreateBucket); err != nil {
					return
				}
				naction := "dsort-create-output-bck"
				warnfmt := "%s: %screate 'output_bck' %s with the 'input_bck' (%s) props"
				if p.forwardCP(w, r, nil /*msg*/, naction, body /*orig body*/) { // to create
					return
				}
				ctx := &bmdModifier{
					pre:   bmodCpProps,
					final: p.bmodSync,
					msg:   &apc.ActMsg{Action: naction},
					txnID: "",
					bcks:  []*meta.Bck{bck, bckTo},
					wait:  true,
				}
				if _, err = p.owner.bmd.modify(ctx); err != nil {
					debug.AssertNoErr(err)
					err = fmt.Errorf(warnfmt+": %w", p, "failed to ", bckTo.String(), bck.String(), err)
					p.writeErr(w, r, err)
					return
				}
				nlog.Warningf(warnfmt, p, "", bckTo.String(), bck.String())
			}
		}
		dsort.PstartHandler(w, r, parsc)
	case http.MethodGet:
		dsort.PgetHandler(w, r)
	case http.MethodDelete:
		switch {
		case len(apiItems) == 1 && apiItems[0] == apc.Abort:
			dsort.PabortHandler(w, r)
		case len(apiItems) == 0:
			dsort.PremoveHandler(w, r)
		default:
			p.writeErrURL(w, r)
		}
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodPost)
	}
}

func (p *proxy) initDsort(config *cmn.Config) { dsort.Pinit(p, config) }

//
// target
//

func (*target) regDsort(handlers []networkHandler) []networkHandler {
	return append(handlers, networkHandler{r: apc.Sort, h: dsort.TargetHandler, net: accessControlData})
}

func (*target) initDsort(db kvdb.Driver, config *cmn.Config) { dsort.Tinit(db, config) }
