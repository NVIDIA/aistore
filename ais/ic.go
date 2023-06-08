// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/xact"
	jsoniter "github.com/json-iterator/go"
)

// Information Center (IC) is a group of proxies that take care of ownership of
// (Job, Task, eXtended action) entities. IC manages their lifecycle and monitors
// status. When a job, task or xaction is created, it gets registered with all IC
// members. Henceforth, IC acts as information source as far as status (running,
// aborted, finished), progress, and statistics.
//
// Non-IC AIS proxies, on the other hand, redirect all corresponding requests to
// one (anyone) of the IC (proxy) members.

const (
	// Implies equal ownership by all IC members and applies to all async ops
	// that have no associated cache other than start/end timestamps and stats counters
	// (case in point: list/query-objects that MAY be cached, etc.)
	equalIC = "\x00"
)

type (
	regIC struct {
		nl    nl.Listener
		smap  *smapX
		query url.Values
		msg   any
	}

	xactRegMsg struct {
		UUID string   `json:"uuid"`
		Kind string   `json:"kind"`
		Srcs []string `json:"srcs"` // list of daemonIDs
	}

	icBundle struct {
		Smap         *smapX              `json:"smap"`
		OwnershipTbl jsoniter.RawMessage `json:"ownership_table"`
	}

	ic struct {
		p *proxy
	}
)

func (ic *ic) init(p *proxy) {
	ic.p = p
}

func (ic *ic) reverseToOwner(w http.ResponseWriter, r *http.Request, uuid string, msg any) (reversedOrFailed bool) {
	retry := true
begin:
	var (
		smap          = ic.p.owner.smap.get()
		selfIC        = smap.IsIC(ic.p.si)
		owner, exists = ic.p.notifs.getOwner(uuid)
		psi           *meta.Snode
	)
	if exists {
		goto outer
	}
	if selfIC {
		if !exists && !retry {
			err := fmt.Errorf("x-[%s] not found (%s)", uuid, smap.StrIC(ic.p.si))
			ic.p.writeErr(w, r, err, http.StatusNotFound, Silent)
			return true
		}
		if retry {
			withRetry(cmn.Timeout.CplaneOperation(), func() bool {
				owner, exists = ic.p.notifs.getOwner(uuid)
				return exists
			})
			if !exists {
				retry = false
				_ = ic.syncICBundle() // TODO handle error
				goto begin
			}
		}
	} else {
		hrwOwner, err := cluster.HrwIC(&smap.Smap, uuid)
		if err != nil {
			ic.p.writeErr(w, r, err, http.StatusInternalServerError)
			return true
		}
		owner = hrwOwner.ID()
	}
outer:
	switch owner {
	case "": // not owned
		return
	case equalIC:
		if selfIC {
			owner = ic.p.SID()
		} else {
			for pid, si := range smap.Pmap {
				if !smap.IsIC(psi) {
					continue
				}
				owner = pid
				psi = si
				break outer
			}
		}
	default: // cached + owned
		psi = smap.GetProxy(owner)
		if psi == nil || !smap.IsIC(psi) {
			var err error
			if psi, err = cluster.HrwIC(&smap.Smap, uuid); err != nil {
				ic.p.writeErr(w, r, err, http.StatusInternalServerError)
				return true
			}
		}
		debug.Assertf(smap.IsIC(psi), "%s, %s", psi, smap.StrIC(ic.p.si))
	}
	if owner == ic.p.SID() {
		return
	}
	// otherwise, hand it over
	if msg != nil {
		body := cos.MustMarshal(msg)
		r.ContentLength = int64(len(body))
		r.Body = io.NopCloser(bytes.NewReader(body))
	}
	ic.p.reverseNodeRequest(w, r, psi)
	return true
}

func (ic *ic) redirectToIC(w http.ResponseWriter, r *http.Request) bool {
	smap := ic.p.owner.smap.get()
	if smap.IsIC(ic.p.si) {
		return false
	}

	var node *meta.Snode
	for _, psi := range smap.Pmap {
		if smap.IsIC(psi) {
			node = psi
			break
		}
	}
	redirectURL := ic.p.redirectURL(r, node, time.Now(), cmn.NetIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
	return true
}

func (ic *ic) xstatusAll(w http.ResponseWriter, r *http.Request, query url.Values) {
	msg := &xact.QueryMsg{}
	if err := cmn.ReadJSON(w, r, msg); err != nil {
		return
	}
	flt := nlFilter{ID: msg.ID, Kind: msg.Kind, Bck: (*meta.Bck)(&msg.Bck), OnlyRunning: msg.OnlyRunning}
	if !msg.Bck.IsEmpty() {
		flt.Bck = (*meta.Bck)(&msg.Bck)
	}
	nls := ic.p.notifs.findAll(flt)

	var vec nl.StatusVec

	if cos.IsParseBool(query.Get(apc.QparamForce)) {
		// (force just-in-time)
		// for each args-selected xaction:
		// check if any of the targets delayed updating the corresponding status,
		// and query those targets directly
		var (
			config   = cmn.GCO.Get()
			interval = config.Periodic.NotifTime.D()
		)
		for _, nl := range nls {
			ic.p.notifs.bcastGetStats(nl, interval)
			status := nl.Status()
			if err := nl.Err(); err != nil {
				status.ErrMsg = err.Error()
			}
			vec = append(vec, *status)
		}
	} else {
		for _, nl := range nls {
			vec = append(vec, *nl.Status())
		}
	}
	b := cos.MustMarshal(vec)
	w.Header().Set(cos.HdrContentLength, strconv.Itoa(len(b)))
	w.Write(b)
}

func (ic *ic) xstatusOne(w http.ResponseWriter, r *http.Request) {
	var (
		nl  nl.Listener
		bck *meta.Bck
		msg = &xact.QueryMsg{}
	)
	if err := cmn.ReadJSON(w, r, msg); err != nil {
		return
	}
	msg.Kind, _ = xact.GetKindName(msg.Kind) // display name => kind
	if msg.ID == "" && msg.Kind == "" {
		ic.p.writeErrStatusf(w, r, http.StatusBadRequest, "invalid %s", msg)
		return
	}

	// for queries of the type {Kind: apc.ActRebalance}
	if msg.ID == "" && ic.redirectToIC(w, r) {
		return
	}
	if msg.ID != "" && ic.reverseToOwner(w, r, msg.ID, msg) {
		return
	}

	if msg.Bck.Name != "" {
		bck = meta.CloneBck(&msg.Bck)
		if err := bck.Init(ic.p.owner.bmd); err != nil {
			ic.p.writeErr(w, r, err, http.StatusNotFound, Silent)
			return
		}
	}
	flt := nlFilter{ID: msg.ID, Kind: msg.Kind, Bck: bck, OnlyRunning: msg.OnlyRunning}
	withRetry(cmn.Timeout.CplaneOperation(), func() bool {
		nl = ic.p.notifs.find(flt)
		return nl != nil
	})
	if nl == nil {
		smap := ic.p.owner.smap.get()
		err := fmt.Errorf("nl not found: %s, %s", smap.StrIC(ic.p.si), msg)
		ic.p.writeErr(w, r, err, http.StatusNotFound, Silent)
		return
	}

	if msg.Kind != "" && nl.Kind() != msg.Kind {
		ic.p.writeErrf(w, r, "kind mismatch: %s, expected kind=%s", msg, nl.Kind())
		return
	}

	// refresh NotifStatus
	var (
		config   = cmn.GCO.Get()
		interval = config.Periodic.NotifTime.D()
	)
	ic.p.notifs.bcastGetStats(nl, interval)

	status := nl.Status()
	if err := nl.Err(); err != nil {
		status.ErrMsg = err.Error()
		if !nl.Aborted() {
			ic.p.writeErrf(w, r, "%v: %v", nl, err)
			return
		}
	}
	b := cos.MustMarshal(status) // TODO: include stats, e.g., progress when ready
	w.Header().Set(cos.HdrContentLength, strconv.Itoa(len(b)))
	w.Write(b)
}

// verb /v1/ic
func (ic *ic) handler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		ic.handleGet(w, r)
	case http.MethodPost:
		ic.handlePost(w, r)
	default:
		debug.Assert(false)
	}
}

// GET /v1/ic
func (ic *ic) handleGet(w http.ResponseWriter, r *http.Request) {
	var (
		smap = ic.p.owner.smap.get()
		what = r.URL.Query().Get(apc.QparamWhat)
	)
	if !smap.IsIC(ic.p.si) {
		ic.p.writeErrf(w, r, "%s: not an IC member", ic.p.si)
		return
	}

	switch what {
	case apc.WhatICBundle:
		bundle := icBundle{Smap: smap, OwnershipTbl: cos.MustMarshal(&ic.p.notifs)}
		ic.p.writeJSON(w, r, bundle, what)
	default:
		ic.p.writeErrf(w, r, fmtUnknownQue, what)
	}
}

// POST /v1/ic
func (ic *ic) handlePost(w http.ResponseWriter, r *http.Request) {
	var (
		smap = ic.p.owner.smap.get()
		msg  = &aisMsg{}
	)
	if err := cmn.ReadJSON(w, r, msg); err != nil {
		return
	}
	if !smap.IsIC(ic.p.si) {
		if !withRetry(cmn.Timeout.CplaneOperation(), func() bool {
			smap = ic.p.owner.smap.get()
			return smap.IsIC(ic.p.si)
		}) {
			ic.p.writeErrf(w, r, "%s: not an IC member", ic.p.si)
			return
		}
	}

	switch msg.Action {
	case apc.ActMergeOwnershipTbl:
		if err := cos.MorphMarshal(msg.Value, &ic.p.notifs); err != nil {
			ic.p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, ic.p.si, msg.Action, msg.Value, err)
			return
		}
	case apc.ActListenToNotif:
		nlMsg := &notifListenMsg{}
		if err := cos.MorphMarshal(msg.Value, nlMsg); err != nil {
			ic.p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, ic.p.si, msg.Action, msg.Value, err)
			return
		}
		if err := ic.p.notifs.add(nlMsg.nl); err != nil {
			ic.p.writeErr(w, r, err)
			return
		}
	case apc.ActRegGlobalXaction:
		var (
			regMsg     = &xactRegMsg{}
			tmap       meta.NodeMap
			callerSver = r.Header.Get(apc.HdrCallerSmapVersion)
			err        error
		)
		if err = cos.MorphMarshal(msg.Value, regMsg); err != nil {
			ic.p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, ic.p.si, msg.Action, msg.Value, err)
			return
		}
		debug.Assert(len(regMsg.Srcs) != 0)
		withRetry(cmn.Timeout.CplaneOperation(), func() bool {
			smap = ic.p.owner.smap.get()
			tmap, err = smap.NewTmap(regMsg.Srcs)
			return err == nil && callerSver == smap.vstr
		})
		if err != nil {
			ic.p.writeErrStatusf(w, r, http.StatusNotFound, "%s: failed to %q: %v", ic.p, msg.Action, err)
			return
		}
		nl := xact.NewXactNL(regMsg.UUID, regMsg.Kind, &smap.Smap, tmap)
		if err = ic.p.notifs.add(nl); err != nil {
			ic.p.writeErr(w, r, err)
			return
		}
	default:
		ic.p.writeErrAct(w, r, msg.Action)
	}
}

func (ic *ic) registerEqual(a regIC) {
	if a.query != nil {
		a.query.Set(apc.QparamNotifyMe, equalIC)
	}
	if a.smap.IsIC(ic.p.si) {
		err := ic.p.notifs.add(a.nl)
		debug.AssertNoErr(err)
	}
	if a.smap.ICCount() > 1 {
		ic.bcastListenIC(a.nl)
	}
}

func (ic *ic) bcastListenIC(nl nl.Listener) {
	var (
		actMsg = apc.ActMsg{Action: apc.ActListenToNotif, Value: newNLMsg(nl)}
		msg    = ic.p.newAmsg(&actMsg, nil)
	)
	ic.p.bcastAsyncIC(msg)
}

func (ic *ic) sendOwnershipTbl(si *meta.Snode) error {
	if ic.p.notifs.size() == 0 {
		if cmn.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s: notifs empty, not sending to %s", ic.p, si)
		}
		return nil
	}
	msg := ic.p.newAmsgActVal(apc.ActMergeOwnershipTbl, &ic.p.notifs)
	cargs := allocCargs()
	{
		cargs.si = si
		cargs.req = cmn.HreqArgs{Method: http.MethodPost, Path: apc.URLPathIC.S, Body: cos.MustMarshal(msg)}
		cargs.timeout = cmn.Timeout.CplaneOperation()
	}
	res := ic.p.call(cargs)
	freeCargs(cargs)
	return res.err
}

// sync ownership table; TODO: review control flows and revisit impl.
func (ic *ic) syncICBundle() error {
	smap := ic.p.owner.smap.get()
	si := ic.p.si
	for _, psi := range smap.Pmap {
		if smap.IsIC(psi) && psi.ID() != si.ID() {
			si = psi
			break
		}
	}

	if si.Equals(ic.p.si) {
		return nil
	}
	cargs := allocCargs()
	{
		cargs.si = si
		cargs.req = cmn.HreqArgs{
			Method: http.MethodGet,
			Path:   apc.URLPathIC.S,
			Query:  url.Values{apc.QparamWhat: []string{apc.WhatICBundle}},
		}
		cargs.timeout = cmn.Timeout.CplaneOperation()
		cargs.cresv = cresIC{} // -> icBundle
	}
	res := ic.p.call(cargs)
	freeCargs(cargs)
	if res.err != nil {
		return res.err
	}

	bundle := res.v.(*icBundle)
	debug.Assertf(smap.UUID == bundle.Smap.UUID, "%s vs %s", smap.StringEx(), bundle.Smap.StringEx())

	if err := ic.p.owner.smap.synchronize(ic.p.si, bundle.Smap, nil /*ms payload*/, ic.p.htrun.smapUpdatedCB); err != nil {
		if !isErrDowngrade(err) {
			glog.Error(cmn.NewErrFailedTo(ic.p, "sync", bundle.Smap, err))
		}
	} else {
		smap = ic.p.owner.smap.get()
		glog.Infof("%s: synch %s", ic.p, smap)
	}

	if !smap.IsIC(ic.p.si) {
		return nil
	}
	if err := jsoniter.Unmarshal(bundle.OwnershipTbl, &ic.p.notifs); err != nil {
		return fmt.Errorf(cmn.FmtErrUnmarshal, ic.p, "ownership table", cos.BHead(bundle.OwnershipTbl), err)
	}
	return nil
}
