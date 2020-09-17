// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
)

// Information Center (IC) is a group of proxies that take care of ownership of
// jtx (Job, Task, eXtended action) entities. It manages the lifecycle of an entity (uuid),
// and monitors its status (metadata). When an entity is created, it is registered with the
// members of IC. The IC members monitor all the entities (by uuid) registered to them,
// and act as information sources for those entities. Non-IC proxies redirect entity related
// requests to one of the IC members.

const (
	// Implies equal ownership by all IC members and applies to all async ops
	// that have no associated cache other than start/end timestamps and stats counters
	// (case in point: list/query-objects that MAY be cached, etc.)
	equalIC = "\x00"
)

type (
	regIC struct {
		nl    notifListener
		smap  *smapX
		query url.Values
		msg   interface{}
	}

	xactRegMsg struct {
		UUID string   `json:"uuid"`
		Kind string   `json:"kind"`
		Scrs []string `json:"srcs"` // list of daemonIDs
	}

	icBundle struct {
		Smap         *smapX              `json:"smap"`
		OwnershipTbl jsoniter.RawMessage `json:"ownership_table"`
	}

	ic struct {
		p *proxyrunner
	}
)

func (ic *ic) init(p *proxyrunner) {
	ic.p = p
}

// TODO -- FIXME: add redirect-to-owner capability to support list/query caching
func (ic *ic) reverseToOwner(w http.ResponseWriter, r *http.Request, uuid string,
	msg interface{}) (reversedOrFailed bool) {
	retry := true
begin:
	var (
		smap          = ic.p.owner.smap.get()
		selfIC        = smap.IsIC(ic.p.si)
		owner, exists = ic.p.notifs.getOwner(uuid)
		psi           *cluster.Snode
	)
	if exists {
		goto outer
	}
	if selfIC {
		if !exists && !retry {
			ic.p.invalmsghdlrf(w, r, "%q not found (%s)", uuid, smap.StrIC(ic.p.si))
			return true
		} else if retry {
			withLocalRetry(func() bool {
				owner, exists = ic.p.notifs.getOwner(uuid)
				return exists
			})

			if !exists {
				retry = false
				_ = ic.syncICBundle() // TODO -- handle error
				goto begin
			}
		}
	} else {
		hrwOwner, err := cluster.HrwIC(&smap.Smap, uuid)
		if err != nil {
			ic.p.invalmsghdlr(w, r, err.Error(), http.StatusInternalServerError)
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
			owner = ic.p.si.ID()
		} else {
			for pid := range smap.IC {
				owner = pid
				psi = smap.GetProxy(owner)
				cmn.Assert(smap.IsIC(psi))
				break outer
			}
		}
	default: // cached + owned
		psi = smap.GetProxy(owner)
		cmn.Assertf(smap.IsIC(psi), "%s, %s", owner, smap.StrIC(ic.p.si)) // TODO -- FIXME: handle
	}
	if owner == ic.p.si.ID() {
		return
	}
	// otherwise, hand it over
	if msg != nil {
		body := cmn.MustMarshal(msg)
		r.Body = ioutil.NopCloser(bytes.NewReader(body))
	}
	ic.p.reverseNodeRequest(w, r, psi)
	return true
}

// TODO: add more functionality similar to reverseToOwner
func (ic *ic) redirectToIC(w http.ResponseWriter, r *http.Request) bool {
	smap := ic.p.owner.smap.get()
	if !smap.IsIC(ic.p.si) {
		var node *cluster.Snode
		for nodeID := range smap.IC {
			node = smap.GetNode(nodeID)
			break
		}
		redirectURL := ic.p.redirectURL(r, node, time.Now(), cmn.NetworkIntraControl)
		http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
		return true
	}
	return false
}

func (ic *ic) checkEntry(w http.ResponseWriter, r *http.Request, uuid string) (nl notifListener, ok bool) {
	nl, exists := ic.p.notifs.entry(uuid)
	if !exists {
		smap := ic.p.owner.smap.get()
		ic.p.invalmsghdlrstatusf(w, r, http.StatusNotFound, "%q not found (%s)", uuid, smap.StrIC(ic.p.si))
		return
	}
	if nl.finished() {
		// TODO: Maybe we should just return empty response and `http.StatusNoContent`?
		smap := ic.p.owner.smap.get()
		ic.p.invalmsghdlrstatusf(w, r, http.StatusGone, "%q finished (%s)", uuid, smap.StrIC(ic.p.si))
		return
	}
	return nl, true
}

func (ic *ic) writeStatus(w http.ResponseWriter, r *http.Request, what string) {
	var (
		msg    = &cmn.XactReqMsg{}
		bck    *cluster.Bck
		nl     notifListener
		exists bool
	)

	if err := cmn.ReadJSON(w, r, msg); err != nil {
		return
	}

	if msg.ID == "" && msg.Kind == "" {
		ic.p.invalmsghdlrstatusf(w, r, http.StatusBadRequest,
			"missing `id` and `kind` for `what`: %v", what)
		return
	}

	// for queries of type {Kind: cmn.ActRebalance}
	if msg.ID == "" && ic.redirectToIC(w, r) {
		return
	} else if msg.ID != "" && ic.reverseToOwner(w, r, msg.ID, msg) {
		return
	}

	if msg.Bck.Name != "" {
		bck = cluster.NewBckEmbed(msg.Bck)
		if err := bck.Init(ic.p.owner.bmd, ic.p.si); err != nil {
			ic.p.invalmsghdlrsilent(w, r, err.Error(), http.StatusNotFound)
			return
		}
	}
	flt := nlFilter{ID: msg.ID, Kind: msg.Kind, Bck: bck, OnlyRunning: msg.OnlyRunning}

	withLocalRetry(func() bool {
		nl, exists = ic.p.notifs.find(flt)
		return exists
	}, 2)

	if !exists {
		smap := ic.p.owner.smap.get()
		ic.p.invalmsghdlrstatusf(w, r, http.StatusNotFound,
			"id:%q, kind:%q, bck:%q not found (%s)", msg.ID, msg.Kind, msg.Bck, smap.StrIC(ic.p.si))
		return
	}

	if msg.Kind != "" && nl.kind() != msg.Kind {
		ic.p.invalmsghdlrf(w, r, "xaction kind mismatch (ID: %s, KIND: %s != %s)",
			msg.ID, msg.Kind, nl.kind())
		return
	}

	nl.rlock()
	defer nl.runlock()

	if err := nl.err(); err != nil && !nl.aborted() {
		ic.p.invalmsghdlr(w, r, err.Error())
		return
	}

	// TODO: Also send stats, eg. progress when ready
	w.Write(cmn.MustMarshal(nl.status()))
}

// verb /v1/ic
func (ic *ic) handler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		ic.handleGet(w, r)
	case http.MethodPost:
		ic.handlePost(w, r)
	default:
		cmn.Assert(false)
	}
}

// GET /v1/ic
func (ic *ic) handleGet(w http.ResponseWriter, r *http.Request) {
	var (
		smap = ic.p.owner.smap.get()
		what = r.URL.Query().Get(cmn.URLParamWhat)
	)
	if !smap.IsIC(ic.p.si) {
		ic.p.invalmsghdlrf(w, r, "%s: not an IC member", ic.p.si)
		return
	}

	switch what {
	case cmn.GetWhatICBundle:
		bundle := icBundle{Smap: smap, OwnershipTbl: cmn.MustMarshal(&ic.p.notifs)}
		ic.p.writeJSON(w, r, bundle, what)
	default:
		ic.p.invalmsghdlrf(w, r, fmtUnknownQue, what)
	}
}

// POST /v1/ic
func (ic *ic) handlePost(w http.ResponseWriter, r *http.Request) {
	smap := ic.p.owner.smap.get()
	msg := &aisMsg{}
	if err := cmn.ReadJSON(w, r, msg); err != nil {
		ic.p.invalmsghdlr(w, r, err.Error())
		return
	}

	reCheck := true
check:
	if !smap.IsIC(ic.p.si) {
		if msg.SmapVersion < smap.Version || !reCheck {
			ic.p.invalmsghdlrf(w, r, "%s: not an IC member", ic.p.si)
			return
		}

		reCheck = false
		// wait for smap update
		withLocalRetry(func() bool {
			smap = ic.p.owner.smap.get()
			return smap.IsIC(ic.p.si)
		})
		goto check
	}

	switch msg.Action {
	case cmn.ActMergeOwnershipTbl:
		if err := cmn.MorphMarshal(msg.Value, &ic.p.notifs); err != nil {
			ic.p.invalmsghdlr(w, r, err.Error())
			return
		}
	case cmn.ActListenToNotif:
		nlMsg := &notifListenMsg{}
		if err := cmn.MorphMarshal(msg.Value, nlMsg); err != nil {
			ic.p.invalmsghdlr(w, r, err.Error())
			return
		}
		cmn.Assert(nlMsg.nl.notifTy() == notifXact || nlMsg.nl.notifTy() == notifCache)
		ic.p.notifs.add(nlMsg.nl)
	case cmn.ActRegGlobalXaction:
		regMsg := &xactRegMsg{}
		if err := cmn.MorphMarshal(msg.Value, regMsg); err != nil {
			ic.p.invalmsghdlr(w, r, err.Error())
			return
		}

		srcs, err := smap.GetTargetMap(regMsg.Scrs)
		if err != nil {
			ic.p.invalmsghdlr(w, r, err.Error())
			return
		}
		nl := newXactNL(regMsg.UUID, smap, srcs, notifXact, regMsg.Kind)
		ic.p.notifs.add(nl)
	default:
		ic.p.invalmsghdlrf(w, r, fmtUnknownAct, msg.ActionMsg)
	}
}

func (ic *ic) registerEqual(a regIC) {
	if a.query != nil {
		a.query.Add(cmn.URLParamNotifyMe, equalIC)
	}
	if a.smap.IsIC(ic.p.si) {
		ic.p.notifs.add(a.nl)
	}
	if len(a.smap.IC) > 1 {
		// TODO -- FIXME: handle errors, here and elsewhere
		_ = ic.bcastListenIC(a.nl, a.smap)
	}
}

func (ic *ic) bcastListenIC(nl notifListener, smap *smapX) (err error) {
	var (
		actMsg = cmn.ActionMsg{Action: cmn.ActListenToNotif, Value: newNLMsg(nl)}
		msg    = ic.p.newAisMsg(&actMsg, smap, nil)
	)

	results := ic.p.bcastToIC(msg)
	for res := range results {
		if res.err != nil {
			glog.Error(res.err)
			err = res.err
		}
	}
	return
}

func (ic *ic) sendOwnershipTbl(si *cluster.Snode) error {
	actMsg := &cmn.ActionMsg{Action: cmn.ActMergeOwnershipTbl, Value: &ic.p.notifs}
	msg := ic.p.newAisMsg(actMsg, nil, nil)
	result := ic.p.call(callArgs{si: si,
		req: cmn.ReqArgs{Method: http.MethodPost,
			Path: cmn.JoinWords(cmn.Version, cmn.IC),
			Body: cmn.MustMarshal(msg),
		}, timeout: cmn.GCO.Get().Timeout.CplaneOperation},
	)
	return result.err
}

// sync ownership table
func (ic *ic) syncICBundle() error {
	smap := ic.p.owner.smap.get()
	si, _ := smap.OldestIC()
	cmn.Assert(si != nil)

	if si.Equals(ic.p.si) {
		return nil
	}

	result := ic.p.call(callArgs{
		si: si,
		req: cmn.ReqArgs{
			Method: http.MethodGet,
			Path:   cmn.JoinWords(cmn.Version, cmn.IC),
			Query:  url.Values{cmn.URLParamWhat: []string{cmn.GetWhatICBundle}},
		},
		timeout: cmn.GCO.Get().Timeout.CplaneOperation,
	})
	if result.err != nil {
		// TODO: Handle error. Should try calling another IC member maybe.
		glog.Errorf("%s: failed to get ownership table from %s, err: %v", ic.p.si, si, result.err)
		return result.err
	}

	bundle := &icBundle{}
	if err := jsoniter.Unmarshal(result.bytes, bundle); err != nil {
		glog.Errorf("%s: failed to unmarshal ic bundle", ic.p.si)
		return err
	}

	cmn.Assertf(smap.UUID == bundle.Smap.UUID, "%s vs %s", smap.StringEx(), bundle.Smap.StringEx())

	if err := ic.p.owner.smap.synchronize(bundle.Smap, true /* lesserIsErr */); err != nil {
		glog.Errorf("%s: sync Smap err %v", ic.p.si, err)
	} else {
		smap = ic.p.owner.smap.get()
		glog.Infof("%s: sync %s", ic.p.si, ic.p.owner.smap.get())
	}

	if !smap.IsIC(ic.p.si) {
		return nil
	}
	return jsoniter.Unmarshal(bundle.OwnershipTbl, &ic.p.notifs)
}
