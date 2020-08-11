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

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
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
)

// TODO -- FIXME: add redirect-to-owner capability to support list/query caching
func (p *proxyrunner) reverseToOwner(w http.ResponseWriter, r *http.Request, uuid string,
	msg interface{}) (reversedOrFailed bool) {
	var (
		smap          = p.owner.smap.get()
		selfIC        = smap.IsIC(p.si)
		owner, exists = p.notifs.getOwner(uuid)
		psi           *cluster.Snode
	)
	if exists {
		goto outer
	}
	if selfIC {
		withLocalRetry(func() bool {
			owner, exists = p.notifs.getOwner(uuid)
			return exists
		})

		if !exists {
			p.invalmsghdlrf(w, r, "%q not found (%s)", uuid, smap.StrIC(p.si))
			return true
		}
	} else {
		hrwOwner, err := cluster.HrwIC(&smap.Smap, uuid)
		if err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusInternalServerError)
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
			owner = p.si.ID()
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
		cmn.AssertMsg(smap.IsIC(psi), owner+", "+smap.StrIC(p.si)) // TODO -- FIXME: handle
	}
	if owner == p.si.ID() {
		return
	}
	// otherwise, hand it over
	if msg != nil {
		body := cmn.MustMarshal(msg)
		r.Body = ioutil.NopCloser(bytes.NewReader(body))
	}
	p.reverseNodeRequest(w, r, psi)
	return true
}

func (p *proxyrunner) checkEntry(w http.ResponseWriter, r *http.Request, uuid string) (nl notifListener, ok bool) {
	nl, exists := p.notifs.entry(uuid)
	if !exists {
		smap := p.owner.smap.get()
		p.invalmsghdlrstatusf(w, r, http.StatusNotFound, "%q not found (%s)", uuid, smap.StrIC(p.si))
		return
	}
	if nl.finished() {
		// TODO: Maybe we should just return empty response and `http.StatusNoContent`?
		smap := p.owner.smap.get()
		p.invalmsghdlrstatusf(w, r, http.StatusGone, "%q finished (%s)", uuid, smap.StrIC(p.si))
		return
	}
	return nl, true
}

func (p *proxyrunner) writeStatus(w http.ResponseWriter, r *http.Request, uuid string) {
	nl, exists := p.notifs.entry(uuid)
	if !exists {
		smap := p.owner.smap.get()
		p.invalmsghdlrstatusf(w, r, http.StatusNotFound, "%q not found (%s)", uuid, smap.StrIC(p.si))
		return
	}

	if err := nl.err(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	// TODO: Also send stats, eg. progress when ready
	w.Write(cmn.MustMarshal(nl.finished()))
}

// verb /v1/ic
func (p *proxyrunner) icHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		p.icPost(w, r)
	default:
		cmn.Assert(false)
	}
}

// POST /v1/ic
func (p *proxyrunner) icPost(w http.ResponseWriter, r *http.Request) {
	smap := p.owner.smap.get()
	msg := &aisMsg{}
	if err := cmn.ReadJSON(w, r, msg); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	reCheck := true
check:
	if !smap.IsIC(p.si) {
		if msg.SmapVersion < smap.Version || !reCheck {
			p.invalmsghdlrf(w, r, "%s: not an IC member", p.si)
			return
		}

		reCheck = false
		// wait for smap update
		withLocalRetry(func() bool {
			smap = p.owner.smap.get()
			return smap.IsIC(p.si)
		})
		goto check
	}

	switch msg.Action {
	case cmn.ActMergeOwnershipTbl:
		if err := cmn.MorphMarshal(msg.Value, &p.notifs); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
	case cmn.ActListenToNotif:
		nlMsg := &notifListenMsg{}
		if err := cmn.MorphMarshal(msg.Value, nlMsg); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		cmn.Assert(nlMsg.nl.notifTy() == notifXact || nlMsg.nl.notifTy() == notifCache)
		p.notifs.add(nlMsg.nl)
	default:
		p.invalmsghdlrf(w, r, fmtUnknownAct, msg.ActionMsg)
	}
}

func (p *proxyrunner) registerEqualIC(a regIC) {
	if a.query != nil {
		a.query.Add(cmn.URLParamNotifyMe, equalIC)
	}
	if a.smap.IsIC(p.si) {
		p.notifs.add(a.nl)
	}
	if len(a.smap.IC) > 1 {
		// TODO -- FIXME: handle errors, here and elsewhere
		_ = p.bcastListenIC(a.nl, a.smap)
	}
}

func (p *proxyrunner) bcastListenIC(nl notifListener, smap *smapX) (err error) {
	nodes := make(cluster.NodeMap, len(smap.IC))
	for pid := range smap.IC {
		if pid != p.si.ID() {
			psi := smap.GetProxy(pid)
			cmn.Assert(psi != nil)
			nodes.Add(psi)
		}
	}
	actMsg := cmn.ActionMsg{Action: cmn.ActListenToNotif, Value: newNLMsg(nl)}
	msg := p.newAisMsg(&actMsg, smap, nil)
	cmn.Assert(len(nodes) > 0)
	results := p.bcastToNodes(bcastArgs{
		req: cmn.ReqArgs{
			Method: http.MethodPost,
			Path:   cmn.URLPath(cmn.Version, cmn.IC),
			Body:   cmn.MustMarshal(msg),
		},
		network: cmn.NetworkIntraControl,
		timeout: cmn.GCO.Get().Timeout.MaxKeepalive,
		nodes:   []cluster.NodeMap{nodes},
	})
	for res := range results {
		if res.err != nil {
			glog.Error(res.err)
			err = res.err
		}
	}
	return
}

func (p *proxyrunner) sendOwnershipTbl(si *cluster.Snode) error {
	actMsg := &cmn.ActionMsg{Action: cmn.ActMergeOwnershipTbl, Value: &p.notifs}
	msg := p.newAisMsg(actMsg, nil, nil)
	result := p.call(callArgs{si: si,
		req: cmn.ReqArgs{Method: http.MethodPost,
			Path: cmn.URLPath(cmn.Version, cmn.IC),
			Body: cmn.MustMarshal(msg),
		}, timeout: cmn.LongTimeout},
	)
	return result.err
}
