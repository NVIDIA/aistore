// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/query"
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
	msg interface{}) (reversed bool, err error) {
	var (
		smap          = p.owner.smap.get()
		selfIC, _     = p.whichIC(smap, nil)
		owner, exists = p.notifs.getOwner(uuid)
		psi           *cluster.Snode
	)
	if exists {
		goto outer
	}
	if selfIC {
		const max = 5
		var (
			sleep = cmn.GCO.Get().Timeout.CplaneOperation / 2
		)
		for i := 0; i < max && !exists; i++ {
			time.Sleep(sleep)
			owner, exists = p.notifs.getOwner(uuid)
		}
		if !exists {
			err = fmt.Errorf("%q not found (%s)", uuid, smap.strIC(p.si))
			return
		}
	} else {
		owner = smap.Primary.ID() // TODO -- FIXME: equalIC works only for non-cached; see also hrw-select
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
				cmn.Assert(smap.isIC(psi))
				break outer
			}
		}
	default: // cached + owned
		psi = smap.GetProxy(owner)
		cmn.AssertMsg(smap.isIC(psi), owner+", "+smap.strIC(p.si)) // TODO -- FIXME: handle
	}
	if owner == p.si.ID() {
		return
	}
	// otherwise, hand it over
	if msg != nil {
		body := cmn.MustMarshal(msg)
		r.Body = ioutil.NopCloser(bytes.NewReader(body))
	}
	reversed = true
	p.reverseNodeRequest(w, r, psi)
	return
}

func (p *proxyrunner) checkEntry(w http.ResponseWriter, r *http.Request, uuid string) (nl notifListener, ok bool) {
	nl, exists := p.notifs.entry(uuid)
	if !exists {
		smap := p.owner.smap.get()
		p.invalmsghdlrstatusf(w, r, http.StatusNotFound, "%q not found (%s)", uuid, smap.strIC(p.si))
		return
	}
	if nl.finished() {
		// TODO: Maybe we should just return empty response and `http.StatusNoContent`?
		smap := p.owner.smap.get()
		p.invalmsghdlrstatusf(w, r, http.StatusGone, "%q finished (%s)", uuid, smap.strIC(p.si))
		return
	}
	return nl, true
}

func (p *proxyrunner) writeStatus(w http.ResponseWriter, r *http.Request, uuid string) {
	nl, exists := p.notifs.entry(uuid)
	if !exists {
		smap := p.owner.smap.get()
		p.invalmsghdlrstatusf(w, r, http.StatusNotFound, "%q not found (%s)", uuid, smap.strIC(p.si))
		return
	}

	if err := nl.err(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	// TODO: Also send stats, eg. progress when ready
	w.Write(cmn.MustMarshal(nl.finished()))
}

// POST /v1/ic
func (p *proxyrunner) listenICHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var (
			msg  notifListenMsg
			smap = p.owner.smap.get()
		)
		if selfIC, _ := p.whichIC(smap, nil); !selfIC {
			p.invalmsghdlrf(w, r, "%s: not IC member", p.si)
			return
		}
		if err := cmn.ReadJSON(w, r, &msg); err != nil {
			cmn.InvalidHandlerWithMsg(w, r, err.Error())
			return
		}
		cmn.Assert(msg.Ty == notifXact || msg.Ty == notifCache)
		cmn.Assert(smap.version() == msg.SmapVersion) // TODO -- FIXME: handle

		tmap, _ := smap.GetTargetMap(msg.Srcs)

		switch msg.Action {
		case cmn.ActQueryObjects:
			initMsg := query.InitMsg{}
			if err := cmn.MorphMarshal(msg.Ext, &initMsg); err != nil {
				p.invalmsghdlrf(w, r, "%s: invalid msg %+v", msg.Action, msg.Ext)
				return
			}
			nlq, err := newQueryListener(msg.UUID, msg.Ty, tmap, &initMsg)
			if err != nil {
				p.invalmsghdlr(w, r, err.Error())
				return
			}
			nlq.setOwner(msg.Owner)
			p.notifs.add(nlq)
		default:
			nl := &notifListenerBase{uuid: msg.UUID, ty: msg.Ty, srcs: tmap}
			nl.setOwner(msg.Owner)
			p.notifs.add(nl)
		}
	default:
		cmn.Assert(false)
	}
}

func (p *proxyrunner) whichIC(smap *smapX, query url.Values) (selfIC, otherIC bool) {
	cmn.Assert(len(smap.IC) > 0)
	selfIC = smap.isIC(p.si)
	otherIC = len(smap.IC) > 1
	cmn.Assert(selfIC || otherIC)
	if query == nil {
		return
	}
	for pid := range smap.IC {
		query.Add(cmn.URLParamNotifyMe, pid)
	}
	return
}

func (p *proxyrunner) registerIC(a regIC) {
	selfIC, otherIC := p.whichIC(a.smap, a.query)
	if selfIC {
		p.notifs.add(a.nl)
	}
	if otherIC {
		// TODO -- FIXME: handle errors, here and elsewhere
		_ = p.bcastListenIC(a.nl, a.smap, a.msg)
	}
}

func (p *proxyrunner) bcastListenIC(nl notifListener, smap *smapX, msg interface{}) (err error) {
	nodes := make(cluster.NodeMap)
	for pid := range smap.IC {
		if pid != p.si.ID() {
			psi := smap.GetProxy(pid)
			cmn.Assert(psi != nil)
			nodes.Add(psi)
		}
	}
	nlMsg := nlMsgFromListener(nl, smap)
	if msg != nil {
		nlMsg.Ext = msg
	}

	cmn.Assert(len(nodes) > 0)
	results := p.bcast(bcastArgs{
		req: cmn.ReqArgs{Method: http.MethodPost,
			Path: cmn.URLPath(cmn.Version, cmn.IC),
			Body: cmn.MustMarshal(nlMsg)},
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

// helpers

func nlMsgFromListener(nl notifListener, smap *smapX) *notifListenMsg {
	cmn.Assert(nl.notifTy() > notifInvalid)
	cmn.Assert(nl.UUID() != "")
	n := &notifListenMsg{
		UUID:        nl.UUID(),
		Ty:          nl.notifTy(),
		SmapVersion: smap.version(),
		Action:      nl.kind(),
		Bck:         nl.bcks(),
		Owner:       nl.getOwner(),
	}
	for daeID := range nl.notifiers() {
		n.Srcs = append(n.Srcs, daeID)
	}
	return n
}
