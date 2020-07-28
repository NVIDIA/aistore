// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"io/ioutil"
	"math/rand"
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
	// How long we should wait to make another check to see if the entry exist.
	// The entry could be on its way and was just added therefore we don't
	// return http.StatusNotFound immediately
	reCheckInterval = time.Second
)

func (p *proxyrunner) redirectToOwner(w http.ResponseWriter, r *http.Request, uuid string, msg interface{}) (redirected bool) {
	cmn.Assert(uuid != "")
	smap := p.owner.smap.get()
	selfIC, _ := p.whichIC(smap)

	if selfIC {
		reCheck := true

	Check:
		if p.notifs.isOwner(uuid) {
			return
		}
		// TODO: if selfIC, but doesn't contain xactID
		// Maybe we should check if other IC member have the entry?
		// For now retry once and return 404 if not found
		if reCheck {
			time.Sleep(reCheckInterval)
			reCheck = false
			goto Check
		}

		p.invalmsghdlrf(w, r, "%q not found", uuid)
		return true
	}

	if msg != nil {
		body := cmn.MustMarshal(msg)
		r.Body = ioutil.NopCloser(bytes.NewReader(body))
	}

	// Pick random owner and forward the request.
	owner := smap.IC[rand.Intn(len(smap.IC))]
	p.reverseNodeRequest(w, r, owner)
	return true
}

func (p *proxyrunner) checkEntry(w http.ResponseWriter, r *http.Request, uuid string) (nl notifListener, ok bool) {
	nl, exists := p.notifs.entry(uuid)
	if !exists {
		p.invalmsghdlrstatusf(w, r, http.StatusNotFound, "%q not found", uuid)
		return
	}
	if nl.finished() {
		// TODO: Maybe we should just return empty response and `http.StatusNoContent`?
		p.invalmsghdlrstatusf(w, r, http.StatusGone, "%q finished", uuid)
		return
	}
	return nl, true
}

func (p *proxyrunner) writeStatus(w http.ResponseWriter, r *http.Request, uuid string) {
	nl, exists := p.notifs.entry(uuid)
	if !exists {
		p.invalmsghdlrstatusf(w, r, http.StatusNotFound, "%q not found", uuid)
		return
	}

	if err := nl.err(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	// TODO: Also send stats, eg. progress when ready
	w.Write(cmn.MustMarshal(nl.finished()))
}

func (p *proxyrunner) registerIC(uuid string, nl notifListener, smap *smapX, msg interface{}, queries ...url.Values) {
	selfIC, otherIC := p.whichIC(smap, queries...)
	if selfIC {
		nl.setOwned(true)
		p.notifs.add(uuid, nl)
	}
	if otherIC {
		_ = p.bcastListenIC(uuid, nl, smap, msg)
		// TODO -- FIXME: handle errors, here and elsewhere
	}
}

func (p *proxyrunner) bcastListenIC(uuid string, nl notifListener, smap *smapX, msg interface{}) (err error) {
	nodes := make(cluster.NodeMap)
	for _, psi := range smap.IC {
		if psi.ID() != p.si.ID() {
			nodes.Add(psi)
		}
	}
	nlMsg := nlMsgFromListener(nl)
	nlMsg.UUID = uuid
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

// HTTP stuff

// POST /v1/ic
func (p *proxyrunner) listenICHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var (
			msg  notifListenMsg
			smap = p.owner.smap.get()
		)
		if selfIC, _ := p.whichIC(smap); !selfIC {
			p.invalmsghdlrf(w, r, "%s: not IC member", p.si)
			return
		}
		if err := cmn.ReadJSON(w, r, &msg); err != nil {
			cmn.InvalidHandlerWithMsg(w, r, err.Error())
			return
		}
		cmn.Assert(msg.Ty == notifXact) // TODO: support other notification types
		switch msg.Action {
		case cmn.ActQueryObjects:
			initMsg := query.InitMsg{}
			if err := cmn.MorphMarshal(msg.Ext, &initMsg); err != nil {
				p.invalmsghdlrf(w, r, "%s: invalid msg %+v", msg.Action, msg.Ext)
				return
			}
			nl, err := newQueryState(&smap.Smap, &initMsg)
			if err != nil {
				p.invalmsghdlr(w, r, err.Error())
				return
			}
			nl.owned = true
			p.notifs.add(msg.UUID, nl)
		default:
			nl := &notifListenerBase{owned: true}
			for _, sid := range msg.Srcs {
				if node, ok := smap.Tmap[sid]; ok {
					nl.srcs.Add(node)
				}
			}
			p.notifs.add(msg.UUID, nl)
		}
	default:
		cmn.Assert(false)
	}
}
