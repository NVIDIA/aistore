// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/query"
)

// Proxy exposes 2 methods:
// - Init(query) -> handle - initializes a query on proxy and targets
// - Next(handle, n) - returns next n objects from query registered by handle.
// Objects are returned in sorted order.

func (p *proxyrunner) queryHandler(w http.ResponseWriter, r *http.Request) {
	if !p.ClusterStarted() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		p.httpqueryget(w, r)
	case http.MethodPost:
		p.httpquerypost(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /query path")
	}
}

func (p *proxyrunner) httpquerypost(w http.ResponseWriter, r *http.Request) {
	if _, err := p.checkRESTItems(w, r, 0, false, cmn.URLPathQueryInit); err != nil {
		return
	}

	// A target will return error if given handle already exists (though is very unlikely).
	handle := cmn.GenUUID()
	header := http.Header{cmn.HeaderHandle: []string{handle}}
	msg := &query.InitMsg{}
	if err := cmn.ReadJSON(w, r, msg); err != nil {
		return
	}

	if _, err := query.NewQueryFromMsg(p, &msg.QueryMsg); err != nil {
		p.invalmsghdlr(w, r, "Failed to parse query message: "+err.Error())
		return
	}
	smap := p.owner.smap.get()
	args := bcastArgs{
		req: cmn.ReqArgs{
			Method: http.MethodPost,
			Path:   cmn.JoinWords(cmn.Version, cmn.Query, cmn.Init),
			Body:   cmn.MustMarshal(msg),
			Header: header,
		},
		timeout: cmn.DefaultTimeout,
		smap:    smap,
	}

	for res := range p.bcastGroup(args) {
		if res.err != nil {
			p.invalmsghdlr(w, r, res.err.Error())
			return
		}
	}

	nlq, err := query.NewQueryListener(handle, &smap.Smap, msg)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	nlq.SetHrwOwner(&smap.Smap)
	p.ic.registerEqual(regIC{nl: nlq, smap: smap, msg: msg})

	w.Write([]byte(handle))
}

func (p *proxyrunner) httpqueryget(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.URLPathQuery)
	if err != nil {
		return
	}

	switch apiItems[0] {
	case cmn.Next:
		p.httpquerygetnext(w, r)
	case cmn.WorkerOwner:
		p.httpquerygetworkertarget(w, r)
	default:
		p.invalmsghdlrf(w, r, "unknown path /%s/%s/%s", cmn.Version, cmn.Query, apiItems[0])
	}
}

// /v1/query/worker
// TODO: change an endpoint and the name
func (p *proxyrunner) httpquerygetworkertarget(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	msg := &query.NextMsg{}
	if err := cmn.ReadJSON(w, r, msg); err != nil {
		return
	}
	if msg.Handle == "" {
		p.invalmsghdlr(w, r, "handle cannot be empty", http.StatusBadRequest)
		return
	}
	if p.ic.reverseToOwner(w, r, msg.Handle, msg) {
		return
	}
	nl, ok := p.ic.checkEntry(w, r, msg.Handle)
	if !ok {
		return
	}
	state := nl.(*query.NotifListenerQuery)
	if err := state.Err(false); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	target, err := state.WorkersTarget(msg.WorkerID)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	url := p.redirectURL(r, target, started, cmn.NetworkIntraControl)
	http.Redirect(w, r, url, http.StatusTemporaryRedirect)
}

func (p *proxyrunner) httpquerygetnext(w http.ResponseWriter, r *http.Request) {
	// get next query
	if _, err := p.checkRESTItems(w, r, 0, false, cmn.URLPathQueryNext); err != nil {
		return
	}

	msg := &query.NextMsg{}
	if err := cmn.ReadJSON(w, r, msg); err != nil {
		return
	}
	if msg.Handle == "" {
		p.invalmsghdlr(w, r, "handle cannot be empty", http.StatusBadRequest)
		return
	}

	if p.ic.reverseToOwner(w, r, msg.Handle, msg) {
		return
	}

	if _, ok := p.ic.checkEntry(w, r, msg.Handle); !ok {
		return
	}

	var (
		results = p.bcastGroup(bcastArgs{
			req: cmn.ReqArgs{
				Method: http.MethodGet,
				Path:   cmn.JoinWords(cmn.Version, cmn.Query, cmn.Peek),
				Body:   cmn.MustMarshal(msg),
				Header: map[string][]string{cmn.HeaderAccept: {cmn.ContentMsgPack}},
			},
			timeout: cmn.LongTimeout,
			fv:      func() interface{} { return &cmn.BucketList{} },
		})
		lists = make([]*cmn.BucketList, 0, len(results))
	)
	for res := range results {
		if res.err != nil {
			if res.status == http.StatusNotFound {
				continue
			}
			p.invalmsghdlr(w, r, res.err.Error(), res.status)
			return
		}
		lists = append(lists, res.v.(*cmn.BucketList))
	}

	result := cmn.ConcatObjLists(lists, msg.Size)
	if len(result.Entries) == 0 {
		// TODO: Maybe we should just return empty response and `http.StatusNoContent`?
		p.invalmsghdlrstatusf(w, r, http.StatusGone, "%q finished", msg.Handle)
		return
	}

	if len(result.Entries) > 0 {
		last := result.Entries[len(result.Entries)-1]
		discardResults := p.bcastGroup(bcastArgs{
			req: cmn.ReqArgs{
				Method: http.MethodPut,
				Path:   cmn.JoinWords(cmn.Version, cmn.Query, cmn.Discard, msg.Handle, last.Name),
			},
			to: cluster.Targets,
		})
		for res := range discardResults {
			if res.err != nil && res.status != http.StatusNotFound {
				p.invalmsghdlr(w, r, res.err.Error())
				return
			}
		}
	}
	p.writeJSON(w, r, result.Entries, "query_objects")
}
