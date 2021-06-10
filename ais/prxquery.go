// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/query"
)

var errQueryHandle = errors.New("handle cannot be empty")

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
		cmn.WriteErr405(w, r, http.MethodGet, http.MethodPost)
	}
}

func (p *proxyrunner) httpquerypost(w http.ResponseWriter, r *http.Request) {
	if _, err := p.checkRESTItems(w, r, 0, false, cmn.URLPathQueryInit.L); err != nil {
		return
	}
	// A target will return error if given handle already exists (though is very unlikely).
	handle := cos.GenUUID()
	header := http.Header{cmn.HdrHandle: []string{handle}}
	msg := &query.InitMsg{}
	if err := cmn.ReadJSON(w, r, msg); err != nil {
		return
	}
	if _, err := query.NewQueryFromMsg(p, &msg.QueryMsg); err != nil {
		p.writeErrMsg(w, r, "failed to parse query message: "+err.Error())
		return
	}
	smap := p.owner.smap.get()
	args := allocBcastArgs()
	args.req = cmn.ReqArgs{
		Method: http.MethodPost,
		Path:   cmn.URLPathQueryInit.S,
		Body:   cos.MustMarshal(msg),
		Header: header,
	}
	args.timeout = cmn.DefaultTimeout
	args.smap = smap
	results := p.bcastGroup(args)
	freeBcastArgs(args)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		p.writeErr(w, r, res.error())
		freeCallResults(results)
		return
	}
	freeCallResults(results)

	nlq, err := query.NewQueryListener(handle, &smap.Smap, msg)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	nlq.SetHrwOwner(&smap.Smap)
	p.ic.registerEqual(regIC{nl: nlq, smap: smap, msg: msg})

	w.Write([]byte(handle))
}

func (p *proxyrunner) httpqueryget(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.URLPathQuery.L)
	if err != nil {
		return
	}
	switch apiItems[0] {
	case cmn.Next:
		p.httpquerygetnext(w, r)
	case cmn.WorkerOwner:
		p.httpquerygetworkertarget(w, r)
	default:
		p.writeErrURL(w, r)
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
		p.writeErr(w, r, errQueryHandle)
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
		p.writeErr(w, r, err)
		return
	}

	target, err := state.WorkersTarget(msg.WorkerID)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	url := p.redirectURL(r, target, started, cmn.NetworkIntraControl)
	http.Redirect(w, r, url, http.StatusTemporaryRedirect)
}

func (p *proxyrunner) httpquerygetnext(w http.ResponseWriter, r *http.Request) {
	// get next query
	if _, err := p.checkRESTItems(w, r, 0, false, cmn.URLPathQueryNext.L); err != nil {
		return
	}
	msg := &query.NextMsg{}
	if err := cmn.ReadJSON(w, r, msg); err != nil {
		return
	}
	if msg.Handle == "" {
		p.writeErr(w, r, errQueryHandle)
		return
	}
	if p.ic.reverseToOwner(w, r, msg.Handle, msg) {
		return
	}
	if _, ok := p.ic.checkEntry(w, r, msg.Handle); !ok {
		return
	}
	args := allocBcastArgs()
	args.req = cmn.ReqArgs{
		Method: http.MethodGet,
		Path:   cmn.URLPathQueryPeek.S,
		Body:   cos.MustMarshal(msg),
		Header: map[string][]string{cmn.HdrAccept: {cmn.ContentMsgPack}},
	}
	args.timeout = cmn.LongTimeout
	args.fv = func() interface{} { return &cmn.BucketList{} }
	results := p.bcastGroup(args)
	freeBcastArgs(args)
	lists := make([]*cmn.BucketList, 0, len(results))
	for _, res := range results {
		if res.err != nil {
			if res.status == http.StatusNotFound {
				continue
			}
			p.writeErr(w, r, res.error())
			freeCallResults(results)
			return
		}
		lists = append(lists, res.v.(*cmn.BucketList))
	}
	freeCallResults(results)

	result := cmn.ConcatObjLists(lists, msg.Size)
	if len(result.Entries) == 0 {
		// TODO: Maybe we should just return empty response and `http.StatusNoContent`?
		p.writeErrStatusf(w, r, http.StatusGone, "%q finished", msg.Handle)
		return
	}

	if len(result.Entries) > 0 {
		last := result.Entries[len(result.Entries)-1]
		discardArgs := allocBcastArgs()
		discardArgs.req = cmn.ReqArgs{
			Method: http.MethodPut,
			Path:   cmn.URLPathQueryDiscard.Join(msg.Handle, last.Name),
		}
		discardArgs.to = cluster.Targets
		discardResults := p.bcastGroup(discardArgs)
		freeBcastArgs(discardArgs)
		for _, res := range discardResults {
			if res.err != nil && res.status != http.StatusNotFound {
				p.writeErr(w, r, res.error())
				return
			}
		}
	}
	p.writeJSON(w, r, result.Entries, "query_objects")
}
