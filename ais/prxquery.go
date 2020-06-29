// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/query"
	jsoniter "github.com/json-iterator/go"
)

// Proxy exposes 2 methods:
// - Init(query) -> handle - initializes a query on proxy and targets
// - Next(handle, n) - returns next n objects from query registered by handle.
// Objects are returned in sorted order.

func (p *proxyrunner) queryHandler(w http.ResponseWriter, r *http.Request) {
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
	if _, err := p.checkRESTItems(w, r, 0, false, cmn.Version, cmn.Query, cmn.Init); err != nil {
		return
	}

	// A target will return error if given handle already exists (though is very unlikely).
	handle := cmn.GenUUID()
	header := http.Header{cmn.HeaderHandle: []string{handle}}
	msg := &query.InitMsg{}
	if err := cmn.ReadJSON(w, r, msg); err != nil {
		return
	}

	if _, err := query.NewQueryFromMsg(&msg.QueryMsg); err != nil {
		p.invalmsghdlr(w, r, "Failed to parse query message: "+err.Error())
		return
	}

	smap := p.owner.smap.get()
	args := bcastArgs{
		req: cmn.ReqArgs{
			Path:   cmn.URLPath(cmn.Version, cmn.Query, cmn.Init),
			Body:   cmn.MustMarshal(msg),
			Header: header,
		},
		timeout: cmn.DefaultTimeout,
		to:      cluster.Targets,
		smap:    smap,
	}

	for res := range p.bcastPost(args) {
		if res.err != nil {
			p.invalmsghdlr(w, r, res.err.Error())
			return
		}
	}

	p.jtx.addEntry(handle)

	w.Write([]byte(handle))
}

func (p *proxyrunner) httpqueryget(w http.ResponseWriter, r *http.Request) {
	// get next query
	if _, err := p.checkRESTItems(w, r, 0, false, cmn.Version, cmn.Query, cmn.Next); err != nil {
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

	if redirected := p.jtx.redirectToOwner(w, r, msg.Handle, msg); redirected {
		return
	}
	if entry, exists := p.jtx.entry(msg.Handle); !exists {
		p.invalmsghdlrstatusf(w, r, http.StatusNotFound, "%q not found", msg.Handle)
		return
	} else if entry.finished() {
		// TODO: Maybe we should just return empty response and `http.StatusNoContent`?
		p.invalmsghdlrstatusf(w, r, http.StatusGone, "%q finished", msg.Handle)
		return
	}

	var (
		lists        = make([]*cmn.BucketList, 0, p.owner.smap.Get().CountTargets())
		bcastResults = p.callTargets(http.MethodGet, cmn.URLPath(cmn.Version, cmn.Query, cmn.Peek), cmn.MustMarshal(msg))
	)

	for res := range bcastResults {
		if res.err != nil {
			if res.status == http.StatusNotFound {
				continue
			}
			p.invalmsghdlr(w, r, res.err.Error(), res.status)
			return
		}
		list := &cmn.BucketList{}
		if err := jsoniter.Unmarshal(res.outjson, list); err != nil {
			p.invalmsghdlr(w, r, "failed to unmarshal target query response", http.StatusInternalServerError)
			return
		}

		lists = append(lists, list)
	}

	result := cmn.ConcatObjLists(lists, msg.Size)
	if len(result.Entries) == 0 {
		// TODO: Maybe we should just return empty response and `http.StatusNoContent`?
		p.invalmsghdlrstatusf(w, r, http.StatusGone, "%q finished", msg.Handle)
		return
	}

	if len(result.Entries) > 0 {
		last := result.Entries[len(result.Entries)-1]
		discardResults := p.callTargets(http.MethodPut, cmn.URLPath(cmn.Version, cmn.Query, cmn.Discard, msg.Handle, last.Name), nil)

		for res := range discardResults {
			if res.err != nil && res.status != http.StatusNotFound {
				p.invalmsghdlr(w, r, res.err.Error())
				return
			}
		}
	}
	w.Write(cmn.MustMarshal(result.Entries))
}
