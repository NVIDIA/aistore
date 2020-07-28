// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/query"
	jsoniter "github.com/json-iterator/go"
)

type (
	// TODO: add more, like target finished, query aborted by user etc.
	queryState struct {
		notifListenerBase
		targets    []*cluster.Snode
		workersCnt uint
	}
)

func newQueryState(smap *cluster.Smap, msg *query.InitMsg) (*queryState, error) {
	if msg.WorkersCnt != 0 && msg.WorkersCnt < uint(smap.CountTargets()) {
		// FIXME: this should not be necessary. Proxy could know that if worker's
		//  target is done, worker should be redirected to the next not-done target.
		//  However, it should be done once query is able to keep more detailed
		//  information about targets.
		return nil, fmt.Errorf("expected workersCnt to be at least %d", smap.CountTargets())
	}
	targets := make([]*cluster.Snode, 0, smap.CountTargets())
	for _, t := range smap.Tmap {
		targets = append(targets, t)
	}
	return &queryState{
		notifListenerBase: *newNLB(smap.Tmap, cmn.ActQueryObjects, msg.QueryMsg.From.Bck),
		workersCnt:        msg.WorkersCnt,
		targets:           targets,
	}, nil
}

func (q *queryState) workersTarget(workerID uint) (*cluster.Snode, error) {
	if q.workersCnt == 0 {
		return nil, errors.New("query registered with 0 workers")
	}
	if workerID == 0 {
		return nil, errors.New("workerID cannot be empty")
	}
	return q.targets[workerID%uint(len(q.targets))], nil
}

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
	smap := p.owner.smap.get()

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

	if _, err := query.NewQueryFromMsg(p, &msg.QueryMsg); err != nil {
		p.invalmsghdlr(w, r, "Failed to parse query message: "+err.Error())
		return
	}

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

	state, err := newQueryState(&smap.Smap, msg)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	p.registerIC(handle, state, smap, msg)

	w.Write([]byte(handle))
}

func (p *proxyrunner) httpqueryget(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Query)
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
	if redirected := p.redirectToOwner(w, r, msg.Handle, msg); redirected {
		return
	}
	nl, ok := p.checkEntry(w, r, msg.Handle)
	if !ok {
		return
	}
	state := nl.(*queryState)
	if err := state.err(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	target, err := state.workersTarget(msg.WorkerID)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	url := p.redirectURL(r, target, started, cmn.NetworkIntraControl)
	http.Redirect(w, r, url, http.StatusTemporaryRedirect)
}

func (p *proxyrunner) httpquerygetnext(w http.ResponseWriter, r *http.Request) {
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

	if redirected := p.redirectToOwner(w, r, msg.Handle, msg); redirected {
		return
	}
	if _, ok := p.checkEntry(w, r, msg.Handle); !ok {
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
