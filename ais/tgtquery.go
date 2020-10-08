// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"io"
	"net/http"
	"strings"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/query"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/registry"
)

// There are 3 methods exposed by targets:
// * Peek(n): get next n objects from a target query, but keep the results in memory.
//   Subsequent Peek(n) request returns the same objects.
// * Discard(n): forget first n elements from a target query.
// * Next(n): Peek(n) + Discard(n)

func (t *targetrunner) queryHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		t.httpqueryget(w, r)
	case http.MethodPost:
		t.httpquerypost(w, r)
	case http.MethodPut:
		t.httpqueryput(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /query path")
	}
}

func (t *targetrunner) httpquerypost(w http.ResponseWriter, r *http.Request) {
	if _, err := t.checkRESTItems(w, r, 0, false, cmn.Version, cmn.Query, cmn.Init); err != nil {
		return
	}

	handle := r.Header.Get(cmn.HeaderHandle) // TODO: should it be from header or from body?
	smap := t.owner.smap.get()
	msg := &query.InitMsg{}
	if err := cmn.ReadJSON(w, r, msg); err != nil {
		return
	}

	q, err := query.NewQueryFromMsg(t, &msg.QueryMsg)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	var (
		ctx = context.Background()
		// TODO: we should use `q` directly instead of passing everything in
		//  additional, redundant `SelectMsg`.
		smsg = &cmn.SelectMsg{
			UUID:   handle,
			Prefix: q.ObjectsSource.Prefix,
			Props:  q.Select.Props,
		}
	)
	if q.Cached {
		smsg.Flags = cmn.SelectCached
	}

	xact, isNew, err := registry.Registry.RenewQuery(ctx, t, q, smsg)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	if !isNew {
		return
	}

	dsts := make([]string, smap.DefaultICSize())
	for pid, psi := range smap.Pmap {
		if psi.IsIC() {
			dsts = append(dsts, pid)
		}
	}
	xact.AddNotif(&xaction.NotifXact{
		NotifBase: nl.NotifBase{When: cluster.UponTerm, Dsts: dsts, F: t.callerNotifyFin},
	})
	go xact.Run()
}

func (t *targetrunner) httpqueryget(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Query)
	if err != nil {
		return
	}

	switch apiItems[0] {
	case cmn.Next, cmn.Peek:
		t.httpquerygetobjects(w, r)
	case cmn.WorkerOwner:
		t.httpquerygetworkertarget(w, r)
	default:
		t.invalmsghdlrf(w, r, "unknown path /%s/%s/%s", cmn.Version, cmn.Query, apiItems[0])
	}
}

// /v1/query/worker
// TODO: change an endpoint and use the logic when #833 is done
func (t *targetrunner) httpquerygetworkertarget(w http.ResponseWriter, _ *http.Request) {
	w.Write([]byte(t.si.DaemonID))
}

func (t *targetrunner) httpquerygetobjects(w http.ResponseWriter, r *http.Request) {
	var entries []*cmn.BucketEntry

	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Query)
	if err != nil {
		return
	}

	msg := &query.NextMsg{}
	if err := cmn.ReadJSON(w, r, msg); err != nil {
		return
	}
	if msg.Handle == "" {
		t.invalmsghdlr(w, r, "handle cannot be empty", http.StatusBadRequest)
		return
	}
	resultSet := query.Registry.Get(msg.Handle)
	if resultSet == nil {
		t.queryDoesntExist(w, r, msg.Handle)
		return
	}

	switch apiItems[0] {
	case cmn.Next:
		entries, err = resultSet.NextN(msg.Size)
	case cmn.Peek:
		entries, err = resultSet.PeekN(msg.Size)
	default:
		t.invalmsghdlrf(w, r, "invalid %s/%s/%s", cmn.Version, cmn.Query, apiItems[0])
		return
	}

	if err != nil && err != io.EOF {
		t.invalmsghdlr(w, r, err.Error(), http.StatusInternalServerError)
		return
	}

	objList := &cmn.BucketList{Entries: entries}
	if strings.Contains(r.Header.Get(cmn.HeaderAccept), cmn.ContentMsgPack) {
		t.writeMsgPack(w, r, objList, "query_objects")
		return
	}
	t.writeJSON(w, r, objList, "query_objects")
}

// v1/query/discard/handle/value
func (t *targetrunner) httpqueryput(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Query, cmn.Discard)
	if err != nil {
		return
	}

	handle, value := apiItems[0], apiItems[1]
	resultSet := query.Registry.Get(handle)
	if resultSet == nil {
		t.queryDoesntExist(w, r, handle)
		return
	}

	resultSet.DiscardUntil(value)
}

func (t *targetrunner) queryDoesntExist(w http.ResponseWriter, r *http.Request, handle string) {
	t.invalmsghdlrsilent(w, r, t.Snode().String()+" handle "+handle+" not found", http.StatusNotFound)
}
