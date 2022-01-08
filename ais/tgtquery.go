// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/xreg"
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
		cmn.WriteErr405(w, r, http.MethodGet, http.MethodPost, http.MethodPut)
	}
}

func (t *targetrunner) httpquerypost(w http.ResponseWriter, r *http.Request) {
	var (
		handle = r.Header.Get(cmn.HdrHandle) // TODO: should it be from header or from body?
		msg    = &query.InitMsg{}
	)
	if _, err := t.checkRESTItems(w, r, 0, false, cmn.URLPathQueryInit.L); err != nil {
		return
	}
	if err := cmn.ReadJSON(w, r, msg); err != nil {
		return
	}
	q, err := query.NewQueryFromMsg(t, &msg.QueryMsg)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	var (
		ctx = context.Background()
		// TODO: we should use `q` directly instead of passing everything in
		//  additional, redundant `ListObjsMsg`.
		lsmsg = &cmn.ListObjsMsg{
			UUID:   handle,
			Prefix: q.ObjectsSource.Prefix,
			Props:  q.Select.Props,
		}
	)
	if q.Cached {
		lsmsg.Flags = cmn.LsPresent
	}
	rns := xreg.RenewQueryObjects(ctx, t, q, lsmsg)
	if rns.Err != nil {
		t.writeErr(w, r, rns.Err)
		return
	}
	if rns.IsRunning() {
		return
	}
	xact := rns.Entry.Get()
	xact.AddNotif(&xaction.NotifXact{
		NotifBase: nl.NotifBase{When: cluster.UponTerm, Dsts: []string{equalIC}, F: t.callerNotifyFin},
		Xact:      xact,
	})
	go xact.Run(nil)
}

func (t *targetrunner) httpqueryget(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.URLPathQuery.L)
	if err != nil {
		return
	}
	switch apiItems[0] {
	case cmn.Next, cmn.Peek:
		t.httpquerygetobjects(w, r)
	case cmn.WorkerOwner:
		t.httpquerygetworkertarget(w, r)
	default:
		t.writeErrURL(w, r)
	}
}

// /v1/query/worker
// TODO: change an endpoint and use the logic when #833 is done
func (t *targetrunner) httpquerygetworkertarget(w http.ResponseWriter, _ *http.Request) {
	w.Write([]byte(t.si.DaemonID))
}

func (t *targetrunner) httpquerygetobjects(w http.ResponseWriter, r *http.Request) {
	var (
		entries []*cmn.BucketEntry
		msg     = &query.NextMsg{}
	)
	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.URLPathQuery.L)
	if err != nil {
		return
	}
	if err := cmn.ReadJSON(w, r, msg); err != nil {
		return
	}
	if msg.Handle == "" {
		t.writeErr(w, r, errQueryHandle)
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
		t.writeErrf(w, r, "invalid %s/%s/%s", cmn.Version, cmn.Query, apiItems[0])
		return
	}
	if err != nil && err != io.EOF {
		t.writeErr(w, r, err, http.StatusInternalServerError)
		return
	}
	objList := &cmn.BucketList{Entries: entries}
	if strings.Contains(r.Header.Get(cmn.HdrAccept), cmn.ContentMsgPack) {
		t.writeMsgPack(w, r, objList, "query_objects")
		return
	}
	t.writeJSON(w, r, objList, "query_objects")
}

// v1/query/discard/handle/value
func (t *targetrunner) httpqueryput(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 2, false, cmn.URLPathQueryDiscard.L)
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
	t.writeErrSilentf(w, r, http.StatusNotFound, "%s: handle %q not found", t.Sname(), handle)
}
