// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

///////////////////////////
// notification receiver //
// see also cmn/notif.go //
///////////////////////////

type (
	notifListener interface {
		callback(n notifListener, msg interface{}, uuid string)
		lock()
		unlock()
		notifiers() cluster.NodeMap
		incRC() int
	}
	notifListenerBase struct {
		sync.Mutex
		srcs cluster.NodeMap                                     // expected notifiers
		f    func(n notifListener, msg interface{}, uuid string) // callback on the notification-receiving side
		rc   int
	}
	notifListenerBckCp struct {
		notifListenerBase
		nlpFrom, nlpTo *cluster.NameLockPair
	}
)

// interface guard
var (
	_ notifListener = &notifListenerBase{}
)

func (nl *notifListenerBase) callback(n notifListener, msg interface{}, uuid string) {
	nl.f(n, msg, uuid)
}
func (nl *notifListenerBase) lock()                      { nl.Lock() }
func (nl *notifListenerBase) unlock()                    { nl.Unlock() }
func (nl *notifListenerBase) notifiers() cluster.NodeMap { return nl.srcs }
func (nl *notifListenerBase) incRC() int                 { nl.rc++; return nl.rc }

///////////////////////////
// notification messages //
///////////////////////////

type (
	// compare w/ cmn.XactReqMsg
	xactPushMsg struct {
		Stats cmn.BaseXactStatsExt `json:"stats"` // NOTE: struct to unmarshal from the interface (see below)
		Snode *cluster.Snode       `json:"snode"`
		Err   error                `json:"err"`
	}
	xactPushMsgTgt struct {
		Stats cmn.XactStats  `json:"stats"` // interface to marshal
		Snode *cluster.Snode `json:"snode"`
		Err   error          `json:"err"`
	}
)

// verb /v1/xactions
func (p *proxyrunner) notifHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /notifs path")
		return
	}
	if _, err := p.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Notifs); err != nil {
		return
	}
	xactMsg := xactPushMsg{} // TODO: add more notification types
	if cmn.ReadJSON(w, r, &xactMsg) != nil {
		return
	}
	p.notifs.RLock()
	var (
		uuid   = xactMsg.Stats.IDX                // xaction UUID
		tid    = r.Header.Get(cmn.HeaderCallerID) // sender node ID
		nl, ok = p.notifs.m[xactMsg.Stats.IDX]    // receive-side notification context
	)
	p.notifs.RUnlock()
	if !ok {
		s := fmt.Sprintf("%s: notification from %s: unknown xaction %q", p.si, tid, uuid)
		p.invalmsghdlr(w, r, s, http.StatusNotFound)
		return
	}
	nl.lock()
	err, status := p.handleXactNotif(nl, &xactMsg, uuid, tid)
	nl.unlock()

	if err != nil {
		p.invalmsghdlr(w, r, err.Error(), status)
	}
}

// TODO: support timeout
func (p *proxyrunner) handleXactNotif(nl notifListener, xactMsg *xactPushMsg, uuid, tid string) (err error, status int) {
	srcs := nl.notifiers()
	tsi, ok := srcs[tid]
	if !ok {
		return fmt.Errorf("%s: notification from unknown node %s, xaction %q", p.si, tid, uuid),
			http.StatusNotFound
	}
	if tsi == nil {
		err = fmt.Errorf("%s: duplicate notification: xaction %q, target %s", p.si, uuid, tid)
		return
	}
	srcs[tid] = nil
	if rc := nl.incRC(); rc >= len(srcs) {
		nl.callback(nl, xactMsg, uuid)
		p.notifs.Lock()
		delete(p.notifs.m, uuid)
		p.notifs.Unlock()
	}
	return
}
