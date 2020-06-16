// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

///////////////////////////
// notification receiver //
// see also cmn/notif.go //
///////////////////////////

type (
	notifListener interface {
		Callback(n notifListener)
	}
	notifListenerBase struct {
		Srcs []cluster.NodeMap     // expected notifiers
		F    func(n notifListener) // callback on the notification-receiving side
	}
	// xaction notification - receiver's side (for sender's, see cmn/xaction.go)
	notifListenerXact struct {
		notifListenerBase
		msg *xactPushMsg
	}
)

// interface guard
var (
	_ notifListener = &notifListenerBase{}
)

func (nl *notifListenerBase) Callback(n notifListener) { nl.F(n) }

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
	var (
		xactMsg xactPushMsg // TODO: more notification types
	)
	if r.Method != http.MethodPost {
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /notifs path")
		return
	}
	if _, err := p.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Notifs); err != nil {
		return
	}
	if cmn.ReadJSON(w, r, &xactMsg) != nil {
		return
	}
	if nl, ok := p.notifs[xactMsg.Stats.IDX]; ok {
		notifXact, ok := nl.(*notifListenerXact)
		cmn.Assert(ok)
		notifXact.msg = &xactMsg
		nl.Callback(notifXact)
		return
	}
	s := fmt.Sprintf("%s: cannot process notification from %s: unknown xaction %q",
		p.si, r.Header.Get(cmn.HeaderCallerName), xactMsg.Stats.IDX)
	p.invalmsghdlr(w, r, s, http.StatusNotFound)
}
