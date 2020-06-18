// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/housekeep/hk"
)

///////////////////////////
// notification receiver //
// see also cmn/notif.go //
///////////////////////////

const (
	notifsName      = ".notifications.prx"
	notifsTimeoutGC = 2 * time.Minute
)

type (
	notifs struct {
		sync.RWMutex
		p *proxyrunner
		m map[string]notifListener // table [UUID => notifListener]
	}
	notifListener interface {
		callback(n notifListener, msg interface{}, uuid string, err error)
		lock()
		unlock()
		rlock()
		runlock()
		notifiers() cluster.NodeMap
		incRC() int
		sinceLast() int64
	}
	notifListenerBase struct {
		sync.RWMutex
		srcs cluster.NodeMap                                                // expected notifiers
		f    func(n notifListener, msg interface{}, uuid string, err error) // callback
		time struct {
			call     int64 // timestamp of the last callback
			progress int64 // last successful progress check
		}
		rc int
	}
	notifListenerBckCp struct {
		notifListenerBase
		nlpFrom, nlpTo *cluster.NameLockPair
	}
	//
	// notification messages - compare w/ cmn.XactReqMsg
	//
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

// interface guard
var (
	_ notifListener = &notifListenerBase{}
)

///////////////////////
// notifListenerBase //
///////////////////////

func (nl *notifListenerBase) callback(n notifListener, msg interface{}, uuid string, err error) {
	nl.time.call = mono.NanoTime()
	nl.f(n, msg, uuid, err)
}
func (nl *notifListenerBase) lock()                      { nl.Lock() }
func (nl *notifListenerBase) unlock()                    { nl.Unlock() }
func (nl *notifListenerBase) rlock()                     { nl.RLock() }
func (nl *notifListenerBase) runlock()                   { nl.RUnlock() }
func (nl *notifListenerBase) notifiers() cluster.NodeMap { return nl.srcs }
func (nl *notifListenerBase) incRC() int                 { nl.rc++; return nl.rc }
func (nl *notifListenerBase) sinceLast() int64 {
	return mono.NanoTime() - cmn.MaxI64(nl.time.call, nl.time.progress)
}

////////////
// notifs //
////////////

func (n *notifs) init(p *proxyrunner) {
	n.p = p
	n.m = make(map[string]notifListener, 8)
	hk.Housekeeper.Register(notifsName+".gc", n.housekeep, notifsTimeoutGC)
	p.owner.smap.Listeners().Reg(n)
}

func (n *notifs) String() string { return notifsName }

func (n *notifs) ListenSmapChanged(newSmapVersionChannel chan int64) {
	for {
		<-newSmapVersionChannel // FIXME #804

		if !n.p.ClusterStarted() {
			continue
		}
		if len(n.m) == 0 {
			continue
		}
		var (
			removed = make(cmn.SimpleKVs)
			smap    = n.p.owner.smap.get()
		)
		n.RLock()
		for uuid, nl := range n.m {
			nl.rlock()
			srcs := nl.notifiers()
			for id, si := range srcs {
				if si == nil {
					continue
				}
				if smap.GetNode(id) == nil {
					removed[uuid] = id
					break
				}
			}
			nl.runlock()
		}
		if len(removed) == 0 {
			n.RUnlock()
			continue
		}
		for uuid, id := range removed {
			s := fmt.Sprintf("%s: stop waiting for %q notifications", n.p.si, uuid)
			err := &errNodeNotFound{s, id, n.p.si, smap}
			nl, ok := n.m[uuid]
			if !ok {
				continue
			}
			nl.lock()
			nl.callback(nl, nil /*msg*/, uuid, err)
			nl.unlock()
		}
		n.RUnlock()
		n.Lock()
		for uuid := range removed {
			delete(n.m, uuid)
		}
		n.Unlock()
	}
}

// verb /v1/notifs
// TODO: extend to handle other than `xactPushMsg` messages
// TODO: extend to handle notifications from proxies
func (n *notifs) handler(w http.ResponseWriter, r *http.Request) {
	var (
		msg  interface{}
		uuid string
		tid  = r.Header.Get(cmn.HeaderCallerID) // sender node ID
	)
	if r.Method != http.MethodPost {
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /notifs path")
		return
	}
	if _, err := n.p.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Notifs); err != nil {
		return
	}

	// BEGIN xacton-specific part of the notification-handling code ==================
	xactMsg := xactPushMsg{}
	if cmn.ReadJSON(w, r, &xactMsg) != nil {
		return
	}
	uuid = xactMsg.Stats.IDX
	msg = &xactMsg
	// END xacton-specific part =======================================================

	n.RLock()
	nl, ok := n.m[uuid]
	n.RUnlock()
	if !ok {
		n.p.invalmsghdlrstatusf(w, r, http.StatusNotFound, "%s: notification from %s: unknown UUID %q (%+v)", n.p.si, tid, uuid, msg)
		return
	}
	nl.lock()
	err, status, done := n.handleMsg(nl, msg, uuid, tid)
	nl.unlock()
	if done {
		n.Lock()
		delete(n.m, uuid)
		n.Unlock()
	}
	if err != nil {
		n.p.invalmsghdlr(w, r, err.Error(), status)
	}
}

func (n *notifs) handleMsg(nl notifListener, msg interface{}, uuid, tid string) (err error, status int, done bool) {
	srcs := nl.notifiers()
	tsi, ok := srcs[tid]
	if !ok {
		err = fmt.Errorf("%s: notification from unknown node %s, xaction %q", n.p.si, tid, uuid)
		status = http.StatusNotFound
		return
	}
	if tsi == nil {
		err = fmt.Errorf("%s: duplicate notification from target %s, xaction %q", n.p.si, tid, uuid)
		return
	}
	srcs[tid] = nil
	if rc := nl.incRC(); rc >= len(srcs) {
		nl.callback(nl, msg, uuid, nil)
		done = true
	}
	return
}

func (n *notifs) housekeep() time.Duration {
	if len(n.m) == 0 {
		return notifsTimeoutGC
	}
	var pending = make([]string, 0) // UUIDs
	n.RLock()
	for uuid, nl := range n.m {
		if nl.sinceLast() < int64(notifsTimeoutGC) {
			continue
		}
		// TODO -- FIXME: find out whether xaction is making progress
		pending = append(pending, uuid)
	}
	n.RUnlock()
	if len(pending) == 0 {
		return notifsTimeoutGC
	}
	// TODO -- FIXME: handle pending here
	return 0
}
