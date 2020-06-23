// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/housekeep/hk"
	jsoniter "github.com/json-iterator/go"
)

///////////////////////////
// notification receiver //
// see also cmn/notif.go //
///////////////////////////

// TODO: cmn.UponProgress as periodic (byte-count, object-count)
// TODO: batch housekeeping for pending notifications

const (
	notifsName      = ".notifications.prx"
	notifsTimeoutGC = 2 * time.Minute
)

type (
	notifCallback func(n notifListener, msg interface{}, uuid string, err error)
	notifs        struct {
		sync.RWMutex
		p       *proxyrunner
		m       map[string]notifListener // table [UUID => notifListener]
		smapVer int64
	}
	notifListener interface {
		callback(n notifListener, msg interface{}, uuid string, err error)
		lock()
		unlock()
		rlock()
		runlock()
		notifiers() cluster.NodeMap
		incRC() int
	}
	notifListenerBase struct {
		sync.RWMutex
		srcs cluster.NodeMap // expected notifiers
		f    notifCallback   // callback
		rc   int             // refcount
		done atomic.Bool
	}
	notifListenerFromTo struct {
		notifListenerBase
		nlpFrom, nlpTo *cluster.NameLockPair
	}
	//
	// notification messages - compare w/ cmn.XactReqMsg
	//
	xactPushMsg struct {
		Stats cmn.BaseXactStatsExt `json:"stats"` // NOTE: struct to unmarshal from the interface (below)
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
	_ notifListener     = &notifListenerBase{}
	_ cluster.Slistener = &notifs{}
)

///////////////////////
// notifListenerBase //
///////////////////////

func (nl *notifListenerBase) callback(n notifListener, msg interface{}, uuid string, err error) {
	if nl.done.CAS(false, true) {
		nl.f(n, msg, uuid, err)
	}
}
func (nl *notifListenerBase) lock()                      { nl.Lock() }
func (nl *notifListenerBase) unlock()                    { nl.Unlock() }
func (nl *notifListenerBase) rlock()                     { nl.RLock() }
func (nl *notifListenerBase) runlock()                   { nl.RUnlock() }
func (nl *notifListenerBase) notifiers() cluster.NodeMap { return nl.srcs }
func (nl *notifListenerBase) incRC() int                 { nl.rc++; return nl.rc }

////////////
// notifs //
////////////

func (n *notifs) init(p *proxyrunner) {
	n.p = p
	n.m = make(map[string]notifListener, 8)
	hk.Housekeeper.Register(notifsName+".gc", n.housekeep, notifsTimeoutGC)
}

func (n *notifs) String() string { return notifsName }

func (n *notifs) add(uuid string, nl notifListener) {
	n.Lock()
	n.m[uuid] = nl
	if len(n.m) == 1 {
		n.smapVer = n.p.owner.smap.get().Version
		n.p.owner.smap.Listeners().Reg(n)
	}
	n.Unlock()
}

func (n *notifs) del(uuid string) {
	n.Lock()
	delete(n.m, uuid)
	unreg := len(n.m) == 0
	n.Unlock()
	if unreg {
		n.p.owner.smap.Listeners().Unreg(n)
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

	// BEGIN xacton-specific ==================
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
		n.del(uuid)
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

//
// housekeeping
//

func (n *notifs) housekeep() time.Duration {
	if len(n.m) == 0 {
		return notifsTimeoutGC
	}
	n.RLock()
	clone := make(map[string]notifListener, len(n.m))
	for uuid, nl := range n.m {
		clone[uuid] = nl
	}
	n.RUnlock()
	q := make(url.Values)
	q.Set(cmn.URLParamWhat, cmn.GetWhatXactStats)
	req := bcastArgs{
		req:     cmn.ReqArgs{Path: cmn.URLPath(cmn.Version, cmn.Xactions), Query: q},
		timeout: cmn.GCO.Get().Timeout.MaxKeepalive,
	}
	for uuid, nl := range clone {
		// BEGIN xacton-specific part ==================
		q.Set(cmn.URLParamUUID, uuid)
		results := n.p.bcastGet(req)
		for res := range results {
			var (
				msg  = &xactPushMsg{}
				done bool
			)
			if res.err == nil {
				if err := jsoniter.Unmarshal(res.outjson, &msg.Stats); err == nil {
					if msg.Stats.Finished() {
						nl.lock()
						_, _, done = n.handleMsg(nl, msg, uuid, res.si.ID())
						nl.unlock()
					}
				} else {
					glog.Errorf("%s: unexpected: failed to unmarshal xaction %q from %s",
						n.p.si, uuid, res.si)
				}
			} else if res.status == http.StatusNotFound { // compare w/ errNodeNotFound below
				nl.callback(nl, nil, uuid, fmt.Errorf("%s: %q not found at %s", n.p.si, uuid, res.si))
				done = true
			}
			// END xacton-specific part =======================================================
			if done {
				n.del(uuid)
				break
			}
		}
	}
	return notifsTimeoutGC
}

func (n *notifs) ListenSmapChanged() {
	if !n.p.ClusterStarted() {
		return
	}
	if len(n.m) == 0 {
		return
	}
	smap := n.p.owner.smap.get()
	if n.smapVer >= smap.Version {
		return
	}
	n.smapVer = smap.Version
	remnl := make(map[string]notifListener)
	remid := make(cmn.SimpleKVs)
	n.RLock()
	for uuid, nl := range n.m {
		nl.rlock()
		srcs := nl.notifiers()
		for id, si := range srcs {
			if si == nil {
				continue
			}
			if smap.GetNode(id) == nil {
				remnl[uuid] = nl
				remid[uuid] = id
				break
			}
		}
		nl.runlock()
	}
	n.RUnlock()
	if len(remnl) == 0 {
		return
	}
	for uuid, nl := range remnl {
		s := fmt.Sprintf("%s: stop waiting for %q notifications", n.p.si, uuid)
		err := &errNodeNotFound{s, remid[uuid], n.p.si, smap}
		nl.callback(nl, nil /*msg*/, uuid, err)
	}
	n.Lock()
	for uuid := range remnl {
		delete(n.m, uuid)
	}
	unreg := len(n.m) == 0
	n.Unlock()
	if unreg {
		go n.p.owner.smap.Listeners().Unreg(n) // cannot unreg from the same goroutine
	}
}
