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
// TODO: extend to handle notifications from proxies

// notifMsg.Ty enum
const (
	notifXact = iota
	// TODO: add more
)

var notifTyText = map[int]string{
	notifXact: "xaction-notification",
}

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
		inHk    atomic.Bool
	}
	notifListener interface {
		callback(n notifListener, msg interface{}, uuid string, err error)
		lock()
		unlock()
		rlock()
		runlock()
		notifiers() cluster.NodeMap
		incRC() int
		notifTy() int
	}
	notifListenerBase struct {
		sync.RWMutex
		srcs cluster.NodeMap // expected notifiers
		f    notifCallback   // the callback
		rc   int             // refcount
		ty   int             // notifMsg.Ty enum (above)
		done atomic.Bool     // true when done
	}
	notifListenerFromTo struct {
		notifListenerBase
		nlpFrom, nlpTo *cluster.NameLockPair
	}
	//
	// notification messages
	//
	notifMsg struct {
		Ty    int32          `json:"type"`    // enumerated type, one of (notifXact, et al.) - see above
		Flags int32          `json:"flags"`   // TODO: add
		Snode *cluster.Snode `json:"snode"`   // node
		Data  []byte         `json:"message"` // typed message
		Err   error          `json:"err"`     // error
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

// is called after all notifiers will have notified OR on failure (err != nil)
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
func (nl *notifListenerBase) notifTy() int               { return nl.ty }

func (msg *notifMsg) String() string {
	text := "unknown-notification"
	if ty, ok := notifTyText[int(msg.Ty)]; ok {
		text = ty
	}
	return fmt.Sprintf("%s[%s,%v]<=%s", text, string(msg.Data), msg.Err, msg.Snode)
}

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
func (n *notifs) handler(w http.ResponseWriter, r *http.Request) {
	var (
		notifMsg = &notifMsg{}
		msg      interface{}
		uuid     string
		tid      = r.Header.Get(cmn.HeaderCallerID) // sender node ID
	)
	if r.Method != http.MethodPost {
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /notifs path")
		return
	}
	if _, err := n.p.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Notifs); err != nil {
		return
	}
	if cmn.ReadJSON(w, r, notifMsg) != nil {
		return
	}
	switch notifMsg.Ty {
	case notifXact:
		stats := &cmn.BaseXactStatsExt{}
		if eru := jsoniter.Unmarshal(notifMsg.Data, stats); eru != nil {
			n.p.invalmsghdlrstatusf(w, r, 0, "%s: failed to unmarshal %s: %v", n.p.si, notifMsg, eru)
			return
		}
		uuid = stats.IDX
		msg = stats
	default:
		n.p.invalmsghdlrstatusf(w, r, 0, "%s: unknown type %s", n.p.si, notifMsg)
		return
	}
	n.RLock()
	nl, ok := n.m[uuid]
	n.RUnlock()
	if !ok {
		n.p.invalmsghdlrstatusf(w, r, http.StatusNotFound, "%s: unknown UUID %q (%s)", n.p.si, uuid, notifMsg)
		return
	}
	//
	// notifListener and notifMsg must have the same type
	//
	cmn.Assert(nl.notifTy() == int(notifMsg.Ty))

	nl.lock()
	err, status, done := n.handleMsg(nl, uuid, tid)
	nl.unlock()
	if done {
		nl.callback(nl, msg, uuid, nil)
		n.del(uuid)
	}
	if err != nil {
		n.p.invalmsghdlr(w, r, err.Error(), status)
	}
}

// is called under notifListener.lock()
func (n *notifs) handleMsg(nl notifListener, uuid, tid string) (err error, status int, done bool) {
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
		done = true
	}
	return
}

//
// housekeeping
//

func (n *notifs) housekeep() time.Duration {
	if !n.inHk.CAS(false, true) {
		return notifsTimeoutGC
	}
	defer n.inHk.Store(false)
	if len(n.m) == 0 {
		return notifsTimeoutGC
	}
	n.RLock()
	tempn := make(map[string]notifListener, len(n.m))
	for uuid, nl := range n.m {
		tempn[uuid] = nl
	}
	n.RUnlock()
	req := bcastArgs{req: cmn.ReqArgs{Query: make(url.Values, 2)}, timeout: cmn.GCO.Get().Timeout.MaxKeepalive}
	for uuid, nl := range tempn {
		switch nl.notifTy() {
		case notifXact:
			req.req.Path = cmn.URLPath(cmn.Version, cmn.Xactions)
			req.req.Query.Set(cmn.URLParamWhat, cmn.GetWhatXactStats)
			req.req.Query.Set(cmn.URLParamUUID, uuid)
		default:
			cmn.Assert(false)
		}
		results := n.p.bcastGet(req)
		for res := range results {
			var (
				msg  interface{}
				err  error
				done bool
			)
			if res.err == nil {
				switch nl.notifTy() {
				case notifXact:
					stats := &cmn.BaseXactStatsExt{}
					if eru := jsoniter.Unmarshal(res.outjson, stats); eru == nil {
						msg = stats
						if stats.Finished() {
							nl.lock()
							_, _, done = n.handleMsg(nl, uuid, res.si.ID())
							nl.unlock()
						}
					} else {
						glog.Errorf("%s: unexpected: failed to unmarshal %q from %s, err: %v",
							n.p.si, uuid, res.si, eru)
					}
				default:
					cmn.Assert(false)
				}
			} else if res.status == http.StatusNotFound { // compare w/ errNodeNotFound below
				err = fmt.Errorf("%s: %q not found at %s", n.p.si, uuid, res.si)
				done = true
			} else if glog.FastV(4, glog.SmoduleAIS) {
				glog.Errorf("%s: failure to get %q from %s: %v", n.p.si, uuid, res.si, res.err)
			}
			if done {
				nl.callback(nl, msg, uuid, err)
				n.del(uuid)
				break
			}
		}
	}
	// cleanup temp cloned notifs
	for u := range tempn {
		delete(tempn, u)
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
		s := fmt.Sprintf("%s: stop waiting for notifications from %q", n.p.si, uuid)
		err := &errNodeNotFound{s, remid[uuid], n.p.si, smap}
		nl.callback(nl, nil /*msg*/, uuid, err)
	}
	n.Lock()
	for uuid := range remnl {
		delete(n.m, uuid)
		// cleanup
		delete(remnl, uuid)
		delete(remid, uuid)
	}
	unreg := len(n.m) == 0
	n.Unlock()
	if unreg {
		go n.p.owner.smap.Listeners().Unreg(n) // cannot unreg from the same goroutine
	}
}
