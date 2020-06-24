// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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
// TODO: add an option to enforce 'if one notifier fails all fail'

// notifMsg.Ty enum
const (
	notifXact = iota
	// TODO: add more
)

var notifTyText = map[int]string{
	notifXact: "xaction",
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
		m       map[string]notifListener // [UUID => notifListener]
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
		addErr(string /*sid*/, error)
		UUID() string
		setUUID(string)
		String() string
	}
	notifListenerBase struct {
		sync.RWMutex
		srcs cluster.NodeMap  // expected notifiers
		errs map[string]error // [node-ID => notifMsg.Err]
		uuid string           // UUID
		f    notifCallback    // the callback
		rc   int              // refcount
		ty   int              // notifMsg.Ty enum (above)
		done atomic.Int64     // timestamp when done
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
	if nl.done.CAS(0, 1) {
		nl.f(n, msg, uuid, err)
		nl.done.Store(time.Now().UnixNano())
	}
}
func (nl *notifListenerBase) lock()                      { nl.Lock() }
func (nl *notifListenerBase) unlock()                    { nl.Unlock() }
func (nl *notifListenerBase) rlock()                     { nl.RLock() }
func (nl *notifListenerBase) runlock()                   { nl.RUnlock() }
func (nl *notifListenerBase) notifiers() cluster.NodeMap { return nl.srcs }
func (nl *notifListenerBase) incRC() int                 { nl.rc++; return nl.rc }
func (nl *notifListenerBase) notifTy() int               { return nl.ty }
func (nl *notifListenerBase) UUID() string               { return nl.uuid }
func (nl *notifListenerBase) setUUID(uuid string)        { cmn.Assert(nl.uuid == ""); nl.uuid = uuid }
func (nl *notifListenerBase) addErr(sid string, err error) {
	if nl.errs == nil {
		nl.errs = make(map[string]error, 2)
	}
	nl.errs[sid] = err
}

func (nl *notifListenerBase) String() string {
	var tm, res string
	hdr := fmt.Sprintf("%s-%q", notifText(nl.ty), nl.uuid)
	if unixnano := nl.done.Load(); unixnano > 0 {
		if nl.errs != nil {
			res = fmt.Sprintf("-fail(%+v)", nl.errs)
		}
		tm = cmn.FormatTimestamp(time.Unix(0, unixnano))
		return fmt.Sprintf("%s-%s%s", hdr, tm, res)
	}
	if nl.rc > 0 {
		return fmt.Sprintf("%s-%d/%d", hdr, nl.rc, len(nl.srcs))
	}
	return hdr
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

// start listening
func (n *notifs) add(uuid string, nl notifListener) {
	n.Lock()
	n.m[uuid] = nl
	nl.setUUID(uuid)
	if len(n.m) == 1 {
		n.smapVer = n.p.owner.smap.get().Version
		n.p.owner.smap.Listeners().Reg(n)
	}
	n.Unlock()
	glog.Infoln(nl.String())
}

func (n *notifs) del(nl notifListener, locked ...bool) {
	var unreg bool
	if len(locked) == 0 {
		n.Lock()
	}
	if _, ok := n.m[nl.UUID()]; ok {
		delete(n.m, nl.UUID())
		unreg = len(n.m) == 0
	}
	if len(locked) == 0 {
		n.Unlock()
		if unreg {
			n.p.owner.smap.Listeners().Unreg(n)
		}
	}
	glog.Infoln(nl.String())
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
	err, status, done := n.handleMsg(nl, uuid, tid, notifMsg.Err)
	nl.unlock()
	if done {
		nl.callback(nl, msg, uuid, nil) // well done (compare with nl.callback below)
		n.del(nl)
	}
	if err != nil {
		n.p.invalmsghdlr(w, r, err.Error(), status)
	}
}

// is called under notifListener.lock()
func (n *notifs) handleMsg(nl notifListener, uuid, tid string, srcErr error) (err error, status int, done bool) {
	srcs := nl.notifiers()
	tsi, ok := srcs[tid]
	if !ok {
		err = fmt.Errorf("%s: notification for %q from unknown node %s", n.p.si, uuid, tid)
		status = http.StatusNotFound
		return
	}
	if tsi == nil {
		err = fmt.Errorf("%s: duplicate notification for %q from node %s", n.p.si, uuid, tid)
		return
	}
	if srcErr != nil {
		nl.addErr(tid, srcErr)
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
					msg, err, done = n.hkXact(nl, res)
				default:
					cmn.Assert(false)
				}
			} else if res.status == http.StatusNotFound { // consider silent done at res.si
				err = fmt.Errorf("%s: %q not found at %s", n.p.si, uuid, res.si)
				nl.lock()
				_, _, done = n.handleMsg(nl, uuid, res.si.ID(), err)
				nl.unlock()
			} else if glog.FastV(4, glog.SmoduleAIS) {
				glog.Errorf("%s: failure to get %q from %s: %v", n.p.si, uuid, res.si, res.err)
			}

			if done {
				nl.callback(nl, msg, uuid, err)
				n.del(nl)
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

func (n *notifs) hkXact(nl notifListener, res callResult) (msg interface{}, err error, done bool) {
	var (
		stats = &cmn.BaseXactStatsExt{}
		uuid  = nl.UUID()
	)
	if eru := jsoniter.Unmarshal(res.outjson, stats); eru != nil {
		glog.Errorf("%s: unexpected: failed to unmarshal %q from %s, err: %v",
			n.p.si, uuid, res.si, eru)
		return
	}
	msg = stats
	if stats.Finished() {
		if stats.Aborted() {
			detail := fmt.Sprintf("%q from %s", uuid, res.si)
			err = cmn.NewAbortedErrorDetails(stats.Kind(), detail)
			done = true // NOTE: one abort == all done
			nl.lock()
			nl.addErr(res.si.ID(), err)
			nl.unlock()
		} else {
			nl.lock()
			_, _, done = n.handleMsg(nl, uuid, res.si.ID(), err)
			nl.unlock()
		}
	}
	return
}

// TODO: consider Smap versioning per notifListener
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
		sid := remid[uuid]
		err := &errNodeNotFound{s, sid, n.p.si, smap}
		nl.lock()
		nl.addErr(sid, err)
		nl.unlock()
		nl.callback(nl, nil /*msg*/, uuid, err)
	}
	n.Lock()
	for uuid, nl := range remnl {
		n.del(nl, true /*locked*/)
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

//////////
// misc //
//////////

func notifText(ty int) string {
	const unk = "unknown"
	if txt, ok := notifTyText[ty]; ok {
		return txt
	}
	return unk
}

func (msg *notifMsg) String() string {
	return fmt.Sprintf("%s[%s,%v]<=%s", notifText(int(msg.Ty)), string(msg.Data), msg.Err, msg.Snode)
}
