// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/hk"
	jsoniter "github.com/json-iterator/go"
)

///////////////////////////
// notification receiver //
// see also cmn/notif.go //
///////////////////////////

// TODO: cmn.UponProgress as periodic (byte-count, object-count)
// TODO: batch housekeeping for pending notifications
// TODO: add an option to enforce 'if one notifier fails all fail'
// TODO: housekeeping: broadcast in a separate goroutine

// notification category
const (
	notifInvalid = iota
	notifXact
	notifCache
)

var notifCatText = map[int]string{
	notifInvalid: "invalid",
	notifXact:    "xaction",
	notifCache:   "cache",
}

const (
	notifsName       = ".notifications.prx"
	notifsHousekeepT = 2 * time.Minute
	notifsRemoveMult = 3 // time-to-keep multiplier (time = notifsRemoveMult * notifsHousekeepT)
)

type (
	notifCallback func(n notifListener, msg interface{}, err error)
	notifs        struct {
		sync.RWMutex
		p       *proxyrunner
		m       map[string]notifListener // running  [UUID => notifListener]
		fin     map[string]notifListener // finished [UUID => notifListener]
		fmu     sync.RWMutex
		smapVer int64
	}
	// TODO: simplify using alternate encoding formats (e.g. GOB)
	jsonNotifs struct {
		Running  []*notifListenMsg `json:"running"`
		Finished []*notifListenMsg `json:"finished"`
	}

	notifListener interface {
		callback(notifs *notifs, n notifListener, msg interface{}, err error, nows ...int64)
		lock()
		unlock()
		rlock()
		runlock()
		notifiers() cluster.NodeMap
		incRC() int
		notifTy() int
		kind() string
		bcks() []cmn.Bck
		addErr(string /*sid*/, error)
		err() error
		UUID() string
		Category() int
		finTime() int64
		finished() bool
		String() string
		getOwner() string
		setOwner(string)
		hrwOwner(smap *smapX)
	}

	notifListenerBase struct {
		sync.RWMutex
		UUIDX       string            // UUID
		Srcs        cluster.NodeMap   // expected notifiers
		Errs        map[string]string // [node-ID => ErrMsg]
		f           notifCallback     // optional listening-side callback
		rc          int               // refcount
		Ty          int               // notification category (above)
		FinTime     atomic.Int64      // timestamp when finished
		Owned       string            // "": not owned | equalIC: IC | otherwise, pid + IC
		SmapVersion int64             // smap version in which NL is added
		// common info
		Action string
		Bck    []cmn.Bck
		// ownership
	}

	//
	// notification messages
	//
	notifMsg struct {
		Ty     int32          `json:"type"`    // enumerated type, one of (notifXact, et al.) - see above
		Flags  int32          `json:"flags"`   // TODO: add
		Snode  *cluster.Snode `json:"snode"`   // node
		Data   []byte         `json:"message"` // typed message
		ErrMsg string         `json:"err"`     // error.Error()
	}
	// receiver to start listening
	// TODO: explore other encoding formats (e.g. GOB) to simplify Marshal and Unmarshal logic
	notifListenMsg struct {
		nl notifListener
	}
	jsonNL struct {
		Type string              `json:"type"`
		NL   jsoniter.RawMessage `json:"nl"`
	}
)

// interface guard
var (
	_ notifListener     = &notifListenerBase{}
	_ cluster.Slistener = &notifs{}
)

func newNLB(uuid string, smap *smapX, ty int, action string, bck ...cmn.Bck) *notifListenerBase {
	cmn.Assert(ty > notifInvalid)
	return &notifListenerBase{UUIDX: uuid, Srcs: smap.Tmap.Clone(), SmapVersion: smap.version(), Ty: ty, Action: action, Bck: bck}
}

///////////////////////
// notifListenerBase //
///////////////////////

// is called after all notifiers will have notified OR on failure (err != nil)
func (nlb *notifListenerBase) callback(notifs *notifs, nl notifListener, msg interface{}, err error, nows ...int64) {
	if nlb.FinTime.CAS(0, 1) {
		var now int64
		if len(nows) > 0 {
			now = nows[0]
		} else {
			now = time.Now().UnixNano()
		}
		// is optional
		if nlb.f != nil {
			nlb.f(nl, msg, err) // invoke user-supplied callback and pass user-supplied notifListener
		}
		nlb.FinTime.Store(now)
		notifs.fmu.Lock()
		notifs.fin[nl.UUID()] = nl
		notifs.fmu.Unlock()
	}
}
func (nlb *notifListenerBase) lock()                      { nlb.Lock() }
func (nlb *notifListenerBase) unlock()                    { nlb.Unlock() }
func (nlb *notifListenerBase) rlock()                     { nlb.RLock() }
func (nlb *notifListenerBase) runlock()                   { nlb.RUnlock() }
func (nlb *notifListenerBase) notifiers() cluster.NodeMap { return nlb.Srcs }
func (nlb *notifListenerBase) incRC() int                 { nlb.rc++; return nlb.rc }
func (nlb *notifListenerBase) notifTy() int               { return nlb.Ty }
func (nlb *notifListenerBase) UUID() string               { return nlb.UUIDX }
func (nlb *notifListenerBase) Category() int              { return nlb.Ty }
func (nlb *notifListenerBase) finTime() int64             { return nlb.FinTime.Load() }
func (nlb *notifListenerBase) finished() bool             { return nlb.finTime() > 0 }
func (nlb *notifListenerBase) addErr(sid string, err error) {
	if nlb.Errs == nil {
		nlb.Errs = make(map[string]string, 2)
	}
	nlb.Errs[sid] = err.Error()
}

func (nlb *notifListenerBase) err() error {
	for _, err := range nlb.Errs {
		return errors.New(err)
	}
	return nil
}

func (nlb *notifListenerBase) String() string {
	var tm, res string
	hdr := fmt.Sprintf("%s-%q", notifText(nlb.Ty), nlb.UUIDX)
	if tfin := nlb.FinTime.Load(); tfin > 0 {
		if nlb.Errs != nil {
			res = fmt.Sprintf("-fail(%+v)", nlb.Errs)
		}
		tm = cmn.FormatTimestamp(time.Unix(0, tfin))
		return fmt.Sprintf("%s-%s%s", hdr, tm, res)
	}
	if nlb.rc > 0 {
		return fmt.Sprintf("%s-%d/%d", hdr, nlb.rc, len(nlb.Srcs))
	}
	return hdr
}

func (nlb *notifListenerBase) getOwner() string  { return nlb.Owned }
func (nlb *notifListenerBase) setOwner(o string) { nlb.Owned = o }
func (nlb *notifListenerBase) kind() string      { return nlb.Action }
func (nlb *notifListenerBase) bcks() []cmn.Bck   { return nlb.Bck }

// effectively, cache owner
func (nlb *notifListenerBase) hrwOwner(smap *smapX) {
	psiOwner, err := cluster.HrwIC(&smap.Smap, nlb.UUID())
	cmn.AssertNoErr(err) // TODO -- FIXME: handle
	nlb.setOwner(psiOwner.ID())
}

////////////
// notifs //
////////////

func (n *notifs) init(p *proxyrunner) {
	n.p = p
	n.m = make(map[string]notifListener, 64)
	n.fin = make(map[string]notifListener, 64)
	hk.Reg(notifsName+".gc", n.housekeep, notifsHousekeepT)
	n.p.GetSowner().Listeners().Reg(n)
}

func (n *notifs) String() string { return notifsName }

// start listening
func (n *notifs) add(nl notifListener) {
	cmn.Assert(nl.UUID() != "")
	cmn.Assert(nl.Category() > notifInvalid)
	n.Lock()
	_, ok := n.m[nl.UUID()]
	cmn.AssertMsg(!ok, nl.UUID())
	n.m[nl.UUID()] = nl
	n.Unlock()
	glog.Infoln(nl.String())
}

func (n *notifs) del(nl notifListener, locked ...bool) {
	var ok bool
	if len(locked) == 0 {
		n.Lock()
	}
	if _, ok = n.m[nl.UUID()]; ok {
		delete(n.m, nl.UUID())
	}
	if len(locked) == 0 {
		n.Unlock()
	}
	if ok {
		glog.Infoln(nl.String())
	}
}

func (n *notifs) entry(uuid string) (notifListener, bool) {
	n.RLock()
	entry, exists := n.m[uuid]
	n.RUnlock()
	if exists {
		return entry, true
	}
	n.fmu.RLock()
	entry, exists = n.fin[uuid]
	n.fmu.RUnlock()
	if exists {
		return entry, true
	}
	return nil, false
}

// verb /v1/notifs
func (n *notifs) handler(w http.ResponseWriter, r *http.Request) {
	var (
		notifMsg = &notifMsg{}
		msg      interface{}
		errMsg   error
		uuid     string
		tid      = r.Header.Get(cmn.HeaderCallerID) // sender node ID
	)
	if r.Method != http.MethodPost {
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /notifs path")
		return
	}
	if _, err := n.p.checkRESTItems(w, r, 0, false, cmn.Version, cmn.Notifs); err != nil {
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
	case notifCache:
		return // TODO -- FIXME: implement
	default:
		n.p.invalmsghdlrstatusf(w, r, 0, "%s: unknown notification type %s", n.p.si, notifMsg)
		return
	}
	var (
		nl     notifListener
		exists bool
	)

	withLocalRetry(func() bool {
		nl, exists = n.entry(uuid)
		return exists
	})

	if !exists {
		n.p.invalmsghdlrstatusf(w, r, http.StatusNotFound, "%s: unknown UUID %q (%s)", n.p.si, uuid, notifMsg)
		return
	}
	if nl.finished() {
		n.p.invalmsghdlrstatusf(w, r, 0, "%s: %q already finished (msg=%s)", n.p.si, uuid, notifMsg)
		return
	}
	//
	// notifListener and notifMsg must have the same type
	//
	cmn.Assert(nl.notifTy() == int(notifMsg.Ty))
	if notifMsg.ErrMsg != "" {
		errMsg = errors.New(notifMsg.ErrMsg)
	}
	nl.lock()
	err, status, done := n.handleMsg(nl, tid, errMsg)
	nl.unlock()
	if done {
		nl.callback(n, nl, msg, nil)
		n.del(nl)
	}
	if err != nil {
		n.p.invalmsghdlr(w, r, err.Error(), status)
	}
}

// is called under notifListener.lock()
func (n *notifs) handleMsg(nl notifListener, tid string, srcErr error) (err error, status int, done bool) {
	srcs := nl.notifiers()
	tsi, ok := srcs[tid]
	if !ok {
		err = fmt.Errorf("%s: %s from unknown node %s", n.p.si, nl, tid)
		status = http.StatusNotFound
		return
	}
	if tsi == nil {
		err = fmt.Errorf("%s: duplicate %s from node %s", n.p.si, nl, tid)
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
	now := time.Now().UnixNano()
	n.fmu.Lock()
	for uuid, nl := range n.fin {
		if time.Duration(now-nl.finTime()) > notifsRemoveMult*notifsHousekeepT {
			delete(n.fin, uuid)
		}
	}
	n.fmu.Unlock()

	if len(n.m) == 0 {
		return notifsHousekeepT
	}
	n.RLock()
	tempn := make(map[string]notifListener, len(n.m))
	for uuid, nl := range n.m {
		tempn[uuid] = nl
	}
	n.RUnlock()
	args := bcastArgs{
		req:     cmn.ReqArgs{Method: http.MethodGet, Query: make(url.Values, 2)},
		timeout: cmn.GCO.Get().Timeout.MaxKeepalive,
	}
	for uuid, nl := range tempn {
		if nl.notifTy() == notifCache { // TODO -- FIXME: implement
			continue
		}
		cmn.AssertMsg(nl.notifTy() == notifXact, nl.String())
		args.req.Path = cmn.URLPath(cmn.Version, cmn.Xactions)
		args.req.Query.Set(cmn.URLParamWhat, cmn.GetWhatXactStats)
		args.req.Query.Set(cmn.URLParamUUID, uuid)
		args.fv = func() interface{} { return &cmn.BaseXactStatsExt{} }
		results := n.p.bcastToGroup(args)
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
				err = fmt.Errorf("%s: %s not found at %s", n.p.si, nl, res.si)
				done = true // NOTE: not-found at one ==> all done
				nl.lock()
				nl.addErr(res.si.ID(), err)
				nl.unlock()
			} else if glog.FastV(4, glog.SmoduleAIS) {
				glog.Errorf("%s: %s, node %s, err: %v", n.p.si, nl, res.si, res.err)
			}

			if done {
				nl.callback(n, nl, msg, err, now)
				n.del(nl)
				break
			}
		}
	}
	// cleanup temp cloned notifs
	for u := range tempn {
		delete(tempn, u)
	}
	return notifsHousekeepT
}

func (n *notifs) getOwner(uuid string) (o string, exists bool) {
	var nl notifListener
	if nl, exists = n.entry(uuid); exists {
		o = nl.getOwner()
	}
	return
}

func (n *notifs) hkXact(nl notifListener, res callResult) (msg interface{}, err error, done bool) {
	stats := res.v.(*cmn.BaseXactStatsExt)
	msg = stats
	if stats.Finished() {
		if stats.Aborted() {
			detail := fmt.Sprintf("%s, node %s", nl, res.si)
			err = cmn.NewAbortedErrorDetails(stats.Kind(), detail)
			done = true // NOTE: one abort ==> all done
			nl.lock()
			nl.addErr(res.si.ID(), err)
			nl.unlock()
		} else {
			nl.lock()
			_, _, done = n.handleMsg(nl, res.si.ID(), err)
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
	smap := n.p.owner.smap.get()
	if n.smapVer >= smap.Version {
		return
	}
	n.smapVer = smap.Version

	if len(n.m) == 0 {
		return
	}

	var (
		remnl = make(map[string]notifListener)
		remid = make(cmn.SimpleKVs)
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
	now := time.Now().UnixNano()
	for uuid, nl := range remnl {
		s := fmt.Sprintf("%s: stop waiting for %s", n.p.si, nl)
		sid := remid[uuid]
		err := &errNodeNotFound{s, sid, n.p.si, smap}
		nl.lock()
		nl.addErr(sid, err)
		nl.unlock()
		nl.callback(n, nl, nil /*msg*/, err, now)
	}
	n.Lock()
	for uuid, nl := range remnl {
		n.del(nl, true /*locked*/)
		// cleanup
		delete(remnl, uuid)
		delete(remid, uuid)
	}
	n.Unlock()
}

func (n *notifs) MarshalJSON() (data []byte, err error) {
	t := jsonNotifs{}
	n.RLock()
	n.fmu.RLock()
	defer func() {
		n.fmu.RUnlock()
		n.RUnlock()
	}()
	t.Running = make([]*notifListenMsg, 0, len(n.m))
	t.Finished = make([]*notifListenMsg, 0, len(n.fin))
	for _, nl := range n.m {
		t.Running = append(t.Running, newNLMsg(nl))
	}

	for _, nl := range n.fin {
		t.Finished = append(t.Finished, newNLMsg(nl))
	}
	return jsoniter.Marshal(t)
}

func (n *notifs) UnmarshalJSON(data []byte) (err error) {
	t := jsonNotifs{}

	if err = jsoniter.Unmarshal(data, &t); err != nil {
		return
	}
	if len(t.Running) > 0 {
		n.Lock()
		_mergeNLs(n.m, t.Running)
		n.Unlock()
	}

	if len(t.Finished) > 0 {
		n.fmu.Lock()
		_mergeNLs(n.fin, t.Finished)
		n.fmu.Unlock()
	}
	return
}

// PRECONDITION: Lock for `nls` must be held
func _mergeNLs(nls map[string]notifListener, msgs []*notifListenMsg) {
	for _, m := range msgs {
		if _, ok := nls[m.nl.UUID()]; !ok {
			nls[m.nl.UUID()] = m.nl
		}
	}
}

func newNLMsg(nl notifListener) *notifListenMsg {
	return &notifListenMsg{nl: nl}
}

func (n *notifListenMsg) MarshalJSON() (data []byte, err error) {
	t := jsonNL{Type: n.nl.kind()}
	t.NL, err = jsoniter.Marshal(n.nl)
	if err != nil {
		return
	}
	return jsoniter.Marshal(t)
}

func (n *notifListenMsg) UnmarshalJSON(data []byte) (err error) {
	t := jsonNL{}
	if err = jsoniter.Unmarshal(data, &t); err != nil {
		return
	}
	if t.Type == cmn.ActQueryObjects {
		n.nl = &notifListenerQuery{}
	} else {
		n.nl = &notifListenerBase{}
	}
	err = jsoniter.Unmarshal(t.NL, &n.nl)
	if err != nil {
		return
	}
	return
}

//////////
// misc //
//////////

func notifText(ty int) string {
	const unk = "unknown"
	if txt, ok := notifCatText[ty]; ok {
		return txt
	}
	return unk
}

func (msg *notifMsg) String() string {
	return fmt.Sprintf("%s[%s,%v]<=%s", notifText(int(msg.Ty)), string(msg.Data), msg.ErrMsg, msg.Snode)
}
