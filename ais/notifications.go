// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/objwalk/query"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
	jsoniter "github.com/json-iterator/go"
)

////////////////////////////////
// notification receiver      //
// see also cluster/notif.go //
//////////////////////////////

// TODO: cmn.UponProgress as periodic (byte-count, object-count)
// TODO: batch housekeeping for pending notifications
// TODO: add an option to enforce 'if one notifier fails all fail'
// TODO: housekeeping: broadcast in a separate goroutine

// notification category
const (
	notifsName       = ".notifications.prx"
	notifsHousekeepT = 2 * time.Minute
	notifsRemoveMult = 3 // time-to-keep multiplier (time = notifsRemoveMult * notifsHousekeepT)
)

type (
	listeners struct {
		sync.RWMutex
		m map[string]nl.NotifListener // [UUID => NotifListener]
	}

	notifs struct {
		p   *proxy
		nls *listeners // running
		fin *listeners // finished

		mu       sync.Mutex
		added    []nl.NotifListener // reusable slice of `nl` to add to `nls`
		removed  []nl.NotifListener // reusable slice of `nl` to remove from `nls`
		finished []nl.NotifListener // reusable slice of `nl` to add to `fin`
		smapVer  int64
	}
	jsonNotifs struct {
		Running  []*notifListenMsg `json:"running"`
		Finished []*notifListenMsg `json:"finished"`
	}

	nlFilter xreg.XactFilter

	//
	// notification messages
	//

	// receiver to start listening
	notifListenMsg struct {
		nl nl.NotifListener
	}
	jsonNL struct {
		Type string              `json:"type"`
		NL   jsoniter.RawMessage `json:"nl"`
	}
)

// interface guard
var (
	_ cluster.Slistener = (*notifs)(nil)

	_ json.Marshaler   = (*notifs)(nil)
	_ json.Unmarshaler = (*notifs)(nil)

	_ json.Marshaler   = (*notifListenMsg)(nil)
	_ json.Unmarshaler = (*notifListenMsg)(nil)
)

///////////////
// listeners //
///////////////

func newListeners() *listeners { return &listeners{m: make(map[string]nl.NotifListener, 64)} }
func (l *listeners) len() int  { return len(l.m) }

func (l *listeners) entry(uuid string) (entry nl.NotifListener, exists bool) {
	l.RLock()
	entry, exists = l.m[uuid]
	l.RUnlock()
	return
}

func (l *listeners) add(nl nl.NotifListener, locked bool) (exists bool) {
	if !locked {
		l.Lock()
	}
	if _, exists = l.m[nl.UUID()]; !exists {
		l.m[nl.UUID()] = nl
	}
	if !locked {
		l.Unlock()
	}
	return
}

func (l *listeners) del(nl nl.NotifListener, locked bool) (ok bool) {
	if !locked {
		l.Lock()
	} else {
		debug.AssertRWMutexLocked(&l.RWMutex)
	}
	if _, ok = l.m[nl.UUID()]; ok {
		delete(l.m, nl.UUID())
	}
	if !locked {
		l.Unlock()
	}
	return
}

// PRECONDITION: `l` should be under lock.
func (l *listeners) exists(uuid string) (ok bool) {
	_, ok = l.m[uuid]
	return
}

// returns a listener that matches the filter condition.
// for finished xaction listeners, returns latest listener (i.e. having highest finish time)
func (l *listeners) find(flt nlFilter) (nl nl.NotifListener, exists bool) {
	l.RLock()
	defer l.RUnlock()

	var ftime int64
	for _, listener := range l.m {
		if listener.EndTime() < ftime {
			continue
		}
		if flt.match(listener) {
			ftime = listener.EndTime()
			nl, exists = listener, true
		}
		if exists && !listener.Finished() {
			return
		}
	}
	return
}

////////////
// notifs //
////////////

func (n *notifs) init(p *proxy) {
	n.p = p
	n.nls = newListeners()
	n.fin = newListeners()

	n.added = make([]nl.NotifListener, 16)
	n.removed = make([]nl.NotifListener, 16)
	n.finished = make([]nl.NotifListener, 16)

	hk.Reg(notifsName+".gc", n.housekeep, notifsHousekeepT)
	n.p.Sowner().Listeners().Reg(n)
}

func (*notifs) String() string { return notifsName }

// start listening
func (n *notifs) add(nl nl.NotifListener) (err error) {
	debug.Assert(cos.IsValidUUID(nl.UUID()) || xact.IsValidRebID(nl.UUID()))
	if nl.ActiveCount() == 0 {
		return fmt.Errorf("cannot add %q with no active notifiers", nl)
	}
	if exists := n.nls.add(nl, false /*locked*/); exists {
		return
	}
	nl.SetAddedTime()
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infoln("add " + nl.String())
	}
	return
}

func (n *notifs) del(nl nl.NotifListener, locked bool) (ok bool) {
	ok = n.nls.del(nl, locked /*locked*/)
	if ok && bool(glog.FastV(4, glog.SmoduleAIS)) {
		glog.Infoln("del " + nl.String())
	}
	return
}

func (n *notifs) entry(uuid string) (nl.NotifListener, bool) {
	entry, exists := n.nls.entry(uuid)
	if exists {
		return entry, true
	}
	entry, exists = n.fin.entry(uuid)
	if exists {
		return entry, true
	}
	return nil, false
}

func (n *notifs) find(flt nlFilter) (nl nl.NotifListener, exists bool) {
	if flt.ID != "" {
		return n.entry(flt.ID)
	}
	nl, exists = n.nls.find(flt)
	if exists || (flt.OnlyRunning != nil && *flt.OnlyRunning) {
		return
	}
	nl, exists = n.fin.find(flt)
	return
}

func (n *notifs) size() (size int) {
	n.nls.RLock()
	n.fin.RLock()
	size = n.nls.len() + n.fin.len()
	n.fin.RUnlock()
	n.nls.RUnlock()
	return
}

// verb /v1/notifs/[progress|finished]
func (n *notifs) handler(w http.ResponseWriter, r *http.Request) {
	var (
		notifMsg = &cluster.NotifMsg{}
		nl       nl.NotifListener
		errMsg   error
		uuid     string
		tid      = r.Header.Get(cmn.HdrCallerID) // sender node ID
		exists   bool
	)
	if r.Method != http.MethodPost {
		cmn.WriteErr405(w, r, http.MethodPost)
		return
	}
	apiItems, err := n.p.checkRESTItems(w, r, 1, false, cmn.URLPathNotifs.L)
	if err != nil {
		return
	}
	if apiItems[0] != cmn.Progress && apiItems[0] != cmn.Finished {
		n.p.writeErrf(w, r, "Invalid route /notifs/%s", apiItems[0])
		return
	}
	if cmn.ReadJSON(w, r, notifMsg) != nil {
		return
	}

	// NOTE: the sender is asynchronous - ignores the response -
	//       which is why we consider `not-found`, `already-finished`,
	//       and `unknown-notifier` benign non-error conditions
	uuid = notifMsg.UUID
	if !withRetry(func() bool { nl, exists = n.entry(uuid); return exists }) {
		return
	}

	var (
		srcs    = nl.Notifiers()
		tsi, ok = srcs[tid]
	)
	if !ok {
		return
	}
	//
	// NotifListener and notifMsg must have the same type
	//
	nl.RLock()
	if nl.HasFinished(tsi) {
		n.p.writeErrSilentf(w, r, http.StatusBadRequest,
			"%s: duplicate %s from %s, %s", n.p.si, notifMsg, tid, nl)
		nl.RUnlock()
		return
	}
	nl.RUnlock()

	if notifMsg.ErrMsg != "" {
		errMsg = errors.New(notifMsg.ErrMsg)
	}

	// NOTE: Default case is not required - will reach here only for valid types.
	switch apiItems[0] {
	// TODO: implement on Started notification
	case cmn.Progress:
		err = n.handleProgress(nl, tsi, notifMsg.Data, errMsg)
	case cmn.Finished:
		err = n.handleFinished(nl, tsi, notifMsg.Data, errMsg)
	}

	if err != nil {
		n.p.writeErr(w, r, err)
	}
}

func (*notifs) handleProgress(nl nl.NotifListener, tsi *cluster.Snode, data []byte,
	srcErr error) (err error) {
	nl.Lock()
	defer nl.Unlock()

	if srcErr != nil {
		nl.SetErr(srcErr)
	}
	if data != nil {
		stats, _, _, err := nl.UnmarshalStats(data)
		debug.AssertNoErr(err)
		nl.SetStats(tsi.ID(), stats)
	}
	return
}

func (n *notifs) handleFinished(nl nl.NotifListener, tsi *cluster.Snode, data []byte, srcErr error) (err error) {
	var (
		stats   interface{}
		aborted bool
	)
	nl.Lock()
	// data can either be `nil` or a valid encoded stats
	if data != nil {
		stats, _, aborted, err = nl.UnmarshalStats(data)
		debug.AssertNoErr(err)
		nl.SetStats(tsi.ID(), stats)
	}
	done := n.markFinished(nl, tsi, srcErr, aborted)
	nl.Unlock()
	if done {
		n.done(nl)
	}
	return
}

// PRECONDITION: `nl` should be under lock.
func (*notifs) markFinished(nl nl.NotifListener, tsi *cluster.Snode, srcErr error, aborted bool) (done bool) {
	nl.MarkFinished(tsi)
	if aborted {
		nl.SetAborted()
		if srcErr == nil {
			detail := fmt.Sprintf("%s, node %s", nl, tsi)
			srcErr = cmn.NewErrAborted(nl.String(), detail, nil)
		}
	}
	if srcErr != nil {
		nl.SetErr(srcErr)
	}
	return nl.ActiveCount() == 0 || aborted
}

func (n *notifs) done(nl nl.NotifListener) {
	if !n.del(nl, false /*locked*/) {
		// `nl` already removed from active map
		return
	}
	n.fin.add(nl, false /*locked*/)

	if nl.Aborted() {
		config := cmn.GCO.Get()
		// NOTE: we accept finished notifications even after
		// `nl` is aborted. Handle locks carefully.
		args := allocBcastArgs()
		args.req = nl.AbortArgs()
		args.network = cmn.NetworkIntraControl
		args.timeout = config.Timeout.MaxKeepalive.D()
		args.nodes = []cluster.NodeMap{nl.Notifiers()}
		args.nodeCount = len(args.nodes[0])
		args.async = true
		_ = n.p.bcastNodes(args)
		freeBcastArgs(args)
	}
	nl.Callback(nl, time.Now().UnixNano())
}

//
// housekeeping
//

func (n *notifs) housekeep() time.Duration {
	now := time.Now().UnixNano()
	n.fin.Lock()
	for _, nl := range n.fin.m {
		if time.Duration(now-nl.EndTime()) > notifsRemoveMult*notifsHousekeepT {
			n.fin.del(nl, true /*locked*/)
		}
	}
	n.fin.Unlock()

	n.nls.RLock()
	if n.nls.len() == 0 {
		n.nls.RUnlock()
		return notifsHousekeepT
	}
	tempn := make(map[string]nl.NotifListener, n.nls.len())
	for uuid, nl := range n.nls.m {
		tempn[uuid] = nl
	}
	n.nls.RUnlock()
	for _, nl := range tempn {
		n.syncStats(nl, notifsHousekeepT)
	}
	// cleanup temp cloned notifs
	for u := range tempn {
		delete(tempn, u)
	}
	return notifsHousekeepT
}

func (n *notifs) syncStats(nl nl.NotifListener, dur ...time.Duration) {
	var (
		config           = cmn.GCO.Get()
		progressInterval = config.Periodic.NotifTime.D()
		done             bool
	)
	nl.RLock()
	nodesTardy, syncRequired := nl.NodesTardy(dur...)
	nl.RUnlock()
	if !syncRequired {
		return
	}
	args := allocBcastArgs()
	args.network = cmn.NetworkIntraControl
	args.timeout = config.Timeout.MaxHostBusy.D()
	args.req = nl.QueryArgs() // nodes to fetch stats from
	args.nodes = []cluster.NodeMap{nodesTardy}
	args.nodeCount = len(args.nodes[0])
	debug.Assert(args.nodeCount > 0) // Ensure that there is at least one node to fetch.

	results := n.p.bcastNodes(args)
	freeBcastArgs(args)
	for _, res := range results {
		if res.err == nil {
			stats, finished, aborted, err := nl.UnmarshalStats(res.bytes)
			if err != nil {
				glog.Errorf("%s: failed to parse stats from %s, err: %v", n.p.si, res.si, err)
				continue
			}
			nl.Lock()
			if finished {
				done = done || n.markFinished(nl, res.si, nil, aborted)
			}
			nl.SetStats(res.si.ID(), stats)
			nl.Unlock()
		} else if res.status == http.StatusNotFound {
			if mono.Since(nl.AddedTime()) < progressInterval {
				// likely didn't start yet - skipping
				continue
			}
			err := fmt.Errorf("%s: %s not found at %s", n.p.si, nl, res.si)
			nl.Lock()
			done = done || n.markFinished(nl, res.si, err, true) // NOTE: not-found at one ==> all done
			nl.Unlock()
		} else if glog.FastV(4, glog.SmoduleAIS) {
			glog.Errorf("%s: %s, node %s, err: %v", n.p.si, nl, res.si, res.err)
		}
	}
	freeBcastRes(results)
	if done {
		n.done(nl)
	}
}

// Return stats from each node for a given UUID.
func (n *notifs) queryStats(uuid string, durs ...time.Duration) (stats *nl.NodeStats, exists bool) {
	var nl nl.NotifListener
	nl, exists = n.entry(uuid)
	if !exists {
		return
	}
	n.syncStats(nl, durs...)
	stats = nl.NodeStats()
	return
}

func (n *notifs) getOwner(uuid string) (o string, exists bool) {
	var nl nl.NotifListener
	if nl, exists = n.entry(uuid); exists {
		o = nl.GetOwner()
	}
	return
}

// TODO: consider Smap versioning per NotifListener
func (n *notifs) ListenSmapChanged() {
	if !n.p.ClusterStarted() {
		return
	}
	smap := n.p.owner.smap.get()
	if n.smapVer >= smap.Version {
		return
	}
	n.smapVer = smap.Version

	n.nls.RLock()
	if n.nls.len() == 0 {
		n.nls.RUnlock()
		return
	}
	var (
		remnl = make(map[string]nl.NotifListener)
		remid = make(cos.SimpleKVs)
	)
	for uuid, nl := range n.nls.m {
		nl.RLock()
		for sid := range nl.ActiveNotifiers() {
			if node := smap.GetNodeNotMaint(sid); node == nil {
				remnl[uuid] = nl
				remid[uuid] = sid
				break
			}
		}
		nl.RUnlock()
	}
	n.nls.RUnlock()
	if len(remnl) == 0 {
		return
	}
	now := time.Now().UnixNano()
	for uuid, nl := range remnl {
		s := fmt.Sprintf("%s: stop waiting for %s", n.p.si, nl)
		sid := remid[uuid]
		err := &errNodeNotFound{s, sid, n.p.si, smap}
		nl.Lock()
		nl.SetErr(err)
		nl.SetAborted()
		nl.Unlock()
	}
	n.fin.Lock()
	for uuid, nl := range remnl {
		cos.Assert(nl.UUID() == uuid)
		n.fin.add(nl, true /*locked*/)
	}
	n.fin.Unlock()
	n.nls.Lock()
	for _, nl := range remnl {
		n.del(nl, true /*locked*/)
	}
	n.nls.Unlock()

	for uuid, nl := range remnl {
		nl.Callback(nl, now)
		// cleanup
		delete(remnl, uuid)
		delete(remid, uuid)
	}
}

func (n *notifs) MarshalJSON() (data []byte, err error) {
	t := jsonNotifs{}
	n.nls.RLock()
	n.fin.RLock()
	if n.nls.len() == 0 && n.fin.len() == 0 {
		n.fin.RUnlock()
		n.nls.RUnlock()
		return
	}
	t.Running = make([]*notifListenMsg, 0, n.nls.len())
	t.Finished = make([]*notifListenMsg, 0, n.fin.len())
	for _, nl := range n.nls.m {
		t.Running = append(t.Running, newNLMsg(nl))
	}
	n.nls.RUnlock()

	for _, nl := range n.fin.m {
		t.Finished = append(t.Finished, newNLMsg(nl))
	}
	n.fin.RUnlock()

	return jsoniter.Marshal(t)
}

func (n *notifs) UnmarshalJSON(data []byte) (err error) {
	if len(data) == 0 {
		return
	}
	t := jsonNotifs{}
	if err = jsoniter.Unmarshal(data, &t); err != nil {
		return
	}
	if len(t.Running) == 0 && len(t.Finished) == 0 {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	// Identify the diff in ownership table and populate `added`, `removed` and `finished` slices
	added, removed, finished := n.added[:0], n.removed[:0], n.finished[:0]
	n.nls.RLock()
	n.fin.RLock()
	for _, m := range t.Running {
		if n.fin.exists(m.nl.UUID()) || n.nls.exists(m.nl.UUID()) {
			continue
		}
		added = append(added, m.nl)
	}

	for _, m := range t.Finished {
		if n.fin.exists(m.nl.UUID()) {
			continue
		}
		if n.nls.exists(m.nl.UUID()) {
			removed = append(removed, m.nl)
		}
		finished = append(finished, m.nl)
	}
	n.fin.RUnlock()
	n.nls.RUnlock()

	if len(removed) == 0 && len(added) == 0 {
		goto fin
	}

	// Add/Remove `nl` - `n.nls`.
	n.nls.Lock()
	for _, nl := range added {
		n.nls.add(nl, true /*locked*/)
	}
	for _, nl := range removed {
		n.nls.del(nl, true /*locked*/)
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infoln("merge: del " + nl.String())
		}
	}
	n.nls.Unlock()

fin:
	if len(finished) == 0 {
		return
	}

	n.fin.Lock()
	// Add `nl` to `n.fin`.
	for _, nl := range finished {
		n.fin.add(nl, true /*locked*/)
	}
	n.fin.Unlock()

	// Call the Callback for each `nl` marking it finished.
	now := time.Now().UnixNano()
	for _, nl := range finished {
		nl.Callback(nl, now)
	}
	return
}

func newNLMsg(nl nl.NotifListener) *notifListenMsg {
	return &notifListenMsg{nl: nl}
}

func (n *notifListenMsg) MarshalJSON() (data []byte, err error) {
	n.nl.RLock()
	msg := jsonNL{Type: n.nl.Kind(), NL: cos.MustMarshal(n.nl)}
	n.nl.RUnlock()
	return jsoniter.Marshal(msg)
}

func (n *notifListenMsg) UnmarshalJSON(data []byte) (err error) {
	t := jsonNL{}
	if err = jsoniter.Unmarshal(data, &t); err != nil {
		return
	}
	if t.Type == cmn.ActQueryObjects {
		n.nl = &query.NotifListenerQuery{}
	} else if downloader.IsType(t.Type) {
		n.nl = &downloader.NotifDownloadListerner{}
	} else {
		n.nl = &xact.NotifXactListener{}
	}
	return jsoniter.Unmarshal(t.NL, &n.nl)
}

//
// Notification listener filter (nlFilter)
//

func (nf *nlFilter) match(nl nl.NotifListener) bool {
	if nl.UUID() == nf.ID {
		return true
	}

	if nl.Kind() == nf.Kind {
		if nf.Bck == nil || nf.Bck.IsEmpty() {
			return true
		}
		for _, bck := range nl.Bcks() {
			if cmn.QueryBcks(nf.Bck.Bck).Contains(bck) {
				return true
			}
		}
	}
	return false
}
