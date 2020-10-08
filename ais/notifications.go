// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/query"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/registry"
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
	notifs struct {
		sync.RWMutex
		p       *proxyrunner
		m       map[string]nl.NotifListener // running  [UUID => NotifListener]
		fin     map[string]nl.NotifListener // finished [UUID => NotifListener]
		fmu     sync.RWMutex
		smapVer int64
	}
	// TODO: simplify using alternate encoding formats (e.g. GOB)
	jsonNotifs struct {
		Running  []*notifListenMsg `json:"running"`
		Finished []*notifListenMsg `json:"finished"`
	}

	nlFilter registry.XactFilter

	//
	// notification messages
	//

	// receiver to start listening
	// TODO: explore other encoding formats (e.g. GOB) to simplify Marshal and Unmarshal logic
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
	_ cluster.Slistener = &notifs{}
)

////////////
// notifs //
////////////

func (n *notifs) init(p *proxyrunner) {
	n.p = p
	n.m = make(map[string]nl.NotifListener, 64)
	n.fin = make(map[string]nl.NotifListener, 64)
	hk.Reg(notifsName+".gc", n.housekeep, notifsHousekeepT)
	n.p.Sowner().Listeners().Reg(n)
}

func (n *notifs) String() string { return notifsName }

// start listening
func (n *notifs) add(nl nl.NotifListener) {
	cmn.Assert(nl.UUID() != "")
	n.Lock()
	if _, ok := n.m[nl.UUID()]; ok {
		n.Unlock()
		return
	}
	n.m[nl.UUID()] = nl
	nl.SetAddedTime()
	n.Unlock()
	glog.Infoln("add " + nl.String())
}

func (n *notifs) del(nl nl.NotifListener, locked ...bool) {
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
		glog.Infoln("del " + nl.String())
	}
}

func (n *notifs) entry(uuid string) (nl.NotifListener, bool) {
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

func (n *notifs) find(flt nlFilter) (nl nl.NotifListener, exists bool) {
	if flt.ID != "" {
		return n.entry(flt.ID)
	}
	n.RLock()
	nl, exists = _findNL(n.m, flt)
	n.RUnlock()
	if exists || (flt.OnlyRunning != nil && *flt.OnlyRunning) {
		return
	}
	n.fmu.RLock()
	nl, exists = _findNL(n.fin, flt)
	n.fmu.RUnlock()
	return
}

// PRECONDITION: Lock for `nls` must be held.
// returns a listener that matches the filter condition.
// for finished xaction listeners, returns latest listener (i.e. having highest finish time)
func _findNL(nls map[string]nl.NotifListener, flt nlFilter) (nl nl.NotifListener, exists bool) {
	var ftime int64
	for _, listener := range nls {
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

// verb /v1/notifs/[progress|finished]
func (n *notifs) handler(w http.ResponseWriter, r *http.Request) {
	var (
		notifMsg = &cluster.NotifMsg{}
		nl       nl.NotifListener
		errMsg   error
		uuid     string
		tid      = r.Header.Get(cmn.HeaderCallerID) // sender node ID
		exists   bool
	)
	if r.Method != http.MethodPost {
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /notifs path")
		return
	}
	apiItems, err := n.p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Notifs)
	if err != nil {
		return
	}
	if apiItems[0] != cmn.Progress && apiItems[0] != cmn.Finished {
		n.p.invalmsghdlrf(w, r, "Invalid route /notifs/%s", apiItems[0])
		return
	}
	if cmn.ReadJSON(w, r, notifMsg) != nil {
		return
	}

	uuid = notifMsg.UUID
	if !withLocalRetry(func() bool { nl, exists = n.entry(uuid); return exists }) {
		n.p.invalmsghdlrstatusf(w, r, http.StatusNotFound, "%s: unknown nl, %s", n.p.si, notifMsg)
		return
	}
	if nl.Finished() {
		s := fmt.Sprintf("%s: %s already finished (%s)", n.p.si, nl, notifMsg)
		n.p.invalmsghdlrsilent(w, r, s, http.StatusGone)
		return
	}

	var (
		srcs    = nl.Notifiers()
		tsi, ok = srcs[tid]
	)
	if !ok {
		n.p.invalmsghdlrstatusf(w, r, http.StatusNotFound, "%s: %s from unknown %s", n.p.si, notifMsg, tid)
		return
	}
	//
	// NotifListener and notifMsg must have the same type
	//
	nl.RLock()
	if nl.HasFinished(tsi) {
		n.p.invalmsghdlrsilent(w, r, fmt.Sprintf("%s: duplicate %s from %s, %s", n.p.si, notifMsg, tid, nl))
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
		n.p.invalmsghdlr(w, r, err.Error())
	}
}

func (n *notifs) handleProgress(nl nl.NotifListener, tsi *cluster.Snode, data []byte,
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

func (n *notifs) handleFinished(nl nl.NotifListener, tsi *cluster.Snode, data []byte,
	srcErr error) (err error) {
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
		nl.Callback(nl, nil, time.Now().UnixNano())
	}
	return
}

// PRECONDITION: `nl` should be under lock.
func (n *notifs) markFinished(nl nl.NotifListener, tsi *cluster.Snode, srcErr error, aborted bool) (done bool) {
	nl.MarkFinished(tsi)

	if aborted {
		nl.SetAborted()
		if srcErr == nil {
			detail := fmt.Sprintf("%s, node %s", nl, tsi)
			srcErr = cmn.NewAbortedErrorDetails(nl.Kind(), detail)
		}
		config := cmn.GCO.Get()
		args := &bcastArgs{
			req:     nl.AbortArgs(),
			network: cmn.NetworkIntraControl,
			timeout: config.Timeout.MaxKeepalive,
			nodes:   []cluster.NodeMap{nl.Notifiers()},
		}
		args.nodeCount = len(args.nodes[0])
		args.skipNodes = nl.FinNotifiers().Clone() // TODO: optimize
		n.p.bcastToNodesAsync(args)
	}
	if srcErr != nil {
		nl.SetErr(srcErr)
	}
	if nl.AllFinished() || aborted {
		n.fmu.Lock()
		n.fin[nl.UUID()] = nl
		n.fmu.Unlock()
		n.del(nl)
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
		if time.Duration(now-nl.EndTime()) > notifsRemoveMult*notifsHousekeepT {
			delete(n.fin, uuid)
		}
	}
	n.fmu.Unlock()

	if len(n.m) == 0 {
		return notifsHousekeepT
	}
	n.RLock()
	tempn := make(map[string]nl.NotifListener, len(n.m))
	for uuid, nl := range n.m {
		tempn[uuid] = nl
	}
	n.RUnlock()
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
		progressInterval = cmn.GCO.Get().Periodic.NotifTime
		done             bool
	)

	nl.RLock()
	uptoDateNodes, syncRequired := nl.NodesUptoDate(dur...)
	nl.RUnlock()
	if !syncRequired {
		return
	}

	args := &bcastArgs{
		network: cmn.NetworkIntraControl,
		timeout: cmn.GCO.Get().Timeout.MaxKeepalive,
	}

	// nodes to fetch stats from
	args.req = nl.QueryArgs()
	args.nodes = []cluster.NodeMap{nl.Notifiers()}
	args.skipNodes = uptoDateNodes
	args.nodeCount = len(args.nodes[0]) - len(args.skipNodes)
	debug.Assert(args.nodeCount > 0) // Ensure that there is at least one node to fetch.

	results := n.p.bcastToNodes(args)
	for res := range results {
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

	if done {
		nl.Callback(nl, nil, time.Now().UnixNano())
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

	if len(n.m) == 0 {
		return
	}

	var (
		remnl = make(map[string]nl.NotifListener)
		remid = make(cmn.SimpleKVs)
	)
	n.RLock()
	for uuid, nl := range n.m {
		nl.RLock()
		srcs := nl.Notifiers()
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
		nl.RUnlock()
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
		nl.Lock()
		nl.SetErr(err)
		nl.SetAborted()
		nl.Unlock()
		nl.Callback(nl, err, now)
	}
	n.fmu.Lock()
	for uuid, nl := range remnl {
		cmn.Assert(nl.UUID() == uuid)
		n.fin[uuid] = nl
	}
	n.fmu.Unlock()
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
func _mergeNLs(nls map[string]nl.NotifListener, msgs []*notifListenMsg) {
	for _, m := range msgs {
		if _, ok := nls[m.nl.UUID()]; !ok {
			nls[m.nl.UUID()] = m.nl
			m.nl.SetAddedTime()
		}
	}
}

func newNLMsg(nl nl.NotifListener) *notifListenMsg {
	return &notifListenMsg{nl: nl}
}

func (n *notifListenMsg) MarshalJSON() (data []byte, err error) {
	n.nl.RLock()
	defer n.nl.RUnlock()
	t := jsonNL{Type: n.nl.Kind()}
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
		n.nl = &query.NotifListenerQuery{}
	} else if isDLType(t.Type) {
		n.nl = &downloader.NotifDownloadListerner{}
	} else {
		n.nl = &xaction.NotifXactListener{}
	}
	err = jsoniter.Unmarshal(t.NL, &n.nl)
	if err != nil {
		return
	}
	return
}

func isDLType(t string) bool {
	return t == string(downloader.DlTypeMulti) ||
		t == string(downloader.DlTypeCloud) ||
		t == string(downloader.DlTypeSingle) ||
		t == string(downloader.DlTypeRange)
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
