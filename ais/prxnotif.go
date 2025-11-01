// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ext/dload"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"

	jsoniter "github.com/json-iterator/go"
)

// Notification "receiver"

const notifsName = "p-notifs"

const (
	shimcnt = 16
	shimask = shimcnt - 1
)

type (
	listeners struct {
		all [shimcnt]struct {
			sync.RWMutex
			m map[string]nl.Listener
		}
		l atomic.Int32 // current len across all maps
	}

	notifs struct {
		p   *proxy
		nls *listeners // running
		fin *listeners // finished

		added    []nl.Listener // reusable slice of `nl` to add to `nls`
		removed  []nl.Listener // reusable slice of `nl` to remove from `nls`
		finished []nl.Listener // reusable slice of `nl` to add to `fin`

		tempnl []nl.Listener

		smapVer int64
		mu      sync.Mutex
	}
	jsonNotifs struct {
		Running  []*notifListenMsg `json:"running"`
		Finished []*notifListenMsg `json:"finished"`
	}

	nlFilter xreg.Flt

	// receiver to start listening
	notifListenMsg struct {
		nl nl.Listener
	}
	jsonNL struct {
		Type string              `json:"type"`
		NL   jsoniter.RawMessage `json:"nl"`
	}
)

// interface guard
var (
	_ meta.Slistener = (*notifs)(nil)

	_ json.Marshaler   = (*notifs)(nil)
	_ json.Unmarshaler = (*notifs)(nil)

	_ json.Marshaler   = (*notifListenMsg)(nil)
	_ json.Unmarshaler = (*notifListenMsg)(nil)
)

////////////
// notifs //
////////////

func (n *notifs) init(p *proxy) {
	n.p = p
	n.nls = newListeners()
	n.fin = newListeners()

	n.added = make([]nl.Listener, 16)
	n.removed = make([]nl.Listener, 16)
	n.finished = make([]nl.Listener, 16)

	n.tempnl = make([]nl.Listener, 0, 16)

	hk.Reg(notifsName+hk.NameSuffix, n.housekeep, hk.Prune2mIval)
	n.p.Sowner().Listeners().Reg(n)

	// (see listeners.index)
	debug.Assert(shimcnt < 0xff)
}

// handle other nodes' notifications
// verb /v1/notifs/[progress|finished] - apc.Progress and apc.Finished, respectively
func (n *notifs) handler(w http.ResponseWriter, r *http.Request) {
	var (
		notifMsg = &core.NotifMsg{}
		nl       nl.Listener
		uuid     string
		tid      = r.Header.Get(apc.HdrSenderID) // sender node ID
	)
	if r.Method != http.MethodPost {
		cmn.WriteErr405(w, r, http.MethodPost)
		return
	}
	apiItems, err := n.p.parseURL(w, r, apc.URLPathNotifs.L, 1, false)
	if err != nil {
		return
	}

	if apiItems[0] != apc.Progress && apiItems[0] != apc.Finished {
		n.p.writeErrf(w, r, "Invalid route /notifs/%s", apiItems[0])
		return
	}
	if cmn.ReadJSON(w, r, notifMsg) != nil {
		return
	}

	// NOTE: the sender is asynchronous - ignores the response -
	// which is why we consider `not-found`, `already-finished`,
	// and `unknown-notifier` benign non-error conditions
	uuid = notifMsg.UUID
	if err := xact.CheckValidUUID(uuid); err != nil {
		n.p.writeErr(w, r, err)
		return
	}

	if !withRetry(cmn.Rom.CplaneOperation(), func() bool {
		nl = n.entry(uuid)
		return nl != nil
	}) {
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

	switch apiItems[0] {
	case apc.Progress:
		nl.Lock()
		n._progress(nl, tsi, notifMsg)
		nl.Unlock()
	case apc.Finished:
		n._finished(nl, tsi, notifMsg)
	} // default not needed - cannot happen
}

func (*notifs) _progress(nl nl.Listener, tsi *meta.Snode, msg *core.NotifMsg) {
	if msg.ErrMsg != "" {
		nl.AddErr(errors.New(msg.ErrMsg))
	}
	// when defined, `data must be valid encoded stats
	if msg.Data != nil {
		stats, _, _, err := nl.UnmarshalStats(msg.Data)
		debug.AssertNoErr(err)
		nl.SetStats(tsi.ID(), stats)
	}
}

func (n *notifs) _finished(nl nl.Listener, tsi *meta.Snode, msg *core.NotifMsg) {
	var (
		srcErr  error
		done    bool
		aborted = msg.AbortedX
	)
	nl.Lock()
	if msg.Data != nil {
		// ditto
		stats, _, abortedSnap, err := nl.UnmarshalStats(msg.Data)
		if err != nil {
			// (unlikely)
			nlog.Errorln("bad finished stats for", nl.String(), "from", tsi.StringEx(), "err:", err)
		} else {
			nl.SetStats(tsi.ID(), stats)

			if abortedSnap != msg.AbortedX && cmn.Rom.V(4, cos.ModAIS) {
				nlog.Infoln("Warning:", msg.String(), "aborted", abortedSnap, "vs", msg.AbortedX, nl.String())
			}
			aborted = aborted || abortedSnap
		}
	}
	if msg.ErrMsg != "" {
		srcErr = errors.New(msg.ErrMsg)
	}
	done = n.markFinished(nl, tsi, srcErr, aborted)
	nl.Unlock()

	if done {
		n.done(nl)
	}
}

// start listening
func (n *notifs) add(nl nl.Listener) error {
	if err := xact.CheckValidUUID(nl.UUID()); err != nil {
		// unlikely
		debug.AssertNoErr(err)
		return err
	}
	if nl.ActiveCount() == 0 {
		return fmt.Errorf("cannot add %q with no active notifiers", nl)
	}
	if exists := n.nls.add(nl, false /*locked*/); exists {
		return nil
	}
	nl.SetAddedTime()
	if cmn.Rom.V(5, cos.ModAIS) {
		nlog.Infoln("add", nl.Name())
	}
	return nil
}

func (n *notifs) del(nl nl.Listener, locked bool) (ok bool) {
	ok = n.nls.del(nl, locked /*locked*/)
	if ok && cmn.Rom.V(5, cos.ModAIS) {
		nlog.Infoln("del", nl.Name())
	}
	return
}

func (n *notifs) entry(uuid string) nl.Listener {
	entry, exists := n.nls.entry(uuid)
	if exists {
		return entry
	}
	entry, exists = n.fin.entry(uuid)
	if exists {
		return entry
	}
	return nil
}

func (n *notifs) find(flt nlFilter) (nl nl.Listener) {
	if flt.ID != "" {
		return n.entry(flt.ID)
	}
	nl = n.nls.find(flt)
	if nl != nil || (flt.OnlyRunning != nil && *flt.OnlyRunning) {
		return nl
	}
	nl = n.fin.find(flt)
	return nl
}

func (n *notifs) findAll(flt nlFilter) (nls []nl.Listener) {
	if flt.ID != "" {
		if nl := n.entry(flt.ID); nl != nil {
			nls = append(nls, nl)
		}
		return
	}
	nls = n.nls.findAll(flt)
	if flt.OnlyRunning != nil && *flt.OnlyRunning {
		return
	}
	if s2 := n.fin.findAll(flt); len(s2) > 0 {
		nls = append(nls, s2...)
	}
	return
}

func (n *notifs) size() int32 {
	return n.nls.l.Load() + n.fin.l.Load()
}

// PRECONDITION: `nl` should be under lock.
func (*notifs) markFinished(nl nl.Listener, tsi *meta.Snode, srcErr error, aborted bool) (done bool) {
	nl.MarkFinished(tsi)
	if aborted {
		nl.SetAborted()
		if srcErr == nil {
			detail := fmt.Sprintf("%s from %s", nl, tsi.StringEx())
			srcErr = cmn.NewErrAborted(nl.String(), detail, nil)
		}
	}
	if srcErr != nil {
		nl.AddErr(srcErr)
	}
	return nl.ActiveCount() == 0 || aborted
}

func (n *notifs) done(nl nl.Listener) {
	if !n.del(nl, false /*locked*/) {
		// `nl` already removed from active map
		return
	}
	n.fin.add(nl, false /*locked*/)

	if nl.Aborted() {
		smap := n.p.owner.smap.get()
		// abort via primary to eliminate redundant intra-cluster messaging-and-handling
		// TODO: confirm & load-balance
		doSend := true
		if smap.Primary != nil { // nil in unit tests
			debug.Assert(n.p.SID() != smap.Primary.ID() || smap.IsPrimary(n.p.si))
			doSend = smap.IsPrimary(n.p.si) ||
				!smap.IsIC(smap.Primary) // never happens but ok
		}
		if doSend {
			// NOTE: we accept finished notifications even after
			// `nl` is aborted. Handle locks carefully.
			args := allocBcArgs()
			args.req = abortReq(nl)
			args.network = cmn.NetIntraControl
			args.timeout = cmn.Rom.MaxKeepalive()
			args.nodes = []meta.NodeMap{nl.Notifiers()}
			args.nodeCount = len(args.nodes[0])
			args.smap = smap
			args.async = true
			_ = n.p.bcastNodes(args) // args.async: result is already discarded/freed
			freeBcArgs(args)
		}
	}
	// may race vs notifs.apply (benign)
	nl.Callback(nl, time.Now().UnixNano())
}

func abortReq(nl nl.Listener) cmn.HreqArgs {
	if _, ok := nl.(*dload.NotifDownloadListerner); ok {
		// HACK
		// - download _job_ vs download xaction - see dload.NewDownloadNL()
		// - downloader implements abort via http.MethodDelete and uses different messaging
		return dload.AbortReq(nl.UUID() /*job ID*/)
	}
	msg := apc.ActMsg{
		Action: apc.ActXactStop,
		Name:   cmn.ErrXactICNotifAbort.Error(),
		Value:  xact.ArgsMsg{ID: nl.UUID() /*xid*/, Kind: nl.Kind()},
	}
	args := cmn.HreqArgs{Method: http.MethodPut}
	args.Body = cos.MustMarshal(msg)
	args.Path = apc.URLPathXactions.S
	return args
}

//
// housekeeping
//

const maxNotifsHK = time.Second >> 2

func (n *notifs) housekeep(now int64) time.Duration {
	n.fin.wlockAll()
	for i := range shimcnt {
		for uuid, nl := range n.fin.all[i].m {
			timeout := cos.Ternary(nl.Kind() == apc.ActList, hk.OldAgeNotifLso, hk.OldAgeNotif)
			if time.Duration(now-nl.EndTime()) > timeout {
				delete(n.fin.all[i].m, uuid)
				n.fin.l.Dec()
			}
		}
	}
	n.fin.wunlockAll()

	if n.nls.l.Load() == 0 {
		return hk.Jitter(hk.Prune2mIval, now)
	}

	if elapsed := mono.Since(now); elapsed > maxNotifsHK {
		return hk.Jitter(hk.Prune2mIval>>1, now)
	}

	n.tempnl = n.tempnl[:0]
	for i := range shimcnt {
		sh := &n.nls.all[i]
		sh.RLock()
		for _, nl := range sh.m {
			n.tempnl = append(n.tempnl, nl)
		}
		sh.RUnlock()
	}
	if cap(n.tempnl) > 2*len(n.tempnl) {
		// clip cap
		n.tempnl = cos.ResetSliceCap(n.tempnl, len(n.tempnl))
	}
	nexthk := hk.Prune2mIval
	for _, nl := range n.tempnl {
		n.bcastGetStats(nl, hk.Prune2mIval)
		if mono.Since(now) > maxNotifsHK {
			nexthk = hk.Prune2mIval >> 1
			break
		}
	}
	// cleanup temp cloned notifs
	clear(n.tempnl)

	return hk.Jitter(nexthk, now)
}

// conditional: query targets iff they delayed updating
func (n *notifs) bcastGetStats(nl nl.Listener, dur time.Duration) {
	var (
		config           = cmn.GCO.Get()
		progressInterval = config.Periodic.NotifTime.D()
		done             bool
	)
	nl.RLock()
	nodesTardy, syncRequired := nl.NodesTardy(dur)
	nl.RUnlock()
	if !syncRequired {
		return
	}
	args := allocBcArgs()
	args.network = cmn.NetIntraControl
	args.timeout = config.Timeout.MaxHostBusy.D()
	args.req = nl.QueryArgs() // nodes to fetch stats from
	args.nodes = []meta.NodeMap{nodesTardy}
	args.nodeCount = len(args.nodes[0])
	args.smap = n.p.owner.smap.get()
	debug.Assert(args.nodeCount > 0) // Ensure that there is at least one node to fetch.

	results := n.p.bcastNodes(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err == nil {
			stats, finished, aborted, err := nl.UnmarshalStats(res.bytes)
			if err != nil {
				nlog.Errorf("%s: failed to parse stats from %s: %v", n.p, res.si.StringEx(), err)
				continue
			}
			nl.Lock()
			if finished {
				done = done || n.markFinished(nl, res.si, nil, aborted)
			}
			nl.SetStats(res.si.ID(), stats)
			nl.Unlock()
			continue
		}
		// err
		if res.status == http.StatusNotFound {
			if mono.Since(nl.AddedTime()) < progressInterval {
				// likely didn't start yet - skipping
				continue
			}
			err := fmt.Errorf("%s: %s not found at %s", n.p.si, nl, res.si.StringEx())
			nl.Lock()
			done = done || n.markFinished(nl, res.si, err, true) // NOTE: not-found at one ==> all done
			nl.Unlock()
		} else if cmn.Rom.V(4, cos.ModAIS) {
			nlog.Errorln(n.p.String(), nl.String(), "node", res.si.StringEx(), res.unwrap())
		}
	}
	freeBcastRes(results)
	if done {
		n.done(nl)
	}
}

func (n *notifs) getOwner(uuid string) (o string, exists bool) {
	var nl nl.Listener
	if nl = n.entry(uuid); nl != nil {
		exists = true
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

	if n.nls.l.Load() == 0 {
		return
	}
	var (
		remnl map[string]nl.Listener
		remid cos.StrKVs
	)

	n.nls.rlockAll()
	for i := range shimcnt {
		for uuid, nl := range n.nls.all[i].m {
			nl.RLock()
			for sid := range nl.ActiveNotifiers() {
				if node := smap.GetActiveNode(sid); node == nil {
					if remnl == nil {
						remnl, remid = _remini()
					}
					remnl[uuid] = nl
					remid[uuid] = sid
					break
				}
			}
			nl.RUnlock()
		}
	}
	n.nls.runlockAll()

	if len(remnl) == 0 {
		return
	}
	now := time.Now().UnixNano()

repeat:
	for uuid, nl := range remnl {
		sid := remid[uuid]
		if nl.Kind() == apc.ActRebalance && nl.Cause() != "" { // for the cause, see ais/rebmeta
			nlog.Infof("Warning: %s: %s is out, ignore 'smap-changed'", nl.String(), sid)
			delete(remnl, uuid)
			goto repeat
		}
		err := &errNodeNotFound{n.p.si, smap, "abort " + nl.String() + " via 'smap-changed':", sid}
		nl.Lock()
		nl.AddErr(err)
		nl.SetAborted()
		nl.Unlock()
	}
	if len(remnl) == 0 {
		return
	}

	// cleanup and callback w/ nl.Err
	n.fin.wlockAll()
	for uuid, nl := range remnl {
		debug.Assert(nl.UUID() == uuid)
		n.fin.add(nl, true /*locked*/)
	}
	n.fin.wunlockAll()

	n.nls.wlockAll()
	for _, nl := range remnl {
		n.del(nl, true /*locked*/)
	}
	n.nls.wunlockAll()

	for _, nl := range remnl {
		nl.Callback(nl, now)
	}
	// cleanup
	clear(remnl)
	clear(remid)
}

func _remini() (map[string]nl.Listener, cos.StrKVs) {
	return make(map[string]nl.Listener, 1), make(cos.StrKVs, 1)
}

func (n *notifs) MarshalJSON() ([]byte, error) {
	if n.size() == 0 {
		return nil, nil
	}
	t := jsonNotifs{}

	n.nls.rlockAll()
	n.fin.rlockAll()

	nlsSize := 0
	finSize := 0
	for i := range shimcnt {
		nlsSize += len(n.nls.all[i].m)
		finSize += len(n.fin.all[i].m)
	}
	t.Running = make([]*notifListenMsg, 0, nlsSize)
	t.Finished = make([]*notifListenMsg, 0, finSize)

	for i := range shimcnt {
		for _, nl := range n.nls.all[i].m {
			t.Running = append(t.Running, newNLMsg(nl))
		}
	}
	n.nls.runlockAll()

	for i := range shimcnt {
		for _, nl := range n.fin.all[i].m {
			t.Finished = append(t.Finished, newNLMsg(nl))
		}
	}
	n.fin.runlockAll()

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
	n.apply(&t)
	n.mu.Unlock()
	return
}

// Identify the diff in ownership table and populate `added`, `removed` and `finished` slices
// (under lock)
func (n *notifs) apply(t *jsonNotifs) {
	added, removed, finished := n.added[:0], n.removed[:0], n.finished[:0]

	n.nls.rlockAll()
	n.fin.rlockAll()
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
	n.fin.runlockAll()
	n.nls.runlockAll()

	if len(removed) == 0 && len(added) == 0 {
		goto fin
	}

	// Add/Remove `nl` - `n.nls`.
	n.nls.wlockAll()
	for _, nl := range added {
		n.nls.add(nl, true /*locked*/)
	}
	for _, nl := range removed {
		n.nls.del(nl, true /*locked*/)
	}
	n.nls.wunlockAll()

fin:
	if len(finished) == 0 {
		return
	}

	n.fin.wlockAll()
	// Add `nl` to `n.fin`.
	for _, nl := range finished {
		n.fin.add(nl, true /*locked*/)
	}
	n.fin.wunlockAll()

	// Call the Callback for each `nl` marking it finished.
	// (may race vs notifs.done (benign))
	now := time.Now().UnixNano()
	for _, nl := range finished {
		nl.Callback(nl, now)
	}
}

func (n *notifs) String() string {
	// not r-locking
	var nlsSize, finSize int
	for i := range shimcnt {
		nlsSize += len(n.nls.all[i].m)
		finSize += len(n.fin.all[i].m)
	}
	return fmt.Sprintf("%s (nls=%d, fin=%d)", notifsName, nlsSize, finSize)
}

///////////////
// listeners //
///////////////

func newListeners() *listeners {
	l := &listeners{}
	for i := range shimcnt {
		l.all[i].m = make(map[string]nl.Listener, 64)
	}
	return l
}

// index returns the idx index for a given UUID using the last 4 bits
func (*listeners) index(uuid string) int {
	return int(uuid[len(uuid)-1]) & shimask
}

func (l *listeners) rlockAll() {
	for i := range shimcnt {
		l.all[i].RLock()
	}
}

func (l *listeners) runlockAll() {
	for i := range shimcnt {
		l.all[i].RUnlock()
	}
}

func (l *listeners) wlockAll() {
	for i := range shimcnt {
		l.all[i].Lock()
	}
}

func (l *listeners) wunlockAll() {
	for i := range shimcnt {
		l.all[i].Unlock()
	}
}

func (l *listeners) entry(uuid string) (entry nl.Listener, exists bool) {
	idx := l.index(uuid)
	l.all[idx].RLock()
	entry, exists = l.all[idx].m[uuid]
	l.all[idx].RUnlock()
	return entry, exists
}

func (l *listeners) add(nl nl.Listener, locked bool) (exists bool) {
	uuid := nl.UUID()
	idx := l.index(uuid)

	if !locked {
		l.all[idx].Lock()
	}
	if _, exists = l.all[idx].m[uuid]; !exists {
		l.all[idx].m[uuid] = nl
		a := l.l.Inc()
		debug.Assert(int(a) > 0)
	}
	if !locked {
		l.all[idx].Unlock()
	}
	return exists
}

func (l *listeners) del(nl nl.Listener, locked bool) (ok bool) {
	uuid := nl.UUID()
	idx := l.index(uuid)

	if !locked {
		l.all[idx].Lock()
	}
	if _, ok = l.all[idx].m[uuid]; ok {
		delete(l.all[idx].m, uuid)
		a := l.l.Dec()
		debug.Assert(int(a) >= 0)
	}
	if !locked {
		l.all[idx].Unlock()
	}
	return ok
}

// is always called under lock
func (l *listeners) exists(uuid string) (ok bool) {
	idx := l.index(uuid)
	_, ok = l.all[idx].m[uuid]
	return ok
}

// Returns a listener that matches the filter condition.
// - returns the first one that's still running, if exists
// - otherwise, returns the one that finished most recently
// (compare with the below)
func (l *listeners) find(flt nlFilter) (nl nl.Listener) {
	var ftime int64

	l.rlockAll()
	for i := range shimcnt {
		for _, listener := range l.all[i].m {
			if !flt.match(listener) {
				continue
			}
			et := listener.EndTime()
			if ftime != 0 && et < ftime {
				debug.Assert(listener.Finished())
				continue
			}
			nl = listener
			if !listener.Finished() {
				break
			}
			ftime = et
		}
	}
	l.runlockAll()
	return
}

// returns all matches
func (l *listeners) findAll(flt nlFilter) (nls []nl.Listener) {
	l.rlockAll()
	for i := range shimcnt {
		for _, listener := range l.all[i].m {
			if flt.match(listener) {
				nls = append(nls, listener)
			}
		}
	}
	l.runlockAll()
	return
}

////////////////////
// notifListenMsg //
////////////////////

func newNLMsg(nl nl.Listener) *notifListenMsg {
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
	if dload.IsType(t.Type) {
		n.nl = &dload.NotifDownloadListerner{}
	} else {
		n.nl = &xact.NotifXactListener{}
	}
	return jsoniter.Unmarshal(t.NL, &n.nl)
}

//
// Notification listener filter (nlFilter)
//

func (nf *nlFilter) match(nl nl.Listener) bool {
	if nl.UUID() == nf.ID {
		return true
	}
	if nf.Kind == "" || nl.Kind() == nf.Kind {
		if nf.Bck == nil || nf.Bck.IsEmpty() {
			return true
		}
		for _, bck := range nl.Bcks() {
			qbck := (*cmn.QueryBcks)(nf.Bck.Bucket())
			if qbck.Contains(bck) {
				return true
			}
		}
	}
	return false
}

// yet another call-retrying utility (TODO: unify)

func withRetry(timeout time.Duration, cond func() bool) (ok bool) {
	sleep := cos.ProbingFrequency(timeout)
	for elapsed := time.Duration(0); elapsed < timeout; elapsed += sleep {
		if ok = cond(); ok {
			break
		}
		time.Sleep(sleep)
	}
	return
}
