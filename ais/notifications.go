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
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/xaction"
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
	notifDownload
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
	notifCallback func(n notifListener, err error)
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

	nlFilter xaction.RegistryXactFilter

	notifListener interface {
		callback(n notifListener, err error, timestamp int64)
		unmarshalStats(rawMsg []byte) (interface{}, bool, bool, error)
		lock()
		unlock()
		rlock()
		runlock()
		notifiers() cluster.NodeMap
		finNotifiers() cmn.StringSet
		notifTy() int
		kind() string
		bcks() []cmn.Bck
		setErr(error)
		err() error
		UUID() string
		setAborted()
		aborted() bool
		status() *cmn.XactStatus
		setStats(daeID string, stats interface{})
		nodeStats() *sync.Map
		bcastArgs() *bcastArgs
		abortArgs() *bcastArgs
		finTime() int64
		hasFinished(node *cluster.Snode) bool
		markFinished(node *cluster.Snode)
		finCount() int
		finished() bool
		String() string
		getOwner() string
		setOwner(string)
		hrwOwner(smap *smapX)
		lastUpdated(si *cluster.Snode) int64
		progressInterval() int64
		nodesUptoDate(intervals ...int64) (cmn.StringSet, bool)
	}

	// Note: variables capitablized to allow marshal and unmarshal
	notifListenerBase struct {
		sync.RWMutex
		Common struct {
			UUID   string
			Action string // async operation kind (see cmn/api_const.go)

			// ownership
			Owned       string // "": not owned | equalIC: IC | otherwise, pid + IC
			SmapVersion int64  // smap version in which NL is added

			Bck []cmn.Bck
			Ty  int // notification category (above)
		}

		Srcs             cluster.NodeMap  // expected notifiers
		FinSrcs          cmn.StringSet    // daeID of notifiers finished
		f                notifCallback    // optional listening-side callback
		Stats            *sync.Map        // [daeID => Stats (e.g. cmn.BaseXactStatsExt)]
		LastUpdated      map[string]int64 // [daeID => last update time]
		ProgressInterval int64            // time in secs at which progress is monitored

		ErrMsg   string        // ErrMsg
		FinTime  atomic.Int64  // timestamp when finished
		Aborted  atomic.Bool   // sets if the xaction is aborted
		ErrCount atomic.Uint32 // number of error encountered
	}

	notifXactBase struct {
		notifListenerBase
	}

	notifDownloadBase struct {
		notifListenerBase
	}

	//
	// notification messages
	//

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
	_ notifListener     = &notifXactBase{}
	_ notifListener     = &notifDownloadBase{}
	_ cluster.Slistener = &notifs{}
)

func newNLB(uuid string, smap *smapX, srcs cluster.NodeMap, ty int, action string, progressInterval int64,
	bck ...cmn.Bck) *notifListenerBase {
	cmn.Assert(ty > notifInvalid)
	cmn.Assert(len(srcs) != 0)
	nlb := &notifListenerBase{
		Srcs:             srcs,
		Stats:            &sync.Map{},
		ProgressInterval: progressInterval,
		LastUpdated:      make(map[string]int64, len(srcs)),
	}
	nlb.Common.UUID = uuid
	nlb.Common.Action = action
	nlb.Common.SmapVersion = smap.version()
	nlb.Common.Bck = bck
	nlb.Common.Ty = ty
	nlb.FinSrcs = make(cmn.StringSet, len(srcs))
	return nlb
}

func newXactNL(uuid string, smap *smapX, srcs cluster.NodeMap, ty int, action string, bck ...cmn.Bck) *notifXactBase {
	return &notifXactBase{
		notifListenerBase: *newNLB(uuid, smap, srcs, ty, action, 0, bck...),
	}
}

func newDownloadNL(uuid string, smap *smapX, srcs cluster.NodeMap, action string, progressInterval int64, bck ...cmn.Bck) *notifDownloadBase {
	return &notifDownloadBase{
		notifListenerBase: *newNLB(uuid, smap, srcs, notifDownload, action, progressInterval, bck...),
	}
}

///////////////////////
// notifListenerBase //
///////////////////////

// is called after all notifiers will have notified OR on failure (err != nil)
func (nlb *notifListenerBase) callback(nl notifListener, err error, timestamp int64) {
	if nlb.FinTime.CAS(0, 1) {
		// is optional
		if nlb.f != nil {
			nlb.f(nl, err) // invoke user-supplied callback and pass user-supplied notifListener
		}
		nlb.FinTime.Store(timestamp)
	}
}
func (nlb *notifListenerBase) lock()                       { nlb.Lock() }
func (nlb *notifListenerBase) unlock()                     { nlb.Unlock() }
func (nlb *notifListenerBase) rlock()                      { nlb.RLock() }
func (nlb *notifListenerBase) runlock()                    { nlb.RUnlock() }
func (nlb *notifListenerBase) notifiers() cluster.NodeMap  { return nlb.Srcs }
func (nlb *notifListenerBase) finNotifiers() cmn.StringSet { return nlb.FinSrcs }
func (nlb *notifListenerBase) notifTy() int                { return nlb.Common.Ty }
func (nlb *notifListenerBase) UUID() string                { return nlb.Common.UUID }
func (nlb *notifListenerBase) aborted() bool               { return nlb.Aborted.Load() }
func (nlb *notifListenerBase) setAborted()                 { nlb.Aborted.CAS(false, true) }
func (nlb *notifListenerBase) finTime() int64              { return nlb.FinTime.Load() }
func (nlb *notifListenerBase) finished() bool              { return nlb.finTime() > 0 }
func (nlb *notifListenerBase) setErr(err error) {
	ecount := nlb.ErrCount.Inc()
	if ecount == 1 {
		nlb.ErrMsg = err.Error()
	}
}

func (nlb *notifListenerBase) err() error {
	if nlb.ErrMsg == "" {
		return nil
	}
	if l := nlb.ErrCount.Load(); l > 1 {
		return fmt.Errorf("%s... (error-count=%d)", nlb.ErrMsg, l)
	}
	return errors.New(nlb.ErrMsg)
}

func (nlb *notifListenerBase) setStats(daeID string, stats interface{}) {
	_, ok := nlb.Srcs[daeID]
	cmn.Assert(ok)
	nlb.Stats.Store(daeID, stats)
	nlb.LastUpdated[daeID] = mono.NanoTime()
}

func (nlb *notifListenerBase) lastUpdated(si *cluster.Snode) int64 {
	return nlb.LastUpdated[si.DaemonID]
}
func (nlb *notifListenerBase) progressInterval() int64 {
	return nlb.ProgressInterval
}
func (nlb *notifListenerBase) nodeStats() *sync.Map {
	return nlb.Stats
}

func (nlb *notifListenerBase) nodesUptoDate(intervals ...int64) (uptoDate cmn.StringSet, tardy bool) {
	var (
		period = int64(cmn.GCO.Get().Periodic.NotifTime.Seconds())
	)
	uptoDate = cmn.NewStringSet()

	if len(intervals) > 0 {
		period = intervals[0]
	} else if nlb.progressInterval() != 0 {
		period = nlb.progressInterval()
	}

	for _, si := range nlb.notifiers() {
		ts := nlb.lastUpdated(si)
		diff := int64(mono.Since(ts).Seconds())
		if _, ok := nlb.Stats.Load(si.ID()); ok && (nlb.hasFinished(si) || diff < period) {
			uptoDate.Add(si.DaemonID)
			continue
		}
		tardy = true
	}
	return
}

func (nlb *notifListenerBase) status() *cmn.XactStatus {
	status := &cmn.XactStatus{
		UUID:     nlb.UUID(),
		FinTime:  nlb.FinTime.Load(),
		AbortedX: nlb.aborted(),
	}
	if err := nlb.err(); err != nil {
		status.ErrMsg = err.Error()
	}
	return status
}

func (nlb *notifListenerBase) String() string {
	var (
		tm, res  string
		hdr      = fmt.Sprintf("%s-%q", notifText(nlb.notifTy()), nlb.UUID())
		finCount = nlb.finCount()
	)
	if tfin := nlb.FinTime.Load(); tfin > 0 {
		if l := nlb.ErrCount.Load(); l > 0 {
			res = fmt.Sprintf("-fail(error-count=%d)", l)
		}
		tm = cmn.FormatTimestamp(time.Unix(0, tfin))
		return fmt.Sprintf("%s-%s%s", hdr, tm, res)
	}
	if finCount > 0 {
		return fmt.Sprintf("%s-%d/%d", hdr, finCount, len(nlb.Srcs))
	}
	return hdr
}

func (nlb *notifListenerBase) getOwner() string  { return nlb.Common.Owned }
func (nlb *notifListenerBase) setOwner(o string) { nlb.Common.Owned = o }
func (nlb *notifListenerBase) kind() string      { return nlb.Common.Action }
func (nlb *notifListenerBase) bcks() []cmn.Bck   { return nlb.Common.Bck }

// effectively, cache owner
func (nlb *notifListenerBase) hrwOwner(smap *smapX) {
	psiOwner, err := cluster.HrwIC(&smap.Smap, nlb.UUID())
	cmn.AssertNoErr(err) // TODO -- FIXME: handle
	nlb.setOwner(psiOwner.ID())
}

func (nlb *notifListenerBase) hasFinished(node *cluster.Snode) bool {
	return nlb.FinSrcs.Contains(node.ID())
}

func (nlb *notifListenerBase) markFinished(node *cluster.Snode) {
	if nlb.FinSrcs == nil {
		nlb.FinSrcs = make(cmn.StringSet)
	}
	nlb.FinSrcs.Add(node.ID())
}

func (nlb *notifListenerBase) finCount() int {
	return len(nlb.FinSrcs)
}

///////////////////
// notifXactBase //
///////////////////

func (nxb *notifXactBase) unmarshalStats(rawMsg []byte) (stats interface{}, finished, aborted bool, err error) {
	xactStats := &cmn.BaseXactStatsExt{}
	if err = jsoniter.Unmarshal(rawMsg, xactStats); err != nil {
		return
	}
	stats = xactStats
	aborted = xactStats.Aborted()
	finished = xactStats.Finished()
	return
}

func (nxb *notifXactBase) bcastArgs() *bcastArgs {
	args := &bcastArgs{
		req:     cmn.ReqArgs{Method: http.MethodGet, Query: make(url.Values, 2)},
		network: cmn.NetworkIntraControl,
		timeout: cmn.GCO.Get().Timeout.MaxKeepalive,
	}
	args.req.Query.Set(cmn.URLParamWhat, cmn.GetWhatXactStats)
	args.req.Query.Set(cmn.URLParamUUID, nxb.UUID())
	args.req.Path = cmn.JoinWords(cmn.Version, cmn.Xactions)
	return args
}

func (nxb *notifXactBase) abortArgs() *bcastArgs {
	msg := cmn.ActionMsg{
		Action: cmn.ActXactStop,
		Value: cmn.XactReqMsg{
			ID: nxb.UUID(),
		},
	}
	args := &bcastArgs{
		req:     cmn.ReqArgs{Method: http.MethodPut},
		network: cmn.NetworkIntraControl,
		timeout: cmn.GCO.Get().Timeout.MaxKeepalive,
	}
	args.req.Body = cmn.MustMarshal(msg)
	args.req.Path = cmn.JoinWords(cmn.Version, cmn.Cluster)
	return args
}

///////////////////////
// notifDownloadBase //
///////////////////////

func (nd *notifDownloadBase) unmarshalStats(data []byte) (stats interface{}, finished, aborted bool, err error) {
	dlStatus := &downloader.DlStatusResp{}
	if err = jsoniter.Unmarshal(data, dlStatus); err != nil {
		return
	}
	stats = dlStatus
	aborted = dlStatus.Aborted
	finished = dlStatus.JobFinished()
	return
}

func (nd *notifDownloadBase) bcastArgs() *bcastArgs {
	args := &bcastArgs{
		req:     cmn.ReqArgs{Method: http.MethodGet},
		network: cmn.NetworkIntraControl,
		timeout: cmn.GCO.Get().Timeout.MaxKeepalive,
	}
	dlBody := downloader.DlAdminBody{
		ID: nd.UUID(),
	}
	args.req.Path = cmn.JoinWords(cmn.Version, cmn.Download)
	args.req.Body = cmn.MustMarshal(dlBody)
	return args
}

func (nd *notifDownloadBase) abortArgs() *bcastArgs {
	args := &bcastArgs{
		req:     cmn.ReqArgs{Method: http.MethodDelete},
		network: cmn.NetworkIntraControl,
		timeout: cmn.GCO.Get().Timeout.MaxKeepalive,
	}
	dlBody := downloader.DlAdminBody{
		ID: nd.UUID(),
	}
	args.req.Path = cmn.JoinWords(cmn.Version, cmn.Download, cmn.Abort)
	args.req.Body = cmn.MustMarshal(dlBody)
	return args
}

////////////
// notifs //
////////////

func (n *notifs) init(p *proxyrunner) {
	n.p = p
	n.m = make(map[string]notifListener, 64)
	n.fin = make(map[string]notifListener, 64)
	hk.Reg(notifsName+".gc", n.housekeep, notifsHousekeepT)
	n.p.Sowner().Listeners().Reg(n)
}

func (n *notifs) String() string { return notifsName }

// start listening
func (n *notifs) add(nl notifListener) {
	cmn.Assert(nl.UUID() != "")
	cmn.Assert(nl.notifTy() > notifInvalid)
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

func (n *notifs) find(flt nlFilter) (nl notifListener, exists bool) {
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
func _findNL(nls map[string]notifListener, flt nlFilter) (nl notifListener, exists bool) {
	var ftime int64
	for _, listener := range nls {
		if listener.finTime() < ftime {
			continue
		}
		if flt.match(listener) {
			ftime = listener.finTime()
			nl, exists = listener, true
		}
		if exists && !listener.finished() {
			return
		}
	}
	return
}

// verb /v1/notifs/[progress|finished]
func (n *notifs) handler(w http.ResponseWriter, r *http.Request) {
	var (
		notifMsg = &cmn.NotifMsg{}
		nl       notifListener
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
	withLocalRetry(func() bool {
		nl, exists = n.entry(uuid)
		return exists
	})

	if !exists {
		n.p.invalmsghdlrstatusf(w, r, http.StatusNotFound, "%s: unknown UUID %q", n.p.si, uuid)
		return
	}
	if nl.finished() {
		s := fmt.Sprintf("%s: %q already finished", n.p.si, uuid)
		n.p.invalmsghdlrsilent(w, r, s, http.StatusGone)
		return
	}

	var (
		srcs    = nl.notifiers()
		tsi, ok = srcs[tid]
	)
	if !ok {
		n.p.invalmsghdlrstatusf(w, r, http.StatusNotFound, "%s: %s from unknown node %s", n.p.si, nl, tid)
		return
	}
	//
	// notifListener and notifMsg must have the same type
	//
	cmn.Assert(nl.notifTy() == int(notifMsg.Ty))
	nl.rlock()
	if nl.hasFinished(tsi) {
		n.p.invalmsghdlrsilent(w, r, fmt.Sprintf("%s: duplicate %s from node %s", n.p.si, nl, tid))
		nl.runlock()
		return
	}
	nl.runlock()

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

func (n *notifs) handleProgress(nl notifListener, tsi *cluster.Snode, data []byte, srcErr error) error {
	nl.lock()
	defer nl.unlock()

	if srcErr != nil {
		nl.setErr(srcErr)
	} else {
		stats, _, _, err := nl.unmarshalStats(data)
		if err != nil {
			return err
		}
		nl.setStats(tsi.ID(), stats)
	}
	return nil
}

func (n *notifs) handleFinished(nl notifListener, tsi *cluster.Snode, data []byte, srcErr error) error {
	nl.lock()
	defer nl.unlock()

	if srcErr != nil {
		n.markFinished(nl, tsi, false, srcErr)
	} else {
		stats, _, aborted, err := nl.unmarshalStats(data)
		if err != nil {
			return err
		}
		nl.setStats(tsi.ID(), stats)
		n.markFinished(nl, tsi, aborted, srcErr)
	}
	return nil
}

// PRECONDITION: `nl` should be under lock.
func (n *notifs) markFinished(nl notifListener, tsi *cluster.Snode, aborted bool, srcErr error) {
	nl.markFinished(tsi)

	if aborted {
		nl.setAborted()
		if srcErr == nil {
			detail := fmt.Sprintf("%s, node %s", nl, tsi)
			srcErr = cmn.NewAbortedErrorDetails(nl.kind(), detail)
		}

		// TODO: implement a better async broadcast for abort
		args := nl.abortArgs()
		args.nodes = []cluster.NodeMap{nl.notifiers()}
		args.nodeCount = len(args.nodes[0])
		args.skipNodes = nl.finNotifiers().Clone()
		n.p.bcastToNodesAsync(args)
	}
	if srcErr != nil {
		nl.setErr(srcErr)
	}

	if nl.finCount() == len(nl.notifiers()) || aborted {
		nl.callback(nl, nil, time.Now().UnixNano())
		n.fmu.Lock()
		n.fin[nl.UUID()] = nl
		n.fmu.Unlock()
		n.del(nl)
	}
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
	for _, nl := range tempn {
		n.syncStats(nl, int64(notifsHousekeepT.Seconds()))
	}
	// cleanup temp cloned notifs
	for u := range tempn {
		delete(tempn, u)
	}
	return notifsHousekeepT
}

func (n *notifs) syncStats(nl notifListener, intervals ...int64 /*secs*/) {
	nl.rlock()
	uptoDateNodes, syncRequired := nl.nodesUptoDate(intervals...)
	nl.runlock()
	if !syncRequired {
		return
	}

	args := nl.bcastArgs()
	args.nodes = []cluster.NodeMap{nl.notifiers()} // Nodes to fetch stats from.
	args.skipNodes = uptoDateNodes
	args.nodeCount = len(args.nodes[0]) - len(args.skipNodes)
	debug.Assert(args.nodeCount > 0) // Ensure that there is at least one node to fetch.

	results := n.p.bcastToNodes(args)
	for res := range results {
		if res.err == nil {
			stats, finished, aborted, err := nl.unmarshalStats(res.bytes)
			if err != nil {
				glog.Errorf("%s: failed to parse stats from %s (err: %s)", n.p.si, res.si, err)
				continue
			}
			nl.lock()
			if finished {
				n.markFinished(nl, res.si, aborted, nil)
			}
			nl.setStats(res.si.ID(), stats)
			nl.unlock()
		} else if res.status == http.StatusNotFound { // consider silent done at res.si
			err := fmt.Errorf("%s: %s not found at %s", n.p.si, nl, res.si)
			nl.lock()
			n.markFinished(nl, res.si, true, err) // NOTE: not-found at one ==> all done
			nl.unlock()
		} else if glog.FastV(4, glog.SmoduleAIS) {
			glog.Errorf("%s: %s, node %s, err: %v", n.p.si, nl, res.si, res.err)
		}
	}
}

// Return stats from each node for a given UUID.
func (n *notifs) queryStats(uuid string, intervals ...int64 /*seconds*/) (stats *sync.Map, exists bool) {
	var (
		nl notifListener
	)
	nl, exists = n.entry(uuid)
	if !exists {
		return
	}
	n.syncStats(nl, intervals...)
	stats = nl.nodeStats()
	return
}

func (n *notifs) getOwner(uuid string) (o string, exists bool) {
	var nl notifListener
	if nl, exists = n.entry(uuid); exists {
		o = nl.getOwner()
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
		nl.setErr(err)
		nl.setAborted()
		nl.unlock()
		nl.callback(nl, err, now)
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
	} else if isDLType(t.Type) {
		n.nl = &notifDownloadBase{}
	} else {
		n.nl = &notifXactBase{}
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

func (nf *nlFilter) match(nl notifListener) bool {
	if nl.UUID() == nf.ID {
		return true
	}

	if nl.kind() == nf.Kind {
		if nf.Bck == nil || nf.Bck.IsEmpty() {
			return true
		}
		for _, bck := range nl.bcks() {
			if cmn.QueryBcks(nf.Bck.Bck).Contains(bck) {
				return true
			}
		}
	}
	return false
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
