// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/memsys"
)

const (
	revsSmapTag = "Smap"
	revsRMDTag  = "RMD"
	revsBMDTag  = "BMD"
	revsConfTag = "Conf"

	revsTokenTag  = "token"
	revsActionTag = "-action" // to make a pair (revs, action)

	revsMaxTags = 4
)

const (
	revsReqSync = iota
	revsReqNotify
)

// ===================== Theory Of Operations (TOO) =============================
//
// The metasync API exposed to the rest of the code includes two methods:
// * sync - to synchronize cluster-level metadata (the main method)
// * becomeNonPrimary - to be called when the current primary becomes non-primary
//
// All other methods (and the metasync's own state) are private and internal to
// the metasync.
//
// The main internal method, doSync, does most of the metasync-ing job and is
// commented with its 6 steps executed in a single serial context.
//
// The job itself consists in synchronoizing REVS across a AIStore cluster.
//
// REVS (interface below) stands for REplicated, Versioned and Shared/Synchronized.
//
// A REVS is an object that represents a certain kind of cluster-wide metadata and
// must be consistently replicated across the entire cluster. To that end, the
// "metasyncer" provides a generic transport to send an arbitrary payload that
// combines any number of data units having the following layout:
//
//         (shared-replicated-object, associated action-message)
//
// Action message (above, see aisMsg) provides receivers with a context as
// to what exactly to do with the newly received versioned replica.
//
// Further, the metasyncer:
//
// 1) tracks the last synchronized REVS versions
// 2) makes sure all version updates are executed strictly in the non-decremental
//    order
// 3) makes sure that nodes that join the cluster get updated with the current set
//    of REVS objects
// 4) handles failures to update existing nodes, by periodically retrying
//    pending synchronizations (for as long as those members remain in the
//    most recent and current cluster map).
//
// Last but not the least, metasyncer checks that only the currently elected
// leader (aka "primary proxy") distributes the REVS objects, thus providing for
// simple serialization of the versioned updates.
//
// The usage is easy - there is a single sync() method that accepts variable
// number of parameters. Example sync-ing Smap and BMD
// asynchronously:
//
// metasyncer.sync(smapOwner.get(), action1, owner.bmd.get(), action2)
//
// To block until all the replicas get delivered:
//
// wg = metasyncer.sync(...)
// wg.Wait()
//
// On the receiving side, the payload (see above) gets extracted, validated,
// version-compared, and the corresponding Rx handler gets invoked
// with additional information that includes the per-replica action message.
//
// ================================ end of TOO ==================================

type (
	revs interface {
		tag() string         // tag of specific revs, look for: `revs*Tag`
		version() int64      // version
		marshal() (b []byte) // marshals the struct
	}
	revsPair struct {
		revs revs
		msg  *aisMsg
	}
	revsReq struct {
		pairs     []revsPair
		wg        *sync.WaitGroup
		failedCnt *atomic.Int32
		reqType   int // enum: revsReqSync, etc.
	}
	nodeRevs struct {
		versions map[string]int64 // used to track daemon, tag => (versions) info
	}
	metasyncer struct {
		p            *proxyrunner        // parent
		nodesRevs    map[string]nodeRevs // sync-ed versions (cluster-wide, by DaemonID)
		lastSynced   map[string]revs     // last/current sync-ed
		lastClone    msPayload           // to enforce CoW
		stopCh       chan struct{}       // stop channel
		workCh       chan revsReq        // work channel
		retryTimer   *time.Timer         // timer to sync pending
		timerStopped bool                // true if retryTimer has been stopped, false otherwise
	}
	msPayload map[string][]byte // tag => revs' body
)

// interface guard
var _ cmn.Runner = (*metasyncer)(nil)

func (req revsReq) isNil() bool { return len(req.pairs) == 0 }

////////////////
// metasyncer //
////////////////

func newMetasyncer(p *proxyrunner) (y *metasyncer) {
	y = &metasyncer{p: p}
	y.lastSynced = make(map[string]revs)
	y.lastClone = make(msPayload)
	y.nodesRevs = make(map[string]nodeRevs)

	y.stopCh = make(chan struct{}, 1)
	y.workCh = make(chan revsReq, 8)

	y.retryTimer = time.NewTimer(time.Hour)
	y.retryTimer.Stop()
	y.timerStopped = true
	return
}

func (y *metasyncer) Name() string { return "metasyncer" }

func (y *metasyncer) Run() error {
	glog.Infof("Starting %s", y.Name())
	for {
		config := cmn.GCO.Get()
		select {
		case revsReq, ok := <-y.workCh:
			if !ok {
				break
			}
			if revsReq.isNil() { // <== see becomeNonPrimary()
				y.nodesRevs = make(map[string]nodeRevs)
				y.lastSynced = make(map[string]revs)
				y.lastClone = make(msPayload)
				y.retryTimer.Stop()
				y.timerStopped = true
				break
			}
			failedCnt := y.doSync(revsReq.pairs, revsReq.reqType)
			if revsReq.wg != nil {
				if revsReq.failedCnt != nil {
					revsReq.failedCnt.Store(int32(failedCnt))
				}
				revsReq.wg.Done()
			}
			if failedCnt > 0 && y.timerStopped {
				y.retryTimer.Reset(config.Periodic.RetrySyncTime)
				y.timerStopped = false
			}
		case <-y.retryTimer.C:
			failedCnt := y.handlePending()
			if failedCnt > 0 {
				y.retryTimer.Reset(config.Periodic.RetrySyncTime)
				y.timerStopped = false
			} else {
				y.timerStopped = true
			}
		case <-y.stopCh:
			y.retryTimer.Stop()
			return nil
		}
	}
}

func (y *metasyncer) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", y.Name(), err)

	y.stopCh <- struct{}{}
	close(y.stopCh)
}

// notify only targets - see bcastTo below
func (y *metasyncer) notify(wait bool, pair revsPair) (failedCnt int) {
	var (
		failedCntAtomic = atomic.NewInt32(0)
		req             = revsReq{pairs: []revsPair{pair}}
	)
	if y.isPrimary() != nil {
		debug.Assert(false)
		return
	}
	if wait {
		req.wg = &sync.WaitGroup{}
		req.wg.Add(1)
		req.failedCnt = failedCntAtomic
		req.reqType = revsReqNotify
	}
	y.workCh <- req

	if wait {
		req.wg.Wait()
		failedCnt = int(failedCntAtomic.Load())
	}
	return
}

func (y *metasyncer) sync(pairs ...revsPair) *sync.WaitGroup {
	debug.Assert(len(pairs) > 0)
	req := revsReq{pairs: pairs}
	req.wg = &sync.WaitGroup{}
	if y.isPrimary() != nil {
		debug.Assert(false)
		return req.wg
	}
	req.wg.Add(1)
	req.reqType = revsReqSync
	y.workCh <- req
	return req.wg
}

// become non-primary (to serialize cleanup of the internal state and stop the timer)
func (y *metasyncer) becomeNonPrimary() {
	y.workCh <- revsReq{}
	glog.Infof("%s: becoming non-primary", y.p.si)
}

// main method; see top of the file; returns number of "sync" failures
func (y *metasyncer) doSync(pairs []revsPair, revsReqType int) (failedCnt int) {
	var (
		refused      cluster.NodeMap
		method       string
		newTargetIDs []string
		smap         = y.p.owner.smap.get()
		config       = cmn.GCO.Get()
		pairsToSend  = pairs[:0] // share original slice
	)
	if daemon.stopping.Load() {
		return
	}
	newCnt := y.countNewMembers(smap)

	// step 1: validation & enforcement (CoW, non-decremental versioning, duplication)
	debug.Func(func() {
		y.validateCoW()
		debug.Assertf(revsReqType == revsReqNotify || revsReqType == revsReqSync,
			"unknown request type: %d", revsReqType)
	})
	if revsReqType == revsReqNotify {
		method = http.MethodPost
	} else {
		method = http.MethodPut
	}
outer:
	for _, pair := range pairs {
		var (
			revs, msg, tag = pair.revs, pair.msg, pair.revs.tag()
			detail         = fmt.Sprintf("[%s, action=%s, version=%d]", tag, msg.Action, revs.version())
		)
		// vs current Smap
		if tag == revsSmapTag {
			if revsReqType == revsReqSync && revs.version() > smap.version() {
				ers := fmt.Sprintf("FATAL: %s is newer than the current %s", detail, smap)
				cmn.AssertMsg(false, ers)
			}
		}
		// vs the last sync-ed: enforcing non-decremental versioning on the wire
		switch lversion := y.lastVersion(tag); {
		case lversion == revs.version():
			if newCnt == 0 {
				glog.Warningf("%s: %s duplicated - already sync-ed or pending", y.p.si, detail)
			}
			glog.Infof("%s: %s duplicated - proceeding to sync %d new member(s)", y.p.si, detail, newCnt)
		case lversion > revs.version():
			s := fmt.Sprintf("%s: older %s: < current v%d", y.p.si, detail, lversion)
			if msg.UUID == "" {
				glog.Errorln(s + " - skipping")
				continue outer
			}
			glog.Warning(s) // NOTE: supporting transactional logic
		}

		pairsToSend = append(pairsToSend, pair)
	}
	if len(pairsToSend) == 0 {
		return
	}

	// step 2: build payload and update last sync-ed
	payload := make(msPayload, 2*len(pairsToSend))
	for _, pair := range pairsToSend {
		revs, msg, tag, s := pair.revs, pair.msg, pair.revs.tag(), ""
		if msg.Action != "" {
			s = ", action " + msg.Action
		}
		glog.Infof("%s: sync %s v%d%s", y.p.si, tag, revs.version(), s)

		y.lastSynced[tag] = revs
		revsBody := revs.marshal()
		debug.Func(func() { y.lastClone[tag] = revsBody })
		msgJSON := cmn.MustMarshal(msg)

		if tag == revsRMDTag {
			md := revs.(*rebMD)
			newTargetIDs = md.TargetIDs
		}

		payload[tag] = revsBody              // payload
		payload[tag+revsActionTag] = msgJSON // action message always on the wire even when empty
	}

	// step 3: b-cast
	var (
		urlPath = cmn.URLPathMetasync.S
		body    = payload.marshal()
		to      = cluster.AllNodes
	)
	defer body.Free()

	if revsReqType == revsReqNotify {
		to = cluster.Targets
	}
	args := allocBcastArgs()
	args.req = cmn.ReqArgs{Method: method, Path: urlPath, BodyR: body}
	args.smap = smap
	args.timeout = config.Timeout.MaxKeepalive // making exception for this critical op
	args.to = to
	args.ignoreMaintenance = true
	results := y.p.bcastGroup(args)
	freeBcastArgs(args)

	// step 4: count failures and fill-in refused
	for _, res := range results {
		if res.err == nil {
			if revsReqType == revsReqSync {
				y.syncDone(res.si, pairsToSend)
			}
			continue
		}
		glog.Warningf("%s: failed to sync %s, err: %v(%d)", y.p.si, res.si, res.err, res.status)
		// in addition to "connection-refused" always retry newTargetID - the joining one
		if cmn.IsErrConnectionRefused(res.err) || cmn.StringInSlice(res.si.ID(), newTargetIDs) {
			if refused == nil {
				refused = make(cluster.NodeMap, 2)
			}
			refused.Add(res.si)
		} else {
			failedCnt++
		}
	}
	freeCallResults(results)
	// step 5: handle connection-refused right away
	for i := 0; i < 4; i++ {
		if len(refused) == 0 {
			break
		}
		time.Sleep(config.Timeout.CplaneOperation)
		smap = y.p.owner.smap.get()
		if !smap.isPrimary(y.p.si) {
			y.becomeNonPrimary()
			return
		}

		y.handleRefused(method, urlPath, body, refused, pairsToSend, config)
	}
	// step 6: housekeep and return new pending
	smap = y.p.owner.smap.get()
	for sid := range y.nodesRevs {
		si := smap.GetNodeNotMaint(sid)
		if si == nil {
			delete(y.nodesRevs, sid)
		}
	}
	failedCnt += len(refused)
	return
}

// keeping track of per-daemon versioning - TODO: extend to take care of aisMsg where pairs may be empty
func (y *metasyncer) syncDone(si *cluster.Snode, pairs []revsPair) {
	rvd, ok := y.nodesRevs[si.ID()]
	smap := y.p.owner.smap.get()
	if smap.GetNodeNotMaint(si.ID()) == nil {
		if ok {
			delete(y.nodesRevs, si.ID())
		}
		return
	}
	if !ok {
		rvd = nodeRevs{versions: make(map[string]int64, revsMaxTags)}
		y.nodesRevs[si.ID()] = rvd
	}
	for _, revsPair := range pairs {
		revs := revsPair.revs
		rvd.versions[revs.tag()] = revs.version()
	}
}

func (y *metasyncer) handleRefused(method, urlPath string, body io.Reader, refused cluster.NodeMap,
	pairs []revsPair, config *cmn.Config) {
	args := allocBcastArgs()
	args.req = cmn.ReqArgs{Method: method, Path: urlPath, BodyR: body}
	args.network = cmn.NetworkIntraControl
	args.timeout = config.Timeout.MaxKeepalive
	args.nodes = []cluster.NodeMap{refused}
	args.nodeCount = len(refused)
	results := y.p.bcastNodes(args)
	freeBcastArgs(args)
	for _, res := range results {
		if res.err == nil {
			delete(refused, res.si.ID())
			y.syncDone(res.si, pairs)
			glog.Infof("%s: handle-refused: sync-ed %s", y.p.si, res.si)
		} else {
			glog.Warningf("%s: handle-refused: failing to sync %s, err: %v (%d)",
				y.p.si, res.si, res.err, res.status)
		}
	}
	freeCallResults(results)
}

// pending (map), if requested, contains only those daemons that need
// to get at least one of the most recently sync-ed tag-ed revs
func (y *metasyncer) pending() (pending cluster.NodeMap, smap *smapX) {
	smap = y.p.owner.smap.get()
	if !smap.isPrimary(y.p.si) {
		y.becomeNonPrimary()
		return
	}
	for _, serverMap := range []cluster.NodeMap{smap.Tmap, smap.Pmap} {
		for _, si := range serverMap {
			if si.ID() == y.p.si.ID() {
				continue
			}
			rvd, ok := y.nodesRevs[si.ID()]
			if !ok {
				y.nodesRevs[si.ID()] = nodeRevs{
					versions: make(map[string]int64, revsMaxTags),
				}
			} else {
				inSync := true
				for tag, revs := range y.lastSynced {
					v, ok := rvd.versions[tag]
					if !ok || v < revs.version() {
						inSync = false
						break
					} else if v > revs.version() {
						// skip older versions
						// TODO -- FIXME: don't skip sending aisMsg associated with revs
						glog.Errorf("v: %d; revs.version: %d", v, revs.version())
					}
				}
				if inSync {
					continue
				}
			}
			if pending == nil {
				pending = make(cluster.NodeMap, 2)
			}
			pending.Add(si)
		}
	}
	return
}

// gets invoked when retryTimer fires; returns updated number of still pending
// using MethodPut since revsReqType here is always revsReqSync
func (y *metasyncer) handlePending() (failedCnt int) {
	pending, smap := y.pending()
	if len(pending) == 0 {
		glog.Infof("no pending revs - all good")
		return
	}
	var (
		payload = make(msPayload, 2*len(y.lastSynced))
		pairs   = make([]revsPair, 0, len(y.lastSynced))
		msg     = y.p.newAmsgStr("metasync: handle-pending", smap, nil) // NOTE: same msg for all revs
		msgBody = cmn.MustMarshal(msg)
	)
	for tag, revs := range y.lastSynced {
		payload[tag] = revs.marshal()
		payload[tag+revsActionTag] = msgBody
		pairs = append(pairs, revsPair{revs, msg})
	}
	var (
		urlPath = cmn.URLPathMetasync.S
		body    = payload.marshal()
		args    = allocBcastArgs()
	)
	args.req = cmn.ReqArgs{Method: http.MethodPut, Path: urlPath, BodyR: body}
	args.network = cmn.NetworkIntraControl
	args.timeout = cmn.GCO.Get().Timeout.MaxKeepalive
	args.nodes = []cluster.NodeMap{pending}
	args.nodeCount = len(pending)
	defer body.Free()
	results := y.p.bcastNodes(args)
	freeBcastArgs(args)
	for _, res := range results {
		if res.err == nil {
			y.syncDone(res.si, pairs)
			glog.Infof("%s: handle-pending: sync-ed %s", y.p.si, res.si)
		} else {
			failedCnt++
			glog.Warningf("%s: handle-pending: failing to sync %s, err: %v (%d)",
				y.p.si, res.si, res.err, res.status)
		}
	}
	freeCallResults(results)
	return
}

func (y *metasyncer) isPrimary() (err error) {
	smap := y.p.owner.smap.get()
	if smap.isPrimary(y.p.si) {
		return
	}
	err = newErrNotPrimary(y.p.si, smap)
	glog.Error(err)
	return
}

func (y *metasyncer) lastVersion(tag string) int64 {
	if revs, ok := y.lastSynced[tag]; ok {
		return revs.version()
	}
	return 0
}

func (y *metasyncer) countNewMembers(smap *smapX) (count int) {
	if len(y.nodesRevs) == 0 {
		y.nodesRevs = make(map[string]nodeRevs, smap.Count())
	}
	for _, serverMap := range []cluster.NodeMap{smap.Tmap, smap.Pmap} {
		for _, si := range serverMap {
			if si.ID() == y.p.si.ID() {
				continue
			}
			if _, ok := y.nodesRevs[si.ID()]; !ok {
				count++
			}
		}
	}
	return
}

func (y *metasyncer) validateCoW() {
	for tag, revs := range y.lastSynced {
		if cowCopy, ok := y.lastClone[tag]; ok {
			if bytes.Equal(cowCopy, revs.marshal()) {
				continue
			}
			s := fmt.Sprintf("CoW violation: previously sync-ed %s v%d has been updated in-place",
				tag, revs.version())
			debug.AssertMsg(false, s)
		}
	}
}

///////////////////////////
// metasync jsp encoding //
///////////////////////////

var msjspOpts = cmn.Jopts{Metaver: cmn.MetaverMetasync, Signature: true, Checksum: true}

func (payload msPayload) marshal() *memsys.SGL {
	return jsp.EncodeSGL(payload, msjspOpts)
}

func (payload msPayload) unmarshal(reader io.ReadCloser, tag string) (err error) {
	_, err = jsp.Decode(reader, &payload, msjspOpts, tag)
	return
}
