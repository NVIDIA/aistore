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
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/memsys"
)

const (
	revsSmapTag  = "Smap"
	revsRMDTag   = "RMD"
	revsBMDTag   = "BMD"
	revsConfTag  = "Conf"
	revsTokenTag = "token"

	revsMaxTags   = 5
	revsActionTag = "-action" // prefix revs tag
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
		tag() string             // enum { revsSmapTag, ... }
		version() int64          // the version
		marshal() (b []byte)     // marshals the revs
		jit(p *proxyrunner) revs // current (just-in-time) instance
		sgl() *memsys.SGL        // jsp-encoded SGL
		String() string          // smap.String(), etc.
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
	msPayload map[string][]byte     // tag => revs' body
	ndRevs    map[string]int64      // tag => version (see nodesRevs)
	tagl      map[int64]*memsys.SGL // version => SGL jsp-formatted

	// main
	metasyncer struct {
		p            *proxyrunner      // parent
		nodesRevs    map[string]ndRevs // cluster-wide node ID => ndRevs sync-ed
		sgls         map[string]tagl   // tag => (version => SGL)
		lastSynced   map[string]revs   // tag => revs last/current sync-ed
		lastClone    msPayload         // debug to enforce CoW
		stopCh       chan struct{}     // stop channel
		workCh       chan revsReq      // work channel
		retryTimer   *time.Timer       // timer to sync pending
		timerStopped bool              // true if retryTimer has been stopped, false otherwise
	}
)

// interface guard
var _ cos.Runner = (*metasyncer)(nil)

func (req revsReq) isNil() bool { return len(req.pairs) == 0 }

////////////////
// metasyncer //
////////////////

func newMetasyncer(p *proxyrunner) (y *metasyncer) {
	y = &metasyncer{p: p}
	y.nodesRevs = make(map[string]ndRevs, 8)
	y.inigls()
	y.lastSynced = make(map[string]revs, revsMaxTags)
	y.lastClone = make(msPayload)

	y.stopCh = make(chan struct{}, 1)
	y.workCh = make(chan revsReq, 32)

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
				y.nodesRevs = make(map[string]ndRevs)
				y.free()
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
	)
	if daemon.stopping.Load() {
		return
	}

	// step 1: validation & enforcement (CoW, non-decremental versioning, duplication)
	debug.Func(func() { y.validateCoW() })
	if revsReqType == revsReqNotify {
		method = http.MethodPost
	} else {
		debug.Assert(revsReqType == revsReqSync)
		method = http.MethodPut
	}

	// step 2: build payload and update last sync-ed
	payload := make(msPayload, 2*len(pairs))
	for _, pair := range pairs {
		var (
			revs, msg, tag, s = pair.revs, pair.msg, pair.revs.tag(), ""
			jitRevs           = revs.jit(y.p)
			revsBody          []byte
		)
		debug.Assertf(revsReqType == revsReqNotify || revs.version() <= jitRevs.version(),
			"%s cannot be newer than jit %s", revs, jitRevs)
		if msg.Action != "" {
			s = ", action " + msg.Action
		}
		// just-in-time: making exception for BMD (and associated bucket-level transactions)
		if jitRevs.version() > revs.version() && tag != revsBMDTag {
			glog.Infof("%s: sync newer %s v%d%s (skipping %s)", y.p.si, tag, jitRevs.version(), s, revs)
			revs = jitRevs
		} else {
			glog.Infof("%s: sync %s v%d%s", y.p.si, tag, revs.version(), s)
		}
		y.lastSynced[tag] = revs
		if sgl := revs.sgl(); sgl != nil {
			// fast path
			y.addnew(revs)
			revsBody = sgl.Bytes()
			y.delold(revs)
		} else {
			// slow path
			revsBody = revs.marshal()
			if sgl := revs.sgl(); sgl != nil {
				y.addnew(revs)
				y.delold(revs)
			}
		}
		debug.Func(func() { y.lastClone[tag] = revsBody })
		msgJSON := cos.MustMarshal(msg)

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
		body    = payload.marshal(y.p.gmm)
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
				y.syncDone(res.si, pairs)
			}
			continue
		}
		glog.Warningf("%s: failed to sync %s, err: %v(%d)", y.p.si, res.si, res.err, res.status)
		// in addition to "connection-refused" always retry newTargetID - the joining one
		if cmn.IsErrConnectionRefused(res.err) || cos.StringInSlice(res.si.ID(), newTargetIDs) {
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

		y.handleRefused(method, urlPath, body, refused, pairs, config)
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
	ndr, ok := y.nodesRevs[si.ID()]
	smap := y.p.owner.smap.get()
	if smap.GetNodeNotMaint(si.ID()) == nil {
		if ok {
			delete(y.nodesRevs, si.ID())
		}
		return
	}
	if !ok {
		ndr = make(map[string]int64, revsMaxTags)
		y.nodesRevs[si.ID()] = ndr
	}
	for _, revsPair := range pairs {
		revs := revsPair.revs
		ndr[revs.tag()] = revs.version()
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
func (y *metasyncer) pending() (pending cluster.NodeMap) {
	smap := y.p.owner.smap.get()
	if !smap.isPrimary(y.p.si) {
		y.becomeNonPrimary()
		return
	}
	for _, serverMap := range []cluster.NodeMap{smap.Tmap, smap.Pmap} {
		for _, si := range serverMap {
			if si.ID() == y.p.si.ID() {
				continue
			}
			ndr, ok := y.nodesRevs[si.ID()]
			if !ok {
				y.nodesRevs[si.ID()] = make(map[string]int64, revsMaxTags)
			} else {
				inSync := true
				for tag, revs := range y.lastSynced {
					v, ok := ndr[tag]
					if !ok || v < revs.version() {
						inSync = false
						break
					} else if v > revs.version() {
						// skip older versions (TODO: don't skip sending associated aisMsg)
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
	pending := y.pending()
	if len(pending) == 0 {
		glog.Infof("no pending revs - all good")
		return
	}
	var (
		payload = make(msPayload, 2*len(y.lastSynced))
		pairs   = make([]revsPair, 0, len(y.lastSynced))
		msg     = y.p.newAmsgStr("metasync: handle-pending", nil) // NOTE: same msg for all revs
		msgBody = cos.MustMarshal(msg)
	)
	for tag, revs := range y.lastSynced {
		if sgl := y.getver(revs); sgl != nil {
			payload[tag] = sgl.Bytes()
		} else {
			payload[tag] = revs.marshal()
			if sgl := revs.sgl(); sgl != nil {
				y.addnew(revs)
			}
		}
		payload[tag+revsActionTag] = msgBody
		pairs = append(pairs, revsPair{revs, msg})
	}
	var (
		urlPath = cmn.URLPathMetasync.S
		body    = payload.marshal(y.p.gmm)
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

////////////
// y.sgls //
////////////

func (y *metasyncer) inigls() {
	y.sgls = make(map[string]tagl, revsMaxTags)
	y.sgls[revsSmapTag] = tagl{}
	y.sgls[revsBMDTag] = tagl{}
	y.sgls[revsConfTag] = tagl{}
}

func (y *metasyncer) free() {
	for _, tagl := range y.sgls {
		for _, sgl := range tagl {
			sgl.Free()
		}
	}
	y.inigls()
}

func (y *metasyncer) addnew(revs revs) {
	vgl, ok := y.sgls[revs.tag()]
	if !ok {
		vgl = tagl{}
		y.sgls[revs.tag()] = vgl
	}
	if sgl, ok := vgl[revs.version()]; ok {
		// free duplicate (created previously via "slow path")
		if sgl != revs.sgl() {
			debug.Assert(sgl.Len() == revs.sgl().Len())
			sgl.Free()
		}
	}
	vgl[revs.version()] = revs.sgl()
}

func (y *metasyncer) getver(revs revs) (sgl *memsys.SGL) {
	vgl, ok := y.sgls[revs.tag()]
	if !ok {
		return
	}
	return vgl[revs.version()]
}

func (y *metasyncer) delold(revs revs) {
	vgl, ok := y.sgls[revs.tag()]
	if !ok {
		return
	}
	for v, sgl := range vgl {
		if v < revs.version() {
			sgl.Free()
			delete(vgl, v)
		}
	}
}

///////////////////////////
// metasync jsp encoding //
///////////////////////////

var (
	msjspOpts = jsp.Options{Metaver: cmn.MetaverMetasync, Signature: true, Checksum: true}
	msimmSize int64
)

func (payload msPayload) marshal(mm *memsys.MMSA) (sgl *memsys.SGL) {
	sgl = mm.NewSGL(msimmSize)
	err := jsp.Encode(sgl, payload, msjspOpts)
	cos.AssertNoErr(err)
	msimmSize = cos.MaxI64(msimmSize, sgl.Len())
	return sgl
}

func (payload msPayload) unmarshal(reader io.ReadCloser, tag string) (err error) {
	_, err = jsp.Decode(reader, &payload, msjspOpts, tag)
	return
}
