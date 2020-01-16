// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

// REVS tags
const (
	smaptag   = "smaptag"
	bmdtag    = "bmdtag"   //
	tokentag  = "tokentag" //
	actiontag = "-action"  // to make a pair (revs, action)
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
// Action message (above, see actionMsgInternal) provides receivers with a context as
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
// metasyncer.sync(false, smapowner.get(), action1, bmdowner.get(), action2)
//
// To block until all the replicas get delivered:
//
// metasyncer.sync(true, ...)
//
// On the receiving side, the payload (see above) gets extracted, validated,
// version-compared, and the corresponding Rx handler gets invoked
// with additional information that includes the per-replica action message.
//
// ================================ end of TOO ==================================

type revs interface {
	tag() string                    // known tags enumerated above
	version() int64                 // version - locking not required
	marshal() (b []byte, err error) // json-marshal - ditto
}

// private types - used internally by the metasync
type (
	revspair struct {
		revs   revs
		msgInt *actionMsgInternal
	}
	revsReq struct {
		pairs     []revspair
		wg        *sync.WaitGroup
		failedCnt *atomic.Int32 // TODO: add cluster.NodeMap to identify the failed ones
		reqType   int           // enum: revsReqSync, etc.
	}
	revsdaemon struct {
		vermap map[string]int64 // by tag; used to track daemon => (versions) info
	}
)

type metasyncer struct {
	cmn.Named
	p            *proxyrunner          // parent
	revsmap      map[string]revsdaemon // sync-ed versions (cluster-wide, by DaemonID)
	last         map[string]revs       // last/current sync-ed
	lastclone    cmn.SimpleKVs         // to enforce CoW
	stopCh       chan struct{}         // stop channel
	workCh       chan revsReq          // work channel
	retryTimer   *time.Timer           // timer to sync pending
	timerStopped bool                  // true if retryTimer has been stopped, false otherwise
}

//
// inner helpers
//
func (req revsReq) isNil() bool { return len(req.pairs) == 0 }

//
// c-tor, register, and runner
//
func newmetasyncer(p *proxyrunner) (y *metasyncer) {
	y = &metasyncer{p: p}
	y.last = make(map[string]revs)
	y.lastclone = make(cmn.SimpleKVs)
	y.revsmap = make(map[string]revsdaemon)

	y.stopCh = make(chan struct{}, 1)
	y.workCh = make(chan revsReq, 8)

	y.retryTimer = time.NewTimer(time.Hour)
	y.retryTimer.Stop()
	y.timerStopped = true
	return
}

func (y *metasyncer) Run() error {
	glog.Infof("Starting %s", y.Getname())
	for {
		config := cmn.GCO.Get()
		select {
		case revsReq, ok := <-y.workCh:
			if !ok {
				break
			}
			if revsReq.isNil() { // <== see becomeNonPrimary()
				y.revsmap = make(map[string]revsdaemon)
				y.last = make(map[string]revs)
				y.lastclone = make(cmn.SimpleKVs)
				y.retryTimer.Stop()
				y.timerStopped = true
				break
			}
			cnt := y.doSync(revsReq.pairs, revsReq.reqType)
			if revsReq.wg != nil {
				if revsReq.failedCnt != nil {
					revsReq.failedCnt.Store(int32(cnt))
				}
				revsReq.wg.Done()
			}
			if cnt > 0 && y.timerStopped && len(revsReq.pairs) > 0 {
				y.retryTimer.Reset(config.Periodic.RetrySyncTime)
				y.timerStopped = false
			}
		case <-y.retryTimer.C:
			cnt := y.handlePending()
			if cnt > 0 {
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
	glog.Infof("Stopping %s, err: %v", y.Getname(), err)

	y.stopCh <- struct{}{}
	close(y.stopCh)
}

//
// methods (notify, sync, becomeNonPrimary) consistute internal API
//

func (y *metasyncer) notify(wait bool, pair revspair) (failedCnt int) {
	if !y.checkPrimary() {
		return
	}
	var (
		failedCntAtomic = atomic.NewInt32(0)
		revsReq         = revsReq{pairs: []revspair{pair}}
	)
	if wait {
		revsReq.wg = &sync.WaitGroup{}
		revsReq.wg.Add(1)
		revsReq.failedCnt = failedCntAtomic
		revsReq.reqType = revsReqNotify
	}
	y.workCh <- revsReq

	if wait {
		revsReq.wg.Wait()
		failedCnt = int(failedCntAtomic.Load())
	}
	return
}

func (y *metasyncer) sync(wait bool, pairs ...revspair) {
	if !y.checkPrimary() {
		return
	}
	cmn.Assert(len(pairs) > 0)
	revsReq := revsReq{pairs: pairs}
	if wait {
		revsReq.wg = &sync.WaitGroup{}
		revsReq.wg.Add(1)
		revsReq.reqType = revsReqSync
	}
	y.workCh <- revsReq

	if wait {
		revsReq.wg.Wait()
	}
}

// become non-primary (to serialize cleanup of the internal state and stop the timer)
func (y *metasyncer) becomeNonPrimary() {
	y.workCh <- revsReq{}
	glog.Infof("becoming non-primary")
}

//
// methods internal to metasync.go
//

// main method; see top of the file; returns number of "sync" failures
func (y *metasyncer) doSync(pairs []revspair, revsReqType int) (cnt int) {
	var (
		refused     cluster.NodeMap
		pairsToSend []revspair
		newTargetID string
		method      string
		//
		payload = make(cmn.SimpleKVs)
		smap    = y.p.smapowner.get()
		config  = cmn.GCO.Get()
	)
	newCnt := y.countNewMembers(smap)
	// step 1: validation & enforcement (CoW, non-decremental versioning, duplication)
	for tag, revs := range y.last {
		revJSON, err := revs.marshal()
		cmn.AssertNoErr(err)
		if cowCopy, ok := y.lastclone[tag]; ok {
			if cowCopy != string(revJSON) {
				s := fmt.Sprintf("CoW violation: previously sync-ed %s v%d has been updated in-place",
					tag, revs.version())
				cmn.AssertMsg(false, s)
			}
		}
	}

	if revsReqType == revsReqNotify {
		method = http.MethodPost
	} else if revsReqType == revsReqSync {
		method = http.MethodPut
	} else {
		cmn.AssertMsg(false, fmt.Sprintf("unknown request type: %d", revsReqType))
	}

	pairsToSend = pairs[:0] // share original slice
outer:
	for _, pair := range pairs {
		var (
			revs, msgInt, tag = pair.revs, pair.msgInt, pair.revs.tag()
			s                 = fmt.Sprintf("[%s, action=%s, version=%d]", tag, msgInt.Action, revs.version())
		)
		// vs current Smap
		if tag == smaptag {
			if revsReqType == revsReqSync && revs.version() > smap.version() {
				ers := fmt.Sprintf("FATAL: %s is newer than the current %s", s, smap)
				cmn.AssertMsg(false, ers)
			}
		}
		// vs the last sync-ed: enforcing non-decremental versioning on the wire
		switch lversion := y.lversion(tag); {
		case lversion == revs.version():
			if newCnt == 0 {
				glog.Errorf("%s duplicated - already sync-ed or pending", s)
				continue outer
			}
			glog.Infof("%s duplicated - proceeding to sync %d new member(s)", s, newCnt)
		case lversion > revs.version():
			glog.Errorf("skipping %s: < current v%d", s, lversion)
			continue outer
		}

		pairsToSend = append(pairsToSend, pair)
	}
	if len(pairsToSend) == 0 {
		return
	}
	// step 2: build payload and update last sync-ed
	for _, pair := range pairsToSend {
		var revs, msgInt, tag, s = pair.revs, pair.msgInt, pair.revs.tag(), ""
		if msgInt.Action != "" {
			s = ", action " + msgInt.Action
		}
		glog.Infof("%s: sync %s v%d%s", y.p.si.Name(), tag[:2], revs.version(), s)

		y.last[tag] = revs
		revJSON, err := revs.marshal()
		cmn.AssertNoErr(err)
		y.lastclone[tag] = string(revJSON)
		msgJSON := cmn.MustMarshal(msgInt)

		action, id := msgInt.Action, msgInt.NewDaemonID
		if action == cmn.ActRegTarget {
			newTargetID = id
		}

		payload[tag] = string(revJSON)           // payload
		payload[tag+actiontag] = string(msgJSON) // action message always on the wire even when empty
	}

	// step 3: b-cast
	var (
		urlPath = cmn.URLPath(cmn.Version, cmn.Metasync)
		body    = cmn.MustMarshal(payload)
	)
	res := y.p.bcastTo(bcastArgs{
		req: cmn.ReqArgs{
			Method: method,
			Path:   urlPath,
			Body:   body,
		},
		smap:    smap,
		timeout: config.Timeout.CplaneOperation * 2, // making exception for this critical op
		to:      cluster.AllNodes,
	})

	// step 4: count failures and fill-in refused
	for r := range res {
		if r.err == nil {
			if revsReqType == revsReqSync {
				y.syncDone(r.si.ID(), pairsToSend)
			}
			continue
		}
		glog.Warningf("Failed to sync %s, err: %v (%d)", r.si, r.err, r.status)
		// in addition to "connection-refused" always retry newTargetID - the joining one
		if cmn.IsErrConnectionRefused(r.err) || r.si.ID() == newTargetID {
			if refused == nil {
				refused = make(cluster.NodeMap, 4)
			}
			refused[r.si.ID()] = r.si
		} else {
			cnt++
		}
	}
	// step 5: handle connection-refused right away
	for i := 0; i < 10; i++ {
		if len(refused) == 0 {
			break
		}
		time.Sleep(config.Timeout.CplaneOperation)
		smap = y.p.smapowner.get()
		if !smap.isPrimary(y.p.si) {
			y.becomeNonPrimary()
			return
		}

		y.handleRefused(method, urlPath, body, refused, pairsToSend, config, smap)
	}
	// step 6: housekeep and return new pending
	smap = y.p.smapowner.get()
	for id := range y.revsmap {
		if !smap.containsID(id) {
			delete(y.revsmap, id)
		}
	}
	cnt += len(refused)
	return
}

// keeping track of per-daemon versioning - FIXME TODO: extend to take care of msgInt where pais may be empty
func (y *metasyncer) syncDone(sid string, pairs []revspair) {
	rvd, ok := y.revsmap[sid]
	if !ok {
		rvd = revsdaemon{vermap: make(map[string]int64)}
		y.revsmap[sid] = rvd
	}
	for _, revspair := range pairs {
		revs := revspair.revs
		rvd.vermap[revs.tag()] = revs.version()
	}
}

func (y *metasyncer) handleRefused(method, urlPath string, body []byte, refused cluster.NodeMap, pairs []revspair,
	config *cmn.Config, smap *smapX) {
	res := y.p.bcast(bcastArgs{
		req: cmn.ReqArgs{
			Method: method,
			Path:   urlPath,
			Body:   body,
		},
		network: cmn.NetworkIntraControl,
		timeout: config.Timeout.MaxKeepalive, // JSON config "max_keepalive"
		nodes:   []cluster.NodeMap{refused},
	})

	for r := range res {
		if r.err == nil {
			delete(refused, r.si.ID())
			y.syncDone(r.si.ID(), pairs)
			glog.Infof("handle-refused: sync-ed %s", smap.printname(r.si.ID()))
		} else {
			glog.Warningf("handle-refused: failing to sync %s, err: %v (%d)",
				smap.printname(r.si.ID()), r.err, r.status)
		}
	}
}

// pending (map), if requested, contains only those daemons that need
// to get at least one of the most recently sync-ed tag-ed revs
func (y *metasyncer) pending(needMap bool) (count int, pending cluster.NodeMap, smap *smapX) {
	smap = y.p.smapowner.get()
	if !smap.isPrimary(y.p.si) {
		y.becomeNonPrimary()
		return
	}
	for _, serverMap := range []cluster.NodeMap{smap.Tmap, smap.Pmap} {
		for id, si := range serverMap {
			rvd, ok := y.revsmap[id]
			if !ok {
				rvd = revsdaemon{vermap: make(map[string]int64)}
				y.revsmap[id] = rvd
				count++
				if !needMap {
					continue
				}
			} else {
				inSync := true
				for tag, revs := range y.last {
					v, ok := rvd.vermap[tag]
					if !ok || v != revs.version() {
						cmn.Assert(!ok || v < revs.version())
						count++
						inSync = false
						break
					}
				}
				if !needMap || inSync {
					continue
				}
			}
			if pending == nil {
				pending = make(cluster.NodeMap)
			}
			pending[id] = si
		}
	}
	return
}

// gets invoked when retryTimer fires; returns updated number of still pending
func (y *metasyncer) handlePending() (cnt int) {
	count, pending, smap := y.pending(true)
	if count == 0 {
		glog.Infof("no pending revs - all good")
		return
	}

	payload := make(cmn.SimpleKVs)
	pairs := make([]revspair, 0, len(y.last))
	msgInt := y.p.newActionMsgInternalStr("metasync: handle-pending", smap, nil) // the same action msg for all
	jsmsg := cmn.MustMarshal(msgInt)
	for tag, revs := range y.last {
		body, err := revs.marshal()
		cmn.AssertNoErr(err)
		payload[tag] = string(body)
		payload[tag+actiontag] = string(jsmsg)
		pairs = append(pairs, revspair{revs, msgInt})
	}

	body := cmn.MustMarshal(payload)
	res := y.p.bcast(bcastArgs{
		req: cmn.ReqArgs{
			Method: http.MethodPut,
			Path:   cmn.URLPath(cmn.Version, cmn.Metasync),
			Body:   body,
		},
		network: cmn.NetworkIntraControl,
		timeout: cmn.GCO.Get().Timeout.CplaneOperation,
		nodes:   []cluster.NodeMap{pending},
	})
	for r := range res {
		if r.err == nil {
			y.syncDone(r.si.ID(), pairs)
			glog.Infof("handle-pending: sync-ed %s", smap.printname(r.si.ID()))
		} else {
			cnt++
			glog.Warningf("handle-pending: failing to sync %s, err: %v (%d)",
				smap.printname(r.si.ID()), r.err, r.status)
		}
	}
	return
}

func (y *metasyncer) checkPrimary() bool {
	smap := y.p.smapowner.get()
	cmn.Assert(smap != nil)
	if smap.isPrimary(y.p.si) {
		return true
	}
	reason := "the primary"
	if !smap.isPresent(y.p.si) {
		reason = "present in the Smap"
	}
	lead := "?"
	if smap.ProxySI != nil {
		lead = smap.ProxySI.ID()
	}
	glog.Errorf("%s self is not %s (primary=%s, %s) - failing the 'sync' request", y.p.si, reason, lead, smap)
	return false
}

func (y *metasyncer) lversion(tag string) int64 {
	if revs, ok := y.last[tag]; ok {
		return revs.version()
	}
	return 0
}

func (y *metasyncer) countNewMembers(smap *smapX) (count int) {
	for _, serverMap := range []cluster.NodeMap{smap.Tmap, smap.Pmap} {
		for id := range serverMap {
			if _, ok := y.revsmap[id]; !ok {
				count++
			}
		}
	}
	return
}
