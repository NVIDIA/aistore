// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dfc

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/json-iterator/go"
)

// REVS tags
const (
	smaptag     = "smaptag"
	bucketmdtag = "bucketmdtag" //
	tokentag    = "tokentag"    //
	actiontag   = "-action"     // to make a pair (revs, action)
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
// The job itself consists in synchronoizing REVS across a DFC cluster.
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
// Action message (above, see cmn.ActionMsg) provides receivers with a context as
// to what exactly to do with the newly received versioned replica.
//
// In addition, storage target in particular make use action message structured as:
// action/newtargetid, which allows targets to figure out whether to rebalance the
// cluster, and how to execute the rebalancing.
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
// number of parameters. Example sync-ing Smap and bucket-metadata
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
		revs revs
		msg  *cmn.ActionMsg
	}
	revsReq struct {
		pairs []revspair
		wg    *sync.WaitGroup
	}
	revsdaemon map[string]int64 // by tag; used to track daemon => (versions) info
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
			if ok {
				if revsReq.pairs == nil { // <== see becomeNonPrimary()
					y.revsmap = make(map[string]revsdaemon)
					y.last = make(map[string]revs)
					y.lastclone = make(cmn.SimpleKVs)
					y.retryTimer.Stop()
					y.timerStopped = true
					break
				}
				cnt := y.doSync(revsReq.pairs)
				if revsReq.wg != nil {
					revsReq.wg.Done()
				}
				if cnt > 0 && y.timerStopped {
					y.retryTimer.Reset(config.Periodic.RetrySyncTime)
					y.timerStopped = false
				}
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
// API
//
func (y *metasyncer) sync(wait bool, params ...interface{}) {
	if !y.checkPrimary() {
		return
	}
	l := len(params) / 2
	cmn.Assert(l > 0 && len(params) == l*2)
	revsReq := revsReq{pairs: make([]revspair, l)}
	for i := 0; i < len(params); i += 2 {
		revs, ok := params[i].(revs)
		cmn.Assert(ok)
		if action, ok := params[i+1].(string); ok {
			msg := &cmn.ActionMsg{Action: action}
			revsReq.pairs[i/2] = revspair{revs, msg}
		} else {
			msg, ok := params[i+1].(*cmn.ActionMsg)
			cmn.Assert(ok)
			revsReq.pairs[i/2] = revspair{revs, msg}
		}
	}
	if wait {
		revsReq.wg = &sync.WaitGroup{}
		revsReq.wg.Add(1)
	}
	y.workCh <- revsReq

	if wait {
		revsReq.wg.Wait()
	}
}

// become non-primary
// (used to serialize cleanup of the internal state and stopping the timer)
func (y *metasyncer) becomeNonPrimary() {
	y.workCh <- revsReq{}
	glog.Infof("becoming non-primary")
}

//
// methods used internally by the metasync
//
// metasync main method - see top of the file; returns number of "sync" failures
func (y *metasyncer) doSync(pairs []revspair) (cnt int) {
	var (
		jsbytes, jsmsg []byte
		err            error
		refused        cluster.NodeMap
		payload        = make(cmn.SimpleKVs)
		smap           = y.p.smapowner.get()
		config         = cmn.GCO.Get()
	)
	newCnt := y.countNewMembers(smap)
	// step 1: validation & enforcement (CoW, non-decremental versioning, duplication)
	for tag, revs := range y.last {
		jsbytes, err = revs.marshal()
		cmn.Assert(err == nil, err)
		if cowcopy, ok := y.lastclone[tag]; ok {
			if cowcopy != string(jsbytes) {
				s := fmt.Sprintf("CoW violation: previously sync-ed %s v%d has been updated in-place",
					tag, revs.version())
				cmn.Assert(false, s)
			}
		}
	}

	pairsToSend := pairs[:0] // share original slice
OUTER:
	for _, pair := range pairs {
		var (
			revs, msg, tag = pair.revs, pair.msg, pair.revs.tag()
			s              = fmt.Sprintf("%s, action=%s, version=%d", tag, msg.Action, revs.version())
		)
		// vs current Smap
		if tag == smaptag {
			v := smap.version()
			if revs.version() > v {
				cmn.Assert(false, fmt.Sprintf("FATAL: %s is newer than the current Smap v%d", s, v))
			} else if revs.version() < v {
				glog.Warningf("Warning: %s: using newer Smap v%d to broadcast", s, v)
			}
		}
		// vs the last sync-ed: enforcing non-decremental versioning on the wire
		switch lversion := y.lversion(tag); {
		case lversion == revs.version():
			if newCnt == 0 {
				glog.Errorf("%s duplicated - already sync-ed or pending", s)
				continue OUTER
			}
			glog.Infof("%s duplicated - proceeding to sync %d new member(s)", s, newCnt)
		case lversion > revs.version():
			glog.Errorf("skipping %s: < current v%d", s, lversion)
			continue OUTER
		}

		pairsToSend = append(pairsToSend, pair)
	}
	if len(pairsToSend) == 0 {
		return
	}
	// step 2: build payload and update last sync-ed
	for _, pair := range pairsToSend {
		var revs, msg, tag = pair.revs, pair.msg, pair.revs.tag()
		glog.Infof("dosync: %s, action=%s, version=%d", tag, msg.Action, revs.version())

		y.last[tag] = revs
		jsbytes, err = revs.marshal()
		cmn.Assert(err == nil, err)
		y.lastclone[tag] = string(jsbytes)
		jsmsg, err = jsoniter.Marshal(msg)
		cmn.Assert(err == nil, err)

		payload[tag] = string(jsbytes)         // payload
		payload[tag+actiontag] = string(jsmsg) // action message always on the wire even when empty
	}
	jsbytes, err = jsoniter.Marshal(payload)
	cmn.Assert(err == nil, err)

	// step 3: b-cast
	urlPath := cmn.URLPath(cmn.Version, cmn.Metasync)
	res := y.p.broadcastTo(
		urlPath,
		nil, // query
		http.MethodPut,
		jsbytes,
		smap,
		config.Timeout.CplaneOperation,
		cmn.NetworkIntraControl,
		cluster.AllNodes,
	)

	// step 4: count failures and fill-in refused
	for r := range res {
		if r.err == nil {
			y.syncDone(r.si.DaemonID, pairsToSend)
			continue
		}
		glog.Warningf("Failed to sync %s, err: %v (%d)", r.si, r.err, r.status)
		if cmn.IsErrConnectionRefused(r.err) {
			if refused == nil {
				refused = make(cluster.NodeMap)
			}
			refused[r.si.DaemonID] = r.si
		} else {
			cnt++
		}
	}
	// step 5: handle connection-refused right away
	for i := 0; i < 2; i++ {
		if len(refused) == 0 {
			break
		}

		time.Sleep(config.Timeout.CplaneOperation)
		smap = y.p.smapowner.get()
		if !smap.isPrimary(y.p.si) {
			y.becomeNonPrimary()
			return
		}
		y.handleRefused(urlPath, jsbytes, refused, pairsToSend)
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

// keeping track of per-daemon versioning
func (y *metasyncer) syncDone(sid string, pairs []revspair) {
	revsdaemon := y.revsmap[sid]
	if revsdaemon == nil {
		revsdaemon = make(map[string]int64)
		y.revsmap[sid] = revsdaemon
	}
	for _, revspair := range pairs {
		revs := revspair.revs
		revsdaemon[revs.tag()] = revs.version()
	}
}

func (y *metasyncer) handleRefused(urlPath string, body []byte, refused cluster.NodeMap, pairs []revspair) {
	bcastArgs := bcastCallArgs{
		req: reqArgs{
			method: http.MethodPut,
			path:   urlPath,
			body:   body,
		},
		network: cmn.NetworkIntraControl,
		timeout: cmn.GCO.Get().Timeout.CplaneOperation,
		nodes:   []cluster.NodeMap{refused},
	}
	res := y.p.broadcast(bcastArgs)

	for r := range res {
		if r.err == nil {
			delete(refused, r.si.DaemonID)
			y.syncDone(r.si.DaemonID, pairs)
			glog.Infof("handle-refused: sync-ed %s", r.si)
		} else {
			glog.Warningf("handle-refused: failing to sync %s, err: %v (%d)", r.si, r.err, r.status)
		}
	}
}

// pending (map), if requested, contains only those daemons that need
// to get at least one of the most recently sync-ed tag-ed revs
func (y *metasyncer) pending(needMap bool) (count int, pending cluster.NodeMap) {
	smap := y.p.smapowner.get()
	if !smap.isPrimary(y.p.si) {
		y.becomeNonPrimary()
		return
	}
	for _, serverMap := range []cluster.NodeMap{smap.Tmap, smap.Pmap} {
		for id, si := range serverMap {
			revsdaemon, ok := y.revsmap[id]
			if !ok {
				revsdaemon = make(map[string]int64)
				y.revsmap[id] = revsdaemon
				count++
				if !needMap {
					continue
				}
			} else {
				inSync := true
				for tag, revs := range y.last {
					v, ok := revsdaemon[tag]
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
	count, pending := y.pending(true)
	if count == 0 {
		glog.Infof("no pending revs - all good")
		return
	}

	payload := make(cmn.SimpleKVs)
	pairs := make([]revspair, 0, len(y.last))
	msg := &cmn.ActionMsg{Action: "metasync: handle-pending"} // the same action msg for all
	jsmsg, err := jsoniter.Marshal(msg)
	cmn.Assert(err == nil, err)
	for tag, revs := range y.last {
		body, err := revs.marshal()
		cmn.Assert(err == nil, err)
		payload[tag] = string(body)
		payload[tag+actiontag] = string(jsmsg)
		pairs = append(pairs, revspair{revs, msg})
	}

	body, err := jsoniter.Marshal(payload)
	cmn.Assert(err == nil, err)

	bcastArgs := bcastCallArgs{
		req: reqArgs{
			method: http.MethodPut,
			path:   cmn.URLPath(cmn.Version, cmn.Metasync),
			body:   body,
		},
		network: cmn.NetworkIntraControl,
		timeout: cmn.GCO.Get().Timeout.CplaneOperation,
		nodes:   []cluster.NodeMap{pending},
	}
	res := y.p.broadcast(bcastArgs)
	for r := range res {
		if r.err == nil {
			y.syncDone(r.si.DaemonID, pairs)
			glog.Infof("handle-pending: sync-ed %s", r.si)
		} else {
			cnt++
			glog.Warningf("handle-pending: failing to sync %s, err: %v (%d)", r.si, r.err, r.status)
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
	if !smap.isPresent(y.p.si, true) {
		reason = "present in the Smap"
	}
	lead := "?"
	if smap.ProxySI != nil {
		lead = smap.ProxySI.DaemonID
	}
	glog.Errorf("%s self is not %s (primary=%s, Smap v%d) - failing the 'sync' request", y.p.si, reason, lead, smap.version())
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
