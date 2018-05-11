// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/golang/glog"
)

// enumerated REVS types (opaque TBD)
const (
	smaptag   = "smaptag"
	lbmaptag  = "lbmaptag" //
	actiontag = "-action"  // to make a pair (revs, action)
)

// =================== A Brief Theory of Operation =================================
//
// REVS (interface below) stands for REplicated, Versioned and Shared/Synchronized.
//
// A REVS is, typically, an object that represents some sort of cluster-wide metadata
// and, therefore, must be consistently replicated across the entire cluster.
// To that end, the "metasyncer" (metasync.go) provides a generic transport to send
// an arbitrary payload that combines any number of data units that look as follows:
//
//         (shared-object, associated action-message)
//
// The action message (ActionMsg), if present, provides receivers with a context as
// to what exactly to do with the newly received versioned replica.
//
// In addition, storage target in particular make use of the previously synchronized
// version of the cluster map delivered to them by the metasyncer itself (as part of
// the aforementioned action message). Having both the current and the previous
// cluster maps allows targets to figure out whether to rebalance the cluster, and
// how to execute the rebalancing.
//
// In addition, the metasyncer:
//
// 1) tracks already synchronized REVS objects
// 2) validates REVS versions - in particular, prevents attempts to downgrade a
//    newer version
// 3) makes sure that nodes that join the cluster get updated with the current set
//    of REVS replicas
// 4) handles failures to reach existing cluster members - by periodically retrying
//    to update them with the current REVS versions (as long and if those members
//    remain listed in the most current/recent cluster map).
//
// Last but not the least, metasyncer checks that only the currently elected
// leader (aka "primary proxy") distributes the REVS objects, thus providing for
// simple serialization of the versioned updates.
//
// The usage is easy - there is a single sync() method that accepts variable
// number of parameters. Example sync-ing asynchronously without action messages:
//
// 	sync(false, newsmap, p.lbmap.cloneL())
//
// To sync with action message(s) and to block until all the replicas are delivered,
// do:
//
//  	pair := &revspair{ p.smap.cloneU(), &ActionMsg{...} }
//  	sync(true, pair)
//
// On the receiving side, the metasyncer-generated payload gets extracted,
// validated, version-compared, and the corresponding Rx handler gets then called
// with the corresponding REVS replica and additional information that includes
// the action message (and the previous version of the cluster map, if applicable).
//
// =================== end of A Brief Theory of Operation ==========================

type revs interface {
	tag() string                    // known tags enumerated above
	cloneL() interface{}            // clone self - the impl. must take lock if need be
	version() int64                 // version - locking not required
	marshal() (b []byte, err error) // json-marshal - ditto
}

// REVS paired with an action message to provide receivers with additional context
type revspair struct {
	revs revs
	msg  *ActionMsg
}

type metasyncer struct {
	namedrunner
	p      *proxyrunner
	synced struct {
		copies map[string]revs // by tag
	}
	pending struct {
		lock     *sync.Mutex
		diamonds map[string]*daemonInfo
		refused  map[string]*daemonInfo
	}
	chfeed     chan []interface{}
	chfeedwait chan []interface{}
	chstop     chan struct{}
	ticker     *time.Ticker
}

// c-tor
func newmetasyncer(p *proxyrunner) (y *metasyncer) {
	y = &metasyncer{p: p}
	y.synced.copies = make(map[string]revs)
	y.pending.diamonds = make(map[string]*daemonInfo)
	y.pending.lock = &sync.Mutex{}
	y.chstop = make(chan struct{}, 4)
	y.chfeed = make(chan []interface{}, 16)
	y.chfeedwait = make(chan []interface{})

	// create the ticker, do not need to start at start up
	y.ticker = time.NewTicker(time.Duration(time.Hour))
	y.ticker.Stop()
	return
}

func (y *metasyncer) sync(wait bool, revsvec ...interface{}) {
	assert(y.p != nil)
	if !y.p.primary {
		lead := "?"
		if y.p.proxysi != nil {
			lead = y.p.proxysi.DaemonID
		}
		glog.Errorf("%s (self) is not the primary proxy (%s) - cannot distribute REVS", y.p.si.DaemonID, lead)
		return
	}
	// validate
	for _, metaif := range revsvec {
		if _, ok := metaif.(revs); !ok {
			if _, ok = metaif.(*revspair); !ok {
				assert(false, fmt.Sprintf("Expecting revs or revspair, getting %T instead", metaif))
			}
		}
	}

	if wait {
		y.chfeedwait <- revsvec
		<-y.chfeedwait
	} else {
		y.chfeed <- revsvec
	}
}

func (y *metasyncer) run() error {
	glog.Infof("Starting %s", y.name)

	for {
		var npending int
		select {
		case revsvec := <-y.chfeedwait:
			npending = y.dosync(revsvec)
			var s []interface{}
			y.chfeedwait <- s
		case revsvec := <-y.chfeed:
			npending = y.dosync(revsvec)
		case <-y.ticker.C:
			npending = y.handlePending()
		case <-y.chstop:
			y.ticker.Stop()
			return nil
		}

		y.ticker.Stop()
		if npending > 0 {
			y.ticker = time.NewTicker(ctx.config.Periodic.RetrySyncTime)
		}
	}
}

func (y *metasyncer) stop(err error) {
	glog.Infof("Stopping %s, err: %v", y.name, err)

	y.chstop <- struct{}{}
	close(y.chstop)
	close(y.chfeed)
	close(y.chfeedwait)
}

func (y *metasyncer) dosync(revsvec []interface{}) int {
	var (
		smap4bcast, smapSynced *Smap
		jsbytes, jsmsg         []byte
		err                    error
		payload                = make(map[string]string)
		newversions            = make(map[string]revs)
		check4newmembers       bool
	)
	if v, ok := y.synced.copies[smaptag]; ok {
		smapSynced = v.(*Smap)
	}
	for _, metaif := range revsvec {
		var msg = &ActionMsg{}
		// either (revs) or (revs, msg) pair
		revs, ok1 := metaif.(revs)
		if !ok1 {
			mpair, ok2 := metaif.(*revspair)
			assert(ok2)
			revs, msg = mpair.revs, mpair.msg
			if glog.V(3) {
				glog.Infof("dosync tag=%s, msg=%+v", revs.tag(), msg)
			}
		}
		tag := revs.tag()
		jsbytes, err = revs.marshal()
		assert(err == nil, err)
		// new smap always carries the previously sync-ed version (in the action message value field)
		if tag == smaptag {
			assert(msg.Value == nil, "reserved for the previously sync-ed copy")
			if smapSynced != nil {
				msg.Value = smapSynced
			}
		}
		jsmsg, err = json.Marshal(msg)
		assert(err == nil, err)

		payload[tag] = string(jsbytes)
		payload[tag+actiontag] = string(jsmsg) // action message always on the wire even when empty
		newversions[tag] = revs
	}
	jsbytes, err = json.Marshal(payload)
	assert(err == nil, err)

	if v, ok := newversions[smaptag]; ok {
		smap4bcast = v.(*Smap)
		check4newmembers = (smapSynced != nil)
	} else if smapSynced == nil {
		smap4bcast = y.p.smap.cloneL().(*Smap)
	} else if smapSynced.version() != y.p.smap.versionL() {
		assert(smapSynced.version() < y.p.smap.versionL())
		smap4bcast = y.p.smap.cloneL().(*Smap)
		check4newmembers = true
	} else {
		smap4bcast = smapSynced
	}
	y.pending.refused = make(map[string]*daemonInfo)
	urlfmt := fmt.Sprintf("%%s/%s/%s/%s", Rversion, Rdaemon, Rmetasync)
	y.p.broadcast(urlfmt, http.MethodPut, jsbytes, smap4bcast, y.callbackSync, ctx.config.Timeout.CplaneOperation)

	// handle connection-refused right away
	for i := 0; i < 2; i++ {
		if len(y.pending.refused) == 0 {
			break
		}
		time.Sleep(time.Second)
		y.handleRefused(urlfmt, http.MethodPut, jsbytes, smap4bcast)
	}
	// find out smap delta and, if exists, piggy-back on the handle-pending "venue"
	// (which may not be optimal)
	if check4newmembers {
		for sid, si := range smap4bcast.Tmap {
			if _, ok := smapSynced.Tmap[sid]; !ok {
				y.pending.diamonds[sid] = si
			}
		}
		for pid, pi := range smap4bcast.Pmap {
			if _, ok := smapSynced.Pmap[pid]; !ok {
				y.pending.diamonds[pid] = &pi.daemonInfo
			}
		}
	}
	for tag, meta := range newversions {
		y.synced.copies[tag] = meta
	}
	return len(y.pending.diamonds)
}

func (y *metasyncer) callbackSync(res callResult) {
	if res.err == nil {
		return
	}
	glog.Warningf("Failed to sync %s, err: %v (%d)", res.si.DaemonID, res.err, res.status)

	y.pending.lock.Lock()
	y.pending.diamonds[res.si.DaemonID] = res.si
	if IsErrConnectionRefused(res.err) {
		y.pending.refused[res.si.DaemonID] = res.si
	}
	y.pending.lock.Unlock()
}

func (y *metasyncer) handlePending() int {
	var (
		jsbytes []byte
		err     error
	)
	for id := range y.pending.diamonds {
		if !y.p.smap.containsL(id) {
			delete(y.pending.diamonds, id)
		}
	}
	if len(y.pending.diamonds) == 0 {
		glog.Infoln("no pending REVS - cluster synchronized")
		return 0
	}
	payload := make(map[string]string)
	for _, revs := range y.synced.copies {
		jsbytes, err = revs.marshal()
		assert(err == nil, err)
		tag := revs.tag()
		payload[tag] = string(jsbytes)
	}
	jsbytes, err = json.Marshal(payload)
	assert(err == nil, err)

	urlfmt := fmt.Sprintf("%%s/%s/%s/%s", Rversion, Rdaemon, Rmetasync)
	wg := &sync.WaitGroup{}
	for _, si := range y.pending.diamonds {
		wg.Add(1)
		go func(si *daemonInfo) {
			defer wg.Done()
			url := fmt.Sprintf(urlfmt, si.DirectURL)
			res := y.p.call(nil, si, url, http.MethodPut, jsbytes, ctx.config.Timeout.CplaneOperation)
			y.callbackPending(si, res.outjson, res.err, res.status)
		}(si)
	}
	wg.Wait()
	return len(y.pending.diamonds)
}

func (y *metasyncer) callbackPending(si *daemonInfo, _ []byte, err error, status int) {
	if err != nil {
		glog.Warningf("... failing to sync %s, err: %v (%d)", si.DaemonID, err, status)
		return
	}
	y.pending.lock.Lock()
	delete(y.pending.diamonds, si.DaemonID)
	y.pending.lock.Unlock()
}

func (y *metasyncer) handleRefused(urlfmt, method string, jsbytes []byte, smap4bcast *Smap) {
	wg := &sync.WaitGroup{}
	for _, si := range y.pending.refused {
		wg.Add(1)
		go func(si *daemonInfo) {
			defer wg.Done()
			url := fmt.Sprintf(urlfmt, si.DirectURL)
			res := y.p.call(nil, si, url, method, jsbytes, ctx.config.Timeout.CplaneOperation)
			y.callbackRefused(si, res.outjson, res.err, res.status)
		}(si)
	}
	wg.Wait()
}

func (y *metasyncer) callbackRefused(si *daemonInfo, _ []byte, err error, status int) {
	if err != nil {
		glog.Warningf("... failing to sync %s, err: %v (%d)", si.DaemonID, err, status)
		return
	}
	y.pending.lock.Lock()
	delete(y.pending.diamonds, si.DaemonID)
	delete(y.pending.refused, si.DaemonID)
	y.pending.lock.Unlock()
	glog.Infoln("retried & sync-ed", si.DaemonID)
}
