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
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

// enumerated REVS types (opaque TBD)
const (
	smaptag     = "smaptag"
	bucketmdtag = "bucketmdtag" //
	actiontag   = "-action"     // to make a pair (revs, action)
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
// 	sync(false, newsmap, bucketmd.clone())
//
// To sync with action message(s) and to block until all the replicas are delivered,
// do:
//
//  	pair := &revspair{ smap, &ActionMsg{...} }
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
		diamonds map[string]*daemonInfo
		refused  map[string]*daemonInfo
	}
	chfeed     chan []interface{}
	chfeedwait chan []interface{}
	chstop     chan struct{}
	retryTimer *time.Timer
}

// c-tor
func newmetasyncer(p *proxyrunner) (y *metasyncer) {
	y = &metasyncer{p: p}
	y.synced.copies = make(map[string]revs)
	y.pending.diamonds = make(map[string]*daemonInfo)
	y.chstop = make(chan struct{}, 4)
	y.chfeed = make(chan []interface{}, 16)
	y.chfeedwait = make(chan []interface{})

	y.retryTimer = time.NewTimer(time.Duration(time.Hour))
	// no retry to run yet
	y.retryTimer.Stop()
	return
}

func (y *metasyncer) sync(wait bool, revsvec ...interface{}) {
	smap := y.p.smapowner.get()
	if !smap.isPrimary(y.p.si) {
		reason := "the primary"
		if !smap.isPresent(y.p.si, true) {
			reason = "present in the Smap"
		}
		lead := "?"
		if smap.ProxySI != nil {
			lead = smap.ProxySI.DaemonID
		}
		glog.Errorf("%s self is not %s (primary=%s, Smap v%d) - failing the 'sync' request",
			y.p.si.DaemonID, reason, lead, smap.version())
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
		select {
		case revsvec, ok := <-y.chfeedwait:
			if ok {
				y.doSync(revsvec)
				var s []interface{}
				y.chfeedwait <- s
			}
		case revsvec, ok := <-y.chfeed:
			if ok {
				y.doSync(revsvec)
			}
		case <-y.retryTimer.C:
			y.handlePending()
		case <-y.chstop:
			y.retryTimer.Stop()
			return nil
		}

		if len(y.pending.diamonds) > 0 {
			y.retryTimer.Reset(ctx.config.Periodic.RetrySyncTime)
		} else {
			y.retryTimer.Stop()
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

func (y *metasyncer) doSync(revsvec []interface{}) {
	var (
		smap4bcast, smapSynced *Smap
		jsbytes, jsmsg         []byte
		err                    error
		payload                = make(simplekvs)
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
				glog.Infof("dosync %s, version=%d, action=%s", revs.tag(), revs.version(), msg.Action)
			}
		} else {
			if glog.V(3) {
				glog.Infof("dosync %s, version=%d", revs.tag(), revs.version())
			}
		}
		tag := revs.tag()
		jsbytes, err = revs.marshal()
		assert(err == nil, err)
		// new smap always carries the previously sync-ed version (in the action message value field)
		if tag == smaptag {
			assert(msg.Value == nil, "reserved for the previously sync-ed copy")
			if smapSynced != nil {
				// note: this assignment modifies the original msg's value field
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

	smap := y.p.smapowner.get()
	if v, ok := newversions[smaptag]; ok {
		smap4bcast = v.(*Smap)
		check4newmembers = (smapSynced != nil)
	} else if smapSynced == nil {
		smap4bcast = smap
	} else if smapSynced.version() != smap.version() {
		if smapSynced.version() > smap.version() {
			s := fmt.Sprintf("%s: sync-ed Smap v%d > v%d the current", y.p.si.DaemonID, smapSynced.version(), smap.version())
			assert(false, s)
		}
		smap4bcast = smap
		check4newmembers = true
	} else {
		smap4bcast = smapSynced
	}

	y.pending.refused = make(map[string]*daemonInfo)
	urlPath := URLPath(Rversion, Rdaemon, Rmetasync)

	res := y.p.broadcastCluster(
		urlPath,
		nil, // query
		http.MethodPut,
		jsbytes,
		smap4bcast,
		ctx.config.Timeout.CplaneOperation,
	)

	for r := range res {
		if r.err == nil {
			continue
		}

		glog.Warningf("Failed to sync %s, err: %v (%d)", r.si.DaemonID, r.err, r.status)

		y.pending.diamonds[r.si.DaemonID] = r.si
		if IsErrConnectionRefused(r.err) {
			y.pending.refused[r.si.DaemonID] = r.si
		}
	}

	// handle connection-refused right away
	for i := 0; i < 2; i++ {
		if len(y.pending.refused) == 0 {
			break
		}

		time.Sleep(time.Second)
		y.handleRefused(urlPath, jsbytes)
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
				y.pending.diamonds[pid] = pi
			}
		}
	}

	for tag, meta := range newversions {
		y.synced.copies[tag] = meta
	}
}

func (y *metasyncer) handlePending() {
	var (
		jsbytes []byte
		err     error
	)

	smap := y.p.smapowner.get()
	for id := range y.pending.diamonds {
		if !smap.containsID(id) {
			delete(y.pending.diamonds, id)
		}
	}

	if len(y.pending.diamonds) == 0 {
		glog.Infoln("no pending REVS - cluster synchronized")
		return
	}

	payload := make(simplekvs)
	for _, revs := range y.synced.copies {
		jsbytes, err = revs.marshal()
		assert(err == nil, err)
		tag := revs.tag()
		payload[tag] = string(jsbytes)
	}

	jsbytes, err = json.Marshal(payload)
	assert(err == nil, err)

	var servers []*daemonInfo
	for _, s := range y.pending.diamonds {
		servers = append(servers, s)
	}

	res := y.p.broadcast(
		URLPath(Rversion, Rdaemon, Rmetasync),
		nil, // query
		http.MethodPut,
		jsbytes,
		servers,
		ctx.config.Timeout.CplaneOperation,
	)

	for r := range res {
		if r.err != nil {
			glog.Warningf("... failing to sync %s, err: %v (%d)", r.si.DaemonID, r.err, r.status)
		} else {
			delete(y.pending.diamonds, r.si.DaemonID)
		}
	}
}

func (y *metasyncer) handleRefused(urlPath string, body []byte) {
	var servers []*daemonInfo
	for _, s := range y.pending.refused {
		servers = append(servers, s)
	}

	res := y.p.broadcast(urlPath, nil, http.MethodPut, body,
		servers, ctx.config.Timeout.CplaneOperation)

	for r := range res {
		if r.err != nil {
			glog.Warningf("... failing to sync %s, err: %v (%d)", r.si.DaemonID, r.err, r.status)
		} else {
			delete(y.pending.diamonds, r.si.DaemonID)
			delete(y.pending.refused, r.si.DaemonID)
			glog.Infoln("retried & sync-ed", r.si.DaemonID)
		}
	}
}
