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
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/golang/glog"
)

// enumerated REVS types (opaque TBD)
const (
	smaptag  = "smaptag"
	lbmaptag = "lbmaptag"
	//
	actiontag = "-action" // to make a pair (revs, action)
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
	t      *targetrunner
	h      *httprunner
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
func newmetasyncer(p *proxyrunner, t *targetrunner) (y *metasyncer) {
	y = &metasyncer{p: p, t: t}
	if p == nil {
		y.h = &t.httprunner
		return
	}
	assert(t == nil)
	y.h = &p.httprunner
	y.synced.copies = make(map[string]revs)
	y.pending.diamonds = make(map[string]*daemonInfo)
	y.pending.lock = &sync.Mutex{}
	y.chstop = make(chan struct{}, 4)
	y.chfeed = make(chan []interface{}, 16)
	y.chfeedwait = make(chan []interface{}, 16)
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
	// feed
	if wait {
		y.chfeedwait <- revsvec
		<-y.chfeedwait
	} else {
		y.chfeed <- revsvec
	}
	return
}

func (y *metasyncer) run() error {
	glog.Infof("Starting %s", y.name)
	var (
		ticking, stopped bool
	)
	for !stopped {
		if ticking {
			ticking, stopped = y.selectWithTicker()
		} else {
			ticking, stopped = y.selectWithoutTicker()
		}
	}
	return nil
}

func (y *metasyncer) selectWithTicker() (ticking, stopped bool) {
	var npending int
	ticking = true
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
		stopped = true
	}
	if npending == 0 {
		y.ticker.Stop()
		ticking = false
	}
	return
}

func (y *metasyncer) selectWithoutTicker() (ticking, stopped bool) {
	var npending int
	select {
	case revsvec := <-y.chfeedwait:
		npending = y.dosync(revsvec)
		var s []interface{}
		y.chfeedwait <- s
	case revsvec := <-y.chfeed:
		npending = y.dosync(revsvec)
	case <-y.chstop:
		stopped = true
	}
	if npending > 0 && !stopped {
		y.ticker = time.NewTicker(ctx.config.Periodic.RetrySyncTime)
		ticking = true
	}
	return
}

func (y *metasyncer) stop(err error) {
	glog.Infof("Stopping %s, err: %v", y.name, err)
	var v struct{}
	y.chstop <- v
	close(y.chstop)
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
	if _, ok := y.synced.copies[smaptag]; ok {
		v := y.synced.copies[smaptag]
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

	if _, ok := newversions[smaptag]; ok {
		v := newversions[smaptag]
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

func (y *metasyncer) callbackSync(si *daemonInfo, _ []byte, err error, status int) {
	if err == nil {
		return
	}
	glog.Warningf("Failed to sync %s, err: %v (%d)", si.DaemonID, err, status)
	y.pending.lock.Lock()
	y.pending.diamonds[si.DaemonID] = si
	if IsErrConnectionRefused(err) {
		y.pending.refused[si.DaemonID] = si
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
			r, err, _, status := y.p.call(si, url, http.MethodPut, jsbytes, ctx.config.Timeout.CplaneOperation)
			y.callbackPending(si, r, err, status)
		}(si)
	}
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
			r, err, _, status := y.p.call(si, url, method, jsbytes, ctx.config.Timeout.CplaneOperation)
			y.callbackRefused(si, r, err, status)
		}(si)
	}
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

//=====================================================================
//
// Rx
//
//=====================================================================
func (y *metasyncer) receive(w http.ResponseWriter, r *http.Request) {
	if y.p != nil && y.p.primary {
		y.p.invalmsghdlr(w, r,
			fmt.Sprintf("Primary proxy (self=%s) cannot receive REVS objects - election in progress?", y.p.si.DaemonID))
		return
	}
	var payload = make(map[string]string)
	if y.h.readJSON(w, r, &payload) != nil {
		return
	}
	// validate
	for tag := range payload {
		switch tag {
		case smaptag:
		case smaptag + actiontag:
		case lbmaptag:
		case lbmaptag + actiontag:
		default:
			assert(false, "Unknown or not yet implemented metatag '"+tag+"'")
		}
	}
	// extract
	newsmap, oldsmap, actionsmap, errstr := y.extractsmap(payload)
	if errstr != "" {
		y.h.invalmsghdlr(w, r, errstr)
		return
	}
	newlbmap, actionlb, errstr := y.extractlbmap(payload)
	if errstr != "" {
		y.h.invalmsghdlr(w, r, errstr)
		return
	}
	if newsmap != nil {
		if y.p != nil {
			errstr = y.p.receivesmap(newsmap, oldsmap, actionsmap)
		} else {
			errstr = y.t.receivesmap(newsmap, oldsmap, actionsmap)
		}
		if errstr != "" {
			y.h.invalmsghdlr(w, r, errstr)
		}
	}
	if newlbmap != nil {
		if y.p != nil {
			y.p.receivelbmap(newlbmap, actionlb)
		} else {
			y.t.receivelbmap(newlbmap, actionlb)
		}
	}
}

func (y *metasyncer) extractsmap(payload map[string]string) (newsmap, oldsmap *Smap, msg *ActionMsg, errstr string) {
	if _, ok := payload[smaptag]; !ok {
		return
	}
	newsmap, oldsmap, msg = &Smap{}, &Smap{}, &ActionMsg{}
	smapvalue := payload[smaptag]
	msgvalue := ""
	if err := json.Unmarshal([]byte(smapvalue), newsmap); err != nil {
		errstr = fmt.Sprintf("Failed to unmarshal new smap, value (%+v, %T), err: %v", smapvalue, smapvalue, err)
		return
	}
	if _, ok := payload[smaptag+actiontag]; ok {
		msgvalue = payload[smaptag+actiontag]
		if err := json.Unmarshal([]byte(msgvalue), msg); err != nil {
			errstr = fmt.Sprintf("Failed to unmarshal action message, value (%+v, %T), err: %v", msgvalue, msgvalue, err)
			return
		}
	}
	if glog.V(3) {
		if msg.Action == "" {
			glog.Infof("extract Smap ver=%d, msg=<nil>", newsmap.version())
		} else {
			glog.Infof("extract Smap ver=%d, action=%s", newsmap.version(), msg.Action)
		}
	}
	myver := y.h.smap.versionL()
	if newsmap.version() == myver {
		newsmap = nil
		return
	}
	if newsmap.version() < myver {
		errstr = fmt.Sprintf("Attempt to downgrade smap version %d to %d", myver, newsmap.version())
		return
	}
	if msgvalue == "" {
		// synchronize with no action message and no old smap
		return
	}
	// old smap
	oldsmap.Tmap = make(map[string]*daemonInfo)
	oldsmap.Pmap = make(map[string]*proxyInfo)
	if msg.Value == nil {
		return
	}
	v1, ok1 := msg.Value.(map[string]interface{})
	assert(ok1, fmt.Sprintf("msg (%+v, %T), msg.Value (%+v, %T)", msg, msg, msg.Value, msg.Value))
	v2, ok2 := v1["tmap"]
	assert(ok2)
	tmapif, ok3 := v2.(map[string]interface{})
	assert(ok3)
	v4, ok4 := v1["pmap"]
	assert(ok4)
	pmapif, ok5 := v4.(map[string]interface{})
	assert(ok5)
	versionf := v1["version"].(float64)

	// partial restore of the old smap - keeping only the respective DaemonIDs and version
	for sid := range tmapif {
		oldsmap.Tmap[sid] = &daemonInfo{}
	}
	for pid := range pmapif {
		oldsmap.Pmap[pid] = &proxyInfo{}
	}
	oldsmap.Version = int64(versionf)
	return
}

func (y *metasyncer) extractlbmap(payload map[string]string) (newlbmap *lbmap, msg *ActionMsg, errstr string) {
	if _, ok := payload[lbmaptag]; !ok {
		return
	}
	newlbmap, msg = &lbmap{}, &ActionMsg{}
	lbmapvalue := payload[lbmaptag]
	msgvalue := ""
	if err := json.Unmarshal([]byte(lbmapvalue), newlbmap); err != nil {
		errstr = fmt.Sprintf("Failed to unmarshal new lbmap, value (%+v, %T), err: %v", lbmapvalue, lbmapvalue, err)
		return
	}
	if _, ok := payload[lbmaptag+actiontag]; ok {
		msgvalue = payload[lbmaptag+actiontag]
		if err := json.Unmarshal([]byte(msgvalue), msg); err != nil {
			errstr = fmt.Sprintf("Failed to unmarshal action message, value (%+v, %T), err: %v", msgvalue, msgvalue, err)
			return
		}
	}
	if glog.V(3) {
		if msg.Action == "" {
			glog.Infof("extract lbmap ver=%d, msg=<nil>", newlbmap.version())
		} else {
			glog.Infof("extract lbmap ver=%d, msg=%+v", newlbmap.version(), msg)
		}
	}
	myver := y.h.lbmap.versionL()
	if newlbmap.version() == myver {
		newlbmap = nil
		return
	}
	if newlbmap.version() < myver {
		errstr = fmt.Sprintf("Attempt to downgrade lbmap version %d to %d", myver, newlbmap.version())
	}
	return
}

//======================================================================================
//
// concrete proxy and target Rx handlers - FIXME: make use of the received ActiveMsg
//
//======================================================================================
func (p *proxyrunner) receivesmap(newsmap, oldsmap *Smap, msg *ActionMsg) (errstr string) {
	if msg.Action == "" {
		glog.Infof("receive Smap: version %d (old %d), ntargets %d", newsmap.version(), oldsmap.version(), len(newsmap.Tmap))
	} else {
		glog.Infof("receive Smap: version %d (old %d), ntargets %d, action %s",
			newsmap.version(), oldsmap.version(), len(newsmap.Tmap), msg.Action)
	}
	existentialQ := (newsmap.getProxy(p.si.DaemonID) != nil)
	if !existentialQ {
		errstr = fmt.Sprintf("FATAL: new Smap does not contain the proxy %s = self", p.si.DaemonID)
		return
	}
	err := p.setPrimaryProxyAndSmapL(newsmap)
	if err != nil {
		errstr = fmt.Sprintf("Failed to update primary proxy and Smap, err: %v", err)
	}
	return
}

func (t *targetrunner) receivesmap(newsmap, oldsmap *Smap, msg *ActionMsg) (errstr string) {
	var (
		newtargetid  string
		existentialQ bool
	)
	if msg.Action == "" {
		glog.Infof("receive Smap: version %d (old %d), ntargets %d", newsmap.version(), oldsmap.version(), len(newsmap.Tmap))
	} else {
		glog.Infof("receive Smap: version %d (old %d), ntargets %d, message %+v", newsmap.version(), oldsmap.version(), len(newsmap.Tmap), msg)
	}
	newlen := len(newsmap.Tmap)
	oldlen := len(oldsmap.Tmap)

	// check whether this target is present in the new Smap
	// rebalance? (nothing to rebalance if the new map is a strict subset of the old)
	// assign proxysi
	// log
	infoln := make([]string, 0, newlen)
	for id, si := range newsmap.Tmap { // log
		if id == t.si.DaemonID {
			existentialQ = true
			infoln = append(infoln, fmt.Sprintf("target: %s <= self", si))
		} else {
			infoln = append(infoln, fmt.Sprintf("target: %s", si))
		}
		if oldlen == 0 {
			continue
		}
		if _, ok := oldsmap.Tmap[id]; !ok {
			if newtargetid != "" {
				glog.Warningf("More than one new target (%s, %s) in the new Smap?", newtargetid, id)
			}
			newtargetid = id
		}
	}
	if !existentialQ {
		errstr = fmt.Sprintf("FATAL: new Smap does not contain the target %s = self", t.si.DaemonID)
		return
	}

	smapLock.Lock()
	orig := t.smap
	t.smap = newsmap
	err := t.setPrimaryProxy(newsmap.ProxySI.DaemonID, "" /* primaryToRemove */, false /* prepare */)
	smap4xaction := newsmap.cloneU()
	if err != nil {
		t.smap = orig
		smapLock.Unlock()
		for _, ln := range infoln {
			glog.Infoln(ln)
		}
		errstr = fmt.Sprintf("Failed to receive Smap, err: %v", err)
		return
	}
	smapLock.Unlock()

	for _, ln := range infoln {
		glog.Infoln(ln)
	}
	if msg.Action == ActRebalance {
		go t.runRebalance(smap4xaction, newtargetid)
		return
	}
	if oldlen == 0 {
		return
	}
	if newtargetid == "" {
		if newlen != oldlen {
			assert(newlen < oldlen)
			glog.Infoln("nothing to rebalance: new Smap is a strict subset of the old")
		} else {
			glog.Infof("nothing to rebalance: num (%d) and IDs of the targets did not change", newlen)
		}
		return
	}
	if !ctx.config.Rebalance.Enabled {
		glog.Infoln("auto-rebalancing disabled")
		return
	}
	uptime := time.Since(t.starttime())
	if uptime < ctx.config.Rebalance.StartupDelayTime && t.si.DaemonID != newtargetid {
		glog.Infof("not auto-rebalancing: uptime %v < %v", uptime, ctx.config.Rebalance.StartupDelayTime)
		aborted, running := t.xactinp.isAbortedOrRunningRebalance()
		if aborted && !running {
			f := func() {
				time.Sleep(ctx.config.Rebalance.StartupDelayTime - uptime)
				currsmap := t.smap.cloneL().(*Smap)
				t.runRebalance(currsmap, "")
			}
			go runRebalanceOnce.Do(f) // only once at startup
		}
		return
	}
	// xaction
	go t.runRebalance(smap4xaction, newtargetid)
	return
}

func (p *proxyrunner) receivelbmap(newlbmap *lbmap, msg *ActionMsg) {
	if msg.Action == "" {
		glog.Infof("receive lbmap: version %d", newlbmap.version())
	} else {
		glog.Infof("receive lbmap: version %d, message %+v", newlbmap.version(), msg)
	}
	lbmapLock.Lock()
	defer lbmapLock.Unlock()
	p.lbmap = newlbmap
	lbpathname := filepath.Join(p.confdir, lbname)
	if err := LocalSave(lbpathname, p.lbmap); err != nil {
		glog.Errorf("Failed to store lbmap %s, err: %v", lbpathname, err)
	}
}

// FIXME: use the message
func (t *targetrunner) receivelbmap(newlbmap *lbmap, msg *ActionMsg) {
	if msg.Action == "" {
		glog.Infof("receive lbmap: version %d", newlbmap.version())
	} else {
		glog.Infof("receive lbmap: version %d, message %+v", newlbmap.version(), msg)
	}
	lbmapLock.Lock()
	defer lbmapLock.Unlock()
	for bucket := range t.lbmap.LBmap {
		_, ok := newlbmap.LBmap[bucket]
		if !ok {
			glog.Infof("Destroy local bucket %s", bucket)
			for mpath := range ctx.mountpaths.Available {
				localbucketfqn := filepath.Join(makePathLocal(mpath), bucket)
				if err := os.RemoveAll(localbucketfqn); err != nil {
					glog.Errorf("Failed to destroy local bucket dir %q, err: %v", localbucketfqn, err)
				}
			}
		}
	}
	t.lbmap = newlbmap
	for mpath := range ctx.mountpaths.Available {
		for bucket := range t.lbmap.LBmap {
			localbucketfqn := filepath.Join(makePathLocal(mpath), bucket)
			if err := CreateDir(localbucketfqn); err != nil {
				glog.Errorf("Failed to create local bucket dir %q, err: %v", localbucketfqn, err)
			}
		}
	}
}
