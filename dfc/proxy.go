// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/golang/glog"
)

//===============
//
// REST messaging by proxy
//
//===============
// GET '{"what": "stats"}' /v1/cluster => client
type Allstats struct {
	Proxystats Proxystats            `json:"proxystats"`
	Storstats  map[string]*Storstats `json:"storstats"`
}

//===========================================================================
//
// proxy runner
//
//===========================================================================
type proxyrunner struct {
	httprunner
	starttime   time.Time
	smapversion int64
	confdir     string
	xactinp     *xactInProgress
	lbmap       *lbmap
	syncmapinp  int64
	keepalive   *keepalive
}

// run
func (p *proxyrunner) run() error {
	p.httprunner.init(getproxystats())
	p.keepalive = getkeepaliverunner()
	p.xactinp = newxactinp()
	// local (aka cache-only) buckets
	p.lbmap = &lbmap{LBmap: make(map[string]string), mutex: &sync.Mutex{}}
	lbpathname := p.confdir + "/" + ctx.config.LBConf
	p.lbmap.lock()
	if localLoad(lbpathname, p.lbmap) != nil {
		// create empty
		p.lbmap.Version = 1
		localSave(lbpathname, p.lbmap)
	}
	p.lbmap.unlock()

	// startup: sync local buckets and cluster map when the latter stabilizes
	go p.synchronizeMaps(true)

	//
	// REST API: register proxy handlers and start listening
	//
	p.httprunner.registerhdlr("/"+Rversion+"/"+Rfiles+"/", p.filehdlr)
	p.httprunner.registerhdlr("/"+Rversion+"/"+Rcluster, p.clusterhdlr)
	p.httprunner.registerhdlr("/"+Rversion+"/"+Rcluster+"/", p.clusterhdlr) // FIXME
	p.httprunner.registerhdlr("/", invalhdlr)
	glog.Infof("Proxy %s is ready", p.si.DaemonID)
	glog.Flush()
	p.starttime = time.Now()

	return p.httprunner.run()
}

// stop gracefully
func (p *proxyrunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", p.name, err)
	p.xactinp.abortAll()
	//
	// give targets a limited time to unregister
	//
	version := ctx.smap.versionLocked()
	for i := 0; i < 5; i++ {
		time.Sleep(time.Second)
		v := ctx.smap.versionLocked()
		if version != v {
			version = v
			time.Sleep(time.Second)
			continue
		}
		break
	}
	p.httprunner.stop(err)
}

//==============
//
// http handlers
//
//==============

// handler for: "/"+Rversion+"/"+Rfiles+"/"
func (p *proxyrunner) filehdlr(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		p.httpfilget(w, r)
	case http.MethodPut:
	case http.MethodDelete:
		p.httpfilputdelete(w, r)
	case http.MethodPost:
		p.httpfilpost(w, r)
	default:
		invalhdlr(w, r)
	}
	glog.Flush()
}

// e.g.: GET /v1/files/bucket/object
func (p *proxyrunner) httpfilget(w http.ResponseWriter, r *http.Request) {
	if ctx.smap.count() < 1 {
		s := errmsgRestAPI("No registered targets yet", r)
		glog.Errorln(s)
		http.Error(w, s, http.StatusServiceUnavailable)
		p.statsif.add("numerr", 1)
		return
	}
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rfiles); apitems == nil {
		return
	}

	bucket, objname := apitems[0], ""
	if len(apitems) > 1 {
		objname = apitems[1]
	}
	if strings.Contains(bucket, "/") {
		s := fmt.Sprintf("Invalid bucket name %s (contains '/')", bucket)
		invalmsghdlr(w, r, s)
		p.statsif.add("numerr", 1)
		return
	}
	var si *daemonInfo
	// for bucket listing - any (random) target will do
	if len(objname) == 0 {
		p.statsif.add("numlist", 1)
		for _, si = range ctx.smap.Smap { // see the spec for "map iteration order"
			break
		}
	} else { // CH target selection to execute GET bucket/objname
		p.statsif.add("numget", 1)
		si = hrwTarget(bucket+"/"+objname, ctx.smap)
	}
	redirecturl := si.DirectURL + r.URL.Path
	if glog.V(3) {
		glog.Infof("Redirecting %q to %s", r.URL.Path, si.DirectURL)
	}
	if !ctx.config.Proxy.Passthru && len(objname) > 0 {
		glog.Infof("passthru=false: proxy initiates the GET %s/%s", bucket, objname)
		p.receiveDrop(w, r, redirecturl) // ignore error, proceed to http redirect
	}
	if len(objname) != 0 {
		http.Redirect(w, r, redirecturl, http.StatusMovedPermanently)
	} else {
		// NOTE:
		//       code 307 is the only way to http-redirect with the
		//       original JSON payload (GetMsg - see REST.go)
		http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)
	}
}

// receiveDrop reads until EOF and uses dummy writer (ReadToNull)
func (p *proxyrunner) receiveDrop(w http.ResponseWriter, r *http.Request, redirecturl string) error {
	if glog.V(3) {
		glog.Infof("GET redirect URL %q", redirecturl)
	}
	newr, err := http.Get(redirecturl)
	if err != nil {
		glog.Errorf("Failed to GET redirect URL %q, err: %v", redirecturl, err)
		return err
	}
	defer func() {
		if newr != nil {
			err = newr.Body.Close()
		}
	}()
	bufreader := bufio.NewReader(newr.Body)
	bytes, err := ReadToNull(bufreader)
	if err != nil {
		glog.Errorf("Failed to copy data to http, URL %q, err: %v", redirecturl, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		p.statsif.add("numerr", 1)
		return err
	}
	if glog.V(3) {
		glog.Infof("Received and discarded %q (size %.2f MB)", redirecturl, float64(bytes)/1000/1000)
	}
	return err
}

func (p *proxyrunner) httpfilputdelete(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 2, Rversion, Rfiles); apitems == nil {
		return
	}
	bucket, objname := apitems[0], ""
	objname = strings.Join(apitems[1:], "/")
	if glog.V(3) {
		glog.Infof("Bucket %s objname %s", bucket, objname)
	}
	si := hrwTarget(bucket+"/"+objname, ctx.smap)
	redirecturl := si.DirectURL + r.URL.Path
	glog.Infof("RedirectURL %s", redirecturl)
	fmt.Fprintf(w, "%s", redirecturl)
	return
}

// { action } "/"+Rversion+"/"+Rfiles + "/" + localbucket
func (p *proxyrunner) httpfilpost(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rfiles); apitems == nil {
		return
	}
	lbucket := apitems[0]
	if strings.Contains(lbucket, "/") {
		s := fmt.Sprintf("Invalid local bucket name %s (contains '/')", lbucket)
		invalmsghdlr(w, r, s)
		p.statsif.add("numerr", 1)
		return
	}
	var msg ActionMsg
	if p.readJSON(w, r, &msg) != nil {
		return
	}
	lbpathname := p.confdir + "/" + ctx.config.LBConf
	switch msg.Action {
	case ActCreateLB:
		p.lbmap.lock()
		if !p.lbmap.add(lbucket) {
			s := fmt.Sprintf("Local bucket %s already exists", lbucket)
			invalmsghdlr(w, r, s)
			p.statsif.add("numerr", 1)
			p.lbmap.unlock()
			return
		}
	case ActRemoveLB:
		p.lbmap.lock()
		if !p.lbmap.del(lbucket) {
			s := fmt.Sprintf("Local bucket %s does not exist, nothing to remove", lbucket)
			invalmsghdlr(w, r, s)
			p.statsif.add("numerr", 1)
			p.lbmap.unlock()
			return
		}
	case ActSyncLB:
		p.lbmap.lock()
	default:
		s := fmt.Sprintf("Unexpected ActionMsg <- JSON [%v]", msg)
		invalmsghdlr(w, r, s)
		return
	}
	localSave(lbpathname, p.lbmap)
	p.lbmap.unlock()

	go p.synchronizeMaps(false)
}

//===========================
//
// control plane
//
//===========================

// handler for: "/"+Rversion+"/"+Rcluster
func (p *proxyrunner) clusterhdlr(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		p.httpcluget(w, r)
	case http.MethodPost:
		p.httpclupost(w, r)
	case http.MethodDelete:
		p.httpcludel(w, r)
	case http.MethodPut:
		p.httpcluput(w, r)
	default:
		invalhdlr(w, r)
	}
	glog.Flush()
}

// gets target info
func (p *proxyrunner) httpcluget(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 0, Rversion, Rcluster); apitems == nil {
		return
	}
	var msg GetMsg
	if p.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.GetWhat {
	case GetWhatConfig:
		jsbytes, err := json.Marshal(ctx.smap)
		assert(err == nil)
		w.Header().Set("Content-Type", "application/json")
		w.Write(jsbytes)
	case GetWhatStats:
		getstatsmsg, err := json.Marshal(msg) // same message to all targets
		assert(err == nil, err)
		p.httpclugetstats(w, r, getstatsmsg)
	default:
		s := fmt.Sprintf("Unexpected GetMsg <- JSON [%v]", msg)
		invalmsghdlr(w, r, s)
	}
}

// FIXME: run this in a goroutine
func (p *proxyrunner) httpclugetstats(w http.ResponseWriter, r *http.Request, getstatsmsg []byte) {
	var out Allstats
	out.Storstats = make(map[string]*Storstats, ctx.smap.count())
	getproxystatsrunner().syncstats(&out.Proxystats)
	for _, si := range ctx.smap.Smap {
		stats := Storstats{}
		out.Storstats[si.DaemonID] = &stats
		url := si.DirectURL + "/" + Rversion + "/" + Rdaemon
		outjson, err := p.call(url, r.Method, getstatsmsg)
		if err != nil {
			s := fmt.Sprintf("Failed to call target %s (is it alive?)", url)
			p.statsif.add("numerr", 1)
			http.Error(w, s, http.StatusGone)
			// ask keepalive to work on it
			p.keepalive.checknow <- err
			return
		}
		err = json.Unmarshal(outjson, &stats)
		assert(err == nil, err)
	}
	jsbytes, err := json.Marshal(&out)
	assert(err == nil, err)
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsbytes)
}

// registers a new target
func (p *proxyrunner) httpclupost(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 0, Rversion, Rcluster); apitems == nil {
		return
	}
	var si daemonInfo
	if p.readJSON(w, r, &si) != nil {
		return
	}
	if net.ParseIP(si.NodeIPAddr) == nil {
		s := "Cannot register: invalid target IP " + si.NodeIPAddr
		s = errmsgRestAPI(s, r)
		glog.Errorln(s)
		http.Error(w, s, http.StatusBadRequest)
		return
	}
	p.statsif.add("numpost", 1)
	ctx.smap.lock()
	if ctx.smap.get(si.DaemonID) != nil {
		glog.Errorf("Warning: duplicate target ID %s", si.DaemonID)
		// fall through
	}
	ctx.smap.add(&si)
	ctx.smap.unlock()
	if glog.V(3) {
		glog.Infof("Registered target ID %s (count %d)", si.DaemonID, ctx.smap.count())
	}
	go p.synchronizeMaps(false)
}

// unregisters a target
func (p *proxyrunner) httpcludel(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 2, Rversion, Rcluster); apitems == nil {
		return
	}
	if apitems[0] != Rdaemon {
		s := fmt.Sprintf("Invalid API element: %s (expecting %s)", apitems[0], Rdaemon)
		invalmsghdlr(w, r, s)
		return
	}
	sid := apitems[1]
	p.statsif.add("numdelete", 1)
	ctx.smap.lock()
	if ctx.smap.get(sid) == nil {
		glog.Errorf("Unknown target %s", sid)
		ctx.smap.unlock()
		return
	}
	ctx.smap.del(sid)
	ctx.smap.unlock()
	//
	// TODO: startup -- leave --
	//
	if glog.V(3) {
		glog.Infof("Unregistered target {%s} (count %d)", sid, ctx.smap.count())
	}
	go p.synchronizeMaps(false)
}

// '{"action": "shutdown"}' /v1/cluster => (proxy) =>
// '{"action": "syncsmap"}' /v1/cluster => (proxy) => PUT '{Smap}' /v1/daemon/syncsmap => target(s)
// '{"action": "rebalance"}' /v1/cluster => (proxy) => PUT '{Smap}' /v1/daemon/rebalance => target(s)
func (p *proxyrunner) httpcluput(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 0, Rversion, Rcluster); apitems == nil {
		return
	}
	var msg ActionMsg
	if p.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case ActShutdown:
		glog.Infoln("Proxy-controlled cluster shutdown...")
		msgbytes, err := json.Marshal(msg) // same message -> this target
		assert(err == nil, err)
		for _, si := range ctx.smap.Smap {
			url := si.DirectURL + "/" + Rversion + "/" + Rdaemon
			glog.Infof("%s: %s", msg.Action, url)
			p.call(url, http.MethodPut, msgbytes)
		}
		time.Sleep(time.Second)
		syscall.Kill(syscall.Getpid(), syscall.SIGINT)

	case ActSyncSmap:
		fallthrough
	case ActionRebalance:
		if ctx.smap.syncversion == ctx.smap.Version {
			glog.Infof("Smap (v%d) is already in sync with the targets", ctx.smap.syncversion)
			return
		}
		go p.synchronizeMaps(false)

	default:
		s := fmt.Sprintf("Unexpected ActionMsg <- JSON [%v]", msg)
		invalmsghdlr(w, r, s)
	}
}

//========================
//
// delayed broadcasts
//
//========================
const (
	syncmapsdelay        = time.Second * 3
	maxsyncmapsdelay     = time.Second * 10
	syncmapsstartupdelay = time.Second * 7
)

// TODO: proxy.stop() must terminate this routine
func (p *proxyrunner) synchronizeMaps(startingup bool) {
	aval := time.Now().Unix()
	if !atomic.CompareAndSwapInt64(&p.syncmapinp, 0, aval) {
		glog.Infof("synchronizeMaps is already running")
		return
	}
	defer atomic.CompareAndSwapInt64(&p.syncmapinp, aval, 0)

	if startingup {
		time.Sleep(syncmapsstartupdelay)
	}
	ctx.smap.lock()
	p.lbmap.lock()
	lbversion := p.lbmap.version()
	smapversion := ctx.smap.version()
	delay := syncmapsdelay
	if lbversion == p.lbmap.syncversion && smapversion == ctx.smap.syncversion {
		glog.Infof("Smap (v%d) and lbmap (v%d) are already in sync with the targets",
			smapversion, lbversion)
		p.lbmap.unlock()
		ctx.smap.unlock()
		return
	}
	p.lbmap.unlock()
	ctx.smap.unlock()
	smapversion_orig := smapversion
	time.Sleep(time.Second)
	for {
		lbv := p.lbmap.versionLocked()
		if lbversion != lbv {
			lbversion = lbv
			time.Sleep(delay)
			continue
		}
		smv := ctx.smap.versionLocked()
		if smapversion != smv {
			smapversion = smv
			// NOTE: double the sleep when the targets keep (un)registering
			//       - but only to a point
			if delay < maxsyncmapsdelay {
				delay += delay
			}
			time.Sleep(delay)
			continue
		}
		// finally:
		// change in the cluster map warrants the broadcast of every other config that
		// must be shared across the cluster;
		// the opposite it not true though, that's why the check below
		p.httpfilput_lb()
		if smapversion_orig != smapversion {
			p.httpcluput_smap(Rsyncsmap)
		}
		break
	}
	ctx.smap.lock()
	p.lbmap.lock()
	ctx.smap.syncversion = smapversion
	p.lbmap.syncversion = lbversion
	p.lbmap.unlock()
	ctx.smap.unlock()
	glog.Infof("Smap (v%d) and lbmap (v%d) are now in sync with the targets", smapversion, lbversion)
}

func (p *proxyrunner) httpcluput_smap(action string) {
	method := http.MethodPut
	assert(action == Rebalance || action == Rsyncsmap)
	ctx.smap.lock()
	jsbytes, err := json.Marshal(ctx.smap)
	ctx.smap.unlock()
	assert(err == nil, err)
	for _, si := range ctx.smap.Smap {
		url := si.DirectURL + "/" + Rversion + "/" + Rdaemon + "/" + action
		glog.Infof("%s: %s", action, url)
		_, err := p.call(url, method, jsbytes)
		if err != nil {
			// ask keepalive to work on it
			p.keepalive.checknow <- err
			return
		}
	}
	ctx.smap.syncversion = ctx.smap.Version
}

func (p *proxyrunner) httpfilput_lb() {
	p.lbmap.lock()
	jsbytes, err := json.Marshal(p.lbmap)
	assert(err == nil, err)
	p.lbmap.unlock()

	for _, si := range ctx.smap.Smap {
		url := si.DirectURL + "/" + Rversion + "/" + Rdaemon + "/" + Rsynclb
		glog.Infof("%s: %+v", url, p.lbmap)
		p.call(url, http.MethodPut, jsbytes)
		if err != nil {
			// ask keepalive to work on it
			p.keepalive.checknow <- err
			return
		}
	}
	p.lbmap.syncversion = p.lbmap.Version
}
