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
}

// run
func (p *proxyrunner) run() error {
	p.httprunner.init(getproxystats())
	p.xactinp = newxactinp()
	// local (aka cache-only) buckets
	p.lbmap = &lbmap{LBmap: make(map[string]string), mutex: &sync.Mutex{}}
	lbpathname := p.confdir + "/" + ctx.config.LocalBuckets
	p.lbmap.lock()
	if localLoad(lbpathname, p.lbmap) != nil {
		// create empty
		p.lbmap.Version = 1
		localSave(lbpathname, p.lbmap)
	}
	p.lbmap.unlock()

	// startup:
	// 	delayed sync local buckets and cluster map as soon as the latter
	// 	stabilizes
	go func() {
		ctx.smap.lock()
		smapversion := ctx.smap.version()
		ctx.smap.unlock()
		time.Sleep(time.Second * 5)
		for {
			ctx.smap.lock()
			v := ctx.smap.version()
			ctx.smap.unlock()
			if smapversion != v {
				smapversion = v
				time.Sleep(time.Second * 3)
				continue
			}
			p.httpfilput_lb()
			p.httpcluput_smap(http.MethodPut, Rsyncsmap)
			break
		}

	}()

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
	ctx.smap.lock()
	version := ctx.smap.version()
	ctx.smap.unlock()
	for i := 0; i < 5; i++ {
		time.Sleep(time.Second)
		ctx.smap.lock()
		v := ctx.smap.version()
		ctx.smap.unlock()
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
		p.httpfilput(w, r)
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
	if !ctx.config.Proxy.Passthru {
		glog.Infoln("Proxy will invoke the GET (ctx.config.Proxy.Passthru = false)")
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

// 3-way copy
func (p *proxyrunner) httpfilput(w http.ResponseWriter, r *http.Request) {
	assert(false, "NIY")
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
	lbpathname := p.confdir + "/" + ctx.config.LocalBuckets
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

	// delay bcast until both lbmap and smap stabilize
	go func() {
		p.lbmap.lock()
		lbversion := p.lbmap.version()
		p.lbmap.unlock()
		ctx.smap.lock()
		smapversion := ctx.smap.version()
		ctx.smap.unlock()
		time.Sleep(time.Second)
		for {
			p.lbmap.lock()
			lbv := p.lbmap.version()
			p.lbmap.unlock()
			if lbversion != lbv {
				lbversion = lbv
				time.Sleep(time.Second * 3)
				continue
			}
			ctx.smap.lock()
			smv := ctx.smap.version()
			ctx.smap.unlock()
			if smapversion != smv {
				smapversion = smv
				time.Sleep(time.Second * 3)
				continue
			}
			// finally
			p.httpfilput_lb()
			break
		}
	}()
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
		assert(err == nil, err)
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
	//
	// TODO: startup -- join -- (startup N + join 1 + shutdown + startup N+1)
	// TODO: sync local buckets and cluster map, possibly rebalance as well
	//
	if glog.V(3) {
		glog.Infof("Registered target ID %s (count %d)", si.DaemonID, ctx.smap.count())
	}
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
		// delay bcast until smap stabilizes
		go func() {
			for {
				ctx.smap.lock()
				smapversion := ctx.smap.version()
				ctx.smap.unlock()
				time.Sleep(time.Second)
				for {
					ctx.smap.lock()
					v := ctx.smap.version()
					ctx.smap.unlock()
					if smapversion != v {
						smapversion = v
						time.Sleep(time.Second * 3)
						continue
					}
					p.httpcluput_smap(http.MethodPut, msg.Action)
					break
				}
			}
		}()

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
func (p *proxyrunner) httpcluput_smap(method, action string) {
	assert(action == Rebalance || action == Rsyncsmap)
	ctx.smap.lock()
	jsbytes, err := json.Marshal(ctx.smap)
	ctx.smap.unlock()
	assert(err == nil, err)
	for _, si := range ctx.smap.Smap {
		url := si.DirectURL + "/" + Rversion + "/" + Rdaemon + "/" + action
		glog.Infof("%s: %s", action, url)
		_, err := p.call(url, method, jsbytes)
		assert(err == nil, err)
	}
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
	}
}
