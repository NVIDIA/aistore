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
}

// run
func (p *proxyrunner) run() error {
	p.httprunner.init(getproxystats())
	//
	// REST API: register proxy handlers and start listening
	//
	p.httprunner.registerhdlr("/"+Rversion+"/"+Rfiles+"/", p.filehdlr)
	p.httprunner.registerhdlr("/"+Rversion+"/"+Rcluster, p.clusterhdlr)
	p.httprunner.registerhdlr("/"+Rversion+"/"+Rcluster+"/", p.clusterhdlr) // FIXME
	p.httprunner.registerhdlr("/", invalhdlr)
	glog.Infof("Proxy %s is ready", p.si.DaemonID)
	glog.Flush()
	return p.httprunner.run()
}

// stop gracefully
func (p *proxyrunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", p.name, err)
	//
	// give targets a limited chance to unregister
	//
	version := ctx.smap.version()
	for i := 0; i < 5; i++ {
		time.Sleep(time.Second)
		v := ctx.smap.version()
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
	default:
		invalhdlr(w, r)
	}
}

func (p *proxyrunner) httpfilget(w http.ResponseWriter, r *http.Request) {
	p.statsif.add("numget", 1)

	if ctx.smap.count() < 1 {
		s := errmsgRestApi("No registered targets yet", r)
		glog.Errorln(s)
		http.Error(w, s, http.StatusServiceUnavailable)
		p.statsif.add("numerr", 1)
		return
	}
	apitems := p.restApiItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rfiles); apitems == nil {
		return
	}
	sid := hrwTarget(strings.Join(apitems, "/"))
	si := ctx.smap.get(sid)
	assert(si != nil, "race NIY")

	redirecturl := si.DirectURL + r.URL.Path
	if glog.V(3) {
		glog.Infof("Redirecting %q to %s", r.URL.Path, si.DirectURL)
	}
	if !ctx.config.Proxy.Passthru {
		glog.Infoln("Proxy will invoke the GET (ctx.config.Proxy.Passthru = false)")
		p.receiveDrop(w, r, redirecturl) // ignore error, proceed to http redirect
	}
	// FIXME: https, HTTP2 here and elsewhere
	http.Redirect(w, r, redirecturl, http.StatusMovedPermanently)
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
	apitems := p.restApiItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 0, Rversion, Rcluster); apitems == nil {
		return
	}
	var msg GetMsg
	if p.readJson(w, r, &msg) != nil {
		return
	}
	switch msg.What {
	case GetConfig:
		jsbytes, err := json.Marshal(ctx.smap)
		assert(err == nil)
		w.Header().Set("Content-Type", "application/json")
		w.Write(jsbytes)
	case GetStats:
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
	out.Storstats = make(map[string]*Storstats, len(ctx.smap.Smap))
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
	apitems := p.restApiItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 0, Rversion, Rcluster); apitems == nil {
		return
	}
	var si ServerInfo
	if p.readJson(w, r, &si) != nil {
		return
	}
	if net.ParseIP(si.NodeIPAddr) == nil {
		s := "Cannot register: invalid target IP " + si.NodeIPAddr
		s = errmsgRestApi(s, r)
		glog.Errorln(s)
		http.Error(w, s, http.StatusBadRequest)
		return
	}
	p.statsif.add("numpost", 1)
	if ctx.smap.get(si.DaemonID) != nil {
		glog.Errorf("Duplicate target {%s}", si.DaemonID)
	}
	ctx.smap.add(&si)
	if glog.V(3) {
		glog.Infof("Registered target {%s} (count %d)", si.DaemonID, ctx.smap.count())
	}
}

// unregisters a target
func (p *proxyrunner) httpcludel(w http.ResponseWriter, r *http.Request) {
	apitems := p.restApiItems(r.URL.Path, 5)
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
	if ctx.smap.get(sid) == nil {
		glog.Errorf("Unknown target {%s}", sid)
		return
	}
	ctx.smap.del(sid)
	if glog.V(3) {
		glog.Infof("Unregistered target {%s} (count %d)", sid, ctx.smap.count())
	}
}

func (p *proxyrunner) httpcluput(w http.ResponseWriter, r *http.Request) {
	apitems := p.restApiItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 0, Rversion, Rcluster); apitems == nil {
		return
	}
	var msg ActionMsg
	if p.readJson(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case ActionShutdown:
		glog.Infoln("Proxy-controlled cluster shutdown...")
		msgbytes, err := json.Marshal(msg) // same message -> this target
		assert(err == nil, err)
		for _, si := range ctx.smap.Smap {
			url := si.DirectURL + "/" + Rversion + "/" + Rdaemon
			p.call(url, http.MethodPut, msgbytes)
		}
		time.Sleep(time.Second)
		syscall.Kill(syscall.Getpid(), syscall.SIGINT)

	case ActionSyncSmap:
	case ActionRebalance:
		// PUT '{"action": "syncsmap"}' /v1/cluster => (proxy) => PUT '{Smap}' /v1/daemon/syncsmap => target(s)
		jsbytes, err := json.Marshal(ctx.smap)
		assert(err == nil, err)
		for _, si := range ctx.smap.Smap {
			url := si.DirectURL + "/" + Rversion + "/" + Rdaemon + "/" + msg.Action
			_, err := p.call(url, r.Method, jsbytes)
			assert(err == nil, err)
		}

	default:
		s := fmt.Sprintf("Unexpected ActionMsg <- JSON [%v]", msg)
		invalmsghdlr(w, r, s)
	}
}
