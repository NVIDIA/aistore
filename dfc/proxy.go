/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"bufio"
	"html"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
)

var (
	httpClient *http.Client
)

const (
	maxidleconns   = 20              // max num idle connections
	requesttimeout = 5 * time.Second // http timeout
)

const (
	nodeIPAddr = "nodeIPAddr" // daemon's IP address
	daemonPort = "daemonPort" // expecting an integer > 1000
	daemonID   = "daemonID"   // node ID must be unique
)

// createHTTPClient
func createHTTPClient() *http.Client {
	client := &http.Client{
		Transport: &http.Transport{MaxIdleConnsPerHost: maxidleconns},
		Timeout:   requesttimeout,
	}
	return client
}

// proxyfilehdlr
func proxyfilehdlr(w http.ResponseWriter, r *http.Request) {
	assert(r.Method == http.MethodGet)
	stats := getproxystats()
	atomic.AddInt64(&stats.numget, 1)

	if len(ctx.smap) < 1 {
		s := errmsgRestApi("No registered targets", r)
		glog.Errorln(s)
		http.Error(w, s, http.StatusServiceUnavailable)
		atomic.AddInt64(&stats.numerr, 1)
		return
	}
	//
	// parse and validate
	//
	urlpath := html.EscapeString(r.URL.Path)
	split := strings.SplitN(urlpath, "/", 5)
	apitems := split[1:]
	if !checkRestAPI(w, r, apitems, 3, apiversion, apiresfiles) {
		atomic.AddInt64(&stats.numerr, 1)
		return
	}
	// skip ver and resource
	sid := hrwTarget(strings.Join(apitems[2:], "/"))

	if !ctx.config.Proxy.Passthru {
		getAndDrop(sid, w, r) // ignore error, proceed to http redirect
	}
	if glog.V(3) {
		glog.Infof("Redirecting %q to %s:%s", urlpath, ctx.smap[sid].ip, ctx.smap[sid].port)
	}
	// FIXME: https, HTTP2 here and elsewhere
	redirecturl := "http://" + ctx.smap[sid].ip + ":" + ctx.smap[sid].port + urlpath
	http.Redirect(w, r, redirecturl, http.StatusMovedPermanently)
}

// proxyreghdlr
func proxyreghdlr(w http.ResponseWriter, r *http.Request) {
	assert(r.Method == http.MethodPost)
	stats := getproxystats()
	atomic.AddInt64(&stats.numpost, 1)
	err := r.ParseForm()
	if err != nil {
		glog.Errorf("Failed to parse POST request, err: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		atomic.AddInt64(&stats.numerr, 1)
		return
	}
	//
	// parse and validate
	//
	urlpath := html.EscapeString(r.URL.Path)
	split := strings.SplitN(urlpath, "/", 5)
	apitems := split[1:]
	if !checkRestAPI(w, r, apitems, 3, apiversion, apirestargets) {
		atomic.AddInt64(&stats.numerr, 1)
		return
	}
	// fill-in server registration
	var sinfo serverinfo
	for str, val := range r.Form {
		if str == nodeIPAddr {
			if glog.V(3) {
				glog.Infof("val : %s", strings.Join(val, ""))
			}
			sinfo.ip = strings.Join(val, "")
		}
		if str == daemonPort {
			if glog.V(3) {
				glog.Infof("val : %s", strings.Join(val, ""))
			}
			sinfo.port = strings.Join(val, "")
		}
		if str == daemonID {
			if glog.V(3) {
				glog.Infof("val : %s", strings.Join(val, ""))
			}
			sinfo.id = strings.Join(val, "")
		}
	}
	_, ok := ctx.smap[sinfo.id]
	assert(!ok, "Duplicate target ID: "+sinfo.id)
	ctx.smap[sinfo.id] = sinfo
	if glog.V(3) {
		glog.Infof("Registered IP %s port %s ID %s (count %d)", sinfo.ip, sinfo.port, sinfo.id, len(ctx.smap))
	}
}

// getAndDrop
func getAndDrop(sid string, w http.ResponseWriter, r *http.Request) error {
	if glog.V(3) {
		glog.Infof("Request path %s sid %s port %s",
			html.EscapeString(r.URL.Path), sid, ctx.smap[sid].port)
	}
	urlpath := html.EscapeString(r.URL.Path)
	redirecturl := "http://" + ctx.smap[sid].ip + ":" + ctx.smap[sid].port + urlpath
	if glog.V(3) {
		glog.Infof("GET redirect URL %q", redirecturl)
	}
	newr, err := http.Get(redirecturl)
	if err != nil {
		glog.Errorf("Failed to GET redirect URL %q, err: %v", redirecturl, err)
		return err
	}
	defer func() {
		err = newr.Body.Close()
	}()
	bufreader := bufio.NewReader(newr.Body)
	bytes, err := ReadToNull(bufreader)
	if err != nil {
		glog.Errorf("Failed to copy data to http response, URL %q, err: %v", urlpath, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		stats := getproxystats()
		atomic.AddInt64(&stats.numerr, 1)
		return err
	}
	if glog.V(3) {
		glog.Infof("Received and discarded %q (size %.2f MB)", redirecturl, float64(bytes)/1000/1000)
	}
	return err
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
func (r *proxyrunner) run() error {
	//
	// REST API: register proxy handlers and start listening
	//
	r.httprunner.registerhdlr("/"+apiversion+"/"+apiresfiles+"/", proxyfilehdlr)
	r.httprunner.registerhdlr("/"+apiversion+"/"+apirestargets+"/", proxyreghdlr)
	r.httprunner.registerhdlr("/", invalhdlr)
	return r.httprunner.run()
}
