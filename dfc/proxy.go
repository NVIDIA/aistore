/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"bufio"
	"encoding/json"
	"html"
	"io/ioutil"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
)

// proxyfilehdlr
func proxyfilehdlr(w http.ResponseWriter, r *http.Request) {
	assert(r.Method == http.MethodGet)
	stats := getproxystats()
	atomic.AddInt64(&stats.numget, 1)

	if ctx.smap.count() < 1 {
		s := errmsgRestApi("No registered targets yet", r)
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
	si := ctx.smap.get(sid)
	assert(si != nil, "race NIY")

	redirecturl := si.DirectURL + urlpath
	if glog.V(3) {
		glog.Infof("Redirecting %q to %s", urlpath, si.DirectURL)
	}
	if !ctx.config.Proxy.Passthru {
		glog.Infoln("Proxy will invoke the GET (ctx.config.Proxy.Passthru = false)")
		getAndDrop(w, r, redirecturl) // ignore error, proceed to http redirect
	}
	// FIXME: https, HTTP2 here and elsewhere
	http.Redirect(w, r, redirecturl, http.StatusMovedPermanently)
}

// proxyreghdlr
func proxyreghdlr(w http.ResponseWriter, r *http.Request) {
	stats := getproxystats()
	if r.Method == http.MethodPost {
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
		var si ServerInfo
		b, err := ioutil.ReadAll(r.Body)
		defer r.Body.Close()
		if err == nil {
			err = json.Unmarshal(b, &si)
		}
		if err != nil {
			glog.Errorf("Failed to json-unmarshal POST request, err: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			atomic.AddInt64(&stats.numerr, 1)
			return
		}
		atomic.AddInt64(&stats.numpost, 1)
		if ctx.smap.get(si.DaemonID) != nil {
			glog.Errorf("Duplicate target {%s}", si.DaemonID)
		}
		ctx.smap.add(&si)
		if glog.V(3) {
			glog.Infof("Registered target {%s} (count %d)", si.DaemonID, ctx.smap.count())
		}
	} else {
		assert(r.Method == http.MethodDelete)
		urlpath := html.EscapeString(r.URL.Path)
		split := strings.SplitN(urlpath, "/", 5)
		apitems := split[1:]
		if !checkRestAPI(w, r, apitems, 4, apiversion, apirestargets) {
			atomic.AddInt64(&stats.numerr, 1)
			return
		}
		sid := apitems[3]
		atomic.AddInt64(&stats.numdelete, 1)
		if ctx.smap.get(sid) == nil {
			glog.Errorf("Unknown target {%s}", sid)
			return
		}
		ctx.smap.del(sid)
		if glog.V(3) {
			glog.Infof("Unregistered target {%s} (count %d)", sid, ctx.smap.count())
		}
	}
}

// getAndDrop reads until EOF and uses dummy writer (ReadToNull)
func getAndDrop(w http.ResponseWriter, r *http.Request, redirecturl string) error {
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
		glog.Errorf("Failed to copy data to http response, URL %q, err: %v", redirecturl, err)
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
	r.httprunner.init()
	//
	// REST API: register proxy handlers and start listening
	//
	r.httprunner.registerhdlr("/"+apiversion+"/"+apiresfiles+"/", proxyfilehdlr)
	r.httprunner.registerhdlr("/"+apiversion+"/"+apirestargets+"/", proxyreghdlr)
	r.httprunner.registerhdlr("/", invalhdlr)
	return r.httprunner.run()
}

// stop gracefully
func (r *proxyrunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.name, err)
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
	r.httprunner.stop(err)
}
