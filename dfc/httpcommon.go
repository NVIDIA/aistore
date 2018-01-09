/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
)

const (
	maxidleconns   = 20              // max num idle connections
	requesttimeout = 5 * time.Second // http timeout
)

// REST API
const (
	apiversion    = "v1"
	apiresfiles   = "files"
	apirestargets = "targets"
)

//===========================================================================
//
// http request parsing helpers
//
//===========================================================================
func checkRestAPI(w http.ResponseWriter, r *http.Request, apitems []string, n int, ver, res string) bool {
	if len(apitems) > 0 && ver != "" && apitems[0] != ver {
		glog.Errorf("Invalid API version: %s (expecting %s)", apitems[0], ver)
		invalhdlr(w, r)
		return false
	}
	if len(apitems) > 1 && res != "" && apitems[1] != res {
		glog.Errorf("Invalid API resource: %s (expecting %s)", apitems[1], res)
		invalhdlr(w, r)
		return false
	}
	if len(apitems) < n {
		glog.Errorf("Invalid API request: num elements %d (expecting at least %d)", len(apitems), n)
		invalhdlr(w, r)
		return false
	}
	return true
}

// FIXME: revisit the following 3 methods, and make consistent
func invalhdlr(w http.ResponseWriter, r *http.Request) {
	s := errmsgRestApi(http.StatusText(http.StatusBadRequest), r)
	glog.Errorln(s)
	http.Error(w, s, http.StatusBadRequest)
}

func errmsgRestApi(s string, r *http.Request) string {
	s += ": " + r.Method + " " + r.URL.Path + " from " + r.RemoteAddr
	return s
}

// FIXME: http.StatusInternalServerError - here and elsewehere
// FIXME: numerr - differentiate
func webinterror(w http.ResponseWriter, errstr string) error {
	glog.Errorln(errstr)
	http.Error(w, errstr, http.StatusInternalServerError)
	stats := getstorstats()
	atomic.AddInt64(&stats.numerr, 1)
	return errors.New(errstr)
}

//===========================================================================
//
// http runner
//
//===========================================================================
type glogwriter struct {
}

func (r *glogwriter) Write(p []byte) (int, error) {
	n := len(p)
	s := string(p[:n])
	glog.Errorln(s)
	return n, nil
}

type httprunner struct {
	namedrunner
	mux        *http.ServeMux
	h          *http.Server
	glogger    *log.Logger
	si         *ServerInfo
	httpclient *http.Client // http client for intra-cluster comm
}

func (r *httprunner) registerhdlr(path string, handler func(http.ResponseWriter, *http.Request)) {
	if r.mux == nil {
		r.mux = http.NewServeMux()
	}
	r.mux.HandleFunc(path, handler)
}

func (r *httprunner) init() error {
	ipaddr, err := getipaddr() // FIXME: this must change
	if err != nil {
		return err
	}
	// http client
	r.httpclient = &http.Client{
		Transport: &http.Transport{MaxIdleConnsPerHost: maxidleconns},
		Timeout:   requesttimeout,
	}
	// init ServerInfo here
	r.si = &ServerInfo{}
	r.si.NodeIPAddr = ipaddr
	r.si.DaemonPort = ctx.config.Listen.Port
	r.si.DaemonID = ipaddr + ":" + ctx.config.Listen.Port
	r.si.DirectURL = "http://" + r.si.NodeIPAddr + ":" + r.si.DaemonPort
	return nil
}

func (r *httprunner) run() error {
	// a wrapper to glog http.Server errors - otherwise
	// os.Stderr would be used, as per golang.org/pkg/net/http/#Server
	r.glogger = log.New(&glogwriter{}, "net/http err: ", 0)

	portstring := ":" + ctx.config.Listen.Port
	r.h = &http.Server{Addr: portstring, Handler: r.mux, ErrorLog: r.glogger}
	if err := r.h.ListenAndServe(); err != nil {
		if err != http.ErrServerClosed {
			glog.Errorf("Terminated %s with err: %v", r.name, err)
			return err
		}
	}
	return nil
}

// stop gracefully
func (r *httprunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.name, err)

	contextwith, _ := context.WithTimeout(context.Background(), ctx.config.HttpTimeout)

	err = r.h.Shutdown(contextwith)
	if err != nil {
		glog.Infof("Stopped %s, err: %v", r.name, err)
	}
}

// intra-cluster IPC, control plane
// http-REST calls another target or a proxy
// optionally, sends a json-encoded content to the callee
// expects only OK or FAIL in the return
func (r *httprunner) call(url string, method string, jsbytes []byte) (err error) {
	var (
		request  *http.Request
		response *http.Response
	)
	if jsbytes == nil || len(jsbytes) == 0 {
		request, err = http.NewRequest(method, url, nil)
		if glog.V(3) {
			glog.Infof("%s URL %q", method, url)
		}
	} else {
		request, err = http.NewRequest(method, url, bytes.NewBuffer(jsbytes))
		if glog.V(3) {
			glog.Infof("%s URL %q, json %s", method, url, string(jsbytes))
		}
		if err == nil {
			request.Header.Set("Content-Type", "application/json")
		}
	}
	if err != nil {
		glog.Errorf("Unexpected failure to create http request %s %s, err: %v", method, url, err)
		return err
	}
	response, err = r.httpclient.Do(request)
	if err != nil || response == nil {
		glog.Errorf("Failed to register with proxy, err: %v", err)
		return err
	}
	defer func() {
		if response != nil {
			err = response.Body.Close()
		}
	}()
	// block until done (note: returned content is ignored and discarded)
	if _, err = ioutil.ReadAll(response.Body); err != nil {
		glog.Errorf("Couldn't parse response body, err: %v", err)
		return err
	}
	return err
}
