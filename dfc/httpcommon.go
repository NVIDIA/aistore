/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"context"
	"log"
	"net/http"

	"github.com/golang/glog"
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

func invalhdlr(w http.ResponseWriter, r *http.Request) {
	s := errmsgRestApi(http.StatusText(http.StatusBadRequest), r)
	glog.Errorln(s)
	http.Error(w, s, http.StatusBadRequest)
}

func errmsgRestApi(s string, r *http.Request) string {
	s += ": " + r.Method + " " + r.URL.Path + " from " + r.RemoteAddr
	return s
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
	mux     *http.ServeMux
	h       *http.Server
	glogger *log.Logger
}

func (r *httprunner) registerhdlr(path string, handler func(http.ResponseWriter, *http.Request)) {
	if r.mux == nil {
		r.mux = http.NewServeMux()
	}
	r.mux.HandleFunc(path, handler)
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
