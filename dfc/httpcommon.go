/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/OneOfOne/xxhash"
	"github.com/golang/glog"
)

const (
	maxidleconns   = 20              // max num idle connections
	requesttimeout = 5 * time.Second // http timeout
)

// RESTful URL path: /v1/....
const (
	Rversion  = "v1"
	Rfiles    = "files"
	Rcluster  = "cluster"
	Rdaemon   = "daemon"
	Rsyncsmap = "syncsmap"
)

//===========================================================================
//
// http request parsing helpers
//
//===========================================================================
func restApiItems(unescapedpath string, maxsplit int) []string {
	escaped := html.EscapeString(unescapedpath)
	split := strings.SplitN(escaped, "/", maxsplit)
	apitems := make([]string, 0, len(split))
	for i := 0; i < len(split); i++ {
		if split[i] != "" { // omit empty
			apitems = append(apitems, split[i])
		}
	}
	return apitems
}

// remove validated fields and return the resulting slice
func checkRestAPI(w http.ResponseWriter, r *http.Request, apitems []string, n int, ver, res string) []string {
	if len(apitems) > 0 && ver != "" {
		if apitems[0] != ver {
			s := fmt.Sprintf("Invalid API version: %s (expecting %s)", apitems[0], ver)
			invalmsghdlr(w, r, s)
			return nil
		}
		apitems = apitems[1:]
	}
	if len(apitems) > 0 && res != "" {
		if apitems[0] != res {
			s := fmt.Sprintf("Invalid API resource: %s (expecting %s)", apitems[0], res)
			invalmsghdlr(w, r, s)
			return nil
		}
		apitems = apitems[1:]
	}
	if len(apitems) < n {
		s := fmt.Sprintf("Invalid API request: num elements %d (expecting at least %d [%v])", len(apitems), n, apitems)
		invalmsghdlr(w, r, s)
		return nil
	}
	return apitems
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

func invalmsghdlr(w http.ResponseWriter, r *http.Request, specific string) {
	s := http.StatusText(http.StatusBadRequest) + ": " + specific
	s += ": " + r.Method + " " + r.URL.Path + " from " + r.RemoteAddr
	glog.Errorln(s)
	http.Error(w, s, http.StatusBadRequest)
}

// FIXME: http.StatusInternalServerError - here and elsewehere
// FIXME: numerr - differentiate
func webinterror(w http.ResponseWriter, errstr string) error {
	glog.Errorln(errstr)
	http.Error(w, errstr, http.StatusInternalServerError)
	stats := getstorstats()
	statsAdd(&stats.Numerr, 1)
	return errors.New(errstr)
}

func readJson(w http.ResponseWriter, r *http.Request, out interface{}) error {
	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err == nil {
		err = json.Unmarshal(b, out)
	}
	if err != nil {
		s := fmt.Sprintf("Failed to json-unmarshal %s request, err: %v [%v]", r.Method, err, string(b))
		invalmsghdlr(w, r, s)
		return err
	}
	return nil
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

	// NOTE: generate and assign ID and URL here
	split := strings.Split(ipaddr, ".")
	cs := xxhash.ChecksumString32S(split[len(split)-1], LCG32)
	r.si.DaemonID = strconv.Itoa(int(cs&0xffff)) + ":" + ctx.config.Listen.Port
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
func (r *httprunner) call(url string, method string, injson []byte) (outjson []byte, err error) {
	var (
		request  *http.Request
		response *http.Response
	)
	if injson == nil || len(injson) == 0 {
		request, err = http.NewRequest(method, url, nil)
		if glog.V(3) {
			glog.Infof("%s URL %q", method, url)
		}
	} else {
		request, err = http.NewRequest(method, url, bytes.NewBuffer(injson))
		if glog.V(3) {
			glog.Infof("%s URL %q, json %s", method, url, string(injson))
		}
		if err == nil {
			request.Header.Set("Content-Type", "application/json")
		}
	}
	if err != nil {
		glog.Errorf("Unexpected failure to create http request %s %s, err: %v", method, url, err)
		return nil, err
	}
	response, err = r.httpclient.Do(request)
	if err != nil || response == nil {
		return nil, err
	}
	defer func() {
		if response != nil {
			err = response.Body.Close()
		}
	}()
	// block until done (note: returned content is ignored and discarded)
	if outjson, err = ioutil.ReadAll(response.Body); err != nil {
		glog.Errorf("Failed to read http, err: %v", err)
		return nil, err
	}
	return outjson, err
}
