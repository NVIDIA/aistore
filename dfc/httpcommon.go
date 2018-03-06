// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/OneOfOne/xxhash"
	"github.com/golang/glog"
	"github.com/hkwi/h2c"
)

const (
	maxidleconns   = 20              // max num idle connections
	requesttimeout = 5 * time.Second // http timeout
)
const (
	initialBucketListSize = 512
)

//===========
//
// interfaces
//
//===========
type cloudif interface {
	listbucket(bucket string, msg *GetMsg) (jsbytes []byte, errstr string, errcode int)
	getobj(fqn, bucket, objname string) (nhobj cksumvalue, size int64, errstr string, errcode int)
	putobj(file *os.File, bucket, objname string, ohobj cksumvalue) (errstr string, errcode int)
	deleteobj(bucket, objname string) (errstr string, errcode int)
}

//===========
//
// generic bad-request http handler
//
//===========
func invalhdlr(w http.ResponseWriter, r *http.Request) {
	s := http.StatusText(http.StatusBadRequest)
	s += ": " + r.Method + " " + r.URL.Path + " from " + r.RemoteAddr
	glog.Errorln(s)
	http.Error(w, s, http.StatusBadRequest)
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
	si         *daemonInfo
	httpclient *http.Client // http client for intra-cluster comm
	statsif    statsif
	kalive     kaliveif
}

func (r *httprunner) registerhdlr(path string, handler func(http.ResponseWriter, *http.Request)) {
	if r.mux == nil {
		r.mux = http.NewServeMux()
	}
	r.mux.HandleFunc(path, handler)
}

func (r *httprunner) init(s statsif) {
	r.statsif = s
	ipaddr, errstr := getipaddr() // FIXME: this must change
	if errstr != "" {
		glog.Fatalf("FATAL: %s", errstr)
	}
	// http client
	r.httpclient = &http.Client{
		Transport: &http.Transport{MaxIdleConnsPerHost: maxidleconns},
		Timeout:   requesttimeout,
	}
	// init daemonInfo here
	r.si = &daemonInfo{}
	r.si.NodeIPAddr = ipaddr
	r.si.DaemonPort = ctx.config.Listen.Port

	id := os.Getenv("DFCDAEMONID")
	if id != "" {
		r.si.DaemonID = id
	} else {
		split := strings.Split(ipaddr, ".")
		cs := xxhash.ChecksumString32S(split[len(split)-1], mLCG32)
		r.si.DaemonID = strconv.Itoa(int(cs&0xffff)) + ":" + ctx.config.Listen.Port
	}

	r.si.DirectURL = "http://" + r.si.NodeIPAddr + ":" + r.si.DaemonPort
	return
}

func (r *httprunner) run() error {
	// a wrapper to glog http.Server errors - otherwise
	// os.Stderr would be used, as per golang.org/pkg/net/http/#Server
	r.glogger = log.New(&glogwriter{}, "net/http err: ", 0)
	var handler http.Handler
	handler = r.mux
	if ctx.config.H2c {
		handler = h2c.Server{Handler: handler}
	}

	portstring := ":" + ctx.config.Listen.Port
	r.h = &http.Server{Addr: portstring, Handler: handler, ErrorLog: r.glogger}
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

	contextwith, cancel := context.WithTimeout(context.Background(), ctx.config.HTTPTimeout)
	defer cancel()

	if r.h == nil {
		return
	}
	err = r.h.Shutdown(contextwith)
	if err != nil {
		glog.Infof("Stopped %s, err: %v", r.name, err)
	}
}

// intra-cluster IPC, control plane; calls (via http) another target or a proxy
// optionally, sends a json-encoded body to the callee
func (r *httprunner) call(si *daemonInfo, url, method string, injson []byte,
	timeout ...time.Duration) (outjson []byte, err error, errstr string, status int) {
	var (
		request  *http.Request
		response *http.Response
		sid      = "unknown"
		timer    *time.Timer
		cancelch chan struct{}
	)
	if si != nil {
		sid = si.DaemonID
	}
	if injson == nil || len(injson) == 0 {
		request, err = http.NewRequest(method, url, nil)
		if glog.V(3) {
			glog.Infof("%s URL %q", method, url)
		}
	} else {
		request, err = http.NewRequest(method, url, bytes.NewBuffer(injson))
		if err == nil {
			request.Header.Set("Content-Type", "application/json")
		}
	}
	if err != nil {
		errstr = fmt.Sprintf("Unexpected failure to create http request %s %s, err: %v", method, url, err)
		return
	}
	if len(timeout) > 0 {
		cancelch = make(chan struct{})
		timer = time.AfterFunc(timeout[0], func() {
			close(cancelch)
			cancelch = nil
		})
		request.Cancel = cancelch
	}
	response, err = r.httpclient.Do(request)
	// For a timer created with AfterFunc(d, f), if t.Stop returns false, then the timer
	// has already expired and the function f has been started in its own goroutine (time/sleep.go)
	if timer != nil && timer.Stop() && cancelch != nil {
		close(cancelch)
	}
	if err != nil {
		if response != nil && response.StatusCode > 0 {
			errstr = fmt.Sprintf("Failed to http-call %s (%s %s): status %s, err %v", sid, method, url, response.Status, err)
			status = response.StatusCode
			return
		}
		errstr = fmt.Sprintf("Failed to http-call %s (%s %s): err %v", sid, method, url, err)
		return
	}
	assert(response != nil, "Unexpected: nil response with no error")
	defer func() { err = response.Body.Close() }()

	if outjson, err = ioutil.ReadAll(response.Body); err != nil {
		errstr = fmt.Sprintf("Failed to http-call %s (%s %s): Unexpected failure to read response body: %v", sid, method, url, err)
		return
	}
	if sid != "unknown" {
		r.kalive.timestamp(sid)
	}
	return
}

//=============================
//
// http request parsing helpers
//
//=============================
func (r *httprunner) restAPIItems(unescapedpath string, maxsplit int) []string {
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
func (h *httprunner) checkRestAPI(w http.ResponseWriter, r *http.Request, apitems []string, n int, ver, res string) []string {
	if len(apitems) > 0 && ver != "" {
		if apitems[0] != ver {
			s := fmt.Sprintf("Invalid API version: %s (expecting %s)", apitems[0], ver)
			h.invalmsghdlr(w, r, s)
			return nil
		}
		apitems = apitems[1:]
	}
	if len(apitems) > 0 && res != "" {
		if apitems[0] != res {
			s := fmt.Sprintf("Invalid API resource: %s (expecting %s)", apitems[0], res)
			h.invalmsghdlr(w, r, s)
			return nil
		}
		apitems = apitems[1:]
	}
	if len(apitems) < n {
		s := fmt.Sprintf("Invalid API request: num elements %d (expecting at least %d [%v])", len(apitems), n, apitems)
		h.invalmsghdlr(w, r, s)
		return nil
	}
	return apitems
}

func (h *httprunner) readJSON(w http.ResponseWriter, r *http.Request, out interface{}) error {
	b, err := ioutil.ReadAll(r.Body)
	errclose := r.Body.Close()
	if err == nil && errclose != nil {
		err = errclose
	}
	if err == nil {
		err = json.Unmarshal(b, out)
	}
	if err != nil {
		s := fmt.Sprintf("Failed to json-unmarshal %s request, err: %v [%v]", r.Method, err, string(b))
		h.invalmsghdlr(w, r, s)
		return err
	}
	return nil
}

func (h *httprunner) writeJSON(w http.ResponseWriter, r *http.Request, jsbytes []byte, tag string) (errstr string) {
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(jsbytes); err != nil {
		errstr = fmt.Sprintf("%s: Unexpected failure to write json to HTTP reply, err: %v", tag, err)
		h.invalmsghdlr(w, r, errstr)
	}
	return
}

//=================
//
// commong set config
//
//=================
func (h *httprunner) setconfig(name, value string) (errstr string) {
	lm, hm := ctx.config.LRUConfig.LowWM, ctx.config.LRUConfig.HighWM
	checkwm := false
	atoi := func(value string) (uint32, error) {
		v, err := strconv.Atoi(value)
		return uint32(v), err
	}
	switch name {
	case "stats_time":
		if v, err := time.ParseDuration(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse stats_time, err: %v", err)
		} else {
			ctx.config.StatsTime, ctx.config.StatsTimeStr = v, value
		}
	case "dont_evict_time":
		if v, err := time.ParseDuration(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse dont_evict_time, err: %v", err)
		} else {
			ctx.config.LRUConfig.DontEvictTime, ctx.config.LRUConfig.DontEvictTimeStr = v, value
		}
	case "lowwm":
		if v, err := atoi(value); err != nil {
			errstr = fmt.Sprintf("Failed to convert lowwm, err: %v", err)
		} else {
			ctx.config.LRUConfig.LowWM, checkwm = v, true
		}
	case "highwm":
		if v, err := atoi(value); err != nil {
			errstr = fmt.Sprintf("Failed to convert highwm, err: %v", err)
		} else {
			ctx.config.LRUConfig.HighWM, checkwm = v, true
		}
	case "passthru":
		if v, err := strconv.ParseBool(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse passthru (proxy-only), err: %v", err)
		} else {
			ctx.config.Proxy.Passthru = v
		}
	case "lru_enabled":
		if v, err := strconv.ParseBool(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse lru_enabled, err: %v", err)
		} else {
			ctx.config.LRUConfig.LRUEnabled = v
		}
	case "validate_cold_get":
		if v, err := strconv.ParseBool(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse validate_cold_get, err: %v", err)
		} else {
			ctx.config.CksumConfig.ValidateColdGet = v
		}
	case "checksum":
		if value == ChecksumXXHash || value == ChecksumNone {
			ctx.config.CksumConfig.Checksum = value
		} else {
			return fmt.Sprintf("Invalid %s type %s - expecting %s or %s", name, value, ChecksumXXHash, ChecksumNone)
		}
	default:
		errstr = fmt.Sprintf("Cannot set config var %s - is readonly or unsupported", name)
	}
	if checkwm {
		hwm, lwm := ctx.config.LRUConfig.HighWM, ctx.config.LRUConfig.LowWM
		if hwm <= 0 || lwm <= 0 || hwm < lwm || lwm > 100 || hwm > 100 {
			ctx.config.LRUConfig.LowWM, ctx.config.LRUConfig.HighWM = lm, hm
			errstr = fmt.Sprintf("Invalid LRU watermarks %+v", ctx.config.LRUConfig)
		}
	}
	return
}

//=================
//
// http err + spec message + code + stats
//
//=================
func (h *httprunner) invalmsghdlr(w http.ResponseWriter, r *http.Request, specific string, other ...interface{}) {
	status := http.StatusBadRequest
	if len(other) > 0 {
		status = other[0].(int)
	}
	s := http.StatusText(status) + ": " + specific
	s += ": " + r.Method + " " + r.URL.Path + " from " + r.RemoteAddr
	glog.Errorln(s)
	glog.Flush()
	http.Error(w, s, status)
	h.statsif.add("numerr", 1)
}
