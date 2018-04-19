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
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/OneOfOne/xxhash"
	"github.com/golang/glog"
	"github.com/hkwi/h2c"
)

const ( // => Transport.MaxIdleConnsPerHost
	targetMaxIdleConnsPer = 4
	proxyMaxIdleConnsPer  = 8
)

const initialBucketListSize = 512

type objectProps struct {
	version string
	size    int64
	nhobj   cksumvalue
}

//===========
//
// interfaces
//
//===========
type cloudif interface {
	listbucket(bucket string, msg *GetMsg) (jsbytes []byte, errstr string, errcode int)
	headbucket(bucket string) (bucketprops map[string]string, errstr string, errcode int)
	getbucketnames() (buckets []string, errstr string, errcode int)
	//
	headobject(bucket string, objname string) (objmeta map[string]string, errstr string, errcode int)
	//
	getobj(fqn, bucket, objname string) (props *objectProps, errstr string, errcode int)
	putobj(file *os.File, bucket, objname string, ohobj cksumvalue) (version string, errstr string, errcode int)
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

	stacktrace := debug.Stack()
	n1 := len(stacktrace)
	s1 := string(stacktrace[:n1])
	glog.Errorln(s1)
	return n, nil
}

type httprunner struct {
	namedrunner
	mux                   *http.ServeMux
	h                     *http.Server
	glogger               *log.Logger
	si                    *daemonInfo
	httpclient            *http.Client // http client for intra-cluster comm
	httpclientLongTimeout *http.Client // http client for long-wait intra-cluster comm
	statsif               statsif
	kalive                kaliveif
	proxysi               *proxyInfo
	smap                  *Smap
}

func (h *httprunner) registerhdlr(path string, handler func(http.ResponseWriter, *http.Request)) {
	if h.mux == nil {
		h.mux = http.NewServeMux()
	}
	h.mux.HandleFunc(path, handler)
}

func (h *httprunner) init(s statsif, isproxy bool) {
	// clivars proxyurl overrides config proxy settings.
	// If it is set, the proxy will not register as the primary proxy.
	if clivars.proxyurl != "" {
		ctx.config.Proxy.Primary.ID = ""
		ctx.config.Proxy.Primary.URL = clivars.proxyurl
	}
	h.statsif = s
	ipaddr, errstr := getipv4addr()
	if errstr != "" {
		glog.Fatalf("FATAL: %s", errstr)
	}
	// http client
	perhost := targetMaxIdleConnsPer
	if isproxy {
		perhost = proxyMaxIdleConnsPer
	}
	numDaemons := ctx.config.Net.HTTP.MaxNumTargets * 2 // an estimate, given a dual-function
	assert(numDaemons < 1024)                           // not a limitation!
	if numDaemons < 4 {
		numDaemons = 4
	}
	h.httpclient = &http.Client{
		Transport: &http.Transport{MaxIdleConnsPerHost: perhost, MaxIdleConns: perhost * numDaemons},
		Timeout:   ctx.config.Timeout.Default,
	}
	h.httpclientLongTimeout = &http.Client{
		Transport: &http.Transport{MaxIdleConnsPerHost: perhost, MaxIdleConns: perhost * numDaemons},
		Timeout:   ctx.config.Timeout.DefaultLong,
	}
	h.smap = &Smap{}
	h.proxysi = &proxyInfo{}
	// init daemonInfo here
	h.si = &daemonInfo{}
	h.si.NodeIPAddr = ipaddr
	h.si.DaemonPort = ctx.config.Net.L4.Port
	id := os.Getenv("DFCDAEMONID")
	if id != "" {
		h.si.DaemonID = id
	} else {
		split := strings.Split(ipaddr, ".")
		cs := xxhash.ChecksumString32S(split[len(split)-1], mLCG32)
		h.si.DaemonID = strconv.Itoa(int(cs&0xffff)) + ":" + ctx.config.Net.L4.Port
	}
	h.si.DirectURL = "http://" + h.si.NodeIPAddr + ":" + h.si.DaemonPort
}

func (h *httprunner) run() error {
	// a wrapper to glog http.Server errors - otherwise
	// os.Stderr would be used, as per golang.org/pkg/net/http/#Server
	h.glogger = log.New(&glogwriter{}, "net/http err: ", 0)
	var handler http.Handler = h.mux
	if ctx.config.H2c {
		handler = h2c.Server{Handler: handler}
	}

	portstring := ":" + ctx.config.Net.L4.Port
	h.h = &http.Server{Addr: portstring, Handler: handler, ErrorLog: h.glogger}
	if err := h.h.ListenAndServe(); err != nil {
		if err != http.ErrServerClosed {
			glog.Errorf("Terminated %s with err: %v", h.name, err)
			return err
		}
	}
	return nil
}

// stop gracefully
func (h *httprunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", h.name, err)

	contextwith, cancel := context.WithTimeout(context.Background(), ctx.config.Timeout.Default)
	defer cancel()

	if h.h == nil {
		return
	}
	err = h.h.Shutdown(contextwith)
	if err != nil {
		glog.Infof("Stopped %s, err: %v", h.name, err)
	}
}

// intra-cluster IPC, control plane; calls (via http) another target or a proxy
// optionally, sends a json-encoded body to the callee
func (h *httprunner) call(si *daemonInfo, url, method string, injson []byte,
	timeout ...time.Duration) (outjson []byte, err error, errstr string, status int) {
	var (
		request  *http.Request
		response *http.Response
		sid      = "unknown"
	)
	if si != nil {
		sid = si.DaemonID
	}
	if len(injson) == 0 {
		request, err = http.NewRequest(method, url, nil)
		if glog.V(4) { // super-verbose
			glog.Infof("%s %s", method, url)
		}
	} else {
		request, err = http.NewRequest(method, url, bytes.NewBuffer(injson))
		if err == nil {
			request.Header.Set("Content-Type", "application/json")
		}
		if glog.V(4) { // super-verbose
			l := len(injson)
			if l > 16 {
				l = 16
			}
			glog.Infof("%s %s %s...}", method, url, string(injson[:l]))
		}
	}
	if err != nil {
		errstr = fmt.Sprintf("Unexpected failure to create http request %s %s, err: %v", method, url, err)
		return
	}
	if len(timeout) > 0 {
		if timeout[0] != 0 {
			contextwith, cancel := context.WithTimeout(context.Background(), timeout[0])
			defer cancel()
			newrequest := request.WithContext(contextwith)
			response, err = h.httpclient.Do(newrequest) // timeout => context.deadlineExceededError
		} else { // zero timeout means the client wants no timeout
			response, err = h.httpclientLongTimeout.Do(request)
		}
	} else {
		response, err = h.httpclient.Do(request)
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
	defer response.Body.Close()

	if outjson, err = ioutil.ReadAll(response.Body); err != nil {
		errstr = fmt.Sprintf("Failed to http-call %s (%s %s): read response err: %v", sid, method, url, err)
		if err == io.EOF {
			trailer := response.Trailer.Get("Error")
			if trailer != "" {
				errstr = fmt.Sprintf("Failed to http-call %s (%s %s): err: %v, trailer: %s", sid, method, url, err, trailer)
			}
		}
		return
	}
	// err == nil && bad status: response.Body contains the error message
	if response.StatusCode >= http.StatusBadRequest {
		err = fmt.Errorf("%s, status code: %d", outjson, response.StatusCode)
		errstr = err.Error()
		status = response.StatusCode
		return
	}
	if sid != "unknown" {
		h.kalive.timestamp(sid)
	}
	return
}

//=============================
//
// http request parsing helpers
//
//=============================
func (h *httprunner) restAPIItems(unescapedpath string, maxsplit int) []string {
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
	if err != nil {
		s := fmt.Sprintf("Failed to read %s request, err: %v", r.Method, err)
		if err == io.EOF {
			trailer := r.Trailer.Get("Error")
			if trailer != "" {
				s = fmt.Sprintf("Failed to read %s request, err: %v, trailer: %s", r.Method, err, trailer)
			}
		}
		h.invalmsghdlr(w, r, s)
		return err
	}

	err = json.Unmarshal(b, out)
	if err != nil {
		s := fmt.Sprintf("Failed to json-unmarshal %s request, err: %v [%v]", r.Method, err, string(b))
		h.invalmsghdlr(w, r, s)
		return err
	}
	return nil
}

// NOTE: must be the last error-generating-and-handling call in the http handler
//       writes http body and header
//       calls invalmsghdlr() on err
func (h *httprunner) writeJSON(w http.ResponseWriter, r *http.Request, jsbytes []byte, tag string) (ok bool) {
	w.Header().Set("Content-Type", "application/json")
	var err error
	if _, err = w.Write(jsbytes); err == nil {
		ok = true
		return
	}
	if isSyscallWriteError(err) {
		// apparently, cannot write to this w: broken-pipe and similar
		glog.Errorf("isSyscallWriteError: %v", err)
		s := "isSyscallWriteError: " + r.Method + " " + r.URL.Path + " from " + r.RemoteAddr
		glog.Errorln(s)
		glog.Flush()
		h.statsif.add("numerr", 1)
		return
	}
	errstr := fmt.Sprintf("%s: Failed to write json, err: %v", tag, err)
	h.invalmsghdlr(w, r, errstr)
	return
}

//=================
//
// commong set config
//
//=================
func (h *httprunner) setconfig(name, value string) (errstr string) {
	lm, hm := ctx.config.LRU.LowWM, ctx.config.LRU.HighWM
	checkwm := false
	atoi := func(value string) (uint32, error) {
		v, err := strconv.Atoi(value)
		return uint32(v), err
	}
	switch name {
	case "loglevel":
		if err := setloglevel(value); err != nil {
			errstr = fmt.Sprintf("Failed to set log level = %s, err: %v", value, err)
		}
	case "stats_time":
		if v, err := time.ParseDuration(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse stats_time, err: %v", err)
		} else {
			ctx.config.Periodic.StatsTime, ctx.config.Periodic.StatsTimeStr = v, value
		}
	case "dont_evict_time":
		if v, err := time.ParseDuration(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse dont_evict_time, err: %v", err)
		} else {
			ctx.config.LRU.DontEvictTime, ctx.config.LRU.DontEvictTimeStr = v, value
		}
	case "capacity_upd_time":
		if v, err := time.ParseDuration(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse capacity_upd_time, err: %v", err)
		} else {
			ctx.config.LRU.CapacityUpdTime, ctx.config.LRU.CapacityUpdTimeStr = v, value
		}
	case "startup_delay_time":
		if v, err := time.ParseDuration(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse startup_delay_time, err: %v", err)
		} else {
			ctx.config.Rebalance.StartupDelayTime, ctx.config.Rebalance.StartupDelayTimeStr = v, value
		}
	case "lowwm":
		if v, err := atoi(value); err != nil {
			errstr = fmt.Sprintf("Failed to convert lowwm, err: %v", err)
		} else {
			ctx.config.LRU.LowWM, checkwm = v, true
		}
	case "highwm":
		if v, err := atoi(value); err != nil {
			errstr = fmt.Sprintf("Failed to convert highwm, err: %v", err)
		} else {
			ctx.config.LRU.HighWM, checkwm = v, true
		}
	case "passthru":
		if v, err := strconv.ParseBool(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse passthru (proxy-only), err: %v", err)
		} else {
			ctx.config.Proxy.Primary.Passthru = v
		}
	case "lru_enabled":
		if v, err := strconv.ParseBool(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse lru_enabled, err: %v", err)
		} else {
			ctx.config.LRU.LRUEnabled = v
		}
	case "rebalancing_enabled":
		if v, err := strconv.ParseBool(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse rebalancing_enabled, err: %v", err)
		} else {
			ctx.config.Rebalance.RebalancingEnabled = v
		}
	case "validate_cold_get":
		if v, err := strconv.ParseBool(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse validate_cold_get, err: %v", err)
		} else {
			ctx.config.Cksum.ValidateColdGet = v
		}
	case "validate_warm_get":
		if v, err := strconv.ParseBool(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse validate_warm_get, err: %v", err)
		} else {
			ctx.config.Ver.ValidateWarmGet = v
		}
	case "checksum":
		if value == ChecksumXXHash || value == ChecksumNone {
			ctx.config.Cksum.Checksum = value
		} else {
			return fmt.Sprintf("Invalid %s type %s - expecting %s or %s", name, value, ChecksumXXHash, ChecksumNone)
		}
	case "versioning":
		if err := validateVersion(value); err == nil {
			ctx.config.Ver.Versioning = value
		} else {
			return err.Error()
		}
	default:
		errstr = fmt.Sprintf("Cannot set config var %s - is readonly or unsupported", name)
	}
	if checkwm {
		hwm, lwm := ctx.config.LRU.HighWM, ctx.config.LRU.LowWM
		if hwm <= 0 || lwm <= 0 || hwm < lwm || lwm > 100 || hwm > 100 {
			ctx.config.LRU.LowWM, ctx.config.LRU.HighWM = lm, hm
			errstr = fmt.Sprintf("Invalid LRU watermarks %+v", ctx.config.LRU)
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
