// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/OneOfOne/xxhash"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const ( // => Transport.MaxIdleConnsPerHost
	targetMaxIdleConnsPer = 4
	proxyMaxIdleConnsPer  = 8
	initialBucketListSize = 512
)

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
	listbucket(ctx context.Context, bucket string, msg *GetMsg) (jsbytes []byte, errstr string, errcode int)
	headbucket(ctx context.Context, bucket string) (bucketprops simplekvs, errstr string, errcode int)
	getbucketnames(ctx context.Context) (buckets []string, errstr string, errcode int)
	//
	headobject(ctx context.Context, bucket string, objname string) (objmeta simplekvs, errstr string, errcode int)
	//
	getobj(ctx context.Context, fqn, bucket, objname string) (props *objectProps, errstr string, errcode int)
	putobj(ctx context.Context, file *os.File, bucket, objname string, ohobj cksumvalue) (version string, errstr string, errcode int)
	deleteobj(ctx context.Context, bucket, objname string) (errstr string, errcode int)
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

// Copies headers from original request(from client) to
// a new one(inter-cluster call)
func copyHeaders(rOrig, rNew *http.Request) {
	if rOrig == nil || rNew == nil {
		return
	}

	for key, value := range rOrig.Header {
		rNew.Header[key] = value
	}
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
	keepalive             keepaliver
	smapowner             *smapowner
	bmdowner              *bmdowner
	revProxy              *httputil.ReverseProxy
}

func (h *httprunner) registerhdlr(path string, handler func(http.ResponseWriter, *http.Request)) {
	h.mux.HandleFunc(path, handler)
	if !strings.HasSuffix(path, "/") {
		h.mux.HandleFunc(path+"/", handler)
	}
}

func (h *httprunner) init(s statsif, isproxy bool) {
	// clivars proxyurl overrides config proxy settings.
	// If it is set, the proxy will not register as the primary proxy.
	if clivars.proxyurl != "" {
		ctx.config.Proxy.PrimaryURL = clivars.proxyurl
	}
	h.statsif = s
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

	h.httpclient =
		&http.Client{Transport: h.createTransport(perhost, numDaemons), Timeout: ctx.config.Timeout.Default}
	h.httpclientLongTimeout =
		&http.Client{Transport: h.createTransport(perhost, numDaemons), Timeout: ctx.config.Timeout.DefaultLong}

	if isproxy && ctx.config.Net.HTTP.UseAsProxy {
		h.revProxy = &httputil.ReverseProxy{
			Director:  func(r *http.Request) {},
			Transport: h.createTransport(proxyMaxIdleConnsPer, numDaemons),
		}
	}

	h.mux = http.NewServeMux()
	h.smapowner = &smapowner{}
	h.bmdowner = &bmdowner{}
}

// initSI initialize a daemon's identification (never changes once it is set)
// Note: Sadly httprunner has become the sharing point where common code for
//       proxyrunner and targetrunner exist.
func (h *httprunner) initSI() {
	ipaddr, errstr := getipv4addr()
	if errstr != "" {
		glog.Fatalf("FATAL: %s", errstr)
	}

	h.si = &daemonInfo{}
	h.si.NodeIPAddr = ipaddr
	h.si.DaemonPort = ctx.config.Net.L4.Port
	id := os.Getenv("DFCDAEMONID")
	if id != "" {
		h.si.DaemonID = id
	} else {
		cs := xxhash.ChecksumString32S(ipaddr+":"+ctx.config.Net.L4.Port, mLCG32)
		h.si.DaemonID = strconv.Itoa(int(cs & 0xfffff))
		if testingFSPpaths() {
			h.si.DaemonID += ":" + ctx.config.Net.L4.Port
		}
	}

	proto := "http"
	if ctx.config.Net.HTTP.UseHTTPS {
		proto = "https"
	}

	h.si.DirectURL = proto + "://" + h.si.NodeIPAddr + ":" + h.si.DaemonPort
}

func (h *httprunner) createTransport(perhost, numDaemons int) *http.Transport {
	defaultTransport := http.DefaultTransport.(*http.Transport)
	transport := &http.Transport{
		// defaults
		Proxy: defaultTransport.Proxy,
		DialContext: (&net.Dialer{ // defaultTransport.DialContext,
			Timeout:   30 * time.Second, // must be reduced & configurable
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		IdleConnTimeout:       defaultTransport.IdleConnTimeout,
		ExpectContinueTimeout: defaultTransport.ExpectContinueTimeout,
		TLSHandshakeTimeout:   defaultTransport.TLSHandshakeTimeout,
		// custom
		MaxIdleConnsPerHost: perhost,
		MaxIdleConns:        perhost * numDaemons,
	}
	if ctx.config.Net.HTTP.UseHTTPS {
		glog.Warningln("HTTPS for inter-cluster communications is not yet supported and should be avoided")
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return transport
}

func (h *httprunner) run() error {
	// a wrapper to glog http.Server errors - otherwise
	// os.Stderr would be used, as per golang.org/pkg/net/http/#Server
	h.glogger = log.New(&glogwriter{}, "net/http err: ", 0)
	addr := ":" + ctx.config.Net.L4.Port

	if ctx.config.Net.HTTP.UseHTTPS {
		h.h = &http.Server{Addr: addr, Handler: h.mux, ErrorLog: h.glogger}
		if err := h.h.ListenAndServeTLS(ctx.config.Net.HTTP.Certificate, ctx.config.Net.HTTP.Key); err != nil {
			if err != http.ErrServerClosed {
				glog.Errorf("Terminated %s with err: %v", h.name, err)
				return err
			}
		}
	} else {
		// Support for h2c is transparent using h2c.NewHandler, which implements a lightweight
		// wrapper around h.mux.ServeHTTP to check for an h2c connection.
		h.h = &http.Server{Addr: addr, Handler: h2c.NewHandler(h.mux, &http2.Server{}), ErrorLog: h.glogger}
		if err := h.h.ListenAndServe(); err != nil {
			if err != http.ErrServerClosed {
				glog.Errorf("Terminated %s with err: %v", h.name, err)
				return err
			}
		}
	}
	return nil
}

// stop gracefully
func (h *httprunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", h.name, err)

	if h.h == nil {
		return
	}
	contextwith, cancel := context.WithTimeout(context.Background(), ctx.config.Timeout.Default)

	err = h.h.Shutdown(contextwith)
	if err != nil {
		glog.Infof("Stopped %s, err: %v", h.name, err)
	}
	cancel()
}

// intra-cluster IPC, control plane; calls (via http) another target or a proxy
// optionally, sends a json-encoded body to the callee
func (h *httprunner) call(rOrig *http.Request, si *daemonInfo, url, method string, injson []byte,
	timeout ...time.Duration) callResult {
	var (
		request       *http.Request
		response      *http.Response
		sid           = "unknown"
		outjson       []byte
		err           error
		errstr        string
		newPrimaryURL string
		status        int
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
		return callResult{si, outjson, err, errstr, "", status}
	}

	copyHeaders(rOrig, request)
	if len(timeout) > 0 {
		if timeout[0] != 0 {
			contextwith, cancel := context.WithTimeout(context.Background(), timeout[0])
			defer cancel()
			newrequest := request.WithContext(contextwith)
			copyHeaders(rOrig, newrequest)
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
			return callResult{si, outjson, err, errstr, "", status}
		}

		errstr = fmt.Sprintf("Failed to http-call %s (%s %s): err %v", sid, method, url, err)
		return callResult{si, outjson, err, errstr, "", status}
	}

	newPrimaryURL = response.Header.Get(HeaderPrimaryProxyURL)
	if outjson, err = ioutil.ReadAll(response.Body); err != nil {
		errstr = fmt.Sprintf("Failed to http-call %s (%s %s): read response err: %v", sid, method, url, err)
		if err == io.EOF {
			trailer := response.Trailer.Get("Error")
			if trailer != "" {
				errstr = fmt.Sprintf("Failed to http-call %s (%s %s): err: %v, trailer: %s", sid, method, url, err, trailer)
			}
		}

		response.Body.Close()
		return callResult{si, outjson, err, errstr, newPrimaryURL, status}
	}
	response.Body.Close()

	// err == nil && bad status: response.Body contains the error message
	if response.StatusCode >= http.StatusBadRequest {
		err = fmt.Errorf("%s, status code: %d", outjson, response.StatusCode)
		errstr = err.Error()
		status = response.StatusCode
		return callResult{si, outjson, err, errstr, newPrimaryURL, status}
	}

	if sid != "unknown" {
		h.keepalive.heardFrom(sid, false /* reset */)
	}

	return callResult{si, outjson, err, errstr, newPrimaryURL, status}
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
			if _, file, line, ok := runtime.Caller(1); ok {
				f := filepath.Base(file)
				s += fmt.Sprintf("(%s, #%d)", f, line)
			}
			h.invalmsghdlr(w, r, s)
			return nil
		}
		apitems = apitems[1:]
	}
	if len(apitems) > 0 && res != "" {
		if apitems[0] != res {
			s := fmt.Sprintf("Invalid API resource: %s (expecting %s)", apitems[0], res)
			if _, file, line, ok := runtime.Caller(1); ok {
				f := filepath.Base(file)
				s += fmt.Sprintf("(%s, #%d)", f, line)
			}
			h.invalmsghdlr(w, r, s)
			return nil
		}
		apitems = apitems[1:]
	}
	if len(apitems) < n {
		s := fmt.Sprintf("Invalid API request: num elements %d (expecting at least %d [%v])", len(apitems), n, apitems)
		if _, file, line, ok := runtime.Caller(1); ok {
			f := filepath.Base(file)
			s += fmt.Sprintf("(%s, #%d)", f, line)
		}
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
		if _, file, line, ok := runtime.Caller(1); ok {
			f := filepath.Base(file)
			s += fmt.Sprintf("(%s, #%d)", f, line)
		}
		h.invalmsghdlr(w, r, s)
		return err
	}

	err = json.Unmarshal(b, out)
	if err != nil {
		s := fmt.Sprintf("Failed to json-unmarshal %s request, err: %v [%v]", r.Method, err, string(b))
		if _, file, line, ok := runtime.Caller(1); ok {
			f := filepath.Base(file)
			s += fmt.Sprintf("(%s, #%d)", f, line)
		}
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
		s := "isSyscallWriteError: " + r.Method + " " + r.URL.Path
		if _, file, line, ok2 := runtime.Caller(1); ok2 {
			f := filepath.Base(file)
			s += fmt.Sprintf("(%s, #%d)", f, line)
		}
		glog.Errorln(s)
		h.statsif.add("numerr", 1)
		return
	}
	errstr := fmt.Sprintf("%s: Failed to write json, err: %v", tag, err)
	if _, file, line, ok := runtime.Caller(1); ok {
		f := filepath.Base(file)
		errstr += fmt.Sprintf("(%s, #%d)", f, line)
	}
	h.invalmsghdlr(w, r, errstr)
	return
}

func (h *httprunner) validatebckname(w http.ResponseWriter, r *http.Request, bucket string) bool {
	if strings.Contains(bucket, string(filepath.Separator)) {
		s := fmt.Sprintf("Invalid bucket name %s (contains '/')", bucket)
		if _, file, line, ok := runtime.Caller(1); ok {
			f := filepath.Base(file)
			s += fmt.Sprintf("(%s, #%d)", f, line)
		}
		h.invalmsghdlr(w, r, s)
		return false
	}
	return true
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
	case "vmodule":
		if err := setGLogVModule(value); err != nil {
			errstr = fmt.Sprintf("Failed to set vmodule = %s, err: %v", value, err)
		}
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
	case "disk_util_low_wm":
		if v, err := atoi(value); err != nil {
			errstr = fmt.Sprintf("Failed to convert disk_util_low_wm, err: %v", err)
		} else {
			ctx.config.Xaction.DiskUtilLowWM = v
		}
	case "disk_util_high_wm":
		if v, err := atoi(value); err != nil {
			errstr = fmt.Sprintf("Failed to convert disk_util_high_wm, err: %v", err)
		} else {
			ctx.config.Xaction.DiskUtilHighWM = v
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
	case "dest_retry_time":
		if v, err := time.ParseDuration(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse dest_retry_time, err: %v", err)
		} else {
			ctx.config.Rebalance.DestRetryTime, ctx.config.Rebalance.DestRetryTimeStr = v, value
		}
	case "send_file_time":
		if v, err := time.ParseDuration(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse send_file_time, err: %v", err)
		} else {
			ctx.config.Timeout.SendFile, ctx.config.Timeout.SendFileStr = v, value
		}
	case "default_timeout":
		if v, err := time.ParseDuration(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse default_timeout, err: %v", err)
		} else {
			ctx.config.Timeout.Default, ctx.config.Timeout.DefaultStr = v, value
		}
	case "default_long_timeout":
		if v, err := time.ParseDuration(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse default_long_timeout, err: %v", err)
		} else {
			ctx.config.Timeout.DefaultLong, ctx.config.Timeout.DefaultLongStr = v, value
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
			ctx.config.Rebalance.Enabled = v
		}
	case "validate_checksum_cold_get":
		if v, err := strconv.ParseBool(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse validate_checksum_cold_get, err: %v", err)
		} else {
			ctx.config.Cksum.ValidateColdGet = v
		}
	case "validate_checksum_warm_get":
		if v, err := strconv.ParseBool(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse validate_checksum_warm_get, err: %v", err)
		} else {
			ctx.config.Cksum.ValidateWarmGet = v
		}
	case "enable_read_range_checksum":
		if v, err := strconv.ParseBool(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse enable_read_range_checksum, err: %v", err)
		} else {
			ctx.config.Cksum.EnableReadRangeChecksum = v
		}
	case "validate_version_warm_get":
		if v, err := strconv.ParseBool(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse validate_version_warm_get, err: %v", err)
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
	case "fschecker_enabled":
		if v, err := strconv.ParseBool(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse fschecker_enabled, err: %v", err)
		} else {
			ctx.config.FSChecker.Enabled = v
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

// errHTTP returns a formatted error string for an HTTP request.
func (h *httprunner) errHTTP(r *http.Request, msg string, status int) string {
	return http.StatusText(status) + ": " + msg + ": " + r.Method + " " + r.URL.Path
}

func (h *httprunner) invalmsghdlr(w http.ResponseWriter, r *http.Request, msg string, other ...interface{}) {
	status := http.StatusBadRequest
	if len(other) > 0 {
		status = other[0].(int)
	}
	s := h.errHTTP(r, msg, status)
	if _, file, line, ok := runtime.Caller(1); ok {
		if !strings.Contains(msg, ".go, #") {
			f := filepath.Base(file)
			s += fmt.Sprintf("(%s, #%d)", f, line)
		}
	}
	glog.Errorln(s)
	http.Error(w, s, status)
	h.statsif.add("numerr", 1)
}

func (h *httprunner) extractSmap(payload simplekvs) (newsmap, oldsmap *Smap, msg *ActionMsg, errstr string) {
	if _, ok := payload[smaptag]; !ok {
		return
	}
	newsmap, oldsmap, msg = &Smap{}, &Smap{}, &ActionMsg{}
	smapvalue := payload[smaptag]
	msgvalue := ""
	if err := json.Unmarshal([]byte(smapvalue), newsmap); err != nil {
		errstr = fmt.Sprintf("Failed to unmarshal new smap, value (%+v, %T), err: %v", smapvalue, smapvalue, err)
		return
	}
	if _, ok := payload[smaptag+actiontag]; ok {
		msgvalue = payload[smaptag+actiontag]
		if err := json.Unmarshal([]byte(msgvalue), msg); err != nil {
			errstr = fmt.Sprintf("Failed to unmarshal action message, value (%+v, %T), err: %v", msgvalue, msgvalue, err)
			return
		}
	}
	localsmap := h.smapowner.get()
	myver := localsmap.version()
	if newsmap.version() == myver {
		newsmap = nil
		return
	}
	if !newsmap.isValid() {
		errstr = fmt.Sprintf("Invalid Smap v%d - lacking or missing the primary", newsmap.version())
		newsmap = nil
		return
	}
	if newsmap.version() < myver {
		if h.si != nil && localsmap.getTarget(h.si.DaemonID) == nil {
			errstr = fmt.Sprintf("%s: Attempt to downgrade Smap v%d to v%d", h.si.DaemonID, myver, newsmap.version())
			newsmap = nil
			return
		}
		if h.si != nil && localsmap.getTarget(h.si.DaemonID) != nil {
			glog.Errorf("target %s: receive Smap v%d < v%d local - proceeding anyway",
				h.si.DaemonID, newsmap.version(), localsmap.version())
		} else {
			errstr = fmt.Sprintf("Attempt to downgrade Smap v%d to v%d", myver, newsmap.version())
			return
		}
	}
	s := ""
	if msg.Action != "" {
		s = ", action " + msg.Action
	}
	glog.Infof("receive Smap v%d (local v%d), ntargets %d%s", newsmap.version(), localsmap.version(), newsmap.countTargets(), s)

	if msgvalue == "" || msg.Value == nil {
		// synchronize with no action message and no old smap
		return
	}
	// old smap
	v1, ok1 := msg.Value.(map[string]interface{})
	assert(ok1, fmt.Sprintf("msg (%+v, %T), msg.Value (%+v, %T)", msg, msg, msg.Value, msg.Value))
	v2, ok2 := v1["tmap"]
	assert(ok2)
	tmapif, ok3 := v2.(map[string]interface{})
	assert(ok3)
	v4, ok4 := v1["pmap"]
	assert(ok4)
	pmapif, ok5 := v4.(map[string]interface{})
	assert(ok5)
	versionf := v1["version"].(float64)

	oldsmap.init(len(tmapif), len(pmapif))

	// partial restore of the old smap - keeping only the respective DaemonIDs and version
	for sid := range tmapif {
		oldsmap.Tmap[sid] = &daemonInfo{}
	}
	for pid := range pmapif {
		oldsmap.Pmap[pid] = &daemonInfo{}
	}
	oldsmap.Version = int64(versionf)
	return
}

func (h *httprunner) extractbucketmd(payload simplekvs) (newbucketmd *bucketMD, msg *ActionMsg, errstr string) {
	if _, ok := payload[bucketmdtag]; !ok {
		return
	}
	newbucketmd, msg = &bucketMD{}, &ActionMsg{}
	bmdvalue := payload[bucketmdtag]
	msgvalue := ""
	if err := json.Unmarshal([]byte(bmdvalue), newbucketmd); err != nil {
		errstr = fmt.Sprintf("Failed to unmarshal new bucket-metadata, value (%+v, %T), err: %v", bmdvalue, bmdvalue, err)
		return
	}
	if _, ok := payload[bucketmdtag+actiontag]; ok {
		msgvalue = payload[bucketmdtag+actiontag]
		if err := json.Unmarshal([]byte(msgvalue), msg); err != nil {
			errstr = fmt.Sprintf("Failed to unmarshal action message, value (%+v, %T), err: %v", msgvalue, msgvalue, err)
			return
		}
	}
	myver := h.bmdowner.get().version()
	if newbucketmd.version() <= myver {
		if newbucketmd.version() < myver {
			errstr = fmt.Sprintf("Attempt to downgrade bucket-metadata version %d to %d", myver, newbucketmd.version())
		}
		newbucketmd = nil
	}
	return
}

// URLPath returns a HTTP URL path by joining all segments with "/"
func URLPath(segments ...string) string {
	return path.Join("/", path.Join(segments...))
}

// broadcast sends a http call to all servers in parallel, wait until all calls are returned
// note: 'u' has only the path and query part, host portion will be set by this function.
func (h *httprunner) broadcast(path string, query url.Values, method string, body []byte,
	servers []*daemonInfo, timeout ...time.Duration) chan callResult {
	var (
		ch = make(chan callResult, len(servers))
		wg = &sync.WaitGroup{}
	)

	for _, s := range servers {
		wg.Add(1)
		go func(di *daemonInfo, wg *sync.WaitGroup) {
			defer wg.Done()

			var u url.URL
			if ctx.config.Net.HTTP.UseHTTPS {
				u.Scheme = "https"
			} else {
				u.Scheme = "http"
			}
			u.Host = di.NodeIPAddr + ":" + di.DaemonPort
			u.Path = path
			u.RawQuery = query.Encode() // golang handles query == nil
			res := h.call(nil, di, u.String(), method, body, timeout...)
			ch <- res
		}(s, wg)
	}

	wg.Wait()
	close(ch)
	return ch
}

// ================================== Background =========================================
//
// Generally, DFC clusters can be deployed with an arbitrary numbers of DFC proxies.
// Each proxy/gateway provides full access to the clustered objects and collaborates with
// all other proxies to perform majority-voted HA failovers.
//
// Not all proxies are equal though.
//
// Two out of all proxies can be designated via configuration as "original" and
// "discovery." The "original" (located at the configurable "original_url") is expected
// to be the primary at cluster (initial) deployment time.
//
// Later on, when and if some HA event triggers an automated failover, the role of the
// primary may be (automatically) assumed by a different proxy/gateway, with the
// corresponding update getting synchronized across all running nodes.
// A new node, however, could potentially experience a problem when trying to join the
// cluster simply because its configuration would still be referring to the old primary.
// The added "discovery_url" is precisely intended to address this scenario.
//
// Here's how a node joins a DFC cluster:
// - first, there's the primary proxy/gateway referenced by the current cluster map (Smap)
//   or - during the cluster deployment time - by the the configured "primary_url"
//   (see setup/config.sh)
//
// - if that one fails, the new node goes ahead and tries the alternatives:
// 	- ctx.config.Proxy.DiscoveryURL ("discovery_url")
// 	- ctx.config.Proxy.OriginalURL ("original_url")
// - but only if those are defined and different from the previously tried.
//
// ================================== Background =========================================
func (h *httprunner) join(isproxy bool, extra string) (res callResult) {
	url, psi := h.getPrimaryURLAndSI()
	res = h.registerToURL(url, psi, 0, isproxy, extra)
	if res.err == nil {
		return
	}
	if ctx.config.Proxy.DiscoveryURL != "" && ctx.config.Proxy.DiscoveryURL != url {
		glog.Errorf("%s: (register => %s: %v - retrying => %s...)", h.si.DaemonID, url, res.err, ctx.config.Proxy.DiscoveryURL)
		resAlt := h.registerToURL(ctx.config.Proxy.DiscoveryURL, psi, 0, isproxy, extra)
		if resAlt.err == nil {
			res = resAlt
			return
		}
	}
	if ctx.config.Proxy.OriginalURL != "" && ctx.config.Proxy.OriginalURL != url &&
		ctx.config.Proxy.OriginalURL != ctx.config.Proxy.DiscoveryURL {
		glog.Errorf("%s: (register => %s: %v - retrying => %s...)", h.si.DaemonID, url, res.err, ctx.config.Proxy.OriginalURL)
		resAlt := h.registerToURL(ctx.config.Proxy.OriginalURL, psi, 0, isproxy, extra)
		if resAlt.err == nil {
			res = resAlt
			return
		}
	}
	return
}

func (h *httprunner) registerToURL(url string, psi *daemonInfo, timeout time.Duration, isproxy bool, extra string) (res callResult) {
	info, err := json.Marshal(h.si)
	assert(err == nil, err)

	f := func(inurl string, isproxy bool, timeout time.Duration, extra string) (outurl string) {
		if isproxy {
			outurl = inurl + URLPath(Rversion, Rcluster, Rproxy)
		} else {
			outurl = inurl + URLPath(Rversion, Rcluster)
		}
		if timeout > 0 { // keepalive
			outurl += URLPath(Rkeepalive)
		}
		if extra != "" {
			outurl += "?" + extra
		}
		return
	}
	regurl := f(url, isproxy, timeout, extra)
	for rcount := 0; rcount < 2; rcount++ {
		if timeout > 0 {
			res = h.call(nil, psi, regurl, http.MethodPost, info, timeout)
		} else {
			res = h.call(nil, psi, regurl, http.MethodPost, info)
		}
		if res.err == nil {
			return
		}
		if res.newPrimaryURL != "" {
			glog.Errorf("%s: (register => %s: %v - retrying => %s...)", h.si.DaemonID, regurl, res.err, url)
			regurl = f(res.newPrimaryURL, isproxy, timeout, extra)
			continue
		}
		if IsErrConnectionRefused(res.err) {
			glog.Errorf("%s: (register => %s: connection refused)", h.si.DaemonID, regurl)
		} else {
			glog.Errorf("%s: (register => %s: %v)", h.si.DaemonID, regurl, res.err)
		}
		break
	}
	return
}

// getPrimaryURLAndSI is a helper function to return primary proxy's URL and daemon info
// if Smap is not yet synced, use the primary proxy from the config
// smap lock is acquired to avoid race between this function and other smap access (for example,
// receiving smap during metasync)
func (h *httprunner) getPrimaryURLAndSI() (url string, proxysi *daemonInfo) {
	smap := h.smapowner.get()
	if smap == nil || smap.ProxySI == nil {
		url, proxysi = ctx.config.Proxy.PrimaryURL, nil
		return
	}
	if smap.ProxySI.DaemonID != "" {
		url, proxysi = smap.ProxySI.DirectURL, smap.ProxySI
		return
	}
	url, proxysi = ctx.config.Proxy.PrimaryURL, smap.ProxySI
	return
}
