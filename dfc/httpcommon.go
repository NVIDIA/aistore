// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
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
	"net"
	"net/http"
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

	"github.com/OneOfOne/xxhash"
	"github.com/golang/glog"
	"github.com/hkwi/h2c"
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
	headbucket(ctx context.Context, bucket string) (bucketprops map[string]string, errstr string, errcode int)
	getbucketnames(ctx context.Context) (buckets []string, errstr string, errcode int)
	//
	headobject(ctx context.Context, bucket string, objname string) (objmeta map[string]string, errstr string, errcode int)
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

// Decrypts token and retreive userID from it
// Returns empty userID in case of token is invalid
func userIDFromRequest(r *http.Request) string {
	if r == nil {
		return ""
	}

	token := ""
	tokenParts := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
	if len(tokenParts) == 2 && tokenParts[0] == tokenStart {
		token = tokenParts[1]
	}

	if token == "" {
		return ""
	}

	authrec, err := decryptToken(token)
	if err != nil {
		glog.Errorf("Failed to decrypt token [%s]: %v", token, err)
		return ""
	}

	return authrec.userID
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
	smap                  *Smap
	lbmap                 *lbmap
}

func (h *httprunner) registerhdlr(path string, handler func(http.ResponseWriter, *http.Request)) {
	if h.mux == nil {
		h.mux = http.NewServeMux()
	}
	h.mux.HandleFunc(path, handler)
	if !strings.HasSuffix(path, "/") {
		h.mux.HandleFunc(path+"/", handler)
	}
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
	if ctx.config.Net.HTTP.UseHTTPS {
		glog.Fatalf("HTTPS for inter-cluster communications is not yet supported (and better be avoided)")
	} else {
		h.httpclient =
			&http.Client{Transport: h.createTransport(perhost, numDaemons), Timeout: ctx.config.Timeout.Default}
		h.httpclientLongTimeout =
			&http.Client{Transport: h.createTransport(perhost, numDaemons), Timeout: ctx.config.Timeout.DefaultLong}
	}
	h.smap = &Smap{}
	h.lbmap = &lbmap{LBmap: make(map[string]string)} // local (aka cache-only) buckets

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
	return transport
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

	if ctx.config.Net.HTTP.UseHTTPS {
		h.h = &http.Server{Addr: portstring, Handler: handler, ErrorLog: h.glogger}
		if err := h.h.ListenAndServeTLS(ctx.config.Net.HTTP.Certificate, ctx.config.Net.HTTP.Key); err != nil {
			if err != http.ErrServerClosed {
				glog.Errorf("Terminated %s with err: %v", h.name, err)
				return err
			}
		}
	} else {
		h.h = &http.Server{Addr: portstring, Handler: handler, ErrorLog: h.glogger}
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
func (h *httprunner) call(rOrig *http.Request, si *daemonInfo, url, method string, injson []byte,
	timeout ...time.Duration) callResult {
	var (
		request  *http.Request
		response *http.Response
		sid      = "unknown"
		outjson  []byte
		err      error
		errstr   string
		status   int
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
		return callResult{si, outjson, err, errstr, status}
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
			return callResult{si, outjson, err, errstr, status}
		}

		errstr = fmt.Sprintf("Failed to http-call %s (%s %s): err %v", sid, method, url, err)
		return callResult{si, outjson, err, errstr, status}
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

		return callResult{si, outjson, err, errstr, status}
	}

	// err == nil && bad status: response.Body contains the error message
	if response.StatusCode >= http.StatusBadRequest {
		err = fmt.Errorf("%s, status code: %d", outjson, response.StatusCode)
		errstr = err.Error()
		status = response.StatusCode
		return callResult{si, outjson, err, errstr, status}
	}

	if sid != "unknown" {
		h.kalive.timestamp(sid)
	}

	return callResult{si, outjson, err, errstr, status}
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
		glog.Flush()
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
			ctx.config.Rebalance.Enabled = v
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
	s := http.StatusText(status) + ": " + specific + ": " + r.Method + " " + r.URL.Path
	if _, file, line, ok := runtime.Caller(1); ok {
		if !strings.Contains(specific, ".go, #") {
			f := filepath.Base(file)
			s += fmt.Sprintf("(%s, #%d)", f, line)
		}
	}
	glog.Errorln(s)
	glog.Flush()
	http.Error(w, s, status)
	h.statsif.add("numerr", 1)
}

func (h *httprunner) extractsmap(payload map[string]string) (newsmap, oldsmap *Smap, msg *ActionMsg, errstr string) {
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
	if glog.V(3) {
		if msg.Action == "" {
			glog.Infof("extract Smap ver=%d, msg=<nil>", newsmap.version())
		} else {
			glog.Infof("extract Smap ver=%d, action=%s", newsmap.version(), msg.Action)
		}
	}
	myver := h.smap.versionL()
	if newsmap.version() == myver {
		newsmap = nil
		return
	}
	if newsmap.version() < myver {
		errstr = fmt.Sprintf("Attempt to downgrade smap version %d to %d", myver, newsmap.version())
		return
	}
	if msgvalue == "" || msg.Value == nil {
		// synchronize with no action message and no old smap
		return
	}
	// old smap
	oldsmap.Tmap = make(map[string]*daemonInfo)
	oldsmap.Pmap = make(map[string]*proxyInfo)
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

	// partial restore of the old smap - keeping only the respective DaemonIDs and version
	for sid := range tmapif {
		oldsmap.Tmap[sid] = &daemonInfo{}
	}
	for pid := range pmapif {
		oldsmap.Pmap[pid] = &proxyInfo{}
	}
	oldsmap.Version = int64(versionf)
	return
}

func (h *httprunner) extractlbmap(payload map[string]string) (newlbmap *lbmap, msg *ActionMsg, errstr string) {
	if _, ok := payload[lbmaptag]; !ok {
		return
	}
	newlbmap, msg = &lbmap{}, &ActionMsg{}
	lbmapvalue := payload[lbmaptag]
	msgvalue := ""
	if err := json.Unmarshal([]byte(lbmapvalue), newlbmap); err != nil {
		errstr = fmt.Sprintf("Failed to unmarshal new lbmap, value (%+v, %T), err: %v", lbmapvalue, lbmapvalue, err)
		return
	}
	if _, ok := payload[lbmaptag+actiontag]; ok {
		msgvalue = payload[lbmaptag+actiontag]
		if err := json.Unmarshal([]byte(msgvalue), msg); err != nil {
			errstr = fmt.Sprintf("Failed to unmarshal action message, value (%+v, %T), err: %v", msgvalue, msgvalue, err)
			return
		}
	}
	if glog.V(3) {
		if msg.Action == "" {
			glog.Infof("extract lbmap ver=%d, msg=<nil>", newlbmap.version())
		} else {
			glog.Infof("extract lbmap ver=%d, msg=%+v", newlbmap.version(), msg)
		}
	}
	myver := h.lbmap.versionL()
	if newlbmap.version() == myver {
		newlbmap = nil
		return
	}
	if newlbmap.version() < myver {
		errstr = fmt.Sprintf("Attempt to downgrade lbmap version %d to %d", myver, newlbmap.version())
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
		wg sync.WaitGroup
	)

	for _, s := range servers {
		wg.Add(1)
		go func(di *daemonInfo, wg *sync.WaitGroup) {
			defer wg.Done()

			var u url.URL
			u.Scheme = "http"
			u.Host = di.NodeIPAddr + ":" + di.DaemonPort
			u.Path = path
			u.RawQuery = query.Encode() // golang handles query == nil
			res := h.call(nil, di, u.String(), method, body, timeout...)
			ch <- res
		}(s, &wg)
	}

	wg.Wait()
	close(ch)
	return ch
}
