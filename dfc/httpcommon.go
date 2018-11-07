/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/dfc/statsd"
	"github.com/OneOfOne/xxhash"
	"github.com/json-iterator/go"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const ( // to compute MaxIdleConnsPerHost and MaxIdleConns
	targetMaxIdleConnsPer = 32
	proxyMaxIdleConnsPer  = 32
)
const ( //  h.call(timeout)
	defaultTimeout = time.Duration(-1)
	longTimeout    = time.Duration(0)
)

type (
	objectProps struct {
		version string
		atime   time.Time
		size    int64
		nhobj   cksumvalue
	}

	// callResult contains data returned by a server to server call
	callResult struct {
		si      *cluster.Snode
		outjson []byte
		err     error
		errstr  string
		status  int
	}

	// reqArgs contains information about the http request which we want to send
	reqArgs struct {
		method string      // GET, POST, ...
		header http.Header // request headers
		base   string      // base URL: http://xyz.abc
		path   string      // path URL: /x/y/z
		query  url.Values  // query: ?x=y&y=z
		body   []byte      // body for POST, PUT, ...
	}

	// callArgs contains arguments for a server call
	callArgs struct {
		req     reqArgs
		timeout time.Duration
		si      *cluster.Snode
	}

	// bcastCallArgs contains arguments for a broadcast server call
	bcastCallArgs struct {
		req             reqArgs
		internal        bool // specifies if the call is the internal communication
		timeout         time.Duration
		servers         []map[string]*cluster.Snode
		serversToIgnore map[string]struct{}
	}

	// SmapVoteMsg contains the cluster map and a bool representing whether or not a vote is currently happening.
	SmapVoteMsg struct {
		VoteInProgress bool      `json:"vote_in_progress"`
		Smap           *smapX    `json:"smap"`
		BucketMD       *bucketMD `json:"bucketmd"`
	}
)

//===========
//
// interfaces
//
//===========
const initialBucketListSize = 512

type cloudif interface {
	listbucket(ctx context.Context, bucket string, msg *cmn.GetMsg) (jsbytes []byte, errstr string, errcode int)
	headbucket(ctx context.Context, bucket string) (bucketprops cmn.SimpleKVs, errstr string, errcode int)
	getbucketnames(ctx context.Context) (buckets []string, errstr string, errcode int)
	//
	headobject(ctx context.Context, bucket string, objname string) (objmeta cmn.SimpleKVs, errstr string, errcode int)
	//
	getobj(ctx context.Context, fqn, bucket, objname string) (props *objectProps, errstr string, errcode int)
	putobj(ctx context.Context, file *os.File, bucket, objname string, ohobj cksumvalue) (version string, errstr string, errcode int)
	deleteobj(ctx context.Context, bucket, objname string) (errstr string, errcode int)
}

func (u reqArgs) url() string {
	url := strings.TrimSuffix(u.base, "/")
	if !strings.HasPrefix(u.path, "/") {
		url += "/"
	}
	url += u.path
	query := u.query.Encode()
	if query != "" {
		url += "?" + query
	}
	return url
}

//===========
//
// generic bad-request http handler
//
//===========

// Copies headers from original request(from client) to
// a new one(inter-cluster call)
func copyHeaders(src http.Header, dst *http.Header) {
	for k, values := range src {
		for _, v := range values {
			dst.Add(k, v)
		}
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

type netServer struct {
	s   *http.Server
	mux *http.ServeMux
}

type httprunner struct {
	cmn.Named
	publicServer          *netServer
	intraControlServer    *netServer
	intraDataServer       *netServer
	glogger               *log.Logger
	si                    *cluster.Snode
	httpclient            *http.Client // http client for intra-cluster comm
	httpclientLongTimeout *http.Client // http client for long-wait intra-cluster comm
	keepalive             keepaliver
	smapowner             *smapowner
	bmdowner              *bmdowner
	xactinp               *xactInProgress
	statsif               statsif
	statsdC               statsd.Client
}

func (server *netServer) listenAndServe(addr string, logger *log.Logger) error {
	if ctx.config.Net.HTTP.UseHTTPS {
		server.s = &http.Server{Addr: addr, Handler: server.mux, ErrorLog: logger}
		if err := server.s.ListenAndServeTLS(ctx.config.Net.HTTP.Certificate, ctx.config.Net.HTTP.Key); err != nil {
			if err != http.ErrServerClosed {
				glog.Errorf("Terminated server with err: %v", err)
				return err
			}
		}
	} else {
		// Support for h2c is transparent using h2c.NewHandler, which implements a lightweight
		// wrapper around server.mux.ServeHTTP to check for an h2c connection.
		server.s = &http.Server{Addr: addr, Handler: h2c.NewHandler(server.mux, &http2.Server{}), ErrorLog: logger}
		if err := server.s.ListenAndServe(); err != nil {
			if err != http.ErrServerClosed {
				glog.Errorf("Terminated server with err: %v", err)
				return err
			}
		}
	}

	return nil
}

func (server *netServer) shutdown() {
	contextwith, cancel := context.WithTimeout(context.Background(), ctx.config.Timeout.Default)
	if err := server.s.Shutdown(contextwith); err != nil {
		glog.Infof("Stopped server, err: %v", err)
	}
	cancel()
}

func (h *httprunner) registerPublicNetHandler(path string, handler func(http.ResponseWriter, *http.Request)) {
	h.publicServer.mux.HandleFunc(path, handler)
	if !strings.HasSuffix(path, "/") {
		h.publicServer.mux.HandleFunc(path+"/", handler)
	}
}

func (h *httprunner) registerIntraControlNetHandler(path string, handler func(http.ResponseWriter, *http.Request)) {
	h.intraControlServer.mux.HandleFunc(path, handler)
	if !strings.HasSuffix(path, "/") {
		h.intraControlServer.mux.HandleFunc(path+"/", handler)
	}
}

func (h *httprunner) registerIntraDataNetHandler(path string, handler func(http.ResponseWriter, *http.Request)) {
	h.intraDataServer.mux.HandleFunc(path, handler)
	if !strings.HasSuffix(path, "/") {
		h.intraDataServer.mux.HandleFunc(path+"/", handler)
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

	h.httpclient = &http.Client{
		Transport: h.createTransport(perhost, 0),
		Timeout:   ctx.config.Timeout.Default, // defaultTimeout
	}
	h.httpclientLongTimeout = &http.Client{
		Transport: h.createTransport(perhost, 0),
		Timeout:   ctx.config.Timeout.DefaultLong, // longTimeout
	}
	h.publicServer = &netServer{
		mux: http.NewServeMux(),
	}
	h.intraControlServer = h.publicServer // by default intra control net is the same as public
	if ctx.config.Net.UseIntraControl {
		h.intraControlServer = &netServer{
			mux: http.NewServeMux(),
		}
	}
	h.intraDataServer = h.publicServer // by default intra data net is the same as public
	if ctx.config.Net.UseIntraData {
		h.intraDataServer = &netServer{
			mux: http.NewServeMux(),
		}
	}

	h.smapowner = &smapowner{}
	h.bmdowner = &bmdowner{}
	h.xactinp = newxactinp() // extended actions
}

// initSI initialize a daemon's identification (never changes once it is set)
// Note: Sadly httprunner has become the sharing point where common code for
//       proxyrunner and targetrunner exist.
func (h *httprunner) initSI() {
	allowLoopback, _ := strconv.ParseBool(os.Getenv("ALLOW_LOOPBACK"))
	addrList, err := getLocalIPv4List(allowLoopback)
	if err != nil {
		glog.Fatalf("FATAL: %v", err)
	}

	ipAddr, err := getipv4addr(addrList, ctx.config.Net.IPv4)
	if err != nil {
		glog.Fatalf("Failed to get public network address: %v", err)
	}
	glog.Infof("Configured PUBLIC NETWORK address: [%s:%d] (out of: %s)\n", ipAddr, ctx.config.Net.L4.Port, ctx.config.Net.IPv4)

	ipAddrIntraControl := net.IP{}
	if ctx.config.Net.UseIntraControl {
		ipAddrIntraControl, err = getipv4addr(addrList, ctx.config.Net.IPv4IntraControl)
		if err != nil {
			glog.Fatalf("Failed to get intra control network address: %v", err)
		}
		glog.Infof("Configured INTRA CONTROL NETWORK address: [%s:%d] (out of: %s)\n",
			ipAddrIntraControl, ctx.config.Net.L4.PortIntraControl, ctx.config.Net.IPv4IntraControl)
	}

	ipAddrIntraData := net.IP{}
	if ctx.config.Net.UseIntraData {
		ipAddrIntraData, err = getipv4addr(addrList, ctx.config.Net.IPv4IntraData)
		if err != nil {
			glog.Fatalf("Failed to get intra data network address: %v", err)
		}
		glog.Infof("Configured INTRA DATA NETWORK address: [%s:%d] (out of: %s)\n",
			ipAddrIntraData, ctx.config.Net.L4.PortIntraData, ctx.config.Net.IPv4IntraData)
	}

	publicAddr := &net.TCPAddr{
		IP:   ipAddr,
		Port: ctx.config.Net.L4.Port,
	}
	intraControlAddr := &net.TCPAddr{
		IP:   ipAddrIntraControl,
		Port: ctx.config.Net.L4.PortIntraControl,
	}
	intraDataAddr := &net.TCPAddr{
		IP:   ipAddrIntraData,
		Port: ctx.config.Net.L4.PortIntraData,
	}

	daemonID := os.Getenv("DFCDAEMONID")
	if daemonID == "" {
		cs := xxhash.ChecksumString32S(publicAddr.String(), cluster.MLCG32)
		daemonID = strconv.Itoa(int(cs & 0xfffff))
		if testingFSPpaths() {
			daemonID += ":" + ctx.config.Net.L4.PortStr
		}
	}

	h.si = newSnode(daemonID, ctx.config.Net.HTTP.proto, publicAddr, intraControlAddr, intraDataAddr)
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
		MaxIdleConns:        0, // Zero means no limit
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

	if ctx.config.Net.UseIntraControl || ctx.config.Net.UseIntraData {
		var errCh chan error
		if ctx.config.Net.UseIntraControl && ctx.config.Net.UseIntraData {
			errCh = make(chan error, 3)
		} else {
			errCh = make(chan error, 2)
		}

		if ctx.config.Net.UseIntraControl {
			go func() {
				addr := h.si.IntraControlNet.NodeIPAddr + ":" + h.si.IntraControlNet.DaemonPort
				errCh <- h.intraControlServer.listenAndServe(addr, h.glogger)
			}()
		}

		if ctx.config.Net.UseIntraData {
			go func() {
				addr := h.si.IntraDataNet.NodeIPAddr + ":" + h.si.IntraDataNet.DaemonPort
				errCh <- h.intraDataServer.listenAndServe(addr, h.glogger)
			}()
		}

		go func() {
			addr := h.si.PublicNet.NodeIPAddr + ":" + h.si.PublicNet.DaemonPort
			errCh <- h.publicServer.listenAndServe(addr, h.glogger)
		}()

		return <-errCh
	}

	// When only public net is configured listen on *:port
	addr := ":" + h.si.PublicNet.DaemonPort
	return h.publicServer.listenAndServe(addr, h.glogger)
}

// stop gracefully
func (h *httprunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", h.Getname(), err)

	h.statsdC.Close()
	if h.publicServer.s == nil {
		return
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		h.publicServer.shutdown()
		wg.Done()
	}()

	if ctx.config.Net.UseIntraControl {
		wg.Add(1)
		go func() {
			h.intraControlServer.shutdown()
			wg.Done()
		}()
	}

	if ctx.config.Net.UseIntraData {
		wg.Add(1)
		go func() {
			h.intraDataServer.shutdown()
			wg.Done()
		}()
	}

	wg.Wait()
}

//=================================
//
// intra-cluster IPC, control plane
//
//=================================
// call another target or a proxy
// optionally, include a json-encoded body
func (h *httprunner) call(args callArgs) callResult {
	var (
		request  *http.Request
		response *http.Response
		sid      = "unknown"
		outjson  []byte
		err      error
		errstr   string
		status   int
	)

	if args.si != nil {
		sid = args.si.DaemonID
	}

	cmn.Assert(args.si != nil || args.req.base != "") // either we have si or base
	if args.req.base == "" && args.si != nil {
		args.req.base = args.si.PublicNet.DirectURL
	}

	url := args.req.url()
	if len(args.req.body) == 0 {
		request, err = http.NewRequest(args.req.method, url, nil)
		if glog.V(4) { // super-verbose
			glog.Infof("%s %s", args.req.method, url)
		}
	} else {
		request, err = http.NewRequest(args.req.method, url, bytes.NewBuffer(args.req.body))
		if err == nil {
			request.Header.Set("Content-Type", "application/json")
		}
		if glog.V(4) { // super-verbose
			l := len(args.req.body)
			if l > 16 {
				l = 16
			}
			glog.Infof("%s %s %s...}", args.req.method, url, string(args.req.body[:l]))
		}
	}

	if err != nil {
		errstr = fmt.Sprintf("Unexpected failure to create http request %s %s, err: %v", args.req.method, url, err)
		return callResult{args.si, outjson, err, errstr, status}
	}

	copyHeaders(args.req.header, &request.Header)
	switch args.timeout {
	case defaultTimeout:
		response, err = h.httpclient.Do(request)
	case longTimeout:
		response, err = h.httpclientLongTimeout.Do(request)
	default:
		contextwith, cancel := context.WithTimeout(context.Background(), args.timeout)
		defer cancel() // timeout => context.deadlineExceededError
		newRequest := request.WithContext(contextwith)
		copyHeaders(args.req.header, &newRequest.Header)
		if args.timeout > h.httpclient.Timeout {
			response, err = h.httpclientLongTimeout.Do(newRequest)
		} else {
			response, err = h.httpclient.Do(newRequest)
		}
	}
	if err != nil {
		if response != nil && response.StatusCode > 0 {
			errstr = fmt.Sprintf("Failed to http-call %s (%s %s): status %s, err %v", sid, args.req.method, url, response.Status, err)
			status = response.StatusCode
			return callResult{args.si, outjson, err, errstr, status}
		}

		errstr = fmt.Sprintf("Failed to http-call %s (%s %s): err %v", sid, args.req.method, url, err)
		return callResult{args.si, outjson, err, errstr, status}
	}

	if outjson, err = ioutil.ReadAll(response.Body); err != nil {
		errstr = fmt.Sprintf("Failed to http-call %s (%s %s): read response err: %v", sid, args.req.method, url, err)
		if err == io.EOF {
			trailer := response.Trailer.Get("Error")
			if trailer != "" {
				errstr = fmt.Sprintf("Failed to http-call %s (%s %s): err: %v, trailer: %s", sid, args.req.method, url, err, trailer)
			}
		}

		response.Body.Close()
		return callResult{args.si, outjson, err, errstr, status}
	}
	response.Body.Close()

	// err == nil && bad status: response.Body contains the error message
	if response.StatusCode >= http.StatusBadRequest {
		err = fmt.Errorf("%s, status code: %d", outjson, response.StatusCode)
		errstr = err.Error()
		status = response.StatusCode
		return callResult{args.si, outjson, err, errstr, status}
	}

	if sid != "unknown" {
		h.keepalive.heardFrom(sid, false /* reset */)
	}

	return callResult{args.si, outjson, err, errstr, status}
}

// broadcast sends a http call to all servers in parallel, wait until all calls are returned
// NOTE: 'u' has only the path and query part, host portion will be set by this function.
func (h *httprunner) broadcast(bcastArgs bcastCallArgs) chan callResult {
	serverCount := 0
	for _, serverMap := range bcastArgs.servers {
		serverCount += len(serverMap)
	}
	ch := make(chan callResult, serverCount)
	wg := &sync.WaitGroup{}

	for _, serverMap := range bcastArgs.servers {
		for sid, serverInfo := range serverMap {
			if sid == h.si.DaemonID {
				continue
			}
			wg.Add(1)
			go func(di *cluster.Snode) {
				args := callArgs{
					si:      di,
					req:     bcastArgs.req,
					timeout: bcastArgs.timeout,
				}
				args.req.base = ""
				if bcastArgs.internal {
					args.req.base = di.IntraControlNet.DirectURL
				}

				res := h.call(args)
				ch <- res
				wg.Done()
			}(serverInfo)
		}
	}

	wg.Wait()
	close(ch)
	return ch
}

//=============================
//
// http request parsing helpers
//
//=============================

// remove validated fields and return the resulting slice
func (h *httprunner) checkRESTItems(w http.ResponseWriter, r *http.Request, itemsAfter int, splitAfter bool, items ...string) ([]string, error) {
	items, err := cmn.MatchRESTItems(r.URL.Path, itemsAfter, splitAfter, items...)
	if err != nil {
		s := err.Error()
		if _, file, line, ok := runtime.Caller(1); ok {
			f := filepath.Base(file)
			s += fmt.Sprintf("(%s, #%d)", f, line)
		}
		h.invalmsghdlr(w, r, s, http.StatusBadRequest)
		return nil, errors.New(s)
	}

	return items, nil
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

	err = jsoniter.Unmarshal(b, out)
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
		h.statsif.addErrorHTTP(r.Method, 1)
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

//=========================
//
// common http req handlers
//
//==========================
func (h *httprunner) httpdaeget(w http.ResponseWriter, r *http.Request) {
	var (
		jsbytes []byte
		err     error
		getWhat = r.URL.Query().Get(cmn.URLParamWhat)
	)
	switch getWhat {
	case cmn.GetWhatConfig:
		jsbytes, err = jsoniter.Marshal(ctx.config)
		cmn.Assert(err == nil, err)
	case cmn.GetWhatSmap:
		jsbytes, err = jsoniter.Marshal(h.smapowner.get())
		cmn.Assert(err == nil, err)
	case cmn.GetWhatBucketMeta:
		jsbytes, err = jsoniter.Marshal(h.bmdowner.get())
		cmn.Assert(err == nil, err)
	case cmn.GetWhatSmapVote:
		_, xx := h.xactinp.findL(cmn.ActElection)
		vote := xx != nil
		msg := SmapVoteMsg{VoteInProgress: vote, Smap: h.smapowner.get(), BucketMD: h.bmdowner.get()}
		jsbytes, err = jsoniter.Marshal(msg)
		cmn.Assert(err == nil, err)
	case cmn.GetWhatDaemonInfo:
		jsbytes, err = jsoniter.Marshal(h.si)
		cmn.Assert(err == nil, err)
	default:
		s := fmt.Sprintf("Invalid GET /daemon request: unrecognized what=%s", getWhat)
		h.invalmsghdlr(w, r, s)
		return
	}
	h.writeJSON(w, r, jsbytes, "httpdaeget-"+getWhat)
}

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
	case "replicate_on_cold_get":
		if v, err := strconv.ParseBool(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse replicate_on_cold_get, err: %v", err)
		} else {
			ctx.config.Replication.ReplicateOnColdGet = v
		}
	case "replicate_on_put":
		if v, err := strconv.ParseBool(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse replicate_on_put, err: %v", err)
		} else {
			ctx.config.Replication.ReplicateOnPut = v
		}
	case "replicate_on_lru_eviction":
		if v, err := strconv.ParseBool(value); err != nil {
			errstr = fmt.Sprintf("Failed to parse replicate_on_lru_eviction, err: %v", err)
		} else {
			ctx.config.Replication.ReplicateOnLRUEviction = v
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
		if value == cmn.ChecksumXXHash || value == cmn.ChecksumNone {
			ctx.config.Cksum.Checksum = value
		} else {
			return fmt.Sprintf("Invalid %s type %s - expecting %s or %s", name, value, cmn.ChecksumXXHash, cmn.ChecksumNone)
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
			ctx.config.FSHC.Enabled = v
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

func (h *httprunner) invalmsghdlr(w http.ResponseWriter, r *http.Request, msg string, errCode ...int) {
	cmn.InvalidHandlerDetailed(w, r, msg, errCode...)
	h.statsif.addErrorHTTP(r.Method, 1)
}

//=====================
//
// metasync Rx handlers
//
//=====================
func (h *httprunner) extractSmap(payload cmn.SimpleKVs) (newsmap *smapX, msg *cmn.ActionMsg, errstr string) {
	if _, ok := payload[smaptag]; !ok {
		return
	}
	newsmap, msg = &smapX{}, &cmn.ActionMsg{}
	smapvalue := payload[smaptag]
	msgvalue := ""
	if err := jsoniter.Unmarshal([]byte(smapvalue), newsmap); err != nil {
		errstr = fmt.Sprintf("Failed to unmarshal new smap, value (%+v, %T), err: %v", smapvalue, smapvalue, err)
		return
	}
	if _, ok := payload[smaptag+actiontag]; ok {
		msgvalue = payload[smaptag+actiontag]
		if err := jsoniter.Unmarshal([]byte(msgvalue), msg); err != nil {
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
		if h.si != nil && localsmap.GetTarget(h.si.DaemonID) == nil {
			errstr = fmt.Sprintf("%s: Attempt to downgrade Smap v%d to v%d", h.si.DaemonID, myver, newsmap.version())
			newsmap = nil
			return
		}
		if h.si != nil && localsmap.GetTarget(h.si.DaemonID) != nil {
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
	glog.Infof("receive Smap v%d (local v%d), ntargets %d%s", newsmap.version(), localsmap.version(), newsmap.CountTargets(), s)
	return
}

func (h *httprunner) extractbucketmd(payload cmn.SimpleKVs) (newbucketmd *bucketMD, msg *cmn.ActionMsg, errstr string) {
	if _, ok := payload[bucketmdtag]; !ok {
		return
	}
	newbucketmd, msg = &bucketMD{}, &cmn.ActionMsg{}
	bmdvalue := payload[bucketmdtag]
	msgvalue := ""
	if err := jsoniter.Unmarshal([]byte(bmdvalue), newbucketmd); err != nil {
		errstr = fmt.Sprintf("Failed to unmarshal new bucket-metadata, value (%+v, %T), err: %v", bmdvalue, bmdvalue, err)
		return
	}
	if _, ok := payload[bucketmdtag+actiontag]; ok {
		msgvalue = payload[bucketmdtag+actiontag]
		if err := jsoniter.Unmarshal([]byte(msgvalue), msg); err != nil {
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

func (h *httprunner) extractRevokedTokenList(payload cmn.SimpleKVs) (*TokenList, string) {
	bytes, ok := payload[tokentag]
	if !ok {
		return nil, ""
	}

	msg := cmn.ActionMsg{}
	if _, ok := payload[tokentag+actiontag]; ok {
		msgvalue := payload[tokentag+actiontag]
		if err := jsoniter.Unmarshal([]byte(msgvalue), &msg); err != nil {
			errstr := fmt.Sprintf(
				"Failed to unmarshal action message, value (%+v, %T), err: %v",
				msgvalue, msgvalue, err)
			return nil, errstr
		}
	}

	tokenList := &TokenList{}
	if err := jsoniter.Unmarshal([]byte(bytes), tokenList); err != nil {
		return nil, fmt.Sprintf(
			"Failed to unmarshal blocked token list, value (%+v, %T), err: %v",
			bytes, bytes, err)
	}

	s := ""
	if msg.Action != "" {
		s = ", action " + msg.Action
	}
	glog.Infof("received TokenList ntokens %d%s", len(tokenList.Tokens), s)

	return tokenList, ""
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
func (h *httprunner) join(isproxy bool, query url.Values) (res callResult) {
	url, psi := h.getPrimaryURLAndSI()
	res = h.registerToURL(url, psi, defaultTimeout, isproxy, query, false)
	if res.err == nil {
		return
	}
	if ctx.config.Proxy.DiscoveryURL != "" && ctx.config.Proxy.DiscoveryURL != url {
		glog.Errorf("%s: (register => %s: %v - retrying => %s...)", h.si.DaemonID, url, res.err, ctx.config.Proxy.DiscoveryURL)
		resAlt := h.registerToURL(ctx.config.Proxy.DiscoveryURL, psi, defaultTimeout, isproxy, query, false)
		if resAlt.err == nil {
			res = resAlt
			return
		}
	}
	if ctx.config.Proxy.OriginalURL != "" && ctx.config.Proxy.OriginalURL != url &&
		ctx.config.Proxy.OriginalURL != ctx.config.Proxy.DiscoveryURL {
		glog.Errorf("%s: (register => %s: %v - retrying => %s...)", h.si.DaemonID, url, res.err, ctx.config.Proxy.OriginalURL)
		resAlt := h.registerToURL(ctx.config.Proxy.OriginalURL, psi, defaultTimeout, isproxy, query, false)
		if resAlt.err == nil {
			res = resAlt
			return
		}
	}
	return
}

func (h *httprunner) registerToURL(url string, psi *cluster.Snode, timeout time.Duration, isproxy bool, query url.Values,
	keepalive bool) (res callResult) {
	info, err := jsoniter.Marshal(h.si)
	cmn.Assert(err == nil, err)

	path := cmn.URLPath(cmn.Version, cmn.Cluster)
	if isproxy {
		path += cmn.URLPath(cmn.Proxy)
	}
	if keepalive {
		path += cmn.URLPath(cmn.Keepalive)
	}

	callArgs := callArgs{
		si: psi,
		req: reqArgs{
			method: http.MethodPost,
			base:   url,
			path:   path,
			query:  query,
			body:   info,
		},
		timeout: timeout,
	}
	for rcount := 0; rcount < 2; rcount++ {
		res = h.call(callArgs)
		if res.err == nil {
			return
		}
		if cmn.IsErrConnectionRefused(res.err) {
			glog.Errorf("%s: (register => %s: connection refused)", h.si.DaemonID, path)
		} else {
			glog.Errorf("%s: (register => %s: %v)", h.si.DaemonID, path, res.err)
		}
		break
	}
	return
}

// getPrimaryURLAndSI is a helper function to return primary proxy's URL and daemon info
// if Smap is not yet synced, use the primary proxy from the config
// smap lock is acquired to avoid race between this function and other smap access (for example,
// receiving smap during metasync)
func (h *httprunner) getPrimaryURLAndSI() (url string, proxysi *cluster.Snode) {
	smap := h.smapowner.get()
	if smap == nil || smap.ProxySI == nil {
		url, proxysi = ctx.config.Proxy.PrimaryURL, nil
		return
	}
	if smap.ProxySI.DaemonID != "" {
		url, proxysi = smap.ProxySI.PublicNet.DirectURL, smap.ProxySI
		return
	}
	url, proxysi = ctx.config.Proxy.PrimaryURL, smap.ProxySI
	return
}

//
// StatsD client using 8125 (default) StatsD port - https://github.com/etsy/statsd
//
func (h *httprunner) initStatsD(daemonStr string) (err error) {
	suffix := strings.Replace(h.si.DaemonID, ":", "_", -1)
	h.statsdC, err = statsd.New("localhost", 8125, daemonStr+"."+suffix)
	if err != nil {
		glog.Infoln("Failed to connect to StatsD daemon")
	}
	return
}

func isReplicationPUT(r *http.Request) (isreplica bool, replicasrc string) {
	replicasrc = r.Header.Get(cmn.HeaderDFCReplicationSrc)
	return replicasrc != "", replicasrc
}
