// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
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

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/3rdparty/golang/mux"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/stats/statsd"
	"github.com/OneOfOne/xxhash"
	jsoniter "github.com/json-iterator/go"
)

const ( //  h.call(timeout)
	defaultTimeout = time.Duration(-1)
	longTimeout    = time.Duration(0)

	unknownDaemonID = "unknown"
)

type (
	metric = statsd.Metric // type alias

	// callResult contains http response
	callResult struct {
		si      *cluster.Snode
		outjson []byte
		err     error
		details string
		status  int
	}

	// callArgs contains arguments for a peer-to-peer control plane call
	callArgs struct {
		req     cmn.ReqArgs
		timeout time.Duration
		si      *cluster.Snode
	}

	// bcastCallArgs contains arguments for an intra-cluster broadcast call
	bcastCallArgs struct {
		req     cmn.ReqArgs
		network string // on of the cmn.KnownNetworks
		timeout time.Duration
		nodes   []cluster.NodeMap
	}

	networkHandler struct {
		r   string           // resource
		h   http.HandlerFunc // handler
		net []string
	}

	// SmapVoteMsg contains the cluster map and a bool representing whether or not a vote is currently happening.
	SmapVoteMsg struct {
		VoteInProgress bool      `json:"vote_in_progress"`
		Smap           *smapX    `json:"smap"`
		BucketMD       *bucketMD `json:"bucketmd"`
	}

	// actionMsgInternal is an extended ActionMsg with extra information for node <=> node control plane communications
	actionMsgInternal struct {
		cmn.ActionMsg
		BMDVersion  int64  `json:"bmdversion"`
		SmapVersion int64  `json:"smapversion"`
		NewDaemonID string `json:"newdaemonid"` // used when a node joins cluster
	}

	// http server and http runner (common for proxy and target)
	netServer struct {
		s             *http.Server
		mux           *mux.ServeMux
		sndRcvBufSize int
	}
	httprunner struct {
		cmn.Named
		publicServer          *netServer
		intraControlServer    *netServer
		intraDataServer       *netServer
		logger                *log.Logger
		si                    *cluster.Snode
		httpclient            *http.Client // http client for intra-cluster comm
		httpclientLongTimeout *http.Client // http client for long-wait intra-cluster comm
		keepalive             keepaliver
		smapowner             *smapowner
		bmdowner              *bmdowner
		xactions              *xactionsRegistry
		statsif               stats.Tracker
		statsdC               statsd.Client
	}

	// AWS and GCP provider interface
	cloudProvider interface {
		cluster.CloudProvider
		headBucket(ctx context.Context, bucket string) (bucketProps cmn.SimpleKVs, err error, errCode int)
		getBucketNames(ctx context.Context) (buckets []string, err error, errCode int)

		headObj(ctx context.Context, lom *cluster.LOM) (objMeta cmn.SimpleKVs, err error, errCode int)
		getObj(ctx context.Context, fqn string, lom *cluster.LOM) (err error, errCode int)
		putObj(ctx context.Context, r io.Reader, lom *cluster.LOM) (version string, err error, errCode int)
		deleteObj(ctx context.Context, lom *cluster.LOM) (err error, errCode int)
	}

	glogWriter struct{}
)

const (
	initialBucketListSize = 128 //nolint:unused,varcheck,deadcode
)

////////////////
// glogWriter //
////////////////

func (r *glogWriter) Write(p []byte) (int, error) {
	n := len(p)
	s := string(p[:n])
	glog.Errorln(s)

	stacktrace := debug.Stack()
	n1 := len(stacktrace)
	s1 := string(stacktrace[:n1])
	glog.Errorln(s1)
	return n, nil
}

///////////////
// netServer //
///////////////

// Override muxer ServeHTTP to support proxying HTTPS requests. Clients
// initiate all HTTPS requests with CONNECT method instead of GET/PUT etc.
func (server *netServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodConnect {
		server.mux.ServeHTTP(w, r)
		return
	}

	// TODO: add support for caching HTTPS requests
	destConn, err := net.DialTimeout("tcp", r.Host, 10*time.Second)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	// First, send that everything is OK. Trying to write a header after
	// hijacking generates a warning and nothing works
	w.WriteHeader(http.StatusOK)
	// Second, hijack the connection. A kind of man-in-the-middle attack
	// Since this moment this function is responsible of HTTP connection
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "Client does not support hijacking", http.StatusInternalServerError)
		return
	}
	clientConn, _, err := hijacker.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	}

	// Third, start transparently sending data between source and destination
	// by creating a tunnel between them
	transfer := func(destination io.WriteCloser, source io.ReadCloser) {
		io.Copy(destination, source)
		source.Close()
		destination.Close()
	}

	// NOTE: it looks like double closing both connections.
	// Need to check how the tunnel works
	go transfer(destConn, clientConn)
	go transfer(clientConn, destConn)
}

func (server *netServer) listenAndServe(addr string, logger *log.Logger) error {
	config := cmn.GCO.Get()

	// Optimization: use "slow" HTTP handler only if the cluster works in Cloud
	// reverse proxy mode. Without the optimization every HTTP request would
	// waste time by getting and reading global configuration, string
	// comparison and branching
	var httpHandler http.Handler = server.mux
	if config.Net.HTTP.RevProxy == cmn.RevProxyCloud {
		httpHandler = server
	}
	server.s = &http.Server{
		Addr:     addr,
		Handler:  httpHandler,
		ErrorLog: logger,
	}
	if server.sndRcvBufSize > 0 {
		server.s.ConnState = server.connStateListener // setsockopt; see also cmn.NewTransport
	}
	if config.Net.HTTP.UseHTTPS {
		if err := server.s.ListenAndServeTLS(config.Net.HTTP.Certificate, config.Net.HTTP.Key); err != nil {
			if err != http.ErrServerClosed {
				glog.Errorf("Terminated server with err: %v", err)
				return err
			}
		}
	} else {
		if err := server.s.ListenAndServe(); err != nil {
			if err != http.ErrServerClosed {
				glog.Errorf("Terminated server with err: %v", err)
				return err
			}
		}
	}

	return nil
}

func (server *netServer) connStateListener(c net.Conn, cs http.ConnState) {
	if cs != http.StateNew {
		return
	}
	tcpconn, ok := c.(*net.TCPConn)
	cmn.Assert(ok)
	rawconn, _ := tcpconn.SyscallConn()
	args := cmn.TransportArgs{SndRcvBufSize: server.sndRcvBufSize}
	rawconn.Control(args.ConnControl(rawconn))
}

func (server *netServer) shutdown() {
	contextwith, cancel := context.WithTimeout(context.Background(), cmn.GCO.Get().Timeout.Default)
	if err := server.s.Shutdown(contextwith); err != nil {
		glog.Infof("Stopped server, err: %v", err)
	}
	cancel()
}

////////////////
// httprunner //
////////////////

func (h *httprunner) registerNetworkHandlers(networkHandlers []networkHandler) {
	config := cmn.GCO.Get()

	for _, nh := range networkHandlers {
		if nh.r == "/" {
			if cmn.StringInSlice(cmn.NetworkPublic, nh.net) {
				h.registerPublicNetHandler("/", cmn.InvalidHandler)
			}
			if config.Net.UseIntraControl {
				h.registerIntraControlNetHandler(cmn.URLPath(cmn.Version, nh.r), nh.h)
			}
			if config.Net.UseIntraData {
				h.registerIntraDataNetHandler(cmn.URLPath(cmn.Version, nh.r), nh.h)
			}
			continue
		}

		if cmn.StringInSlice(cmn.NetworkPublic, nh.net) {
			h.registerPublicNetHandler(cmn.URLPath(cmn.Version, nh.r), nh.h)

			if config.Net.UseIntraControl && cmn.StringInSlice(cmn.NetworkIntraControl, nh.net) {
				h.registerIntraControlNetHandler(cmn.URLPath(cmn.Version, nh.r), nh.h)
			}
			if config.Net.UseIntraData && cmn.StringInSlice(cmn.NetworkIntraData, nh.net) {
				h.registerIntraDataNetHandler(cmn.URLPath(cmn.Version, nh.r), nh.h)
			}
		} else if cmn.StringInSlice(cmn.NetworkIntraControl, nh.net) {
			h.registerIntraControlNetHandler(cmn.URLPath(cmn.Version, nh.r), nh.h)

			if config.Net.UseIntraData && cmn.StringInSlice(cmn.NetworkIntraData, nh.net) {
				h.registerIntraDataNetHandler(cmn.URLPath(cmn.Version, nh.r), nh.h)
			}
		} else if cmn.StringInSlice(cmn.NetworkIntraData, nh.net) {
			h.registerIntraDataNetHandler(cmn.URLPath(cmn.Version, nh.r), nh.h)
		}
	}
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

func (h *httprunner) init(s stats.Tracker, config *cmn.Config) {
	h.statsif = s
	h.httpclient = cmn.NewClient(cmn.TransportArgs{
		Timeout:  config.Timeout.Default,
		UseHTTPS: config.Net.HTTP.UseHTTPS,
	})
	h.httpclientLongTimeout = cmn.NewClient(cmn.TransportArgs{
		Timeout:  config.Timeout.DefaultLong,
		UseHTTPS: config.Net.HTTP.UseHTTPS,
	})

	bufsize := config.Net.L4.SndRcvBufSize
	if h.si.IsProxy() {
		bufsize = 0
	}
	h.publicServer = &netServer{
		mux:           mux.NewServeMux(),
		sndRcvBufSize: bufsize,
	}
	h.intraControlServer = h.publicServer // by default intra control net is the same as public
	if config.Net.UseIntraControl {
		h.intraControlServer = &netServer{
			mux:           mux.NewServeMux(),
			sndRcvBufSize: 0,
		}
	}
	h.intraDataServer = h.publicServer // by default intra data net is the same as public
	if config.Net.UseIntraData {
		h.intraDataServer = &netServer{
			mux:           mux.NewServeMux(),
			sndRcvBufSize: bufsize,
		}
	}

	h.smapowner = newSmapowner()
	h.bmdowner = newBmdowner()
	h.xactions = newXactions() // extended actions
}

// initSI initializes this cluster.Snode
func (h *httprunner) initSI(daemonType string) {
	var (
		s                string
		config           = cmn.GCO.Get()
		port             = config.Net.L4.Port
		allowLoopback, _ = cmn.ParseBool(os.Getenv("ALLOW_LOOPBACK"))
		addrList, err    = getLocalIPv4List(allowLoopback)
	)
	if err != nil {
		glog.Fatalf("FATAL: %v", err)
	}

	ipAddr, err := getIPv4(addrList, config.Net.IPv4)
	if err != nil {
		glog.Fatalf("Failed to get PUBLIC IPv4/hostname: %v", err)
	}
	if config.Net.IPv4 != "" {
		s = " (config: " + config.Net.IPv4 + ")"
	}
	glog.Infof("PUBLIC (user) access: [%s:%d]%s", ipAddr, config.Net.L4.Port, s)

	ipAddrIntraControl := net.IP{}
	if config.Net.UseIntraControl {
		ipAddrIntraControl, err = getIPv4(addrList, config.Net.IPv4IntraControl)
		if err != nil {
			glog.Fatalf("Failed to get INTRA-CONTROL IPv4/hostname: %v", err)
		}
		s = ""
		if config.Net.IPv4IntraControl != "" {
			s = " (config: " + config.Net.IPv4IntraControl + ")"
		}
		glog.Infof("INTRA-CONTROL access: [%s:%d]%s", ipAddrIntraControl, config.Net.L4.PortIntraControl, s)
	}

	ipAddrIntraData := net.IP{}
	if config.Net.UseIntraData {
		ipAddrIntraData, err = getIPv4(addrList, config.Net.IPv4IntraData)
		if err != nil {
			glog.Fatalf("Failed to get INTRA-DATA IPv4/hostname: %v", err)
		}
		s = ""
		if config.Net.IPv4IntraData != "" {
			s = " (config: " + config.Net.IPv4IntraData + ")"
		}
		glog.Infof("INTRA-DATA access: [%s:%d]%s", ipAddrIntraData, config.Net.L4.PortIntraData, s)
	}

	mustDiffer := func(ip1 net.IP, port1 int, use1 bool, ip2 net.IP, port2 int, use2 bool, tag string) {
		if !use1 || !use2 {
			return
		}
		if string(ip1) == string(ip2) && port1 == port2 {
			glog.Fatalf("%s: cannot use the same IP:port (%s:%d) for two networks", tag, string(ip1), port1)
		}
	}
	mustDiffer(ipAddr, config.Net.L4.Port, true, ipAddrIntraControl, config.Net.L4.PortIntraControl, config.Net.UseIntraControl, "pub/ctl")
	mustDiffer(ipAddr, config.Net.L4.Port, true, ipAddrIntraData, config.Net.L4.PortIntraData, config.Net.UseIntraData, "pub/data")
	mustDiffer(ipAddrIntraData, config.Net.L4.PortIntraData, config.Net.UseIntraData,
		ipAddrIntraControl, config.Net.L4.PortIntraControl, config.Net.UseIntraControl, "ctl/data")

	publicAddr := &net.TCPAddr{
		IP:   ipAddr,
		Port: port,
	}
	intraControlAddr := &net.TCPAddr{
		IP:   ipAddrIntraControl,
		Port: config.Net.L4.PortIntraControl,
	}
	intraDataAddr := &net.TCPAddr{
		IP:   ipAddrIntraData,
		Port: config.Net.L4.PortIntraData,
	}

	daemonID := os.Getenv("AIS_DAEMONID")
	if daemonID == "" {
		cs := xxhash.ChecksumString32S(publicAddr.String(), cmn.MLCG32)
		daemonID = strconv.Itoa(int(cs & 0xfffff))
		if config.TestingEnv() {
			daemonID += "_" + config.Net.L4.PortStr
		}
	}

	h.si = newSnode(daemonID, config.Net.HTTP.Proto, daemonType, publicAddr, intraControlAddr, intraDataAddr)
}

func (h *httprunner) run() error {
	config := cmn.GCO.Get()

	// A wrapper to glog http.Server errors - otherwise
	// os.Stderr would be used, as per golang.org/pkg/net/http/#Server
	h.logger = log.New(&glogWriter{}, "net/http err: ", 0)
	if config.Net.UseIntraControl || config.Net.UseIntraData {
		var errCh chan error
		if config.Net.UseIntraControl && config.Net.UseIntraData {
			errCh = make(chan error, 3)
		} else {
			errCh = make(chan error, 2)
		}

		if config.Net.UseIntraControl {
			go func() {
				addr := h.si.IntraControlNet.NodeIPAddr + ":" + h.si.IntraControlNet.DaemonPort
				errCh <- h.intraControlServer.listenAndServe(addr, h.logger)
			}()
		}

		if config.Net.UseIntraData {
			go func() {
				addr := h.si.IntraDataNet.NodeIPAddr + ":" + h.si.IntraDataNet.DaemonPort
				errCh <- h.intraDataServer.listenAndServe(addr, h.logger)
			}()
		}

		go func() {
			addr := h.si.PublicNet.NodeIPAddr + ":" + h.si.PublicNet.DaemonPort
			errCh <- h.publicServer.listenAndServe(addr, h.logger)
		}()

		return <-errCh
	}

	// When only public net is configured listen on *:port
	addr := ":" + h.si.PublicNet.DaemonPort
	return h.publicServer.listenAndServe(addr, h.logger)
}

// stop gracefully
func (h *httprunner) stop(err error) {
	config := cmn.GCO.Get()
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

	if config.Net.UseIntraControl {
		wg.Add(1)
		go func() {
			h.intraControlServer.shutdown()
			wg.Done()
		}()
	}

	if config.Net.UseIntraData {
		wg.Add(1)
		go func() {
			h.intraDataServer.shutdown()
			wg.Done()
		}()
	}

	wg.Wait()
}

//
// intra-cluster IPC, control plane
// call another target or a proxy; optionally, include a json-encoded body
//
func (h *httprunner) call(args callArgs) callResult {
	var (
		req     *http.Request
		sid     = unknownDaemonID
		outjson []byte
		err     error
		details string
		status  int
		client  *http.Client
	)

	if args.si != nil {
		sid = args.si.DaemonID
	}

	cmn.Assert(args.si != nil || args.req.Base != "") // either we have si or base
	if args.req.Base == "" && args.si != nil {
		args.req.Base = args.si.IntraControlNet.DirectURL // by default use intra-cluster control network
	}

	if args.req.Header == nil {
		args.req.Header = make(http.Header)
	}

	switch args.timeout {
	case defaultTimeout:
		req, err = args.req.Req()
		if err != nil {
			break
		}

		client = h.httpclient
	case longTimeout:
		req, err = args.req.Req()
		if err != nil {
			break
		}

		client = h.httpclientLongTimeout
	default:
		var cancel context.CancelFunc
		req, _, cancel, err = args.req.ReqWithTimeout(args.timeout)
		if err != nil {
			break
		}
		defer cancel() // timeout => context.deadlineExceededError

		if args.timeout > h.httpclient.Timeout {
			client = h.httpclientLongTimeout
		} else {
			client = h.httpclient
		}
	}

	if err != nil {
		details = fmt.Sprintf("unexpected failure to create HTTP request %s %s, err: %v", args.req.Method, args.req.URL(), err)
		return callResult{args.si, outjson, err, details, status}
	}

	req.Header.Set(cmn.HeaderCallerID, h.si.DaemonID)
	req.Header.Set(cmn.HeaderCallerName, h.si.Name())
	if smap := h.smapowner.get(); smap.isValid() {
		req.Header.Set(cmn.HeaderCallerSmapVersion, strconv.FormatInt(smap.version(), 10))
	}

	resp, err := client.Do(req)

	if err != nil {
		if resp != nil && resp.StatusCode > 0 {
			details = fmt.Sprintf("Failed to HTTP-call %s (%s %s): status %s, err %v", sid, args.req.Method, args.req.URL(), resp.Status, err)
			status = resp.StatusCode
			return callResult{args.si, outjson, err, details, status}
		}

		details = fmt.Sprintf("Failed to HTTP-call %s (%s %s): err %v", sid, args.req.Method, args.req.URL(), err)
		return callResult{args.si, outjson, err, details, status}
	}

	if outjson, err = ioutil.ReadAll(resp.Body); err != nil {
		details = fmt.Sprintf("Failed to HTTP-call %s (%s %s): read response err: %v", sid, args.req.Method, args.req.URL(), err)
		if err == io.EOF {
			trailer := resp.Trailer.Get("Error")
			if trailer != "" {
				details = fmt.Sprintf("Failed to HTTP-call %s (%s %s): err: %v, trailer: %s", sid, args.req.Method, args.req.URL(), err, trailer)
			}
		}

		resp.Body.Close()
		return callResult{args.si, outjson, err, details, status}
	}
	resp.Body.Close()

	// err == nil && bad status: resp.Body contains the error message
	if resp.StatusCode >= http.StatusBadRequest {
		err = errors.New(string(outjson))
		details = err.Error()
		status = resp.StatusCode
		return callResult{args.si, outjson, err, details, status}
	}

	if sid != unknownDaemonID {
		h.keepalive.heardFrom(sid, false /* reset */)
	}

	return callResult{args.si, outjson, err, details, resp.StatusCode}
}

///////////////
// broadcast //
///////////////

func (h *httprunner) broadcastTo(path string, query url.Values, method string, body []byte,
	smap *smapX, timeout time.Duration, network string, to int) chan callResult {
	var nodes []cluster.NodeMap

	switch to {
	case cluster.Targets:
		nodes = []cluster.NodeMap{smap.Tmap}
	case cluster.Proxies:
		nodes = []cluster.NodeMap{smap.Pmap}
	case cluster.AllNodes:
		nodes = []cluster.NodeMap{smap.Pmap, smap.Tmap}
	default:
		cmn.Assert(false)
	}
	if !cmn.NetworkIsKnown(network) {
		cmn.AssertMsg(false, "unknown network '"+network+"'")
	}
	bcastArgs := bcastCallArgs{
		req: cmn.ReqArgs{
			Method: method,
			Path:   path,
			Query:  query,
			Body:   body,
		},
		network: network,
		timeout: timeout,
		nodes:   nodes,
	}
	return h.broadcast(bcastArgs)
}

// NOTE: 'u' has only the path and query part, host portion will be set by this method.
func (h *httprunner) broadcast(bcastArgs bcastCallArgs) chan callResult {
	nodeCount := 0
	for _, nodeMap := range bcastArgs.nodes {
		nodeCount += len(nodeMap)
	}
	if nodeCount == 0 {
		ch := make(chan callResult)
		close(ch)
		glog.Warningf("node count zero in [%+v] bcast", bcastArgs.req)
		return ch
	}
	ch := make(chan callResult, nodeCount)
	wg := &sync.WaitGroup{}

	for _, nodeMap := range bcastArgs.nodes {
		for sid, serverInfo := range nodeMap {
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
				args.req.Base = di.URL(bcastArgs.network)

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

func (h *httprunner) newActionMsgInternalStr(msgStr string, smap *smapX, bmdowner *bucketMD) *actionMsgInternal {
	return h.newActionMsgInternal(&cmn.ActionMsg{Value: msgStr}, smap, bmdowner)
}

func (h *httprunner) newActionMsgInternal(actionMsg *cmn.ActionMsg, smap *smapX, bmdowner *bucketMD) *actionMsgInternal {
	msgInt := &actionMsgInternal{ActionMsg: *actionMsg}
	if smap != nil {
		msgInt.SmapVersion = smap.Version
	} else {
		msgInt.SmapVersion = h.smapowner.Get().Version
	}
	if bmdowner != nil {
		msgInt.BMDVersion = bmdowner.Version
	} else {
		msgInt.BMDVersion = h.bmdowner.Get().Version
	}
	return msgInt
}

//////////////////////////////////
// HTTP request parsing helpers //
//////////////////////////////////

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
		h.statsif.AddErrorHTTP(r.Method, 1)
		return
	}
	msg := fmt.Sprintf("%s: Failed to write json, err: %v", tag, err)
	if _, file, line, ok := runtime.Caller(1); ok {
		f := filepath.Base(file)
		msg += fmt.Sprintf("(%s, #%d)", f, line)
	}
	h.invalmsghdlr(w, r, msg)
	return
}

func (h *httprunner) validateBucket(w http.ResponseWriter, r *http.Request, bucket, bckProvider string) (bmd *bucketMD, local bool) {
	var err error
	bmd = h.bmdowner.get()
	if local, err = bmd.ValidateBucket(bucket, bckProvider); err != nil {
		h.invalmsghdlr(w, r, err.Error())
		return nil, false
	}
	return
}

func (h *httprunner) parseValidateNCopies(value interface{}) (copies int, err error) {
	switch v := value.(type) {
	case string:
		copies, err = strconv.Atoi(v)
	case float64:
		copies = int(v)
	default:
		err = fmt.Errorf("failed to parse 'copies' (%v, %T) - unexpected type", value, value)
	}
	return
}

//////////////////////////////
// common HTTP req handlers //
//////////////////////////////

func (h *httprunner) httpdaeget(w http.ResponseWriter, r *http.Request) {
	var (
		body    []byte
		getWhat = r.URL.Query().Get(cmn.URLParamWhat)
	)
	switch getWhat {
	case cmn.GetWhatConfig:
		body = cmn.MustMarshal(cmn.GCO.Get())
	case cmn.GetWhatSmap:
		body = cmn.MustMarshal(h.smapowner.get())
	case cmn.GetWhatBucketMeta:
		body = cmn.MustMarshal(h.bmdowner.get())
	case cmn.GetWhatSmapVote:
		voteInProgress := h.xactions.globalXactRunning(cmn.ActElection)
		msg := SmapVoteMsg{VoteInProgress: voteInProgress, Smap: h.smapowner.get(), BucketMD: h.bmdowner.get()}
		body = cmn.MustMarshal(msg)
	case cmn.GetWhatSnode:
		body = cmn.MustMarshal(h.si)
	default:
		s := fmt.Sprintf("Invalid GET /daemon request: unrecognized what=%s", getWhat)
		h.invalmsghdlr(w, r, s)
		return
	}
	h.writeJSON(w, r, body, "httpdaeget-"+getWhat)
}

////////////////////////////////////////////
// HTTP err + spec message + code + stats //
////////////////////////////////////////////

func (h *httprunner) invalmsghdlr(w http.ResponseWriter, r *http.Request, msg string, errCode ...int) {
	if caller := r.Header.Get(cmn.HeaderCallerName); caller != "" {
		msg += " from " + caller
	}
	cmn.InvalidHandlerDetailed(w, r, msg, errCode...)
	h.statsif.AddErrorHTTP(r.Method, 1)
}

func (h *httprunner) invalmsghdlrsilent(w http.ResponseWriter, r *http.Request, msg string, errCode ...int) {
	cmn.InvalidHandlerDetailedNoLog(w, r, msg, errCode...)
}

//////////////////////////
// metasync Rx handlers //
//////////////////////////

func (h *httprunner) extractSmap(payload cmn.SimpleKVs) (newSmap *smapX, msgInt *actionMsgInternal, err error) {
	if _, ok := payload[smaptag]; !ok {
		return
	}
	newSmap, msgInt = &smapX{}, &actionMsgInternal{}
	smapValue := payload[smaptag]
	if err1 := jsoniter.Unmarshal([]byte(smapValue), newSmap); err1 != nil {
		err = fmt.Errorf("failed to unmarshal new smap, value (%+v, %T), err: %v", smapValue, smapValue, err1)
		return
	}
	if msgValue, ok := payload[smaptag+actiontag]; ok {
		if err1 := jsoniter.Unmarshal([]byte(msgValue), msgInt); err1 != nil {
			err = fmt.Errorf("failed to unmarshal action message, value (%+v, %T), err: %v", msgValue, msgValue, err1)
			return
		}
	}
	localsmap := h.smapowner.get()
	myver := localsmap.version()
	isManualReb := msgInt.Action == cmn.ActGlobalReb && msgInt.Value != nil
	if newSmap.version() == myver && !isManualReb {
		newSmap = nil
		return
	}
	if !newSmap.isValid() {
		err = fmt.Errorf("invalid Smap v%d - lacking or missing the primary", newSmap.version())
		newSmap = nil
		return
	}
	if newSmap.version() < myver {
		if h.si != nil && localsmap.GetTarget(h.si.DaemonID) == nil {
			err = fmt.Errorf("%s: attempt to downgrade Smap v%d to v%d", h.si, myver, newSmap.version())
			newSmap = nil
			return
		}
		if h.si != nil && localsmap.GetTarget(h.si.DaemonID) != nil {
			glog.Errorf("target %s: receive Smap v%d < v%d local - proceeding anyway",
				h.si.DaemonID, newSmap.version(), localsmap.version())
		} else {
			err = fmt.Errorf("attempt to downgrade Smap v%d to v%d", myver, newSmap.version())
			return
		}
	}
	s := ""
	if msgInt.Action != "" {
		s = ", action " + msgInt.Action
	}
	glog.Infof("receive Smap v%d (local v%d), ntargets %d%s", newSmap.version(), localsmap.version(), newSmap.CountTargets(), s)
	return
}

func (h *httprunner) extractbucketmd(payload cmn.SimpleKVs) (newBMD *bucketMD, msgInt *actionMsgInternal, err error) {
	if _, ok := payload[bucketmdtag]; !ok {
		return
	}
	newBMD, msgInt = &bucketMD{}, &actionMsgInternal{}
	bmdValue := payload[bucketmdtag]
	if err1 := jsoniter.Unmarshal([]byte(bmdValue), newBMD); err1 != nil {
		err = fmt.Errorf("failed to unmarshal new %s, value (%+v, %T), err: %v", bmdTermName, bmdValue, bmdValue, err1)
		return
	}
	if msgValue, ok := payload[bucketmdtag+actiontag]; ok {
		if err1 := jsoniter.Unmarshal([]byte(msgValue), msgInt); err1 != nil {
			err = fmt.Errorf("failed to unmarshal action message, value (%+v, %T), err: %v", msgValue, msgValue, err1)
			return
		}
	}
	curVersion := h.bmdowner.get().version()
	if newBMD.version() <= curVersion {
		if newBMD.version() < curVersion {
			err = fmt.Errorf("attempt to downgrade %s v%d to v%d", bmdTermName, curVersion, newBMD.version())
		}
		newBMD = nil
	}
	return
}

func (h *httprunner) extractRevokedTokenList(payload cmn.SimpleKVs) (*TokenList, error) {
	bytes, ok := payload[tokentag]
	if !ok {
		return nil, nil
	}

	msgInt := actionMsgInternal{}
	if msgValue, ok := payload[tokentag+actiontag]; ok {
		if err := jsoniter.Unmarshal([]byte(msgValue), &msgInt); err != nil {
			err := fmt.Errorf(
				"failed to unmarshal action message, value (%+v, %T), err: %v",
				msgValue, msgValue, err)
			return nil, err
		}
	}

	tokenList := &TokenList{}
	if err := jsoniter.Unmarshal([]byte(bytes), tokenList); err != nil {
		return nil, fmt.Errorf(
			"failed to unmarshal blocked token list, value (%+v, %T), err: %v",
			bytes, bytes, err)
	}

	s := ""
	if msgInt.Action != "" {
		s = ", action " + msgInt.Action
	}
	glog.Infof("received TokenList ntokens %d%s", len(tokenList.Tokens), s)

	return tokenList, nil
}

// ================================== Background =========================================
//
// Generally, AIStore clusters can be deployed with an arbitrary numbers of proxies.
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
// Here's how a node joins a AIStore cluster:
// - first, there's the primary proxy/gateway referenced by the current cluster map (Smap)
//   or - during the cluster deployment time - by the the configured "primary_url"
//   (see setup/config.sh)
//
// - if that one fails, the new node goes ahead and tries the alternatives:
// 	- config.Proxy.DiscoveryURL ("discovery_url")
// 	- config.Proxy.OriginalURL ("original_url")
// - but only if those are defined and different from the previously tried.
//
// ================================== Background =========================================
func (h *httprunner) join(query url.Values) (res callResult) {
	url, psi := h.getPrimaryURLAndSI()
	res = h.registerToURL(url, psi, defaultTimeout, query, false)
	if res.err == nil {
		return
	}
	config := cmn.GCO.Get()
	if config.Proxy.DiscoveryURL != "" && config.Proxy.DiscoveryURL != url {
		glog.Errorf("%s: (register => %s: %v - retrying => %s...)", h.si, url, res.err, config.Proxy.DiscoveryURL)
		resAlt := h.registerToURL(config.Proxy.DiscoveryURL, psi, defaultTimeout, query, false)
		if resAlt.err == nil {
			res = resAlt
			return
		}
	}
	if config.Proxy.OriginalURL != "" && config.Proxy.OriginalURL != url &&
		config.Proxy.OriginalURL != config.Proxy.DiscoveryURL {
		glog.Errorf("%s: (register => %s: %v - retrying => %s...)", h.si, url, res.err, config.Proxy.OriginalURL)
		resAlt := h.registerToURL(config.Proxy.OriginalURL, psi, defaultTimeout, query, false)
		if resAlt.err == nil {
			res = resAlt
			return
		}
	}
	return
}

func (h *httprunner) registerToURL(url string, psi *cluster.Snode, timeout time.Duration, query url.Values, keepalive bool) (res callResult) {
	req := targetRegMeta{SI: h.si}
	if h.si.IsTarget() && !keepalive {
		xBMD := &bucketMD{}
		err := xBMD.LoadFromFS()
		if err == nil && xBMD.Version != 0 {
			glog.Infof("Target %s is joining cluster and sending xattr %s version %d", h.si.DaemonID, bmdTermName, xBMD.Version)
			req.BMD = xBMD
		}
	}

	info := cmn.MustMarshal(req)

	path := cmn.URLPath(cmn.Version, cmn.Cluster)
	if keepalive {
		path += cmn.URLPath(cmn.Keepalive)
	} else {
		path += cmn.URLPath(cmn.AutoRegister)
	}

	callArgs := callArgs{
		si: psi,
		req: cmn.ReqArgs{
			Method: http.MethodPost,
			Base:   url,
			Path:   path,
			Query:  query,
			Body:   info,
		},
		timeout: timeout,
	}
	for rcount := 0; rcount < 2; rcount++ {
		res = h.call(callArgs)
		if res.err == nil {
			if !keepalive {
				glog.Infof("%s: registered => %s[%s]", h.si, url, path)
			}
			return
		}
		if cmn.IsErrConnectionRefused(res.err) {
			glog.Errorf("%s: (register => %s[%s]: connection refused)", h.si, url, path)
		} else {
			glog.Errorf("%s: (register => %s[%s]: %v)", h.si, url, path, res.err)
		}
	}
	return
}

// getPrimaryURLAndSI is a helper function to return primary proxy's URL and daemon info
// if Smap is not yet synced, use the primary proxy from the config
// smap lock is acquired to avoid race between this function and other smap access (for example,
// receiving smap during metasync)
func (h *httprunner) getPrimaryURLAndSI() (url string, proxysi *cluster.Snode) {
	config := cmn.GCO.Get()
	smap := h.smapowner.get()
	if smap == nil || smap.ProxySI == nil {
		url, proxysi = config.Proxy.PrimaryURL, nil
		return
	}
	if smap.ProxySI.DaemonID != "" {
		url, proxysi = smap.ProxySI.IntraControlNet.DirectURL, smap.ProxySI
		return
	}
	url, proxysi = config.Proxy.PrimaryURL, smap.ProxySI
	return
}
