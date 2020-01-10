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
	"github.com/NVIDIA/aistore/xaction"
	"github.com/OneOfOne/xxhash"
	jsoniter "github.com/json-iterator/go"
)

const ( //  h.call(timeout)
	unknownDaemonID = "unknown"
)

type (
	metric = statsd.Metric // type alias

	// callResult contains http response
	callResult struct {
		si      *cluster.Snode
		outjson []byte
		header  http.Header
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

	// bcastArgs contains arguments for an intra-cluster broadcast call
	bcastArgs struct {
		req     cmn.ReqArgs
		network string // on of the cmn.KnownNetworks
		timeout time.Duration

		nodes []cluster.NodeMap
		smap  *smapX
		to    int
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
		BMDVersion  int64  `json:"bmdversion,string"`
		SmapVersion int64  `json:"smapversion,string"`
		NewDaemonID string `json:"newdaemonid"` // used when a node joins cluster
		GlobRebID   int64  `json:"glob_reb_id,string"`
	}

	// http server and http runner (common for proxy and target)
	netServer struct {
		s             *http.Server
		mux           *mux.ServeMux
		sndRcvBufSize int
	}
	httprunner struct {
		cmn.Named
		publicServer       *netServer
		intraControlServer *netServer
		intraDataServer    *netServer
		logger             *log.Logger
		si                 *cluster.Snode
		httpclient         *http.Client // http client for intra-cluster comm
		httpclientGetPut   *http.Client // http client to execute target <=> target GET & PUT (object)
		keepalive          keepaliver
		smapowner          *smapowner
		bmdowner           bmdOwner
		statsif            stats.Tracker
		statsdC            statsd.Client
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

	// BMD & its origin
	errTgtBmdOriginDiffer struct{ detail string }
	errPrxBmdOriginDiffer struct{ detail string }
	errBmdOriginSplit     struct{ detail string }
	// ditto Smap
	errSmapOriginDiffer struct{ detail string }
)

const (
	// nolint:unused,varcheck,deadcode // used by `aws` and `gcp` but needs to compiled by tags
	initialBucketListSize = 128
)

/////////////////////
// BMD origin errs //
/////////////////////
var errNoBMD = errors.New("no bucket metadata")

func (e errTgtBmdOriginDiffer) Error() string { return e.detail }
func (e errBmdOriginSplit) Error() string     { return e.detail }
func (e errPrxBmdOriginDiffer) Error() string { return e.detail }
func (e errSmapOriginDiffer) Error() string   { return e.detail }

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
		cmn.InvalidHandlerDetailed(w, r, err.Error(), http.StatusServiceUnavailable)
		return
	}

	// Second, hijack the connection. A kind of man-in-the-middle attack
	// Since this moment this function is responsible of HTTP connection
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		cmn.InvalidHandlerDetailed(w, r, "Client does not support hijacking", http.StatusInternalServerError)
		return
	}

	// First, send that everything is OK. Trying to write a header after
	// hijacking generates a warning and nothing works
	w.WriteHeader(http.StatusOK)

	clientConn, _, err := hijacker.Hijack()
	if err != nil {
		// NOTE: cannot send error because we have already written a header.
		glog.Error(err)
		return
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
	if server.s == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), cmn.GCO.Get().Timeout.Default)
	if err := server.s.Shutdown(ctx); err != nil {
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
	h.httpclientGetPut = cmn.NewClient(cmn.TransportArgs{
		Timeout:         config.Timeout.DefaultLong,
		WriteBufferSize: config.Net.HTTP.WriteBufferSize,
		ReadBufferSize:  config.Net.HTTP.ReadBufferSize,
		UseHTTPS:        false, // always plain intra-cluster http for data
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
	mustDiffer(ipAddr, config.Net.L4.Port, true,
		ipAddrIntraControl, config.Net.L4.PortIntraControl, config.Net.UseIntraControl, "pub/ctl")
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
		if config.TestingEnv() {
			daemonID = strconv.Itoa(int(cs & 0xfffff))
			if daemonType == cmn.Target {
				daemonID = daemonID + "t" + config.Net.L4.PortStr
			} else {
				cmn.Assert(daemonType == cmn.Proxy)
				daemonID = daemonID + "p" + config.Net.L4.PortStr
			}
		} else {
			daemonID = strconv.Itoa(int(cs & 0xffffffff))
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
	case cmn.DefaultTimeout:
		req, err = args.req.Req()
		if err != nil {
			break
		}

		client = h.httpclient
	case cmn.LongTimeout:
		req, err = args.req.Req()
		if err != nil {
			break
		}

		client = h.httpclientGetPut
	default:
		var cancel context.CancelFunc
		req, _, cancel, err = args.req.ReqWithTimeout(args.timeout)
		if err != nil {
			break
		}
		defer cancel() // timeout => context.deadlineExceededError

		if args.timeout > h.httpclient.Timeout {
			client = h.httpclientGetPut
		} else {
			client = h.httpclient
		}
	}

	if err != nil {
		details = fmt.Sprintf("unexpected failure to create HTTP request %s %s, err: %v",
			args.req.Method, args.req.URL(), err)
		return callResult{args.si, outjson, nil, err, details, status}
	}

	req.Header.Set(cmn.HeaderCallerID, h.si.DaemonID)
	req.Header.Set(cmn.HeaderCallerName, h.si.Name())
	if smap := h.smapowner.get(); smap.isValid() {
		req.Header.Set(cmn.HeaderCallerSmapVersion, strconv.FormatInt(smap.version(), 10))
	}

	resp, err := client.Do(req)
	if err != nil {
		if resp != nil && resp.StatusCode > 0 {
			details = fmt.Sprintf("Failed to HTTP-call %s (%s %s): status %s, err %v",
				sid, args.req.Method, args.req.URL(), resp.Status, err)
			status = resp.StatusCode
			return callResult{args.si, outjson, nil, err, details, status}
		}
		details = fmt.Sprintf("Failed to HTTP-call %s (%s %s): err %v", sid, args.req.Method, args.req.URL(), err)
		return callResult{args.si, outjson, nil, err, details, status}
	}

	if outjson, err = ioutil.ReadAll(resp.Body); err != nil {
		details = fmt.Sprintf("Failed to HTTP-call %s (%s %s): read response err: %v",
			sid, args.req.Method, args.req.URL(), err)
		if err == io.EOF {
			trailer := resp.Trailer.Get("Error")
			if trailer != "" {
				details = fmt.Sprintf("Failed to HTTP-call %s (%s %s): err: %v, trailer: %s",
					sid, args.req.Method, args.req.URL(), err, trailer)
			}
		}

		resp.Body.Close()
		return callResult{args.si, outjson, nil, err, details, status}
	}
	resp.Body.Close()

	// err == nil && bad status: resp.Body contains the error message
	if resp.StatusCode >= http.StatusBadRequest {
		err = errors.New(string(outjson))
		details = err.Error()
		status = resp.StatusCode
		return callResult{args.si, outjson, nil, err, details, status}
	}

	if sid != unknownDaemonID {
		h.keepalive.heardFrom(sid, false /* reset */)
	}

	return callResult{args.si, outjson, resp.Header, err, details, resp.StatusCode}
}

///////////////
// broadcast //
///////////////

func (h *httprunner) bcastGet(args bcastArgs) chan callResult {
	cmn.Assert(args.req.Method == "")
	args.req.Method = http.MethodGet
	return h.bcastTo(args)
}
func (h *httprunner) bcastPost(args bcastArgs) chan callResult {
	cmn.Assert(args.req.Method == "")
	args.req.Method = http.MethodPost
	return h.bcastTo(args)
}
func (h *httprunner) bcastPut(args bcastArgs) chan callResult {
	cmn.Assert(args.req.Method == "")
	args.req.Method = http.MethodPut
	return h.bcastTo(args)
}

func (h *httprunner) bcastTo(args bcastArgs) chan callResult {
	if args.smap == nil {
		args.smap = h.smapowner.get()
	}
	if args.network == "" {
		args.network = cmn.NetworkIntraControl
	}
	if args.timeout == 0 {
		args.timeout = cmn.GCO.Get().Timeout.CplaneOperation
	}

	switch args.to {
	case cluster.Targets:
		args.nodes = []cluster.NodeMap{args.smap.Tmap}
	case cluster.Proxies:
		args.nodes = []cluster.NodeMap{args.smap.Pmap}
	case cluster.AllNodes:
		args.nodes = []cluster.NodeMap{args.smap.Pmap, args.smap.Tmap}
	default:
		cmn.Assert(false)
	}
	if !cmn.NetworkIsKnown(args.network) {
		cmn.AssertMsg(false, "unknown network '"+args.network+"'")
	}
	return h.bcast(args)
}

// execute 2-phase transaction vis-à-vis all targets with an optional commit if all good
func (h *httprunner) bcast2Phase(args bcastArgs, errmsg string, commit bool) (err error) {
	var (
		results chan callResult
		nr      int
		path    = args.req.Path
	)
	// begin
	args.req.Method = http.MethodPost
	args.req.Path = cmn.URLPath(path, cmn.ActBegin)
	results = h.bcastTo(args)
	for res := range results {
		if res.err != nil {
			err = fmt.Errorf("%s: %s: %v(%d)", res.si.Name(), errmsg, res.err, res.status)
			glog.Errorln(err.Error())
			nr++
		}
	}
	// abort
	if err != nil {
		args.req.Path = cmn.URLPath(path, cmn.ActAbort)
		if nr < len(results) {
			_ = h.bcastTo(args)
		}
		return
	}
	if !commit {
		return
	}
	// commit
	args.req.Path = cmn.URLPath(path, cmn.ActCommit)
	results = h.bcastTo(args)
	for res := range results {
		if res.err != nil {
			err = fmt.Errorf("%s: %s: %v(%d)", res.si.Name(), errmsg, res.err, res.status)
			glog.Error(err)
			break
		}
	}
	return err
}

// NOTE: 'u' has only the path and query part, host portion will be set by this method.
func (h *httprunner) bcast(bargs bcastArgs) chan callResult {
	nodeCount := 0
	for _, nodeMap := range bargs.nodes {
		nodeCount += len(nodeMap)
	}
	if nodeCount == 0 {
		ch := make(chan callResult)
		close(ch)
		glog.Warningf("node count zero in [%+v] bcast", bargs.req)
		return ch
	}
	ch := make(chan callResult, nodeCount)
	wg := &sync.WaitGroup{}

	for _, nodeMap := range bargs.nodes {
		for sid, serverInfo := range nodeMap {
			if sid == h.si.DaemonID {
				continue
			}
			wg.Add(1)
			go func(di *cluster.Snode) {
				cargs := callArgs{
					si:      di,
					req:     bargs.req,
					timeout: bargs.timeout,
				}
				cargs.req.Base = di.URL(bargs.network)

				res := h.call(cargs)
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

func (h *httprunner) checkAction(msg cmn.ActionMsg, expectedActions ...string) (err error) {
	found := false
	for _, action := range expectedActions {
		found = found || msg.Action == action
	}
	if !found {
		err = fmt.Errorf("invalid action %q, expected one of: %v", msg.Action, expectedActions)
	}
	return
}

//////////////////////////////
// Common HTTP req handlers //
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
	case cmn.GetWhatBMD:
		body = cmn.MustMarshal(h.bmdowner.get())
	case cmn.GetWhatSmapVote:
		voteInProgress := xaction.Registry.GlobalXactRunning(cmn.ActElection)
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
		msg += " (from " + caller + ")"
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

func (h *httprunner) extractSmap(payload cmn.SimpleKVs, caller string) (newSmap *smapX, msgInt *actionMsgInternal, err error) {
	if _, ok := payload[smaptag]; !ok {
		return
	}
	newSmap, msgInt = &smapX{}, &actionMsgInternal{}
	smapValue := payload[smaptag]
	if err1 := jsoniter.Unmarshal([]byte(smapValue), newSmap); err1 != nil {
		err = fmt.Errorf("failed to unmarshal new Smap, value (%+v, %T), err: %v", smapValue, smapValue, err1)
		return
	}
	if msgValue, ok := payload[smaptag+actiontag]; ok {
		if err1 := jsoniter.Unmarshal([]byte(msgValue), msgInt); err1 != nil {
			err = fmt.Errorf("failed to unmarshal action message, value (%+v, %T), err: %v", msgValue, msgValue, err1)
			return
		}
	}
	var (
		s           string
		hname       = h.si.Name()
		smap        = h.smapowner.get()
		myver       = smap.version()
		isManualReb = msgInt.Action == cmn.ActGlobalReb && msgInt.Value != nil
	)
	if newSmap.version() == myver && !isManualReb {
		newSmap = nil
		return
	}
	if !newSmap.isValid() {
		err = fmt.Errorf("%s: invalid %s - lacking or missing primary", hname, newSmap)
		newSmap = nil
		return
	}
	if msgInt.Action != "" {
		s = ", action " + msgInt.Action
	}
	if err = h.validateOriginSmap(smap, nil, newSmap, caller); err != nil {
		if h.si.IsProxy() {
			cmn.Assert(!smap.isPrimary(h.si))
			// cluster integrity error: making exception for non-primary proxies
			glog.Errorf("%s (non-primary): %v - proceeding to override Smap", hname, err)
			return
		}
		cmn.ExitLogf("%v", err) // otherwise, FATAL
	}

	glog.Infof("%s: receive %s (local %s)%s", hname, newSmap.StringEx(), smap.StringEx(), s)
	sameOrigin, _, eq := smap.Compare(&newSmap.Smap)
	cmn.Assert(sameOrigin)
	if newSmap.version() < myver {
		if !eq {
			err = fmt.Errorf("%s: attempt to downgrade %s to %s", hname, smap.StringEx(), newSmap.StringEx())
			newSmap = nil
			return
		}
		glog.Warningf("%s: %s and %s are otherwise identical", hname, newSmap.StringEx(), smap.StringEx())
		newSmap = nil
	}
	return
}

func (h *httprunner) extractBMD(payload cmn.SimpleKVs) (newBMD *bucketMD, msgInt *actionMsgInternal, err error) {
	hname := h.si.Name()
	if _, ok := payload[bmdtag]; !ok {
		return
	}
	newBMD, msgInt = &bucketMD{}, &actionMsgInternal{}
	bmdValue := payload[bmdtag]
	if err1 := jsoniter.Unmarshal([]byte(bmdValue), newBMD); err1 != nil {
		err = fmt.Errorf("%s: failed to unmarshal new %s, value (%+v, %T), err: %v",
			hname, bmdTermName, bmdValue, bmdValue, err1)
		return
	}
	if msgValue, ok := payload[bmdtag+actiontag]; ok {
		if err1 := jsoniter.Unmarshal([]byte(msgValue), msgInt); err1 != nil {
			err = fmt.Errorf("%s: failed to unmarshal action message, value (%+v, %T), err: %v",
				hname, msgValue, msgValue, err1)
			return
		}
	}
	bmd := h.bmdowner.get()
	if newBMD.version() <= bmd.version() {
		if newBMD.version() < bmd.version() {
			err = fmt.Errorf("%s: attempt to downgrade %s to %s", hname, bmd.StringEx(), newBMD.StringEx())
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
	res = h.registerToURL(url, psi, cmn.DefaultTimeout, query, false)
	if res.err == nil {
		return
	}
	config := cmn.GCO.Get()
	if config.Proxy.DiscoveryURL != "" && config.Proxy.DiscoveryURL != url {
		glog.Errorf("%s: (register => %s: %v - retrying => %s...)", h.si, url, res.err, config.Proxy.DiscoveryURL)
		resAlt := h.registerToURL(config.Proxy.DiscoveryURL, psi, cmn.DefaultTimeout, query, false)
		if resAlt.err == nil {
			res = resAlt
			return
		}
	}
	if config.Proxy.OriginalURL != "" && config.Proxy.OriginalURL != url &&
		config.Proxy.OriginalURL != config.Proxy.DiscoveryURL {
		glog.Errorf("%s: (register => %s: %v - retrying => %s...)", h.si, url, res.err, config.Proxy.OriginalURL)
		resAlt := h.registerToURL(config.Proxy.OriginalURL, psi, cmn.DefaultTimeout, query, false)
		if resAlt.err == nil {
			res = resAlt
			return
		}
	}
	return
}

func (h *httprunner) registerToURL(url string, psi *cluster.Snode, tout time.Duration,
	query url.Values, keepalive bool) (res callResult) {
	req := targetRegMeta{SI: h.si}
	if h.si.IsTarget() && !keepalive {
		req.BMD = h.bmdowner.get()
		req.Smap = h.smapowner.get()
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
		timeout: tout,
	}
	var err error
	for rcount := 0; rcount < 2; rcount++ {
		res = h.call(callArgs)
		if res.err == nil {
			if !keepalive {
				glog.Infof("%s: registered => %s[%s]", h.si, url, path)
			}
			return
		}
		if cmn.IsErrConnectionRefused(res.err) {
			err = errors.New("connection refused")
		} else {
			err = res.err
		}
	}
	glog.Errorf("%s: (%s: %v)", h.si, psi, err)
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

func (h *httprunner) validateOriginBMD(bmd *bucketMD, nsi *cluster.Snode, nbmd *bucketMD, caller string) (err error) {
	if nbmd == nil || nbmd.Version == 0 || bmd.Version == 0 {
		return
	}
	if bmd.Origin == 0 || nbmd.Origin == 0 {
		return
	}
	if bmd.Origin == nbmd.Origin {
		return
	}
	nsiname := caller
	if nsi != nil {
		nsiname = nsi.Name()
	} else if nsiname == "" {
		nsiname = "???"
	}
	hname := h.si.Name()
	// FATAL: cluster integrity error (cie)
	s := fmt.Sprintf("%s: BMDs have different origins: [%s: %s] vs [%s: %s]",
		ciError(40), hname, bmd.StringEx(), nsiname, nbmd.StringEx())
	err = &errPrxBmdOriginDiffer{s}
	return
}

func (h *httprunner) validateOriginSmap(smap *smapX, nsi *cluster.Snode, newSmap *smapX, caller string) (err error) {
	if newSmap == nil || newSmap.Version == 0 {
		return
	}
	if smap.Origin == 0 || newSmap.Origin == 0 {
		return
	}
	if smap.Origin == newSmap.Origin {
		return
	}
	nsiname := caller
	if nsi != nil {
		nsiname = nsi.Name()
	} else if nsiname == "" {
		nsiname = "???"
	}
	hname := h.si.Name()
	// FATAL: cluster integrity error (cie)
	s := fmt.Sprintf("%s: Smaps have different origins: [%s: %s] vs [%s: %s]",
		ciError(50), hname, smap.StringEx(), nsiname, newSmap.StringEx())
	err = &errSmapOriginDiffer{s}
	return
}

func (h *httprunner) bucketPropsToHdr(bck *cluster.Bck, hdr http.Header, config *cmn.Config, cloudVerEnabled string) {
	finalProps := bck.Props.Clone()
	cksumConf := config.Cksum // FIXME: must be props.CksumConf w/o conditions, here and elsewhere
	if finalProps.Cksum.Type == cmn.PropInherit {
		finalProps.Cksum = cksumConf
	}
	cmn.IterFields(finalProps, func(fieldName string, field cmn.IterField) (error, bool) {
		if fieldName == cmn.HeaderBucketVerEnabled {
			// For Cloud buckets, `versioning.enabled` is a combination of local
			// and cloud settings and is true iff versioning is enabled on both sides
			verEnabled := field.Value().(bool)
			if bck.IsAIS() || !verEnabled {
				hdr.Set(cmn.HeaderBucketVerEnabled, strconv.FormatBool(verEnabled))
			} else if enabled, err := cmn.ParseBool(cloudVerEnabled); !enabled && err == nil {
				hdr.Set(cmn.HeaderBucketVerEnabled, strconv.FormatBool(false))
			}
			return nil, false
		}

		hdr.Set(fieldName, fmt.Sprintf("%v", field.Value()))
		return nil, false
	})
}
