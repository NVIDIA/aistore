// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	rdebug "runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/3rdparty/golang/mux"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction/registry"
	"github.com/OneOfOne/xxhash"
	jsoniter "github.com/json-iterator/go"
	"github.com/tinylib/msgp/msgp"
)

const (
	unknownDaemonID = "unknown"
)

type (
	// callResult contains HTTP response.
	callResult struct {
		si      *cluster.Snode
		bytes   []byte      // Raw bytes from response.
		v       interface{} // Unmarshalled value (set only when requested, see: `callArgs.v`).
		header  http.Header
		err     error
		details string
		status  int
	}

	// callArgs contains arguments for a peer-to-peer control plane call.
	callArgs struct {
		req     cmn.ReqArgs
		timeout time.Duration
		si      *cluster.Snode
		v       interface{} // Value that needs to be unmarshalled.
	}

	// bcastArgs contains arguments for an intra-cluster broadcast call.
	bcastArgs struct {
		req               cmn.ReqArgs        // http args
		network           string             // one of the cmn.KnownNetworks
		timeout           time.Duration      // timeout
		fv                func() interface{} // optional; returns value to be unmarshalled (see `callArgs.v`)
		nodes             []cluster.NodeMap  // broadcast destinations
		skipNodes         cmn.StringSet      // destination IDs to skip
		smap              *smapX             // Smap to use
		to                int                // enumerated alternative to nodes (above)
		nodeCount         int                // greater or equal destination count
		ignoreMaintenance bool               // do not skip nodes under maintenance
	}

	networkHandler struct {
		r   string           // resource
		h   http.HandlerFunc // handler
		net []string
	}

	// SmapVoteMsg contains the cluster map and a bool representing whether or not a vote is currently happening.
	// NOTE: exported for integration testing
	SmapVoteMsg struct {
		Smap           *smapX    `json:"smap"`
		BucketMD       *bucketMD `json:"bucketmd"`
		VoteInProgress bool      `json:"vote_in_progress"`
	}

	// two pieces of metadata a self-registering (joining) target wants to know right away
	nodeRegMeta struct {
		Smap *smapX         `json:"smap"`
		BMD  *bucketMD      `json:"bmd"`
		SI   *cluster.Snode `json:"si"`
	}

	// aisMsg is an extended ActionMsg with extra information for node <=> node control plane communications
	aisMsg struct {
		cmn.ActionMsg
		BMDVersion  int64  `json:"bmdversion,string"`
		SmapVersion int64  `json:"smapversion,string"`
		RMDVersion  int64  `json:"rmdversion,string"`
		UUID        string `json:"uuid"` // cluster-wide ID of this action (operation, transaction)
	}

	// TODO: add usage
	clusterInfo struct {
		BMD struct {
			Version int64  `json:"version,string"`
			UUID    string `json:"uuid"`
		} `json:"bmd"`
		Smap struct {
			Version    int64  `json:"version,string"`
			UUID       string `json:"uuid"`
			PrimaryURL string `json:"primary_url"`
		} `json:"smap"`
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
		owner              struct {
			smap *smapOwner
			bmd  bmdOwner
			rmd  *rmdOwner
		}
		statsT  stats.Tracker
		startup struct {
			cluster atomic.Bool // determines if the cluster has started up
			node    struct {
				time atomic.Time // determines time when the node started up
			}
		}
	}

	glogWriter struct{}

	// BMD & its uuid
	errTgtBmdUUIDDiffer struct{ detail string }
	errPrxBmdUUIDDiffer struct{ detail string }
	errBmdUUIDSplit     struct{ detail string }
	// ditto Smap
	errSmapUUIDDiffer struct{ detail string }
	// node not found
	errNodeNotFound struct {
		msg  string
		id   string
		si   *cluster.Snode
		smap *smapX
	}
)

///////////////////
// BMD uuid errs //
///////////////////

var errNoBMD = errors.New("no bucket metadata")

func (e *errTgtBmdUUIDDiffer) Error() string { return e.detail }
func (e *errBmdUUIDSplit) Error() string     { return e.detail }
func (e *errPrxBmdUUIDDiffer) Error() string { return e.detail }
func (e *errSmapUUIDDiffer) Error() string   { return e.detail }
func (e *errNodeNotFound) Error() string {
	return fmt.Sprintf("%s: %s node %s (not present in the %s)", e.si, e.msg, e.id, e.smap)
}

////////////////
// glogWriter //
////////////////

func (r *glogWriter) Write(p []byte) (int, error) {
	n := len(p)
	s := string(p[:n])
	glog.Errorln(s)

	stacktrace := rdebug.Stack()
	n1 := len(stacktrace)
	s1 := string(stacktrace[:n1])
	glog.Errorln(s1)
	return n, nil
}

/////////////////
// clusterInfo //
/////////////////
func (cii *clusterInfo) String() string { return fmt.Sprintf("%+v", *cii) }

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
	server.s = &http.Server{
		Addr:     addr,
		Handler:  httpHandler,
		ErrorLog: logger,
	}
	if server.sndRcvBufSize > 0 && !config.Net.HTTP.UseHTTPS {
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

	ctx, cancel := context.WithTimeout(context.Background(), cmn.GCO.Get().Timeout.MaxHostBusy/2)
	if err := server.s.Shutdown(ctx); err != nil {
		glog.Infof("Stopped server, err: %v", err)
	}
	cancel()
}

////////////////
// httprunner //
////////////////

func (h *httprunner) Snode() *cluster.Snode      { return h.si }
func (h *httprunner) Bowner() cluster.Bowner     { return h.owner.bmd }
func (h *httprunner) Sowner() cluster.Sowner     { return h.owner.smap }
func (h *httprunner) ClusterStarted() bool       { return h.startup.cluster.Load() }
func (h *httprunner) NodeStarted() bool          { return !h.startup.node.time.Load().IsZero() }
func (h *httprunner) NodeStartedTime() time.Time { return h.startup.node.time.Load() }
func (h *httprunner) markClusterStarted()        { h.startup.cluster.Store(true) }
func (h *httprunner) markNodeStarted()           { h.startup.node.time.Store(time.Now()) }
func (h *httprunner) Client() *http.Client       { return h.httpclientGetPut }

func (h *httprunner) registerNetworkHandlers(networkHandlers []networkHandler) {
	var (
		path   string
		config = cmn.GCO.Get()
	)
	for _, nh := range networkHandlers {
		if nh.r[0] == '/' { // Check if it's an absolute path.
			path = nh.r
		} else {
			path = cmn.JoinWords(cmn.Version, nh.r)
		}
		if cmn.StringInSlice(cmn.NetworkPublic, nh.net) {
			h.registerPublicNetHandler(path, nh.h)

			if config.Net.UseIntraControl && cmn.StringInSlice(cmn.NetworkIntraControl, nh.net) {
				h.registerIntraControlNetHandler(path, nh.h)
			}
			if config.Net.UseIntraData && cmn.StringInSlice(cmn.NetworkIntraData, nh.net) {
				h.registerIntraDataNetHandler(path, nh.h)
			}
		} else if cmn.StringInSlice(cmn.NetworkIntraControl, nh.net) {
			h.registerIntraControlNetHandler(path, nh.h)

			if config.Net.UseIntraData && cmn.StringInSlice(cmn.NetworkIntraData, nh.net) {
				h.registerIntraDataNetHandler(path, nh.h)
			}
		} else if cmn.StringInSlice(cmn.NetworkIntraData, nh.net) {
			h.registerIntraDataNetHandler(path, nh.h)
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
	h.statsT = s
	h.httpclient = cmn.NewClient(cmn.TransportArgs{
		Timeout:    config.Client.Timeout,
		UseHTTPS:   config.Net.HTTP.UseHTTPS,
		SkipVerify: config.Net.HTTP.SkipVerify,
	})
	h.httpclientGetPut = cmn.NewClient(cmn.TransportArgs{
		Timeout:         config.Client.TimeoutLong,
		WriteBufferSize: config.Net.HTTP.WriteBufferSize,
		ReadBufferSize:  config.Net.HTTP.ReadBufferSize,
		UseHTTPS:        config.Net.HTTP.UseHTTPS,
		SkipVerify:      config.Net.HTTP.SkipVerify,
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

	h.owner.smap = newSmapOwner()
	h.owner.rmd = newRMDOwner()
	h.owner.rmd.load()
}

// initSI initializes this cluster.Snode
func (h *httprunner) initSI(daemonType string) {
	var (
		s             string
		config        = cmn.GCO.Get()
		port          = config.Net.L4.Port
		addrList, err = getLocalIPv4List()
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

	daemonID := os.Getenv("AIS_DAEMON_ID")
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
	cmn.InitShortID(h.si.Digest())
}

func mustDiffer(ip1 net.IP, port1 int, use1 bool, ip2 net.IP, port2 int, use2 bool, tag string) {
	if !use1 || !use2 {
		return
	}
	if string(ip1) == string(ip2) && port1 == port2 {
		glog.Fatalf("%s: cannot use the same IP:port (%s:%d) for two networks", tag, string(ip1), port1)
	}
}

// is called at startup;
// given loaded Smap, checks whether Snode is present and whether network info has changed
// - the latter for, possibly, legitimate reasons (K8s, etc.)
func (h *httprunner) checkPresenceNetChange(smap *smapX) error {
	var (
		jsonCompat = jsoniter.ConfigCompatibleWithStandardLibrary
		snode      *cluster.Snode
		changed    bool
	)
	if h.si.IsTarget() {
		snode = smap.GetTarget(h.si.ID())
	} else {
		cmn.Assert(h.si.IsProxy())
		snode = smap.GetProxy(h.si.ID())
	}
	if snode == nil {
		return fmt.Errorf("%s: not present in the loaded Smap", h.si)
	}
	if h.si.PublicNet.NodeIPAddr != snode.PublicNet.NodeIPAddr {
		glog.Errorf("Warning: %s: PUBLIC (user) IPv4 changed: previous %s, current %s", h.si,
			snode.PublicNet.NodeIPAddr, h.si.PublicNet.NodeIPAddr)
		changed = true
	}
	if h.si.IntraControlNet.NodeIPAddr != snode.IntraControlNet.NodeIPAddr {
		glog.Errorf("Warning: %s: INTRA-CONTROL IPv4 changed: previous %s, current %s", h.si,
			snode.IntraControlNet.NodeIPAddr, h.si.IntraControlNet.NodeIPAddr)
		changed = true
	}
	if changed {
		return nil
	}
	if h.si.PublicNet != snode.PublicNet ||
		h.si.IntraControlNet != snode.IntraControlNet ||
		h.si.IntraDataNet != snode.IntraDataNet {
		prev, _ := jsonCompat.MarshalIndent(snode, "", " ")
		curr, _ := jsonCompat.MarshalIndent(h.si, "", " ")
		glog.Errorf("Warning: %s detected a change in network config:\n%scurrently:\n%s\n- proceeding anyway...",
			h.si, string(prev), string(curr))
	}
	return nil
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
	glog.Infof("Stopping %s, err: %v", h.GetRunName(), err)

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
func (h *httprunner) call(args callArgs) (res callResult) {
	var (
		req    *http.Request
		resp   *http.Response
		client *http.Client
		sid    = unknownDaemonID
	)

	if args.si != nil {
		sid = args.si.ID()
		res.si = args.si
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
		req, res.err = args.req.Req()
		if res.err != nil {
			break
		}

		client = h.httpclient
	case cmn.LongTimeout:
		req, res.err = args.req.Req()
		if res.err != nil {
			break
		}

		client = h.httpclientGetPut
	default:
		var cancel context.CancelFunc
		req, _, cancel, res.err = args.req.ReqWithTimeout(args.timeout)
		if res.err != nil {
			break
		}
		defer cancel() // timeout => context.deadlineExceededError

		if args.timeout > h.httpclient.Timeout {
			client = h.httpclientGetPut
		} else {
			client = h.httpclient
		}
	}

	if res.err != nil {
		res.details = fmt.Sprintf("unexpected failure to create HTTP request %s %s, err: %v",
			args.req.Method, args.req.URL(), res.err)
		return
	}

	req.Header.Set(cmn.HeaderCallerID, h.si.ID())
	req.Header.Set(cmn.HeaderCallerName, h.si.Name())
	if smap := h.owner.smap.get(); smap.isValid() {
		req.Header.Set(cmn.HeaderCallerSmapVersion, strconv.FormatInt(smap.version(), 10))
	}

	resp, res.err = client.Do(req)
	if res.err != nil {
		res.details = fmt.Sprintf("Failed to HTTP-call %s (%s %s): err %v",
			sid, args.req.Method, args.req.URL(), res.err)
		return
	}
	defer resp.Body.Close()
	res.status = resp.StatusCode
	res.header = resp.Header

	// err == nil && bad status: resp.Body contains the error message
	if res.status >= http.StatusBadRequest {
		var b bytes.Buffer
		b.ReadFrom(resp.Body)
		res.err = errors.New(b.String())
		res.details = res.err.Error()
		return
	}

	if args.v != nil {
		if v, ok := args.v.(msgp.Decodable); ok {
			res.err = v.DecodeMsg(msgp.NewReaderSize(resp.Body, 10*cmn.KiB))
		} else {
			res.err = jsoniter.NewDecoder(resp.Body).Decode(args.v)
		}
		if res.err != nil {
			res.details = fmt.Sprintf(
				"failed to unmarshal response from %s (%s %s), read response err: %v",
				sid, args.req.Method, args.req.URL(), res.err,
			)
			return
		}
		res.v = args.v
	} else {
		res.bytes, res.err = ioutil.ReadAll(resp.Body)
		if res.err != nil {
			res.details = fmt.Sprintf(
				"failed to HTTP-call %s (%s %s), read response err: %v",
				sid, args.req.Method, args.req.URL(), res.err,
			)
			return
		}
	}

	if sid != unknownDaemonID {
		h.keepalive.heardFrom(sid, false /* reset */)
	}
	return
}

//
// intra-cluster IPC, control plane: notify another node
//

func (h *httprunner) notify(snodes cluster.NodeMap, msgBody []byte, kind string) {
	path := cmn.JoinWords(cmn.Version, cmn.Notifs, kind)
	args := bcastArgs{
		req:       cmn.ReqArgs{Method: http.MethodPost, Path: path, Body: msgBody},
		network:   cmn.NetworkIntraControl,
		timeout:   cmn.DefaultTimeout,
		nodes:     []cluster.NodeMap{snodes},
		nodeCount: len(snodes),
	}
	h.bcastToNodesAsync(&args)
}

func (h *httprunner) callerNotifyFin(n cluster.Notif, err error) {
	h.callerNotify(n, err, cmn.Finished)
}

func (h *httprunner) callerNotifyProgress(n cluster.Notif) {
	h.callerNotify(n, nil, cmn.Progress)
}

func (h *httprunner) callerNotify(n cluster.Notif, err error, kind string) {
	cmn.Assert(kind == cmn.Progress || kind == cmn.Finished)
	msg := n.ToNotifMsg()
	if err != nil {
		msg.ErrMsg = err.Error()
	}
	msg.NodeID = h.si.ID()
	nodes := h.notifDst(n)
	h.notify(nodes, cmn.MustMarshal(&msg), kind)
}

// TODO: optimize avoid allocating a new NodeMap
func (h *httprunner) notifDst(notif cluster.Notif) cluster.NodeMap {
	var (
		smap  = h.owner.smap.get()
		nodes = make(cluster.NodeMap)
	)
	for _, dst := range notif.Subscribers() {
		if dst == equalIC {
			for si := range smap.IC {
				if si != h.si.ID() {
					nodes.Add(smap.GetProxy(si))
				}
			}
			debug.Assert(len(notif.Subscribers()) == 1)
			break
		} else if si := smap.GetNode(dst); si != nil {
			nodes.Add(si)
		} else {
			err := &errNodeNotFound{"failed to notify", dst, h.si, smap}
			glog.Error(err)
		}
	}
	return nodes
}

///////////////
// broadcast //
///////////////

// _callGroup internal helper that transforms raw message parts into `bcastToGroup`.
func (h *httprunner) _callGroup(method, path string, body []byte, to int, query ...url.Values) chan callResult {
	cmn.Assert(method != "" && path != "")
	q := url.Values{}
	if len(query) > 0 {
		q = query[0]
	}
	return h.bcastToGroup(bcastArgs{
		req: cmn.ReqArgs{
			Method: method,
			Path:   path,
			Body:   body,
			Query:  q,
		},
		timeout: cmn.GCO.Get().Timeout.CplaneOperation,
		to:      to,
	})
}

// callTargets neat one-liner method for sending a message to all targets.
func (h *httprunner) callTargets(method, path string, body []byte, query ...url.Values) chan callResult {
	return h._callGroup(method, path, body, cluster.Targets, query...)
}

// callAll neat one-liner method for sending a message to all nodes.
func (h *httprunner) callAll(method, path string, body []byte, query ...url.Values) chan callResult {
	return h._callGroup(method, path, body, cluster.AllNodes, query...)
}

// bcastToGroup broadcasts a message to specific group of nodes (targets, proxies, all).
func (h *httprunner) bcastToGroup(args bcastArgs) chan callResult {
	if args.smap == nil {
		args.smap = h.owner.smap.get()
	}
	if args.network == "" {
		args.network = cmn.NetworkIntraControl
	}
	if args.timeout == 0 {
		args.timeout = cmn.GCO.Get().Timeout.CplaneOperation
		cmn.Assert(args.timeout != 0)
	}

	switch args.to {
	case cluster.Targets:
		args.nodes = []cluster.NodeMap{args.smap.Tmap}
		args.nodeCount = len(args.smap.Tmap)
	case cluster.Proxies:
		args.nodes = []cluster.NodeMap{args.smap.Pmap}
		args.nodeCount = len(args.smap.Pmap)
	case cluster.AllNodes:
		args.nodes = []cluster.NodeMap{args.smap.Pmap, args.smap.Tmap}
		args.nodeCount = len(args.smap.Pmap) + len(args.smap.Tmap)
	default:
		cmn.Assert(false)
	}
	if !cmn.NetworkIsKnown(args.network) {
		cmn.Assertf(false, "unknown network %q", args.network)
	}
	return h.bcastToNodes(&args)
}

// bcastToNodes broadcasts a message to the specified destinations (`bargs.nodes`).
// It then waits and collects all the responses. Note that when `bargs.req.BodyR`
// is used it is expected to implement `cmn.ReadOpenCloser`.
func (h *httprunner) bcastToNodes(bargs *bcastArgs) chan callResult {
	var (
		wg  = &sync.WaitGroup{}
		ch  = make(chan callResult, bargs.nodeCount)
		cnt int
	)
	for _, nodeMap := range bargs.nodes {
		for sid, si := range nodeMap {
			if sid == h.si.ID() || bargs.skipNodes.Contains(sid) {
				continue
			}
			if !bargs.ignoreMaintenance && si.InMaintenance() {
				continue
			}
			wg.Add(1)
			cnt++
			go h.unicastToNode(si, bargs, wg, ch)
		}
	}
	wg.Wait()
	close(ch)
	if cnt == 0 {
		glog.Warningf("%s: node count is zero in [%s %s] broadcast", h.si, bargs.req.Method, bargs.req.Path)
	}
	return ch
}

func (h *httprunner) unicastToNode(si *cluster.Snode, bargs *bcastArgs, wg *sync.WaitGroup, ch chan callResult) {
	cargs := callArgs{si: si, req: bargs.req, timeout: bargs.timeout}
	cargs.req.Base = si.URL(bargs.network)
	if bargs.req.BodyR != nil {
		cargs.req.BodyR, _ = bargs.req.BodyR.(cmn.ReadOpenCloser).Open()
	}
	if bargs.fv != nil {
		cargs.v = bargs.fv()
	}
	ch <- h.call(cargs)
	wg.Done()
}

// asynchronous variation of the `bcastToNodes` (above)
func (h *httprunner) bcastToNodesAsync(bargs *bcastArgs) {
	var cnt int
	for _, nodeMap := range bargs.nodes {
		for sid, si := range nodeMap {
			if sid == h.si.ID() || bargs.skipNodes.Contains(sid) {
				continue
			}
			cnt++
			go func(si *cluster.Snode) {
				cargs := callArgs{si: si, req: bargs.req, timeout: bargs.timeout}
				cargs.req.Base = si.URL(bargs.network)
				_ = h.call(cargs)
			}(si)
		}
		if cnt == 0 {
			glog.Warningf("%s: node count is zero in [%s %s] broadcast",
				h.si, bargs.req.Method, bargs.req.Path)
		}
	}
}

func (h *httprunner) bcastToIC(msg *aisMsg) chan callResult {
	var (
		smap  = h.owner.smap.get()
		wg    = &sync.WaitGroup{}
		ch    = make(chan callResult, len(smap.IC))
		bargs = bcastArgs{
			req: cmn.ReqArgs{
				Method: http.MethodPost,
				Path:   cmn.JoinWords(cmn.Version, cmn.IC),
				Body:   cmn.MustMarshal(msg),
			},
			network: cmn.NetworkIntraControl,
			timeout: cmn.GCO.Get().Timeout.MaxKeepalive,
		}
		cnt int
	)
	for pid := range smap.IC {
		if pid == h.si.ID() {
			continue
		}
		wg.Add(1)
		cnt++
		go h.unicastToNode(smap.GetProxy(pid), &bargs, wg, ch)
	}
	wg.Wait()
	close(ch)
	if cnt == 0 {
		glog.Errorf("%s: node count is zero in broadcast-to-IC, %+v(%d)", h.si, smap.IC, len(smap.IC))
	}
	return ch
}

//////////////////////////////////
// HTTP request parsing helpers //
//////////////////////////////////

// remove validated fields and return the resulting slice
func (h *httprunner) checkRESTItems(w http.ResponseWriter, r *http.Request, itemsAfter int,
	splitAfter bool, items ...string) ([]string, error) {
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

func (h *httprunner) writeMsgPack(w http.ResponseWriter, r *http.Request, v interface{}, tag string) (ok bool) {
	var (
		err error
		mw  = msgp.NewWriterSize(w, 10*cmn.KiB)
	)
	w.Header().Set(cmn.HeaderContentType, cmn.ContentMsgPack)
	if err = v.(msgp.Encodable).EncodeMsg(mw); err == nil {
		err = mw.Flush()
	}
	if err != nil {
		h.writeErrorStatus(w, r, tag, err)
		return
	}
	return true
}

func (h *httprunner) writeJSON(w http.ResponseWriter, r *http.Request, v interface{}, tag string) (ok bool) {
	_, isByteArray := v.([]byte)
	cmn.Assert(!isByteArray)

	w.Header().Set(cmn.HeaderContentType, cmn.ContentJSON)
	if err := jsoniter.NewEncoder(w).Encode(v); err != nil {
		h.writeErrorStatus(w, r, tag, err)
		return
	}
	return true
}

// NOTE: must be the last error-generating-and-handling call in the http handler
//       writes http body and header
//       calls invalmsghdlr() on err
func (h *httprunner) writeBytes(w http.ResponseWriter, r *http.Request, bytes []byte, tag string) (ok bool) {
	if _, err := w.Write(bytes); err != nil {
		h.writeErrorStatus(w, r, tag, err)
		return
	}
	return true
}

func (h *httprunner) writeJSONBytes(w http.ResponseWriter, r *http.Request, bytes []byte, tag string) (ok bool) {
	w.Header().Set(cmn.HeaderContentType, cmn.ContentJSON)
	return h.writeBytes(w, r, bytes, tag)
}

func (h *httprunner) writeErrorStatus(w http.ResponseWriter, r *http.Request, tag string, err error) {
	if isSyscallWriteError(err) {
		// Apparently, cannot write to this w: broken-pipe and similar
		glog.Errorf("isSyscallWriteError: %v", err)
		s := "isSyscallWriteError: " + r.Method + " " + r.URL.Path
		if _, file, line, ok2 := runtime.Caller(1); ok2 {
			f := filepath.Base(file)
			s += fmt.Sprintf("(%s, #%d)", f, line)
		}
		glog.Errorln(s)
		h.statsT.AddErrorHTTP(r.Method, 1)
		return
	}
	msg := fmt.Sprintf("%s: failed to write bytes, err: %v", tag, err)
	if _, file, line, ok := runtime.Caller(1); ok {
		f := filepath.Base(file)
		msg += fmt.Sprintf("(%s, #%d)", f, line)
	}
	h.invalmsghdlr(w, r, msg)
}

func (h *httprunner) parseNCopies(value interface{}) (copies int64, err error) {
	switch v := value.(type) {
	case string:
		copies, err = strconv.ParseInt(v, 10, 64)
	case float64:
		copies = int64(v)
	default:
		err = fmt.Errorf("failed to parse 'copies' (%v, %T) - unexpected type", value, value)
	}
	return
}

func (h *httprunner) checkAction(msg *cmn.ActionMsg, expectedActions ...string) (err error) {
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
		body interface{}
		what = r.URL.Query().Get(cmn.URLParamWhat)
	)
	switch what {
	case cmn.GetWhatConfig:
		body = cmn.GCO.Get()
	case cmn.GetWhatSmap:
		body = h.owner.smap.get()
	case cmn.GetWhatBMD:
		body = h.owner.bmd.get()
	case cmn.GetWhatSmapVote:
		xact := registry.Registry.GetXactRunning(cmn.ActElection)
		msg := SmapVoteMsg{VoteInProgress: xact != nil, Smap: h.owner.smap.get(), BucketMD: h.owner.bmd.get()}
		body = msg
	case cmn.GetWhatSnode:
		body = h.si
	default:
		s := fmt.Sprintf("Invalid GET /daemon request: unrecognized what=%s", what)
		h.invalmsghdlr(w, r, s)
		return
	}
	h.writeJSON(w, r, body, "httpdaeget-"+what)
}

////////////////////////////////////////////
// HTTP err + spec message + code + stats //
////////////////////////////////////////////

func (h *httprunner) invalmsghdlr(w http.ResponseWriter, r *http.Request, msg string, errCode ...int) {
	if caller := r.Header.Get(cmn.HeaderCallerName); caller != "" {
		msg += " (from " + caller + ")"
	}
	cmn.InvalidHandlerDetailed(w, r, msg, errCode...)
	h.statsT.AddErrorHTTP(r.Method, 1)
}

func (h *httprunner) invalmsghdlrsilent(w http.ResponseWriter, r *http.Request, msg string, errCode ...int) {
	cmn.InvalidHandlerDetailedNoLog(w, r, msg, errCode...)
}

func (h *httprunner) invalmsghdlrstatusf(w http.ResponseWriter, r *http.Request, errCode int,
	format string, a ...interface{}) {
	h.invalmsghdlr(w, r, fmt.Sprintf(format, a...), errCode)
}

func (h *httprunner) invalmsghdlrf(w http.ResponseWriter, r *http.Request, format string, a ...interface{}) {
	h.invalmsghdlr(w, r, fmt.Sprintf(format, a...))
}

///////////////////
// health client //
///////////////////

func (h *httprunner) Health(si *cluster.Snode, timeout time.Duration, query url.Values) ([]byte, error, int) {
	var (
		path = cmn.JoinWords(cmn.Version, cmn.Health)
		url  = si.URL(cmn.NetworkIntraControl)
		args = callArgs{
			si:      si,
			req:     cmn.ReqArgs{Method: http.MethodGet, Base: url, Path: path, Query: query},
			timeout: timeout,
		}
	)
	res := h.call(args)
	return res.bytes, res.err, res.status
}

// uses provided Smap and an extended version of the Health(clusterInfo) internal API
// to discover a better Smap (ie., more current), if exists
//
// TODO: utilize for target startup and, for the targets, validate and return maxVerBMD
//
func (h *httprunner) bcastHealth(smap *smapX) (smapMaxVer int64, primaryURL string) {
	var (
		wg      = &sync.WaitGroup{}
		query   = url.Values{cmn.URLParamClusterInfo: []string{"true"}}
		timeout = cmn.GCO.Get().Timeout.CplaneOperation
		mu      = &sync.Mutex{}
	)
	smapMaxVer = smap.version()
	for sid, si := range smap.Pmap {
		if sid == h.si.ID() {
			continue
		}
		wg.Add(1)
		go func(si *cluster.Snode) {
			var (
				ver int64
				url string
			)
			defer wg.Done()
			body, err, _ := h.Health(si, timeout, query)
			if err != nil {
				return
			}
			if ver, url = ciiToSmap(smap, body, h.si, si); ver == 0 {
				return
			}
			mu.Lock()
			if smapMaxVer < ver {
				smapMaxVer = ver
				primaryURL = url
			}
			mu.Unlock()
		}(si)
	}
	wg.Wait()
	return
}

func ciiToSmap(smap *smapX, body []byte, self, si *cluster.Snode) (smapVersion int64, primaryURL string) {
	var cii clusterInfo
	if err := jsoniter.Unmarshal(body, &cii); err != nil {
		glog.Errorf("%s: failed to unmarshal clusterInfo, err: %v", self, err)
		return
	}
	if smap.version() < cii.Smap.Version {
		if smap.UUID != cii.Smap.UUID {
			glog.Errorf("%s: Smap have different UUIDs: %s and %s from %s",
				self, smap.UUID, cii.Smap.UUID, si)
			return
		}
		smapVersion = cii.Smap.Version
		primaryURL = cii.Smap.PrimaryURL
	}
	return
}

//////////////////////////
// metasync Rx handlers //
//////////////////////////

func (h *httprunner) extractSmap(payload msPayload, caller string) (newSmap *smapX, msg *aisMsg, err error) {
	if _, ok := payload[revsSmapTag]; !ok {
		return
	}
	newSmap, msg = &smapX{}, &aisMsg{}
	smapValue := payload[revsSmapTag]
	if err1 := jsoniter.Unmarshal(smapValue, newSmap); err1 != nil {
		err = fmt.Errorf("failed to unmarshal new Smap, value (%s), err: %v", smapValue, err1)
		return
	}
	if msgValue, ok := payload[revsSmapTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err = fmt.Errorf("failed to unmarshal action message, value (%s), err: %v", msgValue, err1)
			return
		}
	}
	var (
		s           string
		smap        = h.owner.smap.get()
		curVer      = smap.version()
		isManualReb = msg.Action == cmn.ActRebalance && msg.Value != nil
	)
	if newSmap.version() == curVer && !isManualReb {
		newSmap = nil
		return
	}
	if !newSmap.isValid() {
		err = fmt.Errorf("%s: invalid %s - lacking or missing primary", h.si, newSmap)
		return
	}
	if !newSmap.isPresent(h.si) {
		err = fmt.Errorf("%s: invalid %s - not finding ourselves in smap", h.si, newSmap)
		return
	}
	if msg.Action != "" {
		s = ", action " + msg.Action
	}
	if err = smap.validateUUID(newSmap, h.si, nil, caller); err != nil {
		if h.si.IsProxy() {
			cmn.Assert(!smap.isPrimary(h.si))
			// cluster integrity error: making exception for non-primary proxies
			glog.Errorf("%s (non-primary): %v - proceeding to override Smap", h.si, err)
			return
		}
		cmn.ExitLogf("%v", err) // otherwise, FATAL
	}

	glog.Infof("%s: receive %s (local %s)%s", h.si, newSmap.StringEx(), smap.StringEx(), s)
	_, sameOrigin, _, eq := smap.Compare(&newSmap.Smap)
	cmn.Assert(sameOrigin)
	if newSmap.version() < curVer {
		if !eq {
			err = fmt.Errorf("%s: attempt to downgrade %s to %s", h.si, smap.StringEx(), newSmap.StringEx())
			return
		}
		glog.Warningf("%s: %s and %s are otherwise identical", h.si, newSmap.StringEx(), smap.StringEx())
		newSmap = nil
	}
	return
}

func (h *httprunner) extractRMD(payload msPayload) (newRMD *rebMD, msg *aisMsg, err error) {
	if _, ok := payload[revsRMDTag]; !ok {
		return
	}
	newRMD, msg = &rebMD{}, &aisMsg{}
	rmdValue := payload[revsRMDTag]
	if err1 := jsoniter.Unmarshal(rmdValue, newRMD); err1 != nil {
		err = fmt.Errorf("%s: failed to unmarshal new RMD, value (%+v, %T), err: %v",
			h.si, rmdValue, rmdValue, err1)
		return
	}
	if msgValue, ok := payload[revsRMDTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err = fmt.Errorf("%s: failed to unmarshal action message, value (%s), err: %v",
				h.si, msgValue, err1)
			return
		}
	}
	rmd := h.owner.rmd.get()
	if newRMD.version() <= rmd.version() {
		if newRMD.version() < rmd.version() {
			err = fmt.Errorf("%s: attempt to downgrade %s to %s", h.si, rmd.String(), newRMD.String())
		}
		newRMD = nil
	}
	return
}

func (h *httprunner) extractBMD(payload msPayload) (newBMD *bucketMD, msg *aisMsg, err error) {
	if _, ok := payload[revsBMDTag]; !ok {
		return
	}
	newBMD, msg = &bucketMD{}, &aisMsg{}
	bmdValue := payload[revsBMDTag]
	if err1 := jsoniter.Unmarshal(bmdValue, newBMD); err1 != nil {
		err = fmt.Errorf("%s: failed to unmarshal new %s, value (%+v, %T), err: %v",
			h.si, bmdTermName, bmdValue, bmdValue, err1)
		return
	}
	if msgValue, ok := payload[revsBMDTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err = fmt.Errorf("%s: failed to unmarshal action message, value (%s), err: %v",
				h.si, msgValue, err1)
			return
		}
	}
	bmd := h.owner.bmd.get()
	if newBMD.version() <= bmd.version() {
		if newBMD.version() < bmd.version() {
			err = fmt.Errorf("%s: attempt to downgrade %s to %s", h.si, bmd.StringEx(), newBMD.StringEx())
		}
		newBMD = nil
	}
	return
}

func (h *httprunner) extractRevokedTokenList(payload msPayload) (*TokenList, error) {
	var (
		msg       aisMsg
		bytes, ok = payload[revsTokenTag]
	)
	if !ok {
		return nil, nil
	}
	if msgValue, ok := payload[revsTokenTag+revsActionTag]; ok {
		if err := jsoniter.Unmarshal(msgValue, &msg); err != nil {
			err := fmt.Errorf(
				"failed to unmarshal action message, value (%s), err: %v",
				msgValue, err)
			return nil, err
		}
	}

	tokenList := &TokenList{}
	if err := jsoniter.Unmarshal(bytes, tokenList); err != nil {
		return nil, fmt.Errorf(
			"failed to unmarshal blocked token list, value (%+v, %T), err: %v",
			bytes, bytes, err)
	}

	s := ""
	if msg.Action != "" {
		s = ", action " + msg.Action
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
//   (see /deploy/dev/local/aisnode_config.sh)
//
// - if that one fails, the new node goes ahead and tries the alternatives:
// 	- config.Proxy.DiscoveryURL ("discovery_url")
// 	- config.Proxy.OriginalURL ("original_url")
// - but only if those are defined and different from the previously tried.
//
// ================================== Background =========================================
func (h *httprunner) join(query url.Values, primaryURLs ...string) (res callResult) {
	var (
		pool   = make([]string, 0, 4)
		config = cmn.GCO.Get()
		f      = func(url string) {
			if url == "" {
				return
			}
			for _, u := range pool {
				if u == url {
					return
				}
			}
			pool = append(pool, url)
		}
		sleep = 125 * time.Millisecond
	)
	// make a pool of unique "join" URLs
	for _, u := range primaryURLs {
		f(u)
	}
	url, psi := h.getPrimaryURLAndSI()
	f(url)
	// NOTE: the url above is either config or the primary's IntraControlNet.DirectURL;
	//       add its public URL as "more static" in various virtualized environments
	if psi != nil {
		f(psi.PublicNet.DirectURL)
	}
	f(config.Proxy.DiscoveryURL)
	f(config.Proxy.OriginalURL)

	// up to 2 attempts
	for i := 0; i < 2; i++ {
		for _, url := range pool {
			res = h.registerToURL(url, nil, cmn.DefaultTimeout, query, false)
			if res.err == nil {
				glog.Infof("%s: joined cluster via %s", h.si, url)
				return
			}
			extra := rand.Int63n(int64(sleep))
			time.Sleep(sleep + time.Duration(extra))
		}
	}
	return
}

func (h *httprunner) registerToURL(url string, psi *cluster.Snode, tout time.Duration,
	query url.Values, keepalive bool) (res callResult) {
	regReq := nodeRegMeta{SI: h.si}
	if h.si.IsTarget() && !keepalive {
		regReq.BMD = h.owner.bmd.get()
		regReq.Smap = h.owner.smap.get()
	}
	info := cmn.MustMarshal(regReq)
	path := cmn.JoinWords(cmn.Version, cmn.Cluster)
	if keepalive {
		path += cmn.JoinWords(cmn.Keepalive)
	} else {
		path += cmn.JoinWords(cmn.AutoRegister)
	}
	callArgs := callArgs{
		si:      psi,
		req:     cmn.ReqArgs{Method: http.MethodPost, Base: url, Path: path, Query: query, Body: info},
		timeout: tout,
	}
	return h.call(callArgs)
}

func (h *httprunner) sendKeepalive(timeout time.Duration) (status int, err error) {
	primaryURL, psi := h.getPrimaryURLAndSI()
	res := h.registerToURL(primaryURL, psi, timeout, nil, true)
	if res.err != nil {
		if strings.Contains(res.err.Error(), ciePrefix) {
			cmn.ExitLogf("%v", res.err) // FATAL: cluster integrity error (cie)
		}
		return res.status, res.err
	}
	debug.Assert(len(res.bytes) == 0)
	return
}

// NOTE: default retry policy for ops like join-cluster et al.
func (h *httprunner) withRetry(call func(arg ...string) (int, error), action string, backoff bool, arg ...string) (err error) {
	var (
		sleep       time.Duration
		config      = cmn.GCO.Get()
		retryPolicy = struct {
			sleep   time.Duration
			anyerr  int
			refused int
			backoff bool
		}{backoff: backoff}
	)
	if backoff {
		retryPolicy.sleep = config.Timeout.CplaneOperation / 2
		retryPolicy.anyerr = 2
		retryPolicy.refused = 4
	} else {
		retryPolicy.sleep = config.Timeout.CplaneOperation / 4
		retryPolicy.anyerr = 1
		retryPolicy.refused = 2
	}
	sleep = retryPolicy.sleep
	for i, j, k := 0, 0, 1; i < retryPolicy.anyerr && j < retryPolicy.refused; k++ {
		var status int
		if status, err = call(arg...); err == nil {
			return
		}
		if strings.Contains(err.Error(), ciePrefix) {
			glog.Error(err)
			return
		}
		glog.Errorf("%s: failed to %s, err: %v(%d)", h.si, action, err, status)
		if cmn.IsErrConnectionRefused(err) {
			j++
		} else {
			i++
		}
		if retryPolicy.backoff && k > 1 {
			sleep = cmn.MinDuration(sleep+retryPolicy.sleep, config.Timeout.MaxKeepalive)
		}
		if i < retryPolicy.anyerr && j < retryPolicy.refused {
			time.Sleep(sleep)
			glog.Errorf("%s: retrying(%d)...", h.si, k)
		}
	}
	return
}

func (h *httprunner) getPrimaryURLAndSI() (url string, psi *cluster.Snode) {
	smap := h.owner.smap.get()
	if smap == nil || smap.version() == 0 || smap.Primary == nil || !smap.isValid() {
		url, psi = cmn.GCO.Get().Proxy.PrimaryURL, nil
		return
	}
	url, psi = smap.Primary.IntraControlNet.DirectURL, smap.Primary
	return
}

func (h *httprunner) pollClusterStarted(timeout time.Duration) {
	for i := 1; ; i++ {
		time.Sleep(time.Duration(i) * time.Second)
		smap := h.owner.smap.get()
		if !smap.isValid() {
			continue
		}
		if _, err, _ := h.Health(smap.Primary, timeout, nil); err == nil {
			break
		}
	}
}

func (h *httprunner) bucketPropsToHdr(bck *cluster.Bck, hdr http.Header) {
	if bck.Props == nil {
		bck.Props = cmn.DefaultCloudBckProps(hdr)
	}
	finalProps := bck.Props.Clone()
	cmn.IterFields(finalProps, func(fieldName string, field cmn.IterField) (error, bool) {
		if fieldName == cmn.HeaderBucketVerEnabled {
			if hdr.Get(cmn.HeaderBucketVerEnabled) == "" {
				verEnabled := field.Value().(bool)
				hdr.Set(cmn.HeaderBucketVerEnabled, strconv.FormatBool(verEnabled))
			}
			return nil, false
		} else if fieldName == cmn.HeaderBucketCreated {
			created := time.Unix(0, field.Value().(int64))
			hdr.Set(cmn.HeaderBucketCreated, created.Format(time.RFC3339))
		}

		hdr.Set(fieldName, fmt.Sprintf("%v", field.Value()))
		return nil, false
	})
}

func (h *httprunner) selectBMDBuckets(bmd *bucketMD, query cmn.QueryBcks) cmn.BucketNames {
	var (
		names = make(cmn.BucketNames, 0, 10)
		cp    = &query.Provider
	)
	if query.Provider == "" {
		cp = nil
	}
	bmd.Range(cp, nil, func(bck *cluster.Bck) bool {
		if query.Equal(bck.Bck) || query.Contains(bck.Bck) {
			names = append(names, bck.Bck)
		}
		return false
	})
	sort.Sort(names)
	return names
}

func newBckFromQuery(bckName string, query url.Values) (*cluster.Bck, error) {
	provider := query.Get(cmn.URLParamProvider)
	if provider != "" && !cmn.IsValidProvider(provider) {
		return nil, fmt.Errorf("invalid provider %q", provider)
	}
	namespace := cmn.ParseNsUname(query.Get(cmn.URLParamNamespace))
	return cluster.NewBck(bckName, provider, namespace), nil
}

////////////////////
// aisMsg helpers //
////////////////////

func (h *httprunner) newAisMsgStr(msgStr string, smap *smapX, bmd *bucketMD) *aisMsg {
	return h.newAisMsg(&cmn.ActionMsg{Value: msgStr}, smap, bmd)
}

func (h *httprunner) newAisMsg(actionMsg *cmn.ActionMsg, smap *smapX, bmd *bucketMD, uuid ...string) *aisMsg {
	msg := &aisMsg{ActionMsg: *actionMsg}
	if smap != nil {
		msg.SmapVersion = smap.Version
	} else {
		msg.SmapVersion = h.owner.smap.Get().Version
	}
	if bmd != nil {
		msg.BMDVersion = bmd.Version
	} else {
		msg.BMDVersion = h.owner.bmd.Get().Version
	}
	if len(uuid) > 0 {
		msg.UUID = uuid[0]
	}
	return msg
}

func (h *httprunner) attachDetachRemoteAIS(query url.Values, action string) error {
	var (
		aisConf cmn.CloudConfAIS
		config  = cmn.GCO.BeginUpdate()
		v, ok   = config.Cloud.ProviderConf(cmn.ProviderAIS)
		changed bool
	)
	if !ok {
		if action == cmn.ActDetach {
			cmn.GCO.DiscardUpdate()
			return fmt.Errorf("%s: remote cluster config is empty", h.si)
		}
		aisConf = make(cmn.CloudConfAIS)
	} else {
		aisConf, ok = v.(cmn.CloudConfAIS)
		cmn.Assert(ok)
	}
	// detach
	if action == cmn.ActDetach {
		for alias := range query {
			if alias == cmn.URLParamWhat {
				continue
			}
			if _, ok := aisConf[alias]; ok {
				changed = true
				delete(aisConf, alias)
			}
		}
		goto rret
	}
	// attach
	for alias, urls := range query {
		if alias == cmn.URLParamWhat {
			continue
		}
		for _, u := range urls {
			if _, err := url.ParseRequestURI(u); err != nil {
				cmn.GCO.DiscardUpdate()
				return fmt.Errorf("%s: cannot attach remote cluster: %v", h.si, err)
			}
			changed = true
		}
		if changed {
			if confURLs, ok := aisConf[alias]; ok {
				aisConf[alias] = append(confURLs, urls...)
			} else {
				aisConf[alias] = urls
			}
		}
	}
rret:
	if !changed {
		cmn.GCO.DiscardUpdate()
		return fmt.Errorf("%s: request to %s remote cluster - nothing to do", h.si, action)
	}
	config.Cloud.ProviderConf(cmn.ProviderAIS, aisConf)
	cmn.GCO.CommitUpdate(config)
	return nil
}
