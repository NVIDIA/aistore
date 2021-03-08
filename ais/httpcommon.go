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
	"net"
	"net/http"
	"net/url"
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
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction/xreg"
	jsoniter "github.com/json-iterator/go"
	"github.com/tinylib/msgp/msgp"
)

const unknownDaemonID = "unknown"

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

	sliceResults []*callResult

	// callArgs contains arguments for a peer-to-peer control plane call.
	callArgs struct {
		req     cmn.ReqArgs
		timeout time.Duration
		si      *cluster.Snode
		v       interface{} // Value that needs to be unmarshalled.
	}

	// bcastArgs contains arguments for an intra-cluster broadcast call.
	bcastArgs struct {
		req               cmn.ReqArgs        // h.call args
		network           string             // one of the cmn.KnownNetworks
		timeout           time.Duration      // call timeout
		fv                func() interface{} // optional; returns marshaled msg (see `callArgs.v`)
		nodes             []cluster.NodeMap  // broadcast destinations - map(s)
		selected          cluster.Nodes      // broadcast destinations - slice of selected few
		smap              *smapX             // Smap to use
		to                int                // (all targets, all proxies, all nodes) enum
		nodeCount         int                // m.b. greater or equal destination count
		ignoreMaintenance bool               // do not skip nodes under maintenance
		async             bool               // ignore results
	}

	networkHandler struct {
		r   string           // resource
		h   http.HandlerFunc // handler
		net netAccess        // handler network access
	}

	// SmapVoteMsg contains cluster-wide MD and a boolean representing whether or not a vote is currently in proress.
	// NOTE: exported for integration testing
	SmapVoteMsg struct {
		Smap           *smapX    `json:"smap"`
		BMD            *bucketMD `json:"bucketmd"`
		RMD            *rebMD    `json:"rmd"`
		VoteInProgress bool      `json:"vote_in_progress"`
	}

	electable interface {
		proxyElection(vr *VoteRecord, primary *cluster.Snode)
	}

	// two pieces of metadata a self-registering (joining) target wants to know right away
	nodeRegMeta struct {
		Smap *smapX         `json:"smap"`
		BMD  *bucketMD      `json:"bmd"`
		SI   *cluster.Snode `json:"si"`
		Reb  bool           `json:"reb"`
	}

	nodeRegPool []nodeRegMeta

	// aisMsg is an extended ActionMsg with extra information for node <=> node control plane communications
	aisMsg struct {
		cmn.ActionMsg
		BMDVersion  int64  `json:"bmdversion,string"`
		SmapVersion int64  `json:"smapversion,string"`
		RMDVersion  int64  `json:"rmdversion,string"`
		UUID        string `json:"uuid"` // cluster-wide ID of this action (operation, transaction)
	}

	// node <=> node cluster-info exchange
	clusterInfo struct {
		BMD struct {
			Version int64  `json:"version,string"`
			UUID    string `json:"uuid"`
		} `json:"bmd"`
		Smap struct {
			Version int64  `json:"version,string"`
			UUID    string `json:"uuid"`
			Primary struct {
				PubURL  string `json:"pub_url"`
				CtrlURL string `json:"control_url"`
				ID      string `json:"id"`
			}
			VoteInProgress bool `json:"vote_in_progress"`
		} `json:"smap"`
		Config struct {
			Version int64 `json:"version,string"`
		} `json:"config"`
	}

	// http server and http runner (common for proxy and target)
	netServer struct {
		s             *http.Server
		muxers        cmn.HTTPMuxers
		sndRcvBufSize int
	}
	httprunner struct {
		name      string
		si        *cluster.Snode
		logger    *log.Logger
		keepalive keepaliver
		statsT    stats.Tracker
		netServ   struct {
			pub     *netServer
			control *netServer
			data    *netServer
		}
		client struct {
			control *http.Client // http client for intra-cluster comm
			data    *http.Client // http client to execute target <=> target GET & PUT (object)
		}
		owner struct {
			smap   *smapOwner
			bmd    bmdOwner
			rmd    *rmdOwner
			config *configOwner
		}
		startup struct {
			cluster atomic.Bool // determines if the cluster has started up
			node    atomic.Time // determines time when the node started up
		}
		gmm                 *memsys.MMSA // system pagesize-based memory manager and slab allocator
		smm                 *memsys.MMSA // system MMSA for small-size allocations
		electable           electable
		inPrimaryTransition atomic.Bool
	}

	glogWriter struct{}

	// error types
	// BMD & its uuid
	errTgtBmdUUIDDiffer struct{ detail string }
	errPrxBmdUUIDDiffer struct{ detail string }
	errBmdUUIDSplit     struct{ detail string }
	// ditto Smap
	errSmapUUIDDiffer struct{ detail string }
	errNodeNotFound   struct {
		msg  string
		id   string
		si   *cluster.Snode
		smap *smapX
	}
	errNotEnoughTargets struct {
		si       *cluster.Snode
		smap     *smapX
		required int // should at least contain
	}
	errDowngrade struct {
		si       *cluster.Snode
		from, to string
	}
	errNotPrimary struct {
		si     *cluster.Snode
		smap   *smapX
		detail string
	}
	// apiRequest
	apiRequest struct {
		prefix []string     // in: URL must start with these items
		after  int          // in: the number of items after the prefix
		bckIdx int          // in: ordinal number of bucket in URL (some paths starts with extra items: EC & ETL)
		msg    interface{}  // in/out: if not nil, Body is unmarshaled to the msg
		items  []string     // out: URL items after the prefix
		bck    *cluster.Bck // out: initialized bucket
	}
)

var allHTTPverbs = []string{
	http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
	http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodTrace,
}

var (
	errShutdown          = errors.New(cmn.ActShutdown)
	errRebalanceDisabled = errors.New("rebalance is disabled")
)

// BMD uuid errs
var errNoBMD = errors.New("no bucket metadata")

func (e *errTgtBmdUUIDDiffer) Error() string { return e.detail }
func (e *errBmdUUIDSplit) Error() string     { return e.detail }
func (e *errPrxBmdUUIDDiffer) Error() string { return e.detail }
func (e *errSmapUUIDDiffer) Error() string   { return e.detail }
func (e *errNodeNotFound) Error() string {
	return fmt.Sprintf("%s: %s node %s (not present in the %s)", e.si, e.msg, e.id, e.smap)
}

//////////////////
// errDowngrade //
//////////////////

func newErrDowngrade(si *cluster.Snode, from, to string) *errDowngrade {
	return &errDowngrade{si, from, to}
}

func (e *errDowngrade) Error() string {
	return fmt.Sprintf("%s: attempt to downgrade %s to %s", e.si, e.from, e.to)
}

func isErrDowngrade(err error) bool {
	if _, ok := err.(*errDowngrade); ok {
		return true
	}
	erd := &errDowngrade{}
	return errors.As(err, &erd)
}

/////////////////////////
// errNotEnoughTargets //
////////////////////////

func (e *errNotEnoughTargets) Error() string {
	return fmt.Sprintf("%s: not enough targets in %s: need %d, have %d",
		e.si, e.smap, e.required, e.smap.CountActiveTargets())
}

///////////////////
// errNotPrimary //
///////////////////

func newErrNotPrimary(si *cluster.Snode, smap *smapX, detail ...string) *errNotPrimary {
	if len(detail) == 0 {
		return &errNotPrimary{si, smap, ""}
	}
	return &errNotPrimary{si, smap, detail[0]}
}

func (e *errNotPrimary) Error() string {
	var present, detail string
	if !e.smap.isPresent(e.si) {
		present = "not present in the "
	}
	if e.detail != "" {
		detail = ": " + e.detail
	}
	return fmt.Sprintf("%s is not primary [%s%s]%s", e.si, present, e.smap.StringEx(), detail)
}

///////////////
// bargsPool //
///////////////

var (
	bargsPool sync.Pool
	bargs0    bcastArgs
)

func allocBcastArgs() (a *bcastArgs) {
	if v := bargsPool.Get(); v != nil {
		a = v.(*bcastArgs)
		return
	}
	return &bcastArgs{}
}

func freeBcastArgs(a *bcastArgs) {
	sel := a.selected
	*a = bargs0
	if sel != nil {
		a.selected = sel[:0]
	}
	bargsPool.Put(a)
}

///////////////////////
// call result pools //
///////////////////////

var (
	resultsPool sync.Pool
	callResPool sync.Pool
	callRes0    callResult
)

func allocCallRes() (a *callResult) {
	if v := callResPool.Get(); v != nil {
		a = v.(*callResult)
		debug.Assert(a.si == nil)
		return
	}
	return &callResult{}
}

func _freeCallRes(res *callResult) {
	*res = callRes0
	callResPool.Put(res)
}

func allocSliceRes() sliceResults {
	if v := resultsPool.Get(); v != nil {
		a := v.(*sliceResults)
		return *a
	}
	return make(sliceResults, 0, 8) // TODO: num-nodes
}

func freeCallResults(results sliceResults) {
	for _, res := range results {
		*res = callRes0
		callResPool.Put(res)
	}
	results = results[:0]
	resultsPool.Put(&results)
}

////////////////
// glogWriter //
////////////////

const tlsHandshakeErrorPrefix = "http: TLS handshake error"

func (r *glogWriter) Write(p []byte) (int, error) {
	s := string(p)
	// Ignore TLS handshake errors (see: https://github.com/golang/go/issues/26918).
	if strings.Contains(s, tlsHandshakeErrorPrefix) {
		return len(p), nil
	}

	glog.Errorln(s)

	stacktrace := rdebug.Stack()
	glog.Errorln(string(stacktrace))
	return len(p), nil
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
		server.muxers.ServeHTTP(w, r)
		return
	}

	// TODO: add support for caching HTTPS requests
	destConn, err := net.DialTimeout("tcp", r.Host, 10*time.Second)
	if err != nil {
		cmn.WriteErr(w, r, err, http.StatusServiceUnavailable)
		return
	}

	// Second, hijack the connection. A kind of man-in-the-middle attack
	// Since this moment this function is responsible of HTTP connection
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		cmn.WriteErr(w, r, errors.New("client does not support hijacking"),
			http.StatusInternalServerError)
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
	// spend time on getting and parsing global configuration, string
	// comparison and branching
	var httpHandler http.Handler = server.muxers
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
	cos.Assert(ok)
	rawconn, _ := tcpconn.SyscallConn()
	args := cmn.TransportArgs{SndRcvBufSize: server.sndRcvBufSize}
	rawconn.Control(args.ConnControl(rawconn))
}

func (server *netServer) shutdown() {
	if server.s == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), cmn.GCO.Get().Timeout.MaxHostBusy)
	if err := server.s.Shutdown(ctx); err != nil {
		glog.Infof("Stopped server, err: %v", err)
	}
	cancel()
}

////////////////
// httprunner //
////////////////

func (h *httprunner) Name() string             { return h.name }
func (h *httprunner) DataClient() *http.Client { return h.client.data }
func (h *httprunner) Snode() *cluster.Snode    { return h.si }
func (h *httprunner) Bowner() cluster.Bowner   { return h.owner.bmd }
func (h *httprunner) Sowner() cluster.Sowner   { return h.owner.smap }

func (h *httprunner) parseAPIRequest(w http.ResponseWriter, r *http.Request, args *apiRequest) (err error) {
	debug.Assert(len(args.prefix) != 0)
	args.items, err = h.checkRESTItems(w, r, args.after, false, args.prefix)
	if err != nil {
		return err
	}
	debug.Assert(len(args.items) > args.bckIdx)
	bckName := args.items[args.bckIdx]
	args.bck, err = newBckFromQuery(bckName, r.URL.Query())
	if err == nil && args.msg != nil {
		err = cmn.ReadJSON(w, r, args.msg)
	}
	if err != nil {
		h.writeErr(w, r, err)
	}
	return err
}

// usage: [API call => handler => ClusterStartedWithRetry ]
func (h *httprunner) ClusterStartedWithRetry() bool {
	if h.startup.cluster.Load() {
		return true
	}
	if !h.NodeStarted() {
		return false
	}
	time.Sleep(time.Second)
	if !h.startup.cluster.Load() {
		glog.ErrorDepth(1, fmt.Sprintf("%s: cluster is starting up (?)", h.si))
	}
	return h.startup.cluster.Load()
}
func (h *httprunner) ClusterStarted() bool       { return h.startup.cluster.Load() }
func (h *httprunner) NodeStarted() bool          { return !h.startup.node.Load().IsZero() }
func (h *httprunner) NodeStartedTime() time.Time { return h.startup.node.Load() }
func (h *httprunner) markClusterStarted()        { h.startup.cluster.Store(true) }
func (h *httprunner) markNodeStarted()           { h.startup.node.Store(time.Now()) }

func (h *httprunner) registerNetworkHandlers(networkHandlers []networkHandler) {
	var (
		path   string
		config = cmn.GCO.Get()
	)
	for r, nh := range debug.Handlers() {
		h.registerPublicNetHandler(r, nh)
	}
	for _, nh := range networkHandlers {
		var reg bool
		if nh.r[0] == '/' { // absolute path
			path = nh.r
		} else {
			path = cos.JoinWords(cmn.Version, nh.r)
		}
		cos.Assert(nh.net != 0)
		if nh.net.isSet(accessNetPublic) {
			h.registerPublicNetHandler(path, nh.h)
			reg = true
		}
		if config.HostNet.UseIntraControl && nh.net.isSet(accessNetIntraControl) {
			h.registerIntraControlNetHandler(path, nh.h)
			reg = true
		}
		if config.HostNet.UseIntraData && nh.net.isSet(accessNetIntraData) {
			h.registerIntraDataNetHandler(path, nh.h)
			reg = true
		}
		if reg {
			continue
		}
		// none of the above
		if !config.HostNet.UseIntraControl && !config.HostNet.UseIntraData {
			// no intra-cluster networks: default to pub net
			h.registerPublicNetHandler(path, nh.h)
		} else if config.HostNet.UseIntraControl && nh.net.isSet(accessNetIntraData) {
			// (not configured) data defaults to (configured) control
			h.registerIntraControlNetHandler(path, nh.h)
		} else {
			cos.Assert(config.HostNet.UseIntraData && nh.net.isSet(accessNetIntraControl))
			// (not configured) control defaults to (configured) data
			h.registerIntraDataNetHandler(path, nh.h)
		}
	}
}

func (h *httprunner) registerPublicNetHandler(path string, handler func(http.ResponseWriter, *http.Request)) {
	for _, v := range allHTTPverbs {
		h.netServ.pub.muxers[v].HandleFunc(path, handler)
		if !strings.HasSuffix(path, "/") {
			h.netServ.pub.muxers[v].HandleFunc(path+"/", handler)
		}
	}
}

func (h *httprunner) registerIntraControlNetHandler(path string, handler func(http.ResponseWriter, *http.Request)) {
	for _, v := range allHTTPverbs {
		h.netServ.control.muxers[v].HandleFunc(path, handler)
		if !strings.HasSuffix(path, "/") {
			h.netServ.control.muxers[v].HandleFunc(path+"/", handler)
		}
	}
}

func (h *httprunner) registerIntraDataNetHandler(path string, handler func(http.ResponseWriter, *http.Request)) {
	for _, v := range allHTTPverbs {
		h.netServ.data.muxers[v].HandleFunc(path, handler)
		if !strings.HasSuffix(path, "/") {
			h.netServ.data.muxers[v].HandleFunc(path+"/", handler)
		}
	}
}

func (h *httprunner) init(config *cmn.Config) {
	h.client.control = cmn.NewClient(cmn.TransportArgs{
		Timeout:    config.Client.Timeout,
		UseHTTPS:   config.Net.HTTP.UseHTTPS,
		SkipVerify: config.Net.HTTP.SkipVerify,
	})
	h.client.data = cmn.NewClient(cmn.TransportArgs{
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

	muxers := newMuxers()
	h.netServ.pub = &netServer{muxers: muxers, sndRcvBufSize: bufsize}
	h.netServ.control = h.netServ.pub // by default, intra-control net is the same as public
	if config.HostNet.UseIntraControl {
		muxers = newMuxers()
		h.netServ.control = &netServer{muxers: muxers, sndRcvBufSize: 0}
	}
	h.netServ.data = h.netServ.control // by default, intra-data net is the same as intra-control
	if config.HostNet.UseIntraData {
		muxers = newMuxers()
		h.netServ.data = &netServer{muxers: muxers, sndRcvBufSize: bufsize}
	}

	h.owner.smap = newSmapOwner()
	h.owner.rmd = newRMDOwner()
	h.owner.rmd.load()

	h.gmm = memsys.DefaultPageMM()
	h.smm = memsys.DefaultSmallMM()
}

func newMuxers() cmn.HTTPMuxers {
	m := make(cmn.HTTPMuxers, len(allHTTPverbs))
	for _, v := range allHTTPverbs {
		m[v] = mux.NewServeMux()
	}
	return m
}

func (h *httprunner) initConfOwner(daemonType string) {
	// check if global config exists
	if h.owner.config == nil {
		h.owner.config = newConfOwner(daemonType)
	}
	if err := h.owner.config.load(); err != nil {
		cos.ExitLogf("Failed to initialize config owner, err: %v", err)
	}
}

// initSI initializes this cluster.Snode
func (h *httprunner) initSI(daemonType string) {
	var (
		s                                        string
		config                                   = cmn.GCO.Get()
		port                                     = strconv.Itoa(config.HostNet.Port)
		proto                                    = config.Net.HTTP.Proto
		addrList, err                            = getLocalIPv4List()
		k8sDetected                              = k8s.Detect() == nil
		pubAddr, intraControlAddr, intraDataAddr cluster.NetInfo
	)
	if err != nil {
		cos.ExitLogf("Failed to get local IP addr list, err: %v", err)
	}

	// NOTE: In K8S deployment, public hostname could be LoadBalancer external IP, or a service DNS.
	if k8sDetected && config.HostNet.Hostname != "" {
		glog.Infof("detected K8S deployment, skipping hostname validation for %q", config.HostNet.Hostname)
		pubAddr = *cluster.NewNetInfo(proto, config.HostNet.Hostname, port)
	} else {
		pubAddr, err = getNetInfo(addrList, proto, config.HostNet.Hostname, port)
	}

	if err != nil {
		cos.ExitLogf("Failed to get %s IPv4/hostname: %v", cmn.NetworkPublic, err)
	}
	if config.HostNet.Hostname != "" {
		s = " (config: " + config.HostNet.Hostname + ")"
	}
	glog.Infof("%s (user) access: [%s]%s", cmn.NetworkPublic, pubAddr, s)

	intraControlAddr = pubAddr
	if config.HostNet.UseIntraControl {
		intraControlAddr, err = getNetInfo(addrList, proto, config.HostNet.HostnameIntraControl, strconv.Itoa(config.HostNet.PortIntraControl))
		if err != nil {
			cos.ExitLogf("Failed to get %s IPv4/hostname: %v", cmn.NetworkIntraControl, err)
		}
		s = ""
		if config.HostNet.HostnameIntraControl != "" {
			s = " (config: " + config.HostNet.HostnameIntraControl + ")"
		}
		glog.Infof("%s access: [%s]%s", cmn.NetworkIntraControl, intraControlAddr, s)
	}

	intraDataAddr = pubAddr
	if config.HostNet.UseIntraData {
		intraDataAddr, err = getNetInfo(addrList, proto, config.HostNet.HostnameIntraData, strconv.Itoa(config.HostNet.PortIntraData))
		if err != nil {
			cos.ExitLogf("Failed to get %s IPv4/hostname: %v", cmn.NetworkIntraData, err)
		}
		s = ""
		if config.HostNet.HostnameIntraData != "" {
			s = " (config: " + config.HostNet.HostnameIntraData + ")"
		}
		glog.Infof("%s access: [%s]%s", cmn.NetworkIntraData, intraDataAddr, s)
	}

	mustDiffer(pubAddr, config.HostNet.Port, true,
		intraControlAddr, config.HostNet.PortIntraControl, config.HostNet.UseIntraControl, "pub/ctl")
	mustDiffer(pubAddr, config.HostNet.Port, true, intraDataAddr, config.HostNet.PortIntraData, config.HostNet.UseIntraData, "pub/data")
	mustDiffer(intraDataAddr, config.HostNet.PortIntraData, config.HostNet.UseIntraData,
		intraControlAddr, config.HostNet.PortIntraControl, config.HostNet.UseIntraControl, "ctl/data")

	daemonID := initDaemonID(daemonType, config)
	h.name = daemonType
	h.si = cluster.NewSnode(
		daemonID,
		daemonType,
		pubAddr,
		intraControlAddr,
		intraDataAddr,
	)
	cos.InitShortID(h.si.Digest())
}

func mustDiffer(ip1 cluster.NetInfo, port1 int, use1 bool, ip2 cluster.NetInfo, port2 int, use2 bool, tag string) {
	if !use1 || !use2 {
		return
	}
	if ip1.NodeHostname == ip2.NodeHostname && port1 == port2 {
		cos.ExitLogf("%s: cannot use the same IP:port (%s) for two networks", tag, ip1)
	}
}

// detectNodeChanges is called at startup. Given loaded Smap, checks whether
// `h.si` is present and whether any information has changed.
func (h *httprunner) detectNodeChanges(smap *smapX) error {
	snode := smap.GetNode(h.si.ID())
	if snode == nil {
		return fmt.Errorf("%s: not present in the loaded %s", h.si, smap)
	}
	if !snode.Equals(h.si) {
		prev, _ := cos.JSON.MarshalIndent(snode, "", " ")
		curr, _ := cos.JSON.MarshalIndent(h.si, "", " ")
		return fmt.Errorf(
			"%s: detected a change in snode configuration (prev: %s, curr: %s)",
			h.si, string(prev), string(curr),
		)
	}
	return nil
}

func (h *httprunner) tryLoadSmap() (_ *smapX, reliable bool) {
	var (
		config      = cmn.GCO.Get()
		smap        = newSmap()
		loaded, err = h.owner.smap.load(smap, config)
	)
	if err != nil {
		glog.Errorf("Failed to load Smap (err: %v)", err)
		smap = newSmap() // Reinitialize smap as it might have been corrupted during loading.
	} else if loaded {
		reliable = true
		if err := h.detectNodeChanges(smap); err != nil {
			glog.Error(err)
			reliable = false
		}
		if _, err := smap.IsDuplicate(h.si); err != nil {
			glog.Error(err)
			reliable = false
		}
	}
	return smap, reliable
}

func (h *httprunner) setDaemonConfig(w http.ResponseWriter, r *http.Request, toUpdate *cmn.ConfigToUpdate, transient bool) (err error) {
	err = h.owner.config.modifyOverride(toUpdate, transient)
	if err != nil {
		h.writeErr(w, r, err)
	}
	return
}

func (h *httprunner) run() error {
	config := cmn.GCO.Get()
	testingEnv := config.TestingEnv()

	// A wrapper to glog http.Server errors - otherwise
	// os.Stderr would be used, as per golang.org/pkg/net/http/#Server
	h.logger = log.New(&glogWriter{}, "net/http err: ", 0)
	if config.HostNet.UseIntraControl || config.HostNet.UseIntraData {
		var errCh chan error
		if config.HostNet.UseIntraControl && config.HostNet.UseIntraData {
			errCh = make(chan error, 3)
		} else {
			errCh = make(chan error, 2)
		}

		if config.HostNet.UseIntraControl {
			go func() {
				addr := h.si.IntraControlNet.TCPEndpoint()
				errCh <- h.netServ.control.listenAndServe(addr, h.logger)
			}()
		}

		if config.HostNet.UseIntraData {
			go func() {
				addr := h.si.IntraDataNet.TCPEndpoint()
				errCh <- h.netServ.data.listenAndServe(addr, h.logger)
			}()
		}

		go func() {
			addr := ":" + h.si.PublicNet.DaemonPort
			if testingEnv {
				// On testing environment just listen on specified `ip:port`.
				// On production env and K8S listen on `*:port`
				addr = h.si.PublicNet.NodeHostname + addr
			}
			errCh <- h.netServ.pub.listenAndServe(addr, h.logger)
		}()

		return <-errCh
	}

	var (
		addr        string
		k8sDetected = k8s.Detect() == nil
	)
	if testingEnv && !k8sDetected {
		// On testing environment just listen on specified `ip:port`.
		addr = h.si.PublicNet.TCPEndpoint()
	} else {
		// When configured or in production env, when only public net is configured,
		// listen on `*:port`.
		addr = ":" + h.si.PublicNet.DaemonPort
	}
	return h.netServ.pub.listenAndServe(addr, h.logger)
}

// remove self from Smap (if required), terminate http, and wait (w/ timeout)
// for running xactions to abort
func (h *httprunner) stop(rmFromSmap bool) {
	if rmFromSmap {
		h.unregisterSelf(true)
	}
	glog.Warningln("Shutting down HTTP")
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		time.Sleep(time.Second / 2)
		h.netServ.pub.shutdown()
		config := cmn.GCO.Get()
		if config.HostNet.UseIntraControl {
			h.netServ.control.shutdown()
		}
		if config.HostNet.UseIntraData {
			h.netServ.data.shutdown()
		}
		wg.Done()
	}()
	entry := xreg.GetRunning(xreg.XactFilter{})
	if entry != nil {
		time.Sleep(time.Second)
		entry = xreg.GetRunning(xreg.XactFilter{})
		if entry != nil {
			glog.Warningf("Timed out waiting for %q to finish aborting", entry.Kind())
		}
	}
	if h.si.IsTarget() {
		wg.Wait()
	}
}

//
// intra-cluster IPC, control plane
// call another target or a proxy; optionally, include a json-encoded body
//
func (h *httprunner) _call(si *cluster.Snode, bargs *bcastArgs) (res *callResult) {
	cargs := callArgs{si: si, req: bargs.req, timeout: bargs.timeout}
	cargs.req.Base = si.URL(bargs.network)
	if bargs.req.BodyR != nil {
		cargs.req.BodyR, _ = bargs.req.BodyR.(cos.ReadOpenCloser).Open()
	}
	if bargs.fv != nil {
		cargs.v = bargs.fv()
	}
	return h.call(cargs)
}

func (h *httprunner) call(args callArgs) (res *callResult) {
	var (
		req    *http.Request
		resp   *http.Response
		client *http.Client
		sid    = unknownDaemonID
	)
	res = allocCallRes()
	if args.si != nil {
		sid = args.si.ID()
		res.si = args.si
	}

	cos.Assert(args.si != nil || args.req.Base != "") // either we have si or base
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

		client = h.client.control
	case cmn.LongTimeout:
		req, res.err = args.req.Req()
		if res.err != nil {
			break
		}

		client = h.client.data
	default:
		var cancel context.CancelFunc
		if args.timeout == 0 {
			args.timeout = cmn.GCO.Get().Timeout.CplaneOperation
		}
		req, _, cancel, res.err = args.req.ReqWithTimeout(args.timeout)
		if res.err != nil {
			break
		}
		defer cancel() // timeout => context.deadlineExceededError

		if args.timeout > h.client.control.Timeout {
			client = h.client.data
		} else {
			client = h.client.control
		}
	}

	if res.err != nil {
		res.details = fmt.Sprintf("unexpected failure to create HTTP request %s %s, err: %v",
			args.req.Method, args.req.URL(), res.err)
		return
	}

	req.Header.Set(cmn.HeaderCallerID, h.si.ID())
	req.Header.Set(cmn.HeaderCallerName, h.si.Name())
	if smap := h.owner.smap.get(); smap.validate() == nil {
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
		if args.req.Method == http.MethodHead {
			msg := resp.Header.Get(cmn.HeaderError)
			res.err = cmn.S2HTTPErr(req, msg, res.status)
		} else {
			var b bytes.Buffer
			b.ReadFrom(resp.Body)
			res.err = cmn.S2HTTPErr(req, b.String(), res.status)
		}
		res.details = res.err.Error()
		return
	}

	if args.v != nil {
		if v, ok := args.v.(msgp.Decodable); ok {
			buf, slab := h.smm.Alloc(10 * cos.KiB)
			res.err = v.DecodeMsg(msgp.NewReaderBuf(resp.Body, buf))
			slab.Free(buf)
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

func (h *httprunner) callerNotifyFin(n cluster.Notif, err error) {
	h.callerNotify(n, err, cmn.Finished)
}

func (h *httprunner) callerNotifyProgress(n cluster.Notif) {
	h.callerNotify(n, nil, cmn.Progress)
}

func (h *httprunner) callerNotify(n cluster.Notif, err error, kind string) {
	var (
		smap  = h.owner.smap.get()
		dsts  = n.Subscribers()
		msg   = n.ToNotifMsg()
		args  = allocBcastArgs()
		nodes = args.selected
	)
	debug.Assert(kind == cmn.Progress || kind == cmn.Finished)
	if len(dsts) == 1 && dsts[0] == equalIC {
		for pid, psi := range smap.Pmap {
			if smap.IsIC(psi) && pid != h.si.ID() && !smap.Pmap.InMaintenance(psi) {
				nodes = append(nodes, psi)
			}
		}
	} else {
		for _, dst := range dsts {
			debug.Assert(dst != equalIC)
			if si := smap.GetNodeNotMaint(dst); si != nil {
				nodes = append(nodes, si)
			} else {
				glog.Error(&errNodeNotFound{"failed to notify", dst, h.si, smap})
			}
		}
	}
	if err != nil {
		msg.ErrMsg = err.Error()
	}
	msg.NodeID = h.si.ID()
	if len(nodes) == 0 {
		glog.Errorf("%s: have no nodes to send notification %s", h.si, &msg)
		return
	}

	path := cmn.URLPathNotifs.Join(kind)
	args.req = cmn.ReqArgs{Method: http.MethodPost, Path: path, Body: cos.MustMarshal(&msg)}
	args.network = cmn.NetworkIntraControl
	args.timeout = cmn.GCO.Get().Timeout.MaxKeepalive
	args.selected = nodes
	args.nodeCount = len(nodes)
	args.async = true
	h.bcastNodes(args)
	freeBcastArgs(args)
}

///////////////
// broadcast //
///////////////

// bcastGroup broadcasts a message to a specific group of nodes: targets, proxies, all.
func (h *httprunner) bcastGroup(args *bcastArgs) sliceResults {
	if args.smap == nil {
		args.smap = h.owner.smap.get()
	}
	present := args.smap.isPresent(h.si)
	if args.network == "" {
		args.network = cmn.NetworkIntraControl
	}
	debug.Assert(cmn.NetworkIsKnown(args.network))
	if args.timeout == 0 {
		args.timeout = cmn.GCO.Get().Timeout.CplaneOperation
		cos.Assert(args.timeout != 0)
	}

	switch args.to {
	case cluster.Targets:
		args.nodes = []cluster.NodeMap{args.smap.Tmap}
		args.nodeCount = len(args.smap.Tmap)
		if present && h.si.IsTarget() {
			args.nodeCount--
		}
	case cluster.Proxies:
		args.nodes = []cluster.NodeMap{args.smap.Pmap}
		args.nodeCount = len(args.smap.Pmap)
		if present && h.si.IsProxy() {
			args.nodeCount--
		}
	case cluster.AllNodes:
		args.nodes = []cluster.NodeMap{args.smap.Pmap, args.smap.Tmap}
		args.nodeCount = len(args.smap.Pmap) + len(args.smap.Tmap)
		if present {
			args.nodeCount--
		}
	default:
		cos.Assert(false)
	}
	return h.bcastNodes(args)
}

// bcastNodes broadcasts a message to the specified destinations (`bargs.nodes`).
// It then waits and collects all the responses (compare with bcastAsyncNodes).
// Note that, if specified, `bargs.req.BodyR` must implement `cos.ReadOpenCloser`.
func (h *httprunner) bcastNodes(bargs *bcastArgs) sliceResults {
	var (
		ch sliceResults
		mu sync.Mutex
		wg = cos.NewLimitedWaitGroup(cluster.MaxBcastParallel(), bargs.nodeCount)
	)
	if !bargs.async {
		ch = allocSliceRes()
	}
	f1 := func(si *cluster.Snode) {
		res := h._call(si, bargs)
		if !bargs.async {
			mu.Lock()
			ch = append(ch, res)
			mu.Unlock()
		}
		wg.Done()
	}
	if bargs.nodes != nil {
		debug.Assert(len(bargs.selected) == 0)
		for _, nodeMap := range bargs.nodes {
			for _, si := range nodeMap {
				if si.ID() == h.si.ID() {
					continue
				}
				if !bargs.ignoreMaintenance && nodeMap.InMaintenance(si) {
					continue
				}
				wg.Add(1)
				go f1(si)
			}
		}
	} else {
		debug.Assert(len(bargs.selected) > 0)
		for _, si := range bargs.selected {
			debug.Assert(si.ID() != h.si.ID())
			debug.Assert(h.owner.smap.get().GetNodeNotMaint(si.ID()) != nil)
			wg.Add(1)
			go f1(si)
		}
	}
	wg.Wait()
	return ch
}

func (h *httprunner) bcastAsyncIC(msg *aisMsg) {
	var (
		wg   = &sync.WaitGroup{}
		smap = h.owner.smap.get()
		args = allocBcastArgs()
	)
	args.req = cmn.ReqArgs{Method: http.MethodPost, Path: cmn.URLPathIC.S, Body: cos.MustMarshal(msg)}
	args.network = cmn.NetworkIntraControl
	args.timeout = cmn.GCO.Get().Timeout.MaxKeepalive
	for pid, psi := range smap.Pmap {
		if pid == h.si.ID() || !smap.IsIC(psi) || smap.GetNodeNotMaint(pid) == nil {
			continue
		}
		wg.Add(1)
		go func(si *cluster.Snode) {
			cargs := callArgs{si: si, req: args.req, timeout: args.timeout}
			_ = h.call(cargs)
			wg.Done()
		}(psi)
	}
	wg.Wait()
	freeBcastArgs(args)
}

//////////////////////////////////
// HTTP request parsing helpers //
//////////////////////////////////

// remove validated fields and return the resulting slice
func (h *httprunner) checkRESTItems(w http.ResponseWriter, r *http.Request, itemsAfter int,
	splitAfter bool, items []string) ([]string, error) {
	items, err := cmn.MatchRESTItems(r.URL.Path, itemsAfter, splitAfter, items)
	if err != nil {
		h.writeErr(w, r, err)
		return nil, err
	}

	return items, nil
}

func (h *httprunner) writeMsgPack(w http.ResponseWriter, r *http.Request, v interface{}, tag string) (ok bool) {
	var (
		err       error
		buf, slab = h.smm.Alloc(10 * cos.KiB)
		mw        = msgp.NewWriterBuf(w, buf)
	)
	defer slab.Free(buf)

	w.Header().Set(cmn.HeaderContentType, cmn.ContentMsgPack)
	if err = v.(msgp.Encodable).EncodeMsg(mw); err == nil {
		err = mw.Flush()
	}
	if err != nil {
		h.handleWriteError(r, tag, err)
		return false
	}
	return true
}

func (h *httprunner) writeJSON(w http.ResponseWriter, r *http.Request, v interface{}, tag string) (ok bool) {
	_, isByteArray := v.([]byte)
	cos.Assert(!isByteArray)

	w.Header().Set(cmn.HeaderContentType, cmn.ContentJSON)
	if err := jsoniter.NewEncoder(w).Encode(v); err != nil {
		h.handleWriteError(r, tag, err)
		return false
	}
	return true
}

func (h *httprunner) writeJSONBytes(w http.ResponseWriter, r *http.Request, bytes []byte, tag string) (ok bool) {
	w.Header().Set(cmn.HeaderContentType, cmn.ContentJSON)
	if _, err := w.Write(bytes); err != nil {
		h.handleWriteError(r, tag, err)
		return false
	}
	return true
}

func (h *httprunner) handleWriteError(r *http.Request, tag string, err error) {
	msg := fmt.Sprintf("%s: failed to write bytes, err: %v", tag, err)
	if _, file, line, ok := runtime.Caller(1); ok {
		f := filepath.Base(file)
		msg += fmt.Sprintf("(%s, #%d)", f, line)
	}
	glog.Errorln(msg)
	h.statsT.AddErrorHTTP(r.Method, 1)
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
		xact := xreg.GetXactRunning(cmn.ActElection)
		msg := SmapVoteMsg{
			VoteInProgress: xact != nil,
			Smap:           h.owner.smap.get(),
			BMD:            h.owner.bmd.get(),
			RMD:            h.owner.rmd.get(),
		}
		body = msg
	case cmn.GetWhatSnode:
		body = h.si
	default:
		h.writeErrf(w, r, "invalid GET /daemon request: unrecognized what=%s", what)
		return
	}
	h.writeJSON(w, r, body, "httpdaeget-"+what)
}

////////////////////////////////////////////
// HTTP err + spec message + code + stats //
////////////////////////////////////////////

func (h *httprunner) writeErr(w http.ResponseWriter, r *http.Request, err error, errCode ...int) {
	cmn.WriteErr(w, r, err, errCode...)
	h.statsT.AddErrorHTTP(r.Method, 1)
}

func (h *httprunner) writeErrMsg(w http.ResponseWriter, r *http.Request, msg string, errCode ...int) {
	cmn.WriteErrMsg(w, r, msg, errCode...)
	h.statsT.AddErrorHTTP(r.Method, 1)
}

func (h *httprunner) writeErrSilent(w http.ResponseWriter, r *http.Request, err error, errCode ...int) {
	const _silent = 1
	if len(errCode) == 0 {
		h.writeErr(w, r, err, http.StatusBadRequest, _silent)
	} else {
		h.writeErr(w, r, err, errCode[0], _silent)
	}
}

func (h *httprunner) writeErrSilentf(w http.ResponseWriter, r *http.Request, errCode int,
	format string, a ...interface{}) {
	err := fmt.Errorf(format, a...)
	h.writeErrSilent(w, r, err, errCode)
}

func (h *httprunner) writeErrStatusf(w http.ResponseWriter, r *http.Request, errCode int,
	format string, a ...interface{}) {
	h.writeErrMsg(w, r, fmt.Sprintf(format, a...), errCode)
}

func (h *httprunner) writeErrStatusSilentf(w http.ResponseWriter, r *http.Request, errCode int,
	format string, a ...interface{}) {
	h.writeErrSilent(w, r, fmt.Errorf(format, a...), errCode)
}

func (h *httprunner) writeErrf(w http.ResponseWriter, r *http.Request, format string, a ...interface{}) {
	h.writeErrMsg(w, r, fmt.Sprintf(format, a...))
}

func (h *httprunner) writeErrURL(w http.ResponseWriter, r *http.Request) {
	err := cmn.NewHTTPErr(r, "invalid URL Path")
	h.writeErr(w, r, err)
	cmn.FreeHTTPErr(err)
}

func (h *httprunner) writeErrAct(w http.ResponseWriter, r *http.Request, action string) {
	err := cmn.NewHTTPErr(r, fmt.Sprintf("invalid action %q", action))
	h.writeErr(w, r, err)
	cmn.FreeHTTPErr(err)
}

func (h *httprunner) writeErrActf(w http.ResponseWriter, r *http.Request, action string,
	format string, a ...interface{}) {
	detail := fmt.Sprintf(format, a...)
	err := cmn.NewHTTPErr(r, fmt.Sprintf("invalid action %q: %s", action, detail))
	h.writeErr(w, r, err)
	cmn.FreeHTTPErr(err)
}

////////////////
// callResult //
////////////////

// error helper for intra-cluster calls
func (res *callResult) error() error {
	if res.err == nil {
		return nil
	}
	// cmn.ErrHTTP
	if httpErr := cmn.Err2HTTPErr(res.err); httpErr != nil {
		// NOTE: optionally overwrite status, add details
		if res.status >= http.StatusBadRequest {
			httpErr.Status = res.status
		}
		if httpErr.Message == "" {
			httpErr.Message = res.details
		}
		return httpErr
	}
	// res => cmn.ErrHTTP
	if res.status >= http.StatusBadRequest {
		var detail string
		if res.details != "" {
			detail = "[" + res.details + "]"
		}
		return cmn.NewHTTPErr(nil, fmt.Sprintf("%v%s", res.err, detail), res.status)
	}
	if res.details == "" {
		return res.err
	}
	return fmt.Errorf("%v[%s]", res.err, res.details)
}

func (res *callResult) errorf(format string, a ...interface{}) error {
	debug.Assert(res.err != nil)
	// add formatted
	msg := fmt.Sprintf(format, a...)
	if httpErr := cmn.Err2HTTPErr(res.err); httpErr != nil {
		httpErr.Message = msg + ": " + httpErr.Message
		res.err = httpErr
	} else {
		res.err = errors.New(msg + ": " + res.err.Error())
	}
	return res.error()
}

///////////////////
// health client //
///////////////////

func (h *httprunner) Health(si *cluster.Snode, timeout time.Duration, query url.Values) (b []byte, status int, err error) {
	var (
		path = cmn.URLPathHealth.S
		url  = si.URL(cmn.NetworkIntraControl)
		args = callArgs{
			si:      si,
			req:     cmn.ReqArgs{Method: http.MethodGet, Base: url, Path: path, Query: query},
			timeout: timeout,
		}
	)
	res := h.call(args)
	b, status, err = res.bytes, res.status, res.err
	_freeCallRes(res)
	return
}

// uses provided Smap and an extended version of the Health(clusterInfo) internal API
// to discover a better Smap (ie., more current), if exists
// TODO: for the targets, validate and return maxVerBMD
// NOTE: if `checkAll` is set to false, we stop making further requests once minPidConfirmations is achieved
func (h *httprunner) bcastHealth(smap *smapX, checkAll ...bool) (maxCii *clusterInfo, cnt int) {
	var (
		query          = url.Values{cmn.URLParamClusterInfo: []string{"true"}}
		timeout        = cmn.GCO.Get().Timeout.CplaneOperation
		mu             = &sync.RWMutex{}
		nodemap        = smap.Pmap
		maxConfVersion int64
		retried        bool
	)
	maxCii = &clusterInfo{}
	maxCii.fillSmap(smap)
	debug.Assert(maxCii.Smap.Primary.ID != "" && maxCii.Smap.Version > 0 && smap.isValid())
retry:
	wg := cos.NewLimitedWaitGroup(cluster.MaxBcastParallel(), len(nodemap))
	for sid, si := range nodemap {
		if sid == h.si.ID() {
			continue
		}
		// have enough confirmations?
		if len(nodemap) > cluster.MaxBcastParallel() {
			mu.RLock()
			if len(checkAll) > 0 && !checkAll[1] && cnt >= minPidConfirmations {
				mu.RUnlock()
				break
			}
			mu.RUnlock()
		}
		wg.Add(1)
		go func(si *cluster.Snode) {
			var cii *clusterInfo
			defer wg.Done()
			body, _, err := h.Health(si, timeout, query)
			if err != nil {
				return
			}
			if cii = ciiToSmap(smap, body, h.si, si); cii == nil {
				return
			}
			mu.Lock()
			if maxCii.Smap.Version < cii.Smap.Version {
				// reset the confirmation count iff there's a disagreement on primary ID
				if maxCii.Smap.Primary.ID != cii.Smap.Primary.ID || cii.Smap.VoteInProgress {
					cnt = 1
				} else {
					cnt++
				}
				maxCii = cii
			} else if maxCii.smapEqual(cii) {
				cnt++
			}
			// TODO: Include confidence count for config
			if maxConfVersion < cii.Config.Version {
				maxConfVersion = cii.Config.Version
			}
			mu.Unlock()
		}(si)
	}
	wg.Wait()

	// retry with Tmap when too few confirmations
	if cnt < minPidConfirmations && !retried && smap.CountTargets() > 0 {
		nodemap = smap.Tmap
		retried = true
		goto retry
	}
	glog.Infof("max config version %d", maxConfVersion)
	return
}

func ciiToSmap(smap *smapX, body []byte, self, si *cluster.Snode) *clusterInfo {
	var cii clusterInfo
	if err := jsoniter.Unmarshal(body, &cii); err != nil {
		glog.Errorf("%s: failed to unmarshal clusterInfo, err: %v", self, err)
		return nil
	}
	if smap.UUID != cii.Smap.UUID {
		glog.Errorf("%s: Smap have different UUIDs: %s and %s from %s", self, smap.UUID, cii.Smap.UUID, si)
		return nil
	}
	return &cii
}

//////////////////////////
// metasync Rx handlers //
//////////////////////////

func (h *httprunner) extractConfig(payload msPayload, caller string) (newConfig *globalConfig, msg *aisMsg, err error) {
	if _, ok := payload[revsConfTag]; !ok {
		return
	}
	newConfig, msg = &globalConfig{}, &aisMsg{}
	confValue := payload[revsConfTag]
	if err1 := jsoniter.Unmarshal(confValue, newConfig); err1 != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "new Config", cmn.BytesHead(confValue), err1)
		return
	}
	if msgValue, ok := payload[revsConfTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "action message", cmn.BytesHead(msgValue), err1)
			return
		}
	}

	conf := h.owner.config.get()
	glog.Infof(
		"[metasync] extract %s from %q (local: %s, action: %q, uuid: %q)",
		newConfig, caller, conf, msg.Action, msg.UUID,
	)

	if newConfig.version() <= conf.version() {
		if newConfig.version() < conf.version() {
			err = newErrDowngrade(h.si, conf.String(), newConfig.String())
		}
		newConfig = nil
	}
	return
}

func (h *httprunner) extractSmap(payload msPayload, caller string) (newSmap *smapX, msg *aisMsg, err error) {
	if _, ok := payload[revsSmapTag]; !ok {
		return
	}
	newSmap, msg = &smapX{}, &aisMsg{}
	smapValue := payload[revsSmapTag]
	if err1 := jsoniter.Unmarshal(smapValue, newSmap); err1 != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "new Smap", cmn.BytesHead(smapValue), err1)
		return
	}
	if msgValue, ok := payload[revsSmapTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "action message", cmn.BytesHead(msgValue), err1)
			return
		}
	}
	var (
		smap        = h.owner.smap.get()
		curVer      = smap.version()
		isManualReb = msg.Action == cmn.ActRebalance && msg.Value != nil
	)
	if newSmap.version() == curVer && !isManualReb {
		newSmap = nil
		return
	}
	if !newSmap.isValid() {
		err = fmt.Errorf("%s: %s is invalid: %v", h.si, newSmap, newSmap.validate())
		return
	}
	if !newSmap.isPresent(h.si) {
		err = fmt.Errorf("%s: not finding ourselves in %s", h.si, newSmap)
		return
	}
	if err = smap.validateUUID(newSmap, h.si, nil, caller); err != nil {
		if h.si.IsProxy() {
			cos.Assert(!smap.isPrimary(h.si))
			// cluster integrity error: making exception for non-primary proxies
			glog.Errorf("%s (non-primary): %v - proceeding to override Smap", h.si, err)
			return
		}
		cos.ExitLogf("%v", err) // otherwise, FATAL
	}

	glog.Infof(
		"[metasync] extract %s from %q (local: %s, action: %q, uuid: %q)",
		newSmap.StringEx(), caller, smap.StringEx(), msg.Action, msg.UUID,
	)

	_, sameOrigin, _, eq := smap.Compare(&newSmap.Smap)
	cos.Assert(sameOrigin)
	if newSmap.version() < curVer {
		if !eq {
			err = newErrDowngrade(h.si, smap.StringEx(), newSmap.StringEx())
			return
		}
		glog.Warningf("%s: %s and %s are otherwise identical", h.si, newSmap.StringEx(), smap.StringEx())
		newSmap = nil
	}
	return
}

func (h *httprunner) extractRMD(payload msPayload, caller string) (newRMD *rebMD, msg *aisMsg, err error) {
	if _, ok := payload[revsRMDTag]; !ok {
		return
	}
	newRMD, msg = &rebMD{}, &aisMsg{}
	rmdValue := payload[revsRMDTag]
	if err1 := jsoniter.Unmarshal(rmdValue, newRMD); err1 != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "new RMD", cmn.BytesHead(rmdValue), err1)
		return
	}
	if msgValue, ok := payload[revsRMDTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "action message", cmn.BytesHead(msgValue), err1)
			return
		}
	}

	rmd := h.owner.rmd.get()
	glog.Infof(
		"[metasync] extract %s from %q (local: %s, action: %q, uuid: %q)",
		newRMD.String(), caller, rmd.String(), msg.Action, msg.UUID,
	)

	if newRMD.version() <= rmd.version() {
		if newRMD.version() < rmd.version() {
			err = newErrDowngrade(h.si, rmd.String(), newRMD.String())
		}
		newRMD = nil
	}
	return
}

func (h *httprunner) extractBMD(payload msPayload, caller string) (newBMD *bucketMD, msg *aisMsg, err error) {
	if _, ok := payload[revsBMDTag]; !ok {
		return
	}
	newBMD, msg = &bucketMD{}, &aisMsg{}
	bmdValue := payload[revsBMDTag]
	if err1 := jsoniter.Unmarshal(bmdValue, newBMD); err1 != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, newBMD, cmn.BytesHead(bmdValue), err1)
		return
	}
	if msgValue, ok := payload[revsBMDTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "action message", cmn.BytesHead(msgValue), err1)
			return
		}
	}

	bmd := h.owner.bmd.get()
	glog.Infof(
		"[metasync] extract %s from %q (local: %s, action: %q, uuid: %q)",
		newBMD.StringEx(), caller, bmd.StringEx(), msg.Action, msg.UUID,
	)

	// skip older iff not transactional - see t.receiveBMD()
	if h.si.IsTarget() && msg.UUID != "" {
		return
	}
	if newBMD.version() <= bmd.version() {
		if newBMD.version() < bmd.version() {
			err = newErrDowngrade(h.si, bmd.StringEx(), newBMD.StringEx())
		}
		newBMD = nil
	}
	return
}

func (h *httprunner) receiveConfig(newConfig *globalConfig, msg *aisMsg, caller string) (err error) {
	glog.Infof(
		"[metasync] receive %s from %q (action: %q, uuid: %q)",
		newConfig, caller, msg.Action, msg.UUID,
	)
	h.owner.config.Lock()
	defer h.owner.config.Unlock()

	conf := h.owner.config.get()
	if newConfig.version() <= conf.version() {
		return newErrDowngrade(h.si, conf.String(), newConfig.String())
	}

	if err = h.owner.config.persist(newConfig); err != nil {
		return
	}

	err = h.owner.config.updateGCO(newConfig)
	debug.AssertNoErr(err)
	return
}

func (h *httprunner) extractRevokedTokenList(payload msPayload, caller string) (*TokenList, error) {
	var (
		msg       aisMsg
		bytes, ok = payload[revsTokenTag]
	)
	if !ok {
		return nil, nil
	}
	if msgValue, ok := payload[revsTokenTag+revsActionTag]; ok {
		if err := jsoniter.Unmarshal(msgValue, &msg); err != nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "action message", cmn.BytesHead(msgValue), err)
			return nil, err
		}
	}

	tokenList := &TokenList{}
	if err := jsoniter.Unmarshal(bytes, tokenList); err != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "blocked token list", cmn.BytesHead(bytes), err)
		return nil, err
	}

	glog.Infof(
		"[metasync] extract token list from %q (count: %d, action: %q, uuid: %q)",
		caller, len(tokenList.Tokens), msg.Action, msg.UUID,
	)

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
// - first, there's the primary proxy/gateway referenced by the current cluster map
//   or - during the cluster deployment time - by the the configured "primary_url"
//   (see /deploy/dev/local/aisnode_config.sh)
// - if that one fails, the new node goes ahead and tries the alternatives:
//  * config.Proxy.PrimaryURL   ("primary_url")
//  * config.Proxy.DiscoveryURL ("discovery_url")
//  * config.Proxy.OriginalURL  ("original_url")
// - if these fails we try the candidates provided by the caller.
//
// ================================== Background =========================================
func (h *httprunner) join(query url.Values, contactURLs ...string) (res *callResult) {
	var (
		config                   = cmn.GCO.Get()
		candidates               = make([]string, 0, 4+len(contactURLs))
		selfPublicURL, pubValid  = cos.ParseURL(h.si.URL(cmn.NetworkPublic))
		selfIntraURL, intraValid = cos.ParseURL(h.si.URL(cmn.NetworkIntraControl))
		addCandidate             = func(url string) {
			if u, valid := cos.ParseURL(url); !valid ||
				u.Host == selfPublicURL.Host ||
				u.Host == selfIntraURL.Host {
				return
			}
			if cos.StringInSlice(url, candidates) {
				return
			}
			candidates = append(candidates, url)
		}
	)
	debug.Assert(pubValid && intraValid)
	primaryURL, psi := h.getPrimaryURLAndSI()
	addCandidate(primaryURL)
	// NOTE: The url above is either config or the primary's IntraControlNet.DirectURL;
	//  Add its public URL as "more static" in various virtualized environments.
	if psi != nil {
		addCandidate(psi.URL(cmn.NetworkPublic))
	}
	addCandidate(config.Proxy.PrimaryURL)
	addCandidate(config.Proxy.DiscoveryURL)
	addCandidate(config.Proxy.OriginalURL)
	for _, u := range contactURLs {
		addCandidate(u)
	}

	for i := 0; i < 2; i++ {
		for _, candidateURL := range candidates {
			if daemon.stopping.Load() {
				return
			}
			res = h.registerToURL(candidateURL, nil, cmn.DefaultTimeout, query, false)
			if res.err == nil {
				glog.Infof("%s: joined cluster via %s", h.si, candidateURL)
				return
			}
		}
		time.Sleep(10 * time.Second)
	}

	smap := h.owner.smap.get()
	if smap.validate() != nil {
		return
	}

	// Failed to join cluster using config, try getting primary URL using existing smap.
	cii, _ := h.bcastHealth(smap, false /*checkAll*/)
	if cii.Smap.Version < smap.version() {
		return
	}
	primaryURL = cii.Smap.Primary.PubURL

	// Daemon is stopping skip register
	if daemon.stopping.Load() {
		return
	}
	res = h.registerToURL(primaryURL, nil, cmn.DefaultTimeout, query, false)
	if res.err == nil {
		glog.Infof("%s: joined cluster via %s", h.si, primaryURL)
	}
	return
}

func (h *httprunner) registerToURL(url string, psi *cluster.Snode, tout time.Duration, query url.Values,
	keepalive bool) *callResult {
	var (
		path   string
		regReq = nodeRegMeta{SI: h.si}
	)
	if h.si.IsTarget() && !keepalive {
		regReq.BMD = h.owner.bmd.get()
		regReq.Smap = h.owner.smap.get()
		glob := xreg.GetRebMarked()
		regReq.Reb = glob.Interrupted
	}
	info := cos.MustMarshal(regReq)
	if keepalive {
		path = cmn.URLPathClusterKalive.S
	} else {
		path = cmn.URLPathClusterAutoReg.S
	}
	callArgs := callArgs{
		si:      psi,
		req:     cmn.ReqArgs{Method: http.MethodPost, Base: url, Path: path, Query: query, Body: info},
		timeout: tout,
	}
	return h.call(callArgs)
}

func (h *httprunner) sendKeepalive(timeout time.Duration) (status int, err error) {
	if daemon.stopping.Load() {
		err = fmt.Errorf("%s is stopping", h.si)
		return
	}
	primaryURL, psi := h.getPrimaryURLAndSI()
	res := h.registerToURL(primaryURL, psi, timeout, nil, true)
	if res.err != nil {
		if strings.Contains(res.err.Error(), ciePrefix) {
			cos.ExitLogf("%v", res.err) // FATAL: cluster integrity error (cie)
		}
		status, err = res.status, res.err
		_freeCallRes(res)
		return
	}
	debug.Assert(len(res.bytes) == 0)
	_freeCallRes(res)
	return
}

func (h *httprunner) getPrimaryURLAndSI() (url string, psi *cluster.Snode) {
	smap := h.owner.smap.get()
	if smap.validate() != nil {
		url, psi = cmn.GCO.Get().Proxy.PrimaryURL, nil
		return
	}
	url, psi = smap.Primary.URL(cmn.NetworkIntraControl), smap.Primary
	return
}

func (h *httprunner) pollClusterStarted(timeout time.Duration) {
	for i := 1; ; i++ {
		time.Sleep(time.Duration(i) * time.Second)
		smap := h.owner.smap.get()
		if smap.validate() != nil {
			continue
		}
		if _, _, err := h.Health(smap.Primary, timeout, nil); err == nil {
			break
		}
	}
}

func (h *httprunner) unregisterSelf(ignoreErr bool) (err error) {
	var status int
	smap := h.owner.smap.get()
	if smap == nil || smap.validate() != nil {
		return
	}
	args := callArgs{
		si:      smap.Primary,
		req:     cmn.ReqArgs{Method: http.MethodDelete, Path: cmn.URLPathClusterDaemon.Join(h.si.ID())},
		timeout: cmn.DefaultTimeout,
	}
	res := h.call(args)
	status, err = res.status, res.err
	if err != nil {
		f := glog.Errorf
		if ignoreErr {
			f = glog.Warningf
		}
		f("%s: failed to unreg self, err: %v(%d)", h.si, err, status)
	}
	_freeCallRes(res)
	return
}

func (h *httprunner) healthByExternalWD(w http.ResponseWriter, r *http.Request) (responded bool) {
	callerID := r.Header.Get(cmn.HeaderCallerID)
	caller := r.Header.Get(cmn.HeaderCallerName)
	// external (i.e. not intra-cluster) call
	if callerID == "" && caller == "" {
		readiness := cos.IsParseBool(r.URL.Query().Get(cmn.URLParamHealthReadiness))
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s: external health-ping from %s", h.si, r.RemoteAddr)
		}
		// respond with 503 as per https://tools.ietf.org/html/rfc7231#section-6.6.4
		// see also:
		// https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes
		if !readiness && !h.ClusterStarted() {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		// TODO: establish the fact that we are healthy...
		return true
	}

	debug.Assert(isIntraCall(r.Header))

	// Intra cluster health ping
	if !h.ensureIntraControl(w, r) {
		responded = true
	}
	return
}

// Determine if the request is intra-control. For now, using the http.Server handling the request.
// TODO: Add other checks based on request e.g. `r.RemoteAddr`
func (h *httprunner) ensureIntraControl(w http.ResponseWriter, r *http.Request) (isIntra bool) {
	// When `config.UseIntraControl` is `false`, intra-control net is same as public net.
	if !cmn.GCO.Get().HostNet.UseIntraControl {
		return true
	}

	intraAddr := h.si.IntraControlNet.TCPEndpoint()
	srvAddr := r.Context().Value(http.ServerContextKey).(*http.Server).Addr
	if srvAddr == intraAddr {
		return true
	}

	h.writeErrf(w, r, "%s: expected %s request", h.si, cmn.NetworkIntraControl)
	return
}

func (h *httprunner) bucketPropsToHdr(bck *cluster.Bck, hdr http.Header) {
	if bck.Props == nil {
		bck.Props = defaultBckProps(bckPropsArgs{bck: bck, hdr: hdr})
	}

	finalProps := bck.Props.Clone()
	cmn.IterFields(finalProps, func(fieldName string, field cmn.IterField) (error, bool) {
		if fieldName == cmn.PropBucketVerEnabled {
			if hdr.Get(cmn.HeaderBucketVerEnabled) == "" {
				verEnabled := field.Value().(bool)
				hdr.Set(cmn.HeaderBucketVerEnabled, strconv.FormatBool(verEnabled))
			}
			return nil, false
		} else if fieldName == cmn.PropBucketCreated {
			created := time.Unix(0, field.Value().(int64))
			hdr.Set(cmn.HeaderBucketCreated, created.Format(time.RFC3339))
		}

		hdr.Set(fieldName, fmt.Sprintf("%v", field.Value()))
		return nil, false
	})

	props := string(cos.MustMarshal(finalProps))
	hdr.Set(cmn.HeaderBucketProps, props)
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
	var (
		provider  = query.Get(cmn.URLParamProvider)
		namespace = cmn.ParseNsUname(query.Get(cmn.URLParamNamespace))
		bck       = cmn.Bck{Name: bckName, Provider: provider, Ns: namespace}
	)
	if err := bck.Validate(); err != nil {
		return nil, err
	}
	return cluster.NewBckEmbed(bck), nil
}

func newQueryBcksFromQuery(query url.Values) (cmn.QueryBcks, error) {
	var (
		provider  = query.Get(cmn.URLParamProvider)
		namespace = cmn.ParseNsUname(query.Get(cmn.URLParamNamespace))
		bck       = cmn.QueryBcks{Provider: provider, Ns: namespace}
	)
	if bck.Provider != "" {
		if err := bck.ValidateProvider(); err != nil {
			return bck, err
		}
	}
	return bck, nil
}

func newBckFromQueryUname(query url.Values, uparam string) (*cluster.Bck, error) {
	uname := query.Get(uparam)
	if uname == "" {
		return nil, nil
	}
	bck, objName := cmn.ParseUname(uname)
	if objName != "" {
		return nil, fmt.Errorf("bucket %s: unexpected non-empty object name %q", bck, objName)
	}
	if err := bck.Validate(); err != nil {
		return nil, err
	}
	return cluster.NewBckEmbed(bck), nil
}

////////////////////
// aisMsg helpers //
////////////////////

func (h *httprunner) newAmsgStr(msgStr string, smap *smapX, bmd *bucketMD) *aisMsg {
	return h.newAmsg(&cmn.ActionMsg{Value: msgStr}, smap, bmd)
}

func (h *httprunner) newAmsgActVal(act string, val interface{}, smap *smapX) *aisMsg {
	return h.newAmsg(&cmn.ActionMsg{Action: act, Value: val}, smap, nil)
}

func (h *httprunner) newAmsg(actionMsg *cmn.ActionMsg, smap *smapX, bmd *bucketMD, uuid ...string) *aisMsg {
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

// Based on default error handler `defaultErrorHandler` in `httputil/reverseproxy.go`.
func (p *proxyrunner) rpErrHandler(w http.ResponseWriter, r *http.Request, err error) {
	msg := fmt.Sprintf("%s: rproxy err %v, req: %s %s", p.si, err, r.Method, r.URL.Path)
	if caller := r.Header.Get(cmn.HeaderCallerName); caller != "" {
		msg += " (from " + caller + ")"
	}
	glog.Errorln(msg)
	w.WriteHeader(http.StatusBadGateway)
}

/////////////////
// clusterInfo //
/////////////////

func (cii *clusterInfo) fill(h *httprunner) {
	var (
		bmd    = h.owner.bmd.get()
		smap   = h.owner.smap.get()
		config = h.owner.config.get()
	)
	cii.BMD.Version = bmd.version()
	cii.BMD.UUID = bmd.UUID
	cii.fillSmap(smap)
	cii.Config.Version = config.version()
}

func (cii *clusterInfo) fillSmap(smap *smapX) {
	cii.Smap.Version = smap.version()
	cii.Smap.UUID = smap.UUID
	cii.Smap.Primary.CtrlURL = smap.Primary.URL(cmn.NetworkIntraControl)
	cii.Smap.Primary.PubURL = smap.Primary.URL(cmn.NetworkPublic)
	cii.Smap.Primary.ID = smap.Primary.ID()
	xact := xreg.GetXactRunning(cmn.ActElection)
	cii.Smap.VoteInProgress = xact != nil
}

func (cii *clusterInfo) smapEqual(other *clusterInfo) (ok bool) {
	if cii == nil || other == nil {
		return false
	}
	return cii.Smap.Version == other.Smap.Version && cii.Smap.Primary.ID == other.Smap.Primary.ID
}
