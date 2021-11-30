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
	"log"
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
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xreg"
	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/tinylib/msgp/msgp"
)

const unknownDaemonID = "unknown"

const msgpObjListBufSize = 32 * cos.KiB

const (
	fmtErrInsuffMpaths1 = "%s: not enough mountpaths (%d) to configure %s as %d-way mirror"
	fmtErrInsuffMpaths2 = "%s: not enough mountpaths (%d) to replicate %s (configured) %d times"

	fmtErrPrimaryNotReadyYet = "%s primary is not ready yet to start rebalance (started=%t, starting-up=%t)"

	fmtErrInvaldAction = "invalid action %q (expected one of %v)"
)

type (
	rangesQuery struct {
		Range string // cmn.HdrRange, see https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
		Size  int64  // size, in bytes
	}

	archiveQuery struct {
		filename string // pathname in archive
		mime     string // https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types
	}

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

	// cluster-wide control information - replicated, versioned, and synchronized
	// usages: primary election, join-cluster
	cluMeta struct {
		Smap           *smapX         `json:"smap"`
		BMD            *bucketMD      `json:"bmd"`
		RMD            *rebMD         `json:"rmd"`
		Config         *globalConfig  `json:"config"`
		SI             *cluster.Snode `json:"si"`
		RebInterrupted bool           `json:"reb_interrupted"`
		VoteInProgress bool           `json:"voting"`
	}
	nodeRegPool []cluMeta

	// what data to omit when sending request/response (join-cluster, kalive)
	cmetaFillOpt struct {
		skipSmap      bool
		skipBMD       bool
		skipRMD       bool
		skipConfig    bool
		fillRebMarker bool
	}

	// node <=> node cluster-info exchange
	clusterInfo struct {
		Smap struct {
			Version int64  `json:"version,string"`
			UUID    string `json:"uuid"`
			Primary struct {
				PubURL  string `json:"pub_url"`
				CtrlURL string `json:"control_url"`
				ID      string `json:"id"`
			}
		} `json:"smap"`
		BMD struct {
			Version int64  `json:"version,string"`
			UUID    string `json:"uuid"`
		} `json:"bmd"`
		RMD struct {
			Version int64 `json:"version,string"`
		} `json:"rmd"`
		Config struct {
			Version int64 `json:"version,string"`
		} `json:"config"`
		Flags struct {
			VoteInProgress bool `json:"vote_in_progress"`
			ClusterStarted bool `json:"cluster_started"`
			NodeStarted    bool `json:"node_started"`
		} `json:"flags"`
	}

	electable interface {
		proxyElection(vr *VoteRecord)
	}

	// aisMsg is an extended ActionMsg with extra information for node <=> node control plane communications
	aisMsg struct {
		cmn.ActionMsg
		BMDVersion int64  `json:"bmdversion,string"`
		RMDVersion int64  `json:"rmdversion,string"`
		UUID       string `json:"uuid"` // cluster-wide ID of this action (operation, transaction)
	}

	// http server and http runner (common for proxy and target)
	netServer struct {
		sync.Mutex
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
			node    atomic.Bool // determines time when the node started up
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

	errNoUnregister struct {
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
		query  url.Values   // r.URL.Query()
	}
)

var allHTTPverbs = []string{
	http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
	http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodTrace,
}

var (
	errRebalanceDisabled = errors.New("rebalance is disabled")
	errForwarded         = errors.New("forwarded")
	errSendingResp       = errors.New("err-sending-resp")
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

//////////////////////
// errNoUnregister //
////////////////////

func (e *errNoUnregister) Error() string { return e.detail }

func isErrNoUnregister(err error) bool {
	if _, ok := err.(*errNoUnregister); ok {
		return true
	}
	enu := &errNoUnregister{}
	return errors.As(err, &enu)
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

func (*glogWriter) Write(p []byte) (int, error) {
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
	var (
		httpHandler http.Handler = server.muxers
		config                   = cmn.GCO.Get()
	)
	server.Lock()
	server.s = &http.Server{
		Addr:     addr,
		Handler:  httpHandler,
		ErrorLog: logger,
	}
	if server.sndRcvBufSize > 0 && !config.Net.HTTP.UseHTTPS {
		server.s.ConnState = server.connStateListener // setsockopt; see also cmn.NewTransport
	}
	server.Unlock()
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
	server.Lock()
	defer server.Unlock()
	if server.s == nil {
		return
	}
	config := cmn.GCO.Get()
	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout.MaxHostBusy.D())
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

func (h *httprunner) parseReq(w http.ResponseWriter, r *http.Request, args *apiRequest) (err error) {
	debug.Assert(len(args.prefix) != 0)
	args.items, err = h.checkRESTItems(w, r, args.after, false, args.prefix)
	if err != nil {
		return
	}
	debug.Assert(len(args.items) > args.bckIdx)
	bckName := args.items[args.bckIdx]
	args.query = r.URL.Query()
	args.bck, err = newBckFromQuery(bckName, args.query)
	if err != nil {
		h.writeErr(w, r, err)
		return
	}
	if args.msg != nil {
		err = cmn.ReadJSON(w, r, args.msg)
	}
	return
}

func (h *httprunner) cluMeta(opts cmetaFillOpt) (*cluMeta, error) {
	xele := xreg.GetXactRunning(cmn.ActElection)
	cm := &cluMeta{
		SI:             h.si,
		VoteInProgress: xele != nil,
	}
	if !opts.skipConfig {
		var err error
		cm.Config, err = h.owner.config.get()
		if err != nil {
			return nil, err
		}
	}
	// don't send Smap when it is undergoing changes (and is about to get metasync-ed)
	if !opts.skipSmap {
		cm.Smap = h.owner.smap.get()
	}
	if !opts.skipBMD {
		cm.BMD = h.owner.bmd.get()
	}
	if !opts.skipRMD {
		cm.RMD = h.owner.rmd.get()
	}
	if h.si.IsTarget() && opts.fillRebMarker {
		rebMarked := xreg.GetRebMarked()
		cm.RebInterrupted = rebMarked.Interrupted
	}
	return cm, nil
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

func (h *httprunner) ClusterStarted() bool { return h.startup.cluster.Load() }
func (h *httprunner) markClusterStarted()  { h.startup.cluster.Store(true) }

func (h *httprunner) NodeStarted() bool { return h.startup.node.Load() }
func (h *httprunner) markNodeStarted()  { h.startup.node.Store(true) }

func (h *httprunner) registerNetworkHandlers(networkHandlers []networkHandler) {
	var (
		path   string
		config = cmn.GCO.Get()
	)
	// common, debug
	for r, nh := range debug.Handlers() {
		h.registerPublicNetHandler(r, nh)
	}
	// node type specific
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
	// common Prometheus
	if h.statsT.IsPrometheus() {
		nh := networkHandler{r: "/" + cmn.Metrics, h: promhttp.Handler().ServeHTTP}
		path := nh.r // absolute
		h.registerPublicNetHandler(path, nh.h)
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
	const (
		defaultControlWriteBufferSize = 16 * cos.KiB // for more defaults see cmn/network.go
		defaultControlReadBufferSize  = 16 * cos.KiB
	)
	h.client.control = cmn.NewClient(cmn.TransportArgs{
		Timeout:         config.Client.Timeout.D(),
		WriteBufferSize: defaultControlWriteBufferSize,
		ReadBufferSize:  defaultControlReadBufferSize,
		UseHTTPS:        config.Net.HTTP.UseHTTPS,
		SkipVerify:      config.Net.HTTP.SkipVerify,
	})
	wbuf, rbuf := config.Net.HTTP.WriteBufferSize, config.Net.HTTP.ReadBufferSize
	// NOTE: when not configured use AIS defaults (to override the usual 4KB)
	if wbuf == 0 {
		wbuf = cmn.DefaultWriteBufferSize
	}
	if rbuf == 0 {
		rbuf = cmn.DefaultReadBufferSize
	}
	h.client.data = cmn.NewClient(cmn.TransportArgs{
		Timeout:         config.Client.TimeoutLong.D(),
		WriteBufferSize: wbuf,
		ReadBufferSize:  rbuf,
		UseHTTPS:        config.Net.HTTP.UseHTTPS,
		SkipVerify:      config.Net.HTTP.SkipVerify,
	})

	tcpbuf := config.Net.L4.SndRcvBufSize
	if h.si.IsProxy() {
		tcpbuf = 0
	} else if tcpbuf == 0 {
		tcpbuf = cmn.DefaultSendRecvBufferSize // ditto: targets use AIS default when not configured
	}

	muxers := newMuxers()
	h.netServ.pub = &netServer{muxers: muxers, sndRcvBufSize: tcpbuf}
	h.netServ.control = h.netServ.pub // if not separately configured, intra-control net is public
	if config.HostNet.UseIntraControl {
		muxers = newMuxers()
		h.netServ.control = &netServer{muxers: muxers, sndRcvBufSize: 0}
	}
	h.netServ.data = h.netServ.control // if not configured, intra-data net is intra-control
	if config.HostNet.UseIntraData {
		muxers = newMuxers()
		h.netServ.data = &netServer{muxers: muxers, sndRcvBufSize: tcpbuf}
	}

	h.owner.smap = newSmapOwner(config)
	h.owner.rmd = newRMDOwner()
	h.owner.rmd.load()

	h.gmm = memsys.PageMM()
	h.gmm.RegWithHK()
	h.smm = memsys.ByteMM()
	h.smm.RegWithHK()
}

func newMuxers() cmn.HTTPMuxers {
	m := make(cmn.HTTPMuxers, len(allHTTPverbs))
	for _, v := range allHTTPverbs {
		m[v] = mux.NewServeMux()
	}
	return m
}

func (h *httprunner) initNetworks() {
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
		icport := strconv.Itoa(config.HostNet.PortIntraControl)
		intraControlAddr, err = getNetInfo(addrList, proto, config.HostNet.HostnameIntraControl, icport)
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
		idport := strconv.Itoa(config.HostNet.PortIntraData)
		intraDataAddr, err = getNetInfo(addrList, proto, config.HostNet.HostnameIntraData, idport)
		if err != nil {
			cos.ExitLogf("Failed to get %s IPv4/hostname: %v", cmn.NetworkIntraData, err)
		}
		s = ""
		if config.HostNet.HostnameIntraData != "" {
			s = " (config: " + config.HostNet.HostnameIntraData + ")"
		}
		glog.Infof("%s access: [%s]%s", cmn.NetworkIntraData, intraDataAddr, s)
	}
	mustDiffer(pubAddr,
		config.HostNet.Port,
		true,
		intraControlAddr,
		config.HostNet.PortIntraControl,
		config.HostNet.UseIntraControl,
		"pub/ctl",
	)
	mustDiffer(pubAddr,
		config.HostNet.Port,
		true,
		intraDataAddr,
		config.HostNet.PortIntraData,
		config.HostNet.UseIntraData,
		"pub/data",
	)
	mustDiffer(intraDataAddr,
		config.HostNet.PortIntraData,
		config.HostNet.UseIntraData,
		intraControlAddr,
		config.HostNet.PortIntraControl,
		config.HostNet.UseIntraControl,
		"ctl/data",
	)
	h.si = &cluster.Snode{
		PublicNet:       pubAddr,
		IntraControlNet: intraControlAddr,
		IntraDataNet:    intraDataAddr,
	}
}

func mustDiffer(ip1 cluster.NetInfo, port1 int, use1 bool, ip2 cluster.NetInfo, port2 int, use2 bool, tag string) {
	if !use1 || !use2 {
		return
	}
	if ip1.NodeHostname == ip2.NodeHostname && port1 == port2 {
		cos.ExitLogf("%s: cannot use the same IP:port (%s) for two networks", tag, ip1)
	}
}

// detectNodeChanges is called at startup. Given loaded Smap, it checks whether
// this node ID is present. NOTE: we are _not_ enforcing node's (`h.si`)
// immutability - in particular, the node's IPs that, in fact, may change upon
// restart in certain environments.
func (h *httprunner) detectNodeChanges(smap *smapX) (err error) {
	if smap.GetNode(h.si.ID()) == nil {
		err = fmt.Errorf("%s: not present in the loaded %s", h.si, smap)
	}
	return
}

func (h *httprunner) tryLoadSmap() (_ *smapX, reliable bool) {
	var (
		smap        = newSmap()
		loaded, err = h.owner.smap.load(smap)
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

func (h *httprunner) setDaemonConfigMsg(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg) {
	transient := cos.IsParseBool(r.URL.Query().Get(cmn.ActTransient))
	toUpdate := &cmn.ConfigToUpdate{}
	if err := cos.MorphMarshal(msg.Value, toUpdate); err != nil {
		h.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, h.si, msg.Action, msg.Value, err)
		return
	}

	err := h.owner.config.setDaemonConfig(toUpdate, transient)
	if err != nil {
		h.writeErr(w, r, err)
	}
}

func (h *httprunner) setDaemonConfigQuery(w http.ResponseWriter, r *http.Request) {
	var (
		query     = r.URL.Query()
		transient = cos.IsParseBool(query.Get(cmn.ActTransient))
		toUpdate  = &cmn.ConfigToUpdate{}
	)
	if err := toUpdate.FillFromQuery(query); err != nil {
		h.writeErr(w, r, err)
		return
	}
	err := h.owner.config.setDaemonConfig(toUpdate, transient)
	if err != nil {
		h.writeErr(w, r, err)
	}
}

func (h *httprunner) run() error {
	config := cmn.GCO.Get()

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
			addr := h.pubListeningAddr(config)
			errCh <- h.netServ.pub.listenAndServe(addr, h.logger)
		}()
		return <-errCh
	}

	addr := h.pubListeningAddr(config)
	return h.netServ.pub.listenAndServe(addr, h.logger)
}

// testing environment excluding Kubernetes: listen on `host:port`
// otherwise (including production):         listen on `*:port`
func (h *httprunner) pubListeningAddr(config *cmn.Config) string {
	var (
		testingEnv  = config.TestingEnv()
		k8sDetected = k8s.Detect() == nil
	)
	if testingEnv && !k8sDetected {
		return h.si.PublicNet.TCPEndpoint()
	}
	return ":" + h.si.PublicNet.DaemonPort
}

func (h *httprunner) stopHTTPServer() {
	h.netServ.pub.shutdown()
	config := cmn.GCO.Get()
	if config.HostNet.UseIntraControl {
		h.netServ.control.shutdown()
	}
	if config.HostNet.UseIntraData {
		h.netServ.data.shutdown()
	}
}

// on success returns `cmn.ActValRmNode` options; on failure writes http error
func (h *httprunner) parseUnregMsg(w http.ResponseWriter, r *http.Request) (*cmn.ActValRmNode, string, error) {
	var (
		msg  cmn.ActionMsg
		opts cmn.ActValRmNode
	)
	if err := cmn.ReadJSON(w, r, &msg, true /*optional*/); err != nil {
		return nil, "", err
	}
	// NOTE: `cmn.ActValRmNode` options are currently supported only by ais targets
	//       and only when decommissioning
	if msg.Action != cmn.ActDecommissionNode || h.si.IsProxy() {
		return nil, msg.Action, nil
	}
	if err := cos.MorphMarshal(msg.Value, &opts); err != nil {
		h.writeErr(w, r, err)
		return nil, msg.Action, err
	}
	return &opts, msg.Action, nil
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
		h.stopHTTPServer()
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
			config := cmn.GCO.Get()
			args.timeout = config.Timeout.CplaneOperation.D()
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

	req.Header.Set(cmn.HdrCallerID, h.si.ID())
	req.Header.Set(cmn.HdrCallerName, h.si.Name())
	if smap := h.owner.smap.get(); smap != nil && smap.vstr != "" {
		req.Header.Set(cmn.HdrCallerSmapVersion, smap.vstr)
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
			msg := resp.Header.Get(cmn.HdrError)
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
			buf, slab := h.gmm.AllocSize(msgpObjListBufSize)
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
		res.bytes, res.err = io.ReadAll(resp.Body)
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
			if smap.IsIC(psi) && pid != h.si.ID() && !psi.IsAnySet(cluster.NodeFlagsMaintDecomm) {
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
	config := cmn.GCO.Get()
	path := cmn.URLPathNotifs.Join(kind)
	args.req = cmn.ReqArgs{Method: http.MethodPost, Path: path, Body: cos.MustMarshal(&msg)}
	args.network = cmn.NetworkIntraControl
	args.timeout = config.Timeout.MaxKeepalive.D()
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
		config := cmn.GCO.Get()
		args.timeout = config.Timeout.CplaneOperation.D()
		debug.Assert(args.timeout != 0)
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
				if !bargs.ignoreMaintenance && si.IsAnySet(cluster.NodeFlagsMaintDecomm) {
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
		wg     = &sync.WaitGroup{}
		smap   = h.owner.smap.get()
		args   = allocBcastArgs()
		config = cmn.GCO.Get()
	)
	args.req = cmn.ReqArgs{Method: http.MethodPost, Path: cmn.URLPathIC.S, Body: cos.MustMarshal(msg)}
	args.network = cmn.NetworkIntraControl
	args.timeout = config.Timeout.MaxKeepalive.D()
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

// bcastReqGroup broadcasts a ReqArgs to a specific group of nodes
func (h *httprunner) bcastReqGroup(w http.ResponseWriter, r *http.Request, req cmn.ReqArgs, to int) {
	args := allocBcastArgs()
	args.req = req
	args.to = to
	results := h.bcastGroup(args)
	freeBcastArgs(args)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		h.writeErr(w, r, res.error())
		freeCallResults(results)
		return
	}
	freeCallResults(results)
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
		buf, slab = h.gmm.AllocSize(msgpObjListBufSize)
		mw        = msgp.NewWriterBuf(w, buf)
	)
	defer slab.Free(buf)

	w.Header().Set(cmn.HdrContentType, cmn.ContentMsgPack)
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
	debug.Assert(!isByteArray)
	ok = true
	w.Header().Set(cmn.HdrContentType, cmn.ContentJSON)
	if err := jsoniter.NewEncoder(w).Encode(v); err != nil {
		h.handleWriteError(r, tag, err)
		ok = false
	}
	return
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

func _parseNCopies(value interface{}) (copies int64, err error) {
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

func _checkAction(msg *cmn.ActionMsg, expectedActions ...string) (err error) {
	found := false
	for _, action := range expectedActions {
		found = found || msg.Action == action
	}
	if !found {
		err = fmt.Errorf(fmtErrInvaldAction, msg.Action, expectedActions)
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
		var err error
		body, err = h.cluMeta(cmetaFillOpt{})
		if err != nil {
			glog.Errorf("failed to fetch cluster config, err: %v", err)
		}
	case cmn.GetWhatSnode:
		body = h.si
	case cmn.GetWhatLog:
		log, err := _sev2logname(r)
		if err != nil {
			h.writeErr(w, r, err)
			return
		}
		file, err := os.Open(log)
		if err != nil {
			errCode := http.StatusInternalServerError
			if os.IsNotExist(err) {
				errCode = http.StatusNotFound
			}
			h.writeErr(w, r, err, errCode)
			return
		}
		buf, slab := h.gmm.Alloc()
		if written, err := io.CopyBuffer(w, file, buf); err != nil {
			// at this point, http err must be already on its way
			glog.Errorf("failed to read %s: %v (written=%d)", log, err, written)
		}
		cos.Close(file)
		slab.Free(buf)
		return
	case cmn.GetWhatStats:
		body = h.statsT.GetWhatStats()
	default:
		h.writeErrf(w, r, "invalid GET /daemon request: unrecognized what=%s", what)
		return
	}
	h.writeJSON(w, r, body, "httpdaeget-"+what)
}

func _sev2logname(r *http.Request) (log string, err error) {
	dir := cmn.GCO.Get().LogDir
	sev := r.URL.Query().Get(cmn.URLParamSev)
	if sev == "" {
		log = filepath.Join(dir, glog.InfoLogName()) // symlink
		return
	}
	v := strings.ToLower(sev)
	switch v[0] {
	case cmn.LogInfo[0]:
		log = filepath.Join(dir, glog.InfoLogName())
	case cmn.LogWarn[0]:
		log = filepath.Join(dir, glog.WarnLogName())
	case cmn.LogErr[0]:
		log = filepath.Join(dir, glog.ErrLogName())
	default:
		err = fmt.Errorf("unknown log severity %q", sev)
	}
	return
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
	err := cmn.NewErrHTTP(r, "invalid URL Path")
	h.writeErr(w, r, err)
	cmn.FreeHTTPErr(err)
}

func (h *httprunner) writeErrAct(w http.ResponseWriter, r *http.Request, action string) {
	err := cmn.NewErrHTTP(r, fmt.Sprintf("invalid action %q", action))
	h.writeErr(w, r, err)
	cmn.FreeHTTPErr(err)
}

func (h *httprunner) writeErrActf(w http.ResponseWriter, r *http.Request, action string,
	format string, a ...interface{}) {
	detail := fmt.Sprintf(format, a...)
	err := cmn.NewErrHTTP(r, fmt.Sprintf("invalid action %q: %s", action, detail))
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
		return cmn.NewErrHTTP(nil, fmt.Sprintf("%v%s", res.err, detail), res.status)
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

/////////////////
// clusterInfo //
/////////////////

func (cii *clusterInfo) String() string { return fmt.Sprintf("%+v", *cii) }

// uses provided Smap and an extended version of the Health(clusterInfo) internal API
// to discover a better Smap (ie., more current), if exists
// TODO: for the targets, validate and return maxVerBMD
// NOTE: if `checkAll` is set to false, we stop making further requests once minPidConfirmations is achieved
func (h *httprunner) bcastHealth(smap *smapX, checkAll ...bool) (maxCii *clusterInfo, cnt int) {
	var (
		query          = url.Values{cmn.URLParamClusterInfo: []string{"true"}}
		config         = cmn.GCO.Get()
		timeout        = config.Timeout.CplaneOperation.D()
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
				if maxCii.Smap.Primary.ID != cii.Smap.Primary.ID || cii.Flags.VoteInProgress {
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

func _msdetail(ver int64, msg *aisMsg, caller string) (d string) {
	if caller != "" {
		d = " from " + caller
	}
	if msg.Action != "" || msg.UUID != "" {
		d += fmt.Sprintf(" (local: v%d, action: %q, uuid: %q)", ver, msg.Action, msg.UUID)
	} else {
		d += fmt.Sprintf(" (local: v%d)", ver)
	}
	return
}

func (h *httprunner) extractConfig(payload msPayload, caller string) (newConfig *globalConfig, msg *aisMsg, err error) {
	if _, ok := payload[revsConfTag]; !ok {
		return
	}
	newConfig, msg = &globalConfig{}, &aisMsg{}
	confValue := payload[revsConfTag]
	reader := bytes.NewBuffer(confValue)
	if _, err1 := jsp.Decode(io.NopCloser(reader), newConfig, newConfig.JspOpts(), "extractConfig"); err1 != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "new Config", cmn.BytesHead(confValue), err1)
		return
	}
	if msgValue, ok := payload[revsConfTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "action message", cmn.BytesHead(msgValue), err1)
			return
		}
	}
	config := cmn.GCO.Get()
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("extract %s%s", newConfig, _msdetail(config.Version, msg, caller))
	}
	if newConfig.version() <= config.Version {
		if newConfig.version() < config.Version {
			err = newErrDowngrade(h.si, config.String(), newConfig.String())
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
	reader := bytes.NewBuffer(smapValue)
	if _, err1 := jsp.Decode(io.NopCloser(reader), newSmap, newSmap.JspOpts(), "extractSmap"); err1 != nil {
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
	if err = smap.validateUUID(h.si, newSmap, caller, 50 /* ciError */); err != nil {
		return // FATAL: cluster integrity error
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("extract %s%s", newSmap, _msdetail(smap.Version, msg, caller))
	}
	_, sameOrigin, _, eq := smap.Compare(&newSmap.Smap)
	debug.Assert(sameOrigin)
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
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("extract %s%s", newRMD, _msdetail(rmd.Version, msg, caller))
	}
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
	reader := bytes.NewBuffer(bmdValue)
	if _, err1 := jsp.Decode(io.NopCloser(reader), newBMD, newBMD.JspOpts(), "extractBMD"); err1 != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "new BMD", cmn.BytesHead(bmdValue), err1)
		return
	}
	if msgValue, ok := payload[revsBMDTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "action message", cmn.BytesHead(msgValue), err1)
			return
		}
	}
	bmd := h.owner.bmd.get()
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("extract %s%s", newBMD, _msdetail(bmd.Version, msg, caller))
	}
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

func (h *httprunner) receiveSmap(newSmap *smapX, msg *aisMsg, payload msPayload,
	caller string, cb func(newSmap *smapX, oldSmap *smapX)) (err error) {
	if newSmap == nil {
		return
	}
	smap := h.owner.smap.get()
	glog.Infof("receive %s%s", newSmap.StringEx(), _msdetail(smap.Version, msg, caller))
	if !newSmap.isPresent(h.si) {
		err = fmt.Errorf("%s: not finding self in the new %s", h.si, newSmap.StringEx())
		glog.Warningf("Error: %s\n%s", err, newSmap.pp())
		return
	}
	err = h.owner.smap.synchronize(h.si, newSmap, payload)
	if err != nil {
		return
	}
	if cb != nil {
		cb(newSmap, smap)
	}
	return
}

func (h *httprunner) receiveConfig(newConfig *globalConfig, msg *aisMsg, payload msPayload, caller string) (err error) {
	config := cmn.GCO.Get()
	glog.Infof("receive %s%s", newConfig, _msdetail(config.Version, msg, caller))

	h.owner.config.Lock()
	defer h.owner.config.Unlock()
	config = cmn.GCO.Get()
	if newConfig.version() <= config.Version {
		if newConfig.version() == config.Version {
			return
		}
		return newErrDowngrade(h.si, config.String(), newConfig.String())
	}
	if err = h.owner.config.persist(newConfig, payload); err != nil {
		return
	}
	err = cmn.GCO.Update(&newConfig.ClusterConfig)
	debug.AssertNoErr(err)
	return
}

func (h *httprunner) extractRevokedTokenList(payload msPayload, caller string) (*tokenList, error) {
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
	tokenList := &tokenList{}
	if err := jsoniter.Unmarshal(bytes, tokenList); err != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "blocked token list", cmn.BytesHead(bytes), err)
		return nil, err
	}
	glog.Infof("extract token list from %q (count: %d, action: %q, uuid: %q)", caller,
		len(tokenList.Tokens), msg.Action, msg.UUID)
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
		path          string
		skipPrxKalive = h.si.IsProxy() || keepalive
		opts          = cmetaFillOpt{
			skipSmap:      skipPrxKalive,
			skipBMD:       skipPrxKalive,
			skipRMD:       keepalive,
			skipConfig:    keepalive,
			fillRebMarker: !keepalive,
		}
	)
	regReq, err := h.cluMeta(opts)
	if err != nil {
		res := allocCallRes()
		res.err = err
		return res
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

func (h *httprunner) pollClusterStarted(config *cmn.Config, psi *cluster.Snode) (maxCii *clusterInfo) {
	var (
		sleep, total, rediscover time.Duration
		healthTimeout            = config.Timeout.CplaneOperation.D()
		query                    = url.Values{cmn.URLParamAskPrimary: []string{"true"}}
	)
	for {
		sleep = cos.MinDuration(config.Timeout.MaxKeepalive.D(), sleep+time.Second)
		time.Sleep(sleep)
		total += sleep
		rediscover += sleep
		if daemon.stopping.Load() {
			return
		}
		smap := h.owner.smap.get()
		if smap.validate() != nil {
			continue
		}
		if h.si.IsProxy() && smap.isPrimary(h.si) { // TODO: unlikely - see httpRequestNewPrimary
			glog.Warningf("%s: started as a non-primary and got ELECTED during startup", h.si)
			return
		}
		if _, _, err := h.Health(smap.Primary, healthTimeout, query /*ask primary*/); err == nil {
			rmd := h.owner.rmd.get()
			glog.Infof("%s via primary health: cluster startup ok, %s, %s", h.si, smap.StringEx(), rmd)
			return
		}
		if rediscover >= config.Timeout.Startup.D()/2 {
			rediscover = 0
			if cii, cnt := h.bcastHealth(smap); cii.Smap.Version > smap.version() {
				var pid string
				if psi != nil {
					pid = psi.ID()
				}
				if cii.Smap.Primary.ID != pid && cnt >= minPidConfirmations {
					glog.Warningf("%s: change of primary %s => %s - must rejoin",
						h.si, pid, cii.Smap.Primary.ID)
					maxCii = cii
					return
				}
			}
		}
		if total > config.Timeout.Startup.D() {
			glog.Errorf("%s: cluster startup is taking unusually long time...", h.si)
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
	callerID := r.Header.Get(cmn.HdrCallerID)
	caller := r.Header.Get(cmn.HdrCallerName)
	// external call
	if callerID == "" && caller == "" {
		readiness := cos.IsParseBool(r.URL.Query().Get(cmn.URLParamHealthReadiness))
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s: external health-ping from %s (readiness=%t)", h.si, r.RemoteAddr, readiness)
		}
		// respond with 503 as per https://tools.ietf.org/html/rfc7231#section-6.6.4
		// see also:
		// https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes
		if !readiness && !h.ClusterStarted() {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		// NOTE: for "readiness" check always return true; otherwise, true if cluster started
		return true
	}
	// intra-cluster health ping
	if !h.ensureIntraControl(w, r, false /* from primary */) {
		responded = true
	}
	return
}

func selectBMDBuckets(bmd *bucketMD, query cmn.QueryBcks) cmn.Bcks {
	var (
		names = make(cmn.Bcks, 0, 10)
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

func newQueryBcksFromQuery(bckName string, query url.Values) (cmn.QueryBcks, error) {
	var (
		provider  = query.Get(cmn.URLParamProvider)
		namespace = cmn.ParseNsUname(query.Get(cmn.URLParamNamespace))
		bck       = cmn.QueryBcks{Name: bckName, Provider: provider, Ns: namespace}
	)
	if err := bck.Validate(); err != nil {
		return bck, err
	}
	return bck, nil
}

func newBckFromQueryUname(query url.Values, required bool) (*cluster.Bck, error) {
	uname := query.Get(cmn.URLParamBucketTo)
	if uname == "" {
		if required {
			return nil, fmt.Errorf("missing %q query parameter", cmn.URLParamBucketTo)
		}
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

//////////////////////////////////////
// intra-cluster request validation //
//////////////////////////////////////

func (h *httprunner) _isIntraCall(hdr http.Header, fromPrimary bool) (err error) {
	debug.Assert(hdr != nil)
	var (
		smap       = h.owner.smap.get()
		callerID   = hdr.Get(cmn.HdrCallerID)
		callerName = hdr.Get(cmn.HdrCallerName)
		callerSver = hdr.Get(cmn.HdrCallerSmapVersion)
		callerVer  int64
		erP        error
	)
	if ok := callerID != "" && callerName != ""; !ok {
		return fmt.Errorf("%s: expected %s request", h.si, cmn.NetworkIntraControl)
	}
	if !smap.isValid() {
		return
	}
	caller := smap.GetNode(callerID)
	if ok := caller != nil && (!fromPrimary || smap.isPrimary(caller)); ok {
		return
	}
	if callerSver != smap.vstr && callerSver != "" {
		callerVer, erP = strconv.ParseInt(callerSver, 10, 64)
		if erP != nil {
			debug.AssertNoErr(erP)
			glog.Error(erP)
			return
		}
		// we still trust the request when the sender's Smap is more current
		if callerVer > smap.version() {
			glog.Errorf("%s: %s < %s-Smap(v%s) - proceeding anyway...", h.si, smap, callerName, callerSver)
			runtime.Gosched()
			return
		}
	}
	if caller == nil {
		if !fromPrimary {
			// assume request from a newly joined node and proceed
			return nil
		}
		return fmt.Errorf("%s: expected %s from a valid node, %s", h.si, cmn.NetworkIntraControl, smap)
	}
	return fmt.Errorf("%s: expected %s from primary (and not %s), %s", h.si, cmn.NetworkIntraControl, caller, smap)
}

func (h *httprunner) isIntraCall(hdr http.Header) (isIntra bool) {
	err := h._isIntraCall(hdr, false /*from primary*/)
	return err == nil
}

func (h *httprunner) ensureIntraControl(w http.ResponseWriter, r *http.Request, onlyPrimary bool) (isIntra bool) {
	err := h._isIntraCall(r.Header, onlyPrimary)
	if err != nil {
		h.writeErr(w, r, err)
		return
	}
	if !cmn.GCO.Get().HostNet.UseIntraControl {
		return true // intra-control == pub
	}
	// NOTE: not checking r.RemoteAddr
	intraAddr := h.si.IntraControlNet.TCPEndpoint()
	srvAddr := r.Context().Value(http.ServerContextKey).(*http.Server).Addr
	if srvAddr == intraAddr {
		return true
	}
	h.writeErrf(w, r, "%s: expected %s request", h.si, cmn.NetworkIntraControl)
	return
}

////////////////////
// aisMsg helpers //
////////////////////

func (h *httprunner) newAmsgStr(msgStr string, bmd *bucketMD) *aisMsg {
	return h.newAmsg(&cmn.ActionMsg{Value: msgStr}, bmd)
}

func (h *httprunner) newAmsgActVal(act string, val interface{}) *aisMsg {
	return h.newAmsg(&cmn.ActionMsg{Action: act, Value: val}, nil)
}

func (h *httprunner) newAmsg(actionMsg *cmn.ActionMsg, bmd *bucketMD, uuid ...string) *aisMsg {
	msg := &aisMsg{ActionMsg: *actionMsg}
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
	if caller := r.Header.Get(cmn.HdrCallerName); caller != "" {
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
		smap = h.owner.smap.get()
		bmd  = h.owner.bmd.get()
		rmd  = h.owner.rmd.get()
	)
	cii.fillSmap(smap)
	cii.BMD.Version = bmd.version()
	cii.BMD.UUID = bmd.UUID
	cii.RMD.Version = rmd.Version
	cii.Config.Version = h.owner.config.version()
	cii.Flags.ClusterStarted = h.ClusterStarted()
	cii.Flags.NodeStarted = h.NodeStarted()
}

func (cii *clusterInfo) fillSmap(smap *smapX) {
	cii.Smap.Version = smap.version()
	cii.Smap.UUID = smap.UUID
	cii.Smap.Primary.CtrlURL = smap.Primary.URL(cmn.NetworkIntraControl)
	cii.Smap.Primary.PubURL = smap.Primary.URL(cmn.NetworkPublic)
	cii.Smap.Primary.ID = smap.Primary.ID()
	xact := xreg.GetXactRunning(cmn.ActElection)
	cii.Flags.VoteInProgress = xact != nil
}

func (cii *clusterInfo) smapEqual(other *clusterInfo) (ok bool) {
	if cii == nil || other == nil {
		return false
	}
	return cii.Smap.Version == other.Smap.Version && cii.Smap.Primary.ID == other.Smap.Primary.ID
}
