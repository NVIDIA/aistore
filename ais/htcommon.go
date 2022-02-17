// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	rdebug "runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/3rdparty/golang/mux"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	jsoniter "github.com/json-iterator/go"
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
	byteRanges struct {
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
	bcastResults struct {
		mu sync.Mutex
		s  sliceResults
	}

	// callArgs contains arguments for a peer-to-peer control plane call.
	callArgs struct {
		req     cmn.HreqArgs
		timeout time.Duration
		si      *cluster.Snode
		v       interface{} // Value that needs to be unmarshalled.
	}

	// bcastArgs contains arguments for an intra-cluster broadcast call.
	bcastArgs struct {
		req               cmn.HreqArgs       // h.call args
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
		EtlMD          *etlMD         `json:"etlMD"`
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
		skipEtlMD     bool
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
		EtlMD struct {
			Version int64 `json:"version,string"`
		} `json:"etlmd"`
		Flags struct {
			VoteInProgress bool `json:"vote_in_progress"`
			ClusterStarted bool `json:"cluster_started"`
			NodeStarted    bool `json:"node_started"`
		} `json:"flags"`
	}
	getMaxCii struct {
		h          *htrun
		mu         sync.Mutex
		maxCii     *clusterInfo
		maxConfVer int64
		timeout    time.Duration
		query      url.Values
		cnt        int
		checkAll   bool
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

	httpMuxers map[string]*mux.ServeMux // by http.Method

	// http server and http runner (common for proxy and target)
	netServer struct {
		sync.Mutex
		s             *http.Server
		muxers        httpMuxers
		sndRcvBufSize int
	}

	glogWriter struct{}

	// error types
	errTgtBmdUUIDDiffer struct{ detail string } // BMD & its uuid
	errPrxBmdUUIDDiffer struct{ detail string }
	errBmdUUIDSplit     struct{ detail string }
	errSmapUUIDDiffer   struct{ detail string } // ditto Smap
	errNodeNotFound     struct {
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

	// RESTful API parse context
	dpq struct {
		provider, namespace string // bucket
		pid, ptime          string // proxy ID, timestamp
		uuid                string // xaction
		skipVC              string // (disconnected backend)
		archpath, archmime  string // archive
		isGFN               string // ditto
		origURL             string // ht://url->
		appendTy, appendHdl string // APPEND { cmn.AppendOp, ... }
		owt                 string // object write transaction { OwtPut, ... }
		dontLookupRemoteBck string // (as the name implies)
	}
	apiRequest struct {
		prefix []string     // in: URL must start with these items
		after  int          // in: the number of items after the prefix
		bckIdx int          // in: ordinal number of bucket in URL (some paths starts with extra items: EC & ETL)
		items  []string     // out: URL items after the prefix
		bck    *cluster.Bck // out: initialized bucket

		// URL query: the conventional/slow and
		// the fast alternative tailored exclusively for the datapath (either/or)
		query url.Values
		dpq   *dpq
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

/////////////////////
// errNoUnregister //
/////////////////////

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
// callArgsPool //
///////////////

var (
	bargsPool, cargsPool sync.Pool
	bargs0               bcastArgs
	cargs0               callArgs
)

func allocBcArgs() (a *bcastArgs) {
	if v := bargsPool.Get(); v != nil {
		a = v.(*bcastArgs)
		return
	}
	return &bcastArgs{}
}

func freeBcArgs(a *bcastArgs) {
	sel := a.selected
	*a = bargs0
	if sel != nil {
		a.selected = sel[:0]
	}
	bargsPool.Put(a)
}

func allocCargs() (a *callArgs) {
	if v := cargsPool.Get(); v != nil {
		a = v.(*callArgs)
		return
	}
	return &callArgs{}
}

func freeCargs(a *callArgs) {
	*a = cargs0
	cargsPool.Put(a)
}

///////////////////////
// call result pools //
///////////////////////

var (
	resultsPool sync.Pool
	callResPool sync.Pool
	callRes0    callResult
)

func allocCR() (a *callResult) {
	if v := callResPool.Get(); v != nil {
		a = v.(*callResult)
		debug.Assert(a.si == nil)
		return
	}
	return &callResult{}
}

func freeCR(res *callResult) {
	*res = callRes0
	callResPool.Put(res)
}

func allocBcastRes(n int) sliceResults {
	if v := resultsPool.Get(); v != nil {
		a := v.(*sliceResults)
		return *a
	}
	return make(sliceResults, 0, n)
}

func freeBcastRes(results sliceResults) {
	for _, res := range results {
		freeCR(res)
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
				glog.Errorf("HTTPS terminated with error: %v", err)
				return err
			}
		}
	} else {
		if err := server.s.ListenAndServe(); err != nil {
			if err != http.ErrServerClosed {
				glog.Errorf("HTTP terminated with error: %v", err)
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
// httpMuxers //
////////////////

// interface guard
var _ http.Handler = httpMuxers{}

func newMuxers() httpMuxers {
	m := make(httpMuxers, len(allHTTPverbs))
	for _, v := range allHTTPverbs {
		m[v] = mux.NewServeMux()
	}
	return m
}

// ServeHTTP dispatches the request to the handler whose
// pattern most closely matches the request URL.
func (m httpMuxers) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if sm, ok := m[r.Method]; ok {
		sm.ServeHTTP(w, r)
		return
	}
	w.WriteHeader(http.StatusBadRequest)
}

/////////////////
// clusterInfo //
/////////////////

func (cii *clusterInfo) String() string { return fmt.Sprintf("%+v", *cii) }

func (cii *clusterInfo) fill(h *htrun) {
	var (
		smap = h.owner.smap.get()
		bmd  = h.owner.bmd.get()
		rmd  = h.owner.rmd.get()
		etl  = h.owner.etl.get()
	)
	cii.fillSmap(smap)
	cii.BMD.Version = bmd.version()
	cii.BMD.UUID = bmd.UUID
	cii.RMD.Version = rmd.Version
	cii.Config.Version = h.owner.config.version()
	cii.EtlMD.Version = etl.version()
	cii.Flags.ClusterStarted = h.ClusterStarted()
	cii.Flags.NodeStarted = h.NodeStarted()
}

func (cii *clusterInfo) fillSmap(smap *smapX) {
	cii.Smap.Version = smap.version()
	cii.Smap.UUID = smap.UUID
	cii.Smap.Primary.CtrlURL = smap.Primary.URL(cmn.NetIntraControl)
	cii.Smap.Primary.PubURL = smap.Primary.URL(cmn.NetPublic)
	cii.Smap.Primary.ID = smap.Primary.ID()
	cii.Flags.VoteInProgress = voteInProgress() != nil
}

func (cii *clusterInfo) smapEqual(other *clusterInfo) (ok bool) {
	if cii == nil || other == nil {
		return false
	}
	return cii.Smap.Version == other.Smap.Version && cii.Smap.Primary.ID == other.Smap.Primary.ID
}

///////////////
// getMaxCii //
///////////////

func (c *getMaxCii) do(si *cluster.Snode, wg cos.WG, smap *smapX) {
	var cii *clusterInfo
	body, _, err := c.h.Health(si, c.timeout, c.query)
	if err != nil {
		goto ret
	}
	if cii = extractCii(body, smap, c.h.si, si); cii == nil {
		goto ret
	}
	c.mu.Lock()
	if c.maxCii.Smap.Version < cii.Smap.Version {
		// reset confirmation count if there's any sign of disagreement
		if c.maxCii.Smap.Primary.ID != cii.Smap.Primary.ID || cii.Flags.VoteInProgress {
			c.cnt = 1
		} else {
			c.cnt++
		}
		c.maxCii = cii
	} else if c.maxCii.smapEqual(cii) {
		c.cnt++
	}
	if c.maxConfVer < cii.Config.Version {
		c.maxConfVer = cii.Config.Version
	}
	c.mu.Unlock()
ret:
	wg.Done()
}

// have enough confirmations?
func (c *getMaxCii) haveEnough() (yes bool) {
	c.mu.Lock()
	yes = c.cnt >= maxVerConfirmations
	c.mu.Unlock()
	return
}

func extractCii(body []byte, smap *smapX, self, si *cluster.Snode) *clusterInfo {
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

////////////////
// apiRequest //
////////////////
var (
	apiReqPool, dpqPool sync.Pool
	apireq0             apiRequest
	dpq0                dpq
)

func apiReqAlloc(after int, prefix []string, useDpq bool) (a *apiRequest) {
	if v := apiReqPool.Get(); v != nil {
		a = v.(*apiRequest)
	} else {
		a = &apiRequest{}
	}
	a.after, a.prefix = after, prefix
	if useDpq {
		a.dpq = dpqAlloc()
	}
	return a
}

func apiReqFree(a *apiRequest) {
	if a.dpq != nil {
		dpqFree(a.dpq)
	}
	*a = apireq0
	apiReqPool.Put(a)
}

func dpqAlloc() *dpq {
	if v := dpqPool.Get(); v != nil {
		return v.(*dpq)
	}
	return &dpq{}
}

func dpqFree(dpq *dpq) {
	*dpq = dpq0
	dpqPool.Put(dpq)
}

// Data Path Query structure (dpq):
// Parse URL query for a selected few parameters used in the datapath.
// (This is a faster alternative to the conventional and RFC-compliant URL.Query()
// to be used narrowly to handle those few (keys) and nothing else.)

func urlQuery(rawQuery string, dpq *dpq) (err error) {
	query := rawQuery
	for query != "" {
		key, value := query, ""
		if i := strings.IndexAny(key, "&"); i >= 0 {
			key, query = key[:i], key[i+1:]
		} else {
			query = ""
		}
		if i := strings.Index(key, "="); i >= 0 {
			key, value = key[:i], key[i+1:]
		}
		// supported URL query parameters explicitly named below; attempt to parse anything
		// outside this list will fail
		switch key {
		case cmn.QparamProvider:
			dpq.provider = value
		case cmn.QparamNamespace:
			if dpq.namespace, err = url.QueryUnescape(value); err != nil {
				return
			}
		case cmn.QparamSkipVC:
			dpq.skipVC = value
		case cmn.QparamProxyID:
			dpq.pid = value
		case cmn.QparamUnixTime:
			dpq.ptime = value
		case cmn.QparamUUID:
			dpq.uuid = value
		case cmn.QparamArchpath:
			if dpq.archpath, err = url.QueryUnescape(value); err != nil {
				return
			}
		case cmn.QparamArchmime:
			if dpq.archmime, err = url.QueryUnescape(value); err != nil {
				return
			}
		case cmn.QparamIsGFNRequest:
			dpq.isGFN = value
		case cmn.QparamOrigURL:
			if dpq.origURL, err = url.QueryUnescape(value); err != nil {
				return
			}
		case cmn.QparamAppendType:
			dpq.appendTy = value
		case cmn.QparamAppendHandle:
			if dpq.appendHdl, err = url.QueryUnescape(value); err != nil {
				return
			}
		case cmn.QparamOWT:
			dpq.owt = value
		case cmn.QparamDontLookupRemoteBck:
			dpq.dontLookupRemoteBck = value
		default:
			err = errors.New("failed to fast-parse [" + rawQuery + "]")
			return
		}
	}
	return
}
