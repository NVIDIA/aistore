// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xact/xreg"
	jsoniter "github.com/json-iterator/go"
	"github.com/tinylib/msgp/msgp"
)

const ua = "aisnode"

const unknownDaemonID = "unknown"

const whatRenamedLB = "renamedlb"

const (
	fmtErrInsuffMpaths1      = "%s: not enough mountpaths (%d) to configure %s as %d-way mirror"
	fmtErrInsuffMpaths2      = "%s: not enough mountpaths (%d) to replicate %s (configured) %d times"
	fmtErrPrimaryNotReadyYet = "%s primary is not ready yet to start rebalance (started=%t, starting-up=%t)"
	fmtErrInvaldAction       = "invalid action %q (expected one of %v)"
	fmtUnknownQue            = "unexpected query [what=%s]"
)

type (
	byteRanges struct {
		Range string // cos.HdrRange, see https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
		Size  int64  // size, in bytes
	}

	archiveQuery struct {
		filename string // pathname in archive
		mime     string // https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types
	}

	// callResult contains HTTP response.
	callResult struct {
		v       any // unmarshalled value (only when requested via `callArgs.v`)
		err     error
		si      *cluster.Snode
		header  http.Header
		details string
		bytes   []byte // response bytes (raw)
		status  int
	}

	sliceResults []*callResult
	bcastResults struct {
		s  sliceResults
		mu sync.Mutex
	}

	// cresv: call result value factory and result-type specific decoder
	// (used in both callArgs and bcastArgs)
	cresv interface {
		newV() any
		read(*callResult, io.Reader)
	}

	// callArgs: unicast control-plane call arguments
	callArgs struct {
		cresv   cresv
		si      *cluster.Snode
		req     cmn.HreqArgs
		timeout time.Duration
	}

	// bcastArgs: intra-cluster broadcast call args
	bcastArgs struct {
		cresv             cresv             // call result value (comment above)
		smap              *smapX            // Smap to use
		network           string            // one of the cmn.KnownNetworks
		req               cmn.HreqArgs      // h.call args
		nodes             []cluster.NodeMap // broadcast destinations - map(s)
		selected          cluster.Nodes     // broadcast destinations - slice of selected few
		timeout           time.Duration     // call timeout
		to                int               // (all targets, all proxies, all nodes) enum
		nodeCount         int               // m.b. greater or equal destination count
		ignoreMaintenance bool              // do not skip nodes under maintenance
		async             bool              // ignore results
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
			Primary struct {
				PubURL  string `json:"pub_url"`
				CtrlURL string `json:"control_url"`
				ID      string `json:"id"`
			}
			Version int64  `json:"version,string"`
			UUID    string `json:"uuid"`
		} `json:"smap"`
		BMD struct {
			UUID    string `json:"uuid"`
			Version int64  `json:"version,string"`
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
		maxCii     *clusterInfo
		query      url.Values
		maxConfVer int64
		timeout    time.Duration
		mu         sync.Mutex
		cnt        int
		checkAll   bool
	}

	electable interface {
		proxyElection(vr *VoteRecord)
	}

	// aisMsg is an extended ActionMsg with extra information for node <=> node control plane communications
	aisMsg struct {
		apc.ActionMsg
		UUID       string `json:"uuid"` // cluster-wide ID of this action (operation, transaction)
		BMDVersion int64  `json:"bmdversion,string"`
		RMDVersion int64  `json:"rmdversion,string"`
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
	apiRequest struct {
		bck *cluster.Bck // out: initialized bucket

		// URL query: the conventional/slow and
		// the fast alternative tailored exclusively for the datapath (either/or)
		dpq   *dpq
		query url.Values

		prefix []string // in: URL must start with these items
		items  []string // out: URL items after the prefix

		after  int // in: the number of items after the prefix
		bckIdx int // in: ordinal number of bucket in URL (some paths starts with extra items: EC & ETL)
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
/////////////////////////

func (e *errNotEnoughTargets) Error() string {
	return fmt.Sprintf("%s: not enough targets: %s, need %d, have %d",
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
// bargsPool & callArgsPool
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

//
// all `cresv` implementations
// and common read-body methods w/ optional value-unmarshaling
//

type (
	cresCM struct{} // -> cluMeta
	cresSM struct{} // -> smapX
	cresND struct{} // -> cluster.Snode
	cresBA struct{} // -> cmn.BackendInfoAIS
	cresEI struct{} // -> etl.InfoList
	cresEL struct{} // -> etl.PodLogsMsg
	cresEH struct{} // -> etl.PodHealthMsg
	cresIC struct{} // -> icBundle
	cresBM struct{} // -> bucketMD

	cresLso   struct{} // -> cmn.LsoResult
	cresBsumm struct{} // -> cmn.AllBsummResults
)

var (
	_ cresv = cresCM{}
	_ cresv = cresLso{}
	_ cresv = cresSM{}
	_ cresv = cresND{}
	_ cresv = cresBA{}
	_ cresv = cresEI{}
	_ cresv = cresEL{}
	_ cresv = cresEH{}
	_ cresv = cresIC{}
	_ cresv = cresBM{}
	_ cresv = cresBsumm{}
)

func (res *callResult) read(body io.Reader)  { res.bytes, res.err = io.ReadAll(body) }
func (res *callResult) jread(body io.Reader) { res.err = jsoniter.NewDecoder(body).Decode(res.v) }

func (res *callResult) mread(body io.Reader) {
	vv, ok := res.v.(msgp.Decodable)
	debug.Assert(ok)
	buf, slab := memsys.PageMM().AllocSize(cmn.MsgpLsoBufSize)
	res.err = vv.DecodeMsg(msgp.NewReaderBuf(body, buf))
	slab.Free(buf)
}

func (cresCM) newV() any                              { return &cluMeta{} }
func (c cresCM) read(res *callResult, body io.Reader) { res.v = c.newV(); res.jread(body) }

func (cresLso) newV() any                              { return &cmn.LsoResult{} }
func (c cresLso) read(res *callResult, body io.Reader) { res.v = c.newV(); res.mread(body) }

func (cresSM) newV() any                              { return &smapX{} }
func (c cresSM) read(res *callResult, body io.Reader) { res.v = c.newV(); res.jread(body) }

func (cresND) newV() any                              { return &cluster.Snode{} }
func (c cresND) read(res *callResult, body io.Reader) { res.v = c.newV(); res.jread(body) }

func (cresBA) newV() any                              { return &cluster.Remotes{} }
func (c cresBA) read(res *callResult, body io.Reader) { res.v = c.newV(); res.jread(body) }

func (cresEI) newV() any                              { return &etl.InfoList{} }
func (c cresEI) read(res *callResult, body io.Reader) { res.v = c.newV(); res.jread(body) }

func (cresEL) newV() any                              { return &etl.PodLogsMsg{} }
func (c cresEL) read(res *callResult, body io.Reader) { res.v = c.newV(); res.jread(body) }

func (cresEH) newV() any                              { return &etl.PodHealthMsg{} }
func (c cresEH) read(res *callResult, body io.Reader) { res.v = c.newV(); res.jread(body) }

func (cresIC) newV() any                              { return &icBundle{} }
func (c cresIC) read(res *callResult, body io.Reader) { res.v = c.newV(); res.jread(body) }

func (cresBM) newV() any                              { return &bucketMD{} }
func (c cresBM) read(res *callResult, body io.Reader) { res.v = c.newV(); res.jread(body) }

func (cresBsumm) newV() any                              { return &cmn.AllBsummResults{} }
func (c cresBsumm) read(res *callResult, body io.Reader) { res.v = c.newV(); res.jread(body) }

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
	// From this point on, this function is responsible for HTTP connection
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		cmn.WriteErr(w, r, errors.New("response writer does not support hijacking"),
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
var _ http.Handler = (*httpMuxers)(nil)

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

// //////////////
// apiRequest //
// //////////////
var (
	apiReqPool sync.Pool
	apireq0    apiRequest
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

//
// misc helpers
//

func newBckFromQ(bckName string, query url.Values, dpq *dpq) (*cluster.Bck, error) {
	bck := _bckFromQ(bckName, query, dpq)
	normp, err := cmn.NormalizeProvider(bck.Provider)
	if err == nil {
		bck.Provider = normp
		err = bck.Validate()
	}
	return bck, err
}

func newQbckFromQ(bckName string, query url.Values, dpq *dpq) (*cmn.QueryBcks, error) {
	qbck := (*cmn.QueryBcks)(_bckFromQ(bckName, query, dpq))
	return qbck, qbck.Validate()
}

func _bckFromQ(bckName string, query url.Values, dpq *dpq) *cluster.Bck {
	var (
		provider  string
		namespace cmn.Ns
	)
	if query != nil {
		debug.Assert(dpq == nil)
		provider = query.Get(apc.QparamProvider)
		namespace = cmn.ParseNsUname(query.Get(apc.QparamNamespace))
	} else {
		provider = dpq.provider
		namespace = cmn.ParseNsUname(dpq.namespace)
	}
	return &cluster.Bck{Name: bckName, Provider: provider, Ns: namespace}
}

func newBckFromQuname(query url.Values, required bool) (*cluster.Bck, error) {
	uname := query.Get(apc.QparamBckTo)
	if uname == "" {
		if required {
			return nil, fmt.Errorf("missing %q query parameter", apc.QparamBckTo)
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
	return cluster.CloneBck(&bck), nil
}

func _reMirror(bprops, nprops *cmn.BucketProps) bool {
	if !bprops.Mirror.Enabled && nprops.Mirror.Enabled {
		return true
	}
	if bprops.Mirror.Enabled && nprops.Mirror.Enabled {
		return bprops.Mirror.Copies != nprops.Mirror.Copies
	}
	return false
}

func _reEC(bprops, nprops *cmn.BucketProps, bck *cluster.Bck, smap *smapX) (targetCnt int, yes bool) {
	if !nprops.EC.Enabled {
		if bprops.EC.Enabled {
			// abort running ec-encode xaction, if exists
			flt := xreg.XactFilter{Kind: apc.ActECEncode, Bck: bck}
			xreg.DoAbort(flt, errors.New("ec-disabled"))
		}
		return
	}
	if smap != nil {
		targetCnt = smap.CountActiveTargets()
	}
	if !bprops.EC.Enabled ||
		(bprops.EC.DataSlices != nprops.EC.DataSlices || bprops.EC.ParitySlices != nprops.EC.ParitySlices) {
		yes = true
	}
	return
}
