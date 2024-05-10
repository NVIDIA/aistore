// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	rdebug "runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/golang/mux"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cifl"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xact/xreg"
	jsoniter "github.com/json-iterator/go"
	"github.com/tinylib/msgp/msgp"
)

const ua = "aisnode"

const unknownDaemonID = "unknown"

const whatRenamedLB = "renamedlb"

// common error formats
const (
	fmtErrInsuffMpaths1 = "%s: not enough mountpaths (%d) to configure %s as %d-way mirror"
	fmtErrInsuffMpaths2 = "%s: not enough mountpaths (%d) to replicate %s (configured) %d times"
	fmtErrInvaldAction  = "invalid action %q (expected one of %v)"
	fmtUnknownQue       = "unexpected query [what=%s]"
	fmtNested           = "%s: nested (%v): failed to %s %q: %v"
	fmtOutside          = "%s is present (vs requested 'flt-outside'(%d))"
	fmtFailedRejoin     = "%s failed to rejoin cluster: %v(%d)"
	fmtSelfNotPresent   = "%s (self) not present in %s"
)

// intra-cluster control messages
type (
	// cluster-wide control information - replicated, versioned, and synchronized
	// usage: elect new primary, join cluster, ...
	cluMeta struct {
		Smap      *smapX        `json:"smap"`
		BMD       *bucketMD     `json:"bmd"`
		RMD       *rebMD        `json:"rmd"`
		EtlMD     *etlMD        `json:"etlMD"`
		Config    *globalConfig `json:"config"`
		SI        *meta.Snode   `json:"si"`
		PrimeTime int64         `json:"prime_time"`
		Flags     cifl.Flags    `json:"flags"`
	}

	// extend control msg: ActionMsg with an extra information for node <=> node control plane communications
	aisMsg struct {
		apc.ActMsg
		UUID       string `json:"uuid"` // cluster-wide ID of this action (operation, transaction)
		BMDVersion int64  `json:"bmdversion,string"`
		RMDVersion int64  `json:"rmdversion,string"`
	}

	cleanmark struct {
		OldVer      int64 `json:"oldver,string"`
		NewVer      int64 `json:"newver,string"`
		Interrupted bool  `json:"interrupted"`
		Restarted   bool  `json:"restarted"`
	}
)

type (
	byteRanges struct {
		Range string // cos.HdrRange, see https://www.rfc-editor.org/rfc/rfc7233#section-2.1
		Size  int64  // size, in bytes
	}

	// callResult contains HTTP response.
	callResult struct {
		v       any // unmarshalled value (only when requested via `callArgs.v`)
		err     error
		si      *meta.Snode
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
		si      *meta.Snode
		req     cmn.HreqArgs
		timeout time.Duration
	}

	// bcastArgs: intra-cluster broadcast call args
	bcastArgs struct {
		cresv             cresv          // call result value (comment above)
		smap              *smapX         // Smap to use
		network           string         // one of the cmn.KnownNetworks
		req               cmn.HreqArgs   // h.call args
		nodes             []meta.NodeMap // broadcast destinations - map(s)
		selected          meta.Nodes     // broadcast destinations - slice of selected few
		timeout           time.Duration  // call timeout
		to                int            // (all targets, all proxies, all nodes) enum
		nodeCount         int            // m.b. greater or equal destination count
		ignoreMaintenance bool           // do not skip nodes in maintenance mode
		async             bool           // ignore results
	}

	networkHandler struct {
		r   string           // resource
		h   http.HandlerFunc // handler
		net netAccess        // handler network access
	}

	nodeRegPool []cluMeta

	// what data to omit when sending request/response (join-cluster, kalive)
	cmetaFillOpt struct {
		htext         htext
		skipSmap      bool
		skipBMD       bool
		skipRMD       bool
		skipConfig    bool
		skipEtlMD     bool
		fillRebMarker bool
		skipPrimeTime bool
	}

	getMaxCii struct {
		h          *htrun
		maxCii     *cifl.Info
		query      url.Values
		maxConfVer int64
		timeout    time.Duration
		mu         sync.Mutex
		cnt        int
		checkAll   bool
	}

	httpMuxers map[string]*mux.ServeMux // by http.Method

	// http server and http runner (common for proxy and target)
	netServer struct {
		sync.Mutex
		s             *http.Server
		muxers        httpMuxers
		sndRcvBufSize int
	}

	nlogWriter struct{}
)

// error types
type (
	errTgtBmdUUIDDiffer struct{ detail string } // BMD & its uuid
	errPrxBmdUUIDDiffer struct{ detail string }
	errBmdUUIDSplit     struct{ detail string }
	errSmapUUIDDiffer   struct{ detail string } // ditto Smap
	errNodeNotFound     struct {
		msg  string
		id   string
		si   *meta.Snode
		smap *smapX
	}
	errNotEnoughTargets struct {
		si       *meta.Snode
		smap     *smapX
		required int // should at least contain
	}
	errDowngrade struct {
		si       *meta.Snode
		from, to string
	}
	errNotPrimary struct {
		si     *meta.Snode
		smap   *smapX
		detail string
	}
	errNoUnregister struct {
		action string
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
	errFastKalive        = errors.New("cannot fast-keepalive")
)

// BMD uuid errs
var errNoBMD = errors.New("no bucket metadata")

func (e *errTgtBmdUUIDDiffer) Error() string { return e.detail }
func (e *errBmdUUIDSplit) Error() string     { return e.detail }
func (e *errPrxBmdUUIDDiffer) Error() string { return e.detail }
func (e *errSmapUUIDDiffer) Error() string   { return e.detail }
func (e *errNodeNotFound) Error() string {
	return fmt.Sprintf("%s: %s node %s not present in the %s", e.si, e.msg, e.id, e.smap)
}

/////////////////////
// errNoUnregister //
/////////////////////

func (e *errNoUnregister) Error() string { return e.action }

func isErrNoUnregister(err error) (ok bool) {
	_, ok = err.(*errNoUnregister)
	return
}

//////////////////
// errDowngrade //
//////////////////

func newErrDowngrade(si *meta.Snode, from, to string) *errDowngrade {
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
		e.si, e.smap, e.required, e.smap.CountActiveTs())
}

///////////////////
// errNotPrimary //
///////////////////

func newErrNotPrimary(si *meta.Snode, smap *smapX, detail ...string) *errNotPrimary {
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
	cresCM struct{} // -> cluMeta; selectively and alternatively, via `recvCluMetaBytes`
	cresSM struct{} // -> smapX
	cresND struct{} // -> meta.Snode
	cresBA struct{} // -> cmn.BackendInfoAIS
	cresEI struct{} // -> etl.InfoList
	cresEL struct{} // -> etl.Logs
	cresEM struct{} // -> etl.CPUMemUsed
	cresIC struct{} // -> icBundle
	cresBM struct{} // -> bucketMD

	cresLso   struct{} // -> cmn.LsoRes
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
	_ cresv = cresEM{}
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

func (cresLso) newV() any                              { return &cmn.LsoRes{} }
func (c cresLso) read(res *callResult, body io.Reader) { res.v = c.newV(); res.mread(body) }

func (cresSM) newV() any                              { return &smapX{} }
func (c cresSM) read(res *callResult, body io.Reader) { res.v = c.newV(); res.jread(body) }

func (cresND) newV() any                              { return &meta.Snode{} }
func (c cresND) read(res *callResult, body io.Reader) { res.v = c.newV(); res.jread(body) }

func (cresBA) newV() any                              { return &meta.RemAisVec{} }
func (c cresBA) read(res *callResult, body io.Reader) { res.v = c.newV(); res.jread(body) }

func (cresEI) newV() any                              { return &etl.InfoList{} }
func (c cresEI) read(res *callResult, body io.Reader) { res.v = c.newV(); res.jread(body) }

func (cresEL) newV() any                              { return &etl.Logs{} }
func (c cresEL) read(res *callResult, body io.Reader) { res.v = c.newV(); res.jread(body) }

func (cresEM) newV() any                              { return &etl.CPUMemUsed{} }
func (c cresEM) read(res *callResult, body io.Reader) { res.v = c.newV(); res.jread(body) }

func (cresIC) newV() any                              { return &icBundle{} }
func (c cresIC) read(res *callResult, body io.Reader) { res.v = c.newV(); res.jread(body) }

func (cresBM) newV() any                              { return &bucketMD{} }
func (c cresBM) read(res *callResult, body io.Reader) { res.v = c.newV(); res.jread(body) }

func (cresBsumm) newV() any                              { return &cmn.AllBsummResults{} }
func (c cresBsumm) read(res *callResult, body io.Reader) { res.v = c.newV(); res.jread(body) }

////////////////
// nlogWriter //
////////////////

const tlsHandshakeErrorPrefix = "http: TLS handshake error"

func (*nlogWriter) Write(p []byte) (int, error) {
	s := string(p)
	// Ignore TLS handshake errors (see: https://github.com/golang/go/issues/26918).
	if strings.Contains(s, tlsHandshakeErrorPrefix) {
		return len(p), nil
	}

	nlog.Errorln(s)

	stacktrace := rdebug.Stack()
	nlog.Errorln(string(stacktrace))
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
		nlog.Errorln(err)
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

func (server *netServer) listen(addr string, logger *log.Logger, tlsConf *tls.Config, config *cmn.Config) (err error) {
	var (
		httpHandler = server.muxers
		tag         = "HTTP"
		retried     bool
	)
	server.Lock()
	server.s = &http.Server{
		Addr:              addr,
		Handler:           httpHandler,
		ErrorLog:          logger,
		ReadHeaderTimeout: apc.ReadHeaderTimeout,
	}
	if timeout, isSet := cmn.ParseReadHeaderTimeout(); isSet { // optional env var
		server.s.ReadHeaderTimeout = timeout
	}
	if server.sndRcvBufSize > 0 && !config.Net.HTTP.UseHTTPS {
		server.s.ConnState = server.connStateListener // setsockopt; see also cmn.NewTransport
	}
	server.s.TLSConfig = tlsConf
	server.Unlock()
retry:
	if config.Net.HTTP.UseHTTPS {
		tag = "HTTPS"
		err = server.s.ListenAndServeTLS(config.Net.HTTP.Certificate, config.Net.HTTP.CertKey)
	} else {
		err = server.s.ListenAndServe()
	}
	if err == http.ErrServerClosed {
		return nil
	}
	if errors.Is(err, syscall.EADDRINUSE) && !retried {
		nlog.Warningf("%q - shutting-down-and-restarting or else? will retry once...", err)
		time.Sleep(max(5*time.Second, config.Timeout.MaxKeepalive.D()))
		retried = true
		goto retry
	}
	nlog.Errorf("%s terminated with error: %v", tag, err)
	return
}

func newTLS(conf *cmn.HTTPConf) (tlsConf *tls.Config, err error) {
	var (
		pool       *x509.CertPool
		caCert     []byte
		clientAuth = tls.ClientAuthType(conf.ClientAuthTLS)
	)
	if clientAuth > tls.RequestClientCert {
		if caCert, err = os.ReadFile(conf.ClientCA); err != nil {
			return
		}
		pool = x509.NewCertPool()
		if ok := pool.AppendCertsFromPEM(caCert); !ok {
			return nil, fmt.Errorf("tls: failed to append CA certs from PEM: %q", conf.ClientCA)
		}
	}
	tlsConf = &tls.Config{ClientAuth: clientAuth, ClientCAs: pool}
	return
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

func (server *netServer) shutdown(config *cmn.Config) {
	server.Lock()
	defer server.Unlock()
	if server.s == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout.MaxHostBusy.D())
	if err := server.s.Shutdown(ctx); err != nil {
		nlog.Infoln("http server shutdown err:", err)
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

func (p *proxy) ciiFill(cii *cifl.Info) {
	p.htrun.fill(cii)
	onl := true
	flt := nlFilter{Kind: apc.ActRebalance, OnlyRunning: &onl}
	if nl := p.notifs.find(flt); nl != nil {
		cii.Flags = cii.Flags.Set(cifl.Rebalancing)
	}
}

func (t *target) ciiFill(cii *cifl.Info) {
	t.htrun.fill(cii)
	marked := xreg.GetRebMarked()
	if marked.Xact != nil {
		cii.Flags = cii.Flags.Set(cifl.Rebalancing)
	}
	if marked.Interrupted {
		cii.Flags = cii.Flags.Set(cifl.RebalanceInterrupted)
	}
	if marked.Restarted {
		cii.Flags = cii.Flags.Set(cifl.Restarted)
	}
	marked = xreg.GetResilverMarked()
	if marked.Xact != nil {
		cii.Flags = cii.Flags.Set(cifl.Resilvering)
	}
	if marked.Interrupted {
		cii.Flags = cii.Flags.Set(cifl.ResilverInterrupted)
	}
}

func (h *htrun) fill(cii *cifl.Info) {
	var (
		smap = h.owner.smap.get()
		bmd  = h.owner.bmd.get()
		rmd  = h.owner.rmd.get()
		etl  = h.owner.etl.get()
	)
	smap.fill(cii)
	cii.BMD.Version = bmd.version()
	cii.BMD.UUID = bmd.UUID
	cii.RMD.Version = rmd.Version
	cii.Config.Version = h.owner.config.version()
	cii.EtlMD.Version = etl.version()
	if h.ClusterStarted() {
		cii.Flags = cii.Flags.Set(cifl.ClusterStarted)
	}
	if h.NodeStarted() {
		cii.Flags = cii.Flags.Set(cifl.NodeStarted)
	}
}

func (smap *smapX) fill(cii *cifl.Info) {
	cii.Smap.Version = smap.version()
	cii.Smap.UUID = smap.UUID
	if smap.Primary != nil {
		cii.Smap.Primary.CtrlURL = smap.Primary.URL(cmn.NetIntraControl)
		cii.Smap.Primary.PubURL = smap.Primary.URL(cmn.NetPublic)
		cii.Smap.Primary.ID = smap.Primary.ID()
		if voteInProgress() != nil {
			cii.Flags = cii.Flags.Set(cifl.VoteInProgress)
		}
	}
}

///////////////
// getMaxCii //
///////////////

func (c *getMaxCii) do(si *meta.Snode, wg cos.WG, smap *smapX) {
	var cii *cifl.Info
	body, _, err := c.h.reqHealth(si, c.timeout, c.query, smap)
	if err != nil {
		goto ret
	}
	if cii = extractCii(body, smap, c.h.si, si); cii == nil {
		goto ret
	}
	if cii.Smap.UUID != smap.UUID {
		if cii.Smap.UUID == "" {
			goto ret
		}
		if smap.UUID != "" {
			// FATAL: cluster integrity error (cie)
			cos.ExitLogf("%s: split-brain uuid [%s %s] vs %+v", ciError(10), c.h, smap.StringEx(), cii.Smap)
		}
	}
	c.mu.Lock()
	if c.maxCii.Smap.Version < cii.Smap.Version {
		// reset confirmation count if there's any sign of disagreement
		if c.maxCii.Smap.Primary.ID != cii.Smap.Primary.ID || cii.Flags.IsSet(cifl.VoteInProgress) {
			c.cnt = 1
		} else {
			c.cnt++
		}
		c.maxCii = cii
	} else if c.maxCii.SmapEqual(cii) {
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

func extractCii(body []byte, smap *smapX, self, si *meta.Snode) *cifl.Info {
	var cii cifl.Info
	if err := jsoniter.Unmarshal(body, &cii); err != nil {
		nlog.Errorf("%s: failed to unmarshal clusterInfo, err: %v", self, err)
		return nil
	}
	if smap.UUID != cii.Smap.UUID {
		nlog.Errorf("%s: Smap have different UUIDs: %s and %s from %s", self, smap.UUID, cii.Smap.UUID, si)
		return nil
	}
	return &cii
}

////////////////
// apiRequest //
////////////////

type apiRequest struct {
	bck *meta.Bck // out: initialized bucket

	// URL query: the conventional/slow and
	// the fast alternative tailored exclusively for the datapath (either/or)
	dpq   *dpq
	query url.Values

	prefix []string // in: URL must start with these items
	items  []string // out: URL items after the prefix

	after  int // in: the number of items after the prefix
	bckIdx int // in: ordinal number of bucket in URL (some paths starts with extra items: EC & ETL)
}

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

func newBckFromQ(bckName string, query url.Values, dpq *dpq) (*meta.Bck, error) {
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

func _bckFromQ(bckName string, query url.Values, dpq *dpq) *meta.Bck {
	var (
		provider  string
		namespace cmn.Ns
	)
	if query != nil {
		debug.Assert(dpq == nil)
		provider = query.Get(apc.QparamProvider)
		namespace = cmn.ParseNsUname(query.Get(apc.QparamNamespace))
	} else {
		provider = dpq.bck.provider
		namespace = cmn.ParseNsUname(dpq.bck.namespace)
	}
	return &meta.Bck{Name: bckName, Provider: provider, Ns: namespace}
}

func newBckFromQuname(query url.Values, required bool) (*meta.Bck, error) {
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
	return meta.CloneBck(&bck), nil
}

func _reMirror(bprops, nprops *cmn.Bprops) bool {
	if !bprops.Mirror.Enabled && nprops.Mirror.Enabled {
		return true
	}
	if bprops.Mirror.Enabled && nprops.Mirror.Enabled {
		return bprops.Mirror.Copies != nprops.Mirror.Copies
	}
	return false
}

func _reEC(bprops, nprops *cmn.Bprops, bck *meta.Bck, smap *smapX) (targetCnt int, yes bool) {
	if !nprops.EC.Enabled {
		if bprops.EC.Enabled {
			// abort running ec-encode xaction, if exists
			flt := xreg.Flt{Kind: apc.ActECEncode, Bck: bck}
			xreg.DoAbort(flt, errors.New("ec-disabled"))
		}
		return
	}
	if smap != nil {
		targetCnt = smap.CountActiveTs()
	}
	if !bprops.EC.Enabled ||
		(bprops.EC.DataSlices != nprops.EC.DataSlices || bprops.EC.ParitySlices != nprops.EC.ParitySlices) {
		yes = true
	}
	return
}
