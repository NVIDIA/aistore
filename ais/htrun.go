// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xact/xreg"
	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/tinylib/msgp/msgp"
)

const ciePrefix = "cluster integrity error cie#"

// extra or extended state - currently, target only
type htext interface {
	interruptedRestarted() (bool, bool)
}

type htrun struct {
	si        *meta.Snode
	keepalive keepaliver
	statsT    stats.Tracker
	owner     struct {
		smap   *smapOwner
		bmd    bmdOwner // an interface with proxy and target impl-s
		rmd    *rmdOwner
		config *configOwner
		etl    etlOwner // ditto
	}
	startup struct {
		cluster atomic.Int64 // mono.NanoTime() since cluster startup, zero prior to that
		node    atomic.Int64 // ditto - for the node
	}
	gmm *memsys.MMSA // system pagesize-based memory manager and slab allocator
	smm *memsys.MMSA // system MMSA for small-size allocations
}

///////////
// htrun //
///////////

// interface guard
var _ core.Node = (*htrun)(nil)

func (*htrun) DataClient() *http.Client { return g.client.data } // TODO: a better way

func (h *htrun) Snode() *meta.Snode { return h.si }
func (h *htrun) callerName() string { return h.si.String() }
func (h *htrun) SID() string        { return h.si.ID() }
func (h *htrun) String() string     { return h.si.String() }

func (h *htrun) Bowner() meta.Bowner { return h.owner.bmd }
func (h *htrun) Sowner() meta.Sowner { return h.owner.smap }

// NOTE: currently, only 'resume' (see also: kaSuspendMsg)
func (h *htrun) smapUpdatedCB(_, _ *smapX, nfl, ofl cos.BitFlags) {
	if ofl.IsAnySet(meta.SnodeMaintDecomm) && !nfl.IsAnySet(meta.SnodeMaintDecomm) {
		h.keepalive.ctrl(kaResumeMsg)
	}
}

func (h *htrun) parseReq(w http.ResponseWriter, r *http.Request, apireq *apiRequest) (err error) {
	debug.Assert(len(apireq.prefix) != 0)
	apireq.items, err = h.parseURL(w, r, apireq.prefix, apireq.after, false)
	if err != nil {
		return
	}
	debug.Assert(len(apireq.items) > apireq.bckIdx)
	bckName := apireq.items[apireq.bckIdx]
	if apireq.dpq == nil {
		apireq.query = r.URL.Query()
	} else if err = apireq.dpq.parse(r.URL.RawQuery); err != nil {
		return
	}
	apireq.bck, err = newBckFromQ(bckName, apireq.query, apireq.dpq)
	if err != nil {
		h.writeErr(w, r, err)
	}
	return err
}

func (h *htrun) cluMeta(opts cmetaFillOpt) (*cluMeta, error) {
	xele := voteInProgress()
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
	smap := h.owner.smap.get()
	if !opts.skipSmap {
		cm.Smap = smap
	}
	if !opts.skipBMD {
		cm.BMD = h.owner.bmd.get()
	}
	if !opts.skipRMD {
		cm.RMD = h.owner.rmd.get()
	}
	if !opts.skipEtlMD {
		cm.EtlMD = h.owner.etl.get()
	}
	if h.si.IsTarget() && opts.fillRebMarker {
		cm.RebInterrupted, cm.Restarted = opts.htext.interruptedRestarted()
	}
	if !opts.skipPrimeTime && smap.IsPrimary(h.si) {
		cm.PrimeTime = time.Now().UnixNano()
	}
	return cm, nil
}

// usage: [API call => handler => ClusterStartedWithRetry ]
func (h *htrun) cluStartedWithRetry() bool {
	if clutime := h.startup.cluster.Load(); clutime > 0 {
		return true
	}
	if !h.NodeStarted() {
		return false
	}
	time.Sleep(time.Second)
	clutime := h.startup.cluster.Load()
	if clutime == 0 {
		nlog.ErrorDepth(1, fmt.Sprintf("%s: cluster is starting up", h))
	}
	return clutime > 0
}

func (h *htrun) ClusterStarted() bool { return h.startup.cluster.Load() > 0 } // see also: p.ready()
func (h *htrun) markClusterStarted()  { h.startup.cluster.Store(mono.NanoTime()) }

func (h *htrun) NodeStarted() bool { return h.startup.node.Load() > 0 }
func (h *htrun) markNodeStarted()  { h.startup.node.Store(mono.NanoTime()) }

func (h *htrun) regNetHandlers(networkHandlers []networkHandler) {
	var (
		path   string
		config = cmn.GCO.Get()
	)
	// common, debug
	for r, nh := range debug.Handlers() {
		handlePub(r, nh)
	}
	// node type specific
	for _, nh := range networkHandlers {
		var reg bool
		if nh.r[0] == '/' { // absolute path
			path = nh.r
		} else {
			path = cos.JoinWords(apc.Version, nh.r)
		}
		debug.Assert(nh.net != 0)
		if nh.net.isSet(accessNetPublic) {
			handlePub(path, nh.h)
			reg = true
		}
		if config.HostNet.UseIntraControl && nh.net.isSet(accessNetIntraControl) {
			handleControl(path, nh.h)
			reg = true
		}
		if config.HostNet.UseIntraData && nh.net.isSet(accessNetIntraData) {
			handleData(path, nh.h)
			reg = true
		}
		if reg {
			continue
		}
		// none of the above
		if !config.HostNet.UseIntraControl && !config.HostNet.UseIntraData {
			// no intra-cluster networks: default to pub net
			handlePub(path, nh.h)
		} else if config.HostNet.UseIntraControl && nh.net.isSet(accessNetIntraData) {
			// (not configured) data defaults to (configured) control
			handleControl(path, nh.h)
		} else {
			debug.Assert(config.HostNet.UseIntraData && nh.net.isSet(accessNetIntraControl))
			// (not configured) control defaults to (configured) data
			handleData(path, nh.h)
		}
	}
	// common Prometheus
	if h.statsT.IsPrometheus() {
		nh := networkHandler{r: "/" + apc.Metrics, h: promhttp.Handler().ServeHTTP}
		path := nh.r // absolute
		handlePub(path, nh.h)
	}
}

func (h *htrun) init(config *cmn.Config) {
	initCtrlClient(config)
	initDataClient(config)

	tcpbuf := config.Net.L4.SndRcvBufSize
	if h.si.IsProxy() {
		tcpbuf = 0
	} else if tcpbuf == 0 {
		tcpbuf = cmn.DefaultSendRecvBufferSize // ditto: targets use AIS default when not configured
	}

	muxers := newMuxers()
	g.netServ.pub = &netServer{muxers: muxers, sndRcvBufSize: tcpbuf}
	g.netServ.control = g.netServ.pub // if not separately configured, intra-control net is public
	if config.HostNet.UseIntraControl {
		muxers = newMuxers()
		g.netServ.control = &netServer{muxers: muxers, sndRcvBufSize: 0}
	}
	g.netServ.data = g.netServ.control // if not configured, intra-data net is intra-control
	if config.HostNet.UseIntraData {
		muxers = newMuxers()
		g.netServ.data = &netServer{muxers: muxers, sndRcvBufSize: tcpbuf}
	}

	h.owner.smap = newSmapOwner(config)
	h.owner.rmd = newRMDOwner()
	h.owner.rmd.load()

	h.gmm = memsys.PageMM()
	h.gmm.RegWithHK()
	h.smm = memsys.ByteMM()
	h.smm.RegWithHK()
}

// steps 1 thru 4
func (h *htrun) initSnode(config *cmn.Config) {
	var (
		pubAddr  meta.NetInfo
		pubExtra []meta.NetInfo
		ctrlAddr meta.NetInfo
		dataAddr meta.NetInfo
		port     = strconv.Itoa(config.HostNet.Port)
		proto    = config.Net.HTTP.Proto
	)
	addrList, err := getLocalIPv4s(config)
	if err != nil {
		cos.ExitLogf("failed to get local IP addr list: %v", err)
	}

	// 1. pub net
	pub, extra := multihome(config.HostNet.Hostname)

	if k8s.IsK8s() && config.HostNet.Hostname != "" {
		// K8s: skip IP addr validation
		// public hostname could be a load balancer's external IP or a service DNS
		nlog.Infof("K8s deployment: skipping hostname validation for %q", config.HostNet.Hostname)
		pubAddr.Init(proto, pub, port)
	} else {
		if err = initNetInfo(&pubAddr, addrList, proto, config.HostNet.Hostname, port); err != nil {
			cos.ExitLogf("failed to get %s IPv4/hostname: %v", cmn.NetPublic, err)
		}
	}
	// multi-home (when config.HostNet.Hostname is a comma-separated list)
	// using the same pub port
	if l := len(extra); l > 0 {
		pubExtra = make([]meta.NetInfo, l)
		for i, addr := range extra {
			pubExtra[i].Init(proto, addr, port)
		}
	} else {
		nlog.Infof("%s (user) access: %v (%q)", cmn.NetPublic, pubAddr, config.HostNet.Hostname)
	}

	// 2. intra-cluster
	ctrlAddr = pubAddr
	if config.HostNet.UseIntraControl {
		icport := strconv.Itoa(config.HostNet.PortIntraControl)
		err = initNetInfo(&ctrlAddr, addrList, proto, config.HostNet.HostnameIntraControl, icport)
		if err != nil {
			cos.ExitLogf("failed to get %s IPv4/hostname: %v", cmn.NetIntraControl, err)
		}
		var s string
		if config.HostNet.HostnameIntraControl != "" {
			s = " (config: " + config.HostNet.HostnameIntraControl + ")"
		}
		nlog.Infof("%s access: %v%s", cmn.NetIntraControl, ctrlAddr, s)
	}
	dataAddr = pubAddr
	if config.HostNet.UseIntraData {
		idport := strconv.Itoa(config.HostNet.PortIntraData)
		err = initNetInfo(&dataAddr, addrList, proto, config.HostNet.HostnameIntraData, idport)
		if err != nil {
			cos.ExitLogf("failed to get %s IPv4/hostname: %v", cmn.NetIntraData, err)
		}
		var s string
		if config.HostNet.HostnameIntraData != "" {
			s = " (config: " + config.HostNet.HostnameIntraData + ")"
		}
		nlog.Infof("%s access: %v%s", cmn.NetIntraData, dataAddr, s)
	}

	// 3. validate
	mustDiffer(pubAddr,
		config.HostNet.Port,
		true,
		ctrlAddr,
		config.HostNet.PortIntraControl,
		config.HostNet.UseIntraControl,
		"pub/ctl",
	)
	mustDiffer(pubAddr,
		config.HostNet.Port,
		true,
		dataAddr,
		config.HostNet.PortIntraData,
		config.HostNet.UseIntraData,
		"pub/data",
	)
	mustDiffer(dataAddr,
		config.HostNet.PortIntraData,
		config.HostNet.UseIntraData,
		ctrlAddr,
		config.HostNet.PortIntraControl,
		config.HostNet.UseIntraControl,
		"ctl/data",
	)

	// 4. new Snode
	h.si = &meta.Snode{
		PubNet:     pubAddr,
		ControlNet: ctrlAddr,
		DataNet:    dataAddr,
	}
	if l := len(pubExtra); l > 0 {
		h.si.PubExtra = make([]meta.NetInfo, l)
		copy(h.si.PubExtra, pubExtra)
		nlog.Infof("%s (multihome) access: %v and %v", cmn.NetPublic, pubAddr, h.si.PubExtra)
	}
}

func mustDiffer(ip1 meta.NetInfo, port1 int, use1 bool, ip2 meta.NetInfo, port2 int, use2 bool, tag string) {
	if !use1 || !use2 {
		return
	}
	if ip1.Hostname == ip2.Hostname && port1 == port2 {
		cos.ExitLogf("%s: cannot use the same IP:port (%s) for two networks", tag, ip1)
	}
}

// at startup, check this Snode vs locally stored Smap replica (NOTE: some errors are FATAL)
func (h *htrun) loadSmap() (smap *smapX, reliable bool) {
	smap = newSmap()
	loaded, err := h.owner.smap.load(smap)

	if err != nil {
		nlog.Errorf("Failed to load cluster map (\"Smap\"): %v - reinitializing", err)
		return
	}
	if !loaded {
		return // no local replica - joining from scratch
	}

	node := smap.GetNode(h.SID())
	if node == nil {
		cos.ExitLogf("%s: %s is not present in the loaded %s", cmn.BadSmapPrefix, h.si, smap.StringEx())
		return
	}
	if node.Type() != h.si.Type() {
		cos.ExitLogf("%s: %s is %q while the node in the loaded %s is %q", cmn.BadSmapPrefix,
			h.si, h.si.Type(), smap.StringEx(), node.Type())
		return
	}

	// NOTE: not enforcing Snode's immutability - in particular, IPs that may change upon restart
	// in certain environments
	if _, err := smap.IsDupNet(h.si); err != nil {
		nlog.Errorln(err)
		return
	}
	reliable = true
	return
}

func (h *htrun) setDaemonConfigMsg(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg, query url.Values) {
	var (
		transient = cos.IsParseBool(query.Get(apc.ActTransient))
		toUpdate  = &cmn.ConfigToSet{}
	)
	if err := cos.MorphMarshal(msg.Value, toUpdate); err != nil {
		h.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, h, msg.Action, msg.Value, err)
		return
	}
	if err := h.owner.config.setDaemonConfig(toUpdate, transient); err != nil {
		h.writeErr(w, r, err)
	}
}

func (h *htrun) setDaemonConfigQuery(w http.ResponseWriter, r *http.Request) {
	var (
		query     = r.URL.Query()
		transient = cos.IsParseBool(query.Get(apc.ActTransient))
		toUpdate  = &cmn.ConfigToSet{}
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

func (h *htrun) run(config *cmn.Config) error {
	var (
		tlsConf *tls.Config
		logger  = log.New(&nlogWriter{}, "net/http err: ", 0) // a wrapper to log http.Server errors
	)
	if config.Net.HTTP.UseHTTPS {
		c, err := newTLS(&config.Net.HTTP)
		if err != nil {
			cos.ExitLog(err)
		}
		tlsConf = c
	}
	if config.HostNet.UseIntraControl {
		go func() {
			_ = g.netServ.control.listen(h.si.ControlNet.TCPEndpoint(), logger, tlsConf, config)
		}()
	}
	if config.HostNet.UseIntraData {
		go func() {
			_ = g.netServ.data.listen(h.si.DataNet.TCPEndpoint(), logger, tlsConf, config)
		}()
	}

	ep := h.si.PubNet.TCPEndpoint()
	if h.pubAddrAny(config) {
		ep = ":" + h.si.PubNet.Port
	} else if len(h.si.PubExtra) > 0 {
		pubAddr2 := h.si.PubExtra[0]
		debug.Assert(pubAddr2.Port == h.si.PubNet.Port)
		g.netServ.pub2 = &netServer{muxers: g.netServ.pub.muxers, sndRcvBufSize: g.netServ.pub.sndRcvBufSize}
		go func() {
			_ = g.netServ.pub2.listen(pubAddr2.TCPEndpoint(), logger, tlsConf, config)
		}()
	}

	return g.netServ.pub.listen(ep, logger, tlsConf, config) // stay here
}

// return true to start listening on `INADDR_ANY:PubNet.Port`
func (h *htrun) pubAddrAny(config *cmn.Config) (inaddrAny bool) {
	switch {
	case config.HostNet.UseIntraControl && h.si.ControlNet.Port == h.si.PubNet.Port:
	case config.HostNet.UseIntraData && h.si.DataNet.Port == h.si.PubNet.Port:
	default:
		inaddrAny = true
	}
	return inaddrAny
}

// remove self from Smap (if required), terminate http, and wait (w/ timeout)
// for running xactions to abort
func (h *htrun) stop(wg *sync.WaitGroup, rmFromSmap bool) {
	const sleep = time.Second >> 1

	if rmFromSmap {
		h.unregisterSelf(true)
	}
	nlog.Infoln("Shutting down HTTP")

	wg.Add(1)
	go func() {
		time.Sleep(sleep)
		shuthttp()
		wg.Done()
	}()
	entry := xreg.GetRunning(xreg.Flt{})
	if entry != nil {
		time.Sleep(sleep)
		entry = xreg.GetRunning(xreg.Flt{})
		if entry != nil {
			nlog.Warningln("Timed out waiting for", entry.Kind(), "... to stop")
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

func (h *htrun) _call(si *meta.Snode, bargs *bcastArgs, results *bcastResults) {
	cargs := allocCargs()
	{
		cargs.si = si
		cargs.req = bargs.req
		cargs.timeout = bargs.timeout
	}
	cargs.req.Base = si.URL(bargs.network)
	if bargs.req.BodyR != nil {
		cargs.req.BodyR, _ = bargs.req.BodyR.(cos.ReadOpenCloser).Open()
	}
	cargs.cresv = bargs.cresv
	res := h.call(cargs, bargs.smap)
	if bargs.async {
		freeCR(res) // discard right away
	} else {
		results.mu.Lock()
		results.s = append(results.s, res)
		results.mu.Unlock()
	}
	freeCargs(cargs)
}

func (h *htrun) call(args *callArgs, smap *smapX) (res *callResult) {
	var (
		req    *http.Request
		resp   *http.Response
		client *http.Client
		sid    = unknownDaemonID
	)
	res = allocCR()
	if args.si != nil {
		sid = args.si.ID()
		res.si = args.si
	}

	debug.Assert(args.si != nil || args.req.Base != "") // either si or base
	if args.req.Base == "" && args.si != nil {
		args.req.Base = args.si.ControlNet.URL // by default, use intra-cluster control network
	}

	if args.req.Header == nil {
		args.req.Header = make(http.Header)
	}

	switch args.timeout {
	case apc.DefaultTimeout:
		req, res.err = args.req.Req()
		if res.err != nil {
			break
		}
		client = g.client.control
	case apc.LongTimeout:
		req, res.err = args.req.Req()
		if res.err != nil {
			break
		}
		client = g.client.data
	default:
		var cancel context.CancelFunc
		if args.timeout == 0 {
			args.timeout = cmn.Rom.CplaneOperation()
		}
		req, _, cancel, res.err = args.req.ReqWithTimeout(args.timeout)
		if res.err != nil {
			break
		}
		defer cancel()

		// NOTE: timeout handling
		// - timeout causes context.deadlineExceededError, i.e. "context deadline exceeded"
		// - the two knobs are configurable via "client_timeout" and "client_long_timeout",
		// respectively (client section in the global config)
		if args.timeout > g.client.control.Timeout {
			client = g.client.data
		} else {
			client = g.client.control
		}
	}
	if res.err != nil {
		res.details = fmt.Sprintf("FATAL: failed to create HTTP request %s %s: %v",
			args.req.Method, args.req.URL(), res.err)
		return
	}

	req.Header.Set(apc.HdrCallerID, h.SID())
	req.Header.Set(apc.HdrCallerName, h.si.Name())
	if smap.vstr != "" {
		if smap.IsPrimary(h.si) {
			req.Header.Set(apc.HdrCallerIsPrimary, "true")
		}
		req.Header.Set(apc.HdrCallerSmapVer, smap.vstr)
	}
	req.Header.Set(cos.HdrUserAgent, ua)

	resp, res.err = client.Do(req)
	if res.err != nil {
		res.details = "[control-plane]" // tcp level, e.g.: connection refused
		return
	}
	defer resp.Body.Close()
	res.status = resp.StatusCode
	res.header = resp.Header

	// err == nil && bad status: resp.Body contains the error message
	if res.status >= http.StatusBadRequest {
		if args.req.Method == http.MethodHead {
			msg := resp.Header.Get(apc.HdrError)
			res.err = res.herr(req, msg)
		} else {
			b := cmn.NewBuffer()
			b.ReadFrom(resp.Body)
			res.err = res.herr(req, b.String())
			cmn.FreeBuffer(b)
		}
		res.details = res.err.Error()
		return
	}

	// read and decode via call result value (`cresv`), if provided
	// othwerwise, read and return bytes for the caller to unmarshal
	if args.cresv != nil {
		res.v = args.cresv.newV()
		args.cresv.read(res, resp.Body)
		if res.err != nil {
			return
		}
	} else {
		res.read(resp.Body)
		if res.err != nil {
			return
		}
	}

	if sid != unknownDaemonID {
		h.keepalive.heardFrom(sid)
	}
	return
}

//
// intra-cluster IPC, control plane: notify another node
//

func (h *htrun) notifyTerm(n core.Notif, err error, aborted bool) {
	h._nfy(n, err, apc.Finished, aborted)
}
func (h *htrun) notifyProgress(n core.Notif) { h._nfy(n, nil, apc.Progress, false) }

func (h *htrun) _nfy(n core.Notif, err error, upon string, aborted bool) {
	var (
		smap  = h.owner.smap.get()
		dsts  = n.Subscribers()
		msg   = n.ToNotifMsg(aborted)
		args  = allocBcArgs()
		nodes = args.selected
	)
	debug.Assert(upon == apc.Progress || upon == apc.Finished)
	if len(dsts) == 1 && dsts[0] == equalIC {
		for pid, psi := range smap.Pmap {
			if smap.IsIC(psi) && pid != h.si.ID() && !psi.InMaintOrDecomm() {
				nodes = append(nodes, psi)
			}
		}
	} else {
		for _, dst := range dsts {
			debug.Assert(dst != equalIC)
			if si := smap.GetActiveNode(dst); si != nil {
				nodes = append(nodes, si)
			} else {
				nlog.Errorln(&errNodeNotFound{"failed to notify", dst, h.si, smap})
			}
		}
	}
	if err != nil {
		msg.ErrMsg = err.Error()
		msg.AbortedX = aborted
	}
	msg.NodeID = h.si.ID()
	if len(nodes) == 0 {
		nlog.Errorf("%s: have no nodes to send [%s] notification", h, &msg)
		return
	}
	path := apc.URLPathNotifs.Join(upon)
	args.req = cmn.HreqArgs{Method: http.MethodPost, Path: path, Body: cos.MustMarshal(&msg)}
	args.network = cmn.NetIntraControl
	args.timeout = cmn.Rom.MaxKeepalive()
	args.selected = nodes
	args.nodeCount = len(nodes)
	args.smap = smap
	args.async = true
	_ = h.bcastSelected(args)
	freeBcArgs(args)
}

//
// intra-cluster comm
//

// bcastGroup broadcasts a message to a specific group of nodes: targets, proxies, all.
func (h *htrun) bcastGroup(args *bcastArgs) sliceResults {
	if args.smap == nil {
		args.smap = h.owner.smap.get()
	}
	present := args.smap.isPresent(h.si)
	if args.network == "" {
		args.network = cmn.NetIntraControl
	}
	debug.Assert(cmn.NetworkIsKnown(args.network))
	if args.timeout == 0 {
		args.timeout = cmn.Rom.CplaneOperation()
		debug.Assert(args.timeout != 0)
	}

	switch args.to {
	case core.Targets:
		args.nodes = []meta.NodeMap{args.smap.Tmap}
		args.nodeCount = len(args.smap.Tmap)
		if present && h.si.IsTarget() {
			args.nodeCount--
		}
	case core.Proxies:
		args.nodes = []meta.NodeMap{args.smap.Pmap}
		args.nodeCount = len(args.smap.Pmap)
		if present && h.si.IsProxy() {
			args.nodeCount--
		}
	case core.AllNodes:
		args.nodes = []meta.NodeMap{args.smap.Pmap, args.smap.Tmap}
		args.nodeCount = len(args.smap.Pmap) + len(args.smap.Tmap)
		if present {
			args.nodeCount--
		}
	case core.SelectedNodes:
		args.nodeCount = len(args.nodes)
		debug.Assert(args.nodeCount > 0)
	default:
		debug.Assert(false, args.to)
	}
	return h.bcastNodes(args)
}

// broadcast to the specified destinations (`bargs.nodes`)
// (if specified, `bargs.req.BodyR` must implement `cos.ReadOpenCloser`)
func (h *htrun) bcastNodes(bargs *bcastArgs) sliceResults {
	var (
		results bcastResults
		wg      = cos.NewLimitedWaitGroup(cmn.MaxParallelism(), bargs.nodeCount)
		f       = func(si *meta.Snode) { h._call(si, bargs, &results); wg.Done() }
	)
	debug.Assert(len(bargs.selected) == 0)
	if !bargs.async {
		results.s = allocBcastRes(len(bargs.nodes))
	}
	for _, nodeMap := range bargs.nodes {
		for _, si := range nodeMap {
			if si.ID() == h.si.ID() {
				continue
			}
			if !bargs.ignoreMaintenance && si.InMaintOrDecomm() {
				continue
			}
			wg.Add(1)
			go f(si)
		}
	}
	wg.Wait()
	return results.s
}

func (h *htrun) bcastSelected(bargs *bcastArgs) sliceResults {
	var (
		results bcastResults
		wg      = cos.NewLimitedWaitGroup(cmn.MaxParallelism(), bargs.nodeCount)
		f       = func(si *meta.Snode) { h._call(si, bargs, &results); wg.Done() }
	)
	debug.Assert(len(bargs.selected) > 0)
	if !bargs.async {
		results.s = allocBcastRes(len(bargs.selected))
	}
	for _, si := range bargs.selected {
		debug.Assert(si.ID() != h.si.ID())
		wg.Add(1)
		go f(si)
	}
	wg.Wait()
	return results.s
}

func (h *htrun) bcastAsyncIC(msg *aisMsg) {
	var (
		wg   = &sync.WaitGroup{}
		smap = h.owner.smap.get()
		args = allocBcArgs()
	)
	args.req = cmn.HreqArgs{Method: http.MethodPost, Path: apc.URLPathIC.S, Body: cos.MustMarshal(msg)}
	args.network = cmn.NetIntraControl
	args.timeout = cmn.Rom.MaxKeepalive()
	for pid, psi := range smap.Pmap {
		if pid == h.si.ID() || !smap.IsIC(psi) || smap.GetActiveNode(pid) == nil {
			continue
		}
		wg.Add(1)
		go func(si *meta.Snode) {
			cargs := allocCargs()
			{
				cargs.si = si
				cargs.req = args.req
				cargs.timeout = args.timeout
			}
			res := h.call(cargs, smap)
			freeCargs(cargs)
			freeCR(res) // discard right away
			wg.Done()
		}(psi)
	}
	wg.Wait()
	freeBcArgs(args)
}

func (h *htrun) bcastAllNodes(w http.ResponseWriter, r *http.Request, args *bcastArgs) {
	args.to = core.AllNodes
	results := h.bcastGroup(args)
	for _, res := range results {
		if res.err != nil {
			h.writeErr(w, r, res.toErr())
			break
		}
	}
	freeBcastRes(results)
}

//
// parsing helpers
//

// remove validated fields and return the resulting slice
func (h *htrun) parseURL(w http.ResponseWriter, r *http.Request, itemsPresent []string, itemsAfter int, splitAfter bool) ([]string, error) {
	items, err := cmn.ParseURL(r.URL.Path, itemsPresent, itemsAfter, splitAfter)
	if err != nil {
		h.writeErr(w, r, err)
	}
	return items, err
}

func (h *htrun) writeMsgPack(w http.ResponseWriter, v msgp.Encodable, tag string) (ok bool) {
	var (
		err       error
		buf, slab = h.gmm.AllocSize(cmn.MsgpLsoBufSize) // max size
		mw        = msgp.NewWriterBuf(w, buf)
	)
	w.Header().Set(cos.HdrContentType, cos.ContentMsgPack)
	if err = v.EncodeMsg(mw); err == nil {
		err = mw.Flush()
	}
	slab.Free(buf)
	if err == nil {
		return true
	}
	h.logerr(tag, v, err)
	return false
}

func (h *htrun) writeJSON(w http.ResponseWriter, r *http.Request, v any, tag string) {
	if err := _writejs(w, r, v); err != nil {
		h.logerr(tag, v, err)
	}
}

// same as above with boolean return to facilitate early termination
func (h *htrun) writeJS(w http.ResponseWriter, r *http.Request, v any, tag string) bool {
	if err := _writejs(w, r, v); err != nil {
		h.logerr(tag, v, err)
		return false
	}
	return true
}

func _writejs(w http.ResponseWriter, r *http.Request, v any) (err error) {
	w.Header().Set(cos.HdrContentType, cos.ContentJSONCharsetUTF)
	if isBrowser(r.Header.Get(cos.HdrUserAgent)) {
		var out []byte
		if out, err = jsoniter.MarshalIndent(v, "", "    "); err == nil {
			w.Header().Set(cos.HdrContentLength, strconv.Itoa(len(out)))
			_, err = w.Write(out)
		}
	} else { // previously: new-encoder(w).encode(v) (non-browser client)
		j := cos.JSON.BorrowStream(nil)
		j.WriteVal(v)
		j.WriteRaw("\n")
		if err = j.Error; err == nil {
			b := j.Buffer()
			w.Header().Set(cos.HdrContentLength, strconv.Itoa(len(b)))
			_, err = w.Write(b)

			// NOTE: consider http.NewResponseController(w).Flush()
		}
		cos.JSON.ReturnStream(j)
	}
	return
}

// See https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/User-Agent
// and https://developer.mozilla.org/en-US/docs/Web/HTTP/Browser_detection_using_the_user_agent
func isBrowser(userAgent string) bool {
	return strings.HasPrefix(userAgent, "Mozilla/5.0")
}

func (h *htrun) logerr(tag string, v any, err error) {
	const maxl = 48
	var efmt, msg string
	if daemon.stopping.Load() {
		return
	}
	if v != nil {
		efmt = fmt.Sprintf("message: {%+v", v)
		if len(efmt) > maxl {
			efmt = efmt[:maxl] + "...}"
		} else {
			efmt += "}"
		}
	}
	efmt = tag + " response error: %v, " + efmt + " at "
	msg = fmt.Sprintf(efmt, err)
	for i := 1; i < 4; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		if i > 1 {
			msg += " <- "
		}
		f := filepath.Base(file)
		msg += fmt.Sprintf("%s:%d", f, line)
	}
	if cos.IsErrBrokenPipe(err) { // client went away
		nlog.Infoln("Warning: " + msg)
	} else {
		nlog.Errorln(msg)
	}
	h.statsT.IncErr(stats.ErrHTTPWriteCount)
}

func _parseNCopies(value any) (copies int64, err error) {
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

func _checkAction(msg *apc.ActMsg, expectedActions ...string) (err error) {
	found := false
	for _, action := range expectedActions {
		found = found || msg.Action == action
	}
	if !found {
		err = fmt.Errorf(fmtErrInvaldAction, msg.Action, expectedActions)
	}
	return
}

//
// common cplane cont-d
//

func (h *htrun) httpdaeget(w http.ResponseWriter, r *http.Request, query url.Values, htext htext) {
	var (
		body any
		what = query.Get(apc.QparamWhat)
	)
	switch what {
	case apc.WhatNodeConfig:
		var (
			c      cmn.Config
			config = cmn.GCO.Get()
		)
		// hide secret
		c = *config
		c.Auth.Secret = "**********"
		body = &c
	case apc.WhatSmap:
		body = h.owner.smap.get()
	case apc.WhatBMD:
		body = h.owner.bmd.get()
	case apc.WhatSmapVote:
		var err error
		body, err = h.cluMeta(cmetaFillOpt{htext: htext, skipPrimeTime: true})
		if err != nil {
			nlog.Errorf("failed to fetch cluster config, err: %v", err)
		}
	case apc.WhatSnode:
		body = h.si
	case apc.WhatLog:
		if cos.IsParseBool(query.Get(apc.QparamAllLogs)) {
			tempdir := h.sendAllLogs(w, r, query)
			if tempdir != "" {
				err := os.RemoveAll(tempdir)
				debug.AssertNoErr(err)
			}
		} else {
			h.sendOneLog(w, r, query)
		}
		return
	case apc.WhatNodeStats:
		statsNode := h.statsT.GetStats()
		statsNode.Snode = h.si
		body = statsNode
	case apc.WhatMetricNames:
		body = h.statsT.GetMetricNames()
	default:
		h.writeErrf(w, r, "invalid GET /daemon request: unrecognized what=%s", what)
		return
	}
	h.writeJSON(w, r, body, "httpdaeget-"+what)
}

func (h *htrun) sendAllLogs(w http.ResponseWriter, r *http.Request, query url.Values) string {
	sev := query.Get(apc.QparamLogSev)
	tempdir, archname, err := h.targzLogs(sev)
	if err != nil {
		h.writeErr(w, r, err)
		return tempdir
	}
	fh, err := os.Open(archname)
	if err != nil {
		h.writeErr(w, r, err)
		return tempdir
	}
	buf, slab := h.gmm.Alloc()
	if written, err := io.CopyBuffer(w, fh, buf); err != nil {
		nlog.Errorf("failed to read %s: %v (written=%d)", archname, err, written)
	}
	cos.Close(fh)
	slab.Free(buf)
	return tempdir
}

func (h *htrun) sendOneLog(w http.ResponseWriter, r *http.Request, query url.Values) {
	sev := query.Get(apc.QparamLogSev)
	log, err := sev2Logname(sev)
	if err != nil {
		h.writeErr(w, r, err)
		return
	}
	fh, err := os.Open(log)
	if err != nil {
		errCode := http.StatusInternalServerError
		if os.IsNotExist(err) {
			errCode = http.StatusNotFound
		}
		h.writeErr(w, r, err, errCode)
		return
	}
	soff := query.Get(apc.QparamLogOff)
	if soff != "" {
		var (
			off   int64
			err   error
			finfo os.FileInfo
		)
		off, err = strconv.ParseInt(soff, 10, 64)
		if err == nil {
			finfo, err = os.Stat(log)
			if err == nil {
				if siz := finfo.Size(); off > siz {
					err = fmt.Errorf("log likely rotated (offset %d, size %d)", off, siz)
				}
			}
		}
		if err == nil {
			_, err = fh.Seek(off, io.SeekStart)
		}
		if err != nil {
			cos.Close(fh)
			h.writeErr(w, r, err)
			return
		}
	}
	buf, slab := h.gmm.Alloc()
	if written, err := io.CopyBuffer(w, fh, buf); err != nil {
		// at this point, http err must be already on its way
		nlog.Errorf("failed to read %s: %v (written=%d)", log, err, written)
	}
	cos.Close(fh)
	slab.Free(buf)
}

// see also: cli 'log get --all'
func (h *htrun) targzLogs(severity string) (tempdir, archname string, err error) {
	var (
		wfh      *os.File
		dentries []os.DirEntry
		logdir   = cmn.GCO.Get().LogDir
	)
	dentries, err = os.ReadDir(logdir)
	if err != nil {
		err = fmt.Errorf("read-dir %w", err)
		return
	}
	tempdir = filepath.Join(os.TempDir(), "aislogs-"+h.SID())
	err = cos.CreateDir(tempdir)
	if err != nil {
		err = fmt.Errorf("create-dir %w", err)
		return
	}
	wfh, err = os.CreateTemp(tempdir, "")
	if err != nil {
		err = fmt.Errorf("create-temp %w", err)
		return
	}
	archname = wfh.Name()
	aw := archive.NewWriter(archive.ExtTarGz, wfh, nil /*checksum*/, nil /*opts*/)

	defer func() {
		aw.Fini()
		wfh.Close()
	}()

	for _, dent := range dentries {
		if !dent.Type().IsRegular() {
			continue
		}
		finfo, errV := dent.Info()
		if errV != nil {
			continue
		}
		var (
			fullPath = filepath.Join(logdir, finfo.Name())
			rfh      *os.File
		)
		if !logname2Sev(fullPath, severity) {
			continue
		}
		rfh, err = os.Open(fullPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return
		}
		oah := cos.SimpleOAH{Size: finfo.Size(), Atime: finfo.ModTime().UnixNano()}
		err = aw.Write(finfo.Name(), oah, rfh)
		rfh.Close()
		if err != nil {
			return
		}
	}
	return
}

func sev2Logname(severity string) (log string, err error) {
	var (
		dir = cmn.GCO.Get().LogDir
		sev = apc.LogInfo[0] // default
	)
	if severity != "" {
		sev = strings.ToLower(severity)[0]
	}
	switch sev {
	case apc.LogInfo[0]:
		log = filepath.Join(dir, nlog.InfoLogName())
	case apc.LogWarn[0], apc.LogErr[0]:
		log = filepath.Join(dir, nlog.ErrLogName())
	default:
		err = fmt.Errorf("unknown log severity %q", severity)
	}
	return
}

func logname2Sev(fname, severity string) bool {
	log, err := sev2Logname(severity)
	if err != nil {
		nlog.Warningln(err)
		return false
	}
	i := strings.LastIndexByte(log, '.')
	if i < 0 {
		nlog.Warningf("%q: unexpected log name format", log)
		return false
	}
	return strings.Contains(fname, log[i:])
}

//
// HTTP err + spec message + code + stats
//

const Silent = 1

func (*htrun) writeErr(w http.ResponseWriter, r *http.Request, err error, errCode ...int) {
	cmn.WriteErr(w, r, err, errCode...) // [errCode[, silent]]
}

func (*htrun) writeErrMsg(w http.ResponseWriter, r *http.Request, msg string, errCode ...int) {
	cmn.WriteErrMsg(w, r, msg, errCode...) // [errCode[, silent]]
}

func (h *htrun) writeErrSilentf(w http.ResponseWriter, r *http.Request, errCode int, format string, a ...any) {
	err := fmt.Errorf(format, a...)
	h.writeErr(w, r, err, errCode, Silent)
}

func (h *htrun) writeErrStatusf(w http.ResponseWriter, r *http.Request, errCode int, format string, a ...any) {
	err := fmt.Errorf(format, a...)
	h.writeErrMsg(w, r, err.Error(), errCode)
}

func (h *htrun) writeErrf(w http.ResponseWriter, r *http.Request, format string, a ...any) {
	err := fmt.Errorf(format, a...)
	if cos.IsNotExist(err, 0) {
		h.writeErrMsg(w, r, err.Error(), http.StatusNotFound)
	} else {
		h.writeErrMsg(w, r, err.Error())
	}
}

func (h *htrun) writeErrURL(w http.ResponseWriter, r *http.Request) {
	if r.URL.Scheme != "" {
		h.writeErrf(w, r, "request '%s %s://%s': invalid URL path", r.Method, r.URL.Scheme, r.URL.Path)
		return
	}
	// ignore GET /favicon.ico by Browsers
	if r.URL.Path == "/favicon.ico" || r.URL.Path == "favicon.ico" {
		return
	}
	h.writeErrf(w, r, "invalid request URI: '%s %s'", r.Method, r.RequestURI)
}

func (h *htrun) writeErrAct(w http.ResponseWriter, r *http.Request, action string) {
	err := cmn.InitErrHTTP(r, fmt.Errorf("invalid action %q", action), 0)
	h.writeErr(w, r, err)
	cmn.FreeHterr(err)
}

func (h *htrun) writeErrActf(w http.ResponseWriter, r *http.Request, action string,
	format string, a ...any) {
	detail := fmt.Sprintf(format, a...)
	err := cmn.InitErrHTTP(r, fmt.Errorf("invalid action %q: %s", action, detail), 0)
	h.writeErr(w, r, err)
	cmn.FreeHterr(err)
}

// also, validatePrefix
func (h *htrun) isValidObjname(w http.ResponseWriter, r *http.Request, name string) bool {
	if err := cmn.ValidateObjName(name); err != nil {
		h.writeErr(w, r, err)
		return false
	}
	return true
}

// health client
func (h *htrun) reqHealth(si *meta.Snode, timeout time.Duration, query url.Values, smap *smapX) (b []byte, status int, err error) {
	var (
		path  = apc.URLPathHealth.S
		url   = si.URL(cmn.NetIntraControl)
		cargs = allocCargs()
	)
	{
		cargs.si = si
		cargs.req = cmn.HreqArgs{Method: http.MethodGet, Base: url, Path: path, Query: query}
		cargs.timeout = timeout
	}
	res := h.call(cargs, smap)
	b, status, err = res.bytes, res.status, res.err
	freeCargs(cargs)
	freeCR(res)
	return
}

// - utilizes reqHealth (above) to discover a _better_ Smap, if exists
// - via getMaxCii.do()
// - checkAll: query all nodes
// - consider adding max-ver BMD bit here as well (TODO)
func (h *htrun) bcastHealth(smap *smapX, checkAll bool) (*clusterInfo, int /*num confirmations*/) {
	if !smap.isValid() {
		nlog.Errorf("%s: cannot execute with invalid %s", h, smap)
		return nil, 0
	}
	c := getMaxCii{
		h:        h,
		maxCii:   &clusterInfo{},
		query:    url.Values{apc.QparamClusterInfo: []string{"true"}},
		timeout:  cmn.Rom.CplaneOperation(),
		checkAll: checkAll,
	}
	c.maxCii.fillSmap(smap)

	h._bch(&c, smap, apc.Proxy)
	if checkAll || (c.cnt < maxVerConfirmations && smap.CountActiveTs() > 0) {
		h._bch(&c, smap, apc.Target)
	}
	nlog.Infoln(h.String()+":", c.maxCii.String())
	return c.maxCii, c.cnt
}

func (h *htrun) _bch(c *getMaxCii, smap *smapX, nodeTy string) {
	var (
		wg       cos.WG
		i, count int
		nodemap  = smap.Pmap
	)
	if nodeTy == apc.Target {
		nodemap = smap.Tmap
	}
	if c.checkAll {
		wg = cos.NewLimitedWaitGroup(cmn.MaxParallelism(), len(nodemap))
	} else {
		count = min(cmn.MaxParallelism(), maxVerConfirmations<<1)
		wg = cos.NewLimitedWaitGroup(count, len(nodemap) /*have*/)
	}
	for sid, si := range nodemap {
		if sid == h.si.ID() {
			continue
		}
		if si.InMaintOrDecomm() {
			continue
		}
		if count > 0 && count < len(nodemap) && i > count {
			if c.haveEnough() {
				break
			}
		}
		wg.Add(1)
		i++
		go c.do(si, wg, smap)
	}
	wg.Wait()
}

//
// metasync Rx
//

func logmsync(ver int64, revs revs, msg *aisMsg, opts ...string) {
	var (
		what   string
		caller = opts[0]
	)
	if len(opts) == 1 {
		what = revs.String()
	} else {
		what = opts[1]
	}
	s := fmt.Sprintf("msync Rx: %s (up from v%d), %s <-- %s", what, ver, msg, caller)
	nlog.InfoDepth(1, s)
}

func (h *htrun) extractConfig(payload msPayload, caller string) (newConfig *globalConfig, msg *aisMsg, err error) {
	if _, ok := payload[revsConfTag]; !ok {
		return
	}
	newConfig, msg = &globalConfig{}, &aisMsg{}
	confValue := payload[revsConfTag]
	reader := bytes.NewBuffer(confValue)
	if _, err1 := jsp.Decode(io.NopCloser(reader), newConfig, newConfig.JspOpts(), "extractConfig"); err1 != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, h, "new Config", cos.BHead(confValue), err1)
		return
	}
	if msgValue, ok := payload[revsConfTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, h, "action message", cos.BHead(msgValue), err1)
			return
		}
	}
	config := cmn.GCO.Get()
	if cmn.Rom.FastV(4, cos.SmoduleAIS) {
		logmsync(config.Version, newConfig, msg, caller)
	}
	if newConfig.version() <= config.Version {
		if newConfig.version() < config.Version {
			err = newErrDowngrade(h.si, config.String(), newConfig.String())
		}
		newConfig = nil
	}
	return
}

func (h *htrun) extractEtlMD(payload msPayload, caller string) (newMD *etlMD, msg *aisMsg, err error) {
	if _, ok := payload[revsEtlMDTag]; !ok {
		return
	}
	newMD, msg = newEtlMD(), &aisMsg{}
	etlMDValue := payload[revsEtlMDTag]
	reader := bytes.NewBuffer(etlMDValue)
	if _, err1 := jsp.Decode(io.NopCloser(reader), newMD, newMD.JspOpts(), "extractEtlMD"); err1 != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, h, "new EtlMD", cos.BHead(etlMDValue), err1)
		return
	}
	if msgValue, ok := payload[revsEtlMDTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, h, "action message", cos.BHead(msgValue), err1)
			return
		}
	}
	etlMD := h.owner.etl.get()
	if cmn.Rom.FastV(4, cos.SmoduleAIS) {
		logmsync(etlMD.Version, newMD, msg, caller)
	}
	if newMD.version() <= etlMD.version() {
		if newMD.version() < etlMD.version() {
			err = newErrDowngrade(h.si, etlMD.String(), newMD.String())
		}
		newMD = nil
	}
	return
}

func (h *htrun) extractSmap(payload msPayload, caller string, skipValidation bool) (newSmap *smapX, msg *aisMsg, err error) {
	if _, ok := payload[revsSmapTag]; !ok {
		return
	}
	newSmap, msg = &smapX{}, &aisMsg{}
	smapValue := payload[revsSmapTag]
	reader := bytes.NewBuffer(smapValue)
	if _, err1 := jsp.Decode(io.NopCloser(reader), newSmap, newSmap.JspOpts(), "extractSmap"); err1 != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, h, "new Smap", cos.BHead(smapValue), err1)
		return
	}
	if msgValue, ok := payload[revsSmapTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, h, "action message", cos.BHead(msgValue), err1)
			return
		}
	}
	if skipValidation {
		return
	}

	var (
		smap        = h.owner.smap.get()
		curVer      = smap.version()
		isManualReb = msg.Action == apc.ActRebalance && msg.Value != nil
	)
	if newSmap.version() == curVer && !isManualReb {
		newSmap = nil
		return
	}
	if !newSmap.isValid() {
		err = cmn.NewErrFailedTo(h, "extract", newSmap, newSmap.validate())
		return
	}
	if !newSmap.isPresent(h.si) {
		err = fmt.Errorf("%s: not finding ourselves in %s", h, newSmap)
		return
	}
	if err = smap.validateUUID(h.si, newSmap, caller, 50 /* ciError */); err != nil {
		return // FATAL: cluster integrity error
	}
	if cmn.Rom.FastV(4, cos.SmoduleAIS) {
		logmsync(smap.Version, newSmap, msg, caller)
	}
	_, sameOrigin, _, eq := smap.Compare(&newSmap.Smap)
	debug.Assert(sameOrigin)
	if newSmap.version() < curVer {
		if !eq {
			err = newErrDowngrade(h.si, smap.StringEx(), newSmap.StringEx())
			return
		}
		nlog.Warningf("%s: %s and %s are otherwise identical", h.si, newSmap.StringEx(), smap.StringEx())
		newSmap = nil
	}
	return
}

func (h *htrun) extractRMD(payload msPayload, caller string) (newRMD *rebMD, msg *aisMsg, err error) {
	if _, ok := payload[revsRMDTag]; !ok {
		return
	}
	newRMD, msg = &rebMD{}, &aisMsg{}
	rmdValue := payload[revsRMDTag]
	if err1 := jsoniter.Unmarshal(rmdValue, newRMD); err1 != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, h, "new RMD", cos.BHead(rmdValue), err1)
		return
	}
	if msgValue, ok := payload[revsRMDTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, h, "action message", cos.BHead(msgValue), err1)
			return
		}
	}
	rmd := h.owner.rmd.get()
	if cmn.Rom.FastV(4, cos.SmoduleAIS) {
		logmsync(rmd.Version, newRMD, msg, caller)
	}
	if newRMD.version() <= rmd.version() {
		if newRMD.version() < rmd.version() {
			err = newErrDowngrade(h.si, rmd.String(), newRMD.String())
		}
		newRMD = nil
	}
	return
}

func (h *htrun) extractBMD(payload msPayload, caller string) (newBMD *bucketMD, msg *aisMsg, err error) {
	if _, ok := payload[revsBMDTag]; !ok {
		return
	}
	newBMD, msg = &bucketMD{}, &aisMsg{}
	bmdValue := payload[revsBMDTag]
	reader := bytes.NewBuffer(bmdValue)
	if _, err1 := jsp.Decode(io.NopCloser(reader), newBMD, newBMD.JspOpts(), "extractBMD"); err1 != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, h, "new BMD", cos.BHead(bmdValue), err1)
		return
	}
	if msgValue, ok := payload[revsBMDTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, h, "action message", cos.BHead(msgValue), err1)
			return
		}
	}
	bmd := h.owner.bmd.get()
	if cmn.Rom.FastV(4, cos.SmoduleAIS) {
		logmsync(bmd.Version, newBMD, msg, caller)
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

func (h *htrun) receiveSmap(newSmap *smapX, msg *aisMsg, payload msPayload, caller string, cb smapUpdatedCB) error {
	if newSmap == nil {
		return nil
	}
	smap := h.owner.smap.get()
	logmsync(smap.Version, newSmap, msg, caller, newSmap.StringEx())

	if !newSmap.isPresent(h.si) {
		return fmt.Errorf("%s: not finding self in the new %s", h, newSmap)
	}
	return h.owner.smap.synchronize(h.si, newSmap, payload, cb)
}

func (h *htrun) receiveEtlMD(newEtlMD *etlMD, msg *aisMsg, payload msPayload, caller string, cb func(ne, oe *etlMD)) (err error) {
	if newEtlMD == nil {
		return
	}
	etlMD := h.owner.etl.get()
	logmsync(etlMD.Version, newEtlMD, msg, caller)

	h.owner.etl.Lock()
	etlMD = h.owner.etl.get()
	if newEtlMD.version() <= etlMD.version() {
		h.owner.etl.Unlock()
		if newEtlMD.version() < etlMD.version() {
			err = newErrDowngrade(h.si, etlMD.String(), newEtlMD.String())
		}
		return
	}
	err = h.owner.etl.putPersist(newEtlMD, payload)
	h.owner.etl.Unlock()
	debug.AssertNoErr(err)

	if cb != nil {
		cb(newEtlMD, etlMD)
	}
	return
}

// under lock
func (h *htrun) _recvCfg(newConfig *globalConfig, payload msPayload) (err error) {
	config := cmn.GCO.Get()
	if newConfig.version() <= config.Version {
		if newConfig.version() == config.Version {
			return
		}
		return newErrDowngrade(h.si, config.String(), newConfig.String())
	}
	if err = h.owner.config.persist(newConfig, payload); err != nil {
		return
	}
	if err = cmn.GCO.Update(&newConfig.ClusterConfig); err != nil {
		return
	}

	// update assorted read-mostly knobs
	cmn.Rom.Set(&newConfig.ClusterConfig)
	return
}

func (h *htrun) extractRevokedTokenList(payload msPayload, caller string) (*tokenList, error) {
	var (
		msg       aisMsg
		bytes, ok = payload[revsTokenTag]
	)
	if !ok {
		return nil, nil
	}
	if msgValue, ok := payload[revsTokenTag+revsActionTag]; ok {
		if err := jsoniter.Unmarshal(msgValue, &msg); err != nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, h, "action message", cos.BHead(msgValue), err)
			return nil, err
		}
	}
	tokenList := &tokenList{}
	if err := jsoniter.Unmarshal(bytes, tokenList); err != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, h, "blocked token list", cos.BHead(bytes), err)
		return nil, err
	}
	nlog.Infof("extract token list from %q (count: %d, action: %q, uuid: %q)", caller,
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
//   - first, there's the primary proxy/gateway referenced by the current cluster map
//     or - during the cluster deployment time - by the the configured "primary_url"
//     (see /deploy/dev/local/aisnode_config.sh)
//   - if that one fails, the new node goes ahead and tries the alternatives:
//   - config.Proxy.PrimaryURL   ("primary_url")
//   - config.Proxy.DiscoveryURL ("discovery_url")
//   - config.Proxy.OriginalURL  ("original_url")
//   - if these fails we try the candidates provided by the caller.
//
// ================================== Background =========================================
func (h *htrun) join(query url.Values, htext htext, contactURLs ...string) (res *callResult) {
	var (
		config                   = cmn.GCO.Get()
		candidates               = make([]string, 0, 4+len(contactURLs))
		selfPublicURL, pubValid  = cos.ParseURL(h.si.URL(cmn.NetPublic))
		selfIntraURL, intraValid = cos.ParseURL(h.si.URL(cmn.NetIntraControl))
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
		resPrev *callResult
	)
	debug.Assert(pubValid && intraValid)
	primaryURL, psi := h.getPrimaryURLAndSI(nil)
	addCandidate(primaryURL)
	if psi != nil {
		addCandidate(psi.URL(cmn.NetPublic))
	}
	addCandidate(config.Proxy.PrimaryURL)
	addCandidate(config.Proxy.DiscoveryURL)
	addCandidate(config.Proxy.OriginalURL)
	for _, u := range contactURLs {
		addCandidate(u)
	}
	sleep := max(2*time.Second, cmn.Rom.MaxKeepalive())
	for i := 0; i < 4; i++ {
		for _, candidateURL := range candidates {
			if daemon.stopping.Load() {
				return
			}
			if resPrev != nil {
				freeCR(resPrev)
				resPrev = nil //nolint:ineffassign // readability
			}
			res = h.regTo(candidateURL, nil, apc.DefaultTimeout, query, htext, false /*keepalive*/)
			if res.err == nil {
				nlog.Infoln(h.String()+": primary responded Ok via", candidateURL)
				return // ok
			}
			resPrev = res
		}
		time.Sleep(sleep)
	}

	smap := h.owner.smap.get()
	if smap.validate() != nil {
		return
	}

	// Failed to join cluster using config, try getting primary URL using existing smap.
	cii, _ := h.bcastHealth(smap, false /*checkAll*/)
	if cii == nil || cii.Smap.Version < smap.version() {
		return
	}
	primaryURL = cii.Smap.Primary.PubURL

	// Daemon is stopping skip register
	if daemon.stopping.Load() {
		return
	}
	if resPrev != nil {
		freeCR(resPrev)
	}
	res = h.regTo(primaryURL, nil, apc.DefaultTimeout, query, htext, false /*keepalive*/)
	if res.err == nil {
		nlog.Infoln(h.String()+": joined cluster via", primaryURL)
	}
	return
}

func (h *htrun) regTo(url string, psi *meta.Snode, tout time.Duration, q url.Values, htext htext, keepalive bool) *callResult {
	var (
		path          string
		skipPrxKalive = h.si.IsProxy() || keepalive
		opts          = cmetaFillOpt{
			htext:         htext,
			skipSmap:      skipPrxKalive,
			skipBMD:       skipPrxKalive,
			skipRMD:       keepalive,
			skipConfig:    keepalive,
			skipEtlMD:     keepalive,
			fillRebMarker: !keepalive,
			skipPrimeTime: true,
		}
	)
	cm, err := h.cluMeta(opts)
	if err != nil {
		res := allocCR()
		res.err = err
		return res
	}

	if keepalive {
		path = apc.URLPathCluKalive.S
	} else {
		path = apc.URLPathCluAutoReg.S
	}
	cargs := allocCargs()
	{
		cargs.si = psi
		cargs.req = cmn.HreqArgs{Method: http.MethodPost, Base: url, Path: path, Query: q, Body: cos.MustMarshal(cm)}
		cargs.timeout = tout
	}
	smap := cm.Smap
	if smap == nil {
		smap = h.owner.smap.get()
	}
	res := h.call(cargs, smap)
	freeCargs(cargs)
	return res
}

func (h *htrun) sendKalive(smap *smapX, htext htext, timeout time.Duration, fast bool) (pid string, status int, err error) {
	if daemon.stopping.Load() {
		err = errors.New(h.String() + " is stopping")
		return
	}
	primaryURL, psi := h.getPrimaryURLAndSI(smap)
	pid = psi.ID()

	if fast {
		// fast path
		debug.Assert(h.ClusterStarted())
		path := apc.URLPathCluKalive.Join(h.SID())
		cargs := allocCargs()
		{
			cargs.si = psi
			cargs.req = cmn.HreqArgs{Method: http.MethodPost, Base: primaryURL, Path: path}
			cargs.timeout = timeout
		}
		res := h.call(cargs, smap)
		freeCargs(cargs)
		err = res.err
		freeCR(res)
		return
	}

	// slow path
	res := h.regTo(primaryURL, psi, timeout, nil, htext, true /*keepalive*/)
	if res.err != nil {
		if strings.Contains(res.err.Error(), ciePrefix) {
			cos.ExitLog(res.err) // FATAL: cluster integrity error (cie)
		}
		status, err = res.status, res.err
		freeCR(res)
		return
	}
	freeCR(res)
	return
}

func (h *htrun) getPrimaryURLAndSI(smap *smapX) (url string, psi *meta.Snode) {
	if smap == nil {
		smap = h.owner.smap.get()
	}
	if smap.validate() != nil {
		url, psi = cmn.GCO.Get().Proxy.PrimaryURL, nil
		return
	}
	url, psi = smap.Primary.URL(cmn.NetIntraControl), smap.Primary
	return
}

func (h *htrun) pollClusterStarted(config *cmn.Config, psi *meta.Snode) (maxCii *clusterInfo) {
	var (
		sleep, total, rediscover time.Duration
		healthTimeout            = config.Timeout.CplaneOperation.D()
		query                    = url.Values{apc.QparamAskPrimary: []string{"true"}}
	)
	for {
		sleep = min(cmn.Rom.MaxKeepalive(), sleep+time.Second)
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
			nlog.Warningf("%s: started as a non-primary and got ELECTED during startup", h.si)
			return
		}
		if _, _, err := h.reqHealth(smap.Primary, healthTimeout, query /*ask primary*/, smap); err == nil {
			// log
			s := fmt.Sprintf("%s via primary health: cluster startup Ok, %s", h.si, smap.StringEx())
			if self := smap.GetNode(h.si.ID()); self == nil {
				nlog.Warningln(s + "; NOTE: not present in the cluster map")
			} else if self.Flags.IsSet(meta.SnodeMaint) {
				nlog.Warningln(s + "; NOTE: starting in maintenance mode")
			} else if rmd := h.owner.rmd.get(); rmd != nil && rmd.version() > 0 {
				nlog.Infoln(s + ", " + rmd.String())
			} else {
				nlog.Infoln(s)
			}
			return
		}

		if rediscover >= config.Timeout.Startup.D()/2 {
			rediscover = 0
			if cii, cnt := h.bcastHealth(smap, true /*checkAll*/); cii != nil && cii.Smap.Version > smap.version() {
				var pid string
				if psi != nil {
					pid = psi.ID()
				}
				if cii.Smap.Primary.ID != pid && cnt >= maxVerConfirmations {
					nlog.Warningf("%s: change of primary %s => %s - must rejoin",
						h.si, pid, cii.Smap.Primary.ID)
					maxCii = cii
					return
				}
			}
		}
		if total > config.Timeout.Startup.D() {
			nlog.Errorln(h.String() + ": " + cmn.StartupMayTimeout)
		}
	}
}

func (h *htrun) unregisterSelf(ignoreErr bool) (err error) {
	var status int
	smap := h.owner.smap.get()
	if smap == nil || smap.validate() != nil {
		return
	}
	cargs := allocCargs()
	{
		cargs.si = smap.Primary
		cargs.req = cmn.HreqArgs{Method: http.MethodDelete, Path: apc.URLPathCluDaemon.Join(h.si.ID())}
		cargs.timeout = apc.DefaultTimeout
	}
	res := h.call(cargs, smap)
	status, err = res.status, res.err
	if err != nil {
		f := nlog.Errorf
		if ignoreErr {
			f = nlog.Infof
		}
		f("%s: failed to unreg self, err: %v(%d)", h.si, err, status)
	}
	freeCargs(cargs)
	freeCR(res)
	return
}

// via /health handler
func (h *htrun) externalWD(w http.ResponseWriter, r *http.Request) (responded bool) {
	callerID := r.Header.Get(apc.HdrCallerID)
	caller := r.Header.Get(apc.HdrCallerName)
	// external call
	if callerID == "" && caller == "" {
		readiness := cos.IsParseBool(r.URL.Query().Get(apc.QparamHealthReadiness))
		if cmn.Rom.FastV(5, cos.SmoduleAIS) {
			nlog.Infof("%s: external health-ping from %s (readiness=%t)", h.si, r.RemoteAddr, readiness)
		}
		// respond with 503 as per https://tools.ietf.org/html/rfc7231#section-6.6.4
		// see also:
		// * https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes
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

//
// intra-cluster request validations and helpers
//

func (h *htrun) isIntraCall(hdr http.Header, fromPrimary bool) (err error) {
	debug.Assert(hdr != nil)
	var (
		smap       = h.owner.smap.get()
		callerID   = hdr.Get(apc.HdrCallerID)
		callerName = hdr.Get(apc.HdrCallerName)
		callerSver = hdr.Get(apc.HdrCallerSmapVer)
		callerVer  int64
		erP        error
	)
	if ok := callerID != "" && callerName != ""; !ok {
		return fmt.Errorf("%s: expected %s request", h, cmn.NetIntraControl)
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
			nlog.Errorln(erP)
			return
		}
		// we still trust the request when the sender's Smap is more current
		if callerVer > smap.version() {
			if h.ClusterStarted() {
				nlog.Errorf("%s: %s < Smap(v%s) from %s - proceeding anyway...", h, smap, callerSver, callerName)
			}
			runtime.Gosched()
			return
		}
	}
	if caller == nil {
		if !fromPrimary {
			// assume request from a newly joined node and proceed
			return nil
		}
		return fmt.Errorf("%s: expected %s from a valid node, %s", h, cmn.NetIntraControl, smap)
	}
	return fmt.Errorf("%s: expected %s from primary (and not %s), %s", h, cmn.NetIntraControl, caller, smap)
}

func (h *htrun) ensureIntraControl(w http.ResponseWriter, r *http.Request, onlyPrimary bool) (isIntra bool) {
	err := h.isIntraCall(r.Header, onlyPrimary)
	if err != nil {
		h.writeErr(w, r, err)
		return
	}
	if !cmn.GCO.Get().HostNet.UseIntraControl {
		return true // intra-control == pub
	}
	// NOTE: not checking r.RemoteAddr
	intraAddr := h.si.ControlNet.TCPEndpoint()
	srvAddr := r.Context().Value(http.ServerContextKey).(*http.Server).Addr
	if srvAddr == intraAddr {
		return true
	}
	h.writeErrf(w, r, "%s: expected %s request", h, cmn.NetIntraControl)
	return
}

func (h *htrun) uptime2hdr(hdr http.Header) {
	now := mono.NanoTime()
	hdr.Set(apc.HdrNodeUptime, strconv.FormatInt(now-h.startup.node.Load(), 10))
	hdr.Set(apc.HdrClusterUptime, strconv.FormatInt(now-h.startup.cluster.Load(), 10))
}

// NOTE: not checking vs Smap (yet)
func isT2TPut(hdr http.Header) bool { return hdr != nil && hdr.Get(apc.HdrT2TPutterID) != "" }

func isRedirect(q url.Values) (ptime string) {
	if len(q) == 0 || q.Get(apc.QparamProxyID) == "" {
		return
	}
	return q.Get(apc.QparamUnixTime)
}

func ptLatency(tts int64, ptime, isPrimary string) (dur int64) {
	pts, err := cos.S2UnixNano(ptime)
	if err != nil {
		debug.AssertNoErr(err)
		return
	}
	if ok, _ := cos.ParseBool(isPrimary); ok {
		xreg.PrimeTime.Store(pts)
		xreg.MyTime.Store(tts)
	}
	dur = tts - pts
	if dur < 0 && -dur < int64(clusterClockDrift) {
		dur = 0
	}
	return
}

//
// aisMsg reader & constructors
//

func (*htrun) readAisMsg(w http.ResponseWriter, r *http.Request) (msg *aisMsg, err error) {
	msg = &aisMsg{}
	err = cmn.ReadJSON(w, r, msg)
	return
}

func (msg *aisMsg) String() string {
	s := "aism[" + msg.Action
	if msg.UUID != "" {
		s += "[" + msg.UUID + "]"
	}
	if msg.Name != "" {
		s += ", name=" + msg.Name
	}
	return s + "]"
}

func (msg *aisMsg) StringEx() (s string) {
	s = msg.String()
	vs, err := jsoniter.Marshal(msg.Value)
	debug.AssertNoErr(err)
	s += ",(" + strings.ReplaceAll(string(vs), ",", ", ") + ")"
	return
}

func (h *htrun) newAmsgStr(msgStr string, bmd *bucketMD) *aisMsg {
	return h.newAmsg(&apc.ActMsg{Value: msgStr}, bmd)
}

func (h *htrun) newAmsgActVal(act string, val any) *aisMsg {
	return h.newAmsg(&apc.ActMsg{Action: act, Value: val}, nil)
}

func (h *htrun) newAmsg(actionMsg *apc.ActMsg, bmd *bucketMD, uuid ...string) *aisMsg {
	msg := &aisMsg{ActMsg: *actionMsg}
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

// apc.ActMsg c-tor and reader
func (*htrun) readActionMsg(w http.ResponseWriter, r *http.Request) (msg *apc.ActMsg, err error) {
	msg = &apc.ActMsg{}
	err = cmn.ReadJSON(w, r, msg)
	return
}

// cmn.ReadJSON with the only difference: EOF is ok
func readJSON(w http.ResponseWriter, r *http.Request, out any) (err error) {
	err = jsoniter.NewDecoder(r.Body).Decode(out)
	cos.Close(r.Body)
	if err == nil || err == io.EOF {
		return nil
	}
	return cmn.WriteErrJSON(w, r, out, err)
}

// (via apc.WhatNodeStatsAndStatus)
func (h *htrun) _status(smap *smapX) (daeStatus string) {
	self := smap.GetNode(h.si.ID()) // updated flags
	switch {
	case self.Flags.IsSet(meta.SnodeMaint):
		daeStatus = apc.NodeMaintenance
	case self.Flags.IsSet(meta.SnodeDecomm):
		daeStatus = apc.NodeDecommission
	}
	return
}

////////////////
// callResult //
////////////////

// error helpers for intra-cluster calls

func (res *callResult) unwrap() (err error) {
	err = errors.Unwrap(res.err)
	if err == nil {
		err = res.err
	}
	return
}

func (res *callResult) toErr() error {
	if res.err == nil {
		return nil
	}
	// is cmn.ErrHTTP
	if herr := cmn.Err2HTTPErr(res.err); herr != nil {
		// add status, details
		if res.status >= http.StatusBadRequest {
			herr.Status = res.status
		}
		if herr.Message == "" {
			herr.Message = res.details
		}
		return herr
	}
	// res => cmn.ErrHTTP
	if res.status >= http.StatusBadRequest {
		var detail string
		if res.details != "" {
			detail = "[" + res.details + "]"
		}
		return res.herr(nil, fmt.Sprintf("%v%s", res.err, detail))
	}
	if res.details == "" {
		return res.err
	}
	return cmn.NewErrFailedTo(nil, "call "+res.si.StringEx(), res.details, res.err)
}

func (res *callResult) herr(r *http.Request, msg string) *cmn.ErrHTTP {
	orig := &cmn.ErrHTTP{}
	if e := jsoniter.Unmarshal([]byte(msg), orig); e == nil {
		return orig
	}
	nherr := cmn.NewErrHTTP(r, errors.New(msg), res.status)
	if res.si != nil {
		nherr.Node = res.si.StringEx()
	}
	return nherr
}

func (res *callResult) errorf(format string, a ...any) error {
	debug.Assert(res.err != nil)
	// add formatted
	msg := fmt.Sprintf(format, a...)
	if herr := cmn.Err2HTTPErr(res.err); herr != nil {
		herr.Message = msg + ": " + herr.Message
		res.err = herr
	} else {
		res.err = errors.New(msg + ": " + res.err.Error())
	}
	return res.toErr()
}
