// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/certloader"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/tracing"
	"github.com/NVIDIA/aistore/xact/xreg"

	jsoniter "github.com/json-iterator/go"
	"github.com/tinylib/msgp/msgp"
)

const ciePrefix = "cluster integrity error cie#"

const notPresentInSmap = `
%s: %s (self) is not present in the local copy of the %s

-----------------
To troubleshoot:
1. first, make sure you are not trying to run two different %s on the same machine
2. double check "fspaths" config (used to find ais target's volume metadata and load its node ID)
3. if none of the above helps, remove possibly outdated cluster map from the %s (located at %s)
4. restart %s
-----------------`

const dfltDetail = "[control-plane]"

// extra or extended state - currently, target only
type htext interface {
	interruptedRestarted() (bool, bool)
}

type ratelim struct {
	sync.Map
	now int64
}

type htrun struct {
	owner struct {
		smap   *smapOwner
		bmd    bmdOwner // interface with proxy and target impl-s
		rmd    *rmdOwner
		config *configOwner
		etl    etlOwner
	}
	keepalive keepaliver
	statsT    stats.Tracker
	si        *meta.Snode
	gmm       *memsys.MMSA // system pagesize-based memory manager and slab allocator
	smm       *memsys.MMSA // small-size allocator (up to 4K)
	ratelim   ratelim
	startup   struct {
		cluster atomic.Int64 // mono.NanoTime() since cluster startup, zero prior to that
		node    atomic.Int64 // ditto - for this node
	}
}

///////////
// htrun //
///////////

// interface guard
var _ core.Node = (*htrun)(nil)

func (h *htrun) Snode() *meta.Snode { return h.si }
func (h *htrun) SID() string        { return h.si.ID() }
func (h *htrun) String() string     { return h.si.String() }

func (h *htrun) Bowner() meta.Bowner { return h.owner.bmd }
func (h *htrun) Sowner() meta.Sowner { return h.owner.smap }

func (h *htrun) StatsUpdater() cos.StatsUpdater { return h.statsT }

func (h *htrun) PageMM() *memsys.MMSA { return h.gmm }
func (h *htrun) ByteMM() *memsys.MMSA { return h.smm }

func (h *htrun) errStopping() error {
	return errors.New(h.si.Name() + " is stopping")
}

// NOTE: currently, only 'resume' (see also: kaSuspendMsg)
func (h *htrun) smapUpdatedCB(_, _ *smapX, nfl, ofl cos.BitFlags) {
	if ofl.IsAnySet(meta.SnodeMaintDecomm) && !nfl.IsAnySet(meta.SnodeMaintDecomm) {
		h.statsT.ClrFlag(cos.NodeAlerts, cos.MaintenanceMode)
		h.keepalive.ctrl(kaResumeMsg)
	}
}

func (h *htrun) parseReq(w http.ResponseWriter, r *http.Request, apireq *apiRequest) (err error) {
	debug.Assert(len(apireq.prefix) != 0)
	apireq.items, err = h.parseURL(w, r, apireq.prefix, apireq.after, false)
	if err != nil {
		return err
	}
	debug.Assert(len(apireq.items) > apireq.bckIdx)
	bckName := apireq.items[apireq.bckIdx]

	if apireq.dpq != nil {
		if err = apireq.dpq.parse(r.URL.RawQuery); err != nil {
			h.writeErr(w, r, err)
			return err
		}
	} else {
		apireq.query = r.URL.Query()
	}

	if apireq.bck, err = newBckFromQ(bckName, apireq.query, apireq.dpq); err != nil {
		h.writeErr(w, r, err)
	}
	return err
}

func (h *htrun) cluMeta(opts cmetaFillOpt) (*cluMeta, error) {
	cm := &cluMeta{SI: h.si}
	if voteInProgress() != nil {
		cm.Flags = cm.Flags.Set(cos.VoteInProgress)
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
		rebInterrupted, restarted := opts.htext.interruptedRestarted()
		if rebInterrupted {
			cm.Flags = cm.Flags.Set(cos.RebalanceInterrupted)
		}
		if restarted {
			cm.Flags = cm.Flags.Set(cos.NodeRestarted)
		}
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

func (h *htrun) markClusterStarted() {
	h.startup.cluster.Store(mono.NanoTime())
	h.statsT.SetFlag(cos.NodeAlerts, cos.ClusterStarted)
}

func (h *htrun) NodeStarted() bool { return h.startup.node.Load() > 0 }

func (h *htrun) markNodeStarted() {
	h.startup.node.Store(mono.NanoTime())
	h.statsT.SetFlag(cos.NodeAlerts, cos.NodeStarted)
}

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
			path = cos.JoinW0(apc.Version, nh.r)
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
		switch {
		case !config.HostNet.UseIntraControl && !config.HostNet.UseIntraData:
			// no intra-cluster networks: default to pub net
			handlePub(path, nh.h)
		case config.HostNet.UseIntraControl && nh.net.isSet(accessNetIntraData):
			// (not configured) data defaults to (configured) control
			handleControl(path, nh.h)
		default:
			debug.Assert(config.HostNet.UseIntraData && nh.net.isSet(accessNetIntraControl))
			// (not configured) control defaults to (configured) data
			handleData(path, nh.h)
		}
	}
	// common Prometheus
	if handler := h.statsT.PromHandler(); handler != nil {
		nh := networkHandler{r: "/" + apc.Metrics, h: handler.ServeHTTP}
		path := nh.r // absolute
		handlePub(path, nh.h)
	}
}

func (h *htrun) init(config *cmn.Config) {
	// before newTLS() below & before intra-cluster clients
	if config.Net.HTTP.UseHTTPS {
		if err := certloader.Init(config.Net.HTTP.Certificate, config.Net.HTTP.CertKey, h.statsT); err != nil {
			cos.ExitLog(err)
		}
	}

	initCtrlClient(config)
	initDataClient(config)

	tcpbuf := config.Net.L4.SndRcvBufSize
	if h.si.IsProxy() {
		tcpbuf = 0
	} else if tcpbuf == 0 {
		tcpbuf = cmn.DefaultSndRcvBufferSize // ditto: targets use AIS default when not configured
	}

	// PubNet enable tracing when configuration is set.
	muxers := newMuxers(tracing.IsEnabled())
	g.netServ.pub = &netServer{muxers: muxers, sndRcvBufSize: tcpbuf}
	g.netServ.control = g.netServ.pub // if not separately configured, intra-control net is public
	if config.HostNet.UseIntraControl {
		// TODO: for now tracing is always disabled for intra-cluster traffic.
		// Allow enabling through config.
		muxers = newMuxers(false /*enableTracing*/)
		g.netServ.control = &netServer{muxers: muxers, sndRcvBufSize: 0, lowLatencyToS: true}
	}
	g.netServ.data = g.netServ.control // if not configured, intra-data net is intra-control
	if config.HostNet.UseIntraData {
		// TODO: for now tracing is always disabled for intra-data traffic.
		// Allow enabling through config.
		muxers = newMuxers(false /*enableTracing*/)
		g.netServ.data = &netServer{muxers: muxers, sndRcvBufSize: tcpbuf}
	}

	h.owner.smap = newSmapOwner(config)
	h.owner.rmd = newRMDOwner(config)
	h.owner.rmd.load()

	h.gmm = memsys.PageMM()
	h.gmm.RegWithHK()
	h.smm = memsys.ByteMM()
	h.smm.RegWithHK()

	hk.Reg("rate-limit"+hk.NameSuffix, h.ratelim.housekeep, hk.PruneRateLimiters)
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

	if l := len(addrList); l > 1 {
		if config.HostNet.Hostname == "" || cmn.Rom.V(4, cos.ModAIS) {
			nlog.Infoln(l, "local unicast IPs:")
			for _, addr := range addrList {
				nlog.Infoln("\t", addr.String())
			}
		}
	}

	// 1. pub net

	// the "hostname" field can be a single IP address or DNS hostname;
	// it can also be a comma-separated list of IP addresses (or DNS hostnames), in which case the function
	// returns pub = list[0] and extra = list[1:]
	pub, extra := multihome(config.HostNet.Hostname)

	if k8s.IsK8s() && config.HostNet.Hostname != "" {
		// K8s: skip IP addr validation
		// public hostname could be a load balancer's external IP or a service DNS
		nlog.Infoln("K8s deployment: skipping hostname validation for", config.HostNet.Hostname)
		pubAddr.Init(proto, pub, port)
	} else if err = initNetInfo(&pubAddr, addrList, proto, config.HostNet.Hostname, port); err != nil {
		cos.ExitLogf("failed to get %s IPv4/hostname: %v", cmn.NetPublic, err)
	}

	// multi-home (when config.HostNet.Hostname is a comma-separated list)
	// using the same pub port
	if l := len(extra); l > 0 {
		pubExtra = make([]meta.NetInfo, l)
		for i, addr := range extra {
			pubExtra[i].Init(proto, addr, port)
		}
		// already logged (pub, extra)
	} else {
		var s string
		if config.HostNet.Hostname != "" {
			s = " (config: " + config.HostNet.Hostname + ")"
		}
		nlog.Infof("%s (user) access: %v%s", cmn.NetPublic, pubAddr, s)
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
		nlog.Errorln(h.String(), "failed to load Smap:", err, "- reinitializing")
		return nil, false
	}
	if !loaded {
		return nil, false // no local replica of a cluster map - bootstrapping (joining from scratch)
	}

	node := smap.GetNode(h.SID())
	if node == nil {
		ty := "targets"
		if h.si.Type() == apc.Proxy {
			ty = "proxies"
		}
		cos.ExitLogf(notPresentInSmap, cmn.BadSmapPrefix, h.si, smap.StringEx(), ty, h.si, h.owner.smap.fpath, h.si)
	}
	if node.Type() != h.si.Type() {
		cos.ExitLogf("%s: %s is %q while the node in the loaded %s is %q", cmn.BadSmapPrefix,
			h.si, h.si.Type(), smap.StringEx(), node.Type())
		return nil, false
	}

	//
	// NOTE: not enforcing Snode's immutability - in particular, IPs that may change upon restart in K8s
	//
	if _, err := smap.IsDupNet(h.si); err != nil {
		nlog.Warningln(err, "- proceeding with the loaded", smap.String(), "anyway...")
	}
	return smap, true
}

func (h *htrun) setDaemonConfigMsg(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg, query url.Values) {
	var (
		transient = cos.IsParseBool(query.Get(apc.QparamTransient))
		toUpdate  = &cmn.ConfigToSet{}
	)
	if err := cos.MorphMarshal(msg.Value, toUpdate); err != nil {
		h.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, h, msg.Action, msg.Value, err)
		return
	}

	co := h.owner.config
	co.Lock()
	err := setConfig(toUpdate, transient)
	co.Unlock()
	if err != nil {
		h.writeErr(w, r, err)
	}
}

func (h *htrun) setDaemonConfigQuery(w http.ResponseWriter, r *http.Request) {
	var (
		query     = r.URL.Query()
		transient = cos.IsParseBool(query.Get(apc.QparamTransient))
		toUpdate  = &cmn.ConfigToSet{}
	)
	if err := toUpdate.FillFromQuery(query); err != nil {
		h.writeErr(w, r, err)
		return
	}

	co := h.owner.config
	co.Lock()
	err := setConfig(toUpdate, transient)
	co.Unlock()
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
		for _, pubExtra := range h.si.PubExtra {
			debug.Assert(pubExtra.Port == h.si.PubNet.Port, "expecting the same TCP port for all multi-home interfaces")
			server := &netServer{muxers: g.netServ.pub.muxers, sndRcvBufSize: g.netServ.pub.sndRcvBufSize}
			go func() {
				_ = server.listen(pubExtra.TCPEndpoint(), logger, tlsConf, config)
			}()
			g.netServ.pubExtra = append(g.netServ.pubExtra, server)
		}
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
		smap := h.owner.smap.get()
		if err := h.rmSelf(smap, true); err != nil && !cos.IsErrConnectionRefused(err) {
			nlog.Warningln(err)
		}
	}
	nlog.Infoln("Shutting down HTTP")

	wg.Add(1)
	go func() {
		time.Sleep(sleep)
		shuthttp()
		wg.Done()
	}()
	var flt xreg.Flt
	entry := xreg.GetRunning(&flt)
	if entry != nil {
		time.Sleep(sleep)
		entry = xreg.GetRunning(&flt)
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

	switch args.timeout {
	case apc.DefaultTimeout:
		req, res.err = args.req.Req()
		client = g.client.control // timeout = config.Client.Timeout ("client.client_timeout")
	case apc.LongTimeout:
		req, res.err = args.req.Req()
		client = g.client.data // timeout = config.Client.TimeoutLong ("client.client_long_timeout")
	default:
		var cancel context.CancelFunc
		if args.timeout == 0 {
			args.timeout = cmn.Rom.CplaneOperation()
		}
		req, _, cancel, res.err = args.req.ReqWith(args.timeout)
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
		return res
	}

	// req header
	if smap.vstr != "" {
		if smap.IsPrimary(h.si) {
			req.Header.Set(apc.HdrSenderIsPrimary, "true")
		}
		req.Header.Set(apc.HdrSenderSmapVer, smap.vstr)
	}
	req.Header.Set(apc.HdrSenderID, h.SID())
	req.Header.Set(apc.HdrSenderName, h.si.Name())
	req.Header.Set(cos.HdrUserAgent, ua)

	resp, res.err = client.Do(req)
	if res.err != nil {
		res.details = dfltDetail // tcp level, e.g.: connection refused
		cmn.HreqFree(req)
		return res
	}

	_doResp(args, req, resp, res)
	resp.Body.Close()
	cmn.HreqFree(req)

	if sid != unknownDaemonID {
		h.keepalive.heardFrom(sid)
	}
	return res
}

func _doResp(args *callArgs, req *http.Request, resp *http.Response, res *callResult) {
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

	// read and decode via call-result-value (`cresv`), if provided;
	// otherwise, read and return bytes for the caller to unmarshal
	if args.cresv != nil {
		res.v = args.cresv.newV()
		args.cresv.read(res, resp.Body)
	} else if args.req.Method != http.MethodHead { // ref: "response to a HEAD method should not have a body"
		res.read(resp.Body, resp.ContentLength)
	}
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
				nlog.Errorln(&errNodeNotFound{h.si, smap, "failed to notify", dst})
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
		wg      = cos.NewLimitedWaitGroup(sys.MaxParallelism(), bargs.nodeCount)
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
		wg      = cos.NewLimitedWaitGroup(sys.MaxParallelism(), bargs.nodeCount)
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

func (h *htrun) bcastAsyncIC(msg *actMsgExt) {
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

func (h *htrun) bcastAndRespond(w http.ResponseWriter, r *http.Request, args *bcastArgs) {
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
	hdr := w.Header()
	hdr.Set(cos.HdrContentType, cos.ContentJSONCharsetUTF)
	if isBrowser(r.Header.Get(cos.HdrUserAgent)) {
		var out []byte
		if out, err = jsoniter.MarshalIndent(v, "", "    "); err == nil {
			hdr.Set(cos.HdrContentLength, strconv.Itoa(len(out)))
			// NOTE: Strict-Transport-Security
			hdr.Set(cos.HdrHSTS, "max-age=31536000; includeSubDomains")
			_, err = w.Write(out)
		}
	} else { // previously: new-encoder(w).encode(v) (non-browser client)
		j := cos.JSON.BorrowStream(nil)
		j.WriteVal(v)
		j.WriteRaw("\n")
		if err = j.Error; err == nil {
			b := j.Buffer()
			hdr.Set(cos.HdrContentLength, strconv.Itoa(len(b)))
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
	var efmt string
	if nlog.Stopping() {
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

	// build msg
	var (
		sb strings.Builder
	)
	sb.Grow(cos.KiB)
	sb.WriteString(fmt.Sprintf(efmt, err))

	for i := 1; i < 4; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		if i > 1 {
			sb.WriteString(" <- ")
		}
		f := filepath.Base(file)
		sb.WriteString(f)
		sb.WriteByte(':')
		sb.WriteString(strconv.Itoa(line))
	}
	msg := sb.String()

	if cos.IsErrBrokenPipe(err) { // client went away
		nlog.Infoln("Warning: " + msg)
	} else {
		nlog.Errorln(msg)
	}
	h.statsT.Inc(stats.ErrHTTPWriteCount)
}

func _parseNCopies(value any) (copies int64, err error) {
	switch v := value.(type) {
	case string:
		copies, err = strconv.ParseInt(v, 10, 16)
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

// see related "GET(what)" set of APIs: api/cluster and api/daemon
// the enum itself in api/apc/query
func (h *htrun) httpdaeget(w http.ResponseWriter, r *http.Request, query url.Values, htext htext) {
	var (
		body any
		what = query.Get(apc.QparamWhat)
	)
	switch what {
	case apc.WhatNodeConfig:
		var (
			out    cmn.Config
			config = cmn.GCO.Get()
		)
		// hide secret
		out = *config
		out.Auth.Secret = "**********"
		body = &out
	case apc.WhatSmap:
		body = h.owner.smap.get()
	case apc.WhatBMD:
		body = h.owner.bmd.get()
	case apc.WhatSmapVote:
		var err error
		body, err = h.cluMeta(cmetaFillOpt{htext: htext, skipPrimeTime: true})
		if err != nil {
			nlog.Errorln("clu-meta failure:", err)
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
	case apc.WhatCertificate: // (see also: daeLoadX509, cluLoadX509)
		body = certloader.Props()
	default:
		h.writeErrf(w, r, "invalid '%s' request: unrecognized 'what=%s' query", r.URL.Path, what)
		return
	}
	h.writeJSON(w, r, body, "httpdaeget-"+what)
}

func (h *htrun) statsAndStatus() (ds *stats.NodeStatus) {
	smap := h.owner.smap.get()
	ds = &stats.NodeStatus{
		Node: stats.Node{
			Snode: h.si,
		},
		Cluster: cos.NodeStateInfo{
			Flags: cos.NodeStateFlags(h.statsT.Get(cos.NodeAlerts)),
		},
		SmapVersion:    smap.Version,
		MemCPUInfo:     apc.GetMemCPU(),
		DeploymentType: deploymentType(),
		Version:        daemon.version,
		BuildTime:      daemon.buildTime,
		K8sPodName:     os.Getenv(env.AisK8sPod),
		Status:         h._status(smap),
	}
	return ds
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
		ecode := cos.Ternary(cos.IsNotExist(err), http.StatusNotFound, http.StatusInternalServerError)
		h.writeErr(w, r, err, ecode)
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
func (h *htrun) targzLogs(severity string) (tempdir, archname string, _ error) {
	logdir := cmn.GCO.Get().LogDir
	dentries, err := os.ReadDir(logdir)
	if err != nil {
		return "", "", fmt.Errorf("read-logdir %s: %w", logdir, err)
	}

	tempdir = filepath.Join(os.TempDir(), "aislogs-"+h.SID())
	if err := cos.CreateDir(tempdir); err != nil {
		return "", "", fmt.Errorf("create-tempdir %s: %w", tempdir, err)
	}

	wfh, erw := os.CreateTemp(tempdir, "")
	if erw != nil {
		return "", "", fmt.Errorf("create-archive %s/<...>: %w", tempdir, erw)
	}

	archname = wfh.Name()
	aw := archive.NewWriter(archive.ExtTarGz, wfh, nil /*checksum*/, nil /*opts*/)

	e := _targzLogs(aw, logdir, severity, dentries)
	if err := aw.Fini(); err != nil && e == nil {
		e = err
	}
	wfh.Close()

	return tempdir, archname, e
}

func _targzLogs(aw archive.Writer, logdir, severity string, dentries []os.DirEntry) error {
	for _, dent := range dentries {
		if !dent.Type().IsRegular() {
			continue
		}
		finfo, errV := dent.Info()
		if errV != nil {
			continue
		}
		fullPath := filepath.Join(logdir, finfo.Name())
		if !logname2Sev(fullPath, severity) {
			continue
		}

		rfh, errO := os.Open(fullPath)
		if errO != nil {
			if cos.IsNotExist(errO) {
				continue
			}
			return fmt.Errorf("open-log %s: %w", fullPath, errO)
		}

		oah := cos.SimpleOAH{Size: finfo.Size(), Atime: finfo.ModTime().UnixNano()}
		err := aw.Write(finfo.Name(), oah, rfh)
		rfh.Close()
		if err != nil {
			return fmt.Errorf("write-arch-log %s: %w", fullPath, err)
		}
	}

	return nil
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

func (*htrun) writeErr(w http.ResponseWriter, r *http.Request, err error, ecode ...int) {
	cmn.WriteErr(w, r, err, ecode...) // [ecode[, silent]]
}

func (*htrun) writeErrMsg(w http.ResponseWriter, r *http.Request, msg string, ecode ...int) {
	cmn.WriteErrMsg(w, r, msg, ecode...) // [ecode[, silent]]
}

func (h *htrun) writeErrSilentf(w http.ResponseWriter, r *http.Request, ecode int, format string, a ...any) {
	err := fmt.Errorf(format, a...)
	h.writeErr(w, r, err, ecode, Silent)
}

func (h *htrun) writeErrStatusf(w http.ResponseWriter, r *http.Request, ecode int, format string, a ...any) {
	err := fmt.Errorf(format, a...)
	h.writeErrMsg(w, r, err.Error(), ecode)
}

func (h *htrun) writeErrf(w http.ResponseWriter, r *http.Request, format string, a ...any) {
	err := fmt.Errorf(format, a...)
	if cos.IsNotExist(err) {
		h.writeErrMsg(w, r, err.Error(), http.StatusNotFound)
	} else {
		h.writeErrMsg(w, r, err.Error())
	}
}

func (h *htrun) writeErrURL(w http.ResponseWriter, r *http.Request) {
	if r.URL.Scheme != "" {
		h.writeErrf(w, r, "request '%s %s://%s' from %s: invalid URL path", r.Method, r.URL.Scheme, r.URL.Path, r.RemoteAddr)
		return
	}
	// ignore GET /favicon.ico by Browsers
	if r.URL.Path == "/favicon.ico" || r.URL.Path == "favicon.ico" {
		return
	}
	h.writeErrf(w, r, "invalid request URI: '%s %s' from %s", r.Method, r.RequestURI, r.RemoteAddr)
}

func (h *htrun) writeErrAct(w http.ResponseWriter, r *http.Request, action string) {
	err := cmn.InitErrHTTP(r, fmt.Errorf("invalid action %q", action), 0)
	h.writeErr(w, r, err)
	cmn.FreeHterr(err)
}

// health client
func (h *htrun) reqHealth(si *meta.Snode, tout time.Duration, q url.Values, smap *smapX, retry bool) ([]byte, int, error) {
	var (
		path  = apc.URLPathHealth.S
		url   = si.URL(cmn.NetIntraControl)
		cargs = allocCargs()
	)
	{
		cargs.si = si
		cargs.req = cmn.HreqArgs{Method: http.MethodGet, Base: url, Path: path, Query: q}
		cargs.timeout = tout
	}
	res := h.call(cargs, smap)
	b, status, err := res.bytes, res.status, res.err
	freeCR(res)

	if err != nil {
		ni, no := h.si.String(), si.StringEx()
		if cmn.Rom.V(5, cos.ModKalive) {
			nlog.Warningln(ni, "failed req-health:", no, "tout", tout, "err: [", err, status, "]")
		}
		if retry {
			// - retrying when about to remove node 'si' from the cluster map, or
			// - about to elect a new primary;
			// - not checking `IsErrDNSLookup` and similar
			// - ie., not trying to narrow down (compare w/ slow-keepalive)
			if si.PubNet.Hostname != si.ControlNet.Hostname {
				u := si.URL(cmn.NetPublic)
				cargs.req.Base = u
				res = h.call(cargs, smap)
				b, status, err = res.bytes, res.status, res.err
				freeCR(res)
				if err != nil {
					nlog.Warningln(ni, "failed req-health retry:", no, "via pub", u, "tout", tout, "err: [", err, status, "]")
				}
			}
		}
	}

	freeCargs(cargs)
	return b, status, err
}

// - utilizes reqHealth (above) to discover a _better_ Smap, if exists
// - via getMaxCii.do()
// - checkAll: query all nodes
// - consider adding max-ver BMD bit here as well (TODO)
func (h *htrun) bcastHealth(smap *smapX, checkAll bool) (*cos.NodeStateInfo, int /*num confirmations*/) {
	if !smap.isValid() {
		nlog.Errorf("%s: cannot execute with invalid %s", h, smap)
		return nil, 0
	}
	c := getMaxCii{
		h:        h,
		maxNsti:  &cos.NodeStateInfo{},
		query:    url.Values{apc.QparamClusterInfo: []string{"true"}},
		timeout:  cmn.Rom.CplaneOperation(),
		checkAll: checkAll,
	}
	smap.fill(c.maxNsti)

	h._bch(&c, smap, apc.Proxy)
	if checkAll || (c.cnt < maxVerConfirmations && smap.CountActiveTs() > 0) {
		h._bch(&c, smap, apc.Target)
	}

	// log
	b, err := jsoniter.MarshalIndent(c.maxNsti.Smap, "", "    ")
	debug.AssertNoErr(err)
	nlog.Infoln(string(b))
	nlog.Infoln("flags:", c.maxNsti.Flags.String())

	return c.maxNsti, c.cnt
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
		wg = cos.NewLimitedWaitGroup(sys.MaxParallelism(), len(nodemap))
	} else {
		count = min(sys.MaxParallelism(), maxVerConfirmations<<1)
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

// TODO: reinforce - make it another `ensure*` method
func (h *htrun) warnMsync(r *http.Request, smap *smapX) {
	const (
		tag = "metasync-recv"
	)
	if !smap.isValid() {
		return
	}
	pid := r.Header.Get(apc.HdrSenderID)
	psi := smap.GetNode(pid)
	if psi == nil {
		err := &errNodeNotFound{msg: tag + " warning:", id: pid, si: h.si, smap: smap}
		nlog.Warningln(err)
	} else if !smap.isPrimary(psi) {
		nlog.Warningln(h.String(), tag, "expecting primary, got", psi.StringEx(), smap.StringEx())
	}
}

func logmsync(lver int64, revs revs, msg *actMsgExt, sender string, opts ...string) { // sender [, what, luuid]
	const tag = "msync Rx:"
	var (
		what  string
		uuid  = revs.uuid()
		lv    = "v" + strconv.FormatInt(lver, 10)
		luuid string
	)
	switch len(opts) {
	case 0:
		what = revs.String()
		if uuid := revs.uuid(); uuid != "" {
			what += "[" + uuid + "]"
		}
	case 1:
		what = opts[0]
		if strings.IndexByte(what, '[') < 0 {
			if uuid != "" {
				what += "[" + uuid + "]"
			}
		}
	default:
		what = opts[0]
		luuid = opts[1]
		lv += "[" + luuid + "]"
	}
	// different uuids (clusters) - versions cannot be compared
	if luuid != "" && uuid != "" && uuid != luuid {
		nlog.InfoDepth(1, "Warning", tag, what, "( different cluster", lv, msg.String(), "<--", sender, msg.String(), ")")
		return
	}

	// compare l(ocal) and newly received
	switch {
	case lver == revs.version():
		s := "( same"
		if lver == 0 {
			s = "( initial"
		}
		nlog.InfoDepth(1, tag, what, s, lv, msg.String(), "<--", sender, ")")
	case lver > revs.version():
		nlog.InfoDepth(1, "Warning", tag, what, "( down from", lv, msg.String(), "<--", sender, msg.String(), ")")
	default:
		nlog.InfoDepth(1, tag, "new", what, "( have", lv, msg.String(), "<--", sender, ")")
	}
}

// return extracted (new) config with associated action message; otherwise error
func (h *htrun) extractConfig(payload msPayload, sender string) (*globalConfig, *actMsgExt, error) {
	confValue, ok := payload[revsConfTag]
	if !ok {
		return nil, nil, nil
	}

	var (
		newConfig = &globalConfig{}
		msg       = &actMsgExt{}
		reader    = bytes.NewBuffer(confValue)
	)
	if _, err1 := jsp.Decode(reader, newConfig, newConfig.JspOpts(), "extractConfig"); err1 != nil {
		err := fmt.Errorf(cmn.FmtErrUnmarshal, h, "new Config", cos.BHead(confValue), err1)
		return nil, nil, err
	}
	if msgValue, ok := payload[revsConfTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err := fmt.Errorf(cmn.FmtErrUnmarshal, h, "action message", cos.BHead(msgValue), err1)
			return newConfig, nil, err
		}
	}
	config := cmn.GCO.Get()
	if cmn.Rom.V(4, cos.ModAIS) {
		logmsync(config.Version, newConfig, msg, sender, newConfig.String(), config.UUID)
	}
	if newConfig.version() <= config.Version && msg.Action != apc.ActPrimaryForce {
		if newConfig.version() < config.Version {
			return newConfig, msg, newErrDowngrade(h.si, config.String(), newConfig.String())
		}
		newConfig = nil
	}

	return newConfig, msg, nil
}

// return extracted (new) etl metadata with associated action message; otherwise error
func (h *htrun) extractEtlMD(payload msPayload, sender string) (*etlMD, *actMsgExt, error) {
	etlMDValue, ok := payload[revsEtlMDTag]
	if !ok {
		return nil, nil, nil
	}
	var (
		newMD  = newEtlMD()
		msg    = &actMsgExt{}
		reader = bytes.NewBuffer(etlMDValue)
	)
	if _, err1 := jsp.Decode(reader, newMD, newMD.JspOpts(), "extractEtlMD"); err1 != nil {
		err := fmt.Errorf(cmn.FmtErrUnmarshal, h, "new EtlMD", cos.BHead(etlMDValue), err1)
		return nil, nil, err
	}
	if msgValue, ok := payload[revsEtlMDTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err := fmt.Errorf(cmn.FmtErrUnmarshal, h, "action message", cos.BHead(msgValue), err1)
			return newMD, nil, err
		}
	}

	etlMD := h.owner.etl.get()
	if cmn.Rom.V(4, cos.ModAIS) {
		logmsync(etlMD.Version, newMD, msg, sender)
	}
	if newMD.version() <= etlMD.version() && msg.Action != apc.ActPrimaryForce {
		if newMD.version() < etlMD.version() {
			return newMD, msg, newErrDowngrade(h.si, etlMD.String(), newMD.String())
		}
		newMD = nil
	}

	return newMD, msg, nil
}

// return extracted (new) Smap with associated action message; otherwise error
func (h *htrun) extractSmap(payload msPayload, sender string, skipValidation bool) (*smapX, *actMsgExt, error) {
	const (
		act = "extract-smap"
	)
	smapValue, ok := payload[revsSmapTag]
	if !ok {
		return nil, nil, nil
	}

	var (
		newSmap = &smapX{}
		msg     = &actMsgExt{}
		reader  = bytes.NewBuffer(smapValue)
	)
	if _, err1 := jsp.Decode(reader, newSmap, newSmap.JspOpts(), act); err1 != nil {
		return nil, nil, fmt.Errorf(cmn.FmtErrUnmarshal, h, "new Smap", cos.BHead(smapValue), err1)
	}
	if msgValue, ok := payload[revsSmapTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err := fmt.Errorf(cmn.FmtErrUnmarshal, h, "action message", cos.BHead(msgValue), err1)
			return newSmap, nil, err
		}
	}
	if skipValidation || (msg.Action == apc.ActPrimaryForce && newSmap.isValid()) {
		return newSmap, msg, nil
	}

	var (
		smap        = h.owner.smap.get()
		curVer      = smap.version()
		isManualReb = msg.Action == apc.ActRebalance && msg.Value != nil
	)
	if newSmap.version() == curVer && !isManualReb {
		return nil, nil, nil
	}
	if !newSmap.isValid() {
		return newSmap, msg, cmn.NewErrFailedTo(h, "extract", newSmap, newSmap.validate())
	}

	if !newSmap.isPresent(h.si) {
		err := &errSelfNotFound{act: act, si: h.si, tag: "new", smap: newSmap}
		if msg.Action != apc.ActPrimaryForce {
			return newSmap, msg, err
		}

		nlog.Warningln(err, "- proceeding with force")
		return newSmap, msg, nil
	}

	if err := smap.validateUUID(h.si, newSmap, sender, 50 /* ciError */); err != nil {
		return newSmap, msg, err // FATAL: cluster integrity error
	}

	if cmn.Rom.V(4, cos.ModAIS) {
		logmsync(smap.Version, newSmap, msg, sender, newSmap.String(), smap.UUID)
	}
	_, sameOrigin, _, eq := smap.Compare(&newSmap.Smap)
	debug.Assert(sameOrigin)
	if newSmap.version() < curVer {
		if !eq {
			err := newErrDowngrade(h.si, smap.StringEx(), newSmap.StringEx())
			return newSmap, msg, err
		}
		nlog.Warningf("%s: %s and %s are otherwise identical", h.si, newSmap.StringEx(), smap.StringEx())
		newSmap = nil
	}

	return newSmap, msg, nil
}

// return extracted (new) RMD with associated action message; otherwise error
func (h *htrun) extractRMD(payload msPayload, sender string) (*rebMD, *actMsgExt, error) {
	rmdValue, ok := payload[revsRMDTag]
	if !ok {
		return nil, nil, nil
	}

	var (
		newRMD = &rebMD{}
		msg    = &actMsgExt{}
	)
	if err1 := jsoniter.Unmarshal(rmdValue, newRMD); err1 != nil {
		err := fmt.Errorf(cmn.FmtErrUnmarshal, h, "new RMD", cos.BHead(rmdValue), err1)
		return nil, nil, err
	}
	if msgValue, ok := payload[revsRMDTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err := fmt.Errorf(cmn.FmtErrUnmarshal, h, "action message", cos.BHead(msgValue), err1)
			return newRMD, nil, err
		}
	}

	rmd := h.owner.rmd.get()
	logmsync(rmd.Version, newRMD, msg, sender, newRMD.String(), rmd.CluID)

	if msg.Action == apc.ActPrimaryForce {
		return newRMD, msg, nil
	}

	if newRMD.CluID != "" && newRMD.CluID != rmd.CluID && rmd.CluID != "" {
		err := h.owner.rmd.newClusterIntegrityErr(h.String(), newRMD.CluID, rmd.CluID, rmd.Version)
		cos.ExitLog(err) // FATAL
		return newRMD, msg, err
	}

	if newRMD.version() <= rmd.version() && msg.Action != apc.ActPrimaryForce {
		if newRMD.version() < rmd.version() {
			return newRMD, msg, newErrDowngrade(h.si, rmd.String(), newRMD.String())
		}
		newRMD = nil
	}

	return newRMD, msg, nil
}

// return extracted (new) BMD with associated action message; otherwise error
func (h *htrun) extractBMD(payload msPayload, sender string) (*bucketMD, *actMsgExt, error) {
	bmdValue, ok := payload[revsBMDTag]
	if !ok {
		return nil, nil, nil
	}

	var (
		newBMD = &bucketMD{}
		msg    = &actMsgExt{}
		reader = bytes.NewBuffer(bmdValue)
	)
	if _, err1 := jsp.Decode(reader, newBMD, newBMD.JspOpts(), "extractBMD"); err1 != nil {
		err := fmt.Errorf(cmn.FmtErrUnmarshal, h, "new BMD", cos.BHead(bmdValue), err1)
		return nil, nil, err
	}
	if msgValue, ok := payload[revsBMDTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err := fmt.Errorf(cmn.FmtErrUnmarshal, h, "action message", cos.BHead(msgValue), err1)
			return newBMD, nil, err
		}
	}

	bmd := h.owner.bmd.get()
	if cmn.Rom.V(4, cos.ModAIS) {
		logmsync(bmd.Version, newBMD, msg, sender, newBMD.String(), bmd.UUID)
	}
	// skip older iff not transactional - see t.receiveBMD()
	if h.si.IsTarget() && msg.UUID != "" {
		return newBMD, msg, nil
	}
	if newBMD.version() <= bmd.version() && msg.Action != apc.ActPrimaryForce {
		if newBMD.version() < bmd.version() {
			return newBMD, msg, newErrDowngrade(h.si, bmd.StringEx(), newBMD.StringEx())
		}
		newBMD = nil
	}

	return newBMD, msg, nil
}

func (h *htrun) receiveSmap(newSmap *smapX, msg *actMsgExt, payload msPayload, sender string, cb smapUpdatedCB) error {
	if newSmap == nil {
		return nil
	}
	smap := h.owner.smap.get()
	logmsync(smap.Version, newSmap, msg, sender, newSmap.StringEx(), smap.UUID)

	if !newSmap.isPresent(h.si) {
		return &errSelfNotFound{act: "receive-smap", si: h.si, tag: "new", smap: newSmap}
	}
	return h.owner.smap.synchronize(h.si, newSmap, payload, cb)
}

func (h *htrun) receiveEtlMD(newEtlMD *etlMD, msg *actMsgExt, payload msPayload, sender string, cb func(ne, oe *etlMD)) (err error) {
	if newEtlMD == nil {
		return
	}
	etlMD := h.owner.etl.get()
	logmsync(etlMD.Version, newEtlMD, msg, sender)

	h.owner.etl.Lock()
	etlMD = h.owner.etl.get()
	if newEtlMD.version() <= etlMD.version() && msg.Action != apc.ActPrimaryForce {
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
func (h *htrun) _recvCfg(newConfig *globalConfig, msg *actMsgExt, payload msPayload) (err error) {
	config := cmn.GCO.Get()
	if newConfig.version() <= config.Version && msg.Action != apc.ActPrimaryForce {
		if newConfig.version() == config.Version {
			return
		}
		return newErrDowngrade(h.si, config.String(), newConfig.String())
	}
	if config.UUID != "" && config.UUID != newConfig.UUID {
		err = fmt.Errorf("%s: cluster configs have different UUIDs: (curr %q vs new %q)", ciError(110), config.UUID, newConfig.UUID)
		if msg.Action != apc.ActPrimaryForce {
			return err
		}
		nlog.Warningln(err, "- proceeding with force")
	}
	if err := h.owner.config.persist(newConfig, payload); err != nil {
		return err
	}
	return cmn.GCO.Update(&newConfig.ClusterConfig)
}

func (h *htrun) extractRevokedTokenList(payload msPayload, sender string) (*tokenList, error) {
	var (
		msg       actMsgExt
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
	nlog.Infof("extract token list from %q (count: %d, action: %q, uuid: %q)", sender,
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
//     or - during the cluster deployment time - by the configured "primary_url"
//     (see /deploy/dev/local/aisnode_config.sh)
//   - if that one fails, the new node goes ahead and tries the alternatives:
//   - config.Proxy.PrimaryURL   ("primary_url")
//   - config.Proxy.DiscoveryURL ("discovery_url")
//   - config.Proxy.OriginalURL  ("original_url")
//   - if these fails we try the candidates provided by the caller.
//
// ================================== Background =========================================
func (h *htrun) join(htext htext, contactURLs ...string) (*callResult, error) {
	var (
		config             = cmn.GCO.Get()
		_, primaryURL, psi = h._primus(nil, config)
	)
	if psi != nil && psi.ID() == h.SID() {
		debug.Assert(h.si.IsProxy())
		return nil, fmt.Errorf("%s (self) - not joining, am primary [%q]", h, primaryURL) // (unlikely)
	}

	var (
		candidates               = make([]string, 0, 4+len(contactURLs))
		selfPublicURL, pubValid  = cos.ParseURL(h.si.URL(cmn.NetPublic))
		selfIntraURL, intraValid = cos.ParseURL(h.si.URL(cmn.NetIntraControl))
		resPrev                  *callResult
	)
	debug.Assertf(pubValid && intraValid, "%q (%t), %q (%t)", selfPublicURL, pubValid, selfIntraURL, intraValid)

	// env first
	if daemon.EP != "" {
		candidates = _addCan(daemon.EP, selfPublicURL.Host, selfIntraURL.Host, candidates)
	}

	candidates = _addCan(primaryURL, selfPublicURL.Host, selfIntraURL.Host, candidates)
	if psi != nil {
		candidates = _addCan(psi.URL(cmn.NetPublic), selfPublicURL.Host, selfIntraURL.Host, candidates)
	}
	candidates = _addCan(config.Proxy.PrimaryURL, selfPublicURL.Host, selfIntraURL.Host, candidates)
	candidates = _addCan(config.Proxy.DiscoveryURL, selfPublicURL.Host, selfIntraURL.Host, candidates)
	candidates = _addCan(config.Proxy.OriginalURL, selfPublicURL.Host, selfIntraURL.Host, candidates)
	for _, u := range contactURLs {
		candidates = _addCan(u, selfPublicURL.Host, selfIntraURL.Host, candidates)
	}

	var (
		res   *callResult
		sleep = max(2*time.Second, cmn.Rom.MaxKeepalive())
	)
	for range 4 { // retry
		for _, candidateURL := range candidates {
			if nlog.Stopping() {
				return res, h.errStopping()
			}
			if resPrev != nil {
				freeCR(resPrev)
				resPrev = nil //nolint:ineffassign,wastedassign // readability
			}
			res = h.regTo(candidateURL, nil, apc.DefaultTimeout, htext, false /*keepalive*/)
			if res.err == nil {
				nlog.Infoln(h.String()+": primary responded Ok via", candidateURL)
				return res, nil // ok
			}
			resPrev = res
		}
		time.Sleep(sleep)
	}
	if resPrev != nil {
		freeCR(resPrev)
	}

	smap := h.owner.smap.get()
	if err := smap.validate(); err != nil {
		return res, fmt.Errorf("%s: %v", h.si, err)
	}

	// Failed to join cluster using config, try getting primary URL using existing smap.
	nsti, _ := h.bcastHealth(smap, false /*checkAll*/)
	if nsti == nil {
		return nil, fmt.Errorf("%s: failed to discover new Smap", h)
	}
	if nsti.Smap.Version < smap.version() {
		return nil, fmt.Errorf("%s: current %s version is newer than %d from the primary (%s)",
			h, smap, nsti.Smap.Version, nsti.Smap.Primary.ID)
	}
	primaryURL = nsti.Smap.Primary.PubURL

	// Daemon is stopping skip register
	if nlog.Stopping() {
		return nil, h.errStopping()
	}

	res = h.regTo(primaryURL, nil, apc.DefaultTimeout, htext, false /*keepalive*/)
	if res.err == nil {
		nlog.Infoln(h.String()+": joined cluster via", primaryURL)
		return res, nil
	}

	err := res.err
	freeCR(res)
	return res, err
}

func _addCan(url, selfPub, selfCtrl string, candidates []string) []string {
	if u, valid := cos.ParseURL(url); !valid || u.Host == selfPub || u.Host == selfCtrl {
		return candidates
	}
	if cos.StringInSlice(url, candidates) {
		return candidates
	}
	return append(candidates, url)
}

func (h *htrun) regTo(url string, psi *meta.Snode, tout time.Duration, htext htext, keepalive bool) *callResult {
	var (
		path          string
		skipPrxKalive = h.si.IsProxy() || keepalive
		opts          = cmetaFillOpt{
			htext:      htext,
			skipSmap:   skipPrxKalive, // when targets self- or admin-join
			skipBMD:    skipPrxKalive, // ditto
			skipRMD:    true,          // NOTE: not used yet
			skipConfig: true,          // ditto
			skipEtlMD:  true,          // ditto

			fillRebMarker: !keepalive && htext != nil,
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
		cargs.req = cmn.HreqArgs{Method: http.MethodPost, Base: url, Path: path, Body: cos.MustMarshal(cm)}
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

// (fast path: nodes => primary)
func (h *htrun) fastKalive(smap *smapX, timeout time.Duration, ecActive, dmActive bool) (string /*pid*/, http.Header, error) {
	if nlog.Stopping() {
		return "", http.Header{}, h.errStopping()
	}
	debug.Assert(h.ClusterStarted())

	pid, primaryURL, psi := h._primus(smap, nil)

	cargs := allocCargs()
	{
		cargs.si = psi
		cargs.req = cmn.HreqArgs{Method: http.MethodPost, Base: primaryURL, Path: apc.URLPathCluKalive.Join(h.SID())}
		cargs.timeout = timeout
	}
	if ecActive || dmActive {
		// (target => primary)
		hdr := make(http.Header, 1)
		if ecActive {
			hdr.Set(apc.HdrActiveEC, "true")
		}
		if dmActive {
			hdr.Set(apc.HdrActiveDM, "true")
		}
		cargs.req.Header = hdr
	}

	res := h.call(cargs, smap)
	freeCargs(cargs)
	err, hdr := res.err, res.header

	freeCR(res)
	return pid, hdr, err
}

// (slow path: nodes => primary)
func (h *htrun) slowKalive(smap *smapX, htext htext, timeout time.Duration) (string /*pid*/, int, error) {
	if nlog.Stopping() {
		return "", 0, h.errStopping()
	}
	pid, primaryURL, psi := h._primus(smap, nil)

	res := h.regTo(primaryURL, psi, timeout, htext, true /*keepalive*/)
	if res.err == nil {
		freeCR(res)
		return pid, 0, nil
	}

	s := res.err.Error()
	if strings.Contains(s, ciePrefix) {
		cos.ExitLog(res.err) // FATAL: cluster integrity error (cie)
	}

	if psi == nil || pid == "" || psi.PubNet.Hostname == psi.ControlNet.Hostname {
		status, err := res.status, res.err
		freeCR(res)
		return pid, status, err
	}

	// intermittent DNS failure? (compare with h.reqHealth)
	if psi.PubNet.Hostname != psi.ControlNet.Hostname {
		primaryURL = psi.URL(cmn.NetPublic)
		nlog.Warningln("retrying via pub addr", primaryURL, "[", s, pid, "]")

		freeCR(res)
		res = h.regTo(primaryURL, psi, timeout, htext, true /*keepalive*/)
	}

	status, err := res.status, res.err
	freeCR(res)
	return pid, status, err
}

func (h *htrun) _primus(smap *smapX, config *cmn.Config) (string /*pid*/, string /*url*/, *meta.Snode) {
	if smap == nil {
		smap = h.owner.smap.get()
	}
	if smap.validate() != nil {
		if config == nil {
			config = cmn.GCO.Get()
		}
		return "", config.Proxy.PrimaryURL, nil
	}
	return smap.Primary.ID(), smap.Primary.URL(cmn.NetIntraControl), smap.Primary
}

// return NodeStateInfo.Smap with a new or changed primary; otherwise nil
func (h *htrun) pollClusterStarted(config *cmn.Config, psi *meta.Snode) *cos.NodeStateInfo {
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
		if nlog.Stopping() {
			return nil
		}
		smap := h.owner.smap.get()
		if smap.validate() != nil {
			continue
		}
		if h.si.IsProxy() && smap.isPrimary(h.si) { // TODO: unlikely - see httpRequestNewPrimary
			nlog.Warningln(h.String(), "started as a non-primary and got _elected_ during startup")
			return nil
		}
		if _, _, err := h.reqHealth(smap.Primary, healthTimeout, query /*ask primary*/, smap, false /*retry pub-addr*/); err == nil {
			// log
			s := fmt.Sprintf("%s via primary health: cluster startup Ok, %s", h.si, smap.StringEx())
			if self := smap.GetNode(h.si.ID()); self == nil {
				nlog.Warningln(s + "; NOTE: not present in the cluster map")
			} else if self.Flags.IsSet(meta.SnodeMaint) {
				h.si.Flags = self.Flags
				nlog.Warningln(s + "; NOTE: starting in maintenance mode")
			} else if rmd := h.owner.rmd.get(); rmd != nil && rmd.version() > 0 {
				if smap.UUID != rmd.CluID {
					if rmd.CluID != "" {
						err = h.owner.rmd.newClusterIntegrityErr(h.String(), smap.UUID, rmd.CluID, rmd.version())
						cos.ExitLog(err) // FATAL
					}

					nlog.Warningf("local copy of RMD v%d does not have cluster ID (expecting %q)",
						rmd.version(), smap.UUID)
					nlog.Infoln(s)
				} else {
					nlog.Infoln(s+",", rmd.String())
				}
			} else {
				nlog.Infoln(s)
			}
			return nil
		}

		if rediscover >= config.Timeout.Startup.D()/2 {
			rediscover = 0
			if nsti, cnt := h.bcastHealth(smap, true /*checkAll*/); nsti != nil && nsti.Smap.Version > smap.version() {
				var pid string
				if psi != nil {
					pid = psi.ID()
				}
				// (primary changed)
				if nsti.Smap.Primary.ID != pid && cnt >= maxVerConfirmations {
					nlog.Warningf("%s: change of primary %s => %s - must rejoin", h.si, pid, nsti.Smap.Primary.ID)
					return nsti
				}
			}
		}
		if total > config.Timeout.Startup.D() {
			nlog.Errorln(h.String() + ": " + cmn.StartupMayTimeout)
		}
	}
}

func (h *htrun) rmSelf(smap *smapX, ignoreErr bool) error {
	if smap == nil || smap.validate() != nil {
		nlog.Warningln("cannot remove", h.String(), "(self): local copy of Smap is invalid")
		return nil
	}
	cargs := allocCargs()
	{
		cargs.si = smap.Primary
		cargs.req = cmn.HreqArgs{Method: http.MethodDelete, Path: apc.URLPathCluDaemon.Join(h.si.ID())}
		cargs.timeout = apc.DefaultTimeout
	}
	res := h.call(cargs, smap)
	status, err := res.status, res.err
	if err != nil {
		f := cos.Ternary(ignoreErr, nlog.Infof, nlog.Errorf)
		f("%s: failed to remove %s (self) from %s: %v(%d)", apc.ActSelfRemove, h.si, smap, err, status)
	} else {
		nlog.Infoln(apc.ActSelfRemove+":", h.String(), "(self) from", smap.StringEx())
	}
	freeCargs(cargs)
	freeCR(res)
	return err
}

// external watchdogs, e.g. K8s (via /v1/health handler)
// - liveness: always 200 (process is alive)
// - readiness: 200 when node and cluster started + not in maint/decomm, 503 otherwise
// TODO: check receiving on PubNet
func (h *htrun) externalWD(w http.ResponseWriter, r *http.Request) bool {
	isIntra, err := h.validateIntraRequest(r.Header, false /* from primary */)
	if err != nil {
		h.writeErr(w, r, err, http.StatusServiceUnavailable)
		return true
	}
	if isIntra {
		return false
	}

	// NOTE: check via substring (not parsed query) to avoid allocation
	isReadiness := strings.Contains(r.URL.RawQuery, apc.QparamHealthReady)
	if cmn.Rom.V(5, cos.ModKalive) {
		nlog.Infoln(h.String(), "external health-probe:", r.RemoteAddr, isReadiness, "[", r.URL.RawQuery, "]")
	}

	if !isReadiness {
		w.WriteHeader(http.StatusOK)
		return true
	}

	if h.isReady() {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	return true
}

func (h *htrun) validateIntraRequest(hdr http.Header, fromPrimary bool) (isIntra bool, err error) {
	senderID := hdr.Get(apc.HdrSenderID)
	senderName := hdr.Get(apc.HdrSenderName)
	isIntra = senderID != "" || senderName != ""
	if isIntra {
		err = h.checkIntraCall(hdr, fromPrimary)
	}
	return
}

// ready when node started, not in maint/decomm, and cluster started (exception is primary in
// a new cluster - requires targets to be registered as well before ClusterStarted can be true)
func (h *htrun) isReady() bool {
	if !h.NodeStarted() || h.si.Flags.IsAnySet(meta.SnodeMaintDecomm) {
		return false
	}

	if !h.ClusterStarted() {
		smap := h.owner.smap.get()
		debug.Assert(smap.isValid())
		return h.si.IsProxy() && smap.isPrimary(h.si) && smap.UUID == ""
	}

	return true
}

//
// intra-cluster request validations and helpers
//

func (h *htrun) ensureSameSmap(hdr http.Header, smap *smapX) (int, error) {
	var (
		senderID   = hdr.Get(apc.HdrSenderID)
		senderName = hdr.Get(apc.HdrSenderName)
		senderSver = hdr.Get(apc.HdrSenderSmapVer)
	)
	if !h.ClusterStarted() {
		return http.StatusServiceUnavailable, errors.New("not ready yet")
	}
	if ok := senderID != "" && senderName != ""; !ok {
		return 0, errIntraControl
	}
	if err := smap.validate(); err != nil {
		return 0, err
	}
	sender := smap.GetNode(senderID)
	if sender == nil {
		return http.StatusConflict, fmt.Errorf("%s: sender %s (%s) not present in the local %s", h, senderID, senderName, smap.StringEx())
	}
	if senderSver != smap.vstr {
		return http.StatusConflict, fmt.Errorf("%s: different Smap version from %s(%s): %q vs local %s",
			h, senderID, senderName, senderSver, smap.StringEx())
	}
	return 0, nil
}

func (h *htrun) checkIntraCall(hdr http.Header, fromPrimary bool) error {
	var (
		smap       = h.owner.smap.get()
		senderID   = hdr.Get(apc.HdrSenderID)
		senderName = hdr.Get(apc.HdrSenderName)
		senderSver = hdr.Get(apc.HdrSenderSmapVer)
	)
	if ok := senderID != "" && senderName != ""; !ok {
		return errIntraControl
	}
	if !smap.isValid() {
		return nil
	}
	node := smap.GetNode(senderID)
	if ok := node != nil && (!fromPrimary || smap.isPrimary(node)); ok {
		return nil
	}
	if senderSver != smap.vstr && senderSver != "" {
		ver, err := strconv.ParseInt(senderSver, 10, 64)
		if err != nil { // (unlikely)
			e := fmt.Errorf("%s: invalid sender's Smap ver [%s, %q, %w], %s", h, senderName, senderSver, err, smap)
			nlog.Errorln(e)
			return e
		}
		// we still trust the request when the sender's Smap is more current
		if ver > smap.version() {
			if h.ClusterStarted() {
				// (exception: setting primary w/ force)
				warn := h.String() + ": local " + smap.String() + " is older than (sender's) " + senderName + " Smap v" + senderSver
				nlog.ErrorDepth(1, warn, "- proceeding anyway...")
			}
			runtime.Gosched()
			return nil
		}
	}
	if node == nil {
		if !fromPrimary {
			// assume request from a newly joined node and proceed
			return nil
		}
		return fmt.Errorf("%s: expected %s from a valid node, %s", h, cmn.NetIntraControl, smap)
	}
	return fmt.Errorf("%s: expected %s from primary (and not %s), %s", h, cmn.NetIntraControl, node, smap)
}

func (h *htrun) ensureIntraControl(w http.ResponseWriter, r *http.Request, onlyPrimary bool) (isIntra bool) {
	err := h.checkIntraCall(r.Header, onlyPrimary)
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
	if len(q) == 0 || q.Get(apc.QparamPID) == "" {
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
// actMsgExt reader & constructors
//

func (*htrun) readAisMsg(w http.ResponseWriter, r *http.Request) (msg *actMsgExt, err error) {
	msg = &actMsgExt{}
	err = cmn.ReadJSON(w, r, msg)
	return
}

func (msg *actMsgExt) String() string {
	s := "aism[" + msg.Action
	if msg.UUID != "" {
		s += "[" + msg.UUID + "]"
	}
	if msg.Name != "" {
		s += ", name=" + msg.Name
	}
	return s + "]"
}

func (msg *actMsgExt) StringEx() (s string) {
	s = msg.String()
	vs, err := jsoniter.Marshal(msg.Value)
	debug.AssertNoErr(err)
	s += ",(" + strings.ReplaceAll(string(vs), ",", ", ") + ")"
	return
}

func (h *htrun) newAmsgStr(msgStr string, bmd *bucketMD) *actMsgExt {
	return h.newAmsg(&apc.ActMsg{Value: msgStr}, bmd)
}

func (h *htrun) newAmsgActVal(act string, val any) *actMsgExt {
	return h.newAmsg(&apc.ActMsg{Action: act, Value: val}, nil)
}

func (h *htrun) newAmsg(amsg *apc.ActMsg, bmd *bucketMD, uuid ...string) *actMsgExt {
	msg := &actMsgExt{ActMsg: *amsg}
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

//
// common housekeeping for rate limiters
//

func (rl *ratelim) housekeep(now int64) time.Duration {
	rl.now = now
	rl.Range(rl.cleanup)
	return hk.PruneRateLimiters
}

func (rl *ratelim) cleanup(k, v any) bool {
	r := v.(cos.Rater)
	if time.Duration(rl.now-r.LastUsed()) >= hk.PruneRateLimiters>>1 {
		rl.Delete(k)
	}
	return true // keep going
}
