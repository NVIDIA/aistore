// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"context"
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

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xact/xreg"
	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/tinylib/msgp/msgp"
)

const ciePrefix = "cluster integrity error cie#"

type htrun struct {
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
		bmd    bmdOwner // interface with proxy and target impl-s
		rmd    *rmdOwner
		config *configOwner
		etl    etlOwner // ditto
	}
	startup struct {
		cluster atomic.Int64 // mono.NanoTime() since cluster startup, zero prior to that
		node    atomic.Int64 // ditto - for the node
	}
	gmm                 *memsys.MMSA // system pagesize-based memory manager and slab allocator
	smm                 *memsys.MMSA // system MMSA for small-size allocations
	electable           electable
	inPrimaryTransition atomic.Bool
}

///////////
// htrun //
///////////

func (h *htrun) Name() string             { return h.name }
func (h *htrun) DataClient() *http.Client { return h.client.data }

func (h *htrun) Snode() *cluster.Snode { return h.si }
func (h *htrun) callerName() string    { return h.si.String() }
func (h *htrun) SID() string           { return h.si.ID() }
func (h *htrun) String() string        { return h.si.String() }

func (h *htrun) Bowner() cluster.Bowner { return h.owner.bmd }
func (h *htrun) Sowner() cluster.Sowner { return h.owner.smap }

func (h *htrun) parseReq(w http.ResponseWriter, r *http.Request, apireq *apiRequest) (err error) {
	debug.Assert(len(apireq.prefix) != 0)
	apireq.items, err = h.apiItems(w, r, apireq.after, false, apireq.prefix)
	if err != nil {
		return
	}
	debug.Assert(len(apireq.items) > apireq.bckIdx)
	bckName := apireq.items[apireq.bckIdx]
	if apireq.dpq == nil {
		apireq.query = r.URL.Query()
	} else if err = apireq.dpq.fromRawQ(r.URL.RawQuery); err != nil {
		return
	}
	apireq.bck, err = newBckFromQ(bckName, apireq.query, apireq.dpq)
	if err != nil {
		h.writeErr(w, r, err)
	}
	return
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
	if !opts.skipSmap {
		cm.Smap = h.owner.smap.get()
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
		rebMarked := xreg.GetRebMarked()
		cm.RebInterrupted = rebMarked.Interrupted
	}
	return cm, nil
}

// usage: [API call => handler => ClusterStartedWithRetry ]
func (h *htrun) ClusterStartedWithRetry() bool {
	if clutime := h.startup.cluster.Load(); clutime > 0 {
		return true
	}
	if !h.NodeStarted() {
		return false
	}
	time.Sleep(time.Second)
	clutime := h.startup.cluster.Load()
	if clutime == 0 {
		glog.ErrorDepth(1, fmt.Sprintf("%s: cluster is starting up (?)", h.si))
	}
	return clutime > 0
}

func (h *htrun) ClusterStarted() bool { return h.startup.cluster.Load() > 0 }
func (h *htrun) markClusterStarted()  { h.startup.cluster.Store(mono.NanoTime()) }

func (h *htrun) NodeStarted() bool { return h.startup.node.Load() > 0 }
func (h *htrun) markNodeStarted()  { h.startup.node.Store(mono.NanoTime()) }

func (h *htrun) registerNetworkHandlers(networkHandlers []networkHandler) {
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
			path = cos.JoinWords(apc.Version, nh.r)
		}
		debug.Assert(nh.net != 0)
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
			debug.Assert(config.HostNet.UseIntraData && nh.net.isSet(accessNetIntraControl))
			// (not configured) control defaults to (configured) data
			h.registerIntraDataNetHandler(path, nh.h)
		}
	}
	// common Prometheus
	if h.statsT.IsPrometheus() {
		nh := networkHandler{r: "/" + apc.Metrics, h: promhttp.Handler().ServeHTTP}
		path := nh.r // absolute
		h.registerPublicNetHandler(path, nh.h)
	}
}

func (h *htrun) registerPublicNetHandler(path string, handler func(http.ResponseWriter, *http.Request)) {
	for _, v := range allHTTPverbs {
		h.netServ.pub.muxers[v].HandleFunc(path, handler)
		if !strings.HasSuffix(path, "/") {
			h.netServ.pub.muxers[v].HandleFunc(path+"/", handler)
		}
	}
}

func (h *htrun) registerIntraControlNetHandler(path string, handler func(http.ResponseWriter, *http.Request)) {
	for _, v := range allHTTPverbs {
		h.netServ.control.muxers[v].HandleFunc(path, handler)
		if !strings.HasSuffix(path, "/") {
			h.netServ.control.muxers[v].HandleFunc(path+"/", handler)
		}
	}
}

func (h *htrun) registerIntraDataNetHandler(path string, handler func(http.ResponseWriter, *http.Request)) {
	for _, v := range allHTTPverbs {
		h.netServ.data.muxers[v].HandleFunc(path, handler)
		if !strings.HasSuffix(path, "/") {
			h.netServ.data.muxers[v].HandleFunc(path+"/", handler)
		}
	}
}

func (h *htrun) init(config *cmn.Config) {
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

func (h *htrun) initNetworks() {
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
		cos.ExitLogf("Failed to get %s IPv4/hostname: %v", cmn.NetPublic, err)
	}
	if config.HostNet.Hostname != "" {
		s = " (config: " + config.HostNet.Hostname + ")"
	}
	glog.Infof("%s (user) access: [%s]%s", cmn.NetPublic, pubAddr, s)

	intraControlAddr = pubAddr
	if config.HostNet.UseIntraControl {
		icport := strconv.Itoa(config.HostNet.PortIntraControl)
		intraControlAddr, err = getNetInfo(addrList, proto, config.HostNet.HostnameIntraControl, icport)
		if err != nil {
			cos.ExitLogf("Failed to get %s IPv4/hostname: %v", cmn.NetIntraControl, err)
		}
		s = ""
		if config.HostNet.HostnameIntraControl != "" {
			s = " (config: " + config.HostNet.HostnameIntraControl + ")"
		}
		glog.Infof("%s access: [%s]%s", cmn.NetIntraControl, intraControlAddr, s)
	}
	intraDataAddr = pubAddr
	if config.HostNet.UseIntraData {
		idport := strconv.Itoa(config.HostNet.PortIntraData)
		intraDataAddr, err = getNetInfo(addrList, proto, config.HostNet.HostnameIntraData, idport)
		if err != nil {
			cos.ExitLogf("Failed to get %s IPv4/hostname: %v", cmn.NetIntraData, err)
		}
		s = ""
		if config.HostNet.HostnameIntraData != "" {
			s = " (config: " + config.HostNet.HostnameIntraData + ")"
		}
		glog.Infof("%s access: [%s]%s", cmn.NetIntraData, intraDataAddr, s)
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
		PubNet:     pubAddr,
		ControlNet: intraControlAddr,
		DataNet:    intraDataAddr,
	}
}

func mustDiffer(ip1 cluster.NetInfo, port1 int, use1 bool, ip2 cluster.NetInfo, port2 int, use2 bool, tag string) {
	if !use1 || !use2 {
		return
	}
	if ip1.Hostname == ip2.Hostname && port1 == port2 {
		cos.ExitLogf("%s: cannot use the same IP:port (%s) for two networks", tag, ip1)
	}
}

// detectNodeChanges is called at startup. Given loaded Smap, it checks whether
// this node ID is present. NOTE: we are _not_ enforcing node's (`h.si`)
// immutability - in particular, the node's IPs that, in fact, may change upon
// restart in certain environments.
func (h *htrun) detectNodeChanges(smap *smapX) (err error) {
	if smap.GetNode(h.si.ID()) == nil {
		err = fmt.Errorf("%s: not present in the loaded %s", h.si, smap)
	}
	return
}

func (h *htrun) tryLoadSmap() (_ *smapX, reliable bool) {
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

func (h *htrun) setDaemonConfigMsg(w http.ResponseWriter, r *http.Request, msg *apc.ActionMsg, query url.Values) {
	var (
		transient = cos.IsParseBool(query.Get(apc.ActTransient))
		toUpdate  = &cmn.ConfigToUpdate{}
	)
	if err := cos.MorphMarshal(msg.Value, toUpdate); err != nil {
		h.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, h.si, msg.Action, msg.Value, err)
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

func (h *htrun) run() error {
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
				addr := h.si.ControlNet.TCPEndpoint()
				errCh <- h.netServ.control.listenAndServe(addr, h.logger)
			}()
		}
		if config.HostNet.UseIntraData {
			go func() {
				addr := h.si.DataNet.TCPEndpoint()
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
func (h *htrun) pubListeningAddr(config *cmn.Config) string {
	var (
		testingEnv  = config.TestingEnv()
		k8sDetected = k8s.Detect() == nil
	)
	if testingEnv && !k8sDetected {
		return h.si.PubNet.TCPEndpoint()
	}
	return ":" + h.si.PubNet.Port
}

func (h *htrun) stopHTTPServer() {
	h.netServ.pub.shutdown()
	config := cmn.GCO.Get()
	if config.HostNet.UseIntraControl {
		h.netServ.control.shutdown()
	}
	if config.HostNet.UseIntraData {
		h.netServ.data.shutdown()
	}
}

// remove self from Smap (if required), terminate http, and wait (w/ timeout)
// for running xactions to abort
func (h *htrun) stop(rmFromSmap bool) {
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

func (h *htrun) _call(si *cluster.Snode, bargs *bcastArgs, results *bcastResults) {
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
	res := h.call(cargs)
	if bargs.async {
		freeCR(res) // discard right away
	} else {
		results.mu.Lock()
		results.s = append(results.s, res)
		results.mu.Unlock()
	}
	freeCargs(cargs)
}

func (h *htrun) call(args *callArgs) (res *callResult) {
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
		client = h.client.control
	case apc.LongTimeout:
		req, res.err = args.req.Req()
		if res.err != nil {
			break
		}
		client = h.client.data
	default:
		var cancel context.CancelFunc
		if args.timeout == 0 {
			args.timeout = cmn.Timeout.CplaneOperation()
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

	req.Header.Set(apc.HdrCallerID, h.si.ID())
	req.Header.Set(apc.HdrCallerName, h.si.Name())
	if smap := h.owner.smap.get(); smap != nil && smap.vstr != "" {
		req.Header.Set(apc.HdrCallerSmapVersion, smap.vstr)
	}
	req.Header.Set(cos.HdrUserAgent, ua)

	resp, res.err = client.Do(req)
	if res.err != nil {
		res.details = fmt.Sprintf("Failed to call %s (%s %s): %v", sid, args.req.Method, args.req.URL(), res.err)
		return
	}
	defer resp.Body.Close()
	res.status = resp.StatusCode
	res.header = resp.Header

	// err == nil && bad status: resp.Body contains the error message
	if res.status >= http.StatusBadRequest {
		if args.req.Method == http.MethodHead {
			msg := resp.Header.Get(apc.HdrError)
			res.err = cmn.S2HTTPErr(req, msg, res.status)
		} else {
			var b bytes.Buffer
			b.ReadFrom(resp.Body)
			res.err = cmn.S2HTTPErr(req, b.String(), res.status)
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
		h.keepalive.heardFrom(sid, false /* reset */)
	}
	return
}

//
// intra-cluster IPC, control plane: notify another node
//

func (h *htrun) callerNotifyFin(n cluster.Notif, err error) {
	h.callerNotify(n, err, apc.Finished)
}

func (h *htrun) callerNotifyProgress(n cluster.Notif) {
	h.callerNotify(n, nil, apc.Progress)
}

func (h *htrun) callerNotify(n cluster.Notif, err error, upon string) {
	var (
		smap  = h.owner.smap.get()
		dsts  = n.Subscribers()
		msg   = n.ToNotifMsg()
		args  = allocBcArgs()
		nodes = args.selected
	)
	debug.Assert(upon == apc.Progress || upon == apc.Finished)
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
	path := apc.URLPathNotifs.Join(upon)
	args.req = cmn.HreqArgs{Method: http.MethodPost, Path: path, Body: cos.MustMarshal(&msg)}
	args.network = cmn.NetIntraControl
	args.timeout = cmn.Timeout.MaxKeepalive()
	args.selected = nodes
	args.nodeCount = len(nodes)
	args.async = true
	_ = h.bcastSelected(args)
	freeBcArgs(args)
}

///////////////
// broadcast //
///////////////

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
		args.timeout = cmn.Timeout.CplaneOperation()
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
		debug.Assert(false)
	}
	return h.bcastNodes(args)
}

// broadcast to the specified destinations (`bargs.nodes`)
// (if specified, `bargs.req.BodyR` must implement `cos.ReadOpenCloser`)
func (h *htrun) bcastNodes(bargs *bcastArgs) sliceResults {
	var (
		results bcastResults
		wg      = cos.NewLimitedWaitGroup(cluster.MaxBcastParallel(), bargs.nodeCount)
		f       = func(si *cluster.Snode) { h._call(si, bargs, &results); wg.Done() }
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
			if !bargs.ignoreMaintenance && si.IsAnySet(cluster.NodeFlagsMaintDecomm) {
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
		wg      = cos.NewLimitedWaitGroup(cluster.MaxBcastParallel(), bargs.nodeCount)
		f       = func(si *cluster.Snode) { h._call(si, bargs, &results); wg.Done() }
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
	args.timeout = cmn.Timeout.MaxKeepalive()
	for pid, psi := range smap.Pmap {
		if pid == h.si.ID() || !smap.IsIC(psi) || smap.GetNodeNotMaint(pid) == nil {
			continue
		}
		wg.Add(1)
		go func(si *cluster.Snode) {
			cargs := allocCargs()
			{
				cargs.si = si
				cargs.req = args.req
				cargs.timeout = args.timeout
			}
			res := h.call(cargs)
			freeCargs(cargs)
			freeCR(res) // discard right away
			wg.Done()
		}(psi)
	}
	wg.Wait()
	freeBcArgs(args)
}

func (h *htrun) bcastReqGroup(w http.ResponseWriter, r *http.Request, args *bcastArgs, to int) {
	args.to = to
	results := h.bcastGroup(args)
	for _, res := range results {
		if res.err != nil {
			h.writeErr(w, r, res.toErr())
			break
		}
	}
	freeBcastRes(results)
}

//////////////////////////////////
// HTTP request parsing helpers //
//////////////////////////////////

// remove validated fields and return the resulting slice
func (h *htrun) apiItems(w http.ResponseWriter, r *http.Request, itemsAfter int,
	splitAfter bool, items []string) ([]string, error) {
	items, err := cmn.MatchItems(r.URL.Path, itemsAfter, splitAfter, items)
	if err != nil {
		h.writeErr(w, r, err)
		return nil, err
	}
	return items, nil
}

func (h *htrun) writeMsgPack(w http.ResponseWriter, r *http.Request, v msgp.Encodable, tag string) (ok bool) {
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
	h.handleWriteError(r, tag, v, err)
	return false
}

func (h *htrun) writeJSON(w http.ResponseWriter, r *http.Request, v any, tag string) bool {
	var err error
	w.Header().Set(cos.HdrContentType, cos.ContentJSONCharsetUTF)
	if isBrowser(r.Header.Get(cos.HdrUserAgent)) {
		var out []byte
		if out, err = jsoniter.MarshalIndent(v, "", "    "); err != nil {
			goto rerr
		}
		written, _ := w.Write(out)
		return written == len(out)
	}
	// non-browser client
	if err = jsoniter.NewEncoder(w).Encode(v); err == nil {
		return true
	}
rerr:
	h.handleWriteError(r, tag, v, err)
	return false
}

// See https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/User-Agent
// and https://developer.mozilla.org/en-US/docs/Web/HTTP/Browser_detection_using_the_user_agent
func isBrowser(userAgent string) bool {
	return strings.HasPrefix(userAgent, "Mozilla/5.0")
}

func (h *htrun) handleWriteError(r *http.Request, tag string, v any, err error) {
	const maxl = 32
	if daemon.stopping.Load() {
		return
	}

	var s string
	if v != nil {
		s = fmt.Sprintf("[%+v", v)
		if len(s) > maxl {
			s = s[:maxl] + "...]"
		} else {
			s += "]"
		}
	}
	msg := fmt.Sprintf("%s: failed to write bytes%s: %v (", s, tag, err)
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
	glog.Errorln(msg + ")")
	h.statsT.AddErrorHTTP(r.Method, 1)
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

func _checkAction(msg *apc.ActionMsg, expectedActions ...string) (err error) {
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

func (h *htrun) httpdaeget(w http.ResponseWriter, r *http.Request, query url.Values) {
	var (
		body any
		what = query.Get(apc.QparamWhat)
	)
	switch what {
	case apc.GetWhatConfig:
		body = cmn.GCO.Get()
	case apc.GetWhatSmap:
		body = h.owner.smap.get()
	case apc.GetWhatBMD:
		body = h.owner.bmd.get()
	case apc.GetWhatSmapVote:
		var err error
		body, err = h.cluMeta(cmetaFillOpt{})
		if err != nil {
			glog.Errorf("failed to fetch cluster config, err: %v", err)
		}
	case apc.GetWhatSnode:
		body = h.si
	case apc.GetWhatLog:
		h.sendlog(w, r, query)
		return
	case apc.GetWhatStats:
		body = h.statsT.GetWhatStats()
	default:
		h.writeErrf(w, r, "invalid GET /daemon request: unrecognized what=%s", what)
		return
	}
	h.writeJSON(w, r, body, "httpdaeget-"+what)
}

func (h *htrun) sendlog(w http.ResponseWriter, r *http.Request, query url.Values) {
	sev := query.Get(apc.QparamLogSev)
	log, err := _sev2logname(sev)
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
		glog.Errorf("failed to read %s: %v (written=%d)", log, err, written)
	}
	cos.Close(fh)
	slab.Free(buf)
}

func _sev2logname(sev string) (log string, err error) {
	dir := cmn.GCO.Get().LogDir
	if sev == "" {
		log = filepath.Join(dir, glog.InfoLogName()) // symlink
		return
	}
	v := strings.ToLower(sev)
	switch v[0] {
	case apc.LogInfo[0]:
		log = filepath.Join(dir, glog.InfoLogName())
	case apc.LogWarn[0]:
		log = filepath.Join(dir, glog.WarnLogName())
	case apc.LogErr[0]:
		log = filepath.Join(dir, glog.ErrLogName())
	default:
		err = fmt.Errorf("unknown log severity %q", sev)
	}
	return
}

////////////////////////////////////////////
// HTTP err + spec message + code + stats //
////////////////////////////////////////////

func (h *htrun) writeErr(w http.ResponseWriter, r *http.Request, err error, errCode ...int) {
	cmn.WriteErr(w, r, err, errCode...)
	h.statsT.AddErrorHTTP(r.Method, 1)
}

func (h *htrun) writeErrMsg(w http.ResponseWriter, r *http.Request, msg string, errCode ...int) {
	cmn.WriteErrMsg(w, r, msg, errCode...)
	h.statsT.AddErrorHTTP(r.Method, 1)
}

func (h *htrun) writeErrSilent(w http.ResponseWriter, r *http.Request, err error, errCode ...int) {
	const _silent = 1
	if len(errCode) == 0 {
		h.writeErr(w, r, err, http.StatusBadRequest, _silent)
	} else {
		h.writeErr(w, r, err, errCode[0], _silent)
	}
}

func (h *htrun) writeErrSilentf(w http.ResponseWriter, r *http.Request, errCode int,
	format string, a ...any) {
	err := fmt.Errorf(format, a...)
	h.writeErrSilent(w, r, err, errCode)
}

func (h *htrun) writeErrStatusf(w http.ResponseWriter, r *http.Request, errCode int, format string, a ...any) {
	err := fmt.Errorf(format, a...)
	h.writeErrMsg(w, r, err.Error(), errCode)
}

func (h *htrun) writeErrStatusSilentf(w http.ResponseWriter, r *http.Request, errCode int,
	format string, a ...any) {
	h.writeErrSilent(w, r, fmt.Errorf(format, a...), errCode)
}

func (h *htrun) writeErrf(w http.ResponseWriter, r *http.Request, format string, a ...any) {
	err := fmt.Errorf(format, a...)
	if cmn.IsNotExist(err) {
		h.writeErrMsg(w, r, err.Error(), http.StatusNotFound)
	} else {
		h.writeErrMsg(w, r, err.Error())
	}
}

func (h *htrun) writeErrURL(w http.ResponseWriter, r *http.Request) {
	if r.URL.Scheme != "" {
		h.writeErrf(w, r, "%s: invalid URL path %q", h.si, r.URL.Path)
		return
	}
	// ignore GET /favicon.ico by Browsers
	if r.URL.Path == "/favicon.ico" || r.URL.Path == "favicon.ico" {
		return
	}
	h.writeErrf(w, r, "%s: %s %q", h.si, cmn.EmptyProtoSchemeForURL, r.URL.Path)
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

////////////////
// callResult //
////////////////

// error helper for intra-cluster calls
func (res *callResult) toErr() error {
	if res.err == nil {
		return nil
	}
	// cmn.ErrHTTP
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
		return cmn.NewErrHTTP(nil, fmt.Errorf("%v%s", res.err, detail), res.status)
	}
	if res.details == "" {
		return res.err
	}
	return cmn.NewErrFailedTo(nil, "call "+res.si.String(), res.details, res.err)
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

///////////////////
// health client //
// max-ver Cii   //
///////////////////

func (h *htrun) Health(si *cluster.Snode, timeout time.Duration, query url.Values) (b []byte, status int, err error) {
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
	res := h.call(cargs)
	b, status, err = res.bytes, res.status, res.err
	freeCargs(cargs)
	freeCR(res)
	return
}

// - utilizes internal API: Health(clusterInfo) to discover a _better_ Smap, if exists
// - checkAll: query all nodes
// - consider adding max-ver BMD bit here as well (TODO)
func (h *htrun) bcastHealth(smap *smapX, checkAll bool) (*clusterInfo, int /*num confirmations*/) {
	if !smap.isValid() {
		glog.Errorf("%s: cannot execute with invalid %s", h.si, smap)
		return nil, 0
	}
	c := getMaxCii{
		h:        h,
		maxCii:   &clusterInfo{},
		query:    url.Values{apc.QparamClusterInfo: []string{"true"}},
		timeout:  cmn.Timeout.CplaneOperation(),
		checkAll: checkAll,
	}
	c.maxCii.fillSmap(smap)

	h._bch(&c, smap, apc.Proxy)
	if checkAll || (c.cnt < maxVerConfirmations && smap.CountActiveTargets() > 0) {
		h._bch(&c, smap, apc.Target)
	}
	glog.Infof("%s: %s", h.si, c.maxCii.String())
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
		wg = cos.NewLimitedWaitGroup(cluster.MaxBcastParallel(), len(nodemap))
	} else {
		count = cos.Min(cluster.MaxBcastParallel(), maxVerConfirmations<<1)
		wg = cos.NewLimitedWaitGroup(count, len(nodemap) /*have*/)
	}
	for sid, si := range nodemap {
		if sid == h.si.ID() {
			continue
		}
		if si.IsAnySet(cluster.NodeFlagsMaintDecomm) {
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

//////////////////////////
// metasync Rx handlers //
//////////////////////////

func _msdetail(ver int64, msg *aisMsg, caller string) (d string) {
	if caller != "" {
		d = " from " + caller
	}
	if msg.Action != "" || msg.UUID != "" {
		d += fmt.Sprintf(" (up from v%d, action %q, uuid %q)", ver, msg.Action, msg.UUID)
	} else {
		d += fmt.Sprintf(" (up from v%d)", ver)
	}
	return
}

func (h *htrun) extractConfig(payload msPayload, caller string) (newConfig *globalConfig, msg *aisMsg, err error) {
	if _, ok := payload[revsConfTag]; !ok {
		return
	}
	newConfig, msg = &globalConfig{}, &aisMsg{}
	confValue := payload[revsConfTag]
	reader := bytes.NewBuffer(confValue)
	if _, err1 := jsp.Decode(io.NopCloser(reader), newConfig, newConfig.JspOpts(), "extractConfig"); err1 != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "new Config", cos.BHead(confValue), err1)
		return
	}
	if msgValue, ok := payload[revsConfTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "action message", cos.BHead(msgValue), err1)
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

func (h *htrun) extractEtlMD(payload msPayload, caller string) (newMD *etlMD, msg *aisMsg, err error) {
	if _, ok := payload[revsEtlMDTag]; !ok {
		return
	}
	newMD, msg = newEtlMD(), &aisMsg{}
	etlMDValue := payload[revsEtlMDTag]
	reader := bytes.NewBuffer(etlMDValue)
	if _, err1 := jsp.Decode(io.NopCloser(reader), newMD, newMD.JspOpts(), "extractEtlMD"); err1 != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "new EtlMD", cos.BHead(etlMDValue), err1)
		return
	}
	if msgValue, ok := payload[revsEtlMDTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "action message", cos.BHead(msgValue), err1)
			return
		}
	}
	etlMD := h.owner.etl.get()
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("extract %s%s", newMD, _msdetail(etlMD.Version, msg, caller))
		glog.Errorf("extract %s%s", newMD, _msdetail(etlMD.Version, msg, caller))
	}
	if newMD.version() <= etlMD.version() {
		if newMD.version() < etlMD.version() {
			err = newErrDowngrade(h.si, etlMD.String(), newMD.String())
		}
		newMD = nil
	}
	return
}

func (h *htrun) extractSmap(payload msPayload, caller string) (newSmap *smapX, msg *aisMsg, err error) {
	if _, ok := payload[revsSmapTag]; !ok {
		return
	}
	newSmap, msg = &smapX{}, &aisMsg{}
	smapValue := payload[revsSmapTag]
	reader := bytes.NewBuffer(smapValue)
	if _, err1 := jsp.Decode(io.NopCloser(reader), newSmap, newSmap.JspOpts(), "extractSmap"); err1 != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "new Smap", cos.BHead(smapValue), err1)
		return
	}
	if msgValue, ok := payload[revsSmapTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "action message", cos.BHead(msgValue), err1)
			return
		}
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

func (h *htrun) extractRMD(payload msPayload, caller string) (newRMD *rebMD, msg *aisMsg, err error) {
	if _, ok := payload[revsRMDTag]; !ok {
		return
	}
	newRMD, msg = &rebMD{}, &aisMsg{}
	rmdValue := payload[revsRMDTag]
	if err1 := jsoniter.Unmarshal(rmdValue, newRMD); err1 != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "new RMD", cos.BHead(rmdValue), err1)
		return
	}
	if msgValue, ok := payload[revsRMDTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "action message", cos.BHead(msgValue), err1)
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

func (h *htrun) extractBMD(payload msPayload, caller string) (newBMD *bucketMD, msg *aisMsg, err error) {
	if _, ok := payload[revsBMDTag]; !ok {
		return
	}
	newBMD, msg = &bucketMD{}, &aisMsg{}
	bmdValue := payload[revsBMDTag]
	reader := bytes.NewBuffer(bmdValue)
	if _, err1 := jsp.Decode(io.NopCloser(reader), newBMD, newBMD.JspOpts(), "extractBMD"); err1 != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "new BMD", cos.BHead(bmdValue), err1)
		return
	}
	if msgValue, ok := payload[revsBMDTag+revsActionTag]; ok {
		if err1 := jsoniter.Unmarshal(msgValue, msg); err1 != nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "action message", cos.BHead(msgValue), err1)
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

func (h *htrun) receiveSmap(newSmap *smapX, msg *aisMsg, payload msPayload,
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

func (h *htrun) receiveEtlMD(newEtlMD *etlMD, msg *aisMsg, payload msPayload,
	caller string, cb func(newELT *etlMD, oldETL *etlMD, action string)) (err error) {
	if newEtlMD == nil {
		return
	}
	etlMD := h.owner.etl.get()
	glog.Infof("receive %s%s", newEtlMD, _msdetail(etlMD.Version, msg, caller))

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
		cb(newEtlMD, etlMD, msg.Action)
	}
	return
}

func (h *htrun) _recvCfg(newConfig *globalConfig, msg *aisMsg, payload msPayload, caller string) (err error) {
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
	if err = cmn.GCO.Update(&newConfig.ClusterConfig); err != nil {
		return
	}
	// update assorted read-mostly knobs
	cmn.Features = newConfig.Features
	cmn.Timeout.Set(&newConfig.ClusterConfig)
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
			err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "action message", cos.BHead(msgValue), err)
			return nil, err
		}
	}
	tokenList := &tokenList{}
	if err := jsoniter.Unmarshal(bytes, tokenList); err != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, h.si, "blocked token list", cos.BHead(bytes), err)
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
func (h *htrun) join(query url.Values, contactURLs ...string) (res *callResult) {
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
	// NOTE: The url above is either config or the primary's ControlNet.URL;
	//       Add its public URL as "more static" in various virtualized environments.
	if psi != nil {
		addCandidate(psi.URL(cmn.NetPublic))
	}
	addCandidate(config.Proxy.PrimaryURL)
	addCandidate(config.Proxy.DiscoveryURL)
	addCandidate(config.Proxy.OriginalURL)
	for _, u := range contactURLs {
		addCandidate(u)
	}
	sleep := cos.MaxDuration(2*time.Second, cmn.Timeout.MaxKeepalive())
	for i := 0; i < 4; i++ {
		for _, candidateURL := range candidates {
			if daemon.stopping.Load() {
				return
			}
			if resPrev != nil {
				freeCR(resPrev)
				resPrev = nil //nolint:ineffassign // readability
			}
			res = h.registerToURL(candidateURL, nil, apc.DefaultTimeout, query, false)
			if res.err == nil {
				glog.Infof("%s: joined cluster via %s", h.si, candidateURL)
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
	res = h.registerToURL(primaryURL, nil, apc.DefaultTimeout, query, false)
	if res.err == nil {
		glog.Infof("%s: joined cluster via %s", h.si, primaryURL)
	}
	return
}

func (h *htrun) registerToURL(url string, psi *cluster.Snode, tout time.Duration, q url.Values, keepalive bool) *callResult {
	var (
		path          string
		skipPrxKalive = h.si.IsProxy() || keepalive
		opts          = cmetaFillOpt{
			skipSmap:      skipPrxKalive,
			skipBMD:       skipPrxKalive,
			skipRMD:       keepalive,
			skipConfig:    keepalive,
			skipEtlMD:     keepalive,
			fillRebMarker: !keepalive,
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
	res := h.call(cargs)
	freeCargs(cargs)
	return res
}

func (h *htrun) sendKalive(smap *smapX, timeout time.Duration) (pid string, status int, err error) {
	if daemon.stopping.Load() {
		err = fmt.Errorf("%s is stopping", h.si)
		return
	}
	primaryURL, psi := h.getPrimaryURLAndSI(smap)
	pid = psi.ID()
	res := h.registerToURL(primaryURL, psi, timeout, nil, true)
	if res.err != nil {
		if strings.Contains(res.err.Error(), ciePrefix) {
			cos.ExitLogf("%v", res.err) // FATAL: cluster integrity error (cie)
		}
		status, err = res.status, res.err
		freeCR(res)
		return
	}
	debug.Assert(len(res.bytes) == 0)
	freeCR(res)
	return
}

func (h *htrun) getPrimaryURLAndSI(smap *smapX) (url string, psi *cluster.Snode) {
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

func (h *htrun) pollClusterStarted(config *cmn.Config, psi *cluster.Snode) (maxCii *clusterInfo) {
	var (
		sleep, total, rediscover time.Duration
		healthTimeout            = cmn.Timeout.CplaneOperation()
		query                    = url.Values{apc.QparamAskPrimary: []string{"true"}}
	)
	for {
		sleep = cos.MinDuration(cmn.Timeout.MaxKeepalive(), sleep+time.Second)
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
			if cii, cnt := h.bcastHealth(smap, true /*checkAll*/); cii != nil && cii.Smap.Version > smap.version() {
				var pid string
				if psi != nil {
					pid = psi.ID()
				}
				if cii.Smap.Primary.ID != pid && cnt >= maxVerConfirmations {
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
	res := h.call(cargs)
	status, err = res.status, res.err
	if err != nil {
		f := glog.Errorf
		if ignoreErr {
			f = glog.Warningf
		}
		f("%s: failed to unreg self, err: %v(%d)", h.si, err, status)
	}
	freeCargs(cargs)
	freeCR(res)
	return
}

func (h *htrun) healthByExternalWD(w http.ResponseWriter, r *http.Request) (responded bool) {
	callerID := r.Header.Get(apc.HdrCallerID)
	caller := r.Header.Get(apc.HdrCallerName)
	// external call
	if callerID == "" && caller == "" {
		readiness := cos.IsParseBool(r.URL.Query().Get(apc.QparamHealthReadiness))
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

//
// intra-cluster request validations and helpers
//

func (h *htrun) isIntraCall(hdr http.Header, fromPrimary bool) (err error) {
	debug.Assert(hdr != nil)
	var (
		smap       = h.owner.smap.get()
		callerID   = hdr.Get(apc.HdrCallerID)
		callerName = hdr.Get(apc.HdrCallerName)
		callerSver = hdr.Get(apc.HdrCallerSmapVersion)
		callerVer  int64
		erP        error
	)
	if ok := callerID != "" && callerName != ""; !ok {
		return fmt.Errorf("%s: expected %s request", h.si, cmn.NetIntraControl)
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
			if h.ClusterStarted() {
				glog.Errorf("%s: %s < %s-Smap(v%s) - proceeding anyway...", h.si, smap, callerName, callerSver)
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
		return fmt.Errorf("%s: expected %s from a valid node, %s", h.si, cmn.NetIntraControl, smap)
	}
	return fmt.Errorf("%s: expected %s from primary (and not %s), %s", h.si, cmn.NetIntraControl, caller, smap)
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
	h.writeErrf(w, r, "%s: expected %s request", h.si, cmn.NetIntraControl)
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

func ptLatency(tts int64, ptime string) (delta int64) {
	pts, err := cos.S2UnixNano(ptime)
	if err != nil {
		return
	}
	delta = tts - pts
	if delta < 0 && -delta < int64(clusterClockDrift) {
		delta = 0
	}
	return
}

//
// aisMsg reader & constructors
//

func (h *htrun) readAisMsg(w http.ResponseWriter, r *http.Request) (msg *aisMsg, err error) {
	msg = &aisMsg{}
	err = cmn.ReadJSON(w, r, msg)
	if err == nil && glog.FastV(4, glog.SmoduleAIS) {
		glog.InfoDepth(1, h.si.String()+": "+msg.String())
	}
	return
}

func (msg *aisMsg) String() string {
	s := msg.ActionMsg.String()
	if msg.UUID == "" {
		return s
	}
	return s + ", uuid=" + msg.UUID
}

func (h *htrun) newAmsgStr(msgStr string, bmd *bucketMD) *aisMsg {
	return h.newAmsg(&apc.ActionMsg{Value: msgStr}, bmd)
}

func (h *htrun) newAmsgActVal(act string, val any) *aisMsg {
	return h.newAmsg(&apc.ActionMsg{Action: act, Value: val}, nil)
}

func (h *htrun) newAmsg(actionMsg *apc.ActionMsg, bmd *bucketMD, uuid ...string) *aisMsg {
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

// apc.ActionMsg c-tor and reader
func (h *htrun) readActionMsg(w http.ResponseWriter, r *http.Request) (msg *apc.ActionMsg, err error) {
	msg = &apc.ActionMsg{}
	err = cmn.ReadJSON(w, r, msg)
	if err == nil && glog.FastV(4, glog.SmoduleAIS) {
		glog.InfoDepth(1, h.si.String()+": "+msg.String())
	}
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
