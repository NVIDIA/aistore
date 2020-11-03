// Package bundle provides multi-streaming transport with the functionality
// to dynamically (un)register receive endpoints, establish long-lived flows, and more.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package bundle

import (
	"fmt"
	"io"
	"sync"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/transport"
)

const (
	closeFin = iota
	closeStop

	// bundle.Multiplier is used to "amplify" intra-cluster workloads when each
	// target talks to each other target: dSort, EC, rebalance, ETL, etc.
	Multiplier = 4
)

type (
	Streams struct {
		sowner       cluster.Sowner
		smap         *cluster.Smap // current Smap
		smaplock     *sync.Mutex
		lsnode       *cluster.Snode // local Snode
		client       transport.Client
		network      string
		trname       string
		streams      atomic.Pointer // points to bundle (below)
		extra        transport.Extra
		lid          string
		rxNodeType   int // receiving nodes: [Targets, ..., AllNodes ] enum above
		multiplier   int // optionally, greater than 1 number of streams per destination (with round-robin selection)
		manualResync bool
	}
	BundleStats map[string]*transport.Stats // by DaemonID
	//
	// private types to support multiple streams to the same destination with round-robin selection
	//
	stsdest []*transport.Stream // STreams to the Same Destination (stsdest)
	robin   struct {
		stsdest stsdest
		i       atomic.Int64
	}
	bundle map[string]*robin // stream "bundle" indexed by DaemonID

	Args struct {
		Network      string
		Trname       string
		Extra        *transport.Extra
		Ntype        int // cluster.Target (0) by default
		Multiplier   int
		ManualResync bool // auto-resync by default
	}
)

// interface guard
var (
	_ cluster.Slistener = &Streams{}
)

//
// API
//

func NewStreams(sowner cluster.Sowner, lsnode *cluster.Snode, cl transport.Client, sbArgs Args) (sb *Streams) {
	if !cmn.NetworkIsKnown(sbArgs.Network) {
		glog.Errorf("Unknown network %s, expecting one of: %v", sbArgs.Network, cmn.KnownNetworks)
	}
	cmn.Assert(sbArgs.Ntype == cluster.Targets || sbArgs.Ntype == cluster.Proxies || sbArgs.Ntype == cluster.AllNodes)
	listeners := sowner.Listeners()
	sb = &Streams{
		sowner:       sowner,
		smap:         &cluster.Smap{},
		smaplock:     &sync.Mutex{},
		lsnode:       lsnode,
		client:       cl,
		network:      sbArgs.Network,
		trname:       sbArgs.Trname,
		rxNodeType:   sbArgs.Ntype,
		multiplier:   sbArgs.Multiplier,
		manualResync: sbArgs.ManualResync,
	}
	if sbArgs.Extra != nil {
		sb.extra = *sbArgs.Extra
	}
	if sb.multiplier == 0 {
		sb.multiplier = 1
	}
	if sb.extra.Config == nil {
		sb.extra.Config = cmn.GCO.Get()
	}
	// update streams when Smap changes
	sb.Resync()

	if !sb.extra.Compressed() {
		sb.lid = fmt.Sprintf("sb[%s=>%s/%s]", sb.lsnode.ID(), sb.network, sb.trname)
	} else {
		sb.lid = fmt.Sprintf("sb[%s=>%s/%s[%s]]", sb.lsnode.ID(), sb.network, sb.trname,
			cmn.B2S(int64(sb.extra.Config.Compression.BlockMaxSize), 0))
	}
	// register this stream-bundle as Smap listener
	if !sb.manualResync {
		listeners.Reg(sb)
	}
	return
}

// Close closes all contained streams and unregisters the bundle from Smap listeners;
// graceful=true blocks until all pending objects get completed (for "completion", see transport/README.md)
func (sb *Streams) Close(gracefully bool) {
	if gracefully {
		sb.apply(closeFin)
	} else {
		sb.apply(closeStop)
	}
	if !sb.manualResync {
		listeners := sb.sowner.Listeners()
		listeners.Unreg(sb)
	}
}

// (nodes == nil): transmit via all established streams
func (sb *Streams) Send(obj *transport.Obj, roc cmn.ReadOpenCloser, nodes ...*cluster.Snode) (err error) {
	var (
		streams = sb.get()
		reopen  bool
	)
	if len(streams) == 0 {
		err = fmt.Errorf("no streams %s => .../%s", sb.lsnode, sb.trname)
		return
	}
	if obj.Callback == nil {
		obj.Callback = sb.extra.Callback
	}
	if obj.IsHeaderOnly() {
		roc = nil
	}
	if nodes == nil {
		obj.SetPrc(len(streams))

		// Reader-reopening logic: since the streams in a bundle are mutually independent
		// and asynchronous, reader.Open() (aka reopen) is skipped for the 1st replica
		// that we put on the wire and is done for the 2nd, 3rd, etc. replicas.
		// In other words, for the N object replicas over the N bundled streams, the
		// original reader will get reopened (N-1) times.
		for _, robin := range streams {
			if err = sb.sendOne(obj, roc, robin, reopen); err != nil {
				return
			}
			reopen = true
		}
	} else {
		// first, find out whether the designated streams are present
		for _, di := range nodes {
			if _, ok := streams[di.ID()]; !ok {
				err = fmt.Errorf("%s: destination mismatch: stream => %s %s", sb, di, cmn.DoesNotExist)
				return
			}
		}
		obj.SetPrc(len(nodes))

		// second, do send. Same comment wrt reopening.
		for _, di := range nodes {
			robin := streams[di.ID()]
			if err = sb.sendOne(obj, roc, robin, reopen); err != nil {
				return
			}
			reopen = true
		}
	}
	return
}

func (sb *Streams) String() string { return sb.lid }

// keep streams to => (clustered nodes as per rxNodeType) in sync at all times
func (sb *Streams) ListenSmapChanged() {
	smap := sb.sowner.Get()
	if smap.Version <= sb.smap.Version {
		return
	}
	sb.Resync()
}

func (sb *Streams) GetStats() BundleStats {
	streams := sb.get()
	stats := make(BundleStats, len(streams))
	for id, robin := range streams {
		s := robin.stsdest[0]
		tstat := s.GetStats()
		stats[id] = &tstat
	}
	return stats
}

//
// private methods
//

func (sb *Streams) get() (bun bundle) {
	optr := (*bundle)(sb.streams.Load())
	if optr != nil {
		bun = *optr
	}
	return
}

// one obj, one stream
func (sb *Streams) sendOne(obj *transport.Obj, roc cmn.ReadOpenCloser, robin *robin, reopen bool) (err error) {
	one := *obj
	one.Reader = roc // reduce to io.ReadCloser
	if reopen && roc != nil {
		var reader io.ReadCloser
		if reader, err = roc.Open(); err != nil { // reopen for every destination
			return fmt.Errorf("unexpected: %s failed to reopen reader, err: %v", sb, err)
		}
		one.Reader = reader
	}
	if sb.multiplier <= 1 {
		s0 := robin.stsdest[0]
		return s0.Send(&one)
	}
	i := int(robin.i.Inc()) % len(robin.stsdest)
	s := robin.stsdest[i]
	return s.Send(&one)
}

func (sb *Streams) apply(action int) {
	cmn.Assert(action == closeFin || action == closeStop)
	var (
		streams = sb.get()
		wg      = &sync.WaitGroup{}
	)
	for _, robin := range streams {
		wg.Add(1)
		go func(stsdest stsdest, wg *sync.WaitGroup) {
			for _, s := range stsdest {
				if !s.Terminated() {
					if action == closeFin {
						s.Fin()
					} else {
						s.Stop()
					}
				}
			}
			wg.Done()
		}(robin.stsdest, wg)
	}
	wg.Wait()
}

// "Resync" streams asynchronously (is a slowpath); calls stream.Stop()
func (sb *Streams) Resync() {
	sb.smaplock.Lock()
	defer sb.smaplock.Unlock()
	smap := sb.sowner.Get()
	if smap.Version <= sb.smap.Version {
		return
	}

	var old, new []cluster.NodeMap
	switch sb.rxNodeType {
	case cluster.Targets:
		old = []cluster.NodeMap{sb.smap.Tmap}
		new = []cluster.NodeMap{smap.Tmap}
	case cluster.Proxies:
		old = []cluster.NodeMap{sb.smap.Pmap}
		new = []cluster.NodeMap{smap.Pmap}
	case cluster.AllNodes:
		old = []cluster.NodeMap{sb.smap.Tmap, sb.smap.Pmap}
		new = []cluster.NodeMap{smap.Tmap, smap.Pmap}
	default:
		cmn.Assert(false)
	}
	added, removed := cluster.NodeMapDelta(old, new)

	obundle := sb.get()
	l := len(added) - len(removed)
	if obundle != nil {
		l = cmn.Max(len(obundle), len(obundle)+l)
	}
	nbundle := make(map[string]*robin, l)
	for id, robin := range obundle {
		nbundle[id] = robin
	}
	for id, si := range added {
		if id == sb.lsnode.ID() {
			continue
		}
		toURL := si.URL(sb.network) + transport.ObjURLPath(sb.trname) // direct destination URL
		nrobin := &robin{stsdest: make(stsdest, sb.multiplier)}
		for k := 0; k < sb.multiplier; k++ {
			var (
				s  string
				ns = transport.NewObjStream(sb.client, toURL, &sb.extra)
			)
			if sb.multiplier > 1 {
				s = fmt.Sprintf("(%d)", k)
			}
			glog.Infof("%s: [+] %s%s => %s via %s", sb, ns, s, id, toURL)
			nrobin.stsdest[k] = ns
		}
		nbundle[id] = nrobin
	}
	for id := range removed {
		if id == sb.lsnode.ID() {
			continue
		}
		orobin := nbundle[id]
		for k := 0; k < sb.multiplier; k++ {
			os := orobin.stsdest[k]
			if !os.Terminated() {
				os.Stop() // the node is gone but the stream appears to be still active - stop it
			}
			glog.Infof("%s: [-] %s => %s via %s", sb, os, id, os.URL())
		}
		delete(nbundle, id)
	}
	sb.streams.Store(unsafe.Pointer(&nbundle))
	sb.smap = smap
}
