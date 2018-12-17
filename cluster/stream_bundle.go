// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/transport"
)

const (
	Targets = iota
	Proxies
	AllNodes
)
const (
	closeFin = iota
	closeStop
)

type (
	StreamBundle struct {
		sowner     Sowner
		smap       *Smap // current Smap
		smaplock   *sync.Mutex
		lsnode     *Snode // local Snode
		client     *http.Client
		network    string
		trname     string
		streams    unsafe.Pointer // points to bundle (below)
		extra      transport.Extra
		rxNodeType int // receiving nodes: [Targets, ..., AllNodes ] enum above
		multiplier int // optionally, greater than 1 number of streams per destination (with round-robin selection)
	}
	Stats map[string]*transport.Stats // by DaemonID
	//
	// private types to support multiple streams to the same destination with round-robin selection
	//
	stsdest []*transport.Stream // STreams to the Same Destination (stsdest)
	robin   struct {
		stsdest stsdest
		i       int64
	}
	bundle map[string]*robin // stream "bundle" indexed by DaemonID
)

var (
	debug bool
)

func init() {
	debug = os.Getenv("DFC_STREAM_DEBUG") != ""
}

//
// API
//

func NewStreamBundle(sowner Sowner, lsnode *Snode, cl *http.Client, network, trname string,
	extra *transport.Extra, ntype int, multiplier ...int) (sb *StreamBundle) {
	if !cmn.NetworkIsKnown(network) {
		glog.Errorf("Unknown network %s, expecting one of: %v", network, cmn.KnownNetworks)
	}
	cmn.Assert(ntype == Targets || ntype == Proxies || ntype == AllNodes)
	listeners := sowner.Listeners()
	sb = &StreamBundle{
		sowner:     sowner,
		smap:       &Smap{},
		smaplock:   &sync.Mutex{},
		lsnode:     lsnode,
		client:     cl,
		network:    network,
		trname:     trname,
		rxNodeType: ntype,
		multiplier: 1,
	}
	if extra != nil {
		sb.extra = *extra
	}
	if len(multiplier) > 0 {
		cmn.Assert(multiplier[0] > 1 && multiplier[0] < 256)
		sb.multiplier = multiplier[0]
	}
	//
	// finish construction: establish streams and register this stream-bundle as Smap listener
	//
	sb.resync()

	listeners.Reg(sb)
	return
}

// Close closes all contained streams and unregisters the bundle from Smap listeners;
// graceful=true blocks until all pending objects get completed (for "completion", see transport/README.md)
func (sb *StreamBundle) Close(gracefully bool) {
	if gracefully {
		sb.apply(closeFin)
	} else {
		sb.apply(closeStop)
	}
	listeners := sb.sowner.Listeners()
	listeners.Unreg(sb)
}

//
// Following are the two main API methods:
// - to broadcast via all established streams, use SendV() and omit the last arg;
// - otherwise, use SendV() with destinations specified as a comma-separated list,
//   or use Send() with a slice of nodes for destinations.
//

func (sb *StreamBundle) SendV(hdr transport.Header, reader cmn.ReadOpenCloser, cb transport.SendCallback, nodes ...*Snode) (err error) {
	return sb.Send(hdr, reader, cb, nodes)
}

func (sb *StreamBundle) Send(hdr transport.Header, reader cmn.ReadOpenCloser, cb transport.SendCallback, nodes []*Snode) (err error) {
	var (
		prc     *int64 // completion refcount (optional)
		streams = sb.get()
	)
	if len(streams) == 0 {
		err = fmt.Errorf("No streams %s => .../%s", sb.lsnode, sb.trname)
		return
	}
	if cb == nil {
		cb = sb.extra.Callback
	}
	if nodes == nil {
		if cb != nil && len(streams) > 1 {
			prc = new(int64)
			*prc = int64(len(streams)) // only when there's a callback and more than 1 dest-s
		}
		//
		// Reader-reopening logic: since the streams in a bundle are mutually independent
		// and asynchronous, reader.Open() (aka reopen) is skipped for the 1st replica
		// that we put on the wire and is done for the 2nd, 3rd, etc. replicas.
		// In other words, for the N object replicas over the N bundled streams, the
		// original reader will get reopened (N-1) times.
		//
		reopen := false
		for _, robin := range streams {
			if err = sb.sendOne(robin, hdr, reader, cb, prc, reopen); err != nil {
				return
			}
			reopen = true
		}
	} else {
		// first, find out whether the designated streams are present
		for _, di := range nodes {
			if _, ok := streams[di.DaemonID]; !ok {
				err = fmt.Errorf("%s: destination mismatch: stream => %s %s", sb, di, cmn.DoesNotExist)
				return
			}
		}
		if cb != nil && len(nodes) > 1 {
			prc = new(int64)
			*prc = int64(len(nodes)) // ditto
		}
		// second, do send. Same comment wrt reopening.
		reopen := false
		for _, di := range nodes {
			robin := streams[di.DaemonID]
			if err = sb.sendOne(robin, hdr, reader, cb, prc, reopen); err != nil {
				return
			}
			reopen = true
		}
	}
	return
}

// implements cluster.Slistener interface. registers with cluster.SmapListeners

var _ Slistener = &StreamBundle{}

func (sb *StreamBundle) String() string {
	return sb.lsnode.DaemonID + "=>" + sb.network + "/" + sb.trname
}

// keep streams to => (clustered nodes as per rxNodeType) in sync at all times
func (sb *StreamBundle) SmapChanged() {
	smap := sb.sowner.Get()
	if smap.Version == sb.smap.Version {
		return
	}
	go sb.resync()
}

func (sb *StreamBundle) apply(action int) {
	streams := sb.get()
	for _, robin := range streams {
		for _, s := range robin.stsdest {
			if s.Terminated() {
				continue
			}
			switch action {
			case closeFin:
				s.Fin()
			case closeStop:
				s.Stop()
			default:
				cmn.Assert(false)
			}
		}
	}
}

// TODO: collect stats from all (stsdest) STreams to the Same Destination, and possibly
//       aggregate averages actross them.
func (sb *StreamBundle) GetStats() Stats {
	streams := sb.get()
	stats := make(Stats, len(streams))
	for id, robin := range streams {
		s := robin.stsdest[0]
		tstat := s.GetStats()
		stats[id] = &tstat
	}
	return stats
}

//==========================================================================
//
// private methods
//
//==========================================================================

func (sb *StreamBundle) get() (bun bundle) {
	optr := (*bundle)(atomic.LoadPointer(&sb.streams))
	if optr != nil {
		bun = *optr
	}
	return
}

func (sb *StreamBundle) sendOne(robin *robin, hdr transport.Header, reader cmn.ReadOpenCloser,
	cb transport.SendCallback, prc *int64, reopen bool) (err error) {
	var (
		i       int
		reader2 io.ReadCloser = reader
	)
	if reopen && reader != nil {
		if reader2, err = reader.Open(); err != nil { // reopen for every destination
			err = fmt.Errorf("Unexpected: %s failed to reopen reader, err: %v", sb, err)
			return
		}
	}
	if sb.multiplier > 1 {
		i = int(atomic.AddInt64(&robin.i, 1)) % len(robin.stsdest)
	}
	s := robin.stsdest[i]
	err = s.Send(hdr, reader2, cb, prc)
	return
}

// "resync" streams asynchronously (is a slowpath); calls stream.Stop()
func (sb *StreamBundle) resync() {
	sb.smaplock.Lock()
	defer sb.smaplock.Unlock()
	smap := sb.sowner.Get()
	if smap.Version == sb.smap.Version {
		return
	}
	cmn.Assert(smap.Version > sb.smap.Version)

	var old, new []NodeMap
	switch sb.rxNodeType {
	case Targets:
		old = []NodeMap{sb.smap.Tmap}
		new = []NodeMap{smap.Tmap}
	case Proxies:
		old = []NodeMap{sb.smap.Pmap}
		new = []NodeMap{smap.Pmap}
	case AllNodes:
		old = []NodeMap{sb.smap.Tmap, sb.smap.Pmap}
		new = []NodeMap{smap.Tmap, smap.Pmap}
	default:
		cmn.Assert(false)
	}
	added, removed := nodeMapDelta(old, new)

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
		if id == sb.lsnode.DaemonID {
			continue
		}
		toURL := si.URL(sb.network) + cmn.URLPath(cmn.Version, cmn.Transport, sb.trname) // NOTE: destination URL
		nrobin := &robin{stsdest: make(stsdest, sb.multiplier)}
		for k := 0; k < sb.multiplier; k++ {
			ns := transport.NewStream(sb.client, toURL, &sb.extra)
			if sb.multiplier > 1 {
				glog.Infof("%s: added new stream %s(%d) => %s://%s", sb, ns, k, id, toURL)
			} else {
				glog.Infof("%s: added new stream %s => %s://%s", sb, ns, id, toURL)
			}
			nrobin.stsdest[k] = ns
		}
		nbundle[id] = nrobin
	}
	for id := range removed {
		if id == sb.lsnode.DaemonID {
			continue
		}
		orobin := nbundle[id]
		for k := 0; k < sb.multiplier; k++ {
			os := orobin.stsdest[k]
			if !os.Terminated() {
				os.Stop() // the node is gone but the stream appears to be still active - stop it
			}
			glog.Infof("%s: removed stream %s => %s://%s", sb, os, id, os.URL())
		}
		delete(nbundle, id)
	}
	atomic.StorePointer(&sb.streams, unsafe.Pointer(&nbundle)) // put
	sb.smap = smap
}
