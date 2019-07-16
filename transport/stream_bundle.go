// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

const (
	closeFin = iota
	closeStop

	// IntraBundleMultiplier is used for intra-cluster workloads when each
	// target talks to each other target: dSort, EC.
	IntraBundleMultiplier = 4
)

type (
	StreamBundle struct {
		sowner       cluster.Sowner
		smap         *cluster.Smap // current Smap
		smaplock     *sync.Mutex
		lsnode       *cluster.Snode // local Snode
		client       *http.Client
		network      string
		trname       string
		streams      atomic.Pointer // points to bundle (below)
		extra        Extra
		lid          string
		rxNodeType   int // receiving nodes: [Targets, ..., AllNodes ] enum above
		multiplier   int // optionally, greater than 1 number of streams per destination (with round-robin selection)
		manualResync bool
	}
	BundleStats map[string]*Stats // by DaemonID
	//
	// private types to support multiple streams to the same destination with round-robin selection
	//
	stsdest []*Stream // STreams to the Same Destination (stsdest)
	robin   struct {
		stsdest stsdest
		i       atomic.Int64
	}
	bundle map[string]*robin // stream "bundle" indexed by DaemonID

	SBArgs struct {
		Network      string
		Trname       string
		Extra        *Extra
		Ntype        int // cluster.Target (0) by default
		Multiplier   int
		ManualResync bool // auto-resync by default
	}
)

//
// API
//

func NewStreamBundle(sowner cluster.Sowner, lsnode *cluster.Snode, cl *http.Client, sbArgs SBArgs) (sb *StreamBundle) {
	if !cmn.NetworkIsKnown(sbArgs.Network) {
		glog.Errorf("Unknown network %s, expecting one of: %v", sbArgs.Network, cmn.KnownNetworks)
	}
	cmn.Assert(sbArgs.Ntype == cluster.Targets || sbArgs.Ntype == cluster.Proxies || sbArgs.Ntype == cluster.AllNodes)
	listeners := sowner.Listeners()
	sb = &StreamBundle{
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

	// register this stream-bundle as Smap listener
	if !sb.manualResync {
		listeners.Reg(sb)
	}
	if !sb.extra.compressed() {
		sb.lid = fmt.Sprintf("sb[%s=>%s/%s]", sb.lsnode.DaemonID, sb.network, sb.trname)
	} else {
		sb.lid = fmt.Sprintf("sb[%s=>%s/%s[%s]]", sb.lsnode.DaemonID, sb.network, sb.trname,
			cmn.B2S(int64(sb.extra.Config.Compression.BlockMaxSize), 0))
	}
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
	if !sb.manualResync {
		listeners := sb.sowner.Listeners()
		listeners.Unreg(sb)
	}
}

//
// Following are the two main API methods:
// - to broadcast via all established streams, use SendV() and omit the last arg;
// - otherwise, use SendV() with destinations specified as a comma-separated list,
//   or use Send() with a slice of nodes for destinations.
//

func (sb *StreamBundle) SendV(hdr Header, reader cmn.ReadOpenCloser, cb SendCallback,
	cmplPtr unsafe.Pointer, nodes ...*cluster.Snode) (err error) {
	return sb.Send(hdr, reader, cb, cmplPtr, nodes)
}

func (sb *StreamBundle) Send(hdr Header, reader cmn.ReadOpenCloser, cb SendCallback,
	cmplPtr unsafe.Pointer, nodes []*cluster.Snode) (err error) {
	var (
		prc     *atomic.Int64 // completion refcount (optional)
		streams = sb.get()
	)
	if len(streams) == 0 {
		err = fmt.Errorf("no streams %s => .../%s", sb.lsnode, sb.trname)
		return
	}
	if cb == nil {
		cb = sb.extra.Callback
	}
	if nodes == nil {
		if cb != nil && len(streams) > 1 {
			prc = atomic.NewInt64(int64(len(streams))) // only when there's a callback and more than 1 dest-s
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
			if err = sb.sendOne(robin, hdr, reader, cb, cmplPtr, prc, reopen); err != nil {
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
			prc = atomic.NewInt64(int64(len(nodes)))
		}
		// second, do send. Same comment wrt reopening.
		reopen := false
		for _, di := range nodes {
			robin := streams[di.DaemonID]
			if err = sb.sendOne(robin, hdr, reader, cb, cmplPtr, prc, reopen); err != nil {
				return
			}
			reopen = true
		}
	}
	return
}

// implements cluster.Slistener interface. registers with cluster.SmapListeners

var _ cluster.Slistener = &StreamBundle{}

func (sb *StreamBundle) String() string { return sb.lid }

// keep streams to => (clustered nodes as per rxNodeType) in sync at all times
func (sb *StreamBundle) ListenSmapChanged(newSmapVersionChannel chan int64) {
	for {
		newSmapVersion, ok := <-newSmapVersionChannel

		if !ok {
			// channel closed by Unreg, it's safe to end listening
			return
		}

		if newSmapVersion <= sb.smap.Version {
			continue
		}

		sb.Resync()
	}
}

func (sb *StreamBundle) apply(action int) {
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

// TODO: collect stats from all (stsdest) STreams to the Same Destination, and possibly
//       aggregate averages actross them.
func (sb *StreamBundle) GetStats() BundleStats {
	streams := sb.get()
	stats := make(BundleStats, len(streams))
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
	optr := (*bundle)(sb.streams.Load())
	if optr != nil {
		bun = *optr
	}
	return
}

func (sb *StreamBundle) sendOne(robin *robin, hdr Header, reader cmn.ReadOpenCloser, cb SendCallback,
	cmplPtr unsafe.Pointer, prc *atomic.Int64, reopen bool) (err error) {
	var (
		i       int
		reader2 io.ReadCloser = reader
	)
	if reopen && reader != nil {
		if reader2, err = reader.Open(); err != nil { // reopen for every destination
			err = fmt.Errorf("unexpected: %s failed to reopen reader, err: %v", sb, err)
			return
		}
	}
	if sb.multiplier > 1 {
		i = int(robin.i.Inc()) % len(robin.stsdest)
	}
	s := robin.stsdest[i]
	err = s.Send(hdr, reader2, cb, cmplPtr, prc)
	return
}

// "Resync" streams asynchronously (is a slowpath); calls stream.Stop()
func (sb *StreamBundle) Resync() {
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
		if id == sb.lsnode.DaemonID {
			continue
		}
		toURL := si.URL(sb.network) + cmn.URLPath(cmn.Version, cmn.Transport, sb.trname) // NOTE: destination URL
		nrobin := &robin{stsdest: make(stsdest, sb.multiplier)}
		for k := 0; k < sb.multiplier; k++ {
			var (
				s  string
				ns = NewStream(sb.client, toURL, &sb.extra)
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
		if id == sb.lsnode.DaemonID {
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
