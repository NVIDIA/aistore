// Package bundle provides multi-streaming transport with the functionality
// to dynamically (un)register receive endpoints, establish long-lived flows, and more.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package bundle

import (
	"fmt"
	"sync"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/transport"
)

const (
	closeFin = iota
	closeStop
)

type (
	Streams struct {
		sowner       meta.Sowner
		client       transport.Client
		smap         *meta.Smap // current Smap
		smaplock     *sync.Mutex
		lsnode       *meta.Snode    // this node
		streams      atomic.Pointer // points to the bundle (map below)
		trname       string
		network      string
		lid          string
		extra        transport.Extra
		rxNodeType   int // receiving nodes: [Targets, ..., AllNodes ] enum above
		multiplier   int // optionally: multiple streams per destination (round-robin)
		manualResync bool
	}
	Stats map[string]*transport.Stats // by DaemonID
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
		Extra        *transport.Extra // additional parameters
		Net          string           // one of cmn.KnownNetworks, empty defaults to cmn.NetIntraData
		Trname       string           // transport endpoint name
		Ntype        int              // cluster.Target (0) by default
		Multiplier   int              // so-many TCP connections per Rx endpoint, with round-robin
		ManualResync bool             // auto-resync by default
	}
)

// interface guard
var _ meta.Slistener = (*Streams)(nil)

var verbose bool

func init() {
	verbose = bool(glog.FastV(4, glog.SmoduleTransport))
}

//
// public
//

func (sb *Streams) UsePDU() bool   { return sb.extra.UsePDU() }
func (sb *Streams) Trname() string { return sb.trname }

func New(sowner meta.Sowner, lsnode *meta.Snode, cl transport.Client, sbArgs Args) (sb *Streams) {
	if sbArgs.Net == "" {
		sbArgs.Net = cmn.NetIntraData
	}
	listeners := sowner.Listeners()
	sb = &Streams{
		sowner:       sowner,
		smap:         &meta.Smap{}, // empty on purpose (see Resync)
		smaplock:     &sync.Mutex{},
		lsnode:       lsnode,
		client:       cl,
		network:      sbArgs.Net,
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
	if !sb.extra.Compressed() {
		sb.lid = fmt.Sprintf("sb[%s-%s-%s]", sb.lsnode.ID(), sb.network, sb.trname)
	} else {
		sb.lid = fmt.Sprintf("sb[%s-%s-%s[%s]]", sb.lsnode.ID(), sb.network, sb.trname,
			cos.ToSizeIEC(int64(sb.extra.Config.Transport.LZ4BlockMaxSize), 0))
	}

	// update streams when Smap changes
	sb.smaplock.Lock()
	sb.Resync()
	sb.smaplock.Unlock()

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

// when (nodes == nil) transmit via all established streams in a bundle
// otherwise, restrict to the specified subset (nodes)
func (sb *Streams) Send(obj *transport.Obj, roc cos.ReadOpenCloser, nodes ...*meta.Snode) (err error) {
	debug.Assert(!transport.ReservedOpcode(obj.Hdr.Opcode))
	streams := sb.get()
	if len(streams) == 0 {
		err = fmt.Errorf("no streams %s => .../%s", sb.lsnode, sb.trname)
	} else if nodes != nil && len(nodes) == 0 {
		err = fmt.Errorf("no destinations %s => .../%s", sb.lsnode, sb.trname)
	} else if obj.IsUnsized() && sb.extra.SizePDU == 0 {
		err = fmt.Errorf("[%s] sending unsized object supported only with PDUs", obj.Hdr.Cname())
	}

	if err != nil {
		glog.Error(err)
		// compare w/ transport doCmpl()
		_doCmpl(obj, roc, err)
		return
	}
	if obj.Callback == nil {
		obj.Callback = sb.extra.Callback
	}
	if obj.IsHeaderOnly() {
		roc = nil
	}

	if nodes == nil {
		idx, cnt := 0, len(streams)
		obj.SetPrc(cnt)
		// Reader-reopening logic: since the streams in a bundle are mutually independent
		// and asynchronous, reader.Open() (aka reopen) is skipped for the 1st replica
		// that we put on the wire and is done for the 2nd, 3rd, etc. replicas.
		// In other words, for the N object replicas over the N bundled streams, the
		// original reader will get reopened (N-1) times.
		for sid, robin := range streams {
			if sb.lsnode.ID() == sid {
				continue
			}
			if err = sb.sendOne(obj, roc, robin, idx, cnt); err != nil {
				return
			}
			idx++
		}
	} else {
		// first, check streams vs destinations
		for _, di := range nodes {
			if _, ok := streams[di.ID()]; ok {
				continue
			}
			err = cos.NewErrNotFound("destination mismatch: stream (%s) => %s", sb, di)
			_doCmpl(obj, roc, err) // ditto
			return
		}
		// second, do send. Same comment wrt reopening.
		cnt := len(nodes)
		obj.SetPrc(cnt)
		for idx, di := range nodes {
			robin := streams[di.ID()]
			if err = sb.sendOne(obj, roc, robin, idx, cnt); err != nil {
				return
			}
		}
	}
	return
}

func _doCmpl(obj *transport.Obj, roc cos.ReadOpenCloser, err error) {
	if roc != nil {
		cos.Close(roc)
	}
	if obj.Callback != nil {
		obj.Callback(obj.Hdr, roc, obj.CmplArg, err)
	}
}

func (sb *Streams) String() string   { return sb.lid }
func (sb *Streams) Smap() *meta.Smap { return sb.smap }

// keep streams to => (clustered nodes as per rxNodeType) in sync at all times
func (sb *Streams) ListenSmapChanged() {
	smap := sb.sowner.Get()
	if smap.Version <= sb.smap.Version {
		return
	}

	sb.smaplock.Lock()
	sb.Resync()
	sb.smaplock.Unlock()
}

func (sb *Streams) GetStats() Stats {
	streams := sb.get()
	stats := make(Stats, len(streams))
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
func (sb *Streams) sendOne(obj *transport.Obj, roc cos.ReadOpenCloser, robin *robin, idx, cnt int) error {
	obj.Hdr.SID = sb.lsnode.ID()
	one := obj
	one.Reader = roc
	if cnt == 1 {
		goto snd
	}
	one = transport.AllocSend()
	*one = *obj
	if idx > 0 && roc != nil {
		reader, err := roc.Open()
		if err != nil { // reopen for every destination
			err := fmt.Errorf("%s failed to reopen %q reader: %v", sb, obj, err)
			debug.AssertNoErr(err) // must never happen
			return err
		}
		one.Reader = reader
	}
snd:
	i := 0
	if sb.multiplier > 1 {
		i = int(robin.i.Inc()) % len(robin.stsdest)
	}
	s := robin.stsdest[i]
	return s.Send(one)
}

func (sb *Streams) Abort() {
	streams := sb.get()
	for _, robin := range streams {
		for _, s := range robin.stsdest {
			s.Abort()
		}
	}
}

func (sb *Streams) apply(action int) {
	cos.Assert(action == closeFin || action == closeStop)
	var (
		streams = sb.get()
		wg      = &sync.WaitGroup{}
	)
	for _, robin := range streams {
		wg.Add(1)
		go func(stsdest stsdest, wg *sync.WaitGroup) {
			for _, s := range stsdest {
				if !s.IsTerminated() {
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

// Resync streams asynchronously
// is a slowpath; is called under lock; NOTE: calls stream.Stop()
func (sb *Streams) Resync() {
	smap := sb.sowner.Get()
	if smap.Version <= sb.smap.Version {
		debug.Assertf(smap.Version == sb.smap.Version, "%s[%s]: %s vs %s", sb.trname, sb.lid, smap, sb.smap)
		return
	}

	var (
		oldm []meta.NodeMap
		newm []meta.NodeMap
		node = smap.GetNode(sb.lsnode.ID()) // upd flags
	)
	switch sb.rxNodeType {
	case cluster.Targets:
		oldm = []meta.NodeMap{sb.smap.Tmap}
		newm = []meta.NodeMap{smap.Tmap}
	case cluster.Proxies:
		oldm = []meta.NodeMap{sb.smap.Pmap}
		newm = []meta.NodeMap{smap.Pmap}
	case cluster.AllNodes:
		oldm = []meta.NodeMap{sb.smap.Tmap, sb.smap.Pmap}
		newm = []meta.NodeMap{smap.Tmap, smap.Pmap}
	default:
		debug.Assert(false)
	}
	if node == nil {
		// extremely unlikely
		debug.Assert(false, sb.lsnode.ID())
		newm = []meta.NodeMap{make(meta.NodeMap)}
	} else {
		sb.lsnode.Flags = node.Flags
	}

	added, removed := mdiff(oldm, newm)

	obundle := sb.get()
	l := len(added) - len(removed)
	if obundle != nil {
		l = cos.Max(len(obundle), len(obundle)+l)
	}
	nbundle := make(map[string]*robin, l)
	for id, robin := range obundle {
		nbundle[id] = robin
	}
	for id, si := range added {
		if id == sb.lsnode.ID() {
			continue
		}
		// not connecting to the peer that's in maintenance and already rebalanced-out
		if si.InMaintPostReb() {
			glog.Infof("%s => %s[-/%#b] - skipping", sb, si.StringEx(), si.Flags)
			continue
		}

		dstURL := si.URL(sb.network) + transport.ObjURLPath(sb.trname) // direct destination URL
		nrobin := &robin{stsdest: make(stsdest, sb.multiplier)}
		for k := 0; k < sb.multiplier; k++ {
			var (
				s  string
				ns = transport.NewObjStream(sb.client, dstURL, id /*dstID*/, &sb.extra)
			)
			if sb.multiplier > 1 {
				s = fmt.Sprintf("(#%d)", k+1)
			}
			if verbose {
				glog.Infof("%s: %s%s via %s", sb, ns, s, dstURL)
			}
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
			if !os.IsTerminated() {
				os.Stop() // the node is gone but the stream appears to be still active - stop it
			}
			if verbose {
				glog.Infof("%s: [-] %s => %s via %s", sb, os, id, os.URL())
			}
		}
		delete(nbundle, id)
	}
	sb.streams.Store(unsafe.Pointer(&nbundle))
	sb.smap = smap
}

// helper to find out NodeMap "delta" or "diff"
func mdiff(oldMaps, newMaps []meta.NodeMap) (added, removed meta.NodeMap) {
	for i, mold := range oldMaps {
		mnew := newMaps[i]
		for id, si := range mnew {
			if _, ok := mold[id]; !ok {
				if added == nil {
					added = make(meta.NodeMap, cos.Max(len(mnew)-len(mold), 1))
				}
				added[id] = si
			}
		}
	}
	for i, mold := range oldMaps {
		mnew := newMaps[i]
		for id, si := range mold {
			if _, ok := mnew[id]; !ok {
				if removed == nil {
					removed = make(meta.NodeMap, 1)
				}
				removed[id] = si
			}
		}
	}
	return
}
