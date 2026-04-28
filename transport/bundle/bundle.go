// Package bundle provides multi-streaming transport with the functionality
// to dynamically (un)register receive endpoints, establish long-lived flows, and more.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package bundle

import (
	"fmt"
	"maps"
	"strconv"
	"sync"
	ratomic "sync/atomic"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/transport"
)

const (
	closeFin = iota
	closeStop
)

type (
	// multiple streams to the same destination with round-robin selection
	stsdest []*transport.Stream
	robin   struct {
		stsdest stsdest
		i       atomic.Int64
	}
	bundle map[string]*robin // stream "bundle" indexed by node ID
)

// Stream bundles maintain long-lived peer-to-peer streams -
// target-to-target data paths used by AIS xactions (batch jobs).
//
// Target membership policy is owned by each specific xaction. A job
// requires a stable target set iff it sets ConflictRebRes (refuse to
// start during rebalance) or AbortByReb (abort if rebalance starts
// mid-flight) - see xact/api_table.go. Jobs with neither flag tolerate
// membership drift; jobs that don't use stream bundles are unaffected.
//
// Generally, AIS jobs fall into three broad groups:
// - jobs that generate/place new objects and require a stable target set
//   (prefetch, rebalance, copy/transform, create-bucket-inventory);
// - jobs that do not generate objects and can tolerate membership drift
//   (LRU eviction, space cleanup);
// - jobs that create local derived state and may engage joined targets via
//   xaction-level policy rather than bundle-level resync
//   (rechunk, shard-index).
//
// The bundle itself records the Smap snapshot used to establish its streams:
// caller-provided Extra.Smap, if present, or the current Smap otherwise.
// Callers can use DM.Smap() to send control messages to the same peer set/epoch.
//
// ReopenPeerStream repairs connectivity to an existing peer in that epoch; it
// does not add/remove peers.
//
// For the documented mechanism for xactions to express their membership policy
// via static flags (ConflictRebRes, AbortByReb), see xact/api_table.go.

type (
	Streams struct {
		client     transport.Client
		smap       *meta.Smap              // assigned at sb.open() for the lifetime; used to build the bundle
		streams    ratomic.Pointer[bundle] // stream bundle (CoW atomic by ReopenPeerStream)
		trname     string
		network    string
		lid        string
		extra      transport.Extra
		multiplier int // optionally: multiple streams per destination (round-robin a.k.a. `robin`)
	}

	Args struct {
		Extra  *transport.Extra // additional parameters
		Net    string           // one of cmn.KnownNetworks, empty defaults to cmn.NetIntraData
		Trname string           // transport endpoint name
	}

	ErrDestinationMissing struct {
		streamStr string
		tname     string
		smapStr   string
	}
)

//
// public
//

func (sb *Streams) UsePDU() bool   { return sb.extra.UsePDU() }
func (sb *Streams) Trname() string { return sb.trname }

func New(cl transport.Client, args Args) (sb *Streams) {
	if args.Net == "" {
		args.Net = cmn.NetIntraData // default net
	}
	sb = &Streams{
		client:  cl,
		network: args.Net,
		trname:  args.Trname,
	}
	sb.extra = *args.Extra
	sb.multiplier = cos.NonZero(args.Extra.SbundleMult, int(1))
	if sb.extra.Config == nil {
		sb.extra.Config = cmn.GCO.Get()
	}

	// open streams (or stream "robins") to all peer targets
	sb.open()

	sb.lid = sb._lid()
	nlog.Infoln("open", sb.lid)

	return sb
}

func (sb *Streams) String() string { return sb.lid }

// (compare w/ transport._loghdr)
func (sb *Streams) _lid() string {
	var s cos.SB
	s.Init(20 + len(sb.trname))

	s.WriteString(cos.Ternary(sb.extra.Compressed(), "sb(z)-[", "sb-["))
	s.WriteString(core.T.SID())
	if sb.network != cmn.NetIntraData {
		s.WriteUint8('-')
		s.WriteString(sb.network)
	}
	s.WriteString("-v")
	s.WriteString(strconv.FormatInt(sb.smap.Version, 10))
	s.WriteUint8('-')
	s.WriteString(sb.trname)

	s.WriteUint8(']')

	return s.String()
}

// Close closes all contained streams;
// graceful=true blocks until all pending objects get completed (for "completion", see transport/README.md)
func (sb *Streams) Close(gracefully bool) {
	if gracefully {
		nlog.Infoln("close", sb.lid)
		sb.apply(closeFin)
	} else {
		nlog.Infoln("stop", sb.lid)
		sb.apply(closeStop)
	}
}

// when (nodes == nil) transmit via all established streams in a bundle
// otherwise, restrict to the specified subset (nodes)
func (sb *Streams) Send(obj *transport.Obj, roc cos.ReadOpenCloser, nodes ...*meta.Snode) error {
	debug.Assert(!transport.ReservedOpcode(obj.Hdr.Opcode))
	streams := sb.get()

	if err := sb._validate(obj, streams, nodes); err != nil {
		if cmn.Rom.V(5, cos.ModTransport) {
			nlog.Warningln(err)
		}
		// compare w/ transport doCmpl()
		_doCmpl(obj, roc, err)
		return err
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
			if core.T.SID() == sid {
				continue
			}
			if err := sb.sendOne(obj, roc, robin, idx, cnt); err != nil {
				return err
			}
			idx++
		}
	} else {
		// first, check streams vs destinations
		for _, di := range nodes {
			if _, ok := streams[di.ID()]; ok {
				continue
			}
			err := &ErrDestinationMissing{sb.String(), di.StringEx(), sb.smap.String()}
			_doCmpl(obj, roc, err) // ditto
			return err
		}
		// second, do send. Same comment wrt reopening.
		cnt := len(nodes)
		obj.SetPrc(cnt)
		for idx, di := range nodes {
			robin := streams[di.ID()]
			if err := sb.sendOne(obj, roc, robin, idx, cnt); err != nil {
				return err
			}
		}
	}

	return nil
}

func (sb *Streams) _validate(obj *transport.Obj, bun bundle, nodes []*meta.Snode) error {
	switch {
	case len(bun) == 0:
		return fmt.Errorf("no streams %s => .../%s", core.T.Snode(), sb.trname)
	case nodes != nil && len(nodes) == 0:
		return fmt.Errorf("no destinations %s => .../%s", core.T.Snode(), sb.trname)
	case obj.IsUnsized() && sb.extra.SizePDU == 0:
		return fmt.Errorf("[%s] sending unsized object supported only with PDUs", obj.Hdr.Cname())
	}
	return nil
}

func _doCmpl(obj *transport.Obj, roc cos.ReadOpenCloser, err error) {
	if roc != nil {
		cos.Close(roc)
	}
	if obj.SentCB != nil {
		obj.SentCB(&obj.Hdr, roc, obj.CmplArg, err)
	}
}

func (sb *Streams) Smap() *meta.Smap { return sb.smap } // TODO -- FIXME: start using

//
// private methods
//

func (sb *Streams) get() (bun bundle) {
	optr := sb.streams.Load()
	if optr != nil {
		bun = *optr
	}
	return
}

// one obj, one stream
func (sb *Streams) sendOne(obj *transport.Obj, roc cos.ReadOpenCloser, robin *robin, idx, cnt int) error {
	obj.Hdr.SID = core.T.SID()
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
	debug.Assert(action == closeFin || action == closeStop)
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

// Open streams to all target peers from the given Smap.
func (sb *Streams) open() {
	smap := sb.extra.Smap
	if smap == nil {
		smap = core.T.Sowner().Get()
	}
	// drop the snapshot reference; bundle membership is fixed post-construction
	sb.extra.Smap = nil

	node := smap.GetNode(core.T.SID())
	if node == nil {
		debug.Assert(false, core.T.SID())
		// keep the post-open invariant: sb.streams is non-nil
		sb.streams.Store(&bundle{})
		sb.smap = smap
		return
	}
	core.T.Snode().Flags = node.Flags

	// Xactions that use intra-cluster transport may increase burstiness for their own
	// data movers by setting their XactConf.Burst (work channel cap). Stream bundles
	// then derive a per-destination stream burst from XactConf.Burst and the
	// number of active peers.
	if sb.extra.XactBurst > 0 {
		if numPeers := smap.CountActiveTs() - 1; numPeers > 0 {
			xburst := cos.DivRound(sb.extra.XactBurst, numPeers)
			sb.extra.Burst = cos.ClampInt(
				xburst,
				max(sb.extra.Config.Transport.Burst, cmn.TransportBurstMin),
				cmn.TransportBurstMax,
			)
		}
	}

	nbundle := make(bundle, smap.CountActiveTs())
	sb._open(nbundle, smap.Tmap, smap)

	sb.streams.Store(&nbundle)
	sb.smap = smap
}

func (sb *Streams) _open(nbundle bundle, nm meta.NodeMap, smap *meta.Smap) {
	for id, si := range nm {
		if id == core.T.SID() {
			continue
		}
		// not connecting to the peer that's in maintenance _and_ already rebalanced-out
		if si.InMaintPostReb() {
			nlog.Infof("%s => %s[-/%s] per %s - skipping", sb, si.StringEx(), si.Fl2S(), smap)
			continue
		}

		dstURL := si.URL(sb.network) + transport.ObjURLPath(sb.trname)
		nrobin := &robin{stsdest: make(stsdest, sb.multiplier)}
		for k := range sb.multiplier {
			nrobin.stsdest[k] = transport.NewObjStream(sb.client, dstURL, id /*dstID*/, &sb.extra)
		}
		nbundle[id] = nrobin
	}
}

// renew stream (or streams) to a given peer in the same Smap "epoch"
func (sb *Streams) ReopenPeerStream(dstID string) error {
	// 1) validate
	old := sb.get()
	orobin, ok := old[dstID]
	if !ok {
		return &ErrDestinationMissing{sb.String(), dstID, sb.smap.String()}
	}
	if len(orobin.stsdest) == 0 {
		debug.Assert(false) // not expecting
		return nil
	}
	smap := core.T.Sowner().Get()
	if smap.Version != sb.smap.Version {
		// to err on the side of caution
		return fmt.Errorf("%s: reopening individual streams when cluster map changes is not supported yet (%s vs %s)",
			sb, smap.StringEx(), sb.smap.StringEx())
	}
	si := sb.smap.GetNode(dstID)
	if si == nil {
		// (unlikely - checked above)
		return cos.NewErrNotFoundFmt(sb, "destination %q (%s)", dstID, sb.smap.StringEx())
	}
	dstURL := si.URL(sb.network) + transport.ObjURLPath(sb.trname)

	// 2) build new `robin` (same multiplier; consider setting nrobin.i)
	nrobin := &robin{stsdest: make(stsdest, len(orobin.stsdest))}
	config := cmn.GCO.Get()
	for k := range nrobin.stsdest {
		extra := sb.extra // by value
		extra.Config = config
		ns := transport.NewObjStream(sb.client, dstURL, dstID, &extra)
		nrobin.stsdest[k] = ns
	}
	nbundle := maps.Clone(old)
	if nbundle == nil {
		nbundle = make(bundle)
	}
	nbundle[dstID] = nrobin

	// 3) switch over
	sb.streams.Store(&nbundle)

	// 4) stop old streams async
	for _, os := range orobin.stsdest {
		if !os.IsTerminated() {
			os.Stop() // via stopCh
		}
	}

	nlog.Infoln(sb.String(), "successfully restablished connectivity to", dstID)
	return nil
}

///////////////////////////
// ErrDestinationMissing //
///////////////////////////

func (e *ErrDestinationMissing) Error() string {
	return fmt.Sprintf("destination missing: stream (%s) => %s, %s", e.streamStr, e.tname, e.smapStr)
}

func IsErrDestinationMissing(e error) bool {
	_, ok := e.(*ErrDestinationMissing)
	return ok
}
