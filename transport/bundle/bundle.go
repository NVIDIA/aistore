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
// Each xaction owns its membership and recovery policy. ConflictRebRes and
// AbortByReb (see xact/api_table.go) govern coexistence with rebalance/resilver;
// they do not fully describe tolerance of peer restarts or membership changes.
// Transport repair restores connectivity; the xaction decides whether to
// continue, recover missing work, or terminate.
//
// The bundle itself records the Smap snapshot used to establish its streams:
// caller-provided Extra.Smap, if present, or the current Smap otherwise.
// Callers can use DM.Smap() to send control messages to the same peer set/epoch.
//
// ReopenPeerStream replaces streams to an existing peer without adding/removing
// peers. The caller validates the snapshot (see meta.Smap.CompareTargets).

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
		reopenMu   sync.Mutex
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
// otherwise, restrict to the specified subset (nodes);
// rules:
// - validation and reader-reopen failures complete locally, before SetCmpl
// - once the refcount is initialized, attempt every destination even after an error
// - transport completes every attempt and invokes the shared callback exactly once
// - return the first immediate send error.
// - transport consumes `obj` and `roc`, including on validation and reader-reopen failures.
func (sb *Streams) Send(obj *transport.Obj, roc cos.ReadOpenCloser, nodes ...*meta.Snode) error {
	debug.AssertFunc(func() bool { return !transport.ReservedOpcode(obj.Hdr.Opcode) })
	streams := sb.get()

	if err := sb._validate(obj, streams, nodes); err != nil {
		if cmn.Rom.V(5, cos.ModTransport) {
			nlog.Warningln(err)
		}
		// compare w/ transport doCmpl()
		sb._doCmpl(obj, roc, err)
		return err
	}

	if obj.IsHeaderOnly() {
		if roc != nil {
			cos.Close(roc)
			roc = nil
		}
	}

	// 1) count destinations
	var cnt int
	if nodes == nil {
		cnt = len(streams)
		debug.Func(func() {
			_, ok := streams[core.T.SID()]
			debug.Assert(!ok, sb.String(), ": self in bundle") // (see _open)
		})
	} else {
		// check streams vs destinations
		for _, di := range nodes {
			if _, ok := streams[di.ID()]; ok {
				continue
			}
			err := &ErrDestinationMissing{sb.String(), di.StringEx(), sb.smap.String()}
			sb._doCmpl(obj, roc, err) // ditto
			return err
		}
		cnt = len(nodes)
	}

	// 2) one reader per destination
	// Reopen (cnt-1) readers before SetCmpl and before transferring ownership to
	// asynchronous streams. On failure, complete locally without a hanging refcount.
	readers, err := _reopen(roc, cnt)
	if err != nil {
		err = fmt.Errorf("%s failed to reopen %q reader: %w", sb, obj, err)
		sb._doCmpl(obj, roc, err)
		return err
	}

	// 3) send
	if cnt > 1 {
		obj.SetCmpl(cnt)
	}
	if nodes == nil {
		idx := 0
		for _, robin := range streams {
			if e := sb.sendOne(obj, _reader(readers, roc, idx), robin, idx, cnt); e != nil && err == nil {
				err = e
			}
			idx++
		}
	} else {
		for idx, di := range nodes {
			robin := streams[di.ID()]
			if e := sb.sendOne(obj, _reader(readers, roc, idx), robin, idx, cnt); e != nil && err == nil {
				err = e
			}
		}
	}

	return err
}

// Open (cnt-1) additional readers, one per destination except the first.
// On error, close all successfully reopened readers; the caller closes the original.
func _reopen(roc cos.ReadOpenCloser, cnt int) ([]cos.ReadOpenCloser, error) {
	if roc == nil || cnt < 2 {
		return nil, nil
	}
	readers := make([]cos.ReadOpenCloser, cnt)
	readers[0] = roc
	for i := 1; i < cnt; i++ {
		reader, err := roc.Open()
		if err != nil {
			if reader != nil {
				debug.Assert(false)
				cos.Close(reader)
			}
			for j := 1; j < i; j++ {
				cos.Close(readers[j])
			}
			return nil, err
		}
		readers[i] = reader
	}
	return readers, nil
}

func _reader(readers []cos.ReadOpenCloser, roc cos.ReadOpenCloser, idx int) cos.ReadOpenCloser {
	if readers == nil {
		return roc // header-only (nil), or a single destination
	}
	return readers[idx]
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

// compare with transport.Stream.doCmpl
// (the object's callback, when defined, overrides the parent callback from transport.Extra)
func (sb *Streams) _doCmpl(obj *transport.Obj, roc cos.ReadOpenCloser, err error) {
	if roc != nil {
		cos.Close(roc)
	}
	switch {
	case obj.SentCB != nil:
		obj.SentCB(&obj.Hdr, roc, obj.CmplArg, err)
	case sb.extra.Parent != nil && sb.extra.Parent.SentCB != nil:
		sb.extra.Parent.SentCB(&obj.Hdr, roc, obj.CmplArg, err)
	}
}

func (sb *Streams) Smap() *meta.Smap { return sb.smap }

// ReopenPeerStream replaces streams to an existing destination using the bundle's
// recorded endpoint. Caller validates the snapshot against current Smap.
// Does not replay interrupted sends or restore remote request state.
func (sb *Streams) ReopenPeerStream(dstID string) error {
	sb.reopenMu.Lock()
	defer sb.reopenMu.Unlock()

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

	nlog.Infoln(sb.String(), "successfully re-established connectivity to", dstID)
	return nil
}

//
// private methods
//

func (sb *Streams) usePDU() bool { return sb.extra.UsePDU() }

func (sb *Streams) get() (bun bundle) {
	optr := sb.streams.Load()
	if optr != nil {
		bun = *optr
	}
	return
}

// one obj, one stream
func (sb *Streams) sendOne(obj *transport.Obj, reader cos.ReadOpenCloser, robin *robin, idx, cnt int) error {
	obj.Hdr.SID = core.T.SID()
	one := obj
	// Keep obj as the stable template until the final send. Earlier destinations
	// get clones; the final destination takes ownership of obj itself.
	if idx < cnt-1 {
		one = transport.AllocSend()
		*one = *obj
	}
	one.Reader = reader

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
		debug.Func(func() { debug.Assert(false, core.T.SID()) })
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
			debug.Assert(sb.extra.Config.Transport.Burst >= cmn.TransportBurstMin)
			sb.extra.Burst = cos.ClampInt(
				xburst,
				sb.extra.Config.Transport.Burst,
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
