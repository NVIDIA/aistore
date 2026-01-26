// Package bundle provides multi-streaming transport with the functionality
// to dynamically (un)register receive endpoints, establish long-lived flows, and more.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package bundle

import (
	"errors"
	"io"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/transport"
)

// Data Mover (DM): a _bundle_ of streams (this target => other target) with atomic reg, open, and broadcast help

type (
	bp struct {
		client  transport.Client
		recv    transport.RecvObj
		streams *Streams
		trname  string
		net     string // one of cmn.KnownNetworks, empty defaults to cmn.NetIntraData
	}
	// additional (and optional) params for new data mover instance
	Extra struct {
		// generic knobs { multiplier, burst, ... } from the parent xaction
		cmn.XactConf
		// extra
		RecvAck    transport.RecvObj
		Config     *cmn.Config
		SizePDU    int32
		MaxHdrSize int32
	}
	// data mover is an easy-to-use stream bundle
	DM struct {
		parent *transport.Parent // optional: (parent xaction, term-cb, sent-cb)
		data   bp                // data
		ack    bp                // ACKs and control
		Extra                    // (embed)
		owt    cmn.OWT
		stage  struct {
			regmtx sync.Mutex
			regged atomic.Bool
			opened atomic.Bool
			laterx atomic.Bool
			unregg atomic.Bool // in UnregRecv
		}
	}
)

// In re `owt` (below): data mover passes it to the target's `PutObject`
// to properly finalize received payload.
// For DMs that do not create new objects (e.g, rebalance) `owt` should
// be set to `OwtMigrateRepl`; all others are expected to have `OwtPut` (see e.g, CopyBucket).

func NewDM(trname string, recvCB transport.RecvObj, owt cmn.OWT, extra Extra) *DM {
	dm := &DM{}
	dm.init(trname, recvCB, owt, extra)
	return dm
}

func (dm *DM) init(trname string, recvCB transport.RecvObj, owt cmn.OWT, extra Extra) {
	debug.Assert(extra.Config != nil)
	dm.owt = owt
	dm.Extra = extra
	if dm.Compression == "" {
		dm.Compression = apc.CompressNever
	}

	dm.data.trname, dm.data.recv = trname, recvCB
	if dm.data.net == "" {
		dm.data.net = cmn.NetIntraData
	}
	dm.data.client = transport.NewIntraDataClient()
	// ack
	if dm.ack.net == "" {
		dm.ack.net = cmn.NetIntraControl
	}
	dm.ack.recv = extra.RecvAck
	if !dm.useACKs() {
		return
	}
	dm.ack.trname = "ack." + trname
	dm.ack.client = transport.NewIntraDataClient()
}

func (dm *DM) useACKs() bool { return dm.ack.recv != nil }
func (dm *DM) NetD() string  { return dm.data.net }
func (dm *DM) NetC() string  { return dm.ack.net }
func (dm *DM) OWT() cmn.OWT  { return dm.owt }

// xaction that drives and utilizes this data mover
func (dm *DM) SetXact(xctn core.Xact) {
	if dm.parent == nil {
		dm.parent = &transport.Parent{}
	}
	dm.parent.Xact = xctn
}
func (dm *DM) xctn() core.Xact {
	if dm.parent == nil {
		return nil
	}
	return dm.parent.Xact
}

// when config changes
func (dm *DM) Renew(trname string, recvCB transport.RecvObj, owt cmn.OWT, extra Extra) *DM {
	debug.Assert(owt == dm.owt)

	// always refresh
	dm.Config = extra.Config

	if extra.Compression == "" {
		extra.Compression = apc.CompressNever
	}
	if dm.XactConf == extra.XactConf && dm.SizePDU == extra.SizePDU && dm.MaxHdrSize == extra.MaxHdrSize {
		return nil
	}

	nlog.Infoln("renew DM", dm.String(), "=> [", extra.XactConf, "]")
	return NewDM(trname, recvCB, owt, extra)
}

// register user's receive-data (and, optionally, receive-ack) wrappers
func (dm *DM) RegRecv() error {
	dm.stage.regmtx.Lock()
	defer dm.stage.regmtx.Unlock()

	if dm.stage.regged.Load() {
		return errors.New("duplicated reg: " + dm.String())
	}
	if err := transport.Handle(dm.data.trname, dm.wrapRecvData); err != nil {
		debug.Assert(err == nil, dm.String(), ": ", err)
		return err
	}
	if dm.useACKs() {
		if err := transport.Handle(dm.ack.trname, dm.wrapRecvACK); err != nil {
			if nerr := transport.Unhandle(dm.data.trname); nerr != nil {
				nlog.Errorln("FATAL:", err, "[ nested:", nerr, dm.String(), "]")
				debug.AssertNoErr(nerr)
			}
			return err
		}
	}

	dm.stage.regged.Store(true)
	return nil
}

func (dm *DM) UnregRecv() {
	if dm == nil {
		return
	}

	dm.stage.regmtx.Lock()
	if !dm.stage.regged.Load() {
		dm.stage.regmtx.Unlock()
		nlog.WarningDepth(1, "duplicated unreg:", dm.String())
		return
	}

	// only one UnregRecv() goes through
	if !dm.stage.unregg.CAS(false, true) {
		dm.stage.regmtx.Unlock()
		nlog.WarningDepth(1, "duplicated unreg:", dm.String())
		return
	}

	xctn := dm.xctn()
	if xctn != nil {
		// quiesce outside locks
		dm.stage.regmtx.Unlock()
		timeout := dm.Config.Transport.QuiesceTime.D()
		if xctn.IsAborted() {
			timeout = time.Second
		}
		dm.quiesce(timeout)
		dm.stage.regmtx.Lock()
	}
	defer dm.stage.regmtx.Unlock()

	if err := transport.Unhandle(dm.data.trname); err != nil {
		nlog.ErrorDepth(1, err, "[", dm.data.trname, dm.String(), "]")
	}
	if dm.useACKs() {
		if err := transport.Unhandle(dm.ack.trname); err != nil {
			nlog.ErrorDepth(1, err, "[", dm.ack.trname, dm.String(), "]")
		}
	}

	dm.stage.regged.Store(false)
	dm.stage.unregg.Store(false)
}

func (dm *DM) IsFree() bool {
	return !dm.stage.regged.Load() && !dm.stage.unregg.Load()
}

func (dm *DM) Open() {
	// data
	extra := &transport.Extra{
		Config:      dm.Config,
		Compression: dm.XactConf.Compression,
		XactBurst:   dm.XactConf.Burst,
		SbundleMult: dm.XactConf.SbundleMult,
		SizePDU:     dm.SizePDU,
		MaxHdrSize:  dm.MaxHdrSize,
	}
	dataArgs := Args{
		Net:          dm.data.net,
		Trname:       dm.data.trname,
		Extra:        extra,
		ManualResync: true,
	}
	dataArgs.Extra.Parent = dm.parent
	dm.data.streams = New(dm.data.client, dataArgs)

	// acks back (optional)
	if dm.useACKs() {
		ackArgs := Args{
			Net:          dm.ack.net,
			Trname:       dm.ack.trname,
			Extra:        &transport.Extra{Config: dm.Config},
			ManualResync: true,
		}
		ackArgs.Extra.Parent = dm.parent
		dm.ack.streams = New(dm.ack.client, ackArgs)
	}

	dm.stage.opened.Store(true)
	nlog.Infoln(dm.String(), "is open")
}

func (dm *DM) String() string {
	s := "pre-or-post-"
	switch {
	case dm.stage.opened.Load():
		s = "open-"
	case dm.stage.regged.Load():
		s = "reg-" // reg-ed handlers, not open yet tho
	}
	if dm.data.streams == nil {
		return "dm-" + s + "no-streams"
	}
	if dm.data.streams.UsePDU() {
		return "dm-pdu-" + s + dm.data.streams.Trname()
	}
	return "dm-" + s + dm.data.streams.Trname()
}

// quiesce *local* Rx
func (dm *DM) quiesce(d time.Duration) core.QuiRes {
	return dm.xctn().Quiesce(d, dm.quicb)
}

func (dm *DM) Close(err error) {
	if dm == nil {
		if cmn.Rom.V(5, cos.ModTransport) {
			nlog.Warningln("Warning: DM is <nil>") // e.g., single-node cluster
		}
		return
	}
	if !dm.stage.opened.CAS(true, false) {
		nlog.Errorln("Warning:", dm.String(), "not open")
		return
	}
	if err == nil {
		if xctn := dm.xctn(); xctn != nil && xctn.IsAborted() {
			err = xctn.AbortErr()
		}
	}
	// nil: close gracefully via `fin`, otherwise abort
	dm.data.streams.Close(err == nil)
	if dm.useACKs() {
		dm.ack.streams.Close(err == nil)
	}
	nlog.Infoln(dm.String(), err)
}

func (dm *DM) Abort() {
	dm.data.streams.Abort()
	if dm.useACKs() {
		dm.ack.streams.Abort()
	}
	dm.stage.opened.Store(false)
	nlog.Warningln("dm.abort", dm.String())
}

func (dm *DM) Send(obj *transport.Obj, roc cos.ReadOpenCloser, tsi *meta.Snode, xctns ...core.Xact) (err error) {
	err = dm.data.streams.Send(obj, roc, tsi)
	if err == nil && !transport.ReservedOpcode(obj.Hdr.Opcode) {
		xctn := dm.xctn()
		if len(xctns) > 0 {
			xctn = xctns[0]
		}
		if xctn != nil {
			xctn.OutObjsAdd(1, obj.Size())
		}
	}
	return
}

func (dm *DM) ACK(hdr *transport.ObjHdr, cb transport.SentCB, tsi *meta.Snode) error {
	return dm.ack.streams.Send(&transport.Obj{Hdr: *hdr, SentCB: cb}, nil, tsi)
}

func (dm *DM) Notif(hdr *transport.ObjHdr) error {
	return dm.ack.streams.Send(&transport.Obj{Hdr: *hdr}, nil)
}

func (dm *DM) Bcast(obj *transport.Obj, roc cos.ReadOpenCloser) error {
	return dm.data.streams.Send(obj, roc)
}

//
// private
//

func (dm *DM) quicb(time.Duration /*total*/) core.QuiRes {
	xctn := dm.xctn()
	switch {
	case xctn != nil && xctn.IsAborted():
		return core.QuiInactiveCB
	case dm.stage.laterx.CAS(true, false):
		return core.QuiActive
	default:
		return core.QuiInactiveCB
	}
}

func (dm *DM) wrapRecvData(hdr *transport.ObjHdr, reader io.Reader, err error) error {
	// NOTE: SDM special case
	if dm.data.trname == SDM.trname() {
		return dm.data.recv(hdr, reader, err)
	}

	if hdr.Bck.Name != "" && hdr.ObjName != "" && hdr.ObjAttrs.Size >= 0 {
		if xctn := dm.xctn(); xctn != nil {
			xctn.InObjsAdd(1, hdr.ObjAttrs.Size)
		}
	}
	// NOTE: in re (hdr.ObjAttrs.Size < 0) see transport.UsePDU()

	dm.stage.laterx.Store(true)
	return dm.data.recv(hdr, reader, err)
}

func (dm *DM) wrapRecvACK(hdr *transport.ObjHdr, reader io.Reader, err error) error {
	dm.stage.laterx.Store(true)
	return dm.ack.recv(hdr, reader, err)
}
