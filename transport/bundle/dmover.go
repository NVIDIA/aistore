// Package bundle provides multi-streaming transport with the functionality
// to dynamically (un)register receive endpoints, establish long-lived flows, and more.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package bundle

import (
	"errors"
	"io"
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

type (
	DataMover struct {
		data struct {
			client  transport.Client
			recv    transport.RecvObj
			streams *Streams
			trname  string
			net     string // one of cmn.KnownNetworks, empty defaults to cmn.NetIntraData
		}
		ack struct {
			client  transport.Client
			recv    transport.RecvObj
			streams *Streams
			trname  string
			net     string // one of cmn.KnownNetworks, empty defaults to cmn.NetIntraControl
		}
		xctn        core.Xact
		config      *cmn.Config
		compression string // enum { apc.CompressNever, ... }
		multiplier  int
		owt         cmn.OWT
		stage       struct {
			regred atomic.Bool
			regout atomic.Bool
			opened atomic.Bool
			laterx atomic.Bool
		}
		sizePDU    int32
		maxHdrSize int32
	}
	// additional (and optional) params for new data mover
	Extra struct {
		RecvAck     transport.RecvObj
		Config      *cmn.Config
		Compression string
		Multiplier  int
		SizePDU     int32
		MaxHdrSize  int32
	}
)

// In re `owt` (below): data mover passes it to the target's `PutObject`
// to properly finalize received payload.
// For DMs that do not create new objects (e.g, rebalance) `owt` should
// be set to `OwtMigrateRepl`; all others are expected to have `OwtPut` (see e.g, CopyBucket).

func NewDM(trname string, recvCB transport.RecvObj, owt cmn.OWT, extra Extra) *DataMover {
	debug.Assert(extra.Config != nil)
	dm := &DataMover{config: extra.Config}
	dm.owt = owt
	dm.multiplier = extra.Multiplier
	dm.sizePDU, dm.maxHdrSize = extra.SizePDU, extra.MaxHdrSize
	dm.stage.regout.Store(true)

	if extra.Compression == "" {
		extra.Compression = apc.CompressNever
	}
	dm.compression = extra.Compression

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
		return dm
	}
	dm.ack.trname = "ack." + trname
	dm.ack.client = transport.NewIntraDataClient()
	return dm
}

func (dm *DataMover) useACKs() bool { return dm.ack.recv != nil }
func (dm *DataMover) NetD() string  { return dm.data.net }
func (dm *DataMover) NetC() string  { return dm.ack.net }
func (dm *DataMover) OWT() cmn.OWT  { return dm.owt }

// xaction that drives and utilizes this data mover
func (dm *DataMover) SetXact(xctn core.Xact) { dm.xctn = xctn }
func (dm *DataMover) GetXact() core.Xact     { return dm.xctn }

// when config changes
func (dm *DataMover) Renew(trname string, recvCB transport.RecvObj, owt cmn.OWT, extra Extra) *DataMover {
	dm.config = extra.Config // always refresh
	if extra.Compression == "" {
		extra.Compression = apc.CompressNever
	}
	debug.Assert(owt == dm.owt)
	if dm.multiplier == extra.Multiplier && dm.compression == extra.Compression && dm.sizePDU == extra.SizePDU && dm.maxHdrSize == extra.MaxHdrSize {
		return nil
	}
	nlog.Infoln("renew DM", dm.String(), "=> [", extra.Compression, extra.Multiplier, "]")
	return NewDM(trname, recvCB, owt, extra)
}

// register user's receive-data (and, optionally, receive-ack) wrappers
func (dm *DataMover) RegRecv() error {
	if !dm.stage.regred.CAS(false, true) {
		return errors.New(dm.String() + ": duplicated or early reg-recv")
	}
	if err := transport.Handle(dm.data.trname, dm.wrapRecvData); err != nil {
		nlog.Errorln(err, "[", dm.String(), "]")
		dm.stage.regred.Store(false)
		return err
	}
	if dm.useACKs() {
		if err := transport.Handle(dm.ack.trname, dm.wrapRecvACK); err != nil {
			if nerr := transport.Unhandle(dm.data.trname); nerr != nil {
				nlog.Errorln("FATAL:", err, "[ nested:", nerr, dm.String(), "]")
				debug.AssertNoErr(nerr)
			}
			dm.stage.regred.Store(false)
			return err
		}
	}

	dm.stage.regout.Store(false)
	return nil
}

func (dm *DataMover) UnregRecv() {
	if dm == nil {
		return
	}
	defer dm.stage.regout.Store(true)
	if !dm.stage.regred.CAS(true, false) {
		return // e.g., 2PC (begin => abort) sequence with no Open
	}
	if dm.xctn != nil {
		timeout := dm.config.Transport.QuiesceTime.D()
		if dm.xctn.IsAborted() {
			timeout = time.Second
		}
		dm.Quiesce(timeout)
	}
	if err := transport.Unhandle(dm.data.trname); err != nil {
		nlog.Errorln("FATAL:", err, "[", dm.String(), "]")
	}
	if dm.useACKs() {
		if err := transport.Unhandle(dm.ack.trname); err != nil {
			nlog.Errorln("FATAL:", err, "[", dm.String(), "]")
		}
	}
}

func (dm *DataMover) IsFree() bool {
	return !dm.stage.regred.Load() && dm.stage.regout.Load()
}

func (dm *DataMover) Open() {
	dataArgs := Args{
		Net:    dm.data.net,
		Trname: dm.data.trname,
		Extra: &transport.Extra{
			Compression: dm.compression,
			Config:      dm.config,
			SizePDU:     dm.sizePDU,
			MaxHdrSize:  dm.maxHdrSize,
		},
		Ntype:        core.Targets,
		Multiplier:   dm.multiplier,
		ManualResync: true,
	}
	dataArgs.Extra.Xact = dm.xctn
	dm.data.streams = New(dm.data.client, dataArgs)
	if dm.useACKs() {
		ackArgs := Args{
			Net:          dm.ack.net,
			Trname:       dm.ack.trname,
			Extra:        &transport.Extra{Config: dm.config},
			Ntype:        core.Targets,
			ManualResync: true,
		}
		ackArgs.Extra.Xact = dm.xctn
		dm.ack.streams = New(dm.ack.client, ackArgs)
	}

	dm.stage.opened.Store(true)
	nlog.Infoln(dm.String(), "is open")
}

func (dm *DataMover) String() string {
	s := "pre-or-post-"
	switch {
	case dm.stage.opened.Load():
		s = "open-"
	case dm.stage.regred.Load():
		s = "reg-" // not open yet or closed but not unreg-ed yet
	}
	if dm.data.streams == nil {
		return "dm-nil-" + s
	}
	if dm.data.streams.UsePDU() {
		return "dm-pdu-" + s + dm.data.streams.Trname()
	}
	return "dm-" + s + dm.data.streams.Trname()
}

// quiesce *local* Rx
func (dm *DataMover) Quiesce(d time.Duration) core.QuiRes {
	return dm.xctn.Quiesce(d, dm.quicb)
}

func (dm *DataMover) Close(err error) {
	if dm == nil {
		if cmn.Rom.FastV(5, cos.SmoduleTransport) {
			nlog.Warningln("Warning: DM is <nil>") // e.g., single-node cluster
		}
		return
	}
	if !dm.stage.opened.CAS(true, false) {
		nlog.Errorln("Warning:", dm.String(), "not open")
		return
	}
	if err == nil && dm.xctn != nil && dm.xctn.IsAborted() {
		err = dm.xctn.AbortErr()
	}
	// nil: close gracefully via `fin`, otherwise abort
	if err == nil {
		dm.data.streams.Close(true)
		if dm.useACKs() {
			dm.ack.streams.Close(true)
		}
		nlog.Infoln(dm.String(), "closed")
	} else {
		dm.data.streams.Close(false)
		if dm.useACKs() {
			dm.ack.streams.Close(false)
		}
		nlog.Infoln(dm.String(), "aborted:", err)
	}
}

func (dm *DataMover) Abort() {
	dm.data.streams.Abort()
	if dm.useACKs() {
		dm.ack.streams.Abort()
	}
}

func (dm *DataMover) Send(obj *transport.Obj, roc cos.ReadOpenCloser, tsi *meta.Snode) (err error) {
	err = dm.data.streams.Send(obj, roc, tsi)
	if err == nil && !transport.ReservedOpcode(obj.Hdr.Opcode) {
		dm.xctn.OutObjsAdd(1, obj.Size())
	}
	return
}

func (dm *DataMover) ACK(hdr *transport.ObjHdr, cb transport.ObjSentCB, tsi *meta.Snode) error {
	return dm.ack.streams.Send(&transport.Obj{Hdr: *hdr, Callback: cb}, nil, tsi)
}

func (dm *DataMover) Bcast(obj *transport.Obj, roc cos.ReadOpenCloser) error {
	return dm.data.streams.Send(obj, roc)
}

//
// private
//

func (dm *DataMover) quicb(time.Duration /*total*/) core.QuiRes {
	switch {
	case dm.xctn != nil && dm.xctn.IsAborted():
		return core.QuiInactiveCB
	case dm.stage.laterx.CAS(true, false):
		return core.QuiActive
	default:
		return core.QuiInactiveCB
	}
}

func (dm *DataMover) wrapRecvData(hdr *transport.ObjHdr, reader io.Reader, err error) error {
	if hdr.Bck.Name != "" && hdr.ObjName != "" && hdr.ObjAttrs.Size >= 0 {
		dm.xctn.InObjsAdd(1, hdr.ObjAttrs.Size)
	}
	// NOTE: in re (hdr.ObjAttrs.Size < 0) see transport.UsePDU()

	dm.stage.laterx.Store(true)
	return dm.data.recv(hdr, reader, err)
}

func (dm *DataMover) wrapRecvACK(hdr *transport.ObjHdr, reader io.Reader, err error) error {
	dm.stage.laterx.Store(true)
	return dm.ack.recv(hdr, reader, err)
}
