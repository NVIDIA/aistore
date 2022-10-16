// Package bundle provides multi-streaming transport with the functionality
// to dynamically (un)register receive endpoints, establish long-lived flows, and more.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package bundle

import (
	"fmt"
	"io"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
)

type (
	DataMover struct {
		t    cluster.Target
		data struct {
			trname  string
			recv    transport.RecvObj
			net     string // one of cmn.KnownNetworks, empty defaults to cmn.NetIntraData
			streams *Streams
			client  transport.Client
		}
		ack struct {
			trname  string
			recv    transport.RecvObj
			net     string // one of cmn.KnownNetworks, empty defaults to cmn.NetIntraControl
			streams *Streams
			client  transport.Client
		}
		stage struct {
			regred atomic.Bool
			opened atomic.Bool
			laterx atomic.Bool
		}
		config      *cmn.Config
		mem         *memsys.MMSA
		compression string // enum { apc.CompressNever, ... }
		xctn        cluster.Xact
		multiplier  int
		owt         cmn.OWT
		sizePDU     int32
	}
	// additional (and optional) params for new data mover
	Extra struct {
		RecvAck     transport.RecvObj
		Compression string
		Multiplier  int
		SizePDU     int32
	}
)

// interface guard
var _ cluster.DataMover = (*DataMover)(nil)

// owt is mandatory DM property: a data mover passes the property to
// `target.PutObject` to make to finalize an object properly after the object
// is saved to local drives(e.g, PUT the object to the Cloud as well).
// For DMs that do not create new objects(e.g, rebalance), owt should
// be set to `OwtMigrate`; all others are expected to have `OwtPut` (see e.g, CopyBucket).
func NewDataMover(t cluster.Target, trname string, recvCB transport.RecvObj, owt cmn.OWT, extra Extra) (*DataMover, error) {
	dm := &DataMover{t: t, config: cmn.GCO.Get(), mem: t.PageMM()}
	dm.owt = owt
	dm.multiplier = extra.Multiplier
	dm.sizePDU = extra.SizePDU
	switch extra.Compression {
	case "":
		dm.compression = apc.CompressNever
	case apc.CompressAlways, apc.CompressNever:
		dm.compression = extra.Compression
	default:
		return nil, fmt.Errorf("invalid compression %q", extra.Compression)
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
		return dm, nil
	}
	dm.ack.trname = "ack." + trname
	dm.ack.client = transport.NewIntraDataClient()
	return dm, nil
}

func (dm *DataMover) useACKs() bool { return dm.ack.recv != nil }
func (dm *DataMover) NetD() string  { return dm.data.net }
func (dm *DataMover) NetC() string  { return dm.ack.net }
func (dm *DataMover) OWT() cmn.OWT  { return dm.owt }

// xaction that drives and utilizes this data mover
func (dm *DataMover) SetXact(xctn cluster.Xact) { dm.xctn = xctn }
func (dm *DataMover) GetXact() cluster.Xact     { return dm.xctn }

// register user's receive-data (and, optionally, receive-ack) wrappers
func (dm *DataMover) RegRecv() (err error) {
	if err = transport.HandleObjStream(dm.data.trname, dm.wrapRecvData); err != nil {
		return
	}
	if dm.useACKs() {
		err = transport.HandleObjStream(dm.ack.trname, dm.wrapRecvACK)
	}
	dm.stage.regred.Store(true)
	return
}

func (dm *DataMover) Open() {
	dataArgs := Args{
		Net:    dm.data.net,
		Trname: dm.data.trname,
		Extra: &transport.Extra{
			Compression: dm.compression,
			Config:      dm.config,
			MMSA:        dm.mem,
			SizePDU:     dm.sizePDU,
		},
		Ntype:        cluster.Targets,
		Multiplier:   dm.multiplier,
		ManualResync: true,
	}
	if dm.xctn != nil {
		dataArgs.Extra.SenderID = dm.xctn.ID()
	}
	dm.data.streams = NewStreams(dm.t.Sowner(), dm.t.Snode(), dm.data.client, dataArgs)
	if dm.useACKs() {
		ackArgs := Args{
			Net:          dm.ack.net,
			Trname:       dm.ack.trname,
			Extra:        &transport.Extra{Config: dm.config},
			Ntype:        cluster.Targets,
			ManualResync: true,
		}
		if dm.xctn != nil {
			ackArgs.Extra.SenderID = dm.xctn.ID()
		}
		dm.ack.streams = NewStreams(dm.t.Sowner(), dm.t.Snode(), dm.ack.client, ackArgs)
	}
	dm.stage.opened.Store(true)
}

func (dm *DataMover) Smap() *cluster.Smap { return dm.data.streams.Smap() }

func (dm *DataMover) String() string {
	s := "pre-or-post-"
	switch {
	case dm.stage.opened.Load():
		s = "open-"
	case dm.stage.regred.Load():
		s = "reg-" // not open yet or closed but not unreg-ed yet
	}
	if dm.data.streams.UsePDU() {
		return "dm-pdu-" + s + dm.data.streams.Trname()
	}
	return "dm-" + s + dm.data.streams.Trname()
}

// quiesce *local* Rx
func (dm *DataMover) Quiesce(d time.Duration) cluster.QuiRes {
	return dm.xctn.Quiesce(d, dm.quicb)
}

func (dm *DataMover) CloseIf(err error) {
	if dm.stage.opened.CAS(true, false) {
		dm._close(err)
	}
}

func (dm *DataMover) Close(err error) {
	debug.Assert(dm.stage.opened.Load())
	dm._close(err)
	dm.stage.opened.Store(false)
}

func (dm *DataMover) _close(err error) {
	if err == nil && dm.xctn != nil {
		if dm.xctn.IsAborted() {
			err = cmn.NewErrAborted(dm.xctn.Name(), "dm-close", dm.xctn.AbortErr())
		}
	}
	dm.data.streams.Close(err == nil) // err == nil: close gracefully via `fin`, otherwise abort
	if dm.useACKs() {
		dm.ack.streams.Close(err == nil)
	}
}

func (dm *DataMover) Abort() {
	dm.data.streams.Abort()
	if dm.useACKs() {
		dm.ack.streams.Abort()
	}
}

func (dm *DataMover) UnregRecv() {
	if !dm.stage.regred.Load() {
		return // e.g., 2PC (begin => abort) sequence with no Open
	}
	if dm.xctn != nil {
		dm.Quiesce(dm.config.Transport.QuiesceTime.D())
	}
	if err := transport.Unhandle(dm.data.trname); err != nil {
		glog.Error(err)
	}
	if dm.useACKs() {
		if err := transport.Unhandle(dm.ack.trname); err != nil {
			glog.Error(err)
		}
	}
	dm.stage.regred.Store(false)
}

func (dm *DataMover) Send(obj *transport.Obj, roc cos.ReadOpenCloser, tsi *cluster.Snode) (err error) {
	err = dm.data.streams.Send(obj, roc, tsi)
	if err == nil && !transport.ReservedOpcode(obj.Hdr.Opcode) {
		dm.xctn.OutObjsAdd(1, obj.Size())
	}
	return
}

func (dm *DataMover) ACK(hdr transport.ObjHdr, cb transport.ObjSentCB, tsi *cluster.Snode) error {
	return dm.ack.streams.Send(&transport.Obj{Hdr: hdr, Callback: cb}, nil, tsi)
}

func (dm *DataMover) Bcast(obj *transport.Obj) (err error) {
	smap := dm.data.streams.Smap()
	if smap.CountActiveTargets() > 1 {
		err = dm.data.streams.Send(obj, nil)
	}
	return
}

//
// private
//

func (dm *DataMover) quicb(_ time.Duration /*accum. sleep time*/) cluster.QuiRes {
	if dm.stage.laterx.CAS(true, false) {
		return cluster.QuiActive
	}
	return cluster.QuiInactiveCB
}

func (dm *DataMover) wrapRecvData(hdr transport.ObjHdr, object io.Reader, err error) error {
	if hdr.ObjAttrs.Size < 0 {
		// see transport.UsePDU(); TODO: dm.data.recv() to return num-received-bytes
		debug.Assert(dm.sizePDU > 0)
	} else {
		dm.xctn.InObjsAdd(1, hdr.ObjAttrs.Size)
	}
	dm.stage.laterx.Store(true)
	return dm.data.recv(hdr, object, err)
}

func (dm *DataMover) wrapRecvACK(hdr transport.ObjHdr, object io.Reader, err error) error {
	dm.stage.laterx.Store(true)
	return dm.ack.recv(hdr, object, err)
}
