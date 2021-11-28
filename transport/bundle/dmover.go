// Package bundle provides multi-streaming transport with the functionality
// to dynamically (un)register receive endpoints, establish long-lived flows, and more.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package bundle

import (
	"fmt"
	"io"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
)

// NOTE: configuration-wise, currently using config.Rebalance.Multiplier and Rebalance.Quiesce

type (
	DataMover struct {
		t    cluster.Target
		data struct {
			trname  string
			recv    transport.ReceiveObj
			net     string // one of cmn.KnownNetworks, empty defaults to cmn.NetworkIntraData
			streams *Streams
			client  transport.Client
		}
		ack struct {
			trname  string
			recv    transport.ReceiveObj
			net     string // one of cmn.KnownNetworks, empty defaults to cmn.NetworkIntraControl
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
		compression string // enum { cmn.CompressNever, ... }
		xact        cluster.Xact
		multiplier  int
		sizePDU     int32
		recvType    cluster.RecvType
	}
	// additional (and optional) params for new data mover
	Extra struct {
		RecvAck     transport.ReceiveObj
		Compression string
		Multiplier  int
		SizePDU     int32
	}
)

// interface guard
var _ cluster.DataMover = (*DataMover)(nil)

// recvType is mandatory DM property: a data mover passes the property to
// `target.PutObject` to make to finalize an object properly after the object
// is saved to local drives(e.g, PUT the object to the Cloud as well).
// For DMs that does not create new objects(e.g, rebalance), recvType should
// be `Migrated`, and `RegularPut` for others(e.g, CopyBucket).
func NewDataMover(t cluster.Target, trname string, recvCB transport.ReceiveObj, recvType cluster.RecvType,
	extra Extra) (*DataMover, error) {
	dm := &DataMover{t: t, config: cmn.GCO.Get(), mem: t.PageMM()}
	if extra.Multiplier == 0 {
		extra.Multiplier = int(dm.config.Rebalance.Multiplier)
	}
	if extra.Multiplier > 8 {
		return nil, fmt.Errorf("invalid multiplier %d", extra.Multiplier)
	}
	dm.recvType = recvType
	dm.multiplier = extra.Multiplier
	dm.sizePDU = extra.SizePDU
	switch extra.Compression {
	case "":
		dm.compression = cmn.CompressNever
	case cmn.CompressAlways, cmn.CompressNever:
		dm.compression = extra.Compression
	default:
		return nil, fmt.Errorf("invalid compression %q", extra.Compression)
	}
	dm.data.trname, dm.data.recv = trname, recvCB
	if dm.data.net == "" {
		dm.data.net = cmn.NetworkIntraData
	}
	dm.data.client = transport.NewIntraDataClient()
	// ack
	if dm.ack.net == "" {
		dm.ack.net = cmn.NetworkIntraControl
	}
	dm.ack.recv = extra.RecvAck
	if !dm.useACKs() {
		return dm, nil
	}
	dm.ack.trname = "ack." + trname
	dm.ack.client = transport.NewIntraDataClient()
	return dm, nil
}

func (dm *DataMover) useACKs() bool              { return dm.ack.recv != nil }
func (dm *DataMover) NetD() string               { return dm.data.net }
func (dm *DataMover) NetC() string               { return dm.ack.net }
func (dm *DataMover) RecvType() cluster.RecvType { return dm.recvType }

// xaction that drives and utilizes this data mover
func (dm *DataMover) SetXact(xact cluster.Xact) { dm.xact = xact }
func (dm *DataMover) GetXact() cluster.Xact     { return dm.xact }

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
	if dm.xact != nil {
		dataArgs.Extra.SenderID = dm.xact.ID()
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
		if dm.xact != nil {
			ackArgs.Extra.SenderID = dm.xact.ID()
		}
		dm.ack.streams = NewStreams(dm.t.Sowner(), dm.t.Snode(), dm.ack.client, ackArgs)
	}
	dm.stage.opened.Store(true)
}

func (dm *DataMover) IsOpen() bool { return dm.stage.opened.Load() }

// quiesce *local* Rx
func (dm *DataMover) Quiesce(d time.Duration) cluster.QuiRes {
	return dm.xact.Quiesce(d, dm.quicb)
}

func (dm *DataMover) Close(err error) {
	debug.Assert(dm.stage.opened.Load()) // Open() must've been called
	if err == nil && dm.xact != nil && dm.xact.Aborted() {
		err = cmn.NewErrAborted(dm.xact.Name(), "abort", nil)
	}
	dm.data.streams.Close(err == nil) // err == nil: close gracefully via `fin`, otherwise abort
	if dm.useACKs() {
		dm.ack.streams.Close(err == nil)
	}
	dm.stage.opened.Store(false)
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
	dm.Quiesce(dm.config.Rebalance.Quiesce.D())
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
		dm.xact.OutObjsAdd(1, obj.Size())
	}
	return
}

func (dm *DataMover) ACK(hdr transport.ObjHdr, cb transport.ObjSentCB, tsi *cluster.Snode) error {
	return dm.ack.streams.Send(&transport.Obj{Hdr: hdr, Callback: cb}, nil, tsi)
}

func (dm *DataMover) Bcast(obj *transport.Obj) (err error) {
	smap := dm.data.streams.Smap()
	if smap.CountTargets() > 1 {
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

func (dm *DataMover) wrapRecvData(hdr transport.ObjHdr, object io.Reader, err error) {
	dm.xact.InObjsAdd(1, hdr.ObjAttrs.Size)
	dm.stage.laterx.Store(true)
	dm.data.recv(hdr, object, err)
}

func (dm *DataMover) wrapRecvACK(hdr transport.ObjHdr, object io.Reader, err error) {
	dm.stage.laterx.Store(true)
	dm.ack.recv(hdr, object, err)
}
