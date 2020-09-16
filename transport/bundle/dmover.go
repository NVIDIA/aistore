// Package bundle TODO
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package bundle

import (
	"fmt"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
)

type (
	DataMover struct {
		t    cluster.Target
		data struct {
			trname  string
			recv    transport.Receive
			net     string
			streams *Streams
			client  transport.Client
		}
		ack struct {
			trname  string
			recv    transport.Receive
			net     string
			streams *Streams
			client  transport.Client
		}
		mem         *memsys.MMSA
		compression string
		multiplier  int
	}
	Extra struct {
		RecvAck     transport.Receive
		Compression string
		Multiplier  int
	}
)

// interface guard
var (
	_ cluster.DataMover = &DataMover{}
)

func NewDataMover(t cluster.Target, trname string, recvCB transport.Receive, extra Extra) (*DataMover, error) {
	var (
		config = cmn.GCO.Get()
		dm     = &DataMover{t: t, mem: t.MMSA()}
	)
	if extra.Multiplier == 0 {
		extra.Multiplier = int(config.Rebalance.Multiplier)
	}
	if extra.Multiplier > 8 {
		return nil, fmt.Errorf("invalid multiplier %d", extra.Multiplier)
	}
	dm.multiplier = extra.Multiplier
	switch extra.Compression {
	case "":
		dm.compression = cmn.CompressNever
	case cmn.CompressAlways, cmn.CompressNever:
		dm.compression = extra.Compression
	default:
		return nil, fmt.Errorf("invalid compression %q", extra.Compression)
	}
	dm.data.trname, dm.data.recv = trname, recvCB
	dm.data.net = cmn.NetworkPublic
	if config.Net.UseIntraData {
		dm.data.net = cmn.NetworkIntraData
	}
	dm.data.client = transport.NewIntraDataClient()
	// ack
	dm.ack.net = cmn.NetworkPublic
	if config.Net.UseIntraControl {
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

func (dm *DataMover) useACKs() bool { return dm.ack.recv != nil }
func (dm *DataMover) NetD() string  { return dm.data.net }
func (dm *DataMover) NetC() string  { return dm.ack.net }

func (dm *DataMover) RegRecv() (err error) {
	if _, err = transport.Register(dm.data.net, dm.data.trname, dm.data.recv); err != nil {
		return
	}
	if dm.useACKs() {
		_, err = transport.Register(dm.ack.net, dm.ack.trname, dm.ack.recv)
	}
	return
}

func (dm *DataMover) Open() {
	var (
		config   = cmn.GCO.Get()
		dataArgs = Args{
			Network: dm.data.net,
			Trname:  dm.data.trname,
			Extra: &transport.Extra{
				Compression: dm.compression,
				Config:      config,
				MMSA:        dm.mem},
			Ntype:        cluster.Targets,
			Multiplier:   dm.multiplier,
			ManualResync: true,
		}
		ackArgs = Args{
			Network:      dm.ack.net,
			Trname:       dm.ack.trname,
			Extra:        &transport.Extra{Config: config},
			ManualResync: true,
		}
	)
	dm.data.streams = NewStreams(dm.t.Sowner(), dm.t.Snode(), dm.data.client, dataArgs)
	if dm.useACKs() {
		dm.ack.streams = NewStreams(dm.t.Sowner(), dm.t.Snode(), dm.ack.client, ackArgs)
	}
}

func (dm *DataMover) Close() {
	dm.data.streams.Close(true /* graceful */)
	dm.data.streams = nil
	if dm.useACKs() {
		dm.ack.streams.Close(true)
	}
}

func (dm *DataMover) UnregRecv() {
	if err := transport.Unregister(dm.data.net, dm.data.trname); err != nil {
		glog.Error(err)
	}
	if dm.useACKs() {
		if err := transport.Unregister(dm.ack.net, dm.ack.trname); err != nil {
			glog.Error(err)
		}
	}
}

func (dm *DataMover) Send(obj transport.Obj, roc cmn.ReadOpenCloser, tsi *cluster.Snode) (err error) {
	return dm.data.streams.Send(obj, roc, tsi)
}

func (dm *DataMover) ACK(hdr transport.Header, cb transport.SendCallback, tsi *cluster.Snode) (err error) {
	return dm.ack.streams.Send(transport.Obj{Hdr: hdr, Callback: cb}, nil, tsi)
}
