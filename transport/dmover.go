// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"fmt"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/memsys"
)

type (
	DataMover struct {
		t    cluster.Target
		data struct {
			trname  string
			recv    Receive
			net     string
			streams *StreamBundle
			client  Client
		}
		ack struct {
			trname  string
			recv    Receive
			net     string
			streams *StreamBundle
			client  Client
		}
		mem         *memsys.MMSA
		compression string
		multiplier  int
	}
	DMExtra struct {
		RecvAck     Receive
		Compression string
		Multiplier  int
	}
)

func NewDataMover(t cluster.Target, trname string, recvData Receive, extra DMExtra) (*DataMover, error) {
	var (
		config = cmn.GCO.Get()
		dm     = &DataMover{t: t, mem: t.GetMMSA()}
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
	dm.data.trname, dm.data.recv = trname, recvData
	dm.data.net = cmn.NetworkPublic
	if config.Net.UseIntraData {
		dm.data.net = cmn.NetworkIntraData
	}
	dm.data.client = NewIntraDataClient()
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
	dm.ack.client = NewIntraDataClient()
	return dm, nil
}

func (dm *DataMover) useACKs() bool { return dm.ack.recv != nil }
func (dm *DataMover) NetD() string  { return dm.data.net }
func (dm *DataMover) NetC() string  { return dm.ack.net }

func (dm *DataMover) RegRecv() (err error) {
	if _, err = Register(dm.data.net, dm.data.trname, dm.data.recv); err != nil {
		return
	}
	if dm.useACKs() {
		_, err = Register(dm.ack.net, dm.ack.trname, dm.ack.recv)
	}
	return
}

func (dm *DataMover) Open() {
	var (
		config   = cmn.GCO.Get()
		dataArgs = SBArgs{
			Network: dm.data.net,
			Trname:  dm.data.trname,
			Extra: &Extra{
				Compression: dm.compression,
				Config:      config,
				MMSA:        dm.mem},
			Ntype:        cluster.Targets,
			Multiplier:   dm.multiplier,
			ManualResync: true,
		}
		ackArgs = SBArgs{
			Network:      dm.ack.net,
			Trname:       dm.ack.trname,
			Extra:        &Extra{Config: config},
			ManualResync: true,
		}
	)
	dm.data.streams = NewStreamBundle(dm.t.GetSowner(), dm.t.Snode(), dm.data.client, dataArgs)
	if dm.useACKs() {
		dm.ack.streams = NewStreamBundle(dm.t.GetSowner(), dm.t.Snode(), dm.ack.client, ackArgs)
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
	if err := Unregister(dm.data.net, dm.data.trname); err != nil {
		glog.Error(err)
	}
	if dm.useACKs() {
		if err := Unregister(dm.ack.net, dm.ack.trname); err != nil {
			glog.Error(err)
		}
	}
}

func (dm *DataMover) Send(obj Obj, roc cmn.ReadOpenCloser, tsi *cluster.Snode) (err error) {
	return dm.data.streams.Send(obj, roc, tsi)
}

func (dm *DataMover) ACK(hdr Header, cb SendCallback, tsi *cluster.Snode) (err error) {
	return dm.ack.streams.Send(Obj{Hdr: hdr, Callback: cb}, nil, tsi)
}
