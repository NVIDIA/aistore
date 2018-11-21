/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
// Package cluster provides common interfaces and local access to cluster-level metadata
package cluster

import (
	"io"
	"net/http"
	"sync"

	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/transport"
)

const (
	Targets = iota
	Proxies
	AllNodes
)

type (
	DestSelector func(old, new *Smap) (added, removed NodeMap) // DeltaTargets(), DeltaProxies(), or custom impl.

	StreamBundle struct {
		sync.Mutex
		sowner  Sowner
		lsnode  *Snode // local Snode
		smap    *Smap  // current Smap
		client  *http.Client
		network string
		trname  string
		streams map[string]*transport.Stream // by DaemonID
		extra   transport.Extra
		to      int
	}
)

func NewStreamBundle(sowner Sowner, lsnode *Snode, cl *http.Client, network, trname string, extra *transport.Extra, to int) (sb *StreamBundle) {
	cmn.Assert(cmn.NetworkIsKnown(network), "unknown network '"+network+"'")
	listeners := sowner.Listeners()
	sb = &StreamBundle{
		sowner:  sowner,
		lsnode:  lsnode,
		client:  cl,
		network: network,
		trname:  trname,
		extra:   *extra,
		to:      to,
	}
	sb.smap = &Smap{}
	sb.SmapChanged() // nil -> smap initialization
	listeners.Reg(sb)
	return
}

// broadcast
func (sb *StreamBundle) Send(hdr transport.Header, reader io.ReadCloser, callback transport.SendCallback) {
	for _, s := range sb.streams {
		s.Send(hdr, reader, callback)
	}
}

// implements cluster.Slistener interface

var _ Slistener = &StreamBundle{}

func (sb *StreamBundle) Tag() string {
	return sb.lsnode.DaemonID + ":" + sb.network + ":" + sb.trname
}

func (sb *StreamBundle) SmapChanged() {
	var old, new []NodeMap
	smap := sb.sowner.Get()
	switch sb.to {
	case Targets:
		old = []NodeMap{sb.smap.Tmap}
		new = []NodeMap{smap.Tmap}
	case Proxies:
		old = []NodeMap{sb.smap.Pmap}
		new = []NodeMap{smap.Pmap}
	case AllNodes:
		old = []NodeMap{sb.smap.Tmap, sb.smap.Pmap}
		new = []NodeMap{smap.Tmap, smap.Pmap}
	default:
		cmn.Assert(false)
	}
	added, removed := nodeMapDelta(old, new)

	if sb.streams == nil {
		sb.streams = make(map[string]*transport.Stream, len(added))
	}
	for id, si := range added {
		if id == sb.lsnode.DaemonID {
			continue
		}
		toURL := cmn.URLPath(si.URL(sb.network), cmn.Version, cmn.Transport, sb.trname)
		ns := transport.NewStream(sb.client, toURL, &sb.extra)
		sb.streams[id] = ns
	}
	for id := range removed {
		if id == sb.lsnode.DaemonID {
			continue
		}
		os := sb.streams[id]
		os.Fin() // NOTE: blocking/graceful
		delete(sb.streams, id)
	}
	sb.smap = smap
}
