/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package cluster provides local access to cluster-level metadata
package cluster

import (
	"fmt"
	"reflect"

	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/OneOfOne/xxhash"
)

// interface to Get current cluster-map instance
// (for implementation, see dfc/clustermap.go)
type Sowner interface {
	Get() (smap *Smap)
}

// NetInfo
type NetInfo struct {
	NodeIPAddr string `json:"node_ip_addr"`
	DaemonPort string `json:"daemon_port"`
	DirectURL  string `json:"direct_url"`
}

//==================================================================
//
// Snode: represents storage daemon in a cluster (gateway or target)
//
//==================================================================
type Snode struct {
	DaemonID        string  `json:"daemon_id"`
	PublicNet       NetInfo `json:"public_net"`
	IntraControlNet NetInfo `json:"intra_control_net"`
	IntraDataNet    NetInfo `json:"intra_data_net"`
	idDigest        uint64
}

func (d *Snode) Digest() uint64 {
	if d.idDigest == 0 {
		d.idDigest = xxhash.ChecksumString64S(d.DaemonID, MLCG32)
	}
	return d.idDigest
}

func (d *Snode) String() string {
	f := "[\n\tDaemonID: %s,\n\tPublicNet: %s,\n\tIntraControlNet: %s,\n\tIntraDataNet: %s,\n\tidDigest: %d]"
	return fmt.Sprintf(f, d.DaemonID, d.PublicNet.DirectURL, d.IntraControlNet.DirectURL, d.IntraDataNet.DirectURL, d.idDigest)
}

func (a *Snode) Equals(b *Snode) bool {
	return a.DaemonID == b.DaemonID &&
		reflect.DeepEqual(a.PublicNet, b.PublicNet) &&
		reflect.DeepEqual(a.IntraControlNet, b.IntraControlNet) &&
		reflect.DeepEqual(a.IntraDataNet, b.IntraDataNet)
}

//===============================================================
//
// Smap: cluster map is a versioned object
// Executing Sowner.Get() gives an immutable version that won't change
// Smap versioning is monotonic and incremental
// Smap uniquely and solely defines the primary proxy
//
//===============================================================
type Smap struct {
	Tmap      map[string]*Snode `json:"tmap"` // daemonID -> Snode
	Pmap      map[string]*Snode `json:"pmap"` // proxyID -> proxyInfo
	NonElects cmn.SimpleKVs     `json:"non_electable"`
	ProxySI   *Snode            `json:"proxy_si"`
	Version   int64             `json:"version"`
}

func (m *Smap) CountTargets() int { return len(m.Tmap) }
func (m *Smap) CountProxies() int { return len(m.Pmap) }

func (m *Smap) GetTarget(sid string) *Snode {
	si, ok := m.Tmap[sid]
	if !ok {
		return nil
	}
	return si
}

func (m *Smap) GetProxy(pid string) *Snode {
	pi, ok := m.Pmap[pid]
	if !ok {
		return nil
	}
	return pi
}

func (a *Smap) Equals(b *Smap) bool {
	if a.Version != b.Version {
		return false
	}
	if !a.ProxySI.Equals(b.ProxySI) {
		return false
	}
	if !reflect.DeepEqual(a.NonElects, b.NonElects) {
		return false
	}
	return mapsEq(a.Tmap, b.Tmap) && mapsEq(a.Pmap, b.Pmap)
}
func mapsEq(a, b map[string]*Snode) bool {
	if len(a) != len(b) {
		return false
	}
	for id, anode := range a {
		if bnode, ok := b[id]; !ok {
			return false
		} else if !anode.Equals(bnode) {
			return false
		}
	}
	return true
}
