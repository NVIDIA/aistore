// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/OneOfOne/xxhash"
)

const (
	Targets = iota // 0 (cluster.Targets) used as default value for NewStreamBundle
	Proxies
	AllNodes
)

// interface to Get current cluster-map instance
// (for implementation, see ais/clustermap.go)
type Sowner interface {
	Get() (smap *Smap)
	Listeners() SmapListeners
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
	DaemonType      string  `json:"daemon_type"`       // enum: "target" or "proxy"
	PublicNet       NetInfo `json:"public_net"`        // cmn.NetworkPublic
	IntraControlNet NetInfo `json:"intra_control_net"` // cmn.NetworkIntraControl
	IntraDataNet    NetInfo `json:"intra_data_net"`    // cmn.NetworkIntraData
	idDigest        uint64
	LocalNet        *net.IPNet `json:"-"`
}

func (d *Snode) Digest() uint64 {
	if d.idDigest == 0 {
		d.idDigest = xxhash.ChecksumString64S(d.DaemonID, cmn.MLCG32)
	}
	return d.idDigest
}

func (d *Snode) ID() string {
	return d.DaemonID
}

const snodeFmt = "[\n\tDaemonID: %s,\n\tDaemonType: %s, \n\tPublicNet: %s,\n\tIntraControl: %s,\n\tIntraData: %s,\n\tidDigest: %d]"

func (d *Snode) _string() string {
	return fmt.Sprintf(snodeFmt, d.DaemonID, d.DaemonType, d.PublicNet.DirectURL,
		d.IntraControlNet.DirectURL, d.IntraDataNet.DirectURL, d.idDigest)
}
func (d *Snode) String() string {
	if glog.FastV(4, glog.SmoduleCluster) {
		return d._string()
	}
	return d.DaemonID
}

func (d *Snode) URL(network string) string {
	switch network {
	case cmn.NetworkPublic:
		return d.PublicNet.DirectURL
	case cmn.NetworkIntraControl:
		return d.IntraControlNet.DirectURL
	case cmn.NetworkIntraData:
		return d.IntraDataNet.DirectURL
	default:
		cmn.AssertMsg(false, "unknown network '"+network+"'")
		return ""
	}
}

func (a *Snode) Equals(b *Snode) bool {
	return a.DaemonID == b.DaemonID && a.DaemonType == b.DaemonType &&
		reflect.DeepEqual(a.PublicNet, b.PublicNet) &&
		reflect.DeepEqual(a.IntraControlNet, b.IntraControlNet) &&
		reflect.DeepEqual(a.IntraDataNet, b.IntraDataNet)
}

// See printname() in clustermap.go
func (d *Snode) Name() string {
	if d.DaemonType == cmn.Proxy {
		return "p[" + d.DaemonID + "]"
	}
	return "t[" + d.DaemonID + "]"
}

func (d *Snode) Validate() error {
	if d == nil {
		return errors.New("invalid Snode: nil")
	}
	if d.DaemonID == "" {
		return errors.New("invalid Snode: missing node ID " + d._string())
	}
	if d.DaemonType != cmn.Proxy && d.DaemonType != cmn.Target {
		return errors.New("invalid Snode: unexpected type " + d._string())
	}
	return nil
}

func (d *Snode) IsProxy() bool  { return d.DaemonType == cmn.Proxy }
func (d *Snode) IsTarget() bool { return d.DaemonType == cmn.Target }

//===============================================================
//
// Smap: cluster map is a versioned object
// Executing Sowner.Get() gives an immutable version that won't change
// Smap versioning is monotonic and incremental
// Smap uniquely and solely defines the primary proxy
//
//===============================================================
type (
	NodeMap map[string]*Snode

	Smap struct {
		Tmap         NodeMap       `json:"tmap"` // daemonID -> Snode
		Pmap         NodeMap       `json:"pmap"` // proxyID -> proxyInfo
		NonElects    cmn.SimpleKVs `json:"non_electable"`
		ProxySI      *Snode        `json:"proxy_si"`
		Version      int64         `json:"version,string"`
		Origin       uint64        `json:"origin,string"` // (unique) origin stays the same for the lifetime
		CreationTime string        `json:"creation_time"`
	}
)

func (m *Smap) InitDigests() {
	for _, node := range m.Tmap {
		node.Digest()
	}
	for _, node := range m.Pmap {
		node.Digest()
	}
}

func (m *Smap) String() string {
	if m == nil {
		return "Smap <nil>"
	}
	return "Smap v" + strconv.FormatInt(m.Version, 10)
}
func (m *Smap) StringEx() string {
	if m == nil {
		return "Smap <nil>"
	}
	return fmt.Sprintf("Smap v%d[...%d, t=%d, p=%d]", m.Version, m.Origin%1000, m.CountTargets(), m.CountProxies())
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

func (m *Smap) GetRandTarget() (si *Snode, err error) {
	if m.CountTargets() == 0 {
		return nil, errNoTargets
	}
	for _, si = range m.Tmap {
		break
	}
	return
}

func (m *Smap) GetProxy(pid string) *Snode {
	pi, ok := m.Pmap[pid]
	if !ok {
		return nil
	}
	return pi
}

func (m *Smap) GetNode(id string) *Snode {
	if node := m.GetTarget(id); node != nil {
		return node
	}
	return m.GetProxy(id)
}

func (a *Smap) Compare(b *Smap) (sameOrigin, sameVersion, eq bool) {
	sameOrigin, sameVersion, eq = true, true, true
	if a.Origin != 0 && b.Origin != 0 && a.Origin != b.Origin {
		sameOrigin = false
	}
	if a.Version != b.Version {
		sameVersion = false
	}
	if a.ProxySI == nil || b.ProxySI == nil || !a.ProxySI.Equals(b.ProxySI) {
		eq = false
		return
	}
	if !reflect.DeepEqual(a.NonElects, b.NonElects) {
		eq = false
		return
	}
	eq = mapsEq(a.Tmap, b.Tmap) && mapsEq(a.Pmap, b.Pmap)
	return
}

func mapsEq(a, b NodeMap) bool {
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

//
// helper to find out Smap "delta" in terms of targets and proxies, added and removed
// can be used as destination selectors - see the DestSelector typedef
//

func NodeMapDelta(old, new []NodeMap) (added, removed NodeMap) {
	added, removed = make(NodeMap), make(NodeMap)
	for i, mold := range old {
		mnew := new[i]
		for id, si := range mnew {
			if _, ok := mold[id]; !ok {
				added[id] = si
			}
		}
	}
	for i, mold := range old {
		mnew := new[i]
		for id, si := range mold {
			if _, ok := mnew[id]; !ok {
				removed[id] = si
			}
		}
	}
	return
}

//==================================================================
//
// minimal smap-update listening frame
//
//==================================================================
type (
	Slistener interface {
		String() string
		ListenSmapChanged(chan int64)
	}
	SmapListeners interface {
		Reg(sl Slistener)
		Unreg(sl Slistener)
	}
)
