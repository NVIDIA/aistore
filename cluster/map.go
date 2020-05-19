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

	"github.com/NVIDIA/aistore/cmn"
	"github.com/OneOfOne/xxhash"
)

const (
	Targets = iota // 0 (cluster.Targets) used as default value for NewStreamBundle
	Proxies
	AllNodes
)

type (
	// interface to Get current cluster-map instance
	Sowner interface {
		Get() (smap *Smap)
		Listeners() SmapListeners
	}

	// Snode's networking info
	NetInfo struct {
		NodeIPAddr string `json:"node_ip_addr"`
		DaemonPort string `json:"daemon_port"`
		DirectURL  string `json:"direct_url"`
	}

	// Snode - a node (gateway or target) in a cluster
	Snode struct {
		DaemonID        string  `json:"daemon_id"`
		DaemonType      string  `json:"daemon_type"`       // enum: "target" or "proxy"
		PublicNet       NetInfo `json:"public_net"`        // cmn.NetworkPublic
		IntraControlNet NetInfo `json:"intra_control_net"` // cmn.NetworkIntraControl
		IntraDataNet    NetInfo `json:"intra_data_net"`    // cmn.NetworkIntraData
		idDigest        uint64
		name            string
		LocalNet        *net.IPNet `json:"-"`
	}
	Nodes   []*Snode          // slice of Snodes
	NodeMap map[string]*Snode // map of Snodes: DaemonID => Snodes

	Smap struct {
		Tmap         NodeMap       `json:"tmap"`                    // targetID -> targetInfo
		Pmap         NodeMap       `json:"pmap"`                    // proxyID -> proxyInfo
		NonElects    cmn.SimpleKVs `json:"non_electable,omitempty"` // non-electable proxies: DaemonID => [info]
		ProxySI      *Snode        `json:"proxy_si"`                // primary
		Version      int64         `json:"version,string"`          // version
		UUID         string        `json:"uuid"`                    // UUID - assigned at creation time
		CreationTime string        `json:"creation_time"`           // creation time
	}
)

func (d *Snode) Digest() uint64 {
	if d.idDigest == 0 {
		d.idDigest = xxhash.ChecksumString64S(d.ID(), cmn.MLCG32)
	}
	return d.idDigest
}

func (d *Snode) ID() string   { return d.DaemonID }
func (d *Snode) Type() string { return d.DaemonType }
func (d *Snode) Name() string { return d.name }
func (d *Snode) SetName() {
	if d.IsProxy() {
		d.name = "p[" + d.DaemonID + "]"
	} else {
		cmn.Assert(d.IsTarget())
		d.name = "t[" + d.DaemonID + "]"
	}
}
func (d *Snode) String() string {
	if d.Name() == "" {
		d.SetName()
	}
	return d.Name()
}

func (d *Snode) NameEx() string {
	if d.PublicNet.DirectURL != d.IntraControlNet.DirectURL ||
		d.PublicNet.DirectURL != d.IntraDataNet.DirectURL {
		return fmt.Sprintf("%s(pub: %s, control: %s, data: %s)", d.Name(),
			d.PublicNet.DirectURL, d.IntraControlNet.DirectURL, d.IntraDataNet.DirectURL)
	}
	return fmt.Sprintf("%s(%s)", d.Name(), d.PublicNet.DirectURL)
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
	return a.ID() == b.ID() && a.DaemonType == b.DaemonType &&
		reflect.DeepEqual(a.PublicNet, b.PublicNet) &&
		reflect.DeepEqual(a.IntraControlNet, b.IntraControlNet) &&
		reflect.DeepEqual(a.IntraDataNet, b.IntraDataNet)
}

func (d *Snode) Validate() error {
	if d == nil {
		return errors.New("invalid Snode: nil")
	}
	if d.ID() == "" {
		return errors.New("invalid Snode: missing node " + d.NameEx())
	}
	if d.DaemonType != cmn.Proxy && d.DaemonType != cmn.Target {
		cmn.AssertMsg(false, "invalid Snode type '+"+d.DaemonType+"'")
	}
	return nil
}

func (d *Snode) isDuplicateURL(n *Snode) bool {
	var (
		du = []string{d.PublicNet.DirectURL, d.IntraControlNet.DirectURL, d.IntraDataNet.DirectURL}
		nu = []string{n.PublicNet.DirectURL, n.IntraControlNet.DirectURL, n.IntraDataNet.DirectURL}
	)
	for _, ni := range nu {
		for _, di := range du {
			if ni == di {
				return true
			}
		}
	}
	return false
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
	return fmt.Sprintf("Smap v%d[%s, t=%d, p=%d]", m.Version, m.UUID, m.CountTargets(), m.CountProxies())
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
		return nil, ErrNoTargets
	}
	for _, si = range m.Tmap {
		break
	}
	return
}

func (m *Smap) GetRandProxy(excludePrimary bool) (si *Snode, err error) {
	if excludePrimary {
		for _, proxy := range m.Pmap {
			if m.ProxySI.DaemonID != proxy.DaemonID {
				return proxy, nil
			}
		}
		return nil, fmt.Errorf("internal error: couldn't find non primary proxy")
	}
	for _, proxy := range m.Pmap {
		return proxy, nil
	}
	return nil, fmt.Errorf("cluster doesn't have enough proxies, expected at least 1")
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

func (m *Smap) IsDuplicateURL(nsi *Snode) (osi *Snode, err error) {
	for _, tsi := range m.Tmap {
		if tsi.ID() != nsi.ID() && tsi.isDuplicateURL(nsi) {
			osi = tsi
			break
		}
	}
	for _, psi := range m.Pmap {
		if psi.ID() != nsi.ID() && psi.isDuplicateURL(nsi) {
			osi = psi
			break
		}
	}
	if osi == nil {
		return
	}
	err = fmt.Errorf("duplicate URL:\n>>> new:\t%s\n<<< old:\t%s", nsi.NameEx(), osi.NameEx())
	return
}

func (a *Smap) Compare(b *Smap) (uuid string, sameOrigin, sameVersion, eq bool) {
	sameOrigin, sameVersion, eq = true, true, true
	if a.UUID != "" && b.UUID != "" && a.UUID != b.UUID {
		sameOrigin = false
	} else {
		uuid = a.UUID
		if uuid == "" {
			uuid = b.UUID
		}
	}
	if a.Version != b.Version {
		sameVersion = false
	}
	if a.ProxySI == nil || b.ProxySI == nil || !a.ProxySI.Equals(b.ProxySI) {
		eq = false
		return
	}
	if !a.NonElects.Compare(b.NonElects) {
		eq = false
		return
	}
	eq = mapsEq(a.Tmap, b.Tmap) && mapsEq(a.Pmap, b.Pmap)
	return
}

func (m *NodeMap) Add(snode *Snode) {
	if *m == nil {
		*m = make(NodeMap)
	}
	(*m)[snode.DaemonID] = snode
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
