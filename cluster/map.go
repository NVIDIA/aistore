// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/OneOfOne/xxhash"
)

const (
	Targets = iota // 0 (cluster.Targets) used as default value for NewStreamBundle
	Proxies
	AllNodes
)

// enum: Snode flags
const (
	SnodeNonElectable = 1 << iota
	SnodeIC
	SnodeMaintenance
	SnodeDecomission
)

const (
	SnodeMaintenanceMask SnodeFlags = SnodeMaintenance | SnodeDecomission

	icGroupSize = 3 // desirable gateway count in the Information Center
)

type (
	// interface to Get current cluster-map instance
	Sowner interface {
		Get() (smap *Smap)
		Listeners() SmapListeners
	}

	SnodeFlags uint64

	// Snode's networking info
	NetInfo struct {
		NodeIPAddr string `json:"node_ip_addr"`
		DaemonPort string `json:"daemon_port"`
		DirectURL  string `json:"direct_url"`
	}

	// Snode - a node (gateway or target) in a cluster
	Snode struct {
		DaemonID        string     `json:"daemon_id"`
		DaemonType      string     `json:"daemon_type"`       // enum: "target" or "proxy"
		PublicNet       NetInfo    `json:"public_net"`        // cmn.NetworkPublic
		IntraControlNet NetInfo    `json:"intra_control_net"` // cmn.NetworkIntraControl
		IntraDataNet    NetInfo    `json:"intra_data_net"`    // cmn.NetworkIntraData
		Flags           SnodeFlags `json:"flags"`             // enum cmn.Snode*
		idDigest        uint64
		name            string
		LocalNet        *net.IPNet `json:"-"`
	}
	Nodes   []*Snode          // slice of Snodes
	NodeMap map[string]*Snode // map of Snodes: DaemonID => Snodes

	Smap struct {
		Tmap         NodeMap `json:"tmap"`           // targetID -> targetInfo
		Pmap         NodeMap `json:"pmap"`           // proxyID -> proxyInfo
		Primary      *Snode  `json:"proxy_si"`       // (json tag preserved for back. compat.)
		Version      int64   `json:"version,string"` // version
		UUID         string  `json:"uuid"`           // UUID (assigned once at creation time)
		CreationTime string  `json:"creation_time"`  // creation time
	}

	// Smap on-change listeners
	Slistener interface {
		String() string
		ListenSmapChanged()
	}
	SmapListeners interface {
		Reg(sl Slistener)
		Unreg(sl Slistener)
	}
)

////////////////
// SnodeFlags //
////////////////

func (f SnodeFlags) Set(flags SnodeFlags) SnodeFlags {
	return f | flags
}

func (f SnodeFlags) Clear(flags SnodeFlags) SnodeFlags {
	return f &^ flags
}

func (f SnodeFlags) IsSet(flags SnodeFlags) bool {
	return f&flags == flags
}

func (f SnodeFlags) IsAnySet(flags SnodeFlags) bool {
	return f&flags != 0
}

///////////
// Snode //
///////////

func NewSnode(id, proto, daeType string, publicAddr, intraControlAddr, intraDataAddr *net.TCPAddr) (snode *Snode) {
	publicNet := NetInfo{
		NodeIPAddr: publicAddr.IP.String(),
		DaemonPort: strconv.Itoa(publicAddr.Port),
		DirectURL:  proto + "://" + publicAddr.String(),
	}
	intraControlNet := publicNet
	if len(intraControlAddr.IP) > 0 {
		intraControlNet = NetInfo{
			NodeIPAddr: intraControlAddr.IP.String(),
			DaemonPort: strconv.Itoa(intraControlAddr.Port),
			DirectURL:  proto + "://" + intraControlAddr.String(),
		}
	}
	intraDataNet := publicNet
	if len(intraDataAddr.IP) > 0 {
		intraDataNet = NetInfo{
			NodeIPAddr: intraDataAddr.IP.String(),
			DaemonPort: strconv.Itoa(intraDataAddr.Port),
			DirectURL:  proto + "://" + intraDataAddr.String(),
		}
	}
	snode = &Snode{
		DaemonID:        id,
		DaemonType:      daeType,
		PublicNet:       publicNet,
		IntraControlNet: intraControlNet,
		IntraDataNet:    intraDataNet,
	}
	snode.SetName()
	snode.Digest()
	return
}

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
		debug.Assert(d.IsTarget())
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
		cmn.Assertf(false, "unknown network %q", network)
		return ""
	}
}

func (d *Snode) Equals(other *Snode) bool {
	return d.ID() == other.ID() && d.DaemonType == other.DaemonType &&
		reflect.DeepEqual(d.PublicNet, other.PublicNet) &&
		reflect.DeepEqual(d.IntraControlNet, other.IntraControlNet) &&
		reflect.DeepEqual(d.IntraDataNet, other.IntraDataNet)
}

func (d *Snode) Validate() error {
	if d == nil {
		return errors.New("invalid Snode: nil")
	}
	if d.ID() == "" {
		return errors.New("invalid Snode: missing node " + d.NameEx())
	}
	if d.DaemonType != cmn.Proxy && d.DaemonType != cmn.Target {
		cmn.Assertf(false, "invalid Snode type %q", d.DaemonType)
	}
	return nil
}

func (d *Snode) Clone() *Snode {
	var dst Snode
	cmn.CopyStruct(&dst, d)
	return &dst
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

// Functions nonElectable, inMaintenance, and isIC must be used in `cluster`
// package only. All other packages must use Smap's or NodeMap's methods
func (d *Snode) nonElectable() bool  { return d.Flags.IsSet(SnodeNonElectable) }
func (d *Snode) inMaintenance() bool { return d.Flags.IsAnySet(SnodeMaintenanceMask) }
func (d *Snode) isIC() bool          { return d.Flags.IsSet(SnodeIC) }

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
func (m *Smap) Count() int        { return len(m.Pmap) + len(m.Tmap) }
func (m *Smap) CountActiveTargets() (count int) {
	for _, t := range m.Tmap {
		if !t.inMaintenance() {
			count++
		}
	}
	return
}

func (m *Smap) CountNonElectable() (count int) {
	for _, p := range m.Pmap {
		if p.nonElectable() {
			count++
		}
	}
	return
}

func (m *Smap) CountActiveProxies() (count int) {
	for _, t := range m.Pmap {
		if !t.inMaintenance() {
			count++
		}
	}
	return
}

func (m *Smap) GetProxy(pid string) *Snode {
	psi, ok := m.Pmap[pid]
	if !ok {
		return nil
	}
	return psi
}

func (m *Smap) GetTarget(sid string) *Snode {
	tsi, ok := m.Tmap[sid]
	if !ok {
		return nil
	}
	return tsi
}

func (m *Smap) IsPrimary(si *Snode) bool {
	return m.Primary.Equals(si)
}

func (m *Smap) NewTmap(tids []string) (tmap NodeMap, err error) {
	for _, tid := range tids {
		if m.GetTarget(tid) == nil {
			return nil, cmn.NewNotFoundError("t[%s]", tid)
		}
	}
	tmap = make(NodeMap, len(tids))
	for _, tid := range tids {
		tmap[tid] = m.GetTarget(tid)
	}
	return
}

func (m *Smap) GetNode(id string) *Snode {
	if node := m.GetTarget(id); node != nil {
		return node
	}
	return m.GetProxy(id)
}

func (m *Smap) GetRandTarget() (tsi *Snode, err error) {
	for _, tsi = range m.Tmap {
		if tsi.inMaintenance() {
			tsi = nil
			continue
		}
		return tsi, nil
	}
	err = cmn.NewNoNodesError(cmn.Target)
	return
}

func (m *Smap) GetRandProxy(excludePrimary bool) (si *Snode, err error) {
	if excludePrimary {
		for _, proxy := range m.Pmap {
			if proxy.inMaintenance() {
				continue
			}
			if m.Primary.DaemonID != proxy.DaemonID {
				return proxy, nil
			}
		}
		return nil, fmt.Errorf("internal error: couldn't find non primary proxy")
	}
	for _, psi := range m.Pmap {
		if psi.inMaintenance() {
			continue
		}
		return psi, nil
	}
	return nil, fmt.Errorf("cluster doesn't have enough proxies, expected at least 1")
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

func (m *Smap) Compare(other *Smap) (uuid string, sameOrigin, sameVersion, eq bool) {
	sameOrigin, sameVersion, eq = true, true, true
	if m.UUID != "" && other.UUID != "" && m.UUID != other.UUID {
		sameOrigin = false
	} else {
		uuid = m.UUID
		if uuid == "" {
			uuid = other.UUID
		}
	}
	if m.Version != other.Version {
		sameVersion = false
	}
	if m.Primary == nil || other.Primary == nil || !m.Primary.Equals(other.Primary) {
		eq = false
		return
	}
	eq = mapsEq(m.Tmap, other.Tmap) && mapsEq(m.Pmap, other.Pmap)
	return
}

func (m *Smap) CompareTargets(other *Smap) (equal bool) {
	return mapsEq(m.Tmap, other.Tmap)
}

func (m *Smap) NonElectable(psi *Snode) (ok bool) {
	node := m.GetProxy(psi.ID())
	return node != nil && node.nonElectable()
}

func (m *Smap) InMaintenance(psi *Snode) (ok bool) {
	node := m.GetNode(psi.ID())
	return node != nil && node.inMaintenance()
}

// Method is used when comparing one SMap to another ones, so
// it cannot be replaced with single `psi.isIC()` call
func (m *Smap) IsIC(psi *Snode) (ok bool) {
	node := m.GetProxy(psi.ID())
	return node != nil && node.isIC()
}

func (m *Smap) StrIC(node *Snode) string {
	all := make([]string, 0, m.DefaultICSize())
	for pid, psi := range m.Pmap {
		if !psi.isIC() {
			continue
		}
		if node != nil && pid == node.ID() {
			all = append(all, pid+"(*)")
		} else {
			all = append(all, pid)
		}
	}
	return strings.Join(all, ",")
}

func (m *Smap) ICCount() int {
	count := 0
	for _, psi := range m.Pmap {
		if psi.isIC() {
			count++
		}
	}
	return count
}

func (m *Smap) DefaultICSize() int { return icGroupSize }

/////////////
// NodeMap //
/////////////

func (m NodeMap) Add(snode *Snode) { debug.Assert(m != nil); m[snode.DaemonID] = snode }

func (m NodeMap) ActiveMap() (clone NodeMap) {
	clone = make(NodeMap, len(m))
	for id, node := range m {
		if node.inMaintenance() {
			continue
		}
		clone[id] = node
	}
	return
}

func (m NodeMap) ActiveNodes() []*Snode {
	snodes := make([]*Snode, 0, len(m))
	for _, node := range m {
		if node.inMaintenance() {
			continue
		}
		snodes = append(snodes, node)
	}
	return snodes
}

func (m NodeMap) Contains(daeID string) (exists bool) {
	_, exists = m[daeID]
	return
}

func (m NodeMap) InMaintenance(si *Snode) bool {
	node, exists := m[si.ID()]
	return exists && node.inMaintenance()
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

// helper to find out NodeMap "delta" or "diff"
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
