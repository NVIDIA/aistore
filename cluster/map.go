// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/sys"
	"github.com/OneOfOne/xxhash"
)

const (
	Targets = iota // 0 (cluster.Targets) used as default value for NewStreamBundle
	Proxies
	AllNodes
)

// number of broadcasting goroutines <= cmn.NumCPU() * MaxBcastMultiplier
const MaxBcastMultiplier = 2

// enum Snode.Flags
const (
	SnodeNonElectable cos.BitFlags = 1 << iota
	SnodeIC
	NodeFlagMaint
	NodeFlagDecomm
)

const NodeFlagsMaintDecomm = NodeFlagMaint | NodeFlagDecomm

const icGroupSize = 3 // desirable gateway count in the Information Center

type (
	// interface to Get current cluster-map instance
	Sowner interface {
		Get() (smap *Smap)
		Listeners() SmapListeners
	}

	// Snode's networking info
	NetInfo struct {
		Hostname    string `json:"node_ip_addr"`
		Port        string `json:"daemon_port"`
		URL         string `json:"direct_url"`
		tcpEndpoint string
	}

	// Snode - a node (gateway or target) in a cluster
	Snode struct {
		Ext        any        `json:"ext,omitempty"` // within meta-version extensions
		LocalNet   *net.IPNet `json:"-"`
		PubNet     NetInfo    `json:"public_net"`        // cmn.NetPublic
		DataNet    NetInfo    `json:"intra_data_net"`    // cmn.NetIntraData
		ControlNet NetInfo    `json:"intra_control_net"` // cmn.NetIntraControl
		DaeType    string     `json:"daemon_type"`       // "target" or "proxy"
		DaeID      string     `json:"daemon_id"`
		name       string
		Flags      cos.BitFlags `json:"flags"` // enum { SnodeNonElectable, SnodeIC, ... }
		idDigest   uint64
	}
	Nodes   []*Snode          // slice of Snodes
	NodeMap map[string]*Snode // map of Snodes: DaeID => Snodes

	Smap struct {
		Ext          any     `json:"ext,omitempty"`
		Pmap         NodeMap `json:"pmap"` // [pid => Snode]
		Primary      *Snode  `json:"proxy_si"`
		Tmap         NodeMap `json:"tmap"`          // [tid => Snode]
		UUID         string  `json:"uuid"`          // assigned once at creation time and never change
		CreationTime string  `json:"creation_time"` // creation timestamp
		Version      int64   `json:"version,string"`
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

func MaxBcastParallel() int { return sys.NumCPU() * MaxBcastMultiplier }

///////////
// Snode //
///////////

func NewSnode(id, daeType string, publicNet, intraControlNet, intraDataNet NetInfo) (snode *Snode) {
	snode = &Snode{PubNet: publicNet, ControlNet: intraControlNet, DataNet: intraDataNet}
	snode.Init(id, daeType)
	return
}

func (d *Snode) Init(id, daeType string) {
	debug.Assert(d.DaeID == "" && d.DaeType == "")
	debug.Assert(id != "" && daeType != "")
	d.DaeID, d.DaeType = id, daeType
	d.SetName()
	d.Digest()
}

func (d *Snode) Digest() uint64 {
	if d.idDigest == 0 {
		d.idDigest = xxhash.ChecksumString64S(d.ID(), cos.MLCG32)
	}
	return d.idDigest
}

func (d *Snode) ID() string   { return d.DaeID }
func (d *Snode) Type() string { return d.DaeType }

func (d *Snode) Name() string   { return d.name }
func (d *Snode) String() string { return d.Name() }

func (d *Snode) SetName() {
	name := d.StringEx()
	if d.name != "" && d.name != name {
		cos.AssertMsg(false, d.name+" vs. "+name)
	}
	d.name = name
}

const (
	PnamePrefix = "p["
	TnamePrefix = "t["
	SnameSuffix = "]"
)

func Pname(pid string) string { return PnamePrefix + pid + "]" }
func Tname(tid string) string { return TnamePrefix + tid + "]" }

func N2ID(name string) string {
	if len(name) > 2 && (name[:2] == TnamePrefix || name[:2] == PnamePrefix) {
		return name[2 : len(name)-1]
	}
	return name
}

func (d *Snode) StringEx() string {
	if d.IsProxy() {
		return Pname(d.DaeID)
	}
	return Tname(d.DaeID)
}

func (d *Snode) nameNets() string {
	if d.PubNet.URL != d.ControlNet.URL ||
		d.PubNet.URL != d.DataNet.URL {
		return fmt.Sprintf("%s(pub: %s, control: %s, data: %s)", d.Name(),
			d.PubNet.URL, d.ControlNet.URL, d.DataNet.URL)
	}
	return fmt.Sprintf("%s(%s)", d.Name(), d.PubNet.URL)
}

func (d *Snode) URL(network string) string {
	switch network {
	case cmn.NetPublic:
		return d.PubNet.URL
	case cmn.NetIntraControl:
		return d.ControlNet.URL
	case cmn.NetIntraData:
		return d.DataNet.URL
	default:
		cos.Assertf(false, "unknown network %q", network)
		return ""
	}
}

func (d *Snode) Equals(o *Snode) (eq bool) {
	if d == nil || o == nil {
		return
	}
	eq = d.ID() == o.ID()
	debug.Func(func() {
		if !eq {
			return
		}
		eq2 := d.DaeType == o.DaeType && d.PubNet.Equals(o.PubNet) &&
			d.ControlNet.Equals(o.ControlNet) && d.DataNet.Equals(o.DataNet)
		debug.Assertf(eq2, "%s(pub: %s, control: %s, data: %s) != %s(pub: %s, control: %s, data: %s)",
			d.StringEx(), d.PubNet.URL, d.ControlNet.URL, d.DataNet.URL,
			o.StringEx(), o.PubNet.URL, o.ControlNet.URL, o.DataNet.URL)
	})
	return
}

func (d *Snode) Validate() error {
	if d == nil {
		return errors.New("invalid Snode: nil")
	}
	if d.ID() == "" {
		return errors.New("invalid Snode: missing node " + d.nameNets())
	}
	if d.DaeType != apc.Proxy && d.DaeType != apc.Target {
		cos.Assertf(false, "invalid Snode type %q", d.DaeType)
	}
	return nil
}

func (d *Snode) Clone() *Snode {
	var dst Snode
	cos.CopyStruct(&dst, d)
	return &dst
}

func (d *Snode) isDuplicate(n *Snode) error {
	var (
		du = []string{d.PubNet.URL, d.ControlNet.URL, d.DataNet.URL}
		nu = []string{n.PubNet.URL, n.ControlNet.URL, n.DataNet.URL}
	)
	for _, ni := range nu {
		np, err := url.Parse(ni)
		if err != nil {
			return fmt.Errorf("FATAL: failed to parse %s URL %q: %v", n.StringEx(), ni, err)
		}
		for _, di := range du {
			dp, err := url.Parse(di)
			if err != nil {
				return fmt.Errorf("FATAL: failed to parse %s URL %q: %v", d.StringEx(), di, err)
			}
			if np.Host == dp.Host {
				return fmt.Errorf("duplicate IPs: %s and %s share the same %q (hint: node ID changed or lost/renewed?)",
					d.StringEx(), n.StringEx(), np.Host)
			}
			if ni == di {
				return fmt.Errorf("duplicate URLs: %s and %s share the same %q", d.StringEx(), n.StringEx(), ni)
			}
		}
	}
	return nil
}

func (d *Snode) IsProxy() bool  { return d.DaeType == apc.Proxy }
func (d *Snode) IsTarget() bool { return d.DaeType == apc.Target }

// node flags
func (d *Snode) IsAnySet(flags cos.BitFlags) bool { return d.Flags.IsAnySet(flags) }
func (d *Snode) nonElectable() bool               { return d.Flags.IsSet(SnodeNonElectable) }
func (d *Snode) isIC() bool                       { return d.Flags.IsSet(SnodeIC) }

//////////////////////
//	  NetInfo       //
//////////////////////

func NewNetInfo(proto, hostname, port string) *NetInfo {
	tcpEndpoint := fmt.Sprintf("%s:%s", hostname, port)
	return &NetInfo{
		Hostname:    hostname,
		Port:        port,
		URL:         fmt.Sprintf("%s://%s", proto, tcpEndpoint),
		tcpEndpoint: tcpEndpoint,
	}
}

func (ni *NetInfo) TCPEndpoint() string {
	if ni.tcpEndpoint == "" {
		ni.tcpEndpoint = fmt.Sprintf("%s:%s", ni.Hostname, ni.Port)
	}
	return ni.tcpEndpoint
}

func (ni *NetInfo) String() string {
	return ni.TCPEndpoint()
}

func (ni *NetInfo) Equals(other NetInfo) bool {
	return ni.Hostname == other.Hostname && ni.Port == other.Port && ni.URL == other.URL
}

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
	if m.Primary == nil {
		return fmt.Sprintf("Smap v%d[%s, primary=nil, t=%d, p=%d]", m.Version, m.UUID,
			m.CountTargets(), m.CountProxies())
	}
	return fmt.Sprintf("Smap v%d[%s, %s, t=%d, p=%d]", m.Version, m.UUID,
		m.Primary.StringEx(), m.CountTargets(), m.CountProxies())
}

func (m *Smap) CountTargets() int { return len(m.Tmap) }
func (m *Smap) CountProxies() int { return len(m.Pmap) }
func (m *Smap) Count() int        { return len(m.Pmap) + len(m.Tmap) }
func (m *Smap) CountActiveTargets() (count int) {
	for _, t := range m.Tmap {
		if !t.IsAnySet(NodeFlagsMaintDecomm) {
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
		if !t.IsAnySet(NodeFlagsMaintDecomm) {
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
			return nil, cmn.NewErrNotFound("new-tmap: t[%s]", tid)
		}
	}
	tmap = make(NodeMap, len(tids))
	for _, tid := range tids {
		tmap[tid] = m.GetTarget(tid)
	}
	return
}

// (compare w/ GetNodeNotMaint)
func (m *Smap) GetNode(id string) *Snode {
	if node := m.GetTarget(id); node != nil {
		return node
	}
	return m.GetProxy(id)
}

// present and _not_ in maintenance
// (compare w/ PresentInMaint)
func (m *Smap) GetNodeNotMaint(sid string) (si *Snode) {
	si = m.GetNode(sid)
	if si != nil && si.IsAnySet(NodeFlagsMaintDecomm) {
		si = nil
	}
	return
}

func (m *Smap) GetRandTarget() (tsi *Snode, err error) {
	for _, tsi = range m.Tmap {
		if !tsi.IsAnySet(NodeFlagsMaintDecomm) {
			return
		}
	}
	return nil, cmn.NewErrNoNodes(apc.Target)
}

func (m *Smap) GetRandProxy(excludePrimary bool) (si *Snode, err error) {
	var cnt int
	for _, psi := range m.Pmap {
		if psi.IsAnySet(NodeFlagsMaintDecomm) {
			cnt++
			continue
		}
		if !excludePrimary || !m.IsPrimary(psi) {
			return psi, nil
		}
	}
	return nil, fmt.Errorf("failed to find a random proxy (num=%d, in-maintenance=%d, exclude-primary=%t)",
		len(m.Pmap), cnt, excludePrimary)
}

func (m *Smap) IsDuplicate(nsi *Snode) (osi *Snode, err error) {
	for _, tsi := range m.Tmap {
		if tsi.ID() == nsi.ID() {
			continue
		}
		if err = tsi.isDuplicate(nsi); err != nil {
			osi = tsi
			return
		}
	}
	for _, psi := range m.Pmap {
		if psi.ID() == nsi.ID() {
			continue
		}
		if err = psi.isDuplicate(nsi); err != nil {
			osi = psi
			return
		}
	}
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

// true when present and in maintenance (compare w/ GetNodeNotMaint)
func (m *Smap) PresentInMaint(si *Snode) (ok bool) {
	node := m.GetNode(si.ID())
	return node != nil && node.IsAnySet(NodeFlagsMaintDecomm)
}

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

func (*Smap) DefaultICSize() int { return icGroupSize }

/////////////
// NodeMap //
/////////////

func (m NodeMap) Add(snode *Snode) { debug.Assert(m != nil); m[snode.DaeID] = snode }

func (m NodeMap) ActiveMap() (clone NodeMap) {
	clone = make(NodeMap, len(m))
	for id, node := range m {
		if node.IsAnySet(NodeFlagsMaintDecomm) {
			continue
		}
		clone[id] = node
	}
	return
}

func (m NodeMap) ActiveNodes() []*Snode {
	snodes := make([]*Snode, 0, len(m))
	for _, node := range m {
		if node.IsAnySet(NodeFlagsMaintDecomm) {
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
func NodeMapDelta(oldNodeMap, newNodeMap []NodeMap) (added, removed NodeMap) {
	added, removed = make(NodeMap), make(NodeMap)
	for i, mold := range oldNodeMap {
		mnew := newNodeMap[i]
		for id, si := range mnew {
			if _, ok := mold[id]; !ok {
				added[id] = si
			}
		}
	}
	for i, mold := range oldNodeMap {
		mnew := newNodeMap[i]
		for id, si := range mold {
			if _, ok := mnew[id]; !ok {
				removed[id] = si
			}
		}
	}
	return
}
