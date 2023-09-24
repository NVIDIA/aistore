// Package meta: cluster-level metadata
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package meta

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/sys"
	"github.com/OneOfOne/xxhash"
)

// enum Snode.Flags
const (
	SnodeNonElectable cos.BitFlags = 1 << iota
	SnodeIC
	SnodeMaint
	SnodeDecomm
	SnodeMaintPostReb
)

const SnodeMaintDecomm = SnodeMaint | SnodeDecomm

// desirable gateway count in the Information Center (IC)
const DfltCountIC = 3

type (
	// interface to Get current (immutable, versioned) cluster map (Smap) instance
	Sowner interface {
		Get() (smap *Smap)
		Listeners() SmapListeners
	}
	// Smap On-change listeners (see ais/clustermap.go for impl-s)
	Slistener interface {
		String() string
		ListenSmapChanged()
	}
	SmapListeners interface {
		Reg(sl Slistener)
		Unreg(sl Slistener)
	}
)

type (
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
	NodeMap map[string]*Snode // map of Snodes indexed by node ID (Pmap & Tmap below)

	// cluster map
	Smap struct {
		Ext          any     `json:"ext,omitempty"`
		Pmap         NodeMap `json:"pmap"` // [pid => Snode]
		Primary      *Snode  `json:"proxy_si"`
		Tmap         NodeMap `json:"tmap"`          // [tid => Snode]
		UUID         string  `json:"uuid"`          // assigned once at creation time and never change
		CreationTime string  `json:"creation_time"` // creation timestamp
		Version      int64   `json:"version,string"`
	}
)

// number of broadcasting goroutines <= cmn.NumCPU() * maxBcastMultiplier
const maxBcastMultiplier = 2

func MaxBcastParallel() int { return sys.NumCPU() * maxBcastMultiplier }

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
	d.setDigest()
}

func (d *Snode) Digest() uint64 { return d.idDigest }

func (d *Snode) setDigest() {
	if d.idDigest == 0 {
		d.idDigest = xxhash.Checksum64S(cos.UnsafeB(d.ID()), cos.MLCG32)
	}
}

func (d *Snode) ID() string   { return d.DaeID }
func (d *Snode) Type() string { return d.DaeType } // enum { apc.Proxy, apc.Target }

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
		name := d.StringEx()
		debug.Assertf(d.DaeType == o.DaeType, "%s: node type %q vs %q", name, d.DaeType, o.DaeType)
		// generally, expecting network equality with local reconfig (and re-join) considered
		// legit
		if !d.PubNet.eq(&o.PubNet) {
			nlog.Errorf("Warning %s: pub %s vs %s", name, d.PubNet.TCPEndpoint(), o.PubNet.TCPEndpoint())
		}
		if !d.ControlNet.eq(&o.ControlNet) {
			nlog.Errorf("Warning %s: control %s vs %s", name, d.ControlNet.TCPEndpoint(), o.ControlNet.TCPEndpoint())
		}
		if !d.DataNet.eq(&o.DataNet) {
			nlog.Errorf("Warning %s: data %s vs %s", name, d.DataNet.TCPEndpoint(), o.DataNet.TCPEndpoint())
		}
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

func (d *Snode) isDupNet(n *Snode, smap *Smap) error {
	var (
		du = []string{d.PubNet.URL, d.ControlNet.URL, d.DataNet.URL}
		nu = []string{n.PubNet.URL, n.ControlNet.URL, n.DataNet.URL}
	)
	for _, ni := range nu {
		np, err := url.Parse(ni)
		if err != nil {
			return fmt.Errorf("%s %s: failed to parse %s URL %q: %v",
				cmn.BadSmapPrefix, smap, n.StringEx(), ni, err)
		}
		for _, di := range du {
			dp, err := url.Parse(di)
			if err != nil {
				return fmt.Errorf("%s %s: failed to parse %s URL %q: %v",
					cmn.BadSmapPrefix, smap, d.StringEx(), di, err)
			}
			if np.Host == dp.Host {
				return fmt.Errorf("duplicate IPs: %s and %s share the same %q, %s",
					d.StringEx(), n.StringEx(), np.Host, smap.StringEx())
			}
			if ni == di {
				return fmt.Errorf("duplicate URLs: %s and %s share the same %q, %s",
					d.StringEx(), n.StringEx(), ni, smap.StringEx())
			}
		}
	}
	return nil
}

func (d *Snode) IsProxy() bool  { return d.DaeType == apc.Proxy }
func (d *Snode) IsTarget() bool { return d.DaeType == apc.Target }

// node flags
func (d *Snode) InMaintOrDecomm() bool { return d.Flags.IsAnySet(SnodeMaintDecomm) }
func (d *Snode) InMaint() bool         { return d.Flags.IsAnySet(SnodeMaint) }
func (d *Snode) InMaintPostReb() bool {
	return d.Flags.IsSet(SnodeMaint) && d.Flags.IsSet(SnodeMaintPostReb)
}
func (d *Snode) nonElectable() bool { return d.Flags.IsSet(SnodeNonElectable) }
func (d *Snode) IsIC() bool         { return d.Flags.IsSet(SnodeIC) }

func (d *Snode) Fl2S() string {
	if d.Flags == 0 {
		return "none"
	}
	var a = make([]string, 0, 2)
	switch {
	case d.Flags&SnodeNonElectable != 0:
		a = append(a, "non-elect")
	case d.Flags&SnodeIC != 0:
		a = append(a, "ic")
	case d.Flags&SnodeMaint != 0:
		a = append(a, "maintenance-mode")
	case d.Flags&SnodeDecomm != 0:
		a = append(a, "decommission")
	case d.Flags&SnodeMaintPostReb != 0:
		a = append(a, "post-rebalance")
	}
	return strings.Join(a, ",")
}

/////////////
// NetInfo //
/////////////

func _ep(hostname, port string) string { return hostname + ":" + port }

func NewNetInfo(proto, hostname, port string) *NetInfo {
	ep := _ep(hostname, port)
	return &NetInfo{
		Hostname:    hostname,
		Port:        port,
		URL:         fmt.Sprintf("%s://%s", proto, ep),
		tcpEndpoint: ep,
	}
}

func (ni *NetInfo) TCPEndpoint() string {
	if ni.tcpEndpoint == "" {
		ni.tcpEndpoint = _ep(ni.Hostname, ni.Port)
	}
	return ni.tcpEndpoint
}

func (ni *NetInfo) String() string {
	return ni.TCPEndpoint()
}

func (ni *NetInfo) eq(o *NetInfo) bool {
	return ni.Port == o.Port && ni.Hostname == o.Hostname
}

//////////
// Smap //
//////////

// Cluster map (aks Smap) is a versioned, protected and replicated object
// Smap versioning is monotonic and incremental

func (m *Smap) InitDigests() {
	for _, node := range m.Tmap {
		node.setDigest()
	}
	for _, node := range m.Pmap {
		node.setDigest()
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
		return fmt.Sprintf("Smap v%d[%s, nil]", m.Version, m.UUID)
	}
	return fmt.Sprintf("Smap v%d[%s, %s, t=%d, p=%d]", m.Version, m.UUID,
		m.Primary.StringEx(), m.CountTargets(), m.CountProxies())
}

func (m *Smap) CountTargets() int { return len(m.Tmap) }
func (m *Smap) CountProxies() int { return len(m.Pmap) }
func (m *Smap) Count() int        { return len(m.Pmap) + len(m.Tmap) }

func (m *Smap) CountActiveTs() (count int) {
	for _, t := range m.Tmap {
		if !t.InMaintOrDecomm() {
			count++
		}
	}
	return
}

// whether this target has active peers
func (m *Smap) HasActiveTs(except string) bool {
	for tid, t := range m.Tmap {
		if tid == except || t.InMaintOrDecomm() {
			continue
		}
		return true
	}
	return false
}

func (m *Smap) CountActivePs() (count int) {
	for _, p := range m.Pmap {
		if !p.InMaintOrDecomm() {
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
	return m.Primary != nil && m.Primary.ID() == si.ID()
}

func (m *Smap) NewTmap(tids []string) (tmap NodeMap, err error) {
	for _, tid := range tids {
		if m.GetTarget(tid) == nil {
			return nil, cos.NewErrNotFound("new-tmap: t[%s]", tid)
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

// (convenient, slightly redundant)
func (m *Smap) GetActiveNode(sid string) (si *Snode) {
	si = m.GetNode(sid)
	if si != nil && si.InMaintOrDecomm() {
		si = nil
	}
	return
}

// (random active)
func (m *Smap) GetRandTarget() (tsi *Snode, err error) {
	var cnt int
	for _, tsi = range m.Tmap {
		if !tsi.InMaintOrDecomm() {
			return
		}
		cnt++
	}
	err = fmt.Errorf("GetRandTarget failure: %s, in maintenance >= %d", m.StringEx(), cnt)
	return
}

func (m *Smap) GetRandProxy(excludePrimary bool) (si *Snode, err error) {
	var cnt int
	for _, psi := range m.Pmap {
		if psi.InMaintOrDecomm() {
			cnt++
			continue
		}
		if !excludePrimary || !m.IsPrimary(psi) {
			return psi, nil
		}
	}
	err = fmt.Errorf("GetRandProxy failure: %s, in maintenance >= %d, excl-primary %t", m.StringEx(), cnt, excludePrimary)
	return
}

// whether IP is in use by a different node
func (m *Smap) IsDupNet(nsi *Snode) (osi *Snode, err error) {
	for _, tsi := range m.Tmap {
		if tsi.ID() == nsi.ID() {
			continue
		}
		if err = tsi.isDupNet(nsi, m); err != nil {
			osi = tsi
			return
		}
	}
	for _, psi := range m.Pmap {
		if psi.ID() == nsi.ID() {
			continue
		}
		if err = psi.isDupNet(nsi, m); err != nil {
			osi = psi
			return
		}
	}
	return
}

func (m *Smap) Compare(other *Smap) (uuid string, sameOrigin, sameVersion, eq bool) {
	sameOrigin, sameVersion = true, true
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
		return // eq == false
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

// given Snode, check (usually, the current) Smap that it is present _and_ InMaintOrDecomm
// (see also GetActiveNode)
func (m *Smap) InMaintOrDecomm(si *Snode) bool {
	node := m.GetNode(si.ID())
	return node != nil && node.InMaintOrDecomm()
}

func (m *Smap) InMaint(si *Snode) bool {
	node := m.GetNode(si.ID())
	return node != nil && node.InMaint()
}

func (m *Smap) IsIC(psi *Snode) (ok bool) {
	node := m.GetProxy(psi.ID())
	return node != nil && node.IsIC()
}

func (m *Smap) StrIC(node *Snode) string {
	all := make([]string, 0, DfltCountIC)
	for pid, psi := range m.Pmap {
		if !psi.IsIC() {
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
		if psi.IsIC() {
			count++
		}
	}
	return count
}

// checking pub net only
func (m *Smap) PubNet2Node(hostport string) *Snode {
	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		return nil
	}
	all := []NodeMap{m.Tmap, m.Pmap}
	for _, mm := range all {
		for _, si := range mm {
			if si.PubNet.Hostname == host && si.PubNet.Port == port {
				return si
			}
		}
	}
	return nil
}

/////////////
// NodeMap //
/////////////

func (m NodeMap) Add(snode *Snode) { debug.Assert(m != nil); m[snode.DaeID] = snode }

func (m NodeMap) ActiveMap() (clone NodeMap) {
	clone = make(NodeMap, len(m))
	for id, node := range m {
		if node.InMaintOrDecomm() {
			continue
		}
		clone[id] = node
	}
	return
}

func (m NodeMap) ActiveNodes() []*Snode {
	snodes := make([]*Snode, 0, len(m))
	for _, node := range m {
		if node.InMaintOrDecomm() {
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

//
// mem-pool of Nodes (slices)
//

var nodesPool sync.Pool

func AllocNodes(capacity int) (nodes Nodes) {
	if v := nodesPool.Get(); v != nil {
		pnodes := v.(*Nodes)
		nodes = *pnodes
		debug.Assert(nodes != nil && len(nodes) == 0)
	} else {
		debug.Assert(capacity > 0)
		nodes = make(Nodes, 0, capacity)
	}
	return
}

func FreeNodes(nodes Nodes) {
	nodes = nodes[:0]
	nodesPool.Put(&nodes)
}
