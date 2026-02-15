// Package meta: cluster-level metadata
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package meta

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"

	onexxh "github.com/OneOfOne/xxhash"
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
	// swagger:model
	NetInfo struct {
		Hostname    string `json:"node_ip_addr"`
		Port        string `json:"daemon_port"`
		URL         string `json:"direct_url"`
		tcpEndpoint string
	}
	errNetInfoChanged struct {
		sname    string
		tag      string
		oep, nep string
	}

	// Snode - a node (gateway or target) in a cluster
	// swagger:model
	Snode struct {
		nmr        NetNamer
		LocalNet   *net.IPNet `json:"-"`
		PubNet     NetInfo    `json:"public_net"`        // cmn.NetPublic
		DataNet    NetInfo    `json:"intra_data_net"`    // cmn.NetIntraData
		ControlNet NetInfo    `json:"intra_control_net"` // cmn.NetIntraControl
		DaeType    string     `json:"daemon_type"`       // apc.Proxy | apc.Target
		DaeID      string     `json:"daemon_id"`
		name       string
		PubExtra   []NetInfo    `json:"pub_extra,omitempty"`
		Flags      cos.BitFlags `json:"flags"` // enum { SnodeNonElectable, SnodeIC, ... }
		IDDigest   uint64       `json:"id_digest"`
	}

	Nodes   []*Snode          // slice of Snodes
	NodeMap map[string]*Snode // map of Snodes indexed by node ID (Pmap & Tmap below)

	// cluster map
	Smap struct {
		Ext          any     `json:"ext,omitempty"`
		Pmap         NodeMap `json:"pmap"` // [pid => Snode]
		Primary      *Snode  `json:"proxy_si"`
		Tmap         NodeMap `json:"tmap"`          // [tid => Snode]
		UUID         string  `json:"uuid"`          // is assigned once at creation time, never changes
		CreationTime string  `json:"creation_time"` // creation timestamp
		Version      int64   `json:"version,string"`
	}
)

///////////
// Snode //
///////////

// init self
func (d *Snode) Init(id, daeType string) {
	debug.Assert(d.DaeID == "" && d.DaeType == "")
	debug.Assert(id != "" && daeType != "")
	d.DaeID, d.DaeType = id, daeType
	d.SetName()
	d.setDigest()
}

func (d *Snode) digest() uint64 { return d.IDDigest }

func (d *Snode) setDigest() {
	if d.IDDigest == 0 {
		d.IDDigest = onexxh.Checksum64S(cos.UnsafeB(d.ID()), cos.MLCG32)
	}
}

func (d *Snode) ID() string   { return d.DaeID }
func (d *Snode) Type() string { return d.DaeType } // enum { apc.Proxy, apc.Target }

func (d *Snode) Name() string   { return d.name }
func (d *Snode) String() string { return d.name }

func (d *Snode) SetName() {
	name := d.StringEx()
	debug.Assert(d.name == "" || d.name == name, name, d.name)
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

func (d *Snode) StrURLs() string {
	if d.PubNet.URL != d.ControlNet.URL ||
		d.PubNet.URL != d.DataNet.URL {
		return fmt.Sprintf("%s(pub: %s, control: %s, data: %s)", d.Name(),
			d.PubNet.URL, d.ControlNet.URL, d.DataNet.URL)
	}
	return fmt.Sprintf("%s(%s)", d.Name(), d.PubNet.URL)
}

func (d *Snode) URL(network string) (u string) {
	switch network {
	case cmn.NetPublic:
		u = d.PubNet.URL
	case cmn.NetIntraControl:
		u = d.ControlNet.URL
	case cmn.NetIntraData:
		u = d.DataNet.URL
	default: // (exclusively via HrwMultiHome)
		debug.Assert(strings.Contains(network, "://"), network, " node: ", d.String()) // "is URI" per rfc2396.txt
		u = network
	}
	return u
}

func (d *Snode) Eq(o *Snode) (eq bool) {
	if d == nil || o == nil {
		return
	}
	eq = d.ID() == o.ID()
	if eq {
		if err := d.NetEq(o); err != nil {
			nlog.Warningln(err)
			eq = false
		}
	}
	return eq
}

func (d *Snode) NetEq(o *Snode) error {
	name := d.StringEx()
	debug.Assertf(d.DaeType == o.DaeType, "%s: node type %q vs %q", name, d.DaeType, o.DaeType)
	if !d.PubNet.eq(&o.PubNet) {
		return &errNetInfoChanged{name, "pub", d.PubNet.TCPEndpoint(), o.PubNet.TCPEndpoint()}
	}
	if !d.ControlNet.eq(&o.ControlNet) {
		return &errNetInfoChanged{name, "control", d.ControlNet.TCPEndpoint(), o.ControlNet.TCPEndpoint()}
	}
	if !d.DataNet.eq(&o.DataNet) {
		return &errNetInfoChanged{name, "data", d.DataNet.TCPEndpoint(), o.DataNet.TCPEndpoint()}
	}
	return nil
}

func (d *Snode) Validate() error {
	if d == nil {
		return errors.New("invalid Snode: nil")
	}
	if d.ID() == "" {
		return errors.New("invalid Snode: missing node " + d.StrURLs())
	}
	if d.DaeType != apc.Proxy && d.DaeType != apc.Target {
		return fmt.Errorf("invalid Snode type %q", d.DaeType)
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

// NOTE: used only for starting-up proxies and assumes that proxy's listening on a single NIC (no multihoming)
func (d *Snode) HasURL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		nlog.Errorf("failed to parse raw URL %q: %v", rawURL, err)
		return false
	}

	var (
		host, port         = u.Hostname(), u.Port()
		nis                = []NetInfo{d.PubNet, d.ControlNet, d.DataNet}
		sameHost, samePort bool
	)
	for _, ni := range nis {
		if ni.Hostname == host && ni.Port == port {
			return true
		}
		if ni.Hostname == host {
			sameHost = true
		}
		if ni.Port == port {
			samePort = true
		}
	}
	if sameHost && samePort {
		nlog.Warningln("assuming that", d.StrURLs(), "\"contains\"", rawURL)
		return true
	}

	rips, err := _resolveAll(host)
	if err != nil {
		nlog.Warningln(host, err)
		return false
	}

	for _, ni := range nis {
		if ni.Port != port {
			continue
		}
		nips, err := _resolveAll(ni.Hostname)
		if err != nil {
			nlog.Warningln(ni.Hostname, err)
			continue // try other ni's; don't fail the entire check
		}
		for _, rip := range rips {
			if slices.ContainsFunc(nips, rip.Equal) {
				return true
			}
		}
	}

	return false
}

// Slow path: canonicalize by resolving to IP sets (order-independent).
// - rawURL may contain a hostname (e.g. "localhost", runner hostname, DNS name)
// - Snode URLs may contain IP literals
// - resolver ordering differs across environments (e.g., localhost => ::1, 127.0.0.1)
func _resolveAll(h string) ([]net.IP, error) {
	if ip := net.ParseIP(h); ip != nil {
		// Keep as-is: could be v4 or v6 (including bracket-stripped already by url.Parse)
		return []net.IP{ip}, nil
	}

	timeout := max(time.Second, cmn.Rom.CplaneOperation())
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, h)
	if err != nil {
		return nil, err
	}
	ips := make([]net.IP, 0, len(addrs))
	for _, a := range addrs {
		ip := a.IP
		if cmn.IsDialableHostIP(ip) {
			ips = append(ips, ip)
		}
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("no dialable IPs for %q (have %v)", h, addrs)
	}
	return ips, nil
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

func (e *errNetInfoChanged) Error() string {
	return fmt.Sprintf("%s: %s %s vs %s", e.sname, e.tag, e.nep, e.oep)
}

func (ni *NetInfo) Init(proto, hostname, port string) {
	ep := cmn.HostPort(hostname, port)
	ni.Hostname = hostname
	ni.Port = port

	ni.URL = cmn.ProtoEndpoint(proto, ep)
	ni.tcpEndpoint = ep
}

func (ni *NetInfo) TCPEndpoint() string {
	if ni.tcpEndpoint == "" {
		ni.tcpEndpoint = cmn.HostPort(ni.Hostname, ni.Port)
	}
	return ni.tcpEndpoint
}

func (ni *NetInfo) String() string {
	return ni.TCPEndpoint()
}

func (ni *NetInfo) IsEmpty() bool {
	return ni.Hostname == "" && ni.Port == ""
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

	var (
		sb cos.SB
		l  = 80
	)
	sb.Init(l)
	sb.WriteString("Smap v")
	sb.WriteString(strconv.FormatInt(m.Version, 10))
	sb.WriteUint8('[')
	sb.WriteString(m.UUID)
	if m.Primary == nil {
		sb.WriteString(", nil]")
		return sb.String()
	}
	sb.WriteString(", ")
	sb.WriteString(m.Primary.StringEx())
	sb.WriteString(", t=")
	_counts(&sb, m.CountTargets(), m.CountActiveTs())
	sb.WriteString(", p=")
	_counts(&sb, m.CountProxies(), m.CountActivePs())
	sb.WriteUint8(']')

	return sb.String()
}

func _counts(sb *cos.SB, all, active int) {
	if all == active {
		sb.WriteString(strconv.Itoa(all))
	} else {
		sb.WriteUint8('(')
		sb.WriteString(strconv.Itoa(active))
		sb.WriteUint8('/')
		sb.WriteString(strconv.Itoa(all))
		sb.WriteUint8(')')
	}
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

func (m *Smap) SameTargets(other *Smap) bool {
	for tid, t := range m.Tmap {
		if t.InMaintOrDecomm() {
			continue
		}
		if !other.Tmap.Contains(tid) {
			return false
		}
	}
	for tid, t := range other.Tmap {
		if t.InMaintOrDecomm() {
			continue
		}
		if !m.Tmap.Contains(tid) {
			return false
		}
	}
	return true
}

func (m *Smap) HasPeersToRebalance(except string) bool {
	for tid, t := range m.Tmap {
		if tid == except {
			continue
		}
		if !t.InMaintOrDecomm() {
			return true
		}
		// is a "peer" if still transitioning to post-rebalance state
		if !t.Flags.IsSet(SnodeMaintPostReb) {
			return true
		}
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
			return nil, cos.NewErrNotFound(nil, "new-tmap: target "+tid)
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
	if m.Primary == nil || other.Primary == nil || !m.Primary.Eq(other.Primary) {
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
func (m *Smap) InMaintOrDecomm(sid string) bool {
	node := m.GetNode(sid)
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

func (m *Smap) ICCount() (count int) {
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

func (m NodeMap) IDs() []string {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	return ids
}

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
		} else if !anode.Eq(bnode) {
			return false
		}
	}
	return true
}

//
// mem-pool of Nodes (slices)
//

var nodesPool = sync.Pool{
	New: func() any { return new(Nodes) },
}

// note: ec is the only user
func AllocNodes(capacity int) (nodes Nodes) {
	p := nodesPool.Get().(*Nodes)
	nodes = *p
	if cap(nodes) < capacity {
		nodes = make(Nodes, 0, capacity)
	} else {
		nodes = nodes[:0]
	}
	return
}

func FreeNodes(nodes Nodes) {
	nodes = nodes[:0]
	nodesPool.Put(&nodes)
}
