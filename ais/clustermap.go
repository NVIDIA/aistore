// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"sync"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
	jsoniter "github.com/json-iterator/go"
)

// NOTE: to access Snode, Smap and related structures, external
//       packages and HTTP clients must import aistore/cluster (and not ais)

//=====================================================================
//
// - smapX is a server-side extension of the cluster.Smap
// - smapX represents AIStore cluster in terms of its member nodes and their properties
// - smapX (instance) can be obtained via smapOwner.get()
// - smapX is immutable and versioned
// - smapX versioning is monotonic and incremental
// - smapX uniquely and solely defines the current primary proxy in the AIStore cluster
//
// smapX typical update transaction:
// lock -- clone() -- modify the clone -- smapOwner.put(clone) -- unlock
//
// (*) for merges and conflict resolution, check smapX version prior to put()
//     (version check must be protected by the same critical section)
//
//=====================================================================

const smapFname = ".ais.smap" // Smap basename

type smapX struct {
	cluster.Smap
}

var (
	// interface guard
	_ revs = &smapX{}
)

func newSmap() (smap *smapX) {
	smap = &smapX{}
	smap.init(8, 8, 0)
	return
}

func (m *smapX) init(tsize, psize, elsize int) {
	m.Tmap = make(cluster.NodeMap, tsize)
	m.Pmap = make(cluster.NodeMap, psize)
	m.NonElects = make(cmn.SimpleKVs, elsize)
}

func (m *smapX) tag() string    { return revsSmapTag }
func (m *smapX) version() int64 { return m.Version }
func (m *smapX) marshal() (b []byte) {
	b, err := jsonCompat.Marshal(m) // jsoniter + sorting
	cmn.AssertNoErr(err)
	return b
}

func (m *smapX) isValid() bool {
	if m == nil {
		return false
	}
	if m.ProxySI == nil {
		return false
	}
	return m.isPresent(m.ProxySI)
}

func (m *smapX) isPrimary(self *cluster.Snode) bool {
	if !m.isValid() {
		return false
	}
	return m.ProxySI.ID() == self.ID()
}

func (m *smapX) isPresent(si *cluster.Snode) bool {
	if si.IsProxy() {
		psi := m.GetProxy(si.ID())
		return psi != nil
	}
	tsi := m.GetTarget(si.ID())
	return tsi != nil
}

func (m *smapX) containsID(id string) bool {
	if tsi := m.GetTarget(id); tsi != nil {
		return true
	}
	if psi := m.GetProxy(id); psi != nil {
		return true
	}
	return false
}

func (m *smapX) addTarget(tsi *cluster.Snode) {
	if m.containsID(tsi.ID()) {
		cmn.AssertMsg(false, "FATAL: duplicate daemon ID: '"+tsi.ID()+"'")
	}
	m.Tmap[tsi.ID()] = tsi
	m.Version++
}

func (m *smapX) addProxy(psi *cluster.Snode) {
	if m.containsID(psi.ID()) {
		cmn.AssertMsg(false, "FATAL: duplicate daemon ID: '"+psi.ID()+"'")
	}
	m.Pmap[psi.ID()] = psi
	m.Version++
}

func (m *smapX) delTarget(sid string) {
	if m.GetTarget(sid) == nil {
		cmn.AssertMsg(false, fmt.Sprintf("FATAL: target: %s is not in: %s", sid, m.pp()))
	}
	delete(m.Tmap, sid)
	m.Version++
}

func (m *smapX) delProxy(pid string) {
	if m.GetProxy(pid) == nil {
		cmn.AssertMsg(false, fmt.Sprintf("FATAL: proxy: %s is not in: %s", pid, m.pp()))
	}
	delete(m.Pmap, pid)
	delete(m.NonElects, pid)
	m.Version++
}

func (m *smapX) putNode(nsi *cluster.Snode, nonElectable bool) {
	id := nsi.ID()
	if nsi.IsProxy() {
		if m.GetProxy(id) != nil {
			m.delProxy(id)
		}
		m.addProxy(nsi)
		if nonElectable {
			m.NonElects[id] = ""
			glog.Warningf("%s won't be electable", nsi)
		}
		if glog.V(3) {
			glog.Infof("joined %s (num proxies %d)", nsi, m.CountProxies())
		}
	} else {
		cmn.Assert(nsi.IsTarget())
		if m.GetTarget(id) != nil { // ditto
			m.delTarget(id)
		}
		m.addTarget(nsi)
		if glog.V(3) {
			glog.Infof("joined %s (num targets %d)", nsi, m.CountTargets())
		}
	}
}

func (m *smapX) clone() *smapX {
	dst := &smapX{}
	m.deepCopy(dst)
	return dst
}

func (m *smapX) deepCopy(dst *smapX) {
	cmn.CopyStruct(dst, m)
	dst.init(m.CountTargets(), m.CountProxies(), len(m.NonElects))
	for id, v := range m.Tmap {
		dst.Tmap[id] = v
	}
	for id, v := range m.Pmap {
		dst.Pmap[id] = v
	}
	for id, v := range m.NonElects {
		dst.NonElects[id] = v
	}
}

func (m *smapX) merge(dst *smapX, override bool) (added int, err error) {
	var osi *cluster.Snode
	f := func(nsi *cluster.Snode) { // detect duplicate URLs
		if osi, err = dst.IsDuplicateURL(nsi); err != nil {
			glog.Error(err)
			if !override {
				return
			}
			err = nil
			glog.Errorf("Warning: removing (old/obsolete) %s from future Smaps", osi)
			if osi.IsProxy() {
				dst.delProxy(osi.ID())
			} else {
				dst.delTarget(osi.ID())
			}
		}
	}
	for id, si := range m.Tmap {
		f(si)
		if err != nil {
			return
		}
		if _, ok := dst.Tmap[id]; !ok {
			if _, ok = dst.Pmap[id]; !ok {
				dst.Tmap[id] = si
				added++
			}
		}
	}
	for id, si := range m.Pmap {
		f(si)
		if err != nil {
			return
		}
		if _, ok := dst.Pmap[id]; !ok {
			if _, ok = dst.Tmap[id]; !ok {
				dst.Pmap[id] = si
				added++
			}
		}
	}
	if m.UUID != "" && dst.UUID == "" {
		dst.UUID = m.UUID
		dst.CreationTime = m.CreationTime
	}
	return
}

func (m *smapX) validateUUID(newSmap *smapX, si, nsi *cluster.Snode, caller string) (err error) {
	if newSmap == nil || newSmap.Version == 0 {
		return
	}
	if m.UUID == "" || newSmap.UUID == "" {
		return
	}
	if m.UUID == newSmap.UUID {
		return
	}
	nsiname := caller
	if nsi != nil {
		nsiname = nsi.Name()
	} else if nsiname == "" {
		nsiname = "???"
	}
	// FATAL: cluster integrity error (cie)
	s := fmt.Sprintf("%s: Smaps have different uuids: [%s: %s] vs [%s: %s]",
		ciError(50), si, m.StringEx(), nsiname, newSmap.StringEx())
	err = &errSmapUUIDDiffer{s}
	return
}

func (m *smapX) pp() string {
	s, _ := jsoniter.MarshalIndent(m, "", " ")
	return string(s)
}

//=====================================================================
//
// smapOwner
//
//=====================================================================

type smapOwner struct {
	sync.Mutex
	smap      atomic.Pointer
	listeners *smaplisteners
}

// implements cluster.Sowner
var _ cluster.Sowner = &smapOwner{}

func newSmapOwner() *smapOwner {
	return &smapOwner{
		listeners: newSmapListeners(),
	}
}

func (r *smapOwner) load(smap *smapX, config *cmn.Config) error {
	return jsp.Load(filepath.Join(config.Confdir, smapFname), smap, jsp.CCSign())
}

func (r *smapOwner) Get() *cluster.Smap               { return &r.get().Smap }
func (r *smapOwner) Listeners() cluster.SmapListeners { return r.listeners }

//
// private to the package
//

func (r *smapOwner) put(smap *smapX) {
	smap.InitDigests()
	r.smap.Store(unsafe.Pointer(smap))

	if r.listeners != nil {
		r.listeners.notify(smap.version()) // notify of Smap change all listeners (cluster.Slistener)
	}
}

func (r *smapOwner) get() (smap *smapX) {
	return (*smapX)(r.smap.Load())
}

func (r *smapOwner) synchronize(newSmap *smapX, lesserVersionIsErr bool) (err error) {
	if !newSmap.isValid() {
		err = fmt.Errorf("invalid smapX: %s", newSmap.pp())
		return
	}
	r.Lock()
	smap := r.Get()
	if smap != nil {
		curVer, newVer := smap.Version, newSmap.version()
		if newVer <= curVer {
			if lesserVersionIsErr && newVer < curVer {
				err = fmt.Errorf("attempt to downgrade local smapX v%d to v%d", curVer, newVer)
			}
			r.Unlock()
			return
		}
	}
	if err = r.persist(newSmap); err == nil {
		r.put(newSmap)
	}
	r.Unlock()
	return
}

func (r *smapOwner) persist(newSmap *smapX) error {
	confFile := cmn.GCO.GetConfigFile()
	config := cmn.GCO.BeginUpdate()
	defer cmn.GCO.CommitUpdate(config)

	origURL := config.Proxy.PrimaryURL
	config.Proxy.PrimaryURL = newSmap.ProxySI.PublicNet.DirectURL
	if err := jsp.Save(confFile, config, jsp.Plain()); err != nil {
		err = fmt.Errorf("failed writing config file %s, err: %v", confFile, err)
		config.Proxy.PrimaryURL = origURL
		return err
	}
	smapPathName := filepath.Join(config.Confdir, smapFname)
	if err := jsp.Save(smapPathName, newSmap, jsp.CCSign()); err != nil {
		glog.Errorf("error writing Smap to %s: %v", smapPathName, err)
	}
	return nil
}

func (r *smapOwner) modify(lock bool, pre func(clone *smapX) error, post ...func(clone *smapX)) (*smapX, error) {
	if lock {
		r.Lock()
		defer r.Unlock()
	}
	clone := r.get().clone()
	if err := pre(clone); err != nil {
		return nil, err
	}

	if err := r.persist(clone); err != nil {
		glog.Errorf("failed to persist smap: %v", err)
	}
	r.put(clone)
	if len(post) == 1 {
		post[0](clone)
	}
	return clone, nil
}

//=====================================================================
//
// new cluster.Snode
//
//=====================================================================
func newSnode(id, proto, daeType string, publicAddr, intraControlAddr, intraDataAddr *net.TCPAddr) (snode *cluster.Snode) {
	publicNet := cluster.NetInfo{
		NodeIPAddr: publicAddr.IP.String(),
		DaemonPort: strconv.Itoa(publicAddr.Port),
		DirectURL:  proto + "://" + publicAddr.String(),
	}
	intraControlNet := publicNet
	if len(intraControlAddr.IP) > 0 {
		intraControlNet = cluster.NetInfo{
			NodeIPAddr: intraControlAddr.IP.String(),
			DaemonPort: strconv.Itoa(intraControlAddr.Port),
			DirectURL:  proto + "://" + intraControlAddr.String(),
		}
	}
	intraDataNet := publicNet
	if len(intraDataAddr.IP) > 0 {
		intraDataNet = cluster.NetInfo{
			NodeIPAddr: intraDataAddr.IP.String(),
			DaemonPort: strconv.Itoa(intraDataAddr.Port),
			DirectURL:  proto + "://" + intraDataAddr.String(),
		}
	}
	snode = &cluster.Snode{
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

//=====================================================================
//
// smaplisteners: implements cluster.Listeners interface
//
//=====================================================================
var _ cluster.SmapListeners = &smaplisteners{}

type smaplisteners struct {
	sync.RWMutex
	listenersChannels map[cluster.Slistener]chan int64
	listenersNames    map[string]uint
}

func newSmapListeners() *smaplisteners {
	return &smaplisteners{
		listenersChannels: make(map[cluster.Slistener]chan int64),
		listenersNames:    make(map[string]uint),
	}
}

func (sls *smaplisteners) Reg(sl cluster.Slistener) {
	sls.Lock()

	smapVersionCh := make(chan int64, 8)

	if _, ok := sls.listenersChannels[sl]; ok {
		cmn.AssertMsg(false, fmt.Sprintf("FATAL: smap-listener %s is already registered", sl))
	}

	if _, ok := sls.listenersNames[sl.String()]; ok {
		glog.Warningf("duplicate smap-listener %s", sl)
	} else {
		sls.listenersNames[sl.String()] = 0
	}

	sls.listenersChannels[sl] = smapVersionCh
	sls.listenersNames[sl.String()]++

	sls.Unlock()
	glog.Infof("registered smap-listener %s", sl)

	go sl.ListenSmapChanged(smapVersionCh)
}

func (sls *smaplisteners) Unreg(sl cluster.Slistener) {
	sls.Lock()

	if _, ok := sls.listenersChannels[sl]; !ok {
		cmn.AssertMsg(false, fmt.Sprintf("FATAL: smap-listener %s is not registered", sl))
	}

	close(sls.listenersChannels[sl])

	delete(sls.listenersChannels, sl)
	sls.listenersNames[sl.String()]--

	if sls.listenersNames[sl.String()] == 0 {
		delete(sls.listenersNames, sl.String())
	}

	sls.Unlock()
}

func (sls *smaplisteners) notify(newMapVersion int64) {
	sls.RLock()
	for _, ch := range sls.listenersChannels {
		ch <- newMapVersion
	}
	sls.RUnlock()
}
