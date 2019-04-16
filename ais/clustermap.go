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
	jsoniter "github.com/json-iterator/go"
)

// NOTE: to access Snode, Smap and related structures, external
//       packages and HTTP clients must import aistore/cluster (and not ais)

//=====================================================================
//
// - smapX is a server-side extension of the cluster.Smap
// - smapX represents AIStore cluster in terms of its member nodes and their properties
// - smapX (instance) can be obtained via smapowner.get()
// - smapX is immutable and versioned
// - smapX versioning is monotonic and incremental
// - smapX uniquely and solely defines the current primary proxy in the AIStore cluster
//
// smapX typical update transaction:
// lock -- clone() -- modify the clone -- smapowner.put(clone) -- unlock
//
// (*) for merges and conflict resolution, check smapX version prior to put()
//     (version check must be protected by the same critical section)
//
//=====================================================================
type smapX struct {
	cluster.Smap
}

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

func (m *smapX) tag() string                    { return smaptag }
func (m *smapX) version() int64                 { return m.Version }
func (m *smapX) marshal() (b []byte, err error) { return jsonCompat.Marshal(m) } // jsoniter + sorting

func (m *smapX) isValid() bool {
	if m.ProxySI == nil {
		return false
	}
	return m.isPresent(m.ProxySI)
}

func (m *smapX) isPrimary(self *cluster.Snode) bool {
	if !m.isValid() {
		return false
	}
	return m.ProxySI.DaemonID == self.DaemonID
}

func (m *smapX) isPresent(si *cluster.Snode) bool {
	if si.DaemonType == cmn.Proxy {
		psi := m.GetProxy(si.DaemonID)
		return psi != nil
	}
	tsi := m.GetTarget(si.DaemonID)
	return tsi != nil
}

func (m *smapX) printname(id string) string {
	if si := m.GetProxy(id); si != nil {
		return si.Name()
	}
	if si := m.GetTarget(id); si != nil {
		return si.Name()
	}
	return "???[" + id + "]"
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
	if m.containsID(tsi.DaemonID) {
		cmn.AssertMsg(false, "FATAL: duplicate daemon ID: '"+tsi.DaemonID+"'")
	}
	m.Tmap[tsi.DaemonID] = tsi
	m.Version++
}

func (m *smapX) addProxy(psi *cluster.Snode) {
	if m.containsID(psi.DaemonID) {
		cmn.AssertMsg(false, "FATAL: duplicate daemon ID: '"+psi.DaemonID+"'")
	}
	m.Pmap[psi.DaemonID] = psi
	m.Version++
}

func (m *smapX) delTarget(sid string) {
	if m.GetTarget(sid) == nil {
		cmn.AssertMsg(false, fmt.Sprintf("FATAL: target: %s is not in the smap: %s", sid, m.pp()))
	}
	delete(m.Tmap, sid)
	m.Version++
}

func (m *smapX) delProxy(pid string) {
	if m.GetProxy(pid) == nil {
		cmn.AssertMsg(false, fmt.Sprintf("FATAL: proxy: %s is not in the smap: %s", pid, m.pp()))
	}
	delete(m.Pmap, pid)
	delete(m.NonElects, pid)
	m.Version++
}

func (m *smapX) putNode(nsi *cluster.Snode, nonElectable bool) {
	id := nsi.DaemonID
	if nsi.DaemonType == cmn.Proxy {
		if m.GetProxy(id) != nil {
			m.delProxy(id)
		}
		m.addProxy(nsi)
		if nonElectable {
			m.NonElects[id] = ""
			glog.Warningf("%s won't be electable", nsi.Name())
		}
		if glog.V(3) {
			glog.Infof("joined %s (num proxies %d)", nsi.Name(), m.CountProxies())
		}
	} else {
		cmn.Assert(nsi.DaemonType == cmn.Target)
		if m.GetTarget(id) != nil { // ditto
			m.delTarget(id)
		}
		m.addTarget(nsi)
		if glog.V(3) {
			glog.Infof("joined %s (num targets %d)", nsi.Name(), m.CountTargets())
		}
	}
}

func (m *smapX) clone() *smapX {
	dst := &smapX{}
	m.deepcopy(dst)
	return dst
}

func (m *smapX) deepcopy(dst *smapX) {
	cmn.CopyStruct(dst, m)
	dst.init(len(m.Tmap), len(m.Pmap), len(m.NonElects))
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

func (m *smapX) merge(dst *smapX) {
	for id, v := range m.Tmap {
		if _, ok := dst.Tmap[id]; !ok {
			if _, ok = dst.Pmap[id]; !ok {
				dst.Tmap[id] = v
			}
		}
	}
	for id, v := range m.Pmap {
		if _, ok := dst.Pmap[id]; !ok {
			if _, ok = dst.Tmap[id]; !ok {
				dst.Pmap[id] = v
			}
		}
	}
}

func (m *smapX) pp() string {
	s, _ := jsoniter.MarshalIndent(m, "", " ")
	return fmt.Sprintf("smapX v%d:\n%s", m.Version, string(s))
}

//=====================================================================
//
// smapowner
//
//=====================================================================

type smapowner struct {
	sync.Mutex
	smap      atomic.Pointer
	listeners *smaplisteners
}

// implements cluster.Sowner
var _ cluster.Sowner = &smapowner{}

func newSmapowner() *smapowner {
	return &smapowner{
		listeners: newSmapListeners(),
	}
}

func (r *smapowner) Get() *cluster.Smap {
	return &r.get().Smap
}

func (r *smapowner) Listeners() cluster.SmapListeners {
	return r.listeners
}

//
// private to the package
//

func (r *smapowner) put(smap *smapX) {
	smap.InitDigests()
	r.smap.Store(unsafe.Pointer(smap))

	if r.listeners != nil {
		r.listeners.notify(smap.version()) // notify of Smap change all listeners (cluster.Slistener)
	}
}

func (r *smapowner) get() (smap *smapX) {
	return (*smapX)(r.smap.Load())
}

func (r *smapowner) synchronize(newsmap *smapX, saveSmap, lesserVersionIsErr bool) (errstr string) {
	if !newsmap.isValid() {
		errstr = fmt.Sprintf("Invalid smapX: %s", newsmap.pp())
		return
	}
	r.Lock()
	smap := r.Get()
	if smap != nil {
		myver := smap.Version
		if newsmap.version() <= myver {
			if lesserVersionIsErr && newsmap.version() < myver {
				errstr = fmt.Sprintf("Attempt to downgrade local smapX v%d to v%d", myver, newsmap.version())
			}
			r.Unlock()
			return
		}
	}
	if errstr = r.persist(newsmap, saveSmap); errstr == "" {
		r.put(newsmap)
	}
	r.Unlock()
	return
}

func (r *smapowner) persist(newsmap *smapX, saveSmap bool) (errstr string) {
	confFile := cmn.GCO.GetConfigFile()
	config := cmn.GCO.BeginUpdate()
	defer cmn.GCO.CommitUpdate(config)

	origURL := config.Proxy.PrimaryURL
	config.Proxy.PrimaryURL = newsmap.ProxySI.PublicNet.DirectURL
	if err := cmn.LocalSave(confFile, config); err != nil {
		errstr = fmt.Sprintf("Error writing config file %s, err: %v", confFile, err)
		config.Proxy.PrimaryURL = origURL
		return
	}

	if saveSmap {
		smappathname := filepath.Join(config.Confdir, cmn.SmapBackupFile)
		if err := cmn.LocalSave(smappathname, newsmap); err != nil {
			glog.Errorf("Error writing smapX %s, err: %v", smappathname, err)
		}
	}
	return
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
	snode = &cluster.Snode{DaemonID: id, DaemonType: daeType, PublicNet: publicNet, IntraControlNet: intraControlNet, IntraDataNet: intraDataNet}
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
