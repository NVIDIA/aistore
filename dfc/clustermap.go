// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dfc

import (
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	jsoniter "github.com/json-iterator/go"
)

// NOTE: to access Snode, Smap and related structures, external
//       packages and HTTP clients must import dfcpub/cluster (and not dfc)

//=====================================================================
//
// - smapX is a server-side extension of the cluster.Smap
// - smapX represents DFC cluster in terms of its nodes and their properties
// - smapX (instance) can be obtained via smapowner.get()
// - smapX is immutable and versioned
// - smapX versioning is monotonic and incremental
// - smapX uniquely and solely defines the current primary proxy in the DFC cluster
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
	if elsize > 0 {
		m.NonElects = make(cmn.SimpleKVs, elsize)
	}
}

func (m *smapX) tag() string                    { return smaptag }
func (m *smapX) version() int64                 { return m.Version }
func (m *smapX) marshal() (b []byte, err error) { return jsonCompat.Marshal(m) } // jsoniter + sorting

func (m *smapX) isValid() bool {
	if m.ProxySI == nil {
		return false
	}
	return m.isPresent(m.ProxySI, true)
}

func (m *smapX) isPrimary(self *cluster.Snode) bool {
	if !m.isValid() {
		return false
	}
	return m.ProxySI.DaemonID == self.DaemonID
}

func (m *smapX) isPresent(si *cluster.Snode, isproxy bool) bool {
	if isproxy {
		psi := m.GetProxy(si.DaemonID)
		return psi != nil
	}
	tsi := m.GetTarget(si.DaemonID)
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
	cmn.Assert(!m.containsID(tsi.DaemonID), "FATAL: duplicate daemon ID: '"+tsi.DaemonID+"'")
	m.Tmap[tsi.DaemonID] = tsi
	m.Version++
}

func (m *smapX) addProxy(psi *cluster.Snode) {
	cmn.Assert(!m.containsID(psi.DaemonID), "FATAL: duplicate daemon ID: '"+psi.DaemonID+"'")
	m.Pmap[psi.DaemonID] = psi
	m.Version++
}

func (m *smapX) delTarget(sid string) {
	if m.GetTarget(sid) == nil {
		cmn.Assert(false, fmt.Sprintf("FATAL: target: %s is not in the smap: %s", sid, m.pp()))
	}
	delete(m.Tmap, sid)
	m.Version++
}

func (m *smapX) delProxy(pid string) {
	if m.GetProxy(pid) == nil {
		cmn.Assert(false, fmt.Sprintf("FATAL: proxy: %s is not in the smap: %s", pid, m.pp()))
	}
	delete(m.Pmap, pid)
	m.Version++
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
	smap      unsafe.Pointer
	listeners *smaplisteners
}

// implements cluster.Sowner
var _ cluster.Sowner = &smapowner{}

func (r *smapowner) Get() *cluster.Smap {
	smapx := (*smapX)(atomic.LoadPointer(&r.smap))
	return &smapx.Smap
}
func (r *smapowner) Listeners() cluster.SmapListeners {
	return r.listeners
}

//
// private to the package
//

func (r *smapowner) put(smap *smapX) {
	for _, snode := range smap.Tmap {
		snode.Digest()
	}
	for _, snode := range smap.Pmap {
		snode.Digest()
	}
	atomic.StorePointer(&r.smap, unsafe.Pointer(smap))

	if r.listeners != nil {
		r.listeners.notify() // notify of Smap change all listeners (cluster.Slistener)
	}
}

func (r *smapowner) get() (smap *smapX) {
	return (*smapX)(atomic.LoadPointer(&r.smap))
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
	config := cmn.GCO.BeginUpdate()
	defer cmn.GCO.CommitUpdate(config)

	origURL := config.Proxy.PrimaryURL
	config.Proxy.PrimaryURL = newsmap.ProxySI.PublicNet.DirectURL
	if err := cmn.LocalSave(clivars.conffile, config); err != nil {
		errstr = fmt.Sprintf("Error writing config file %s, err: %v", clivars.conffile, err)
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
func newSnode(id, proto string, publicAddr, intraControlAddr, intraDataAddr *net.TCPAddr) (snode *cluster.Snode) {
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
	snode = &cluster.Snode{DaemonID: id, PublicNet: publicNet, IntraControlNet: intraControlNet, IntraDataNet: intraDataNet}
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
	listeners []cluster.Slistener
}

func (sls *smaplisteners) Reg(sl cluster.Slistener) {
	sls.Lock()
	l := len(sls.listeners)
	for k := 0; k < l; k++ {
		if sls.listeners[k] == sl {
			cmn.Assert(false, fmt.Sprintf("FATAL: smap-listener %s is already registered", sl))
		}
		if sls.listeners[k].String() == sl.String() {
			glog.Warningf("duplicate smap-listener %s", sl)
		}
	}
	sls.listeners = append(sls.listeners, sl)
	sls.Unlock()
	glog.Infof("registered smap-listener %s", sl)
}

func (sls *smaplisteners) Unreg(sl cluster.Slistener) {
	sls.Lock()
	l := len(sls.listeners)
	for k := 0; k < l; k++ {
		if sls.listeners[k] == sl {
			if k < l-1 {
				copy(sls.listeners[k:], sls.listeners[k+1:])
			}
			sls.listeners[l-1] = nil
			sls.listeners = sls.listeners[:l-1]
			sls.Unlock()
			glog.Infof("unregistered smap-listener %s", sl)
			return
		}
	}
	sls.Unlock()
	cmn.Assert(false, fmt.Sprintf("FATAL: smap-listener %s is not registered", sl))
}

func (sls *smaplisteners) notify() {
	sls.RLock()
	l := len(sls.listeners)
	for k := 0; k < l; k++ {
		sls.listeners[k].SmapChanged()
	}
	sls.RUnlock()
}
