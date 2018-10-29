// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
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
	"github.com/NVIDIA/dfcpub/common"
	jsoniter "github.com/json-iterator/go"
)

// NOTE: to access Snode, Smap and related structures, external (to dfc)
//       packages and HTTP clients must import dfcpub/cluster;
//       direct import of the github.com/NVIDIA/dfcpub/dfc is permitted
//       for testing only

//=====================================================================
//
// SmapX: server-side extension of the cluster.Smap
// SmapX is an immutable and versioned object
// Executing Sowner.Get() gives an immutable version that won't change
// SmapX versioning is monotonic and incremental
// SmapX uniquely and solely defines the primary proxy
// SmapX updating involves the sequence:
//    lock -- clone -- modify the clone -- smapowner.put(clone) -- unlock
// Version check followed by the modification is protected by the same lock
//
//=====================================================================
type SmapX struct {
	cluster.Smap
}

func newSmap() (smap *SmapX) {
	smap = &SmapX{}
	smap.init(8, 8, 0)
	return
}

func (m *SmapX) init(tsize, psize, elsize int) {
	m.Tmap = make(map[string]*cluster.Snode, tsize)
	m.Pmap = make(map[string]*cluster.Snode, psize)
	if elsize > 0 {
		m.NonElects = make(common.SimpleKVs, elsize)
	}
}

func (m *SmapX) tag() string                    { return smaptag }
func (m *SmapX) version() int64                 { return m.Version }
func (m *SmapX) marshal() (b []byte, err error) { return jsonCompat.Marshal(m) } // jsoniter + sorting

func (m *SmapX) isValid() bool {
	if m.ProxySI == nil {
		return false
	}
	return m.isPresent(m.ProxySI, true)
}

func (m *SmapX) isPrimary(self *cluster.Snode) bool {
	if !m.isValid() {
		return false
	}
	return m.ProxySI.DaemonID == self.DaemonID
}

func (m *SmapX) isPresent(si *cluster.Snode, isproxy bool) bool {
	if isproxy {
		psi := m.GetProxy(si.DaemonID)
		return psi != nil
	}
	tsi := m.GetTarget(si.DaemonID)
	return tsi != nil
}

func (m *SmapX) containsID(id string) bool {
	if tsi := m.GetTarget(id); tsi != nil {
		return true
	}
	if psi := m.GetProxy(id); psi != nil {
		return true
	}
	return false
}

func (m *SmapX) addTarget(tsi *cluster.Snode) {
	common.Assert(!m.containsID(tsi.DaemonID), "FATAL: duplicate daemon ID: '"+tsi.DaemonID+"'")
	m.Tmap[tsi.DaemonID] = tsi
	m.Version++
}

func (m *SmapX) addProxy(psi *cluster.Snode) {
	common.Assert(!m.containsID(psi.DaemonID), "FATAL: duplicate daemon ID: '"+psi.DaemonID+"'")
	m.Pmap[psi.DaemonID] = psi
	m.Version++
}

func (m *SmapX) delTarget(sid string) {
	if m.GetTarget(sid) == nil {
		common.Assert(false, fmt.Sprintf("FATAL: target: %s is not in the smap: %s", sid, m.pp()))
	}
	delete(m.Tmap, sid)
	m.Version++
}

func (m *SmapX) delProxy(pid string) {
	if m.GetProxy(pid) == nil {
		common.Assert(false, fmt.Sprintf("FATAL: proxy: %s is not in the smap: %s", pid, m.pp()))
	}
	delete(m.Pmap, pid)
	m.Version++
}

func (m *SmapX) clone() *SmapX {
	dst := &SmapX{}
	m.deepcopy(dst)
	return dst
}

func (m *SmapX) deepcopy(dst *SmapX) {
	common.CopyStruct(dst, m)
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

func (m *SmapX) merge(dst *SmapX) {
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

func (m *SmapX) pp() string {
	s, _ := jsoniter.MarshalIndent(m, "", " ")
	return fmt.Sprintf("SmapX v%d:\n%s", m.Version, string(s))
}

//=====================================================================
//
// smapowner: implements cluster.Sowner
//
//=====================================================================
type smapowner struct {
	sync.Mutex
	smap unsafe.Pointer
}

func (r *smapowner) put(smap *SmapX) {
	for _, snode := range smap.Tmap {
		snode.Digest()
	}
	for _, snode := range smap.Pmap {
		snode.Digest()
	}
	atomic.StorePointer(&r.smap, unsafe.Pointer(smap))
}

func (r *smapowner) Get() (smap *cluster.Smap) {
	smapx := (*SmapX)(atomic.LoadPointer(&r.smap))
	return &smapx.Smap
}
func (r *smapowner) get() (smap *SmapX) {
	return (*SmapX)(atomic.LoadPointer(&r.smap))
}

func (r *smapowner) synchronize(newsmap *SmapX, saveSmap, lesserVersionIsErr bool) (errstr string) {
	if !newsmap.isValid() {
		errstr = fmt.Sprintf("Invalid SmapX: %s", newsmap.pp())
		return
	}
	r.Lock()
	smap := r.Get()
	if smap != nil {
		myver := smap.Version
		if newsmap.version() <= myver {
			if lesserVersionIsErr && newsmap.version() < myver {
				errstr = fmt.Sprintf("Attempt to downgrade local SmapX v%d to v%d", myver, newsmap.version())
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

func (r *smapowner) persist(newsmap *SmapX, saveSmap bool) (errstr string) {
	origURL := ctx.config.Proxy.PrimaryURL
	ctx.config.Proxy.PrimaryURL = newsmap.ProxySI.PublicNet.DirectURL
	if err := common.LocalSave(clivars.conffile, ctx.config); err != nil {
		errstr = fmt.Sprintf("Error writing config file %s, err: %v", clivars.conffile, err)
		ctx.config.Proxy.PrimaryURL = origURL
		return
	}

	if saveSmap {
		smappathname := filepath.Join(ctx.config.Confdir, smapname)
		if err := common.LocalSave(smappathname, newsmap); err != nil {
			glog.Errorf("Error writing SmapX %s, err: %v", smappathname, err)
		}
	}
	return
}

//=====================================================================
//
// new cluster.Snode
//
//=====================================================================
func newSnode(id, proto string, publicAddr, internalAddr, replAddr *net.TCPAddr) (snode *cluster.Snode) {
	publicNet := cluster.NetInfo{
		NodeIPAddr: publicAddr.IP.String(),
		DaemonPort: strconv.Itoa(publicAddr.Port),
		DirectURL:  proto + "://" + publicAddr.String(),
	}
	internalNet := publicNet
	if len(internalAddr.IP) > 0 {
		internalNet = cluster.NetInfo{
			NodeIPAddr: internalAddr.IP.String(),
			DaemonPort: strconv.Itoa(internalAddr.Port),
			DirectURL:  proto + "://" + internalAddr.String(),
		}
	}
	replNet := publicNet
	if len(replAddr.IP) > 0 {
		replNet = cluster.NetInfo{
			NodeIPAddr: replAddr.IP.String(),
			DaemonPort: strconv.Itoa(replAddr.Port),
			DirectURL:  proto + "://" + replAddr.String(),
		}
	}
	snode = &cluster.Snode{DaemonID: id, PublicNet: publicNet, InternalNet: internalNet, ReplNet: replNet}
	snode.Digest()
	return
}
