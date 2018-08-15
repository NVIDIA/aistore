// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/constants"
	"github.com/OneOfOne/xxhash"
)

// ============================= Background ==================================
// Cluster map aka Smap is an immutable and versioned object
// Executing smapowner.get() gives back an immutable version that won't change
// Smap versioning is monotonic and incremental
// Smap uniquely and solely defines the primary proxy
// Smap updating involves the sequence:
//    lock -- clone -- modify the clone -- smapowner.put(clone) -- unlock
// Version check followed by the modification is protected by the same lock
// ============================= Background ==================================

type NetInfo struct {
	NodeIPAddr string `json:"node_ip_addr"`
	DaemonPort string `json:"daemon_port"`
	DirectURL  string `json:"direct_url"`
}

// each DFC daemon - storage eway - is represented by:
type daemonInfo struct {
	DaemonID    string  `json:"daemon_id"`
	PublicNet   NetInfo `json:"public_net"`
	InternalNet NetInfo `json:"internal_net"`
	idDigest    uint64
}

// Smap aka cluster map
type Smap struct {
	Tmap      map[string]*daemonInfo `json:"tmap"` // daemonID -> daemonInfo
	Pmap      map[string]*daemonInfo `json:"pmap"` // proxyID -> proxyInfo
	NonElects simplekvs              `json:"non_electable"`
	ProxySI   *daemonInfo            `json:"proxy_si"`
	Version   int64                  `json:"version"`
}

type smapowner struct {
	sync.Mutex
	smap unsafe.Pointer
}

func newDaemonInfo(id, proto string, publicAddr, internalAddr *net.TCPAddr) *daemonInfo {
	publicNet := NetInfo{
		NodeIPAddr: publicAddr.IP.String(),
		DaemonPort: strconv.Itoa(publicAddr.Port),
		DirectURL:  proto + "://" + publicAddr.String(),
	}
	internalNet := publicNet
	if len(internalAddr.IP) > 0 {
		internalNet = NetInfo{
			NodeIPAddr: internalAddr.IP.String(),
			DaemonPort: strconv.Itoa(internalAddr.Port),
			DirectURL:  proto + "://" + internalAddr.String(),
		}
	}

	return &daemonInfo{
		DaemonID:    id,
		PublicNet:   publicNet,
		InternalNet: internalNet,
		idDigest:    xxhash.ChecksumString64S(id, constants.MLCG32),
	}
}

func (d *daemonInfo) String() string {
	f := "[\n\tDaemonID: %s,\n\tPublicNet: %s,\n\tInternalNet: %s,\n\tidDigest: %d]"
	return fmt.Sprintf(f, d.DaemonID, d.PublicNet.DirectURL, d.InternalNet.DirectURL, d.idDigest)
}

func (a *daemonInfo) Equals(b *daemonInfo) bool {
	return (a.DaemonID == b.DaemonID &&
		reflect.DeepEqual(a.PublicNet, b.PublicNet) &&
		reflect.DeepEqual(a.InternalNet, b.InternalNet))
}

func (r *smapowner) put(smap *Smap) {
	for _, sinfo := range smap.Tmap {
		if sinfo.idDigest == 0 {
			sinfo.idDigest = xxhash.ChecksumString64S(sinfo.DaemonID, constants.MLCG32)
		}
	}
	for _, sinfo := range smap.Pmap {
		if sinfo.idDigest == 0 {
			sinfo.idDigest = xxhash.ChecksumString64S(sinfo.DaemonID, constants.MLCG32)
		}
	}
	atomic.StorePointer(&r.smap, unsafe.Pointer(smap))
}

// the intended and implied usage of this inconspicuous method is CoW:
// - read (shared/replicated bucket-metadata object) freely
// - clone for writing
// - and never-ever modify in place
func (r *smapowner) get() (smap *Smap) {
	smap = (*Smap)(atomic.LoadPointer(&r.smap))
	return
}

func (r *smapowner) synchronize(newsmap *Smap, saveSmap, lesserVersionIsErr bool) (errstr string) {
	if !newsmap.isValid() {
		errstr = fmt.Sprintf("Invalid Smap: %s", newsmap.pp())
		return
	}
	r.Lock()
	smap := r.get()
	if smap != nil {
		myver := smap.version()
		if newsmap.version() <= myver {
			if lesserVersionIsErr && newsmap.version() < myver {
				errstr = fmt.Sprintf("Attempt to downgrade local Smap v%d to v%d", myver, newsmap.version())
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

func (r *smapowner) persist(newsmap *Smap, saveSmap bool) (errstr string) {
	origURL := ctx.config.Proxy.PrimaryURL
	ctx.config.Proxy.PrimaryURL = newsmap.ProxySI.PublicNet.DirectURL
	if err := LocalSave(clivars.conffile, ctx.config); err != nil {
		errstr = fmt.Sprintf("Error writing config file %s, err: %v", clivars.conffile, err)
		ctx.config.Proxy.PrimaryURL = origURL
		return
	}

	if saveSmap {
		smappathname := filepath.Join(ctx.config.Confdir, smapname)
		if err := LocalSave(smappathname, newsmap); err != nil {
			glog.Errorf("Error writing Smap %s, err: %v", smappathname, err)
		}
	}
	return
}

func newSmap() (smap *Smap) {
	smap = &Smap{}
	smap.init(8, 8, 0)
	return
}

func (m *Smap) init(tsize, psize, elsize int) {
	m.Tmap = make(map[string]*daemonInfo, tsize)
	m.Pmap = make(map[string]*daemonInfo, psize)
	if elsize > 0 {
		m.NonElects = make(simplekvs, elsize)
	}
}

func (m *Smap) pp() string {
	s, _ := json.MarshalIndent(m, "", "\t")
	return fmt.Sprintf("Smap v%d:\n%s", m.version(), string(s))
}

func (m *Smap) isValid() bool {
	if m.ProxySI == nil {
		return false
	}
	return m.isPresent(m.ProxySI, true)
}

func (m *Smap) isPrimary(self *daemonInfo) bool {
	if !m.isValid() {
		return false
	}
	return m.ProxySI.DaemonID == self.DaemonID
}

func (m *Smap) isPresent(si *daemonInfo, isproxy bool) bool {
	if isproxy {
		psi := m.getProxy(si.DaemonID)
		return psi != nil
	}
	tsi := m.getTarget(si.DaemonID)
	return tsi != nil
}

func (m *Smap) containsID(id string) bool {
	if tsi := m.getTarget(id); tsi != nil {
		return true
	}
	if psi := m.getProxy(id); psi != nil {
		return true
	}
	return false
}

func (m *Smap) addTarget(tsi *daemonInfo) {
	assert(!m.containsID(tsi.DaemonID), "FATAL: duplicate daemon ID: '"+tsi.DaemonID+"'")
	m.Tmap[tsi.DaemonID] = tsi
	m.Version++
}

func (m *Smap) addProxy(psi *daemonInfo) {
	assert(!m.containsID(psi.DaemonID), "FATAL: duplicate daemon ID: '"+psi.DaemonID+"'")
	m.Pmap[psi.DaemonID] = psi
	m.Version++
}

func (m *Smap) delTarget(sid string) {
	if m.getTarget(sid) == nil {
		assert(false, fmt.Sprintf("FATAL: target: %s is not in the smap: %s", sid, m.pp()))
	}
	delete(m.Tmap, sid)
	m.Version++
}

func (m *Smap) delProxy(pid string) {
	if m.getProxy(pid) == nil {
		assert(false, fmt.Sprintf("FATAL: proxy: %s is not in the smap: %s", pid, m.pp()))
	}
	delete(m.Pmap, pid)
	m.Version++
}

func (m *Smap) countTargets() int {
	return len(m.Tmap)
}

func (m *Smap) countProxies() int {
	return len(m.Pmap)
}

func (m *Smap) getTarget(sid string) *daemonInfo {
	si, ok := m.Tmap[sid]
	if !ok {
		return nil
	}
	return si
}

func (m *Smap) getProxy(pid string) *daemonInfo {
	pi, ok := m.Pmap[pid]
	if !ok {
		return nil
	}
	return pi
}

func (m *Smap) clone() *Smap {
	dst := &Smap{}
	m.deepcopy(dst)
	return dst
}

func (m *Smap) deepcopy(dst *Smap) {
	copyStruct(dst, m)
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

func (m *Smap) merge(dst *Smap) {
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

func (a *Smap) Equals(b *Smap) bool {
	return a.Version == b.Version && a.ProxySI.Equals(b.ProxySI) &&
		reflect.DeepEqual(a.NonElects, b.NonElects) && areDaemonMapsEqual(a.Tmap, b.Tmap) &&
		areDaemonMapsEqual(a.Pmap, b.Pmap)
}

func areDaemonMapsEqual(a, b map[string]*daemonInfo) bool {
	if len(a) != len(b) {
		return false
	}
	for id, aInfo := range a {
		if bInfo, ok := b[id]; !ok {
			return false
		} else if !aInfo.Equals(bInfo) {
			return false
		}
	}
	return true
}

//
// revs interface
//
func (m *Smap) tag() string    { return smaptag }
func (m *Smap) version() int64 { return m.Version }

func (m *Smap) marshal() (b []byte, err error) {
	b, err = json.Marshal(m)
	return
}
