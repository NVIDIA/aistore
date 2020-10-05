// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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

const (
	ICGroupSize = 3 // desirable gateway count in the Information Center
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

type (
	smapX struct {
		cluster.Smap
	}
	smapOwner struct {
		sync.Mutex
		smap      atomic.Pointer
		listeners *smapLis
	}
	smapLis struct {
		sync.RWMutex
		listeners map[string]cluster.Slistener
		postCh    chan int64
		wg        sync.WaitGroup
		running   atomic.Bool
	}
	smapModifier struct {
		pre          func(ctx *smapModifier, clone *smapX) error
		post         func(ctx *smapModifier, clone *smapX)
		smap         *smapX
		msg          *cmn.ActionMsg
		nsi          *cluster.Snode
		nid          string
		sid          string
		stage        int64
		status       int
		nonElectable bool
		exists       bool
	}
)

// interface guard
var (
	_ revs                  = &smapX{}
	_ cluster.Sowner        = &smapOwner{}
	_ cluster.SmapListeners = &smapLis{}
)

//
// constructors
//
func newSmap() (smap *smapX) {
	smap = &smapX{}
	smap.init(8, 8, 8, 2)
	return
}

func newSnode(id, proto, daeType string, publicAddr, intraControlAddr,
	intraDataAddr *net.TCPAddr) (snode *cluster.Snode) {
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

///////////
// smapX //
///////////

func (m *smapX) init(tsize, psize, elsize, msize int) {
	m.Tmap = make(cluster.NodeMap, tsize)
	m.Pmap = make(cluster.NodeMap, psize)
	m.NonElects = make(cmn.SimpleKVs, elsize)
	m.IC = make(cmn.SimpleKVsInt, ICGroupSize)
	m.Maintenance = make(cmn.SimpleKVsInt, msize)
}

func (m *smapX) _fillIC() {
	if len(m.IC) >= ICGroupSize {
		return
	}

	// try to select the missing members - upto ICGroupSize - if available
	for pid, si := range m.Pmap {
		if m.NonElects.Contains(pid) {
			continue
		}
		m.addIC(si)
		if len(m.IC) >= ICGroupSize {
			break
		}
	}
}

// only used by primary
func (m *smapX) staffIC() {
	m.addIC(m.Primary)
	m._fillIC()
	m.evictIC()
}

// ensure num IC members doesn't exceed max value
// Evict the most recently added IC member
func (m *smapX) evictIC() {
	if len(m.IC) <= ICGroupSize {
		return
	}
	// everything except new primary
	var maxVer int64
	maxSid := ""
	for sid, ver := range m.IC {
		if ver > maxVer && sid != m.Primary.ID() {
			maxSid = sid
			maxVer = ver
		}
	}
	delete(m.IC, maxSid)
}

func (m *smapX) addIC(psi *cluster.Snode) {
	if !m.IsIC(psi) {
		m.IC[psi.ID()] = m.Version
	}
}

func (m *smapX) tag() string    { return revsSmapTag }
func (m *smapX) version() int64 { return m.Version }
func (m *smapX) marshal() (b []byte) {
	jsonCompat := jsoniter.ConfigCompatibleWithStandardLibrary
	b, err := jsonCompat.Marshal(m) // jsoniter + sorting
	cmn.AssertNoErr(err)
	return b
}

func (m *smapX) isValid() bool {
	if m == nil {
		return false
	}
	if m.Primary == nil {
		return false
	}
	if m.isPresent(m.Primary) {
		cmn.Assert(m.Primary.ID() != "")
		return true
	}
	return false
}

func (m *smapX) isPrimary(self *cluster.Snode) bool {
	if !m.isValid() {
		return false
	}
	return m.Primary.ID() == self.ID()
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
		cmn.Assertf(false, "FATAL: duplicate daemon ID: %q", tsi.ID())
	}
	m.Tmap[tsi.ID()] = tsi
	m.Version++
}

func (m *smapX) addProxy(psi *cluster.Snode) {
	if m.containsID(psi.ID()) {
		cmn.Assertf(false, "FATAL: duplicate daemon ID: %q", psi.ID())
	}
	m.Pmap[psi.ID()] = psi
	m.Version++
}

func (m *smapX) delTarget(sid string) {
	if m.GetTarget(sid) == nil {
		cmn.Assertf(false, "FATAL: target: %s is not in: %s", sid, m.pp())
	}
	delete(m.Tmap, sid)
	delete(m.Maintenance, sid)
	m.Version++
}

func (m *smapX) delProxy(pid string) {
	if m.GetProxy(pid) == nil {
		cmn.Assertf(false, "FATAL: proxy: %s is not in: %s", pid, m.pp())
	}
	delete(m.Pmap, pid)
	delete(m.NonElects, pid)
	delete(m.IC, pid)
	delete(m.Maintenance, pid)
	m.Version++
}

func (m *smapX) putNode(nsi *cluster.Snode, nonElectable bool) (exists bool) {
	id := nsi.ID()
	if nsi.IsProxy() {
		if m.GetProxy(id) != nil {
			m.delProxy(id)
			exists = true
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
			exists = true
		}
		m.addTarget(nsi)
		if glog.V(3) {
			glog.Infof("joined %s (num targets %d)", nsi, m.CountTargets())
		}
	}
	return
}

func (m *smapX) clone() *smapX {
	dst := &smapX{}
	m.deepCopy(dst)
	return dst
}

func (m *smapX) deepCopy(dst *smapX) {
	cmn.CopyStruct(dst, m)
	dst.init(m.CountTargets(), m.CountProxies(), len(m.NonElects), len(m.Maintenance))
	for id, v := range m.Tmap {
		dst.Tmap[id] = v
	}
	for id, v := range m.Pmap {
		dst.Pmap[id] = v
	}
	for id, v := range m.NonElects {
		dst.NonElects[id] = v
	}
	for id, v := range m.IC {
		dst.IC[id] = v
	}
	for id, v := range m.Maintenance {
		dst.Maintenance[id] = v
	}
}

func (m *smapX) merge(dst *smapX, override bool) (added int, err error) {
	for id, si := range m.Tmap {
		err = dst.handleDuplicateURL(si, override)
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
		err = dst.handleDuplicateURL(si, override)
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

// detect duplicate URLs and delete the old one if required
func (m *smapX) handleDuplicateURL(nsi *cluster.Snode, del bool) (err error) {
	var osi *cluster.Snode
	if osi, err = m.IsDuplicateURL(nsi); err == nil {
		return
	}
	glog.Error(err)
	if !del {
		return
	}
	err = nil
	glog.Errorf("Warning: removing (old/obsolete) %s from future Smaps", osi)
	if osi.IsProxy() {
		m.delProxy(osi.ID())
	} else {
		m.delTarget(osi.ID())
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

func (m *smapX) startMaintenance(sid string, stage int64) {
	if m.GetNode(sid) == nil {
		cmn.Assertf(false, "FATAL: node: %s is not in: %s", sid, m)
	}
	delete(m.IC, sid)
	m.Maintenance[sid] = stage
	m.Version++
}

func (m *smapX) stopMaintenance(sid string) error {
	_, ok := m.Maintenance[sid]
	if !ok {
		return fmt.Errorf("node %q is not under maintenance", sid)
	}
	delete(m.Maintenance, sid)
	m.Version++
	return nil
}

///////////////
// smapOwner //
///////////////

func newSmapOwner() *smapOwner {
	return &smapOwner{
		listeners: newSmapListeners(),
	}
}

func (r *smapOwner) load(smap *smapX, config *cmn.Config) (err error) {
	err = jsp.Load(filepath.Join(config.Confdir, smapFname), smap, jsp.CCSign())
	if err == nil && (smap.version() == 0 || !smap.isValid()) {
		err = fmt.Errorf("unexpected: persistent Smap %s is invalid", smap)
	}
	return
}

func (r *smapOwner) Get() *cluster.Smap               { return &r.get().Smap }
func (r *smapOwner) Listeners() cluster.SmapListeners { return r.listeners }

//
// private to the package
//

func (r *smapOwner) put(smap *smapX) {
	smap.InitDigests()
	r.smap.Store(unsafe.Pointer(smap))
	r.listeners.notify(smap.version())
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
	config.Proxy.PrimaryURL = newSmap.Primary.PublicNet.DirectURL
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

func (r *smapOwner) modify(ctx *smapModifier) error {
	r.Lock()
	defer r.Unlock()
	clone := r.get().clone()
	if err := ctx.pre(ctx, clone); err != nil {
		return err
	}
	if err := r.persist(clone); err != nil {
		glog.Errorf("failed to persist %s: %v", clone, err)
	}
	r.put(clone)
	if ctx.post != nil {
		ctx.post(ctx, clone)
	}
	return nil
}

/////////////
// smapLis //
/////////////

func newSmapListeners() *smapLis {
	sls := &smapLis{
		listeners: make(map[string]cluster.Slistener, 8),
		postCh:    make(chan int64, 8),
	}
	return sls
}

func (sls *smapLis) run() {
	// drain
	for len(sls.postCh) > 0 {
		<-sls.postCh
	}
	sls.wg.Done()
	sls.running.Store(true)
	for ver := range sls.postCh {
		if ver == -1 {
			break
		}
		sls.RLock()
		for _, l := range sls.listeners {
			// NOTE: Reg() or Unreg() from inside ListenSmapChanged() callback
			//       may cause a trivial deadlock
			l.ListenSmapChanged()
		}
		sls.RUnlock()
	}
	// drain
	for len(sls.postCh) > 0 {
		<-sls.postCh
	}
}

func (sls *smapLis) Reg(sl cluster.Slistener) {
	cmn.Assert(sl.String() != "")
	sls.Lock()
	_, ok := sls.listeners[sl.String()]
	cmn.Assert(!ok)
	sls.listeners[sl.String()] = sl
	if len(sls.listeners) == 1 {
		sls.wg.Add(1)
		go sls.run()
		sls.wg.Wait()
	}
	sls.Unlock()
	glog.Infof("registered Smap listener %q", sl)
}

func (sls *smapLis) Unreg(sl cluster.Slistener) {
	sls.Lock()
	_, ok := sls.listeners[sl.String()]
	cmn.Assert(ok)
	delete(sls.listeners, sl.String())
	if len(sls.listeners) == 0 {
		sls.running.Store(false)
		sls.postCh <- -1
	}
	sls.Unlock()
}

func (sls *smapLis) notify(ver int64) {
	cmn.Assert(ver >= 0)
	if sls.running.Load() {
		sls.postCh <- ver
	}
}
