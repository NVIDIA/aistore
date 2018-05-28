// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

// runners
const (
	xproxy        = "proxy"
	xtarget       = "target"
	xsignal       = "signal"
	xproxystats   = "proxystats"
	xstorstats    = "storstats"
	xproxykalive  = "proxykalive"
	xtargetkalive = "targetkalive"
	xiostat       = "iostat"
	xfskeeper     = "fskeeper"
	xatime        = "atime"
	xmetasyncer   = "metasyncer"
)

type (
	cliVars struct {
		role      string
		conffile  string
		loglevel  string
		statstime time.Duration
		ntargets  int
		proxyurl  string
	}

	// FIXME: consider sync.Map; NOTE: atomic version is used by readers
	// Smap contains id:daemonInfo pairs and related metadata
	Smap struct {
		Tmap    map[string]*daemonInfo `json:"tmap"` // daemonID -> daemonInfo
		Pmap    map[string]*daemonInfo `json:"pmap"` // proxyID -> proxyInfo
		ProxySI *daemonInfo            `json:"proxy_si"`
		Version int64                  `json:"version"`
	}

	mountedFS struct {
		sync.Mutex `json:"-"`
		Available  map[string]*mountPath `json:"available"`
		Offline    map[string]*mountPath `json:"offline"`
	}

	// daemon instance: proxy or storage target
	daemon struct {
		config     dfconfig
		mountpaths mountedFS
		rg         *rungroup
	}

	// each daemon is represented by:
	daemonInfo struct {
		NodeIPAddr string `json:"node_ip_addr"`
		DaemonPort string `json:"daemon_port"`
		DaemonID   string `json:"daemon_id"`
		DirectURL  string `json:"direct_url"`
	}

	// local (cache-only) bucket names and their TBD props
	lbmap struct {
		sync.Mutex
		LBmap   map[string]string `json:"l_bmap"`
		Version int64             `json:"version"`
	}

	namedrunner struct {
		name string
	}

	rungroup struct {
		runarr []runner
		runmap map[string]runner // redundant, named
		errch  chan error
		idxch  chan int
		stpch  chan error
	}

	runner interface {
		run() error
		stop(error)
		setname(string)
	}

	// callResult contains data returned by a server to server call
	callResult struct {
		si      *daemonInfo
		outjson []byte
		err     error
		errstr  string
		status  int
	}
)

func (r *namedrunner) setname(n string) { r.name = n }

//====================
//
// globals
//
//====================
var (
	build     string
	ctx       = &daemon{}
	clivars   = &cliVars{}
	smapLock  = &sync.Mutex{}
	lbmapLock = &sync.Mutex{}
)

//====================
//
// smap wrapper - NOTE - caller must take the lock
//
//====================
func (m *Smap) add(si *daemonInfo) {
	m.Tmap[si.DaemonID] = si
	m.Version++
}

func (m *Smap) addProxy(pi *daemonInfo) {
	m.Pmap[pi.DaemonID] = pi
	m.Version++
}

func (m *Smap) del(sid string) {
	delete(m.Tmap, sid)
	m.Version++
}

func (m *Smap) delProxy(pid string) {
	delete(m.Pmap, pid)
	m.Version++
}

func (m *Smap) versionL() int64 {
	smapLock.Lock()
	defer smapLock.Unlock()
	return m.Version
}

func (m *Smap) count() int {
	return len(m.Tmap)
}

func (m *Smap) countProxies() int {
	return len(m.Pmap)
}

func (m *Smap) countL() int {
	smapLock.Lock()
	defer smapLock.Unlock()
	return m.count()
}

func (m *Smap) get(sid string) *daemonInfo {
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

func (m *Smap) containsL(id string) bool {
	smapLock.Lock()
	defer smapLock.Unlock()
	if _, ok := m.Tmap[id]; ok {
		return true
	} else if _, ok := m.Pmap[id]; ok {
		return true
	}
	return false
}

func (m *Smap) copyL(dst *Smap) {
	smapLock.Lock()
	defer smapLock.Unlock()
	m.deepcopy(dst)
}

func (m *Smap) cloneU() *Smap {
	dst := &Smap{}
	m.deepcopy(dst)
	return dst
}

func (m *Smap) deepcopy(dst *Smap) {
	copyStruct(dst, m)
	dst.Tmap = make(map[string]*daemonInfo, len(m.Tmap))
	for id, v := range m.Tmap {
		dst.Tmap[id] = v
	}

	dst.Pmap = make(map[string]*daemonInfo, len(m.Pmap))
	for id, v := range m.Pmap {
		dst.Pmap[id] = v
	}

	if m.ProxySI != nil {
		copyStruct(dst.ProxySI, m.ProxySI)
	}
}

// Remove is a helper function that removes a server from the smap without knowing before hand
// whether it is a proxy or a target.
// Assuming the ID is unique for proxies and targets. If the ID shows up in both maps, both will be deleted.
// No lock held during delete.
// No map version change.
func (m *Smap) Remove(id string) {
	delete(m.Tmap, id)
	delete(m.Pmap, id)
}

// totalServers returns total number of proxies plus targets.
// no lock held.
func (m *Smap) totalServers() int {
	return len(m.Pmap) + len(m.Tmap)
}

// Dump prints the smap
func (m *Smap) Dump() {
	fmt.Printf("Smap: version = %d\n", m.Version)
	fmt.Printf("Targets\n")
	for _, v := range m.Tmap {
		fmt.Printf("\tid = %-15s\turl = %s\n", v.DaemonID, v.DirectURL)
	}

	fmt.Printf("Proxies\n")
	for _, v := range m.Pmap {
		fmt.Printf("\tid = %-15s\turl = %-30s\n", v.DaemonID, v.DirectURL)
	}

	fmt.Printf("Primary proxy\n")
	if m.ProxySI != nil {
		fmt.Printf("\tid = %-15s\turl = %-30s\n",
			m.ProxySI.DaemonID, m.ProxySI.DirectURL)
	}
}

//
// revs interface
//
func (m *Smap) tag() string    { return smaptag }
func (m *Smap) version() int64 { return m.Version }

func (m *Smap) cloneL() interface{} {
	smapLock.Lock()
	defer smapLock.Unlock()
	return m.cloneU()
}

func (m *Smap) marshal() (b []byte, err error) {
	b, err = json.Marshal(m)
	return
}

func newLBMap() *lbmap {
	return &lbmap{LBmap: make(map[string]string)}
}

func (m *lbmap) add(b string) bool {
	_, ok := m.LBmap[b]
	if ok {
		return false
	}
	m.LBmap[b] = ""
	m.Version++
	return true
}

func (m *lbmap) contains(b string) (ok bool) {
	_, ok = m.LBmap[b]
	return
}

func (m *lbmap) del(b string) bool {
	_, ok := m.LBmap[b]
	if !ok {
		return false
	}
	delete(m.LBmap, b)
	m.Version++
	return true
}

func (m *lbmap) versionL() int64 {
	lbmapLock.Lock()
	defer lbmapLock.Unlock()
	return m.Version
}

func (m *lbmap) cloneU() *lbmap {
	dst := &lbmap{}
	m.deepcopy(dst)
	return dst
}

func (m *lbmap) copyL(dst *lbmap) {
	lbmapLock.Lock()
	defer lbmapLock.Unlock()
	m.deepcopy(dst)
}

func (m *lbmap) deepcopy(dst *lbmap) {
	copyStruct(dst, m)
	dst.LBmap = make(map[string]string, len(m.LBmap))
	for name, v := range m.LBmap {
		dst.LBmap[name] = v
	}
}

//
// revs interface
//
func (m *lbmap) tag() string    { return lbmaptag }
func (m *lbmap) version() int64 { return m.Version }

func (m *lbmap) cloneL() interface{} {
	lbmapLock.Lock()
	defer lbmapLock.Unlock()
	return m.cloneU()
}

func (m *lbmap) marshal() (b []byte, err error) {
	b, err = json.Marshal(m)
	return
}

//====================
//
// rungroup
//
//====================
func (g *rungroup) add(r runner, name string) {
	r.setname(name)
	g.runarr = append(g.runarr, r)
	g.runmap[name] = r
}

func (g *rungroup) run() error {
	if len(g.runarr) == 0 {
		return nil
	}
	g.errch = make(chan error, len(g.runarr))
	g.stpch = make(chan error, 1)
	for i, r := range g.runarr {
		go func(i int, r runner) {
			g.errch <- r.run()
		}(i, r)
	}

	// wait here for the first completed runner(likely signal runner)
	err := <-g.errch

	for _, r := range g.runarr {
		r.stop(err)
	}
	glog.Flush()
	for i := 0; i < cap(g.errch)-1; i++ {
		<-g.errch
		glog.Flush()
	}
	g.stpch <- nil
	return err
}

func init() {
	// CLI to override dfc JSON config
	flag.StringVar(&clivars.role, "role", "", "role: proxy OR target")
	flag.StringVar(&clivars.conffile, "config", "", "config filename")
	flag.StringVar(&clivars.loglevel, "loglevel", "", "glog loglevel")
	flag.DurationVar(&clivars.statstime, "statstime", 0, "http and capacity utilization statistics log interval")
	flag.IntVar(&clivars.ntargets, "ntargets", 0, "number of storage targets to expect at startup (hint, proxy-only)")
	flag.StringVar(&clivars.proxyurl, "proxyurl", "", "Override config Proxy settings")
}

//==================
//
// daemon init & run
//
//==================
func dfcinit() {
	flag.Parse()

	if clivars.conffile == "" {
		fmt.Fprintf(os.Stderr, "Missing configuration file - must be provided via command line\n")
		fmt.Fprintf(os.Stderr, "Usage: ... -role=<proxy|target> -config=<json> ...\n")
		os.Exit(2)
	}
	if err := initconfigparam(); err != nil {
		glog.Fatalf("Failed to initialize, config %q, err: %v", clivars.conffile, err)
	}

	// init daemon
	ctx.rg = &rungroup{
		runarr: make([]runner, 0, 4),
		runmap: make(map[string]runner),
	}
	assert(clivars.role == xproxy || clivars.role == xtarget, "Invalid flag: role="+clivars.role)
	if clivars.role == xproxy {
		confdir := ctx.config.Confdir
		p := &proxyrunner{confdir: confdir}
		p.initSI()
		ctx.rg.add(p, xproxy)
		ctx.rg.add(&proxystatsrunner{}, xproxystats)
		ctx.rg.add(newproxykalive(p), xproxykalive)
		ctx.rg.add(newmetasyncer(p), xmetasyncer)
	} else {
		t := &targetrunner{}
		t.initSI()
		ctx.rg.add(t, xtarget)
		ctx.rg.add(&storstatsrunner{}, xstorstats)
		ctx.rg.add(newtargetkalive(t), xtargetkalive)
		if iostatverok() {
			ctx.rg.add(&iostatrunner{}, xiostat)
		}

		if ctx.config.FSKeeper.Enabled {
			ctx.rg.add(newfskeeper(t), xfskeeper)
		}

		ctx.rg.add(&atimerunner{
			chstop:   make(chan struct{}, 4),
			chfqn:    make(chan string, chfqnSize),
			atimemap: &atimemap{m: make(map[string]time.Time, atimeCacheIni)},
		}, xatime)

		// Note:
		// Move this code from run() to here to fix a race between target run() and storage stats
		// run() DFC's runner start doesn't have a concept of sequence, all runners are started
		// without a clean way of making sure all fields needed by a runner are initialized.
		// The code should be reworked to include a clean way of initializing all runnners
		// sequentilly based on runner's dependency, so when runners' run()
		// is called, they have all their needed fields created and initialized.
		// Here is one example, when targetrunner.run() and storstatsrunner.run() both are running,
		// ctx.mountpaths.Available is supposed to be filled by targetrunner when it calls startupMpaths(),
		// but storstatsrunner.run() started to use it, resulted in the read/write race.
		ctx.mountpaths.Available = make(map[string]*mountPath, len(ctx.config.FSpaths))
		ctx.mountpaths.Offline = make(map[string]*mountPath, len(ctx.config.FSpaths))
		if t.testingFSPpaths() {
			glog.Infof("Warning: configuring %d fspaths for testing", ctx.config.TestFSP.Count)
			t.testCachepathMounts()
		} else {
			t.fspath2mpath()
			t.mpath2Fsid() // enforce FS uniqueness
		}
	}
	ctx.rg.add(&sigrunner{}, xsignal)
}

// Run is the 'main' where everything gets started
func Run() {
	dfcinit()
	var ok bool

	err := ctx.rg.run()
	if err == nil {
		goto m
	}
	_, ok = err.(*signalError)
	if ok {
		goto m
	}
	glog.Errorln()
	glog.Errorf("Terminated with err: %v\n", err)
	os.Exit(1)
m:
	glog.Infoln("Terminated OK")
	glog.Flush()
}

//==================
//
// global helpers
//
//==================
func getproxystatsrunner() *proxystatsrunner {
	r := ctx.rg.runmap[xproxystats]
	rr, ok := r.(*proxystatsrunner)
	assert(ok)
	return rr
}

func getproxystats() *proxyCoreStats {
	rr := getproxystatsrunner()
	return &rr.Core
}

func getproxy() *proxyrunner {
	r := ctx.rg.runmap[xproxy]
	rr, ok := r.(*proxyrunner)
	assert(ok)
	return rr
}

func getproxykalive() *proxykalive {
	r := ctx.rg.runmap[xproxykalive]
	rr, ok := r.(*proxykalive)
	assert(ok)
	return rr
}

func gettarget() *targetrunner {
	r := ctx.rg.runmap[xtarget]
	rr, ok := r.(*targetrunner)
	assert(ok)
	return rr
}

func gettargetkalive() *targetkalive {
	r := ctx.rg.runmap[xtargetkalive]
	rr, ok := r.(*targetkalive)
	assert(ok)
	return rr
}

func getstorstatsrunner() *storstatsrunner {
	r := ctx.rg.runmap[xstorstats]
	rr, ok := r.(*storstatsrunner)
	assert(ok)
	return rr
}

func getiostatrunner() *iostatrunner {
	r := ctx.rg.runmap[xiostat]
	rr, _ := r.(*iostatrunner)
	// not asserting: a) sysstat installed? b) mac
	return rr
}

func getatimerunner() *atimerunner {
	r := ctx.rg.runmap[xatime]
	rr, ok := r.(*atimerunner)
	assert(ok)
	return rr
}

func getcloudif() cloudif {
	r := ctx.rg.runmap[xtarget]
	rr, ok := r.(*targetrunner)
	assert(ok)
	return rr.cloudif
}

func getfskeeper() *fskeeper {
	if !ctx.config.FSKeeper.Enabled {
		return nil
	}
	r := ctx.rg.runmap[xfskeeper]
	rr, ok := r.(*fskeeper)
	assert(ok)
	return rr
}

func getmetasyncer() *metasyncer {
	r := ctx.rg.runmap[xmetasyncer]
	rr, ok := r.(*metasyncer)
	assert(ok)
	return rr
}
