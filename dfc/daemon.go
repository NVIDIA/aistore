// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/golang/glog"
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
)

//======
//
// types
//
//======
type cliVars struct {
	role      string
	conffile  string
	loglevel  string
	statstime time.Duration
	ntargets  int
}

// FIXME: consider sync.Map; NOTE: atomic version is used by readers
type Smap struct {
	sync.Mutex
	Smap        map[string]*daemonInfo `json:"smap"`
	ProxySI     *daemonInfo            `json:"proxy_si"`
	Version     int64                  `json:"version"`
	syncversion int64
}

// daemon instance: proxy or storage target
type daemon struct {
	smap       *Smap
	config     dfconfig
	mountpaths map[string]*mountPath
	rg         *rungroup
}

// each daemon is represented by:
type daemonInfo struct {
	NodeIPAddr string `json:"node_ip_addr"`
	DaemonPort string `json:"daemon_port"`
	DaemonID   string `json:"daemon_id"`
	DirectURL  string `json:"direct_url"`
}

// local (cache-only) bucket names and their TBD props
type lbmap struct {
	sync.Mutex
	LBmap       map[string]string `json:"l_bmap"`
	Version     int64             `json:"version"`
	syncversion int64
}

// runner if
type runner interface {
	run() error
	stop(error)
	setname(string)
}

type namedrunner struct {
	name string
}

func (r *namedrunner) run() error       { assert(false); return nil }
func (r *namedrunner) stop(error)       { assert(false) }
func (r *namedrunner) setname(n string) { r.name = n }

// rungroup
type rungroup struct {
	runarr []runner
	runmap map[string]runner // redundant, named
	errch  chan error
	idxch  chan int
	stpch  chan error
}

type gstopError struct {
}

//====================
//
// globals
//
//====================
var (
	ctx     = &daemon{}
	clivars = &cliVars{}
)

//====================
//
// smap wrapper - NOTE - caller must take the lock
//
//====================
func (m *Smap) add(si *daemonInfo) {
	m.Smap[si.DaemonID] = si
	m.Version++
}

func (m *Smap) del(sid string) {
	delete(m.Smap, sid)
	m.Version++
}

func (m *Smap) version() int64 {
	return m.Version
}

func (m *Smap) versionLocked() int64 {
	m.lock()
	defer m.unlock()
	return m.Version
}

func (m *Smap) count() int {
	return len(m.Smap)
}

func (m *Smap) get(sid string) *daemonInfo {
	si, _ := m.Smap[sid]
	return si
}

func (m *Smap) lock() {
	m.Lock()
}

func (m *Smap) unlock() {
	m.Unlock()
}

//====================
//
// lbmap wrapper - NOTE - caller must take the lock
//
//====================
func (m *lbmap) add(b string) bool {
	_, ok := m.LBmap[b]
	if ok {
		return false
	}
	m.LBmap[b] = ""
	m.Version++
	return true
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

func (m *lbmap) version() int64 {
	return m.Version
}

func (m *lbmap) versionLocked() int64 {
	m.lock()
	defer m.unlock()
	return m.Version
}

func (m *lbmap) lock() {
	m.Lock()
}

func (m *lbmap) unlock() {
	m.Unlock()
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
	g.idxch = make(chan int, len(g.runarr))
	g.stpch = make(chan error, 1)
	for i, r := range g.runarr {
		go func(i int, r runner) {
			g.errch <- r.run()
			g.idxch <- i
		}(i, r)
	}
	// wait here
	err := <-g.errch
	idx := <-g.idxch

	for _, r := range g.runarr {
		r.stop(err)
	}
	glog.Flush()
	for i := 0; i < cap(g.errch); i++ {
		if i == idx {
			continue
		}
		<-g.errch
		glog.Flush()
	}
	g.stpch <- nil
	return err
}

func (g *rungroup) stop() {
	g.errch <- &gstopError{}
	g.idxch <- -1
	<-g.stpch
}

func (ge *gstopError) Error() string {
	return "rungroup stop"
}

//==================
//
// daemon init & run
//
//==================
func dfcinit() {
	// CLI to override dfc JSON config
	flag.StringVar(&clivars.role, "role", "", "role: proxy OR target")
	flag.StringVar(&clivars.conffile, "config", "", "config filename")
	flag.StringVar(&clivars.loglevel, "loglevel", "", "glog loglevel")
	flag.DurationVar(&clivars.statstime, "statstime", 0, "http and capacity utilization statistics log interval")
	flag.IntVar(&clivars.ntargets, "ntargets", 0, "number of storage targets to expect at startup (hint, proxy-only)")

	flag.Parse()
	if clivars.conffile == "" {
		fmt.Fprintf(os.Stderr, "Usage: go run dfc.go -role=<proxy|target> -config=<json> [...]\n")
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
		if clivars.ntargets <= 0 {
			glog.Fatalf("Unspecified or invalid number (%d) of storage targets (a hint for the http proxy)",
				clivars.ntargets)
		}
		confdir := filepath.Dir(clivars.conffile)
		ctx.smap = &Smap{Smap: make(map[string]*daemonInfo, 8)}
		p := &proxyrunner{confdir: confdir}
		ctx.rg.add(p, xproxy)
		ctx.rg.add(&proxystatsrunner{}, xproxystats)
		ctx.rg.add(newproxykalive(p), xproxykalive)
	} else {
		t := &targetrunner{}
		ctx.rg.add(t, xtarget)
		ctx.rg.add(&storstatsrunner{}, xstorstats)
		ctx.rg.add(newtargetkalive(t), xtargetkalive)
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

func getstorstats() *targetCoreStats {
	rr := getstorstatsrunner()
	return &rr.Core
}

func getcloudif() cloudif {
	r := ctx.rg.runmap[xtarget]
	rr, ok := r.(*targetrunner)
	assert(ok)
	return rr.cloudif
}
