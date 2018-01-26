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
	xproxy      = "proxy"
	xtarget     = "target"
	xsignal     = "signal"
	xproxystats = "proxystats"
	xstorstats  = "storstats"
	xkeepalive  = "keepalive"
)

//====================
//
// global
//
//====================
var ctx = &daemon{}

//======
//
// types
//
//======
// FIXME: consider sync.Map; NOTE: atomic version is used by readers
type Smap struct {
	Smap        map[string]*daemonInfo `json:"smap"`
	Version     int64                  `json:"version"`
	mutex       *sync.Mutex
	syncversion int64
}

// daemon instance: proxy or storage target
type daemon struct {
	smap       *Smap
	config     dfconfig
	mountpaths map[string]mountPath
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
	LBmap       map[string]string `json:"l_bmap"`
	Version     int64             `json:"version"`
	mutex       *sync.Mutex
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
	m.mutex.Lock()
}

func (m *Smap) unlock() {
	m.mutex.Unlock()
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
	m.mutex.Lock()
}

func (m *lbmap) unlock() {
	m.mutex.Unlock()
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
	var (
		role      string
		conffile  string
		loglevel  string
		statstime time.Duration
	)
	flag.StringVar(&role, "role", "", "role: proxy OR target")
	flag.StringVar(&conffile, "config", "", "config filename")
	flag.StringVar(&loglevel, "loglevel", "", "glog loglevel")
	flag.DurationVar(&statstime, "statstime", 0, "http and capacity utilization statistics log interval")

	flag.Parse()
	if conffile == "" {
		fmt.Fprintf(os.Stderr, "Usage: go run dfc.go -role=<proxy|target> -config=<json> [...]\n")
		os.Exit(2)
	}
	assert(role == xproxy || role == xtarget, "Invalid flag: role="+role)
	if err := initconfigparam(conffile, loglevel, role, statstime); err != nil {
		glog.Fatalf("Failed to initialize, config %q, err: %v", conffile, err)
	}
	assert(role == xproxy || role == xtarget, "Invalid configuration: role="+role)

	// init daemon
	ctx.rg = &rungroup{
		runarr: make([]runner, 0, 4),
		runmap: make(map[string]runner),
	}
	if role == xproxy {
		confdir := filepath.Dir(conffile)
		ctx.smap = &Smap{Smap: make(map[string]*daemonInfo, 8), mutex: &sync.Mutex{}}
		ctx.rg.add(&proxyrunner{confdir: confdir}, xproxy)
		ctx.rg.add(&proxystatsrunner{}, xproxystats)
		ctx.rg.add(&keepalive{}, xkeepalive)
	} else {
		ctx.rg.add(&targetrunner{}, xtarget)
		ctx.rg.add(&storstatsrunner{}, xstorstats)
	}
	ctx.rg.add(&sigrunner{}, xsignal)
}

// main
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
	// dump stack trace and exit
	glog.Fatalf("Terminated with err: %v\n", err)
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

func getproxystats() *Proxystats {
	rr := getproxystatsrunner()
	return &rr.stats
}

func getproxy() *proxyrunner {
	r := ctx.rg.runmap[xproxy]
	rr, ok := r.(*proxyrunner)
	assert(ok)
	return rr
}

func getkeepaliverunner() *keepalive {
	r := ctx.rg.runmap[xkeepalive]
	rr, ok := r.(*keepalive)
	assert(ok)
	return rr
}

func gettarget() *targetrunner {
	r := ctx.rg.runmap[xtarget]
	rr, ok := r.(*targetrunner)
	assert(ok)
	return rr
}

func getstorstatsrunner() *storstatsrunner {
	r := ctx.rg.runmap[xstorstats]
	rr, ok := r.(*storstatsrunner)
	assert(ok)
	return rr
}

func getstorstats() *Storstats {
	rr := getstorstatsrunner()
	return &rr.stats
}

func initusedstats() {
	r := ctx.rg.runmap[xstorstats]
	rr, ok := r.(*storstatsrunner)
	assert(ok)
	rr.used = make(map[string]int, len(ctx.mountpaths))
	for path := range ctx.mountpaths {
		rr.used[path] = 0
	}
}

func getusedstats() usedstats {
	r := ctx.rg.runmap[xstorstats]
	rr, ok := r.(*storstatsrunner)
	assert(ok)
	return rr.used
}

func getcloudif() cinterface {
	r := ctx.rg.runmap[xtarget]
	rr, ok := r.(*targetrunner)
	assert(ok)
	return rr.cloudif
}
