/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"flag"
	"fmt"
	"os"
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
)

//====================
//
// global
//
//====================
var ctx = &daemon{}

//====================
//
// types
//
//====================
// daemon instance: proxy or storage target
type daemon struct {
	smap       map[string]serverinfo // registered storage targets (proxy only)
	config     dfconfig              // Configuration
	mountpaths []mountPath           //
	rg         *rungroup
}

// registration info
type serverinfo struct {
	port string // http listening port
	ip   string // FIXME: always picking the first IP address as the server's IP
	id   string // macaddr (as a unique ID)
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
	flag.StringVar(&conffile, "configfile", "", "config filename")
	flag.StringVar(&loglevel, "loglevel", "", "glog loglevel")
	flag.DurationVar(&statstime, "statstime", 0, "http and capacity utilization statistics log interval")

	flag.Parse()
	if conffile == "" {
		fmt.Fprintf(os.Stderr, "Usage: go run dfc.go -role=<proxy|target> -configfile=<somefile.json> [-statstime=<duration>]\n")
		os.Exit(2)
	}
	assert(role == xproxy || role == xtarget, "Invalid flag: role="+role)
	err := initconfigparam(conffile, loglevel, role, statstime)
	if err != nil {
		glog.Fatalf("Failed to initialize, config %q, err: %v", conffile, err)
	}
	assert(role == xproxy || role == xtarget, "Invalid configuration: role="+role)

	// init daemon
	ctx.rg = &rungroup{
		runarr: make([]runner, 0, 4),
		runmap: make(map[string]runner),
	}
	if role == xproxy {
		ctx.smap = make(map[string]serverinfo)
		ctx.rg.add(&proxyrunner{}, xproxy)
		ctx.rg.add(&proxystatsrunner{}, xproxystats)
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
	glog.Fatalf("============== Terminated with err: %v\n", err)
m:
	glog.Infoln("============== Terminated OK")
	glog.Flush()
}

//==================
//
// global helpers
//
//==================
func getproxystats() *proxystats {
	r := ctx.rg.runmap[xproxystats]
	rr, ok := r.(*proxystatsrunner)
	assert(ok)
	return &rr.stats
}

func getstorstats() *storstats {
	r := ctx.rg.runmap[xstorstats]
	rr, ok := r.(*storstatsrunner)
	assert(ok)
	return &rr.stats
}

func initusedstats() {
	r := ctx.rg.runmap[xstorstats]
	rr, ok := r.(*storstatsrunner)
	assert(ok)
	rr.used = make(map[string]int, len(ctx.mountpaths))
	for _, mountpath := range ctx.mountpaths {
		rr.used[mountpath.Path] = 0
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
