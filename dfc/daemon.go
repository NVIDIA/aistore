// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
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

	// most basic and commonly used key/value map where both the keys and the values are strings
	simplekvs map[string]string

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
		si            *daemonInfo
		outjson       []byte
		err           error
		errstr        string
		newPrimaryURL string
		status        int
	}
)

func (r *namedrunner) setname(n string) { r.name = n }

//====================
//
// globals
//
//====================
var (
	build   string
	ctx     = &daemon{}
	clivars = &cliVars{}
)

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

	// wait here for (any/first) runner termination
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
		p := &proxyrunner{}
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
			ctx.rg.add(newFSKeeper(&ctx.config.FSKeeper,
				&ctx.mountpaths, t.fqn2workfile), xfskeeper)
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

func getFSKeeper() *fsKeeper {
	if !ctx.config.FSKeeper.Enabled {
		return nil
	}
	r := ctx.rg.runmap[xfskeeper]
	rr, ok := r.(*fsKeeper)
	assert(ok)
	return rr
}

func getmetasyncer() *metasyncer {
	r := ctx.rg.runmap[xmetasyncer]
	rr, ok := r.(*metasyncer)
	assert(ok)
	return rr
}
