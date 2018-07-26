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
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/fs"
)

// runners
const (
	xproxy           = "proxy"
	xtarget          = "target"
	xsignal          = "signal"
	xproxystats      = "proxystats"
	xstorstats       = "storstats"
	xproxykeepalive  = "proxykeepalive"
	xtargetkeepalive = "targetkeepalive"
	xiostat          = "iostat"
	xatime           = "atime"
	xmetasyncer      = "metasyncer"
	xfshealthchecker = "fshealthchecker"
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

	// daemon instance: proxy or storage target
	daemon struct {
		config     dfconfig
		mountpaths fs.MountedFS // for mountpath definition, see fs/mountfs.go
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
		stpch  chan error
	}

	runner interface {
		run() error
		stop(error)
		setname(string)
		getName() string
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
func (r *namedrunner) getName() string  { return r.name }

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
			err := r.run()
			glog.Warningf("Runner [%s] threw error [%v].", r.getName(), err)
			g.errch <- err
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
		ctx.rg.add(newProxyKeepaliveRunner(p), xproxykeepalive)
		ctx.rg.add(newmetasyncer(p), xmetasyncer)
	} else {
		t := &targetrunner{}
		t.initSI()
		ctx.rg.add(t, xtarget)
		ctx.rg.add(&storstatsrunner{}, xstorstats)
		ctx.rg.add(newTargetKeepaliveRunner(t), xtargetkeepalive)

		// iostat is required: ensure that it is installed and its version is right
		if err := checkIostatVersion(); err != nil {
			glog.Exit(err)
		}
		ctx.rg.add(newIostatRunner(), xiostat)

		ctx.rg.add(&atimerunner{
			chstop:      make(chan struct{}, 4),
			chfqn:       make(chan string, chfqnSize),
			atimemap:    &atimemap{fsToFilesMap: make(map[string]map[string]time.Time, atimeCacheFlushThreshold)},
			chGetAtime:  make(chan string),
			chSendAtime: make(chan accessTimeResponse),
		}, xatime)

		// for mountpath definition, see fs/mountfs.go
		if testingFSPpaths() {
			glog.Infof("Warning: configuring %d fspaths for testing", ctx.config.TestFSP.Count)
			t.testCachepathMounts()
		} else {
			fsPaths := make([]string, 0, len(ctx.config.FSpaths))
			for path := range ctx.config.FSpaths {
				fsPaths = append(fsPaths, path)
			}

			if err := ctx.mountpaths.Init(fsPaths); err != nil {
				glog.Fatal(err)
			}
		}

		ctx.rg.add(
			newFSHealthChecker(&ctx.mountpaths, &ctx.config.FSChecker, t.fqn2workfile),
			xfshealthchecker,
		)
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

func getproxykeepalive() *proxyKeepaliveRunner {
	r := ctx.rg.runmap[xproxykeepalive]
	rr, ok := r.(*proxyKeepaliveRunner)
	assert(ok)
	return rr
}

func gettarget() *targetrunner {
	r := ctx.rg.runmap[xtarget]
	rr, ok := r.(*targetrunner)
	assert(ok)
	return rr
}

func gettargetkeepalive() *targetKeepaliveRunner {
	r := ctx.rg.runmap[xtargetkeepalive]
	rr, ok := r.(*targetKeepaliveRunner)
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
	rr, ok := r.(*iostatrunner)
	assert(ok)
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

func getmetasyncer() *metasyncer {
	r := ctx.rg.runmap[xmetasyncer]
	rr, ok := r.(*metasyncer)
	assert(ok)
	return rr
}

func getfshealthchecker() *fsHealthChecker {
	r := ctx.rg.runmap[xfshealthchecker]
	rr, ok := r.(*fsHealthChecker)
	assert(ok)
	return rr
}
