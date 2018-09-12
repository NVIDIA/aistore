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
	"strconv"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/json-iterator/go"
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
	xfshc            = "fshc"
	xreadahead       = "readahead"
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
		mountpaths *fs.MountedFS // for mountpath definition, see fs/mountfs.go
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
)

func (r *namedrunner) setname(n string) { r.name = n }
func (r *namedrunner) getName() string  { return r.name }

// - selective disabling of a disk and/or network IO.
// - dry-run is initialized at startup and cannot be changed.
// - the values can be set via CLI or environment (environment will override CLI).
// - for details see README, section "Performance testing"
type dryRunConfig struct {
	sizeStr string // random content size used when disk IO is disabled (-dryobjsize/DFCDRYOBJSIZE)
	size    int64  // as above converted to bytes from a string like '8m'
	disk    bool   // dry-run disk (-nodiskio/DFCNODISKIO)
	network bool   // dry-run network (-nonetio/DFCNONETIO)
}

//====================
//
// globals
//
//====================
var (
	build      string
	ctx        = &daemon{}
	clivars    = &cliVars{}
	jsonCompat = jsoniter.ConfigCompatibleWithStandardLibrary
	dryRun     = &dryRunConfig{}
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
	for i := 0; i < cap(g.errch)-1; i++ {
		<-g.errch
	}
	glog.Flush()
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

	flag.BoolVar(&dryRun.disk, "nodiskio", false, "if true, no disk operations for GET and PUT")
	flag.BoolVar(&dryRun.network, "nonetio", false, "if true, no network operations for GET and PUT")
	flag.StringVar(&dryRun.sizeStr, "dryobjsize", "8m", "in-memory random content")
}

// dry-run environment overrides dry-run CLI
func dryinit() {
	str := os.Getenv("DFCNODISKIO")
	if b, err := strconv.ParseBool(str); err == nil {
		dryRun.disk = b
	}
	str = os.Getenv("DFCNONETIO")
	if b, err := strconv.ParseBool(str); err == nil {
		dryRun.network = b
	}
	str = os.Getenv("DFCDRYOBJSIZE")
	if str != "" {
		if size, err := strToBytes(str); size > 0 && err == nil {
			dryRun.size = size
		}
	}
	if dryRun.disk {
		warning := "Dry-run: disk IO will be disabled"
		fmt.Fprintf(os.Stderr, "%s\n", warning)
		glog.Infof("%s - in memory file size: %d (%s) bytes", warning, dryRun.size, bytesToStr(dryRun.size, 0))
	}
	if dryRun.network {
		warning := "Dry-run: GET won't return objects, PUT won't send objects"
		fmt.Fprintf(os.Stderr, "%s\n", warning)
		glog.Info(warning)
	}
}

//==================
//
// daemon init & run
//
//==================
func dfcinit() {
	var err error

	flag.Parse()
	assert(clivars.role == xproxy || clivars.role == xtarget, "Invalid flag: role="+clivars.role)

	dryRun.size, err = strToBytes(dryRun.sizeStr)
	if dryRun.size < 1 || err != nil {
		fmt.Fprintf(os.Stderr, "Invalid object size: %d [%s]\n", dryRun.size, dryRun.sizeStr)
	}

	if clivars.conffile == "" {
		fmt.Fprintf(os.Stderr, "Missing configuration file - must be provided via command line\n")
		fmt.Fprintf(os.Stderr, "Usage: ... -role=<proxy|target> -config=<json> ...\n")
		os.Exit(2)
	}
	if err := initconfigparam(); err != nil {
		glog.Fatalf("Failed to initialize, config %q, err: %v", clivars.conffile, err)
	}

	// init daemon
	ctx.mountpaths = fs.NewMountedFS()
	// NOTE: proxy and, respectively, target terminations are executed in the same
	//       exact order as the initializations below
	ctx.rg = &rungroup{
		runarr: make([]runner, 0, 8),
		runmap: make(map[string]runner, 8),
	}
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

		t.fsprg.init(t) // subgroup of the ctx.rg rungroup

		iostat := newIostatRunner()
		ctx.rg.add(iostat, xiostat)
		t.fsprg.add(iostat)

		atime := newAtimeRunner()
		ctx.rg.add(atime, xatime)

		// for mountpath definition, see fs/mountfs.go
		if testingFSPpaths() {
			glog.Infof("Warning: configuring %d fspaths for testing", ctx.config.TestFSP.Count)
			ctx.mountpaths.DisableFsIDCheck()
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

		fshc := newFSHC(ctx.mountpaths, &ctx.config.FSHC, t.fqn2workfile)
		ctx.rg.add(fshc, xfshc)
		t.fsprg.add(fshc)

		if ctx.config.Readahead.Enabled {
			readaheader := newReadaheader()
			ctx.rg.add(readaheader, xreadahead)
			t.fsprg.add(readaheader)
			t.readahead = readaheader
		} else {
			t.readahead = &dummyreadahead{}
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

func getfshealthchecker() *fshc {
	r := ctx.rg.runmap[xfshc]
	rr, ok := r.(*fshc)
	assert(ok)
	return rr
}
