// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dfc

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/atime"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/health"
	"github.com/NVIDIA/dfcpub/ios"
	"github.com/NVIDIA/dfcpub/memsys"
	"github.com/NVIDIA/dfcpub/stats"
	"github.com/json-iterator/go"
)

// runners
const (
	xproxy           = "proxy"
	xtarget          = "target"
	xmem             = "gmem2"
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
	xreplication     = "replication" // TODO: fix replication
)

type (
	cliVars struct {
		role      string
		conffile  string
		loglevel  string
		statstime time.Duration
		proxyurl  string
		ntargets  int
	}

	// daemon instance: proxy or storage target
	daemon struct {
		rg *rungroup
	}

	rungroup struct {
		runarr []cmn.Runner
		runmap map[string]cmn.Runner // redundant, named
		errCh  chan error
		stopCh chan error
	}
)

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
	gmem2      *memsys.Mem2 // gen-purpose system-wide memory manager and slab/SGL allocator (instance, runner)
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
func (g *rungroup) add(r cmn.Runner, name string) {
	r.Setname(name)
	g.runarr = append(g.runarr, r)
	g.runmap[name] = r
}

func (g *rungroup) run() error {
	if len(g.runarr) == 0 {
		return nil
	}
	g.errCh = make(chan error, len(g.runarr))
	g.stopCh = make(chan error, 1)
	for i, r := range g.runarr {
		go func(i int, r cmn.Runner) {
			err := r.Run()
			glog.Warningf("Runner [%s] threw error [%v].", r.Getname(), err)
			g.errCh <- err
		}(i, r)
	}

	// wait here for (any/first) runner termination
	err := <-g.errCh
	for _, r := range g.runarr {
		r.Stop(err)
	}
	for i := 0; i < cap(g.errCh)-1; i++ {
		<-g.errCh
	}
	glog.Flush()
	g.stopCh <- nil
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
		if size, err := cmn.S2B(str); size > 0 && err == nil {
			dryRun.size = size
		}
	}
	if dryRun.disk {
		warning := "Dry-run: disk IO will be disabled"
		fmt.Fprintf(os.Stderr, "%s\n", warning)
		glog.Infof("%s - in memory file size: %d (%s) bytes", warning, dryRun.size, cmn.B2S(dryRun.size, 0))
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
	cmn.Assert(clivars.role == xproxy || clivars.role == xtarget, "Invalid flag: role="+clivars.role)

	dryRun.size, err = cmn.S2B(dryRun.sizeStr)
	if dryRun.size < 1 || err != nil {
		fmt.Fprintf(os.Stderr, "Invalid object size: %d [%s]\n", dryRun.size, dryRun.sizeStr)
	}

	if clivars.conffile == "" {
		fmt.Fprintf(os.Stderr, "Missing configuration file - must be provided via command line\n")
		fmt.Fprintf(os.Stderr, "Usage: ... -role=<proxy|target> -config=<json> ...\n")
		os.Exit(2)
	}
	if err := cmn.LoadConfig(clivars.conffile, clivars.statstime, clivars.proxyurl, clivars.loglevel); err != nil {
		glog.Fatalf("Failed to initialize, config %q, err: %v", clivars.conffile, err)
	}

	// init daemon
	fs.Mountpaths = fs.NewMountedFS()
	// NOTE: proxy and, respectively, target terminations are executed in the same
	//       exact order as the initializations below
	ctx.rg = &rungroup{
		runarr: make([]cmn.Runner, 0, 8),
		runmap: make(map[string]cmn.Runner, 8),
	}
	if clivars.role == xproxy {
		p := &proxyrunner{}
		p.initSI()
		ctx.rg.add(p, xproxy)
		ps := &stats.Prunner{}
		ps.Init()
		ctx.rg.add(ps, xproxystats)
		_ = p.initStatsD("dfcproxy")
		ps.Core.StatsdC = &p.statsdC

		ctx.rg.add(newProxyKeepaliveRunner(p), xproxykeepalive)
		ctx.rg.add(newmetasyncer(p), xmetasyncer)
	} else {
		t := &targetrunner{}
		t.initSI()
		ctx.rg.add(t, xtarget)
		ts := &stats.Trunner{TargetRunner: t} // iostat below
		ts.Init()
		ctx.rg.add(ts, xstorstats)
		_ = t.initStatsD("dfctarget")
		ts.Core.StatsdC = &t.statsdC

		ctx.rg.add(newTargetKeepaliveRunner(t), xtargetkeepalive)

		// iostat is required: ensure that it is installed and its version is right
		if err := ios.CheckIostatVersion(); err != nil {
			glog.Exit(err)
		}

		t.fsprg.init(t) // subgroup of the ctx.rg rungroup

		// system-wide gen-purpose memory manager and slab/SGL allocator
		mem := &memsys.Mem2{MinPctTotal: 4, MinFree: cmn.GiB * 2} // free mem: try to maintain at least the min of these two
		_ = mem.Init(false)                                       // don't ignore init-time errors
		ctx.rg.add(mem, xmem)                                     // to periodically house-keep
		gmem2 = getmem2()                                         // making it global; getmem2() can still be used

		// fs.Mountpaths must be inited prior to all runners that utilize all
		// or run per filesystem(s); for mountpath definition, see fs/mountfs.go
		config := cmn.GCO.Get()
		if cmn.TestingEnv() {
			glog.Infof("Warning: configuring %d fspaths for testing", config.TestFSP.Count)
			fs.Mountpaths.DisableFsIDCheck()
			t.testCachepathMounts()
		} else {
			fsPaths := make([]string, 0, len(config.FSpaths))
			for path := range config.FSpaths {
				fsPaths = append(fsPaths, path)
			}

			if err := fs.Mountpaths.Init(fsPaths); err != nil {
				glog.Fatal(err)
			}
		}

		iostat := ios.NewIostatRunner()
		ctx.rg.add(iostat, xiostat)
		t.fsprg.Reg(iostat)
		ts.Riostat = iostat

		fshc := health.NewFSHC(fs.Mountpaths, gmem2, fs.CSM)
		ctx.rg.add(fshc, xfshc)
		t.fsprg.Reg(fshc)

		if config.Readahead.Enabled {
			readaheader := newReadaheader()
			ctx.rg.add(readaheader, xreadahead)
			t.fsprg.Reg(readaheader)
			t.readahead = readaheader
		} else {
			t.readahead = &dummyreadahead{}
		}

		// TODO: not ready yet but will be
		// replRunner := newReplicationRunner(t, fs.Mountpaths)
		// ctx.rg.add(replRunner, xreplication, nil)
		// t.fsprg.Reg(replRunner)

		atime := atime.NewRunner(fs.Mountpaths, iostat)
		ctx.rg.add(atime, xatime)
		t.fsprg.Reg(atime)
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
func getproxystatsrunner() *stats.Prunner {
	r := ctx.rg.runmap[xproxystats]
	rr, ok := r.(*stats.Prunner)
	cmn.Assert(ok)
	return rr
}

func getproxykeepalive() *proxyKeepaliveRunner {
	r := ctx.rg.runmap[xproxykeepalive]
	rr, ok := r.(*proxyKeepaliveRunner)
	cmn.Assert(ok)
	return rr
}

func getmem2() *memsys.Mem2 {
	r := ctx.rg.runmap[xmem]
	rr, ok := r.(*memsys.Mem2)
	cmn.Assert(ok)
	return rr
}

func gettargetkeepalive() *targetKeepaliveRunner {
	r := ctx.rg.runmap[xtargetkeepalive]
	rr, ok := r.(*targetKeepaliveRunner)
	cmn.Assert(ok)
	return rr
}

// TODO: fix replication
func getreplicationrunner() *replicationRunner {
	r := ctx.rg.runmap[xreplication]
	rr, ok := r.(*replicationRunner)
	cmn.Assert(ok)
	return rr
}

func getstorstatsrunner() *stats.Trunner {
	r := ctx.rg.runmap[xstorstats]
	rr, ok := r.(*stats.Trunner)
	cmn.Assert(ok)
	return rr
}

func getiostatrunner() *ios.IostatRunner {
	r := ctx.rg.runmap[xiostat]
	rr, ok := r.(*ios.IostatRunner)
	cmn.Assert(ok)
	return rr
}

func getatimerunner() *atime.Runner {
	r := ctx.rg.runmap[xatime]
	rr, ok := r.(*atime.Runner)
	cmn.Assert(ok)
	return rr
}

func getcloudif() cloudif {
	r := ctx.rg.runmap[xtarget]
	rr, ok := r.(*targetrunner)
	cmn.Assert(ok)
	return rr.cloudif
}

func getmetasyncer() *metasyncer {
	r := ctx.rg.runmap[xmetasyncer]
	rr, ok := r.(*metasyncer)
	cmn.Assert(ok)
	return rr
}

func getfshealthchecker() *health.FSHC {
	r := ctx.rg.runmap[xfshc]
	rr, ok := r.(*health.FSHC)
	cmn.Assert(ok)
	return rr
}
