// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"flag"
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/health"
	"github.com/NVIDIA/aistore/housekeep/hk"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/transport"
	jsoniter "github.com/json-iterator/go"
)

// runners
const (
	xstreamc         = "stream-collector"
	xsignal          = "signal"
	xproxystats      = "proxystats"
	xstorstats       = "storstats"
	xproxykeepalive  = "proxykeepalive"
	xtargetkeepalive = "targetkeepalive"
	xmetasyncer      = "metasyncer"
	xfshc            = "fshc"
)

// not to confuse with default ones, see memsys/mmsa.go
const (
	gmmName = ".ais.mm"
	smmName = ".ais.mm.small"
)

type (
	// - selective disabling of a disk and/or network IO.
	// - dry-run is initialized at startup and cannot be changed.
	// - the values can be set via clivars or environment (environment will override clivars).
	// - for details see README, section "Performance testing"
	dryRunConfig struct {
		sizeStr string // random content size used when disk IO is disabled (-dryobjsize/AIS_DRYOBJSIZE)
		size    int64  // as above converted to bytes from a string like '8m'
		disk    bool   // dry-run disk (-nodiskio/AIS_NODISKIO)
	}

	cliFlags struct {
		role        string        // proxy | target
		config      jsp.ConfigCLI // selected config overrides
		confjson    string        // JSON formatted "{name: value, ...}" string to override selected knob(s)
		persist     bool          // true: make cmn.ConfigCLI settings permanent, false: leave them transient
		skipStartup bool          // determines if the proxy should skip waiting for targets
		ntargets    int           // expected number of targets in a starting-up cluster (proxy only)
	}

	// daemon instance: proxy or storage target
	daemonCtx struct {
		cli    cliFlags
		dryRun dryRunConfig
		gmm    *memsys.MMSA // system pagesize-based memory manager and slab allocator
		smm    *memsys.MMSA // system MMSA for small-size allocations
		rg     *rungroup
	}

	rungroup struct {
		runarr []cmn.Runner
		runmap map[string]cmn.Runner // redundant, named
		errCh  chan error
	}
)

//====================
//
// globals
//
//====================
var (
	daemon     = daemonCtx{}
	jsonCompat = jsoniter.ConfigCompatibleWithStandardLibrary
)

//====================
//
// rungroup
//
//====================
func (g *rungroup) add(r cmn.Runner, name string) {
	r.SetRunName(name)
	g.runarr = append(g.runarr, r)
	g.runmap[name] = r
}

func (g *rungroup) run() error {
	if len(g.runarr) == 0 {
		return nil
	}
	g.errCh = make(chan error, len(g.runarr))
	for _, r := range g.runarr {
		go func(r cmn.Runner) {
			err := r.Run()
			glog.Warningf("runner [%s] exited with err [%v]", r.GetRunName(), err)
			g.errCh <- err
		}(r)
	}

	// wait here for (any/first) runner termination
	err := <-g.errCh
	for _, r := range g.runarr {
		r.Stop(err)
	}
	for i := 0; i < len(g.runarr)-1; i++ {
		<-g.errCh
	}
	return err
}

func init() {
	// role aka `DaemonType`
	flag.StringVar(&daemon.cli.role, "role", "", "role of this AIS daemon: proxy | target")

	// config itself and its command line overrides
	flag.StringVar(&daemon.cli.config.ConfFile, "config", "", "config filename: local file that stores this daemon's configuration")
	flag.StringVar(&daemon.cli.config.LogLevel, "loglevel", "", "log verbosity level (2 - minimal, 3 - default, 4 - super-verbose)")
	flag.DurationVar(&daemon.cli.config.StatsTime, "stats_time", 0, "stats reporting (logging) interval")
	flag.StringVar(&daemon.cli.config.ProxyURL, "proxyurl", "", "primary proxy/gateway URL to override local configuration")
	flag.StringVar(&daemon.cli.confjson, "confjson", "", "JSON formatted \"{name: value, ...}\" string to override selected knob(s)")
	flag.BoolVar(&daemon.cli.persist, "persist", false,
		"true: apply command-line args to the configuration and save the latter to disk\nfalse: keep it transient (for this run only)")
	flag.BoolVar(&daemon.cli.skipStartup, "skipstartup", false,
		"determines if primary proxy should skip waiting for target registrations when starting up")
	flag.IntVar(&daemon.cli.ntargets, "ntargets", 0, "number of storage targets to expect at startup (hint, proxy-only)")
	// dry-run
	flag.BoolVar(&daemon.dryRun.disk, "nodiskio", false, "dry-run: if true, no disk operations for GET and PUT")
	flag.StringVar(&daemon.dryRun.sizeStr, "dryobjsize", "8m", "dry-run: in-memory random content")
}

// dry-run environment overrides dry-run clivars
func dryRunInit() {
	str := os.Getenv("AIS_NODISKIO")
	if b, err := cmn.ParseBool(str); err == nil {
		daemon.dryRun.disk = b
	}
	str = os.Getenv("AIS_DRYOBJSIZE")
	if str != "" {
		if size, err := cmn.S2B(str); size > 0 && err == nil {
			daemon.dryRun.size = size
		}
	}
	if daemon.dryRun.disk {
		warning := "Dry-run: disk IO will be disabled"
		fmt.Fprintf(os.Stderr, "%s\n", warning)
		glog.Infof("%s - in memory file size: %d (%s) bytes", warning, daemon.dryRun.size, daemon.dryRun.sizeStr)
	}
}

func initDaemon(version, build string) {
	var (
		err error
	)
	flag.Parse()
	if daemon.cli.role != cmn.Proxy && daemon.cli.role != cmn.Target {
		cmn.ExitInfof(
			"Invalid value of flag `role`: %q, expected %q or %q",
			daemon.cli.role, cmn.Proxy, cmn.Target,
		)
	}

	if daemon.dryRun.disk {
		daemon.dryRun.size, err = cmn.S2B(daemon.dryRun.sizeStr)
		if daemon.dryRun.size < 1 || err != nil {
			cmn.ExitLogf("Invalid object size: %d [%s]\n", daemon.dryRun.size, daemon.dryRun.sizeStr)
		}
	}
	if daemon.cli.config.ConfFile == "" {
		str := "Missing `config` flag pointing to configuration file (must be provided via command line)\n"
		str += "Usage: ... -role=<proxy|target> -config=<conf.json> ..."
		cmn.ExitLogf(str)
	}
	config, confChanged := jsp.LoadConfig(&daemon.cli.config)
	if confChanged {
		if err := jsp.Save(cmn.GCO.GetConfigFile(), config, jsp.Plain()); err != nil {
			cmn.ExitLogf("CLI %s: failed to write, err: %s", cmn.ActSetConfig, err)
		}
		glog.Infof("CLI %s: stored", cmn.ActSetConfig)
	}

	glog.Infof("git: %s | build-time: %s\n", version, build)

	containerized := sys.Containerized()
	cpus, limited := sys.NumCPU()
	memStat, _ := sys.Mem()
	if limited {
		glog.Infof("containerized=%t, number of CPUs is limited to %d", containerized, cpus)
	} else {
		glog.Infof("containerized=%t, using all %d CPUs", containerized, cpus)
	}
	glog.Infof("Memory total: %s, free: %s(actual free %s)",
		cmn.B2S(int64(memStat.Total), 0), cmn.B2S(int64(memStat.Free), 0), cmn.B2S(int64(memStat.ActualFree), 0))

	// Optimize GOMAXPROCS if the daemon is running inside a container with
	// limited amount of memory and CPU.
	sys.UpdateMaxProcs()

	// Initialize filesystem/mountpaths manager.
	fs.InitMountedFS()

	// NOTE: proxy and, respectively, target terminations are executed in the same
	//       exact order as the initializations below
	daemon.rg = &rungroup{
		runarr: make([]cmn.Runner, 0, 8),
		runmap: make(map[string]cmn.Runner, 8),
	}
	// system MMSAs
	daemon.gmm = &memsys.MMSA{Name: gmmName}
	_ = daemon.gmm.Init(true /*panic on error*/)
	daemon.smm = &memsys.MMSA{Name: smmName, Small: true}
	_ = daemon.smm.Init(true /* ditto */)
	daemon.gmm.Sibling, daemon.smm.Sibling = daemon.smm, daemon.gmm

	if daemon.cli.role == cmn.Proxy {
		p := &proxyrunner{}
		p.initSI(cmn.Proxy)
		p.initClusterCIDR()
		daemon.rg.add(p, cmn.Proxy)

		ps := &stats.Prunner{}
		psStartedUp := ps.Init(p)
		daemon.rg.add(ps, xproxystats)

		daemon.rg.add(newProxyKeepaliveRunner(p, ps, psStartedUp), xproxykeepalive)
		daemon.rg.add(newMetasyncer(p), xmetasyncer)
	} else {
		t := &targetrunner{}
		t.initSI(cmn.Target)
		t.initHostIP()
		daemon.rg.add(t, cmn.Target)

		ts := &stats.Trunner{T: t} // iostat below
		tsStartedUp := ts.Init(t)
		daemon.rg.add(ts, xstorstats)

		daemon.rg.add(newTargetKeepaliveRunner(t, ts, tsStartedUp), xtargetkeepalive)

		t.fsprg.init(t) // subgroup of the daemon.rg rungroup

		// Stream Collector - a singleton object with responsibilities that include:
		sc := transport.Init()
		daemon.rg.add(sc, xstreamc)

		// fs.Mountpaths must be inited prior to all runners that utilize them
		// for mountpath definition, see fs/mountfs.go
		if cmn.GCO.Get().TestingEnv() {
			glog.Infof("Warning: configuring %d fspaths for testing", config.TestFSP.Count)
			fs.Mountpaths.DisableFsIDCheck()
			t.testCachepathMounts()
		} else {
			fsPaths := make([]string, 0, len(config.FSpaths.Paths))
			for path := range config.FSpaths.Paths {
				fsPaths = append(fsPaths, path)
			}
			if err := fs.Mountpaths.Init(fsPaths); err != nil {
				cmn.ExitLogf("%s", err)
			}
		}

		fshc := health.NewFSHC(t, fs.Mountpaths, daemon.gmm, fs.CSM)
		daemon.rg.add(fshc, xfshc)

		housekeep, initialInterval := cluster.LomCacheHousekeep(daemon.gmm, t)
		hk.Housekeeper.Register("lom-cache", housekeep, initialInterval)
		_ = ts.UpdateCapacityOOS(nil) // goes after fs.Mountpaths.Init
	}
	daemon.rg.add(&sigrunner{}, xsignal)

	// even more config changes, e.g:
	// -config=/etc/ais.json -role=target -persist=true -confjson="{\"client_timeout\": \"13s\" }"
	if daemon.cli.confjson != "" {
		var nvmap cmn.SimpleKVs
		if err = jsoniter.Unmarshal([]byte(daemon.cli.confjson), &nvmap); err != nil {
			cmn.ExitLogf("Failed to unmarshal JSON [%s], err: %s", daemon.cli.confjson, err)
		}
		if err := jsp.SetConfigMany(nvmap); err != nil {
			cmn.ExitLogf("Failed to set config: %s", err)
		}
	}
}

// Run is the 'main' where everything gets started
func Run(version, build string) {
	// Always flush the logger (even in case of panic).
	defer glog.Flush()

	initDaemon(version, build)

	// Start all the runners
	err := daemon.rg.run()
	if err == nil {
		glog.Infoln("Terminated OK")
		return
	}
	if e, ok := err.(*signalError); ok {
		glog.Infof("Terminated OK (via signal: %s)\n", e.signal.String())
		exitCode := 128 + int(e.signal) // see: https://tldp.org/LDP/abs/html/exitcodes.html
		cmn.ExitWithCode(exitCode)
		return
	}
	cmn.ExitLogf("Terminated with err: %s", err)
}

//==================
//
// global helpers
//
//==================
func getproxystatsrunner() *stats.Prunner {
	r := daemon.rg.runmap[xproxystats]
	rr, ok := r.(*stats.Prunner)
	cmn.Assert(ok)
	return rr
}

func getproxykeepalive() *proxyKeepaliveRunner {
	r := daemon.rg.runmap[xproxykeepalive]
	rr, ok := r.(*proxyKeepaliveRunner)
	cmn.Assert(ok)
	return rr
}

func gettargetkeepalive() *targetKeepaliveRunner {
	r := daemon.rg.runmap[xtargetkeepalive]
	rr, ok := r.(*targetKeepaliveRunner)
	cmn.Assert(ok)
	return rr
}

func getstorstatsrunner() *stats.Trunner {
	r := daemon.rg.runmap[xstorstats]
	rr, ok := r.(*stats.Trunner)
	cmn.Assert(ok)
	return rr
}

func getmetasyncer() *metasyncer {
	r := daemon.rg.runmap[xmetasyncer]
	rr, ok := r.(*metasyncer)
	cmn.Assert(ok)
	return rr
}

func getfshealthchecker() *health.FSHC {
	r := daemon.rg.runmap[xfshc]
	rr, ok := r.(*health.FSHC)
	cmn.Assert(ok)
	return rr
}
