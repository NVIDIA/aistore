// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/health"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/transport"
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
		sizeStr string // random content size used when disk IO is disabled (-dryobjsize/AIS_DRY_OBJ_SIZE)
		size    int64  // as above converted to bytes from a string like '8m'
		disk    bool   // dry-run disk (-nodiskio/AIS_NO_DISK_IO)
	}

	cliFlags struct {
		role        string // proxy | target
		confPath    string // path to config
		confCustom  string // "key1=value1,key2=value2" formatted string to override selected entries in config
		ntargets    int    // expected number of targets in a starting-up cluster (proxy only)
		skipStartup bool   // determines if the proxy should skip waiting for targets
		transient   bool   // false: make cmn.ConfigCLI settings permanent, true: leave them transient
	}

	// daemon instance: proxy or storage target
	daemonCtx struct {
		cli    cliFlags
		dryRun dryRunConfig
		rg     *rungroup
	}

	rungroup struct {
		rs    map[string]cmn.Runner
		errCh chan error
	}
)

// globals
var (
	daemon = daemonCtx{}
)

func (g *rungroup) add(r cmn.Runner) {
	cmn.Assert(r.Name() != "")
	_, exists := g.rs[r.Name()]
	cmn.Assert(!exists)

	g.rs[r.Name()] = r
}

func (g *rungroup) run(mainRunner cmn.Runner) error {
	var rdone cmn.Runner
	g.errCh = make(chan error, len(g.rs))
	for _, r := range g.rs {
		go func(r cmn.Runner) {
			err := r.Run()
			if err != nil {
				glog.Warningf("runner [%s] exited with err [%v]", r.Name(), err)
			}
			rdone = r
			g.errCh <- err
		}(r)
	}

	// Stop all runners, target (or proxy) first.
	err := <-g.errCh
	if rdone.Name() != mainRunner.Name() {
		mainRunner.Stop(err)
	}
	for _, r := range g.rs {
		if r.Name() != mainRunner.Name() {
			r.Stop(err)
		}
	}
	// Wait for all terminations.
	for i := 0; i < len(g.rs)-1; i++ {
		<-g.errCh
	}
	return err
}

func init() {
	// role aka `DaemonType`
	flag.StringVar(&daemon.cli.role, "role", "", "role of this AIS daemon: proxy | target")

	// config itself and its command line overrides
	flag.StringVar(&daemon.cli.confPath, "config", "",
		"config filename: local file that stores this daemon's configuration")
	flag.StringVar(&daemon.cli.confCustom, "config_custom", "",
		"\"key1=value1,key2=value2\" formatted string to override selected entries in config")

	flag.BoolVar(&daemon.cli.transient, "transient", false,
		"false: apply command-line args to the configuration and save the latter to disk\ntrue: keep it transient (for this run only)")

	flag.BoolVar(&daemon.cli.skipStartup, "skip_startup", false,
		"determines if primary proxy should skip waiting for target registrations when starting up")
	flag.IntVar(&daemon.cli.ntargets, "ntargets", 0, "number of storage targets to expect at startup (hint, proxy-only)")

	// dry-run
	flag.BoolVar(&daemon.dryRun.disk, "nodiskio", false, "dry-run: if true, no disk operations for GET and PUT")
	flag.StringVar(&daemon.dryRun.sizeStr, "dryobjsize", "8m", "dry-run: in-memory random content")
}

// dry-run environment overrides dry-run clivars
func dryRunInit() {
	str := os.Getenv("AIS_NO_DISK_IO")
	if b, err := cmn.ParseBool(str); err == nil {
		daemon.dryRun.disk = b
	}
	str = os.Getenv("AIS_DRY_OBJ_SIZE")
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

func initDaemon(version, build string) (rmain cmn.Runner) {
	var err error
	flag.Parse()
	if daemon.cli.role != cmn.Proxy && daemon.cli.role != cmn.Target {
		cmn.ExitLogf(
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
	if daemon.cli.confPath == "" {
		str := "Missing `config` flag pointing to configuration file (must be provided via command line)\n"
		str += "Usage: aisnode -role=<proxy|target> -config=</dir/config.json> ..."
		cmn.ExitLogf(str)
	}
	jsp.MustLoadConfig(daemon.cli.confPath)

	// even more config changes, e.g:
	// -config=/etc/ais.json -role=target -persist=true -config_custom="client.timeout=13s,
	// proxy.primary_url=https://localhost:10080"
	if daemon.cli.confCustom != "" {
		var (
			nvmap = make(cmn.SimpleKVs, 10)
			kvs   = strings.Split(daemon.cli.confCustom, ",")
		)
		for _, kv := range kvs {
			entry := strings.SplitN(kv, "=", 2)
			if len(entry) != 2 {
				cmn.ExitLogf("Failed to parse `-config_custom` flag (invalid entry: %q)", kv)
			}
			nvmap[entry[0]] = entry[1]
		}
		if err := jsp.SetConfigMany(nvmap); err != nil {
			cmn.ExitLogf("Failed to set config: %s", err)
		}
	}
	if !daemon.cli.transient {
		if err := jsp.SaveConfig(cmn.ActTransient); err != nil {
			cmn.ExitLogf("Failed to save config: %v", err)
		}
	}

	glog.Infof("git: %s | build-time: %s\n", version, build)

	if debug.Enabled {
		debug.Errorf("starting with debug asserts/logs")
	}

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

	// NOTE: Daemon terminations get executed in the same exact order as initializations below.
	daemon.rg = &rungroup{rs: make(map[string]cmn.Runner, 8)}

	daemon.rg.add(hk.DefaultHK)
	if daemon.cli.role == cmn.Proxy {
		rmain = initProxy()
	} else {
		rmain = initTarget()
	}
	return
}

func initProxy() cmn.Runner {
	p := &proxyrunner{gmm: &memsys.MMSA{Name: gmmName}}
	_ = p.gmm.Init(true /*panicOnErr*/)
	p.initSI(cmn.Proxy)

	// Persist daemon ID on disk
	if err := writeProxyDID(cmn.GCO.Get(), p.si.ID()); err != nil {
		cmn.ExitLogf("%v", err)
	}

	p.initClusterCIDR()
	daemon.rg.add(p)

	ps := &stats.Prunner{}
	startedUp := ps.Init(p)
	daemon.rg.add(ps)
	p.statsT = ps

	k := newProxyKeepaliveRunner(p, ps, startedUp)
	daemon.rg.add(k)
	p.keepalive = k

	m := newMetasyncer(p)
	daemon.rg.add(m)
	p.metasyncer = m
	return p
}

func newTarget() *targetrunner {
	t := &targetrunner{
		gmm:   &memsys.MMSA{Name: gmmName},
		smm:   &memsys.MMSA{Name: smmName, Small: true},
		cloud: make(clouds, 8),
	}
	t.gfn.local.tag, t.gfn.global.tag = "local GFN", "global GFN"
	t.owner.bmd = newBMDOwnerTgt()
	return t
}

func initTarget() cmn.Runner {
	// Initialize filesystem/mountpaths manager.
	fs.Init()

	t := newTarget()
	_ = t.gmm.Init(true /*panicOnErr*/)
	_ = t.smm.Init(true /*panicOnErr*/)
	t.gmm.Sibling, t.smm.Sibling = t.smm, t.gmm

	// fs.Mountpaths must be inited prior to all runners that utilize them
	// for mountpath definition, see fs/mountfs.go
	config := cmn.GCO.Get()
	if config.TestingEnv() {
		glog.Infof("Warning: configuring %d fspaths for testing", config.TestFSP.Count)
		fs.DisableFsIDCheck()
		t.testCachepathMounts()
	}

	t.initSI(cmn.Target)

	config = cmn.GCO.Get()
	fsPaths := config.FSpaths.Paths.Keys()
	if err := fs.SetMountpaths(fsPaths, t.si.ID()); err != nil {
		cmn.ExitLogf("%s", err)
	}

	if _, err := fs.CreateNewVMD(t.si.ID()); err != nil {
		cmn.ExitLogf("%v", err)
	}

	t.initHostIP()
	daemon.rg.add(t)

	ts := &stats.Trunner{T: t} // iostat below
	startedUp := ts.Init(t)
	daemon.rg.add(ts)
	t.statsT = ts

	k := newTargetKeepaliveRunner(t, ts, startedUp)
	daemon.rg.add(k)
	t.keepalive = k

	t.fsprg.init(t) // subgroup of the daemon.rg rungroup

	// Stream Collector - a singleton object with responsibilities that include:
	sc := transport.Init()
	daemon.rg.add(sc)

	fshc := health.NewFSHC(t, t.gmm, fs.CSM)
	daemon.rg.add(fshc)
	t.fshc = fshc

	housekeep, initialInterval := cluster.LomCacheHousekeep(t.gmm, t)
	hk.Reg("lom-cache", housekeep, initialInterval)
	if err := ts.InitCapacity(); err != nil { // goes after fs.Init
		cmn.ExitLogf("%s", err)
	}
	return t
}

// Run is the 'main' where everything gets started
func Run(version, build string) int {
	defer glog.Flush() // always flush

	rmain := initDaemon(version, build)
	err := daemon.rg.run(rmain)

	// NOTE: This must be done *after* `rg.run()` so we don't remove
	//  marker on panic (which can happen in `rg.run()`).
	defer fs.RemoveMarker(fs.NodeRestartedMarker)

	if err == nil {
		glog.Infoln("Terminated OK")
		return 0
	}
	if e, ok := err.(*cmn.SignalError); ok {
		glog.Infof("Terminated OK (via signal: %v)\n", e)
		return e.ExitCode()
	}
	if errors.Is(err, cmn.ErrStartupTimeout) {
		// NOTE: stats and keepalive runners wait for the ClusterStarted() - i.e., for the primary
		//       to reach the corresponding stage. There must be an external "restarter" (e.g. K8s)
		//       to restart the daemon if the primary gets killed or panics prior (to reaching that state)
		glog.Errorln("Timed-out while starting up")
	}
	glog.Errorf("Terminated with err: %s", err)
	return 1
}
