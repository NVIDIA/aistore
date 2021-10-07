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
	"runtime"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/sys"
)

const usecli = `
   Usage:
        aisnode -role=<proxy|target> -config=</dir/config.json> -local_config=</dir/local-config.json> ...`

type (
	daemonCtx struct {
		cli       cliFlags
		rg        *rungroup
		version   string      // major.minor.build (see cmd/aisnode)
		buildTime string      // YYYY-MM-DD HH:MM:SS-TZ
		stopping  atomic.Bool // true when exiting
		resilver  struct {
			reason   string // Reason why resilver needs to be run.
			required bool   // Determines if the resilver needs to be started.
		}
	}
	cliFlags struct {
		localConfigPath  string // path to local config
		globalConfigPath string // path to global config
		role             string // proxy | target
		daemonID         string // daemon ID to assign
		confCustom       string // "key1=value1,key2=value2" formatted to override selected entries in config
		primary          struct {
			ntargets    int  // expected number of targets in a starting-up cluster
			skipStartup bool // determines if primary should skip waiting for targets to join
		}
		transient        bool // false: make cmn.ConfigCLI settings permanent, true: leave them transient
		overrideBackends bool // if true primary will metasync backends from deployment-time plain-text config
		standby          bool // do not try to join cluster automatically upon startup - standby and wait for request
		usage            bool // show usage and exit
	}
	rungroup struct {
		rs    map[string]cos.Runner
		errCh chan error
	}
)

var daemon = daemonCtx{}

func init() {
	// role aka `DaemonType`
	flag.StringVar(&daemon.cli.role, "role", "", "_role_ of this aisnode: 'proxy' OR 'target'")
	flag.StringVar(&daemon.cli.daemonID, "daemon_id", "", "user-specified node ID (advanced usage only!)")

	// config itself and its command line overrides
	flag.StringVar(&daemon.cli.globalConfigPath, "config", "",
		"config filename: local file that stores the global cluster configuration")
	flag.StringVar(&daemon.cli.localConfigPath, "local_config", "",
		"config filename: local file that stores daemon's local configuration")
	flag.StringVar(&daemon.cli.confCustom, "config_custom", "",
		"\"key1=value1,key2=value2\" formatted string to override selected entries in config")
	flag.BoolVar(&daemon.cli.transient, "transient", false,
		"false: store customized (via config_custom) configuration\ntrue: runtime only (non-persistent)")
	flag.BoolVar(&daemon.cli.usage, "h", false, "show usage and exit")
	flag.BoolVar(&daemon.cli.overrideBackends, "override_backends", false, "configure remote backends at deployment time (and override previously stored configuration)")
	flag.BoolVar(&daemon.cli.standby, "standby", false, "when starting up, do not try to join cluster - standby and wait for user request")

	// primary-only:
	flag.IntVar(&daemon.cli.primary.ntargets, "ntargets", 0, "number of storage targets expected to be joining at startup (optional, primary-only)")
	flag.BoolVar(&daemon.cli.primary.skipStartup, "skip_startup", false,
		"whether primary, when starting up, should skip waiting for target joins (used only in tests)")
}

func initDaemon(version, buildTime string) cos.Runner {
	const erfm = "Missing `%s` flag pointing to configuration file (must be provided via command line)\n"
	var (
		config *cmn.Config
		err    error
	)
	flag.Parse()
	if daemon.cli.usage || len(os.Args[1:]) == 0 {
		flag.Usage()
		cos.Exitf(usecli)
	}
	if daemon.cli.role != cmn.Proxy && daemon.cli.role != cmn.Target {
		cos.ExitLogf("Invalid daemon's role %q, expecting %q or %q", daemon.cli.role, cmn.Proxy, cmn.Target)
	}
	if daemon.cli.globalConfigPath == "" {
		cos.ExitLogf(erfm, "config")
	}
	if daemon.cli.localConfigPath == "" {
		cos.ExitLogf(erfm, "local-config")
	}

	// Cleanup shutdown marker if exists.
	deleteShutdownMarker()

	config = &cmn.Config{}
	err = cmn.LoadConfig(daemon.cli.globalConfigPath, daemon.cli.localConfigPath, daemon.cli.role, config)
	if err != nil {
		cos.ExitLogf("%v", err)
	}
	cmn.GCO.Put(config)

	// Examples overriding default configuration at a node startup via command line:
	// 1) set client timeout to 13s and store the updated value on disk:
	// $ aisnode -config=/etc/ais.json -local_config=/etc/ais_local.json -role=target \
	//   -config_custom="client.client_timeout=13s"
	//
	// 2) same as above except that the new timeout will remain transient
	//    (won't persist across restarts):
	// $ aisnode -config=/etc/ais.json -local_config=/etc/ais_local.json -role=target -transient=true \
	//   -config_custom="client.client_timeout=13s"
	if daemon.cli.confCustom != "" {
		var (
			toUpdate = &cmn.ConfigToUpdate{}
			kvs      = strings.Split(daemon.cli.confCustom, ",")
		)
		if err := toUpdate.FillFromKVS(kvs); err != nil {
			cos.ExitLogf(err.Error())
		}
		if err := cmn.SetConfigInMem(toUpdate, config, cmn.Daemon); err != nil {
			cos.ExitLogf("Failed to update config in memory: %v", err)
		}

		overrideConfig := cmn.GCO.GetOverrideConfig()
		if overrideConfig == nil {
			overrideConfig = toUpdate
		} else {
			overrideConfig.Merge(toUpdate)
		}

		if !daemon.cli.transient {
			if err = cmn.SaveOverrideConfig(config.ConfigDir, overrideConfig); err != nil {
				cos.ExitLogf("Failed to save override config: %v", err)
			}
		}
	}

	daemon.version, daemon.buildTime = version, buildTime
	glog.Infof("| version: %s | build-time: %s |\n", version, buildTime)
	debug.Errorln("starting with debug asserts/logs")

	containerized := sys.Containerized()
	cpus := sys.NumCPU()
	glog.Infof("num CPUs(%d, %d), container %t", cpus, runtime.NumCPU(), containerized)
	sys.SetMaxProcs()

	memStat, err := sys.Mem()
	debug.AssertNoErr(err)
	glog.Infof("Memory total: %s, free: %s(actual free %s)",
		cos.B2S(int64(memStat.Total), 0), cos.B2S(int64(memStat.Free), 0), cos.B2S(int64(memStat.ActualFree), 0))

	daemon.rg = &rungroup{rs: make(map[string]cos.Runner, 8)}
	daemon.rg.add(hk.DefaultHK)

	co := newConfigOwner(config)
	if daemon.cli.role == cmn.Proxy {
		p := newProxy(co)
		p.init(config)
		glog.Infoln("Node", p.si.String())
		glog.GetNodeName = p.si.Name
		return p
	}
	t := newTarget(co)
	t.init(config)
	glog.Infoln("Node", t.si.String())
	glog.GetNodeName = t.si.Name

	return t
}

func newProxy(co *configOwner) *proxyrunner {
	p := &proxyrunner{}
	p.name = cmn.Proxy
	p.owner.config = co
	return p
}

func newTarget(co *configOwner) *targetrunner {
	t := &targetrunner{backend: make(backends, 8)}
	t.name = cmn.Target
	t.gfn.local.tag, t.gfn.global.tag = "local GFN", "global GFN"
	t.owner.bmd = newBMDOwnerTgt()
	t.owner.config = co
	return t
}

// Run is the 'main' where everything gets started
func Run(version, buildTime string) int {
	defer glog.Flush() // always flush

	rmain := initDaemon(version, buildTime)
	err := daemon.rg.run(rmain)

	if err == nil {
		glog.Infoln("Terminated OK")
		return 0
	}
	if e, ok := err.(*cos.ErrSignal); ok {
		glog.Infof("Terminated OK via %v", e)
		return e.ExitCode()
	}
	if errors.Is(err, cmn.ErrStartupTimeout) {
		// NOTE: stats and keepalive runners wait for the ClusterStarted() - i.e., for the primary
		//       to reach the corresponding stage. There must be an external "restarter" (e.g. K8s)
		//       to restart the daemon if the primary gets killed or panics prior (to reaching that state)
		glog.Errorln("Timed-out while starting up")
	}
	glog.Errorf("Terminated with err: %v", err)
	return 1
}

//////////////
// rungroup //
//////////////

func (g *rungroup) add(r cos.Runner) {
	cos.Assert(r.Name() != "")
	_, exists := g.rs[r.Name()]
	cos.Assert(!exists)

	g.rs[r.Name()] = r
}

func (g *rungroup) run(mainRunner cos.Runner) error {
	var mainDone atomic.Bool
	g.errCh = make(chan error, len(g.rs))
	daemon.stopping.Store(false)
	for _, r := range g.rs {
		go func(r cos.Runner) {
			err := r.Run()
			if err != nil {
				glog.Warningf("runner [%s] exited with err [%v]", r.Name(), err)
			}
			if r.Name() == mainRunner.Name() {
				mainDone.Store(true) // load it only once
			}
			g.errCh <- err
		}(r)
	}

	// Stop all runners, target (or proxy) first.
	err := <-g.errCh
	daemon.stopping.Store(true)
	if !mainDone.Load() {
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

///////////////
// daemon ID //
///////////////

const (
	daemonIDEnv = "AIS_DAEMON_ID"
)

func envDaemonID(daemonType string) (daemonID string) {
	if daemon.cli.daemonID != "" {
		glog.Warningf("%s[%q] ID from command-line", daemonType, daemon.cli.daemonID)
		return daemon.cli.daemonID
	}
	if daemonID = os.Getenv(daemonIDEnv); daemonID != "" {
		glog.Warningf("%s[%q] ID from env", daemonType, daemonID)
	}
	return
}

func generateDaemonID(daemonType string, config *cmn.Config) string {
	if !config.TestingEnv() {
		return cos.GenDaemonID()
	}
	daemonID := cos.RandStringStrong(4)
	switch daemonType {
	case cmn.Target:
		return fmt.Sprintf("%st%d", daemonID, config.HostNet.Port)
	case cmn.Proxy:
		return fmt.Sprintf("%sp%d", daemonID, config.HostNet.Port)
	}
	cos.AssertMsg(false, daemonType)
	return ""
}
