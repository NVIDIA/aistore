// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ext/dload"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/space"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/NVIDIA/aistore/xact/xs"
)

const usecli = " -role=<proxy|target> -config=</dir/config.json> -local_config=</dir/local-config.json> ..."

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
		transient bool // true: keep command-line provided `-config-custom` settings in memory only
		target    struct {
			// do not try to auto-join cluster upon startup - stand by and wait for admin request
			standby bool
			// allow: disk sharing by multiple mountpaths and mountpaths with no disks whatsoever
			// (usage: testing, minikube env, etc.)
			allowSharedDisksAndNoDisks bool
			// force starting up with a lost or missing mountpath
			startWithLostMountpath bool
			// use loopback devices
			useLoopbackDevs bool
		}
		usage bool // show usage and exit
	}
	runRet struct {
		name string
		err  error
	}
	rungroup struct {
		rs    map[string]cos.Runner
		errCh chan runRet
	}
)

var daemon = daemonCtx{}

func init() {
	// role aka `DaeType`
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
		"false: store customized (via '-config_custom') configuration\ntrue: keep '-config_custom' settings in memory only (non-persistent)")
	flag.BoolVar(&daemon.cli.usage, "h", false, "show usage and exit")

	// target-only
	flag.BoolVar(&daemon.cli.target.standby, "standby", false, "when starting up, do not try to auto-join cluster - stand by and wait for admin request (target-only)")
	flag.BoolVar(&daemon.cli.target.allowSharedDisksAndNoDisks, "allow_shared_no_disks", false, "disk sharing by multiple mountpaths and mountpaths with no disks whatsoever (target-only)")
	flag.BoolVar(&daemon.cli.target.useLoopbackDevs, "loopback", false, "use loopback devices (local playground, target-only)")
	flag.BoolVar(&daemon.cli.target.startWithLostMountpath, "start_with_lost_mountpath", false, "force starting up with a lost or missing mountpath (target-only)")

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
	if daemon.cli.usage || len(os.Args) == 1 {
		fmt.Fprintln(os.Stderr, "  Usage: "+os.Args[0]+usecli+"\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\n  Version %s (build: %s)\n", version, buildTime)
		fmt.Fprintln(os.Stderr, "  Usage:\n\t"+os.Args[0]+usecli)
		os.Exit(0)
	}
	if len(os.Args) == 2 && os.Args[1] == "version" {
		fmt.Fprintf(os.Stderr, "version %s (build: %s)\n", version, buildTime)
		os.Exit(0)
	}
	if daemon.cli.role != apc.Proxy && daemon.cli.role != apc.Target {
		cos.ExitLogf("Invalid daemon's role %q, expecting %q or %q", daemon.cli.role, apc.Proxy, apc.Target)
	}
	if daemon.cli.globalConfigPath == "" {
		cos.ExitLogf(erfm, "config")
	}
	if daemon.cli.localConfigPath == "" {
		cos.ExitLogf(erfm, "local-config")
	}

	// TODO: detect graceful shutdown, delete shutdown marker

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
		if err := setConfigInMem(toUpdate, config, apc.Daemon); err != nil {
			cos.ExitLogf("Failed to update config in memory: %v", err)
		}

		overrideConfig := cmn.GCO.MergeOverrideConfig(toUpdate)
		if !daemon.cli.transient {
			if err = cmn.SaveOverrideConfig(config.ConfigDir, overrideConfig); err != nil {
				cos.ExitLogf("Failed to save override config: %v", err)
			}
		}
	}

	daemon.version, daemon.buildTime = version, buildTime
	loghdr := fmt.Sprintf("Version %s, build time %s, debug %t", version, buildTime, debug.ON())
	glog.Infoln(loghdr)
	cmn.AppGloghdr(loghdr)
	cpus := sys.NumCPU()
	if containerized := sys.Containerized(); containerized {
		loghdr = fmt.Sprintf("CPUs(%d, runtime=%d), containerized", cpus, runtime.NumCPU())
	} else {
		loghdr = fmt.Sprintf("CPUs(%d, runtime=%d)", cpus, runtime.NumCPU())
	}
	glog.Infoln(loghdr)
	sys.SetMaxProcs()

	daemon.rg = &rungroup{rs: make(map[string]cos.Runner, 8)}
	hk.Init(&daemon.stopping)
	daemon.rg.add(hk.DefaultHK)

	// reg xaction factories
	xreg.Init()
	xs.Xreg()

	// fork (proxy | target)
	co := newConfigOwner(config)
	if daemon.cli.role == apc.Proxy {
		p := newProxy(co)
		p.init(config)
		cmn.AppGloghdr("Node: " + p.si.Name() + ", " + loghdr)
		cmn.SetNodeName(p.si.Name())
		return p
	}

	// reg more xaction factories
	space.Xreg()
	dload.Xreg()

	t := newTarget(co)
	t.init(config)
	cmn.AppGloghdr("Node: " + t.si.Name() + ", " + loghdr)
	cmn.SetNodeName(t.si.Name())

	return t
}

func newProxy(co *configOwner) *proxy {
	p := &proxy{}
	p.name = apc.Proxy
	p.owner.config = co
	return p
}

func newTarget(co *configOwner) *target {
	t := &target{backend: make(backends, 8)}
	t.name = apc.Target
	t.owner.bmd = newBMDOwnerTgt()
	t.owner.etl = newEtlMDOwnerTgt()
	t.owner.config = co
	return t
}

// Run is the 'main' where everything gets started
func Run(version, buildTime string) int {
	defer glog.Flush() // always flush

	rmain := initDaemon(version, buildTime)
	err := daemon.rg.runAll(rmain)

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

func (g *rungroup) run(r cos.Runner) {
	err := r.Run()
	if err != nil {
		glog.Warningf("runner [%s] exited with err [%v]", r.Name(), err)
	}
	g.errCh <- runRet{r.Name(), err}
}

func (g *rungroup) runAll(mainRunner cos.Runner) error {
	g.errCh = make(chan runRet, len(g.rs))
	daemon.stopping.Store(false)

	// run all, housekeeper first
	go g.run(hk.DefaultHK)
	runtime.Gosched()
	hk.WaitStarted()
	for _, r := range g.rs {
		if r.Name() == hk.DefaultHK.Name() {
			continue
		}
		go g.run(r)
	}

	// Stop all runners, target (or proxy) first.
	ret := <-g.errCh
	daemon.stopping.Store(true)
	if ret.name != mainRunner.Name() {
		mainRunner.Stop(ret.err)
	}
	for _, r := range g.rs {
		if r.Name() != mainRunner.Name() {
			r.Stop(ret.err)
		}
	}
	// Wait for all terminations.
	for i := 0; i < len(g.rs)-1; i++ {
		<-g.errCh
	}
	return ret.err
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

func genDaemonID(daemonType string, config *cmn.Config) string {
	if !config.TestingEnv() {
		return cos.GenDaemonID()
	}
	switch daemonType {
	case apc.Target:
		return cos.GenTestingDaemonID(fmt.Sprintf("t%d", config.HostNet.Port))
	case apc.Proxy:
		return cos.GenTestingDaemonID(fmt.Sprintf("p%d", config.HostNet.Port))
	}
	cos.AssertMsg(false, daemonType)
	return ""
}
