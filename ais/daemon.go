// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/cmn/tls"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
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
		version   string // major.minor.build (see cmd/aisnode)
		buildTime string // YYYY-MM-DD HH:MM:SS-TZ
		EP        string // env "AIS_PRIMARY_EP"
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

func initFlags(flset *flag.FlagSet) {
	// role aka `DaeType`
	flset.StringVar(&daemon.cli.role, "role", "", "_role_ of this aisnode: 'proxy' OR 'target'")
	flset.StringVar(&daemon.cli.daemonID, "daemon_id", "", "user-specified node ID (advanced usage only!)")

	// config itself and its command line overrides
	flset.StringVar(&daemon.cli.globalConfigPath, "config", "",
		"config filename: local file that stores the global cluster configuration")
	flset.StringVar(&daemon.cli.localConfigPath, "local_config", "",
		"config filename: local file that stores daemon's local configuration")
	flset.StringVar(&daemon.cli.confCustom, "config_custom", "",
		"\"key1=value1,key2=value2\" formatted string to override selected entries in config")
	flset.BoolVar(&daemon.cli.transient, "transient", false, "false: store customized (via '-config_custom') configuration\n"+
		"true: keep '-config_custom' settings in memory only (non-persistent)")
	flset.BoolVar(&daemon.cli.usage, "h", false, "show usage and exit")

	// target-only
	flset.BoolVar(&daemon.cli.target.standby, "standby", false,
		"when starting up, do not try to auto-join cluster - stand by and wait for admin request (target-only)")
	flset.BoolVar(&cmn.AllowSharedDisksAndNoDisks, "allow_shared_no_disks", false,
		"NOTE: deprecated, will be removed in future releases")
	flset.BoolVar(&daemon.cli.target.useLoopbackDevs, "loopback", false,
		"use loopback devices (local playground, target-only)")
	flset.BoolVar(&daemon.cli.target.startWithLostMountpath, "start_with_lost_mountpath", false,
		"force starting up with a lost or missing mountpath (target-only)")

	// primary-only:
	flset.IntVar(&daemon.cli.primary.ntargets, "ntargets", 0,
		"number of storage targets expected to be joining at startup (optional, primary-only)")
	flset.BoolVar(&daemon.cli.primary.skipStartup, "skip_startup", false,
		"whether primary, when starting up, should skip waiting for target joins (used only in tests)")
}

func initDaemon(version, buildTime string) cos.Runner {
	const erfm = "Missing `%s` flag pointing to configuration file (must be provided via command line)\n"
	var (
		flset  *flag.FlagSet
		config *cmn.Config
		err    error
	)
	// flags
	flset = flag.NewFlagSet(os.Args[0], flag.ExitOnError) // discard flags of imported packages
	initFlags(flset)
	flset.Parse(os.Args[1:])
	if daemon.cli.usage || len(os.Args) == 1 {
		fmt.Fprintln(os.Stderr, "  Usage: "+os.Args[0]+usecli+"\n")
		flset.PrintDefaults()
		fmt.Fprintln(os.Stderr, "  ---")
		fmt.Fprintf(os.Stderr, "  Version %s (build: %s)\n", version, buildTime)
		fmt.Fprintln(os.Stderr, "  Usage:\n\t"+os.Args[0]+usecli)
		os.Exit(0)
	}
	if len(os.Args) == 2 && os.Args[1] == "version" {
		fmt.Fprintf(os.Stderr, "version %s (build: %s)\n", version, buildTime)
		os.Exit(0)
	}
	os.Args = []string{os.Args[0]}
	flag.Parse() // so that imported packages don't complain

	// validation
	if daemon.cli.role != apc.Proxy && daemon.cli.role != apc.Target {
		cos.ExitLogf("invalid node's role %q, expecting %q or %q", daemon.cli.role, apc.Proxy, apc.Target)
	}
	if daemon.cli.globalConfigPath == "" {
		cos.ExitLogf(erfm, "config")
	}
	if daemon.cli.localConfigPath == "" {
		cos.ExitLogf(erfm, "local-config")
	}

	// config
	config = &cmn.Config{}
	err = cmn.LoadConfig(daemon.cli.globalConfigPath, daemon.cli.localConfigPath, daemon.cli.role, config)
	if err != nil {
		cos.ExitLog(err)
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
	// 3) e.g. updating log level:
	//   -config_custom="log.level=4611"
	//   (once done, `ais config node ... inherited log` will show "3 (modules: ec,xs)")
	if daemon.cli.confCustom != "" {
		var (
			toUpdate = &cmn.ConfigToSet{}
			kvs      = strings.Split(daemon.cli.confCustom, ",")
		)
		if err := toUpdate.FillFromKVS(kvs); err != nil {
			cos.ExitLog(err)
		}
		if err := setConfigInMem(toUpdate, config, apc.Daemon); err != nil {
			cos.ExitLogf("failed to update config in memory: %v", err)
		}

		overrideConfig := cmn.GCO.MergeOverride(toUpdate)
		if !daemon.cli.transient {
			if err = cmn.SaveOverrideConfig(config.ConfigDir, overrideConfig); err != nil {
				cos.ExitLogf("failed to save 'override' config: %v", err)
			}
		}
	}

	daemon.version, daemon.buildTime = version, buildTime
	loghdr := _loghdr()
	sys.GoEnvMaxprocs()

	daemon.rg = &rungroup{rs: make(map[string]cos.Runner, 6)}
	hk.Init()
	daemon.rg.add(hk.DefaultHK)

	// K8s
	k8s.Init()

	// declared xactions, as per xact/api.go
	xreg.Init()

	if config.Net.HTTP.UseHTTPS {
		err = tls.Init(config.Net.HTTP.Certificate, config.Net.HTTP.CertKey)
		if err != nil {
			cos.ExitLogf("failed to initialize Certificate Manager: %v", err)
		}
	}

	// primary 'host[:port]' endpoint or URL from the environment
	if daemon.EP = os.Getenv(env.AIS.PrimaryEP); daemon.EP != "" {
		scheme := "http"
		if config.Net.HTTP.UseHTTPS {
			scheme = "https"
		}
		if strings.Contains(daemon.EP, "://") {
			u, err := url.Parse(daemon.EP)
			if err != nil {
				cos.ExitLogf("invalid environment %s=%s: %v", env.AIS.PrimaryEP, daemon.EP, err)
			}
			if u.Path != "" && u.Path != "/" {
				cos.ExitLogf("invalid environment %s=%s (not expecting path %q)",
					env.AIS.PrimaryEP, daemon.EP, u.Path)
			}
			// reassemble and compare
			ustr := scheme + "://" + u.Hostname()
			if port := u.Port(); port != "" {
				ustr += ":" + port
			}
			if ustr != daemon.EP {
				nlog.Warningln("environment-set primary URL mismatch:", daemon.EP, "vs", ustr)
				daemon.EP = ustr
			}
		} else {
			daemon.EP = scheme + "://" + daemon.EP
		}
	}

	// fork (proxy | target)
	co := newConfigOwner(config)
	if daemon.cli.role == apc.Proxy {
		xs.Xreg(true /* x-ele only */)
		p := newProxy(co)
		p.init(config)
		title := _loghdr2(p.si, loghdr)
		nlog.Infoln(title)

		// aux plumbing
		nlog.SetTitle(title)
		cmn.InitErrs(p.si.Name(), nil)
		return p
	}

	// reg xaction factories
	xs.Xreg(false /* x-ele only */)
	space.Xreg()

	t := newTarget(co)
	t.init(config)
	title := _loghdr2(t.si, loghdr)
	nlog.Infoln(title)

	// aux plumbing
	nlog.SetTitle(title)
	cmn.InitErrs(t.si.Name(), fs.CleanPathErr)

	return t
}

func _loghdr2(si *meta.Snode, loghdr string) string {
	var sb strings.Builder
	sb.WriteString("Node ")
	sb.WriteString(si.Name())
	sb.WriteString(", ")
	sb.WriteString(loghdr)
	sb.WriteString("\n")
	return sb.String()
}

func _loghdr() (loghdr string) {
	var sb strings.Builder
	sb.WriteString("Version ")
	sb.WriteString(daemon.version)
	if debug.ON() {
		sb.WriteString(", DEBUG build ")
	} else {
		sb.WriteString(", build ")
	}
	sb.WriteString(daemon.buildTime)

	cpus := sys.NumCPU()
	sb.WriteString(", CPUs(")
	sb.WriteString(strconv.Itoa(cpus))
	sb.WriteString(", runtime=")
	sb.WriteString(strconv.Itoa(runtime.NumCPU()))
	sb.WriteByte(')')

	if sys.Containerized() {
		sb.WriteString(", containerized")
	}
	loghdr = sb.String()
	nlog.Infoln(loghdr) // redundant (see below), prior to start/init
	return loghdr
}

func newProxy(co *configOwner) *proxy {
	p := &proxy{}
	p.owner.config = co
	return p
}

func newTarget(co *configOwner) *target {
	t := &target{backend: make(backends, 8)}
	t.owner.bmd = newBMDOwnerTgt()
	t.owner.etl = newEtlMDOwnerTgt()
	t.owner.config = co
	return t
}

// Run is the 'main' where everything gets started
func Run(version, buildTime string) int {
	rmain := initDaemon(version, buildTime)
	err := daemon.rg.runAll(rmain)

	if err == nil {
		nlog.Infoln("Terminated OK")
		return 0
	}
	if e, ok := err.(*cos.ErrSignal); ok {
		nlog.Infoln("Terminated OK via", e)
		return e.ExitCode()
	}
	if errors.Is(err, cmn.ErrStartupTimeout) {
		// NOTE:
		// stats and keepalive runners wait for the ClusterStarted() - i.e., for the primary
		// to reach the corresponding stage. There must be an external "restarter" (e.g. K8s)
		// to restart the daemon if the primary gets killed or panics prior (to reaching that state)
		nlog.Errorln("Timed-out while starting up")
	}
	nlog.Errorln("Terminated with err:", err)
	return 1
}

//////////////
// rungroup //
//////////////

func (g *rungroup) add(r cos.Runner) {
	debug.Assert(r.Name() != "")
	_, exists := g.rs[r.Name()]
	debug.Assert(!exists)

	g.rs[r.Name()] = r
}

func (g *rungroup) run(r cos.Runner) {
	err := r.Run()
	if err != nil {
		nlog.Warningf("runner [%s] exited with err [%v]", r.Name(), err)
	}
	g.errCh <- runRet{r.Name(), err}
}

func (g *rungroup) runAll(mainRunner cos.Runner) error {
	g.errCh = make(chan runRet, len(g.rs))

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
	nlog.SetStopping()
	if ret.name != mainRunner.Name() {
		mainRunner.Stop(ret.err)
	}
	for _, r := range g.rs {
		if r.Name() != mainRunner.Name() {
			r.Stop(ret.err)
		}
	}
	// Wait for all terminations.
	for range len(g.rs) - 1 {
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
		nlog.Warningf("%s[%q] ID from command-line", daemonType, daemon.cli.daemonID)
		return daemon.cli.daemonID
	}
	if daemonID = os.Getenv(daemonIDEnv); daemonID != "" {
		nlog.Warningf("%s[%q] ID from env", daemonType, daemonID)
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
