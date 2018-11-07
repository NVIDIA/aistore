/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
)

// $CONFDIR/*
const (
	bucketmdbase  = "bucket-metadata" // base name of the config file; not to confuse with config.Localbuckets mpath
	mpname        = "mpaths"          // base name to persist fs.Mountpaths
	smapname      = "smap.json"
	rebinpname    = ".rebalancing"
	reblocinpname = ".localrebalancing"
)

const (
	RevProxyCloud  = "cloud"
	RevProxyTarget = "target"
)

//==============================
//
// config types
//
//==============================
type dfconfig struct {
	Confdir          string             `json:"confdir"`
	CloudProvider    string             `json:"cloudprovider"`
	CloudBuckets     string             `json:"cloud_buckets"`
	LocalBuckets     string             `json:"local_buckets"`
	Readahead        rahconfig          `json:"readahead"`
	Log              logconfig          `json:"log"`
	Periodic         periodic           `json:"periodic"`
	Timeout          timeoutconfig      `json:"timeout"`
	Proxy            proxyconfig        `json:"proxyconfig"`
	LRU              cmn.LRUConfig   `json:"lru_config"`
	Xaction          xactionConfig      `json:"xaction_config"`
	Rebalance        rebalanceconf      `json:"rebalance_conf"`
	Replication      replicationconfig  `json:"replication"`
	Cksum            cmn.CksumConfig `json:"cksum_config"`
	Ver              versionconfig      `json:"version_config"`
	FSpaths          cmn.SimpleKVs   `json:"fspaths"`
	TestFSP          testfspathconf     `json:"test_fspaths"`
	Net              netconfig          `json:"netconfig"`
	FSHC             fshcconf           `json:"fshc"`
	Auth             authconf           `json:"auth"`
	KeepaliveTracker keepaliveTrackers  `json:"keepalivetracker"`
}

type xactionConfig struct {
	DiskUtilLowWM  uint32 `json:"disk_util_low_wm"`  // Low watermark below which no throttling is required
	DiskUtilHighWM uint32 `json:"disk_util_high_wm"` // High watermark above which throttling is required for longer duration
}

type rahconfig struct {
	ObjectMem int64 `json:"rahobjectmem"`
	TotalMem  int64 `json:"rahtotalmem"`
	ByProxy   bool  `json:"rahbyproxy"`
	Discard   bool  `json:"rahdiscard"`
	Enabled   bool  `json:"rahenabled"`
}

type logconfig struct {
	Dir      string `json:"logdir"`      // log directory
	Level    string `json:"loglevel"`    // log level aka verbosity
	MaxSize  uint64 `json:"logmaxsize"`  // size that triggers log rotation
	MaxTotal uint64 `json:"logmaxtotal"` // max total size of all the logs in the log directory
}

type periodic struct {
	StatsTimeStr     string `json:"stats_time"`
	RetrySyncTimeStr string `json:"retry_sync_time"`
	// omitempty
	StatsTime     time.Duration `json:"-"`
	RetrySyncTime time.Duration `json:"-"`
}

// timeoutconfig contains timeouts used for intra-cluster communication
type timeoutconfig struct {
	DefaultStr         string        `json:"default_timeout"`
	Default            time.Duration `json:"-"` // omitempty
	DefaultLongStr     string        `json:"default_long_timeout"`
	DefaultLong        time.Duration `json:"-"` //
	MaxKeepaliveStr    string        `json:"max_keepalive"`
	MaxKeepalive       time.Duration `json:"-"` //
	ProxyPingStr       string        `json:"proxy_ping"`
	ProxyPing          time.Duration `json:"-"` //
	CplaneOperationStr string        `json:"cplane_operation"`
	CplaneOperation    time.Duration `json:"-"` //
	SendFileStr        string        `json:"send_file_time"`
	SendFile           time.Duration `json:"-"` //
	StartupStr         string        `json:"startup_time"`
	Startup            time.Duration `json:"-"` //
}

type proxyconfig struct {
	NonElectable bool   `json:"non_electable"`
	PrimaryURL   string `json:"primary_url"`
	OriginalURL  string `json:"original_url"`
	DiscoveryURL string `json:"discovery_url"`
}

type rebalanceconf struct {
	DestRetryTimeStr string        `json:"dest_retry_time"`
	DestRetryTime    time.Duration `json:"-"` //
	Enabled          bool          `json:"rebalancing_enabled"`
}

type replicationconfig struct {
	ReplicateOnColdGet     bool `json:"replicate_on_cold_get"`     // object replication on cold GET request
	ReplicateOnPut         bool `json:"replicate_on_put"`          // object replication on PUT request
	ReplicateOnLRUEviction bool `json:"replicate_on_lru_eviction"` // object replication on LRU eviction
}

type testfspathconf struct {
	Root     string `json:"root"`
	Count    int    `json:"count"`
	Instance int    `json:"instance"`
}

type netconfig struct {
	IPv4             string  `json:"ipv4"`
	IPv4IntraControl string  `json:"ipv4_intra_control"`
	IPv4IntraData    string  `json:"ipv4_intra_data"`
	UseIntraControl  bool    `json:"-"`
	UseIntraData     bool    `json:"-"`
	L4               l4cnf   `json:"l4"`
	HTTP             httpcnf `json:"http"`
}

type l4cnf struct {
	Proto               string `json:"proto"` // tcp, udp
	PortStr             string `json:"port"`  // listening port
	Port                int    `json:"-"`
	PortIntraControlStr string `json:"port_intra_control"` // listening port for intra control network
	PortIntraControl    int    `json:"-"`
	PortIntraDataStr    string `json:"port_intra_data"` // listening port for intra data network
	PortIntraData       int    `json:"-"`
}

type httpcnf struct {
	proto         string // http or https
	RevProxy      string `json:"rproxy"`             // RevProxy* enum
	Certificate   string `json:"server_certificate"` // HTTPS: openssl certificate
	Key           string `json:"server_key"`         // HTTPS: openssl key
	MaxNumTargets int    `json:"max_num_targets"`    // estimated max num targets (to count idle conns)
	UseHTTPS      bool   `json:"use_https"`          // use HTTPS instead of HTTP
}

type versionconfig struct {
	ValidateWarmGet bool   `json:"validate_version_warm_get"` // True: validate object version upon warm GET
	Versioning      string `json:"versioning"`                // types of objects versioning is enabled for: all, cloud, local, none
}

type fshcconf struct {
	Enabled       bool `json:"fshc_enabled"`
	TestFileCount int  `json:"fshc_test_files"`  // the number of files to read and write during a test
	ErrorLimit    int  `json:"fshc_error_limit"` // thresholds of number of errors, exceeding any of them results in disabling a mountpath
}

type authconf struct {
	Secret  string `json:"secret"`
	Enabled bool   `json:"enabled"`
	CredDir string `json:"creddir"`
}

// config for one keepalive tracker
// all type of trackers share the same struct, not all fields are used by all trackers
type keepaliveTrackerConf struct {
	IntervalStr string        `json:"interval"` // keepalives are sent(target)/checked(promary proxy) every interval
	Interval    time.Duration `json:"-"`
	Name        string        `json:"name"`   // "heartbeat", "average"
	Factor      int           `json:"factor"` // "average" only
}

type keepaliveTrackers struct {
	Proxy  keepaliveTrackerConf `json:"proxy"`  // how proxy tracks target keepalives
	Target keepaliveTrackerConf `json:"target"` // how target tracks primary proxies keepalives
}

//==============================
//
// config functions
//
//==============================
func initconfigparam() error {
	getConfig(clivars.conffile)

	err := flag.Lookup("log_dir").Value.Set(ctx.config.Log.Dir)
	if err != nil {
		glog.Errorf("Failed to flag-set glog dir %q, err: %v", ctx.config.Log.Dir, err)
	}
	if err = cmn.CreateDir(ctx.config.Log.Dir); err != nil {
		glog.Errorf("Failed to create log dir %q, err: %v", ctx.config.Log.Dir, err)
		return err
	}
	if err = validateconf(); err != nil {
		return err
	}
	// glog rotate
	glog.MaxSize = ctx.config.Log.MaxSize
	if glog.MaxSize > cmn.GiB {
		glog.Errorf("Log.MaxSize %d exceeded 1GB, setting the default 1MB", glog.MaxSize)
		glog.MaxSize = cmn.MiB
	}
	// CLI override
	if clivars.statstime != 0 {
		ctx.config.Periodic.StatsTime = clivars.statstime
	}
	if clivars.proxyurl != "" {
		ctx.config.Proxy.PrimaryURL = clivars.proxyurl
	}
	if clivars.loglevel != "" {
		if err = setloglevel(clivars.loglevel); err != nil {
			glog.Errorf("Failed to set log level = %s, err: %v", clivars.loglevel, err)
		}
	} else {
		if err = setloglevel(ctx.config.Log.Level); err != nil {
			glog.Errorf("Failed to set log level = %s, err: %v", ctx.config.Log.Level, err)
		}
	}

	// Set helpers
	ctx.config.Net.HTTP.proto = "http"
	if ctx.config.Net.HTTP.UseHTTPS {
		ctx.config.Net.HTTP.proto = "https"
	}

	differentIPs := ctx.config.Net.IPv4 != ctx.config.Net.IPv4IntraControl
	differentPorts := ctx.config.Net.L4.Port != ctx.config.Net.L4.PortIntraControl
	ctx.config.Net.UseIntraControl = false
	if ctx.config.Net.IPv4IntraControl != "" && ctx.config.Net.L4.PortIntraControl != 0 && (differentIPs || differentPorts) {
		ctx.config.Net.UseIntraControl = true
	}

	differentIPs = ctx.config.Net.IPv4 != ctx.config.Net.IPv4IntraData
	differentPorts = ctx.config.Net.L4.Port != ctx.config.Net.L4.PortIntraData
	ctx.config.Net.UseIntraData = false
	if ctx.config.Net.IPv4IntraData != "" && ctx.config.Net.L4.PortIntraData != 0 && (differentIPs || differentPorts) {
		ctx.config.Net.UseIntraData = true
	}

	if build != "" {
		glog.Infof("Build:  %s", build) // git rev-parse --short HEAD
	}
	glog.Infof("Logdir: %q Proto: %s Port: %d Verbosity: %s",
		ctx.config.Log.Dir, ctx.config.Net.L4.Proto, ctx.config.Net.L4.Port, ctx.config.Log.Level)
	glog.Infof("Config: %q Role: %s StatsTime: %v", clivars.conffile, clivars.role, ctx.config.Periodic.StatsTime)
	return err
}

func getConfig(fpath string) {
	err := cmn.LocalLoad(fpath, &ctx.config)
	if err != nil {
		glog.Errorf("Failed to load config %q, err: %v", fpath, err)
		os.Exit(1)
	}
}

func validateVersion(version string) error {
	versions := []string{cmn.VersionAll, cmn.VersionCloud, cmn.VersionLocal, cmn.VersionNone}
	versionValid := false
	for _, v := range versions {
		if v == version {
			versionValid = true
			break
		}
	}
	if !versionValid {
		return fmt.Errorf("Invalid version: %s - expecting one of %s", version, strings.Join(versions, ", "))
	}
	return nil
}

func validateconf() (err error) {
	// durations
	if ctx.config.Periodic.StatsTime, err = time.ParseDuration(ctx.config.Periodic.StatsTimeStr); err != nil {
		return fmt.Errorf("Bad stats-time format %s, err: %v", ctx.config.Periodic.StatsTimeStr, err)
	}
	if int(ctx.config.Periodic.StatsTime/time.Second) <= 0 {
		return fmt.Errorf("stats-time refresh period is too low (should be higher than 1 second")
	}
	if ctx.config.Periodic.RetrySyncTime, err = time.ParseDuration(ctx.config.Periodic.RetrySyncTimeStr); err != nil {
		return fmt.Errorf("Bad retry_sync_time format %s, err: %v", ctx.config.Periodic.RetrySyncTimeStr, err)
	}
	if ctx.config.Timeout.Default, err = time.ParseDuration(ctx.config.Timeout.DefaultStr); err != nil {
		return fmt.Errorf("Bad Timeout default format %s, err: %v", ctx.config.Timeout.DefaultStr, err)
	}
	if ctx.config.Timeout.DefaultLong, err = time.ParseDuration(ctx.config.Timeout.DefaultLongStr); err != nil {
		return fmt.Errorf("Bad Timeout default_long format %s, err %v", ctx.config.Timeout.DefaultLongStr, err)
	}
	if ctx.config.LRU.DontEvictTime, err = time.ParseDuration(ctx.config.LRU.DontEvictTimeStr); err != nil {
		return fmt.Errorf("Bad dont_evict_time format %s, err: %v", ctx.config.LRU.DontEvictTimeStr, err)
	}
	if ctx.config.LRU.CapacityUpdTime, err = time.ParseDuration(ctx.config.LRU.CapacityUpdTimeStr); err != nil {
		return fmt.Errorf("Bad capacity_upd_time format %s, err: %v", ctx.config.LRU.CapacityUpdTimeStr, err)
	}
	if ctx.config.Rebalance.DestRetryTime, err = time.ParseDuration(ctx.config.Rebalance.DestRetryTimeStr); err != nil {
		return fmt.Errorf("Bad dest_retry_time format %s, err: %v", ctx.config.Rebalance.DestRetryTimeStr, err)
	}

	hwm, lwm := ctx.config.LRU.HighWM, ctx.config.LRU.LowWM
	if hwm <= 0 || lwm <= 0 || hwm < lwm || lwm > 100 || hwm > 100 {
		return fmt.Errorf("Invalid LRU configuration %+v", ctx.config.LRU)
	}

	diskUtilHWM, diskUtilLWM := ctx.config.Xaction.DiskUtilHighWM, ctx.config.Xaction.DiskUtilLowWM
	if diskUtilHWM <= 0 || diskUtilLWM <= 0 || diskUtilHWM <= diskUtilLWM || diskUtilLWM > 100 || diskUtilHWM > 100 {
		return fmt.Errorf("Invalid Xaction configuration %+v", ctx.config.Xaction)
	}

	if ctx.config.Cksum.Checksum != cmn.ChecksumXXHash && ctx.config.Cksum.Checksum != cmn.ChecksumNone {
		return fmt.Errorf("Invalid checksum: %s - expecting %s or %s", ctx.config.Cksum.Checksum, cmn.ChecksumXXHash, cmn.ChecksumNone)
	}
	if err := validateVersion(ctx.config.Ver.Versioning); err != nil {
		return err
	}
	if ctx.config.Timeout.MaxKeepalive, err = time.ParseDuration(ctx.config.Timeout.MaxKeepaliveStr); err != nil {
		return fmt.Errorf("Bad Timeout max_keepalive format %s, err %v", ctx.config.Timeout.MaxKeepaliveStr, err)
	}
	if ctx.config.Timeout.ProxyPing, err = time.ParseDuration(ctx.config.Timeout.ProxyPingStr); err != nil {
		return fmt.Errorf("Bad Timeout proxy_ping format %s, err %v", ctx.config.Timeout.ProxyPingStr, err)
	}
	if ctx.config.Timeout.CplaneOperation, err = time.ParseDuration(ctx.config.Timeout.CplaneOperationStr); err != nil {
		return fmt.Errorf("Bad Timeout vote_request format %s, err %v", ctx.config.Timeout.CplaneOperationStr, err)
	}
	if ctx.config.Timeout.SendFile, err = time.ParseDuration(ctx.config.Timeout.SendFileStr); err != nil {
		return fmt.Errorf("Bad Timeout send_file_time format %s, err %v", ctx.config.Timeout.SendFileStr, err)
	}
	if ctx.config.Timeout.Startup, err = time.ParseDuration(ctx.config.Timeout.StartupStr); err != nil {
		return fmt.Errorf("Bad Proxy startup_time format %s, err %v", ctx.config.Timeout.StartupStr, err)
	}

	ctx.config.KeepaliveTracker.Proxy.Interval, err = time.ParseDuration(ctx.config.KeepaliveTracker.Proxy.IntervalStr)
	if err != nil {
		return fmt.Errorf("bad proxy keep alive interval %s", ctx.config.KeepaliveTracker.Proxy.IntervalStr)
	}

	ctx.config.KeepaliveTracker.Target.Interval, err = time.ParseDuration(ctx.config.KeepaliveTracker.Target.IntervalStr)
	if err != nil {
		return fmt.Errorf("bad target keep alive interval %s", ctx.config.KeepaliveTracker.Target.IntervalStr)
	}

	if !ValidKeepaliveType(ctx.config.KeepaliveTracker.Proxy.Name) {
		return fmt.Errorf("bad proxy keepalive tracker type %s", ctx.config.KeepaliveTracker.Proxy.Name)
	}

	if !ValidKeepaliveType(ctx.config.KeepaliveTracker.Target.Name) {
		return fmt.Errorf("bad target keepalive tracker type %s", ctx.config.KeepaliveTracker.Target.Name)
	}

	// NETWORK

	// Parse ports
	if ctx.config.Net.L4.Port, err = parsePort(ctx.config.Net.L4.PortStr); err != nil {
		return fmt.Errorf("Bad public port specified: %v", err)
	}

	ctx.config.Net.L4.PortIntraControl = 0
	if ctx.config.Net.L4.PortIntraControlStr != "" {
		if ctx.config.Net.L4.PortIntraControl, err = parsePort(ctx.config.Net.L4.PortIntraControlStr); err != nil {
			return fmt.Errorf("Bad internal port specified: %v", err)
		}
	}
	ctx.config.Net.L4.PortIntraData = 0
	if ctx.config.Net.L4.PortIntraDataStr != "" {
		if ctx.config.Net.L4.PortIntraData, err = parsePort(ctx.config.Net.L4.PortIntraDataStr); err != nil {
			return fmt.Errorf("Bad replication port specified: %v", err)
		}
	}

	ctx.config.Net.IPv4 = strings.Replace(ctx.config.Net.IPv4, " ", "", -1)
	ctx.config.Net.IPv4IntraControl = strings.Replace(ctx.config.Net.IPv4IntraControl, " ", "", -1)
	ctx.config.Net.IPv4IntraData = strings.Replace(ctx.config.Net.IPv4IntraData, " ", "", -1)

	if overlap, addr := ipv4ListsOverlap(ctx.config.Net.IPv4, ctx.config.Net.IPv4IntraControl); overlap {
		return fmt.Errorf(
			"Public and internal addresses overlap: %s (public: %s; internal: %s)",
			addr, ctx.config.Net.IPv4, ctx.config.Net.IPv4IntraControl,
		)
	}
	if overlap, addr := ipv4ListsOverlap(ctx.config.Net.IPv4, ctx.config.Net.IPv4IntraData); overlap {
		return fmt.Errorf(
			"Public and replication addresses overlap: %s (public: %s; replication: %s)",
			addr, ctx.config.Net.IPv4, ctx.config.Net.IPv4IntraData,
		)
	}
	if overlap, addr := ipv4ListsOverlap(ctx.config.Net.IPv4IntraControl, ctx.config.Net.IPv4IntraData); overlap {
		return fmt.Errorf(
			"Internal and replication addresses overlap: %s (internal: %s; replication: %s)",
			addr, ctx.config.Net.IPv4IntraControl, ctx.config.Net.IPv4IntraData,
		)
	}

	if ctx.config.Net.HTTP.RevProxy != "" {
		if ctx.config.Net.HTTP.RevProxy != RevProxyCloud && ctx.config.Net.HTTP.RevProxy != RevProxyTarget {
			return fmt.Errorf("Invalid http rproxy configuration: %s (expecting: ''|%s|%s)",
				ctx.config.Net.HTTP.RevProxy, RevProxyCloud, RevProxyTarget)
		}
	}
	return nil
}

func setloglevel(loglevel string) (err error) {
	v := flag.Lookup("v").Value
	if v == nil {
		return fmt.Errorf("nil -v Value")
	}
	err = v.Set(loglevel)
	if err == nil {
		ctx.config.Log.Level = loglevel
	}
	return
}

// setGLogVModule sets glog's vmodule flag
// sets 'v' as is, no verificaton is done here
// syntax for v: target=5,proxy=1, p*=3, etc
func setGLogVModule(v string) error {
	f := flag.Lookup("vmodule")
	if f == nil {
		return nil
	}

	err := f.Value.Set(v)
	if err == nil {
		glog.Info("log level vmodule changed to ", v)
	}

	return err
}

// testingFSPpath returns true if DFC is running in dev environment, and
// moreover, all the cluster is running on a single machine
func testingFSPpaths() bool {
	return ctx.config.TestFSP.Count > 0
}

// ipv4ListsOverlap checks if two comma-separated ipv4 address lists
// contain at least one common ipv4 address
func ipv4ListsOverlap(alist, blist string) (overlap bool, addr string) {
	if alist == "" || blist == "" {
		return
	}
	alistAddrs := strings.Split(alist, ",")
	for _, a := range alistAddrs {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		if strings.Contains(blist, a) {
			return true, a
		}
	}
	return
}
