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
	"strings"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
)

// checksums: xattr, http header, and config
const (
	XattrXXHashVal  = "user.obj.dfchash"
	XattrObjVersion = "user.obj.version"

	ChecksumNone   = "none"
	ChecksumXXHash = "xxhash"
	ChecksumMD5    = "md5"

	VersionAll   = "all"
	VersionCloud = "cloud"
	VersionLocal = "local"
	VersionNone  = "none"
)

// $CONFDIR/*
const (
	bucketmdbase = "bucket-metadata" // base name of the config file; not to confuse with config.Localbuckets mpath
	mpname       = "mpaths"          // base name to persist ctx.mountpaths
	smapname     = "smap.json"
	rebinpname   = ".rebalancing"
)

//==============================
//
// config types
//
//==============================
type dfconfig struct {
	Confdir          string            `json:"confdir"`
	CloudProvider    string            `json:"cloudprovider"`
	CloudBuckets     string            `json:"cloud_buckets"`
	LocalBuckets     string            `json:"local_buckets"`
	Log              logconfig         `json:"log"`
	Periodic         periodic          `json:"periodic"`
	Timeout          timeoutconfig     `json:"timeout"`
	Proxy            proxyconfig       `json:"proxyconfig"`
	LRU              lruconfig         `json:"lru_config"`
	Rebalance        rebalanceconf     `json:"rebalance_conf"`
	Cksum            cksumconfig       `json:"cksum_config"`
	Ver              versionconfig     `json:"version_config"`
	FSpaths          simplekvs         `json:"fspaths"`
	TestFSP          testfspathconf    `json:"test_fspaths"`
	Net              netconfig         `json:"netconfig"`
	FSKeeper         fskeeperconf      `json:"fskeeper"`
	Auth             authconf          `json:"auth"`
	KeepaliveTracker keepaliveTrackers `json:"keepalivetracker"`
	CallStats        callStats         `json:"callstats"`
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
	Primary  proxycnf `json:"primary"`
	Original proxycnf `json:"original"`
}

type proxycnf struct {
	ID       string `json:"id"`       // used to register caching servers/other proxies
	URL      string `json:"url"`      // used to register caching servers/other proxies
	Passthru bool   `json:"passthru"` // false: get then redirect, true (default): redirect right away
}

type lruconfig struct {
	LowWM              uint32        `json:"lowwm"`             // capacity usage low watermark
	HighWM             uint32        `json:"highwm"`            // capacity usage high watermark
	AtimeCacheMax      uint64        `json:"atime_cache_max"`   // atime cache - max num entries
	DontEvictTimeStr   string        `json:"dont_evict_time"`   // eviction is not permitted during [atime, atime + dont]
	CapacityUpdTimeStr string        `json:"capacity_upd_time"` // min time to update capacity
	DontEvictTime      time.Duration `json:"-"`                 // omitempty
	CapacityUpdTime    time.Duration `json:"-"`                 // ditto
	LRUEnabled         bool          `json:"lru_enabled"`       // LRU will only run when LRUEnabled is true
}

type rebalanceconf struct {
	StartupDelayTimeStr string        `json:"startup_delay_time"`
	StartupDelayTime    time.Duration `json:"-"` // omitempty
	DestRetryTimeStr    string        `json:"dest_retry_time"`
	DestRetryTime       time.Duration `json:"-"` //
	Enabled             bool          `json:"rebalancing_enabled"`
}

type testfspathconf struct {
	Root     string `json:"root"`
	Count    int    `json:"count"`
	Instance int    `json:"instance"`
}

type netconfig struct {
	IPv4 string  `json:"ipv4"`
	L4   l4cnf   `json:"l4"`
	HTTP httpcnf `json:"http"`
}

type l4cnf struct {
	Proto string `json:"proto"` // tcp, udp
	Port  string `json:"port"`  // listening port
}

type httpcnf struct {
	MaxNumTargets int    `json:"max_num_targets"`    // estimated max num targets (to count idle conns)
	UseHTTP2      bool   `json:"use_http2"`          // use HTTP/2 instead of HTTP/1.1
	UseHTTPS      bool   `json:"use_https"`          // use HTTPS instead of HTTP
	UseAsProxy    bool   `json:"use_as_proxy"`       // use DFC as an HTTP proxy
	Certificate   string `json:"server_certificate"` // HTTPS: openssl certificate
	Key           string `json:"server_key"`         // HTTPS: openssl key
}

type cksumconfig struct {
	Checksum                string `json:"checksum"`                   // DFC checksum: xxhash:none
	ValidateColdGet         bool   `json:"validate_checksum_cold_get"` // MD5 (ETag) validation upon cold GET
	ValidateWarmGet         bool   `json:"validate_checksum_warm_get"` // MD5 (ETag) validation upon warm GET
	EnableReadRangeChecksum bool   `json:"enable_read_range_checksum"` // Return read range checksum otherwise return entire object checksum
}

type versionconfig struct {
	ValidateWarmGet bool   `json:"validate_version_warm_get"` // True: validate object version upon warm GET
	Versioning      string `json:"versioning"`                // types of objects versioning is enabled for: all, cloud, local, none
}

type fskeeperconf struct {
	FSCheckTimeStr        string        `json:"fs_check_time"`
	FSCheckTime           time.Duration `json:"-"` // omitempty
	OfflineFSCheckTimeStr string        `json:"offline_fs_check_time"`
	OfflineFSCheckTime    time.Duration `json:"-"` // omitempty
	Enabled               bool          `json:"fskeeper_enabled"`
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
	Name        string        `json:"name"` // "heartbeat", "average"
	MaxStr      string        `json:"max"`  // "heartbeat" only
	Max         time.Duration `json:"-"`
	Factor      int           `json:"factor"` // "average" only
}

type keepaliveTrackers struct {
	Proxy  keepaliveTrackerConf `json:"proxy"`  // how proxy tracks target keepalives
	Target keepaliveTrackerConf `json:"target"` // how target tracks primary proxies keepalives
}

type callStats struct {
	RequestIncluded []string `json:"request_included"`
	Factor          float32  `json:"factor"`
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
	if err = CreateDir(ctx.config.Log.Dir); err != nil {
		glog.Errorf("Failed to create log dir %q, err: %v", ctx.config.Log.Dir, err)
		return err
	}
	if err = validateconf(); err != nil {
		return err
	}
	// glog rotate
	glog.MaxSize = ctx.config.Log.MaxSize
	if glog.MaxSize > GiB {
		glog.Errorf("Log.MaxSize %d exceeded 1GB, setting the default 1MB", glog.MaxSize)
		glog.MaxSize = MiB
	}
	// CLI override
	if clivars.statstime != 0 {
		ctx.config.Periodic.StatsTime = clivars.statstime
	}
	if clivars.proxyurl != "" {
		ctx.config.Proxy.Primary.ID = ""
		ctx.config.Proxy.Primary.URL = clivars.proxyurl
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
	if build != "" {
		glog.Infof("Build:  %s", build) // git rev-parse --short HEAD
	}
	glog.Infof("Logdir: %q Proto: %s Port: %s Verbosity: %s",
		ctx.config.Log.Dir, ctx.config.Net.L4.Proto, ctx.config.Net.L4.Port, ctx.config.Log.Level)
	glog.Infof("Config: %q Role: %s StatsTime: %v", clivars.conffile, clivars.role, ctx.config.Periodic.StatsTime)
	return err
}

func getConfig(fpath string) {
	err := LocalLoad(fpath, &ctx.config)
	if err != nil {
		glog.Errorf("Failed to load config %q, err: %v", fpath, err)
		os.Exit(1)
	}
}

func validateVersion(version string) error {
	versions := []string{VersionAll, VersionCloud, VersionLocal, VersionNone}
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
	if ctx.config.Rebalance.StartupDelayTime, err = time.ParseDuration(ctx.config.Rebalance.StartupDelayTimeStr); err != nil {
		return fmt.Errorf("Bad startup_delay_time format %s, err: %v", ctx.config.Rebalance.StartupDelayTimeStr, err)
	}
	if ctx.config.Rebalance.DestRetryTime, err = time.ParseDuration(ctx.config.Rebalance.DestRetryTimeStr); err != nil {
		return fmt.Errorf("Bad dest_retry_time format %s, err: %v", ctx.config.Rebalance.DestRetryTimeStr, err)
	}

	hwm, lwm := ctx.config.LRU.HighWM, ctx.config.LRU.LowWM
	if hwm <= 0 || lwm <= 0 || hwm < lwm || lwm > 100 || hwm > 100 {
		return fmt.Errorf("Invalid LRU configuration %+v", ctx.config.LRU)
	}
	if ctx.config.Cksum.Checksum != ChecksumXXHash && ctx.config.Cksum.Checksum != ChecksumNone {
		return fmt.Errorf("Invalid checksum: %s - expecting %s or %s", ctx.config.Cksum.Checksum, ChecksumXXHash, ChecksumNone)
	}
	if err := validateVersion(ctx.config.Ver.Versioning); err != nil {
		return err
	}
	if ctx.config.FSKeeper.FSCheckTime, err = time.ParseDuration(ctx.config.FSKeeper.FSCheckTimeStr); err != nil {
		return fmt.Errorf("Bad FSKeeper fs_check_time format %s, err %v", ctx.config.FSKeeper.FSCheckTimeStr, err)
	}
	if ctx.config.FSKeeper.OfflineFSCheckTime, err = time.ParseDuration(ctx.config.FSKeeper.OfflineFSCheckTimeStr); err != nil {
		return fmt.Errorf("Bad FSKeeper offline_fs_check_time format %s, err %v", ctx.config.FSKeeper.OfflineFSCheckTimeStr, err)
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

	ctx.config.KeepaliveTracker.Proxy.Max, err = time.ParseDuration(ctx.config.KeepaliveTracker.Proxy.MaxStr)
	if err != nil {
		return fmt.Errorf("bad proxy keep alive max %s", ctx.config.KeepaliveTracker.Proxy.MaxStr)
	}

	ctx.config.KeepaliveTracker.Target.Interval, err = time.ParseDuration(ctx.config.KeepaliveTracker.Target.IntervalStr)
	if err != nil {
		return fmt.Errorf("bad target keep alive interval %s", ctx.config.KeepaliveTracker.Target.IntervalStr)
	}

	ctx.config.KeepaliveTracker.Target.Max, err = time.ParseDuration(ctx.config.KeepaliveTracker.Target.MaxStr)
	if err != nil {
		return fmt.Errorf("bad targetkeep alive max %s", ctx.config.KeepaliveTracker.Target.MaxStr)
	}

	if !IsKeepaliveTypeSupported(ctx.config.KeepaliveTracker.Proxy.Name) {
		return fmt.Errorf("bad proxy keepalive tracker type %s", ctx.config.KeepaliveTracker.Proxy.Name)
	}

	if !IsKeepaliveTypeSupported(ctx.config.KeepaliveTracker.Target.Name) {
		return fmt.Errorf("bad target keepalive tracker type %s", ctx.config.KeepaliveTracker.Target.Name)
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
