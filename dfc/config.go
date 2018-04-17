// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/golang/glog"
)

const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
)

// checksums: xattr, http header, and config
const (
	xattrXXHashVal  = "user.obj.dfchash"
	xattrObjVersion = "user.obj.version"

	ChecksumNone   = "none"
	ChecksumXXHash = "xxhash"
	ChecksumMD5    = "md5"

	VersionAll   = "all"
	VersionCloud = "cloud"
	VersionLocal = "local"
	VersionNone  = "none"
)

const (
	AckWhenInMem  = "memory"
	AckWhenOnDisk = "disk" // the default
)

const (
	lbname = "localbuckets" // base name of the lbconfig file; not to confuse with config.Localbuckets mpath sub-directory
	mpname = "mpaths"       // base name to persist ctx.mountpaths
)

//==============================
//
// config types
//
//==============================
type dfconfig struct {
	Confdir       string `json:"confdir"`
	CloudProvider string `json:"cloudprovider"`
	CloudBuckets  string `json:"cloud_buckets"`
	LocalBuckets  string `json:"local_buckets"`
	// structs
	Log          logconfig         `json:"log"`
	Periodic     periodic          `json:"periodic"`
	Timeout      timeoutconfig     `json:"timeout"`
	Proxy        proxyconfig       `json:"proxyconfig"`
	LRU          lruconfig         `json:"lru_config"`
	Rebalance    rebalanceconf     `json:"rebalance_conf"`
	Cksum        cksumconfig       `json:"cksum_config"`
	Ver          versionconfig     `json:"version_config"`
	FSpaths      map[string]string `json:"fspaths"`
	TestFSP      testfspathconf    `json:"test_fspaths"`
	Net          netconfig         `json:"netconfig"`
	FSKeeper     fskeeperconf      `json:"fskeeper"`
	Experimental experimental      `json:"experimental"`
	H2c          bool              `json:"h2c"`
}

type logconfig struct {
	Dir      string `json:"logdir"`      // log directory
	Level    string `json:"loglevel"`    // log level aka verbosity
	MaxSize  uint64 `json:"logmaxsize"`  // size that triggers log rotation
	MaxTotal uint64 `json:"logmaxtotal"` // max total size of all the logs in the log directory
}

type periodic struct {
	StatsTimeStr     string `json:"stats_time"`
	KeepAliveTimeStr string `json:"keep_alive_time"`
	// omitempty
	StatsTime     time.Duration `json:"-"`
	KeepAliveTime time.Duration `json:"-"`
}

// timeoutconfig contains timeouts used for intra-cluster communication
type timeoutconfig struct {
	DefaultStr      string        `json:"default"`
	Default         time.Duration `json:"-"` // omitempty
	DefaultLongStr  string        `json:"default_long"`
	DefaultLong     time.Duration `json:"-"` //
	MaxKeepaliveStr string        `json:"max_keepalive"`
	MaxKeepalive    time.Duration `json:"-"` //
	ProxyPingStr    string        `json:"proxy_ping"`
	ProxyPing       time.Duration `json:"-"` //
	VoteRequestStr  string        `json:"vote_request"`
	VoteRequest     time.Duration `json:"-"` //
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
	RebalancingEnabled  bool          `json:"rebalancing_enabled"`
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
	UseHTTPS      bool   `json:"use_https"`          // use HTTPS instead of HTTP
	Certificate   string `json:"server_certificate"` // HTTPS: openssl certificate
	Key           string `json:"server_key"`         // HTTPS: openssl key
}

type cksumconfig struct {
	Checksum        string `json:"checksum"`          // DFC checksum: xxhash:none
	ValidateColdGet bool   `json:"validate_cold_get"` // MD5 (ETag) validation upon cold GET
}

type versionconfig struct {
	ValidateWarmGet bool   `json:"validate_warm_get"` // True: validate object version upon warm GET
	Versioning      string `json:"versioning"`        // types of objects versioning is enabled for: all, cloud, local, none
}

type fskeeperconf struct {
	FSCheckTimeStr        string        `json:"fs_check_time"`
	FSCheckTime           time.Duration `json:"-"` // omitempty
	OfflineFSCheckTimeStr string        `json:"offline_fs_check_time"`
	OfflineFSCheckTime    time.Duration `json:"-"` // omitempty
	Enabled               bool          `json:"fskeeper_enabled"`
}

type experimental struct {
	AckPut   string `json:"ack_put"`
	MaxMemMB int    `json:"max_mem_mb"` // max memory size for the "memory" option - FIXME: niy
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
	raw, err := ioutil.ReadFile(fpath)
	if err != nil {
		glog.Errorf("Failed to read config %q, err: %v", fpath, err)
		os.Exit(1)
	}
	err = json.Unmarshal(raw, &ctx.config)
	if err != nil {
		glog.Errorf("Failed to json-unmarshal config %q, err: %v", fpath, err)
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

//	StartupDelayTimeStr string        `json:"startup_delay_time"`
//	StartupDelayTime    time.Duration `json:"-"` // omitempty
func validateconf() (err error) {
	// durations
	if ctx.config.Periodic.StatsTime, err = time.ParseDuration(ctx.config.Periodic.StatsTimeStr); err != nil {
		return fmt.Errorf("Bad stats-time format %s, err: %v", ctx.config.Periodic.StatsTimeStr, err)
	}
	if ctx.config.Timeout.Default, err = time.ParseDuration(ctx.config.Timeout.DefaultStr); err != nil {
		return fmt.Errorf("Bad Timeout default format %s, err: %v", ctx.config.Timeout.DefaultStr, err)
	}
	if ctx.config.Timeout.DefaultLong, err = time.ParseDuration(ctx.config.Timeout.DefaultLongStr); err != nil {
		return fmt.Errorf("Bad Timeout default_long format %s, err %v", ctx.config.Timeout.DefaultLongStr, err)
	}
	if ctx.config.Periodic.KeepAliveTime, err = time.ParseDuration(ctx.config.Periodic.KeepAliveTimeStr); err != nil {
		return fmt.Errorf("Bad keep_alive_time format %s, err: %v", ctx.config.Periodic.KeepAliveTimeStr, err)
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

	hwm, lwm := ctx.config.LRU.HighWM, ctx.config.LRU.LowWM
	if hwm <= 0 || lwm <= 0 || hwm < lwm || lwm > 100 || hwm > 100 {
		return fmt.Errorf("Invalid LRU configuration %+v", ctx.config.LRU)
	}
	if ctx.config.TestFSP.Count == 0 {
		for fp1 := range ctx.config.FSpaths {
			for fp2 := range ctx.config.FSpaths {
				if fp1 != fp2 && (strings.HasPrefix(fp1, fp2) || strings.HasPrefix(fp2, fp1)) {
					return fmt.Errorf("Invalid fspaths: %q is a prefix or includes as a prefix %q", fp1, fp2)
				}
			}
		}
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
	if ctx.config.Timeout.VoteRequest, err = time.ParseDuration(ctx.config.Timeout.VoteRequestStr); err != nil {
		return fmt.Errorf("Bad Timeout vote_request format %s, err %v", ctx.config.Timeout.VoteRequestStr, err)
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

func writeConfigFile() error {
	return localSave(clivars.conffile, ctx.config)
}
