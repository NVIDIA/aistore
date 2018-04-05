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
	AckWhenOnDisk = "disk"
)

const lbname = "localbuckets"

//==============================
//
// config types
//
//==============================
type dfconfig struct {
	Log              logconfig         `json:"log"`
	Confdir          string            `json:"confdir"`
	CloudProvider    string            `json:"cloudprovider"`
	CloudBuckets     string            `json:"cloud_buckets"`
	LocalBuckets     string            `json:"local_buckets"`
	StatsTimeStr     string            `json:"stats_time"`
	StatsTime        time.Duration     `json:"-"` // omitempty
	HTTP             httpconfig        `json:"http"`
	KeepAliveTimeStr string            `json:"keep_alive_time"`
	KeepAliveTime    time.Duration     `json:"-"` // omitempty
	Listen           listenconfig      `json:"listen"`
	Proxy            proxyconfig       `json:"proxy"`
	S3               s3config          `json:"s3"`
	LRUConfig        lruconfig         `json:"lru_config"`
	RebalanceConf    rebalanceconf     `json:"rebalance_conf"`
	CksumConfig      cksumconfig       `json:"cksum_config"`
	VersionConfig    versionconfig     `json:"version_config"`
	FSpaths          map[string]string `json:"fspaths"`
	TestFSP          testfspathconf    `json:"test_fspaths"`
	AckPolicy        ackpolicy         `json:"ack_policy"`
	Network          netconfig         `json:"network"`
	FSKeeper         fskeeperconf      `json:"fskeeper"`
	H2c              bool              `json:"h2c"`
}

type logconfig struct {
	Dir      string `json:"logdir"`      // log directory
	Level    string `json:"loglevel"`    // log level aka verbosity
	MaxSize  uint64 `json:"logmaxsize"`  // size that triggers log rotation
	MaxTotal uint64 `json:"logmaxtotal"` // max total size of all the logs in the log directory
}

type s3config struct {
	Maxconcurrdownld uint32 `json:"maxconcurrdownld"` // Concurent Download for a session.
	Maxconcurrupld   uint32 `json:"maxconcurrupld"`   // Concurrent Upload for a session.
	Maxpartsize      uint64 `json:"maxpartsize"`      // Maximum part size for Upload and Download used for buffering.
}

type lruconfig struct {
	LowWM              uint32        `json:"lowwm"`             // capacity usage low watermark
	HighWM             uint32        `json:"highwm"`            // capacity usage high watermark
	DontEvictTimeStr   string        `json:"dont_evict_time"`   // eviction is not permitted during [atime, atime + dont]
	CapacityUpdTimeStr string        `json:"capacity_upd_time"` // min time to update capacity
	LRUEnabled         bool          `json:"lru_enabled"`       // LRU will only run when LRUEnabled is true
	DontEvictTime      time.Duration `json:"-"`                 // omitempty
	CapacityUpdTime    time.Duration `json:"-"`                 // ditto
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

type listenconfig struct {
	Proto string `json:"proto"` // Prototype : tcp, udp
	Port  string `json:"port"`  // Listening port.
}

type proxyconfig struct {
	URL      string `json:"url"`      // used to register caching servers
	Passthru bool   `json:"passthru"` // false: get then redirect, true (default): redirect right away
}

type cksumconfig struct {
	ValidateColdGet bool   `json:"validate_cold_get"` // MD5 (ETag) validation upon cold GET
	Checksum        string `json:"checksum"`          // DFC checksum: xxhash:none
}

type ackpolicy struct {
	Put      string `json:"put"`        // ditto, see enum AckWhen... above
	MaxMemMB int    `json:"max_mem_mb"` // max memory size for the "memory" option - FIXME: niy
}

// httpconfig configures parameters for the HTTP clients used by the Proxy
type httpconfig struct {
	TimeoutStr     string        `json:"timeout"`
	Timeout        time.Duration `json:"-"` // omitempty
	LongTimeoutStr string        `json:"long_timeout"`
	LongTimeout    time.Duration `json:"-"` // omitempty
}

type versionconfig struct {
	// True enables object version validation for WARM GET.
	ValidateWarmGet bool   `json:"validate_warm_get"`
	Versioning      string `json:"versioning"` // for what types of objects versioning is enabled: all, cloud, local, none
}

type netconfig struct {
	IPv4 string `json:"ipv4"`
}

type fskeeperconf struct {
	FSCheckTimeStr        string        `json:"fs_check_time"`
	FSCheckTime           time.Duration `json:"-"` // omitempty
	OfflineFSCheckTimeStr string        `json:"offline_fs_check_time"`
	OfflineFSCheckTime    time.Duration `json:"-"` // omitempty
	Enabled               bool          `json:"fskeeper_enabled"`
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
		ctx.config.StatsTime = clivars.statstime
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
		ctx.config.Log.Dir, ctx.config.Listen.Proto, ctx.config.Listen.Port, ctx.config.Log.Level)
	glog.Infof("Config: %q Role: %s StatsTime: %v", clivars.conffile, clivars.role, ctx.config.StatsTime)
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
	if ctx.config.StatsTime, err = time.ParseDuration(ctx.config.StatsTimeStr); err != nil {
		return fmt.Errorf("Bad stats-time format %s, err: %v", ctx.config.StatsTimeStr, err)
	}
	if ctx.config.HTTP.Timeout, err = time.ParseDuration(ctx.config.HTTP.TimeoutStr); err != nil {
		return fmt.Errorf("Bad HTTP timeout format %s, err: %v", ctx.config.HTTP.TimeoutStr, err)
	}
	if ctx.config.HTTP.LongTimeout, err = time.ParseDuration(ctx.config.HTTP.LongTimeoutStr); err != nil {
		return fmt.Errorf("Bad HTTP long_timeout format %s, err %v", ctx.config.HTTP.LongTimeoutStr, err)
	}
	if ctx.config.KeepAliveTime, err = time.ParseDuration(ctx.config.KeepAliveTimeStr); err != nil {
		return fmt.Errorf("Bad keep_alive_time format %s, err: %v", ctx.config.KeepAliveTimeStr, err)
	}
	if ctx.config.LRUConfig.DontEvictTime, err = time.ParseDuration(ctx.config.LRUConfig.DontEvictTimeStr); err != nil {
		return fmt.Errorf("Bad dont_evict_time format %s, err: %v", ctx.config.LRUConfig.DontEvictTimeStr, err)
	}
	if ctx.config.LRUConfig.CapacityUpdTime, err = time.ParseDuration(ctx.config.LRUConfig.CapacityUpdTimeStr); err != nil {
		return fmt.Errorf("Bad capacity_upd_time format %s, err: %v", ctx.config.LRUConfig.CapacityUpdTimeStr, err)
	}
	if ctx.config.RebalanceConf.StartupDelayTime, err = time.ParseDuration(ctx.config.RebalanceConf.StartupDelayTimeStr); err != nil {
		return fmt.Errorf("Bad startup_delay_time format %s, err: %v", ctx.config.RebalanceConf.StartupDelayTimeStr, err)
	}

	hwm, lwm := ctx.config.LRUConfig.HighWM, ctx.config.LRUConfig.LowWM
	if hwm <= 0 || lwm <= 0 || hwm < lwm || lwm > 100 || hwm > 100 {
		return fmt.Errorf("Invalid LRU configuration %+v", ctx.config.LRUConfig)
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
	if ctx.config.CksumConfig.Checksum != ChecksumXXHash && ctx.config.CksumConfig.Checksum != ChecksumNone {
		return fmt.Errorf("Invalid checksum: %s - expecting %s or %s", ctx.config.CksumConfig.Checksum, ChecksumXXHash, ChecksumNone)
	}
	if err := validateVersion(ctx.config.VersionConfig.Versioning); err != nil {
		return err
	}
	if ctx.config.FSKeeper.FSCheckTime, err = time.ParseDuration(ctx.config.FSKeeper.FSCheckTimeStr); err != nil {
		return fmt.Errorf("Bad FSKeeper fs_check_time format %s, err %v", ctx.config.FSKeeper.FSCheckTimeStr, err)
	}
	if ctx.config.FSKeeper.OfflineFSCheckTime, err = time.ParseDuration(ctx.config.FSKeeper.OfflineFSCheckTimeStr); err != nil {
		return fmt.Errorf("Bad FSKeeper offline_fs_check_time format %s, err %v", ctx.config.FSKeeper.OfflineFSCheckTimeStr, err)
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
