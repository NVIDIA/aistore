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

// checksums: xattr, http header, and config
const (
	xattrXXHashVal  = "user.obj.dfchash"
	xattrObjVersion = "user.obj.version"

	ChecksumNone   = "none"
	ChecksumXXHash = "xxhash"
	ChecksumMD5    = "md5"
)

const (
	AckWhenInMem  = "memory"
	AckWhenOnDisk = "disk"
)

//==============================
//
// config types
//
//==============================
type dfconfig struct {
	Logdir           string            `json:"logdir"`
	Loglevel         string            `json:"loglevel"`
	CloudProvider    string            `json:"cloudprovider"`
	CloudBuckets     string            `json:"cloud_buckets"`
	LocalBuckets     string            `json:"local_buckets"`
	LBConf           string            `json:"lb_conf"`
	StatsTimeStr     string            `json:"stats_time"`
	StatsTime        time.Duration     `json:"-"` // omitempty
	HTTP             httpconfig        `json:"http"`
	KeepAliveTimeStr string            `json:"keep_alive_time"`
	KeepAliveTime    time.Duration     `json:"-"` // omitempty
	H2c              bool              `json:"h2c"`
	Listen           listenconfig      `json:"listen"`
	Proxy            proxyconfig       `json:"proxy"`
	S3               s3config          `json:"s3"`
	LRUConfig        lruconfig         `json:"lru_config"`
	CksumConfig      cksumconfig       `json:"cksum_config"`
	VersionConfig    versionconfig     `json:"version_config"`
	FSpaths          map[string]string `json:"fspaths"`
	TestFSP          testfspathconf    `json:"test_fspaths"`
	AckPolicy        ackpolicy         `json:"ack_policy"`
}

type s3config struct {
	Maxconcurrdownld uint32 `json:"maxconcurrdownld"` // Concurent Download for a session.
	Maxconcurrupld   uint32 `json:"maxconcurrupld"`   // Concurrent Upload for a session.
	Maxpartsize      uint64 `json:"maxpartsize"`      // Maximum part size for Upload and Download used for buffering.
}

type lruconfig struct {
	LowWM            uint32        `json:"lowwm"`           // capacity usage low watermark
	HighWM           uint32        `json:"highwm"`          // capacity usage high watermark
	DontEvictTimeStr string        `json:"dont_evict_time"` // eviction is not permitted during [atime, atime + dont]
	LRUEnabled       bool          `json:"lru_enabled"`     // LRU will only run when LRUEnabled is true
	DontEvictTime    time.Duration `json:"-"`               // omitempty
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
	ValidateWarmGet bool `json:"validate_warm_get"`
}

//==============================
//
// config functions
//
//==============================
func initconfigparam() error {
	getConfig(clivars.conffile)

	err := flag.Lookup("log_dir").Value.Set(ctx.config.Logdir)
	if err != nil {
		glog.Errorf("Failed to flag-set glog dir %q, err: %v", ctx.config.Logdir, err)
	}
	if err = CreateDir(ctx.config.Logdir); err != nil {
		glog.Errorf("Failed to create log dir %q, err: %v", ctx.config.Logdir, err)
		return err
	}
	if err = validateconf(); err != nil {
		return err
	}
	// CLI override
	if clivars.statstime != 0 {
		ctx.config.StatsTime = clivars.statstime
	}
	if clivars.loglevel != "" {
		err = flag.Lookup("v").Value.Set(clivars.loglevel)
		ctx.config.Loglevel = clivars.loglevel
	} else {
		err = flag.Lookup("v").Value.Set(ctx.config.Loglevel)
	}
	if err != nil {
		//  Not fatal as it will use default logging level
		glog.Errorf("Failed to set loglevel %v", err)
	}
	glog.Infof("Logdir: %q Proto: %s Port: %s Verbosity: %s",
		ctx.config.Logdir, ctx.config.Listen.Proto, ctx.config.Listen.Port, ctx.config.Loglevel)
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

func validateconf() (err error) {
	// durations
	if ctx.config.StatsTime, err = time.ParseDuration(ctx.config.StatsTimeStr); err != nil {
		return fmt.Errorf("Bad stats-time format %s, err: %v", ctx.config.StatsTimeStr, err)
	}
	if ctx.config.HTTP.Timeout, err = time.ParseDuration(ctx.config.HTTP.TimeoutStr); err != nil {
		return fmt.Errorf("Bad http-timeout format %s, err: %v", ctx.config.HTTP.TimeoutStr, err)
	}
	if ctx.config.HTTP.Timeout, err = time.ParseDuration(ctx.config.HTTP.LongTimeoutStr); err != nil {
		return fmt.Errorf("Bad http-long-timeout format %s, err %v", ctx.config.HTTP.LongTimeoutStr, err)
	}
	if ctx.config.KeepAliveTime, err = time.ParseDuration(ctx.config.KeepAliveTimeStr); err != nil {
		return fmt.Errorf("Bad keep-alive format %s, err: %v", ctx.config.KeepAliveTimeStr, err)
	}
	if ctx.config.LRUConfig.DontEvictTime, err = time.ParseDuration(ctx.config.LRUConfig.DontEvictTimeStr); err != nil {
		return fmt.Errorf("Bad dont-evict-time format %s, err: %v", ctx.config.LRUConfig.DontEvictTimeStr, err)
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
	return nil
}
