/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package cmn provides common low-level types and utilities for all dfcpub projects
package cmn

import (
	"time"
)

//
// CONFIGURATION
//
type Config struct {
	Confdir          string          `json:"confdir"`
	CloudProvider    string          `json:"cloudprovider"`
	CloudBuckets     string          `json:"cloud_buckets"`
	LocalBuckets     string          `json:"local_buckets"`
	Readahead        RahConf         `json:"readahead"`
	Log              LogConf         `json:"log"`
	Periodic         PeriodConf      `json:"periodic"`
	Timeout          TimeoutConf     `json:"timeout"`
	Proxy            ProxyConf       `json:"proxyconfig"`
	LRU              LRUConf         `json:"lru_config"`
	Xaction          XactionConf     `json:"xaction_config"`
	Rebalance        RebalanceConf   `json:"rebalance_conf"`
	Replication      ReplicationConf `json:"replication"`
	Cksum            CksumConf       `json:"cksum_config"`
	Ver              VersionConf     `json:"version_config"`
	FSpaths          SimpleKVs       `json:"fspaths"`
	TestFSP          TestfspathConf  `json:"test_fspaths"`
	Net              NetConf         `json:"netconfig"`
	FSHC             FSHCConf        `json:"fshc"`
	Auth             AuthConf        `json:"auth"`
	KeepaliveTracker KeepaliveConf   `json:"keepalivetracker"`
}

type RahConf struct {
	ObjectMem int64 `json:"rahobjectmem"`
	TotalMem  int64 `json:"rahtotalmem"`
	ByProxy   bool  `json:"rahbyproxy"`
	Discard   bool  `json:"rahdiscard"`
	Enabled   bool  `json:"rahenabled"`
}

type LogConf struct {
	Dir      string `json:"logdir"`      // log directory
	Level    string `json:"loglevel"`    // log level aka verbosity
	MaxSize  uint64 `json:"logmaxsize"`  // size that triggers log rotation
	MaxTotal uint64 `json:"logmaxtotal"` // max total size of all the logs in the log directory
}

type PeriodConf struct {
	StatsTimeStr     string `json:"stats_time"`
	RetrySyncTimeStr string `json:"retry_sync_time"`
	// omitempty
	StatsTime     time.Duration `json:"-"`
	RetrySyncTime time.Duration `json:"-"`
}

// timeoutconfig contains timeouts used for intra-cluster communication
type TimeoutConf struct {
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

type ProxyConf struct {
	NonElectable bool   `json:"non_electable"`
	PrimaryURL   string `json:"primary_url"`
	OriginalURL  string `json:"original_url"`
	DiscoveryURL string `json:"discovery_url"`
}

type LRUConf struct {
	// LowWM: Self-throttling mechanisms are suspended if disk utilization is below LowWM
	LowWM int64 `json:"lowwm"`

	// HighWM: Self-throttling mechanisms are fully engaged if disk utilization is above HighWM
	HighWM int64 `json:"highwm"`

	// AtimeCacheMax represents the maximum number of entries
	AtimeCacheMax uint64 `json:"atime_cache_max"`

	// DontEvictTimeStr denotes the period of time during which eviction of an object
	// is forbidden [atime, atime + DontEvictTime]
	DontEvictTimeStr string `json:"dont_evict_time"`

	// DontEvictTime is the parsed value of DontEvictTimeStr
	DontEvictTime time.Duration `json:"-"`

	// CapacityUpdTimeStr denotes the frequency with which DFC updates filesystem usage
	CapacityUpdTimeStr string `json:"capacity_upd_time"`

	// CapacityUpdTime is the parsed value of CapacityUpdTimeStr
	CapacityUpdTime time.Duration `json:"-"`

	// LRUEnabled: LRU will only run when set to true
	LRUEnabled bool `json:"lru_enabled"`
}

type XactionConf struct {
	DiskUtilLowWM  int64 `json:"disk_util_low_wm"`  // Low watermark below which no throttling is required
	DiskUtilHighWM int64 `json:"disk_util_high_wm"` // High watermark above which throttling is required for longer duration
}

type RebalanceConf struct {
	DestRetryTimeStr string        `json:"dest_retry_time"`
	DestRetryTime    time.Duration `json:"-"` //
	Enabled          bool          `json:"rebalancing_enabled"`
}

type ReplicationConf struct {
	ReplicateOnColdGet     bool `json:"replicate_on_cold_get"`     // object replication on cold GET request
	ReplicateOnPut         bool `json:"replicate_on_put"`          // object replication on PUT request
	ReplicateOnLRUEviction bool `json:"replicate_on_lru_eviction"` // object replication on LRU eviction
}

type CksumConf struct {
	// Checksum: hashing algorithm used to check for object corruption
	// Values: none, xxhash, md5, inherit
	// Value of 'none' disables hash checking
	Checksum string `json:"checksum"`

	// ValidateColdGet determines whether or not the checksum of received object
	// is checked after downloading it from the cloud or next tier
	ValidateColdGet bool `json:"validate_checksum_cold_get"`

	// ValidateWarmGet: if enabled, the object's version (if in Cloud-based bucket)
	// and checksum are checked. If either value fail to match, the object
	// is removed from local storage
	ValidateWarmGet bool `json:"validate_checksum_warm_get"`

	// EnableReadRangeChecksum: Return read range checksum otherwise return entire object checksum
	EnableReadRangeChecksum bool `json:"enable_read_range_checksum"`
}

type VersionConf struct {
	ValidateWarmGet bool   `json:"validate_version_warm_get"` // True: validate object version upon warm GET
	Versioning      string `json:"versioning"`                // types of objects versioning is enabled for: all, cloud, local, none
}

type TestfspathConf struct {
	Root     string `json:"root"`
	Count    int    `json:"count"`
	Instance int    `json:"instance"`
}

type NetConf struct {
	IPv4             string   `json:"ipv4"`
	IPv4IntraControl string   `json:"ipv4_intra_control"`
	IPv4IntraData    string   `json:"ipv4_intra_data"`
	UseIntraControl  bool     `json:"-"`
	UseIntraData     bool     `json:"-"`
	L4               L4Conf   `json:"l4"`
	HTTP             HTTPConf `json:"http"`
}

type L4Conf struct {
	Proto               string `json:"proto"` // tcp, udp
	PortStr             string `json:"port"`  // listening port
	Port                int    `json:"-"`
	PortIntraControlStr string `json:"port_intra_control"` // listening port for intra control network
	PortIntraControl    int    `json:"-"`
	PortIntraDataStr    string `json:"port_intra_data"` // listening port for intra data network
	PortIntraData       int    `json:"-"`
}

type HTTPConf struct {
	Proto         string `json:"proto"`              // http or https
	RevProxy      string `json:"rproxy"`             // RevProxy* enum
	Certificate   string `json:"server_certificate"` // HTTPS: openssl certificate
	Key           string `json:"server_key"`         // HTTPS: openssl key
	MaxNumTargets int    `json:"max_num_targets"`    // estimated max num targets (to count idle conns)
	UseHTTPS      bool   `json:"use_https"`          // use HTTPS instead of HTTP
}

type FSHCConf struct {
	Enabled       bool `json:"fshc_enabled"`
	TestFileCount int  `json:"fshc_test_files"`  // the number of files to read and write during a test
	ErrorLimit    int  `json:"fshc_error_limit"` // max number of errors (exceeding any results in disabling mpath)
}

type AuthConf struct {
	Secret  string `json:"secret"`
	Enabled bool   `json:"enabled"`
	CredDir string `json:"creddir"`
}

// config for one keepalive tracker
// all type of trackers share the same struct, not all fields are used by all trackers
type KeepaliveTrackerConf struct {
	IntervalStr string        `json:"interval"` // keepalives are sent(target)/checked(promary proxy) every interval
	Interval    time.Duration `json:"-"`
	Name        string        `json:"name"`   // "heartbeat", "average"
	Factor      int           `json:"factor"` // "average" only
}

type KeepaliveConf struct {
	Proxy  KeepaliveTrackerConf `json:"proxy"`  // how proxy tracks target keepalives
	Target KeepaliveTrackerConf `json:"target"` // how target tracks primary proxies keepalives
}
