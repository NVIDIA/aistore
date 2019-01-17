// Package cmn provides common low-level types and utilities for all dfcpub projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

// as in: mountpath/<content-type>/<CloudBs|LocalBs>/<bucket-name>/...
const (
	CloudBs = "cloud"
	LocalBs = "local"
)

// $CONFDIR/*
const (
	SmapBackupFile       = "smap.json"
	BucketmdBackupFile   = "bucket-metadata" // base name of the config file; not to confuse with config.Localbuckets mpath
	MountpathBackupFile  = "mpaths"          // base name to persist fs.Mountpaths
	RebalanceMarker      = ".rebalancing"
	LocalRebalanceMarker = ".localrebalancing"
)

const (
	RevProxyCloud  = "cloud"
	RevProxyTarget = "target"

	KeepaliveHeartbeatType = "heartbeat"
	KeepaliveAverageType   = "average"
)

const (
	ThrottleSleepMin = time.Millisecond * 10
	ThrottleSleepAvg = time.Millisecond * 100
	ThrottleSleepMax = time.Second
)

//
// CONFIG PROVIDER
//
var (
	_ ConfigOwner = &globalConfigOwner{}
)

type (
	// ConfigOwner is interface for interacting with config. For updating we
	// introduce two functions: BeginUpdate and CommitUpdate. These two should
	// protect config from being updated simultaneously (update should work
	// as transaction).
	//
	// Subscribe method should be used by services which require to be notified
	// about any config changes.
	ConfigOwner interface {
		Get() *Config
		BeginUpdate() *Config
		CommitUpdate(config *Config)
		Subscribe(cl ConfigListener)
	}

	// ConfigListener is interface for listeners which require to be notified
	// about config updates.
	ConfigListener interface {
		ConfigUpdate(oldConf, newConf *Config)
	}
)

// globalConfigOwner implements ConfigOwner interface. The implementation is
// protecting config only from concurrent updates but does not use CoW or other
// techniques which involves cloning and updating config. This might change when
// we will have use case for that - then Get and Put would need to be changed
// accordingly.
type globalConfigOwner struct {
	mtx       sync.Mutex // mutex for protecting updates of config
	c         unsafe.Pointer
	lmtx      sync.Mutex // mutex for protecting listeners
	listeners []ConfigListener
}

var (
	// GCO stands for global config owner which is responsible for updating
	// and notifying listeners about any changes in the config. Config is loaded
	// at startup and then can be accessed/updated by other services.
	GCO = &globalConfigOwner{}
)

func init() {
	config := &Config{}
	atomic.StorePointer(&GCO.c, unsafe.Pointer(config))
}

func (gco *globalConfigOwner) Get() *Config {
	return (*Config)(atomic.LoadPointer(&gco.c))
}

// When updating we need to make sure that the update is transaction and no
// other update can happen when other transaction is in progress. Therefore,
// we introduce locking mechanism which targets this problem.
//
// NOTE: BeginUpdate should be followed by CommitUpdate.
func (gco *globalConfigOwner) BeginUpdate() *Config {
	gco.mtx.Lock()
	config := &Config{}

	// FIXME: CopyStruct is actually shallow copy but because Config
	// has only values (no pointers or slices, except FSPaths) it is
	// deepcopy. This may break in the future, so we need solution
	// to make sure that we do *proper* deepcopy with good performance.
	CopyStruct(config, gco.Get())

	return config
}

// CommitUpdate ends transaction of updating config and notifies listeners
// about changes in config.
//
// NOTE: CommitUpdate should be preceded by BeginUpdate.
func (gco *globalConfigOwner) CommitUpdate(config *Config) {
	oldConf := gco.Get()
	atomic.StorePointer(&GCO.c, unsafe.Pointer(config))

	// TODO: Notify listeners is protected by GCO lock to make sure
	// that config updates are done in correct order. But it has
	// performance impact and it needs to be revisited.
	gco.notifyListeners(oldConf)

	gco.mtx.Unlock()
}

func (gco *globalConfigOwner) notifyListeners(oldConf *Config) {
	gco.lmtx.Lock()
	newConf := gco.Get()
	for _, listener := range gco.listeners {
		listener.ConfigUpdate(oldConf, newConf)
	}
	gco.lmtx.Unlock()
}

// Subscribe allows listeners to sign up for notifications about config updates.
func (gco *globalConfigOwner) Subscribe(cl ConfigListener) {
	gco.lmtx.Lock()
	gco.listeners = append(gco.listeners, cl)
	gco.lmtx.Unlock()
}

//
// CONFIGURATION
//
type Config struct {
	Confdir          string          `json:"confdir"`
	CloudProvider    string          `json:"cloudprovider"`
	Mirror           MirrorConf      `json:"mirror"`
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

type MirrorConf struct {
	Copies            int64 `json:"copies"`              // num local copies
	MirrorBurst       int64 `json:"mirror_burst_buffer"` // channel buffer size
	MirrorUtilThresh  int64 `json:"mirror_util_thresh"`  // utilizations are considered equivalent when below this threshold
	MirrorOptimizePUT bool  `json:"mirror_optimize_put"` // optimization objective
	MirrorEnabled     bool  `json:"mirror_enabled"`      // will only generate local copies when set to true
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
	IostatTimeStr    string `json:"iostat_time"`
	RetrySyncTimeStr string `json:"retry_sync_time"`
	// omitempty
	StatsTime     time.Duration `json:"-"`
	IostatTime    time.Duration `json:"-"`
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
	AtimeCacheMax int64 `json:"atime_cache_max"`

	// DontEvictTimeStr denotes the period of time during which eviction of an object
	// is forbidden [atime, atime + DontEvictTime]
	DontEvictTimeStr string `json:"dont_evict_time"`

	// DontEvictTime is the parsed value of DontEvictTimeStr
	DontEvictTime time.Duration `json:"-"`

	// CapacityUpdTimeStr denotes the frequency at which AIStore updates local capacity utilization
	CapacityUpdTimeStr string `json:"capacity_upd_time"`

	// CapacityUpdTime is the parsed value of CapacityUpdTimeStr
	CapacityUpdTime time.Duration `json:"-"`

	// LRULocalBuckets: Enables or disables LRU for local buckets
	LRULocalBuckets bool `json:"lru_local_buckets"`

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

//==============================
//
// config functions
//
//==============================
func LoadConfig(configFile string, statsTime time.Duration, proxyURL string, logLevel string) error {
	config := GCO.BeginUpdate()
	defer GCO.CommitUpdate(config)

	err := LocalLoad(configFile, &config)
	if err != nil {
		glog.Errorf("Failed to load config %q, err: %v", configFile, err)
		os.Exit(1)
	}

	if err = flag.Lookup("log_dir").Value.Set(config.Log.Dir); err != nil {
		glog.Errorf("Failed to flag-set glog dir %q, err: %v", config.Log.Dir, err)
	}
	if err = CreateDir(config.Log.Dir); err != nil {
		glog.Errorf("Failed to create log dir %q, err: %v", config.Log.Dir, err)
		return err
	}
	if err = validateConfig(config); err != nil {
		return err
	}

	// glog rotate
	glog.MaxSize = config.Log.MaxSize
	if glog.MaxSize > GiB {
		glog.Errorf("Log.MaxSize %d exceeded 1GB, setting the default 1MB", glog.MaxSize)
		glog.MaxSize = MiB
	}

	// Set helpers
	config.Net.HTTP.Proto = "http" // not validating: read-only, and can take only two values
	if config.Net.HTTP.UseHTTPS {
		config.Net.HTTP.Proto = "https"
	}

	differentIPs := config.Net.IPv4 != config.Net.IPv4IntraControl
	differentPorts := config.Net.L4.Port != config.Net.L4.PortIntraControl
	config.Net.UseIntraControl = false
	if config.Net.IPv4IntraControl != "" && config.Net.L4.PortIntraControl != 0 && (differentIPs || differentPorts) {
		config.Net.UseIntraControl = true
	}

	differentIPs = config.Net.IPv4 != config.Net.IPv4IntraData
	differentPorts = config.Net.L4.Port != config.Net.L4.PortIntraData
	config.Net.UseIntraData = false
	if config.Net.IPv4IntraData != "" && config.Net.L4.PortIntraData != 0 && (differentIPs || differentPorts) {
		config.Net.UseIntraData = true
	}

	// CLI override
	if statsTime != 0 {
		config.Periodic.StatsTime = statsTime
	}
	if proxyURL != "" {
		config.Proxy.PrimaryURL = proxyURL
	}
	if logLevel != "" {
		if err = SetLogLevel(config, logLevel); err != nil {
			glog.Errorf("Failed to set log level = %s, err: %v", logLevel, err)
		}
	} else {
		if err = SetLogLevel(config, config.Log.Level); err != nil {
			glog.Errorf("Failed to set log level = %s, err: %v", config.Log.Level, err)
		}
	}
	glog.Infof("Logdir: %q Proto: %s Port: %d Verbosity: %s",
		config.Log.Dir, config.Net.L4.Proto, config.Net.L4.Port, config.Log.Level)
	glog.Infof("Config: %q StatsTime: %v", configFile, config.Periodic.StatsTime)
	return err
}

func ValidateVersion(version string) error {
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

func validateConfig(config *Config) (err error) {
	const badfmt = "Bad %q format, err: %v"
	var (
		periodic  = &config.Periodic
		lru       = &config.LRU
		mirror    = &config.Mirror
		timeout   = &config.Timeout
		keepalive = &config.KeepaliveTracker
		net       = &config.Net
	)
	// durations
	if periodic.StatsTime, err = time.ParseDuration(periodic.StatsTimeStr); err != nil {
		return fmt.Errorf(badfmt, periodic.StatsTimeStr, err)
	}
	if periodic.IostatTime, err = time.ParseDuration(periodic.IostatTimeStr); err != nil {
		return fmt.Errorf(badfmt, periodic.IostatTimeStr, err)
	}
	if periodic.RetrySyncTime, err = time.ParseDuration(periodic.RetrySyncTimeStr); err != nil {
		return fmt.Errorf(badfmt, periodic.RetrySyncTimeStr, err)
	}
	if lru.DontEvictTime, err = time.ParseDuration(lru.DontEvictTimeStr); err != nil {
		return fmt.Errorf(badfmt, lru.DontEvictTimeStr, err)
	}
	if lru.CapacityUpdTime, err = time.ParseDuration(lru.CapacityUpdTimeStr); err != nil {
		return fmt.Errorf(badfmt, lru.CapacityUpdTimeStr, err)
	}
	if config.Rebalance.DestRetryTime, err = time.ParseDuration(config.Rebalance.DestRetryTimeStr); err != nil {
		return fmt.Errorf(badfmt, config.Rebalance.DestRetryTimeStr, err)
	}

	hwm, lwm := lru.HighWM, lru.LowWM
	if hwm <= 0 || lwm <= 0 || hwm < lwm || lwm > 100 || hwm > 100 {
		return fmt.Errorf("Invalid LRU configuration %+v", lru)
	}
	if mirror.MirrorUtilThresh < 0 || mirror.MirrorUtilThresh > 100 || mirror.MirrorBurst < 0 {
		return fmt.Errorf("Invalid mirror configuration %+v", mirror)
	}
	if mirror.MirrorEnabled && mirror.Copies != 2 {
		return fmt.Errorf("Invalid mirror configuration %+v", mirror)
	}

	diskUtilHWM, diskUtilLWM := config.Xaction.DiskUtilHighWM, config.Xaction.DiskUtilLowWM
	if diskUtilHWM <= 0 || diskUtilLWM <= 0 || diskUtilHWM <= diskUtilLWM || diskUtilLWM > 100 || diskUtilHWM > 100 {
		return fmt.Errorf("Invalid Xaction configuration %+v", config.Xaction)
	}

	if config.Cksum.Checksum != ChecksumXXHash && config.Cksum.Checksum != ChecksumNone {
		return fmt.Errorf("Invalid checksum: %s - expecting %s or %s", config.Cksum.Checksum, ChecksumXXHash, ChecksumNone)
	}
	if err := ValidateVersion(config.Ver.Versioning); err != nil {
		return err
	}
	if timeout.Default, err = time.ParseDuration(timeout.DefaultStr); err != nil {
		return fmt.Errorf(badfmt, timeout.DefaultStr, err)
	}
	if timeout.DefaultLong, err = time.ParseDuration(timeout.DefaultLongStr); err != nil {
		return fmt.Errorf(badfmt, timeout.DefaultLongStr, err)
	}
	if timeout.MaxKeepalive, err = time.ParseDuration(timeout.MaxKeepaliveStr); err != nil {
		return fmt.Errorf("Bad Timeout max_keepalive format %s, err %v", timeout.MaxKeepaliveStr, err)
	}
	if timeout.ProxyPing, err = time.ParseDuration(timeout.ProxyPingStr); err != nil {
		return fmt.Errorf("Bad Timeout proxy_ping format %s, err %v", timeout.ProxyPingStr, err)
	}
	if timeout.CplaneOperation, err = time.ParseDuration(timeout.CplaneOperationStr); err != nil {
		return fmt.Errorf("Bad Timeout vote_request format %s, err %v", timeout.CplaneOperationStr, err)
	}
	if timeout.SendFile, err = time.ParseDuration(timeout.SendFileStr); err != nil {
		return fmt.Errorf("Bad Timeout send_file_time format %s, err %v", timeout.SendFileStr, err)
	}
	if timeout.Startup, err = time.ParseDuration(timeout.StartupStr); err != nil {
		return fmt.Errorf("Bad Proxy startup_time format %s, err %v", timeout.StartupStr, err)
	}
	keepalive.Proxy.Interval, err = time.ParseDuration(keepalive.Proxy.IntervalStr)
	if err != nil {
		return fmt.Errorf("bad proxy keep alive interval %s", keepalive.Proxy.IntervalStr)
	}

	keepalive.Target.Interval, err = time.ParseDuration(keepalive.Target.IntervalStr)
	if err != nil {
		return fmt.Errorf("bad target keep alive interval %s", keepalive.Target.IntervalStr)
	}

	if !validKeepaliveType(keepalive.Proxy.Name) {
		return fmt.Errorf("bad proxy keepalive tracker type %s", keepalive.Proxy.Name)
	}

	if !validKeepaliveType(keepalive.Target.Name) {
		return fmt.Errorf("bad target keepalive tracker type %s", keepalive.Target.Name)
	}

	// NETWORK

	// Parse ports
	if net.L4.Port, err = ParsePort(net.L4.PortStr); err != nil {
		return fmt.Errorf("Bad public port specified: %v", err)
	}

	net.L4.PortIntraControl = 0
	if net.L4.PortIntraControlStr != "" {
		if net.L4.PortIntraControl, err = ParsePort(net.L4.PortIntraControlStr); err != nil {
			return fmt.Errorf("Bad internal port specified: %v", err)
		}
	}
	net.L4.PortIntraData = 0
	if net.L4.PortIntraDataStr != "" {
		if net.L4.PortIntraData, err = ParsePort(net.L4.PortIntraDataStr); err != nil {
			return fmt.Errorf("Bad replication port specified: %v", err)
		}
	}

	net.IPv4 = strings.Replace(net.IPv4, " ", "", -1)
	net.IPv4IntraControl = strings.Replace(net.IPv4IntraControl, " ", "", -1)
	net.IPv4IntraData = strings.Replace(net.IPv4IntraData, " ", "", -1)

	if overlap, addr := ipv4ListsOverlap(net.IPv4, net.IPv4IntraControl); overlap {
		return fmt.Errorf(
			"Public and internal addresses overlap: %s (public: %s; internal: %s)",
			addr, net.IPv4, net.IPv4IntraControl,
		)
	}
	if overlap, addr := ipv4ListsOverlap(net.IPv4, net.IPv4IntraData); overlap {
		return fmt.Errorf(
			"Public and replication addresses overlap: %s (public: %s; replication: %s)",
			addr, net.IPv4, net.IPv4IntraData,
		)
	}
	if overlap, addr := ipv4ListsOverlap(net.IPv4IntraControl, net.IPv4IntraData); overlap {
		return fmt.Errorf(
			"Internal and replication addresses overlap: %s (internal: %s; replication: %s)",
			addr, net.IPv4IntraControl, net.IPv4IntraData,
		)
	}
	if net.HTTP.RevProxy != "" {
		if net.HTTP.RevProxy != RevProxyCloud && net.HTTP.RevProxy != RevProxyTarget {
			return fmt.Errorf("Invalid http rproxy configuration: %s (expecting: ''|%s|%s)",
				net.HTTP.RevProxy, RevProxyCloud, RevProxyTarget)
		}
	}
	return nil
}

func SetLogLevel(config *Config, loglevel string) (err error) {
	v := flag.Lookup("v").Value
	if v == nil {
		return fmt.Errorf("nil -v Value")
	}
	err = v.Set(loglevel)
	if err == nil {
		config.Log.Level = loglevel
	}
	return
}

// setGLogVModule sets glog's vmodule flag
// sets 'v' as is, no verificaton is done here
// syntax for v: target=5,proxy=1, p*=3, etc
func SetGLogVModule(v string) error {
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

// TestingEnv returns true if AIStore is running in a development environment
// where a single local filesystem is partitioned between all (locally running)
// targets and is used for both local and Cloud buckets
func TestingEnv() bool {
	return GCO.Get().TestFSP.Count > 0
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

// validKeepaliveType returns true if the keepalive type is supported.
func validKeepaliveType(t string) bool {
	return t == KeepaliveHeartbeatType || t == KeepaliveAverageType
}
