// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/glog"
)

// as in: mountpath/<content-type>/<CloudBs|LocalBs>/<bucket-name>/...
const (
	CloudBs = "cloud"
	LocalBs = "local"
)

// $CONFDIR/*
const (
	SmapBackupFile      = "smap.json"
	BucketmdBackupFile  = "bucket-metadata" // base name of the config file; not to confuse with config.Localbuckets mpath
	MountpathBackupFile = "mpaths"          // base name to persist fs.Mountpaths
	GlobalRebMarker     = ".global_rebalancing"
	LocalRebMarker      = ".local_rebalancing"
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

	// EC
	MinSliceCount = 1  // minimum number of data or parity slices
	MaxSliceCount = 32 // maximum number of data or parity slices
)

const (
	// L4
	tcpProto = "tcp"

	// L7
	httpProto  = "http"
	httpsProto = "https"
)

type (
	ValidationArgs struct {
		BckIsLocal bool
		TargetCnt  int // for EC
	}

	Validator interface {
		Validate() error
	}
	PropsValidator interface {
		ValidateAsProps(args *ValidationArgs) error
	}
)

var (
	supportedL4Protos = []string{tcpProto}
	supportedL7Protos = []string{httpProto, httpsProto}

	_ Validator = &Config{}
	_ Validator = &CksumConf{}
	_ Validator = &LRUConf{}
	_ Validator = &MirrorConf{}
	_ Validator = &ECConf{}
	_ Validator = &VersionConf{}
	_ Validator = &KeepaliveConf{}
	_ Validator = &PeriodConf{}
	_ Validator = &TimeoutConf{}
	_ Validator = &RebalanceConf{}
	_ Validator = &NetConf{}
	_ Validator = &DownloaderConf{}

	_ PropsValidator = &CksumConf{}
	_ PropsValidator = &LRUConf{}
	_ PropsValidator = &MirrorConf{}
	_ PropsValidator = &ECConf{}
)

//
// CONFIG PROVIDER
//
var (
	_ ConfigOwner = &globalConfigOwner{}
)

type (
	// ConfigOwner is interface for interacting with config. For updating we
	// introduce three functions: BeginUpdate, CommitUpdate and DiscardUpdate.
	// These funcs should protect config from being updated simultaneously
	// (update should work as transaction).
	//
	// Subscribe method should be used by services which require to be notified
	// about any config changes.
	ConfigOwner interface {
		Get() *Config
		Clone() *Config
		BeginUpdate() *Config
		CommitUpdate(config *Config)
		DiscardUpdate()

		Subscribe(cl ConfigListener)

		SetConfigFile(path string)
		GetConfigFile() string
	}

	// ConfigListener is interface for listeners which require to be notified
	// about config updates.
	ConfigListener interface {
		ConfigUpdate(oldConf, newConf *Config)
	}
	// selected config overrides via command line
	ConfigCLI struct {
		ConfFile  string        // config filename
		LogLevel  string        // takes precedence over config.Log.Level
		StatsTime time.Duration // overrides config.Periodic.StatsTime
		ProxyURL  string        // primary proxy URL to override config.Proxy.PrimaryURL
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
	confFile  string
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

func (gco *globalConfigOwner) Clone() *Config {
	config := &Config{}

	// FIXME: CopyStruct is actually shallow copy but because Config
	// has only values (no pointers or slices, except FSPaths) it is
	// deepcopy. This may break in the future, so we need solution
	// to make sure that we do *proper* deepcopy with good performance.
	CopyStruct(config, gco.Get())
	return config
}

// When updating we need to make sure that the update is transaction and no
// other update can happen when other transaction is in progress. Therefore,
// we introduce locking mechanism which targets this problem.
//
// NOTE: BeginUpdate should be followed by CommitUpdate.
func (gco *globalConfigOwner) BeginUpdate() *Config {
	gco.mtx.Lock()
	return gco.Clone()
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

// CommitUpdate ends transaction but contrary to CommitUpdate it does not update
// the config nor it notifies listeners.
//
// NOTE: CommitUpdate should be preceded by BeginUpdate.
func (gco *globalConfigOwner) DiscardUpdate() {
	gco.mtx.Unlock()
}

func (gco *globalConfigOwner) SetConfigFile(path string) {
	gco.mtx.Lock()
	gco.confFile = path
	gco.mtx.Unlock()
}
func (gco *globalConfigOwner) GetConfigFile() string {
	gco.mtx.Lock()
	defer gco.mtx.Unlock()
	return gco.confFile
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
	Proxy            ProxyConf       `json:"proxy"`
	LRU              LRUConf         `json:"lru"`
	Xaction          XactionConf     `json:"xaction"`
	Rebalance        RebalanceConf   `json:"rebalance"`
	Replication      ReplicationConf `json:"replication"`
	Cksum            CksumConf       `json:"cksum"`
	Ver              VersionConf     `json:"version"`
	FSpaths          SimpleKVs       `json:"fspaths"`
	TestFSP          TestfspathConf  `json:"test_fspaths"`
	Net              NetConf         `json:"net"`
	FSHC             FSHCConf        `json:"fshc"`
	Auth             AuthConf        `json:"auth"`
	KeepaliveTracker KeepaliveConf   `json:"keepalivetracker"`
	Downloader       DownloaderConf  `json:"downloader"`
}

type MirrorConf struct {
	Copies      int64 `json:"copies"`       // num local copies
	Burst       int64 `json:"burst_buffer"` // channel buffer size
	UtilThresh  int64 `json:"util_thresh"`  // utilizations are considered equivalent when below this threshold
	OptimizePUT bool  `json:"optimize_put"` // optimization objective
	Enabled     bool  `json:"enabled"`      // will only generate local copies when set to true
}

type RahConf struct {
	ObjectMem int64 `json:"object_mem"`
	TotalMem  int64 `json:"total_mem"`
	ByProxy   bool  `json:"by_proxy"`
	Discard   bool  `json:"discard"`
	Enabled   bool  `json:"enabled"`
}

type LogConf struct {
	Dir      string `json:"dir"`       // log directory
	Level    string `json:"level"`     // log level aka verbosity
	MaxSize  uint64 `json:"max_size"`  // size that triggers log rotation
	MaxTotal uint64 `json:"max_total"` // max total size of all the logs in the log directory
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
	ListStr            string        `json:"list_timeout"`
	List               time.Duration `json:"-"` //
}

type ProxyConf struct {
	NonElectable bool   `json:"non_electable"`
	PrimaryURL   string `json:"primary_url"`
	OriginalURL  string `json:"original_url"`
	DiscoveryURL string `json:"discovery_url"`
}

type LRUConf struct {
	// LowWM: used capacity low-watermark (% of total local storage capacity)
	LowWM int64 `json:"lowwm"`

	// HighWM: used capacity high-watermark (% of total local storage capacity)
	// NOTE:
	// - LRU starts evicting objects when the currently used capacity (used-cap) gets above HighWM
	// - and keeps evicting objects until the used-cap gets below LowWM
	// - while self-throttling itself in accordance with target utilization
	HighWM int64 `json:"highwm"`

	// Out-of-Space: if exceeded, the target starts failing new PUTs and keeps
	// failing them until its local used-cap gets back below HighWM (see above)
	OOS int64 `json:"out_of_space"`

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

	// LocalBuckets: Enables or disables LRU for local buckets
	LocalBuckets bool `json:"local_buckets"`

	// Enabled: LRU will only run when set to true
	Enabled bool `json:"enabled"`
}

type XactionConf struct {
	DiskUtilLowWM  int64 `json:"disk_util_low_wm"`  // Low watermark below which no throttling is required
	DiskUtilHighWM int64 `json:"disk_util_high_wm"` // High watermark above which throttling is required for longer duration
}

type RebalanceConf struct {
	DestRetryTimeStr string        `json:"dest_retry_time"`
	DestRetryTime    time.Duration `json:"-"` //
	Enabled          bool          `json:"enabled"`
}

type ReplicationConf struct {
	OnColdGet     bool `json:"on_cold_get"`     // object replication on cold GET request
	OnPut         bool `json:"on_put"`          // object replication on PUT request
	OnLRUEviction bool `json:"on_lru_eviction"` // object replication on LRU eviction
}

type CksumConf struct {
	// Type of hashing algorithm used to check for object corruption
	// Values: none, xxhash, md5, inherit
	// Value of 'none' disables hash checking
	Type string `json:"type"`

	// ValidateColdGet determines whether or not the checksum of received object
	// is checked after downloading it from the cloud or next tier
	ValidateColdGet bool `json:"validate_cold_get"`

	// ValidateWarmGet: if enabled, the object's version (if in Cloud-based bucket)
	// and checksum are checked. If either value fail to match, the object
	// is removed from local storage
	ValidateWarmGet bool `json:"validate_warm_get"`

	// ValidateClusterMigration determines if the migrated objects across single cluster
	// should have their checksum validated.
	ValidateClusterMigration bool `json:"validate_cluster_migration"`

	// EnableReadRange: Return read range checksum otherwise return entire object checksum
	EnableReadRange bool `json:"enable_read_range"`
}

type VersionConf struct {
	Versioning      string `json:"versioning"`        // types of objects versioning is enabled for: all, cloud, local, none
	ValidateWarmGet bool   `json:"validate_warm_get"` // True: validate object version upon warm GET
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
	RevProxyCache bool   `json:"rproxy_cache"`       // RevProxy caches or work as transparent proxy
	Certificate   string `json:"server_certificate"` // HTTPS: openssl certificate
	Key           string `json:"server_key"`         // HTTPS: openssl key
	MaxNumTargets int    `json:"max_num_targets"`    // estimated max num targets (to count idle conns)
	UseHTTPS      bool   `json:"use_https"`          // use HTTPS instead of HTTP
}

type FSHCConf struct {
	Enabled       bool `json:"enabled"`
	TestFileCount int  `json:"test_files"`  // the number of files to read and write during a test
	ErrorLimit    int  `json:"error_limit"` // max number of errors (exceeding any results in disabling mpath)
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
	Factor      uint8         `json:"factor"` // only average
}

type KeepaliveConf struct {
	Proxy         KeepaliveTrackerConf `json:"proxy"`  // how proxy tracks target keepalives
	Target        KeepaliveTrackerConf `json:"target"` // how target tracks primary proxies keepalives
	RetryFactor   uint8                `json:"retry_factor"`
	TimeoutFactor uint8                `json:"timeout_factor"`
}

type DownloaderConf struct {
	TimeoutStr string        `json:"timeout"`
	Timeout    time.Duration `json:"-"`
}

//==============================
//
// config functions
//
//==============================
func LoadConfig(clivars *ConfigCLI) (changed bool) {
	GCO.SetConfigFile(clivars.ConfFile)

	config := GCO.BeginUpdate()
	defer GCO.CommitUpdate(config)

	err := LocalLoad(clivars.ConfFile, &config)

	// NOTE: glog.Errorf + os.Exit is used instead of glog.Fatalf to not crash
	// with dozens of backtraces on screen - this is user error not some
	// internal error.

	if err != nil {
		glog.Errorf("failed to load config %q, err: %v", clivars.ConfFile, err)
		os.Exit(1)
	}
	if err = flag.Lookup("log_dir").Value.Set(config.Log.Dir); err != nil {
		glog.Errorf("failed to flag-set glog dir %q, err: %v", config.Log.Dir, err)
		os.Exit(1)
	}
	if err = CreateDir(config.Log.Dir); err != nil {
		glog.Errorf("failed to create log dir %q, err: %v", config.Log.Dir, err)
		os.Exit(1)
	}
	if err := config.Validate(); err != nil {
		glog.Errorf("%v", err)
		os.Exit(1)
	}

	// glog rotate
	glog.MaxSize = config.Log.MaxSize
	if glog.MaxSize > GiB {
		glog.Errorf("Log.MaxSize %d exceeded 1GB, setting the default 1MB", glog.MaxSize)
		glog.MaxSize = MiB
	}

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
	if clivars.StatsTime != 0 {
		config.Periodic.StatsTime = clivars.StatsTime
		changed = true
	}
	if clivars.ProxyURL != "" {
		config.Proxy.PrimaryURL = clivars.ProxyURL
		changed = true
	}
	if clivars.LogLevel != "" {
		if err = SetLogLevel(config, clivars.LogLevel); err != nil {
			glog.Errorf("Failed to set log level = %s, err: %v", clivars.LogLevel, err)
			os.Exit(1)
		}
		config.Log.Level = clivars.LogLevel
		changed = true
	} else if err = SetLogLevel(config, config.Log.Level); err != nil {
		glog.Errorf("Failed to set log level = %s, err: %v", config.Log.Level, err)
		os.Exit(1)
	}
	glog.Infof("Logdir: %q Proto: %s Port: %d Verbosity: %s",
		config.Log.Dir, config.Net.L4.Proto, config.Net.L4.Port, config.Log.Level)
	glog.Infof("Config: %q StatsTime: %v", clivars.ConfFile, config.Periodic.StatsTime)
	return
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
		return fmt.Errorf("invalid version: %s - expecting one of %s", version, strings.Join(versions, ", "))
	}
	return nil
}

func (c *Config) Validate() error {
	validators := []Validator{
		&c.Xaction, &c.LRU, &c.Mirror, &c.Cksum,
		&c.Timeout, &c.Periodic, &c.Rebalance, &c.KeepaliveTracker, &c.Net, &c.Ver,
		&c.Downloader,
	}
	for _, validator := range validators {
		if err := validator.Validate(); err != nil {
			return err
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

func (c *XactionConf) Validate() error {
	lwm, hwm := c.DiskUtilLowWM, c.DiskUtilHighWM
	if lwm <= 0 || hwm <= lwm || hwm > 100 {
		return fmt.Errorf("invalid xaction (disk_util_lwm, disk_util_hwm) configuration %+v", c)
	}
	return nil
}

func (c *LRUConf) Validate() (err error) {
	lwm, hwm, oos := c.LowWM, c.HighWM, c.OOS
	if lwm <= 0 || hwm < lwm || oos < hwm || oos > 100 {
		return fmt.Errorf("invalid lru (lwm, hwm, oos) configuration %+v", c)
	}
	if c.DontEvictTime, err = time.ParseDuration(c.DontEvictTimeStr); err != nil {
		return fmt.Errorf("invalid lru.dont_evict_time format: %v", err)
	}
	if c.CapacityUpdTime, err = time.ParseDuration(c.CapacityUpdTimeStr); err != nil {
		return fmt.Errorf("invalid lru.capacity_upd_time format: %v", err)
	}
	return nil
}

func (c *LRUConf) ValidateAsProps(args *ValidationArgs) (err error) {
	if !c.Enabled {
		return nil
	}
	return c.Validate()
}

func (c *CksumConf) Validate() error {
	if c.Type != ChecksumXXHash && c.Type != ChecksumNone {
		return fmt.Errorf("invalid checksum.type: %s (expected one of [%s, %s])", c.Type, ChecksumXXHash, ChecksumNone)
	}
	return nil
}

func (c *CksumConf) ValidateAsProps(args *ValidationArgs) error {
	if c.Type != ChecksumInherit && c.Type != ChecksumNone && c.Type != ChecksumXXHash {
		return fmt.Errorf("invalid checksum.type: %s (expected one of: [%s, %s, %s])",
			c.Type, ChecksumXXHash, ChecksumNone, ChecksumInherit)
	}
	return nil
}

func (c *MirrorConf) Validate() error {
	if c.UtilThresh < 0 || c.UtilThresh > 100 {
		return fmt.Errorf("bad mirror.util_thresh: %v (expected value in range [0, 100])", c.UtilThresh)
	}
	if c.Burst < 0 {
		return fmt.Errorf("bad mirror.burst: %v (expected >0)", c.UtilThresh)
	}
	if c.Enabled && c.Copies != 2 {
		return fmt.Errorf("bad mirror.copies: %d (expected 2)", c.Copies)
	}
	return nil
}

func (c *MirrorConf) ValidateAsProps(args *ValidationArgs) error {
	if !c.Enabled {
		return nil
	}
	return c.Validate()
}

func (c *ECConf) Validate() error {
	if c.ObjSizeLimit < 0 {
		return fmt.Errorf("bad ec.obj_size_limit: %d (expected >=0)", c.ObjSizeLimit)
	}
	if c.DataSlices < MinSliceCount || c.DataSlices > MaxSliceCount {
		return fmt.Errorf("bad ec.data_slices: %d (expected value in range [%d, %d])", c.DataSlices, MinSliceCount, MaxSliceCount)
	}
	// TODO: warn about performance if number is OK but large?
	if c.ParitySlices < MinSliceCount || c.ParitySlices > MaxSliceCount {
		return fmt.Errorf("bad ec.parity_slices: %d (expected value in range [%d, %d])", c.ParitySlices, MinSliceCount, MaxSliceCount)
	}
	return nil
}

func (c *ECConf) ValidateAsProps(args *ValidationArgs) error {
	if !c.Enabled {
		return nil
	}
	if !args.BckIsLocal {
		return fmt.Errorf("erasure coding does not support cloud buckets")
	}
	if err := c.Validate(); err != nil {
		return err
	}
	// data slices + parity slices + original object
	required := c.DataSlices + c.ParitySlices + 1
	if args.TargetCnt < required {
		return fmt.Errorf(
			"erasure coding requires %d targets to use %d data and %d parity slices "+
				"(the cluster has only %d targets)",
			required, c.DataSlices, c.ParitySlices, args.TargetCnt)
	}
	return nil
}

func (c *TimeoutConf) Validate() (err error) {
	if c.Default, err = time.ParseDuration(c.DefaultStr); err != nil {
		return fmt.Errorf("bad timeout.default format %s, err %v", c.DefaultStr, err)
	}
	if c.DefaultLong, err = time.ParseDuration(c.DefaultLongStr); err != nil {
		return fmt.Errorf("bad timeout.default_long format %s, err %v", c.DefaultLongStr, err)
	}
	if c.List, err = time.ParseDuration(c.ListStr); err != nil {
		return fmt.Errorf("bad timeout.list_timeout format %s, err %v", c.ListStr, err)
	}
	if c.MaxKeepalive, err = time.ParseDuration(c.MaxKeepaliveStr); err != nil {
		return fmt.Errorf("bad timeout.max_keepalive format %s, err %v", c.MaxKeepaliveStr, err)
	}
	if c.ProxyPing, err = time.ParseDuration(c.ProxyPingStr); err != nil {
		return fmt.Errorf("bad timeout.proxy_ping format %s, err %v", c.ProxyPingStr, err)
	}
	if c.CplaneOperation, err = time.ParseDuration(c.CplaneOperationStr); err != nil {
		return fmt.Errorf("bad timeout.vote_request format %s, err %v", c.CplaneOperationStr, err)
	}
	if c.SendFile, err = time.ParseDuration(c.SendFileStr); err != nil {
		return fmt.Errorf("bad timeout.send_file_time format %s, err %v", c.SendFileStr, err)
	}
	if c.Startup, err = time.ParseDuration(c.StartupStr); err != nil {
		return fmt.Errorf("bad timeout.startup_time format %s, err %v", c.StartupStr, err)
	}
	return nil
}

func (c *RebalanceConf) Validate() (err error) {
	if c.DestRetryTime, err = time.ParseDuration(c.DestRetryTimeStr); err != nil {
		return fmt.Errorf("bad rebalance.dest_retry_time format %s, err %v", c.DestRetryTimeStr, err)
	}
	return nil
}

func (c *PeriodConf) Validate() (err error) {
	if c.StatsTime, err = time.ParseDuration(c.StatsTimeStr); err != nil {
		return fmt.Errorf("bad periodic.stats_time format %s, err %v", c.StatsTimeStr, err)
	}
	if c.IostatTime, err = time.ParseDuration(c.IostatTimeStr); err != nil {
		return fmt.Errorf("bad periodic.iostat_time format %s, err %v", c.IostatTimeStr, err)
	}
	if c.RetrySyncTime, err = time.ParseDuration(c.RetrySyncTimeStr); err != nil {
		return fmt.Errorf("bad periodic.retry_sync_time format %s, err %v", c.RetrySyncTimeStr, err)
	}
	return nil
}

func (c *VersionConf) Validate() error {
	return ValidateVersion(c.Versioning)
}

func (c *KeepaliveConf) Validate() (err error) {
	if c.Proxy.Interval, err = time.ParseDuration(c.Proxy.IntervalStr); err != nil {
		return fmt.Errorf("bad keepalivetracker.proxy.interval %s", c.Proxy.IntervalStr)
	}
	if c.Target.Interval, err = time.ParseDuration(c.Target.IntervalStr); err != nil {
		return fmt.Errorf("bad keepalivetracker.target.interval %s", c.Target.IntervalStr)
	}
	if !validKeepaliveType(c.Proxy.Name) {
		return fmt.Errorf("bad keepalivetracker.proxy.name %s", c.Proxy.Name)
	}
	if !validKeepaliveType(c.Target.Name) {
		return fmt.Errorf("bad keepalivetracker.target.name %s", c.Target.Name)
	}
	return nil
}

func (c *NetConf) Validate() (err error) {
	if !StringInSlice(c.L4.Proto, supportedL4Protos) {
		return fmt.Errorf("l4 proto is not recognized %s, expected one of: %s", c.L4.Proto, supportedL4Protos)
	}

	if !StringInSlice(c.HTTP.Proto, supportedL7Protos) {
		return fmt.Errorf("http proto is not recognized %s, expected one of: %s", c.HTTP.Proto, supportedL7Protos)
	}

	// Parse ports
	if c.L4.Port, err = ParsePort(c.L4.PortStr); err != nil {
		return fmt.Errorf("bad public port specified: %v", err)
	}
	if c.L4.PortIntraControl != 0 {
		if c.L4.PortIntraControl, err = ParsePort(c.L4.PortIntraControlStr); err != nil {
			return fmt.Errorf("bad intra control port specified: %v", err)
		}
	}
	if c.L4.PortIntraData != 0 {
		if c.L4.PortIntraData, err = ParsePort(c.L4.PortIntraDataStr); err != nil {
			return fmt.Errorf("bad intra data port specified: %v", err)
		}
	}

	c.IPv4 = strings.Replace(c.IPv4, " ", "", -1)
	c.IPv4IntraControl = strings.Replace(c.IPv4IntraControl, " ", "", -1)
	c.IPv4IntraData = strings.Replace(c.IPv4IntraData, " ", "", -1)

	if overlap, addr := ipv4ListsOverlap(c.IPv4, c.IPv4IntraControl); overlap {
		return fmt.Errorf(
			"public and internal addresses overlap: %s (public: %s; internal: %s)",
			addr, c.IPv4, c.IPv4IntraControl,
		)
	}
	if overlap, addr := ipv4ListsOverlap(c.IPv4, c.IPv4IntraData); overlap {
		return fmt.Errorf(
			"public and replication addresses overlap: %s (public: %s; replication: %s)",
			addr, c.IPv4, c.IPv4IntraData,
		)
	}
	if overlap, addr := ipv4ListsOverlap(c.IPv4IntraControl, c.IPv4IntraData); overlap {
		return fmt.Errorf(
			"internal and replication addresses overlap: %s (internal: %s; replication: %s)",
			addr, c.IPv4IntraControl, c.IPv4IntraData,
		)
	}
	if c.HTTP.RevProxy != "" {
		if c.HTTP.RevProxy != RevProxyCloud && c.HTTP.RevProxy != RevProxyTarget {
			return fmt.Errorf("invalid http rproxy configuration: %s (expecting: ''|%s|%s)",
				c.HTTP.RevProxy, RevProxyCloud, RevProxyTarget)
		}
	}
	return nil
}

func (c *DownloaderConf) Validate() (err error) {
	if c.Timeout, err = time.ParseDuration(c.TimeoutStr); err != nil {
		return fmt.Errorf("bad downloader.timeout %s", c.TimeoutStr)
	}
	return nil
}

func (conf *Config) update(key, value string) error {
	// updateValue sets `to` value (required to be pointer) and runs number of
	// provided validators which would check if the value which was just set is
	// correct.
	updateValue := func(to interface{}, validators ...Validator) error {
		// `to` parameter needs to pointer so we can set it
		Assert(reflect.ValueOf(to).Kind() == reflect.Ptr)

		tmpValue := value
		switch to.(type) {
		case *string:
			// Strings must be quoted so that Unmarshal treat it well
			tmpValue = strconv.Quote(tmpValue)
		default:
			break
		}

		// Unmarshal not only tries to parse `tmpValue` but since `to` is pointer,
		// it will set value of it.
		if err := json.Unmarshal([]byte(tmpValue), to); err != nil {
			return fmt.Errorf("failed to parse %q, %s err: %v", key, value, err)
		}

		for _, v := range validators {
			if err := v.Validate(); err != nil {
				return fmt.Errorf("failed to set %q, err: %v", key, err)
			}
		}
		return nil
	}

	switch key {
	// TOP LEVEL CONFIG
	case "vmodule":
		if err := SetGLogVModule(value); err != nil {
			return fmt.Errorf("failed to set vmodule = %s, err: %v", value, err)
		}
	case "log_level", "log.level":
		if err := SetLogLevel(conf, value); err != nil {
			return fmt.Errorf("failed to set log level = %s, err: %v", value, err)
		}

	// PERIODIC
	case "stats_time", "periodic.stats_time":
		return updateValue(&conf.Periodic.StatsTimeStr, &conf.Periodic)
	case "iostat_time", "periodic.iostat_time":
		return updateValue(&conf.Periodic.IostatTimeStr, &conf.Periodic)

	// LRU
	case "lru_enabled", "lru.enabled":
		return updateValue(&conf.LRU.Enabled, &conf.LRU)
	case "lowwm", "lru.lowwm":
		return updateValue(&conf.LRU.LowWM, &conf.LRU)
	case "highwm", "lru.highwm":
		return updateValue(&conf.LRU.HighWM, &conf.LRU)
	case "dont_evict_time", "lru.dont_evict_time":
		return updateValue(&conf.LRU.DontEvictTimeStr, &conf.LRU)
	case "capacity_upd_time", "lru.capacity_upd_time":
		return updateValue(&conf.LRU.CapacityUpdTimeStr, &conf.LRU)
	case "lru_local_buckets", "lru.local_buckets":
		return updateValue(&conf.LRU.LocalBuckets, &conf.LRU)

	// XACTION
	case "disk_util_low_wm", "xaction.disk_util_low_wm":
		return updateValue(&conf.Xaction.DiskUtilLowWM, &conf.Xaction)
	case "disk_util_high_wm", "xaction.disk_util_high_wm":
		return updateValue(&conf.Xaction.DiskUtilHighWM, &conf.Xaction)

	// REBALANCE
	case "dest_retry_time", "rebalance.dest_retry_time":
		return updateValue(&conf.Rebalance.DestRetryTimeStr, &conf.Rebalance)
	case "rebalance_enabled", "rebalance.enabled":
		return updateValue(&conf.Rebalance.Enabled)

	// TIMEOUT
	case "send_file_time", "timeout.send_file_time":
		return updateValue(&conf.Timeout.SendFileStr, &conf.Timeout)
	case "default_timeout", "timeout.default_timeout":
		return updateValue(&conf.Timeout.DefaultStr, &conf.Timeout)
	case "default_long_timeout", "timeout.default_long_timeout":
		return updateValue(&conf.Timeout.DefaultLongStr, &conf.Timeout)
	case "proxy_ping", "timeout.proxy_ping":
		return updateValue(&conf.Timeout.ProxyPingStr, &conf.Timeout)
	case "cplane_operation", "timeout.cplane_operation":
		return updateValue(&conf.Timeout.CplaneOperationStr, &conf.Timeout)
	case "max_keepalive", "timeout.max_keepalive":
		return updateValue(&conf.Timeout.MaxKeepaliveStr, &conf.Timeout)

	// CHECKSUM
	case "checksum", "cksum.type":
		return updateValue(&conf.Cksum.Type, &conf.Cksum)
	case "validate_checksum_cold_get", "cksum.validate_cold_get":
		return updateValue(&conf.Cksum.ValidateColdGet, &conf.Cksum)
	case "validate_checksum_warm_get", "cksum.validate_warm_get":
		return updateValue(&conf.Cksum.ValidateWarmGet, &conf.Cksum)
	case "enable_read_range_checksum", "cksum.enable_read_range":
		return updateValue(&conf.Cksum.EnableReadRange, &conf.Cksum)

	// VERSION
	case "versioning", "version.versioning":
		return updateValue(&conf.Ver.Versioning, &conf.Ver)
	case "validate_version_warm_get", "version.validate_warm_get":
		return updateValue(&conf.Ver.ValidateWarmGet)

	// FSHC
	case "fshc_enabled", "fshc.enabled":
		return updateValue(&conf.FSHC.Enabled)

	// MIRROR
	case "mirror_enabled", "mirror.enabled":
		return updateValue(&conf.Mirror.Enabled, &conf.Mirror)
	case "mirror_burst_buffer", "mirror.burst_buffer":
		return updateValue(&conf.Mirror.Burst, &conf.Mirror)
	case "mirror_util_thresh", "mirror.util_thresh":
		return updateValue(&conf.Mirror.UtilThresh, &conf.Mirror)

	// KEEPALIVE
	case "keepalivetracker.proxy.interval":
		return updateValue(&conf.KeepaliveTracker.Proxy.IntervalStr, &conf.KeepaliveTracker)
	case "keepalivetracker.proxy.factor":
		return updateValue(&conf.KeepaliveTracker.Proxy.Factor, &conf.KeepaliveTracker)
	case "keepalivetracker.target.interval":
		return updateValue(&conf.KeepaliveTracker.Target.IntervalStr, &conf.KeepaliveTracker)
	case "keepalivetracker.target.factor":
		return updateValue(&conf.KeepaliveTracker.Target.Factor, &conf.KeepaliveTracker)

	default:
		return fmt.Errorf("cannot set config key: %q - is readonly or unsupported", key)
	}
	return nil
}

func SetConfigMany(nvmap SimpleKVs) (err error) {
	if len(nvmap) == 0 {
		return errors.New("setConfig: empty nvmap")
	}

	conf := GCO.BeginUpdate()

	var (
		persist bool
	)
	for name, value := range nvmap {
		if name == ActPersist {
			if persist, err = strconv.ParseBool(value); err != nil {
				err = fmt.Errorf("invalid value set for %s, err: %v", name, err)
				GCO.DiscardUpdate()
				return
			}
		} else if err = conf.update(name, value); err != nil {
			GCO.DiscardUpdate()
			return
		}

		glog.Infof("%s: %s=%s", ActSetConfig, name, value)
	}
	GCO.CommitUpdate(conf)

	if persist {
		conf := GCO.Get()
		if err := LocalSave(GCO.GetConfigFile(), conf); err != nil {
			glog.Errorf("%s: failed to write, err: %v", ActSetConfig, err)
		} else {
			glog.Infof("%s: stored", ActSetConfig)
		}
	}
	return
}
