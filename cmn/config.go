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
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	jsoniter "github.com/json-iterator/go"
)

// $CONFDIR/*
const (
	SmapBackupFile      = "smap.json"
	BucketmdBackupFile  = "bucket-metadata" // base name of the config file; not to confuse with config.Localbuckets mpath
	MountpathBackupFile = "mpaths"          // base name to persist fs.Mountpaths
	GlobalRebMarker     = ".global_rebalancing"
	LocalRebMarker      = ".resilvering"
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
	IgnoreReaction = "ignore"
	WarnReaction   = "warn"
	AbortReaction  = "abort"
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
		BckIsAIS  bool
		TargetCnt int // for EC
	}

	Validator interface {
		Validate(c *Config) error
	}
	PropsValidator interface {
		ValidateAsProps(args *ValidationArgs) error
	}
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
		ConfFile       string        // config filename
		LogLevel       string        // takes precedence over config.Log.Level
		StatsTime      time.Duration // overrides config.Periodic.StatsTime
		ListBucketTime time.Duration // overrides config.Timeout.ListBucket
		ProxyURL       string        // primary proxy URL to override config.Proxy.PrimaryURL
	}
)

// globalConfigOwner implements ConfigOwner interface. The implementation is
// protecting config only from concurrent updates but does not use CoW or other
// techniques which involves cloning and updating config. This might change when
// we will have use case for that - then Get and Put would need to be changed
// accordingly.
type globalConfigOwner struct {
	mtx       sync.Mutex // mutex for protecting updates of config
	c         atomic.Pointer
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
	GCO.c.Store(unsafe.Pointer(config))
	loadDebugMap()
}

func (gco *globalConfigOwner) Get() *Config {
	return (*Config)(gco.c.Load())
}

func (gco *globalConfigOwner) Clone() *Config {
	config := &Config{}

	// FIXME: CopyStruct is actually shallow copy but because Config
	// has only values (no pointers or slices, except FSPaths) it is
	// deep copy. This may break in the future, so we need solution
	// to make sure that we do *proper* deep copy with good performance.
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
	GCO.c.Store(unsafe.Pointer(config))

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
var (
	SupportedReactions = []string{IgnoreReaction, WarnReaction, AbortReaction}
	supportedL4Protos  = []string{tcpProto}
	supportedL7Protos  = []string{httpProto, httpsProto}

	// NOTE: new validators must be run via Config.Validate() - see below

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
	_ Validator = &DSortConf{}
	_ Validator = &FSPathsConf{}
	_ Validator = &TestfspathConf{}
	_ Validator = &CompressionConf{}

	_ PropsValidator = &CksumConf{}
	_ PropsValidator = &LRUConf{}
	_ PropsValidator = &MirrorConf{}
	_ PropsValidator = &ECConf{}

	_ json.Marshaler   = &FSPathsConf{}
	_ json.Unmarshaler = &FSPathsConf{}

	// Debugging
	pkgDebug = make(map[string]glog.Level)
)

type Config struct {
	Confdir          string          `json:"confdir"`
	CloudProvider    string          `json:"cloudprovider"`
	Mirror           MirrorConf      `json:"mirror"`
	EC               ECConf          `json:"ec"`
	Log              LogConf         `json:"log"`
	Periodic         PeriodConf      `json:"periodic"`
	Timeout          TimeoutConf     `json:"timeout"`
	Proxy            ProxyConf       `json:"proxy"`
	LRU              LRUConf         `json:"lru"`
	Disk             DiskConf        `json:"disk"`
	Rebalance        RebalanceConf   `json:"rebalance"`
	Replication      ReplicationConf `json:"replication"`
	Cksum            CksumConf       `json:"cksum"`
	Ver              VersionConf     `json:"version"`
	FSpaths          FSPathsConf     `json:"fspaths"`
	TestFSP          TestfspathConf  `json:"test_fspaths"`
	Net              NetConf         `json:"net"`
	FSHC             FSHCConf        `json:"fshc"`
	Auth             AuthConf        `json:"auth"`
	KeepaliveTracker KeepaliveConf   `json:"keepalivetracker"`
	Downloader       DownloaderConf  `json:"downloader"`
	DSort            DSortConf       `json:"distributed_sort"`
	Compression      CompressionConf `json:"compression"`
	Readahead        RahConf         `json:"readahead"`
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
	ListBucketStr      string        `json:"list_timeout"`
	ListBucket         time.Duration `json:"-"` //
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

	// DontEvictTimeStr denotes the period of time during which eviction of an object
	// is forbidden [atime, atime + DontEvictTime]
	DontEvictTimeStr string `json:"dont_evict_time"`

	// DontEvictTime is the parsed value of DontEvictTimeStr
	DontEvictTime time.Duration `json:"-"`

	// CapacityUpdTimeStr denotes the frequency at which AIStore updates local capacity utilization
	CapacityUpdTimeStr string `json:"capacity_upd_time"`

	// CapacityUpdTime is the parsed value of CapacityUpdTimeStr
	CapacityUpdTime time.Duration `json:"-"`

	// Buckets: Enables or disables LRU for ais buckets
	// -- TODO -- FIXME: combine these two booleans into one enum { evict-all, evict-cloud, evict-none }
	EvictAISBuckets bool `json:"ais_buckets"`

	// Enabled: LRU will only run when set to true
	Enabled bool `json:"enabled"`
}

type DiskConf struct {
	DiskUtilLowWM   int64         `json:"disk_util_low_wm"`  // Low watermark below which no throttling is required
	DiskUtilHighWM  int64         `json:"disk_util_high_wm"` // High watermark above which throttling is required for longer duration
	DiskUtilMaxWM   int64         `json:"disk_util_max_wm"`  // Max beyond which workload must be throttled
	IostatTimeLong  time.Duration `json:"-"`
	IostatTimeShort time.Duration `json:"-"`

	IostatTimeLongStr  string `json:"iostat_time_long"`
	IostatTimeShortStr string `json:"iostat_time_short"`
}

type RebalanceConf struct {
	DestRetryTimeStr string        `json:"dest_retry_time"` // max time to wait for ACKs and for neighbors to complete
	Compression      string        `json:"compression"`     // see CompressAlways, etc. enum
	DestRetryTime    time.Duration `json:"-"`               // (runtime)
	Multiplier       uint8         `json:"multiplier"`      // stream-bundle-and-jogger multiplier
	Enabled          bool          `json:"enabled"`         // true: auto-rebalance, false: manual rebalancing
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

	// ValidateObjMove determines if migrated objects should have their checksum validated
	ValidateObjMove bool `json:"validate_obj_move"`

	// EnableReadRange: Return read range checksum otherwise return entire object checksum
	EnableReadRange bool `json:"enable_read_range"`
}

type VersionConf struct {
	Type            string `json:"type"`              // inherited/owned
	Enabled         bool   `json:"enabled"`           // defined by the Versioning; can be redefined on a bucket level
	ValidateWarmGet bool   `json:"validate_warm_get"` // validate object version upon warm GET
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
	L4               L4Conf   `json:"l4"`
	HTTP             HTTPConf `json:"http"`
	UseIntraControl  bool     `json:"-"`
	UseIntraData     bool     `json:"-"`
}

type L4Conf struct {
	Proto               string `json:"proto"`              // tcp, udp
	PortStr             string `json:"port"`               // listening port
	PortIntraControlStr string `json:"port_intra_control"` // listening port for intra control network
	PortIntraDataStr    string `json:"port_intra_data"`    // listening port for intra data network
	Port                int    `json:"-"`                  // (runtime)
	PortIntraControl    int    `json:"-"`                  // --/--
	PortIntraData       int    `json:"-"`                  // --/--
	SndRcvBufSize       int    `json:"sndrcv_buf_size"`    // SO_RCVBUF and SO_SNDBUF
}

type HTTPConf struct {
	Proto         string `json:"proto"`              // http or https
	RevProxy      string `json:"rproxy"`             // RevProxy* enum
	Certificate   string `json:"server_certificate"` // HTTPS: openssl certificate
	Key           string `json:"server_key"`         // HTTPS: openssl key
	RevProxyCache bool   `json:"rproxy_cache"`       // RevProxy caches or work as transparent proxy
	UseHTTPS      bool   `json:"use_https"`          // use HTTPS instead of HTTP
	Chunked       bool   `json:"chunked_transfer"`   // https://tools.ietf.org/html/rfc7230#page-36
}

type FSHCConf struct {
	TestFileCount int  `json:"test_files"`  // the number of files to read and write during a test
	ErrorLimit    int  `json:"error_limit"` // max number of errors (exceeding any results in disabling mpath)
	Enabled       bool `json:"enabled"`
}

type AuthConf struct {
	Secret  string `json:"secret"`
	CredDir string `json:"creddir"`
	Enabled bool   `json:"enabled"`
}

// config for one keepalive tracker
// all type of trackers share the same struct, not all fields are used by all trackers
type KeepaliveTrackerConf struct {
	IntervalStr string        `json:"interval"` // keepalives are sent(target)/checked(promary proxy) every interval
	Interval    time.Duration `json:"-"`        // (runtime)
	Name        string        `json:"name"`     // "heartbeat", "average"
	Factor      uint8         `json:"factor"`   // only average
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

type DSortConf struct {
	DuplicatedRecords   string        `json:"duplicated_records"`
	MissingShards       string        `json:"missing_shards"`
	EKMMalformedLine    string        `json:"ekm_malformed_line"`
	EKMMissingKey       string        `json:"ekm_missing_key"`
	DefaultMaxMemUsage  string        `json:"default_max_mem_usage"`
	CallTimeoutStr      string        `json:"call_timeout"`
	Compression         string        `json:"compression"` // see CompressAlways, etc. enum
	DSorterMemThreshold string        `json:"dsorter_mem_threshold"`
	CallTimeout         time.Duration `json:"-"` // determines how long target should wait for other target to respond
}

type FSPathsConf struct {
	Paths map[string]struct{} `json:"paths,omitempty"`
}

// lz4 block and frame formats: http://fastcompression.blogspot.com/2013/04/lz4-streaming-format-final.html
type CompressionConf struct {
	BlockMaxSize int  `json:"block_size"` // *uncompressed* block max size
	Checksum     bool `json:"checksum"`   // true: checksum lz4 frames
}

//==============================
//
// config functions
//
//==============================

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

func LoadConfig(clivars *ConfigCLI) (config *Config, changed bool) {
	config, changed, err := LoadConfigErr(clivars)

	if err != nil {
		ExitLogf(err.Error())
	}
	return
}

func LoadConfigErr(clivars *ConfigCLI) (config *Config, changed bool, err error) {
	GCO.SetConfigFile(clivars.ConfFile)

	config = GCO.BeginUpdate()
	defer GCO.CommitUpdate(config)

	err = LocalLoad(clivars.ConfFile, &config)

	// NOTE: glog.Errorf + os.Exit is used instead of glog.Fatalf to not crash
	// with dozens of backtraces on screen - this is user error not some
	// internal error.

	if err != nil {
		return nil, false, fmt.Errorf("Failed to load config %q, err: %s", clivars.ConfFile, err)
	}
	if err = flag.Lookup("log_dir").Value.Set(config.Log.Dir); err != nil {
		return nil, false, fmt.Errorf("Failed to flag-set glog dir %q, err: %s", config.Log.Dir, err)
	}
	if err = CreateDir(config.Log.Dir); err != nil {
		return nil, false, fmt.Errorf("Failed to create log dir %q, err: %s", config.Log.Dir, err)
	}
	if err := config.Validate(); err != nil {
		return nil, false, err
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
	if clivars.ListBucketTime != 0 {
		config.Timeout.ListBucket = clivars.ListBucketTime
		changed = true
	}
	if clivars.ProxyURL != "" {
		config.Proxy.PrimaryURL = clivars.ProxyURL
		changed = true
	}
	if clivars.LogLevel != "" {
		if err = SetLogLevel(config, clivars.LogLevel); err != nil {
			return nil, false, fmt.Errorf("Failed to set log level = %s, err: %s", clivars.LogLevel, err)
		}
		config.Log.Level = clivars.LogLevel
		changed = true
	} else if err = SetLogLevel(config, config.Log.Level); err != nil {
		return nil, false, fmt.Errorf("Failed to set log level = %s, err: %s", config.Log.Level, err)
	}
	glog.Infof("Logdir: %q Proto: %s Port: %d Verbosity: %s",
		config.Log.Dir, config.Net.L4.Proto, config.Net.L4.Port, config.Log.Level)
	glog.Infof("Config: %q StatsTime: %v", clivars.ConfFile, config.Periodic.StatsTime)
	return
}

// TestingEnv returns true if config is set to a development environment
// where a single local filesystem is partitioned between all (locally running)
// targets and is used for both local and Cloud buckets
func (c *Config) TestingEnv() bool {
	return c.TestFSP.Count > 0
}

func (c *Config) Validate() error {
	validators := []Validator{
		&c.Disk, &c.LRU, &c.Mirror, &c.Cksum,
		&c.Timeout, &c.Periodic, &c.Rebalance, &c.KeepaliveTracker, &c.Net, &c.Ver,
		&c.Downloader, &c.DSort, &c.TestFSP, &c.FSpaths, &c.Compression,
	}
	for _, validator := range validators {
		if err := validator.Validate(c); err != nil {
			return err
		}
	}
	return nil
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

func ipv4ListsEqual(alist, blist string) bool {
	alistAddrs := strings.Split(alist, ",")
	blistAddrs := strings.Split(blist, ",")
	f := func(in []string) (out []string) {
		out = make([]string, 0, len(in))
		for _, i := range in {
			i = strings.TrimSpace(i)
			if i == "" {
				continue
			}
			out = append(out, i)
		}
		return
	}
	al := f(alistAddrs)
	bl := f(blistAddrs)
	if len(al) == 0 || len(bl) == 0 || len(al) != len(bl) {
		return false
	}
	return StrSlicesEqual(al, bl)
}

// validKeepaliveType returns true if the keepalive type is supported.
func validKeepaliveType(t string) bool {
	return t == KeepaliveHeartbeatType || t == KeepaliveAverageType
}

func (c *DiskConf) Validate(_ *Config) (err error) {
	lwm, hwm, maxwm := c.DiskUtilLowWM, c.DiskUtilHighWM, c.DiskUtilMaxWM
	if lwm <= 0 || hwm <= lwm || maxwm <= hwm || maxwm > 100 {
		return fmt.Errorf("invalid disk (disk_util_lwm, disk_util_hwm, disk_util_maxwm) configuration %+v", c)
	}

	if c.IostatTimeLong, err = time.ParseDuration(c.IostatTimeLongStr); err != nil {
		return fmt.Errorf("bad disk.iostat_time_long format %s, err %v", c.IostatTimeLongStr, err)
	}
	if c.IostatTimeShort, err = time.ParseDuration(c.IostatTimeShortStr); err != nil {
		return fmt.Errorf("bad disk.iostat_time_short format %s, err %v", c.IostatTimeShortStr, err)
	}
	if c.IostatTimeLong <= 0 {
		return fmt.Errorf("disk.iostat_time_long is zero")
	}
	if c.IostatTimeShort <= 0 {
		return fmt.Errorf("disk.iostat_time_short is zero")
	}
	if c.IostatTimeLong < c.IostatTimeShort {
		return fmt.Errorf("disk.iostat_time_long %v shorter than disk.iostat_time_short %v", c.IostatTimeLong, c.IostatTimeShort)
	}

	return nil
}

func (c *LRUConf) Validate(_ *Config) (err error) {
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
	return c.Validate(nil)
}

func (c *CksumConf) Validate(_ *Config) error {
	if c.Type != ChecksumXXHash && c.Type != ChecksumNone {
		return fmt.Errorf("invalid checksum.type: %s (expected one of [%s, %s])", c.Type, ChecksumXXHash, ChecksumNone)
	}
	return nil
}

func (c *CksumConf) ValidateAsProps(args *ValidationArgs) error {
	if c.Type != PropInherit && c.Type != ChecksumNone && c.Type != ChecksumXXHash {
		return fmt.Errorf("invalid checksum.type: %s (expected one of: [%s, %s, %s])",
			c.Type, ChecksumXXHash, ChecksumNone, PropInherit)
	}
	return nil
}

func (c *VersionConf) Validate(_ *Config) error {
	if c.ValidateWarmGet && !c.Enabled {
		return errors.New("validate-warm-get requires versioning to be enabled")
	}
	return nil
}
func (c *VersionConf) ValidateAsProps() error { return c.Validate(nil) }

func (c *MirrorConf) Validate(_ *Config) error {
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
	return c.Validate(nil)
}

func (c *ECConf) Validate(_ *Config) error {
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
	if err := c.Validate(nil); err != nil {
		return err
	}
	if required := c.RequiredEncodeTargets(); args.TargetCnt < required {
		return fmt.Errorf(
			"erasure coding requires %d targets to use %d data and %d parity slices "+
				"(the cluster has only %d targets)",
			required, c.DataSlices, c.ParitySlices, args.TargetCnt)

	}
	return nil
}

func (c *TimeoutConf) Validate(_ *Config) (err error) {
	if c.Default, err = time.ParseDuration(c.DefaultStr); err != nil {
		return fmt.Errorf("bad timeout.default format %s, err %v", c.DefaultStr, err)
	}
	if c.DefaultLong, err = time.ParseDuration(c.DefaultLongStr); err != nil {
		return fmt.Errorf("bad timeout.default_long format %s, err %v", c.DefaultLongStr, err)
	}
	if c.ListBucket, err = time.ParseDuration(c.ListBucketStr); err != nil {
		return fmt.Errorf("bad timeout.list_timeout format %s, err %v", c.ListBucketStr, err)
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

func (c *RebalanceConf) Validate(_ *Config) (err error) {
	if c.DestRetryTime, err = time.ParseDuration(c.DestRetryTimeStr); err != nil {
		return fmt.Errorf("bad rebalance.dest_retry_time format %s, err %v", c.DestRetryTimeStr, err)
	}
	return nil
}

func (c *PeriodConf) Validate(_ *Config) (err error) {
	if c.StatsTime, err = time.ParseDuration(c.StatsTimeStr); err != nil {
		return fmt.Errorf("bad periodic.stats_time format %s, err %v", c.StatsTimeStr, err)
	}
	if c.RetrySyncTime, err = time.ParseDuration(c.RetrySyncTimeStr); err != nil {
		return fmt.Errorf("bad periodic.retry_sync_time format %s, err %v", c.RetrySyncTimeStr, err)
	}
	return nil
}

func (c *KeepaliveConf) Validate(_ *Config) (err error) {
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

func (c *NetConf) Validate(_ *Config) (err error) {
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

	c.IPv4 = strings.ReplaceAll(c.IPv4, " ", "")
	c.IPv4IntraControl = strings.ReplaceAll(c.IPv4IntraControl, " ", "")
	c.IPv4IntraData = strings.ReplaceAll(c.IPv4IntraData, " ", "")

	if overlap, addr := ipv4ListsOverlap(c.IPv4, c.IPv4IntraControl); overlap {
		return fmt.Errorf("public (%s) and intra-cluster control (%s) IPv4 lists overlap: %s", c.IPv4, c.IPv4IntraControl, addr)
	}
	if overlap, addr := ipv4ListsOverlap(c.IPv4, c.IPv4IntraData); overlap {
		return fmt.Errorf("public (%s) and intra-cluster data (%s) IPv4 lists overlap: %s", c.IPv4, c.IPv4IntraData, addr)
	}
	if overlap, addr := ipv4ListsOverlap(c.IPv4IntraControl, c.IPv4IntraData); overlap {
		if ipv4ListsEqual(c.IPv4IntraControl, c.IPv4IntraData) {
			glog.Warningf("control and data share one intra-cluster network (%s)", c.IPv4IntraData)
		} else {
			glog.Warningf("intra-cluster control (%s) and data (%s) IPv4 lists overlap: %s",
				c.IPv4IntraControl, c.IPv4IntraData, addr)
		}
	}
	if c.HTTP.RevProxy != "" {
		if c.HTTP.RevProxy != RevProxyCloud && c.HTTP.RevProxy != RevProxyTarget {
			return fmt.Errorf("invalid http rproxy configuration: %s (expecting: ''|%s|%s)",
				c.HTTP.RevProxy, RevProxyCloud, RevProxyTarget)
		}
	}
	if !c.HTTP.Chunked {
		glog.Warningln("disabled chunked transfer may cause a slow down (see also: Content-Length)")
	}
	return nil
}

func (c *DownloaderConf) Validate(_ *Config) (err error) {
	if c.Timeout, err = time.ParseDuration(c.TimeoutStr); err != nil {
		return fmt.Errorf("bad downloader.timeout %s", c.TimeoutStr)
	}
	return nil
}

func (c *DSortConf) Validate(_ *Config) (err error) {
	if !StringInSlice(c.DuplicatedRecords, SupportedReactions) {
		return fmt.Errorf("bad distributed_sort.duplicated_records: %s (expecting one of: %s)",
			c.DuplicatedRecords, SupportedReactions)
	}
	if !StringInSlice(c.MissingShards, SupportedReactions) {
		return fmt.Errorf("bad distributed_sort.missing_records: %s (expecting one of: %s)", c.MissingShards, SupportedReactions)
	}
	if !StringInSlice(c.EKMMalformedLine, SupportedReactions) {
		return fmt.Errorf("bad distributed_sort.ekm_malformed_line: %s (expecting one of: %s)", c.EKMMalformedLine, SupportedReactions)
	}
	if !StringInSlice(c.EKMMissingKey, SupportedReactions) {
		return fmt.Errorf("bad distributed_sort.ekm_missing_key: %s (expecting one of: %s)", c.EKMMissingKey, SupportedReactions)
	}
	if _, err := ParseQuantity(c.DefaultMaxMemUsage); err != nil {
		return fmt.Errorf("bad distributed_sort.default_max_mem_usage: %s (err: %s)", c.DefaultMaxMemUsage, err)
	}
	if c.CallTimeout, err = time.ParseDuration(c.CallTimeoutStr); err != nil {
		return fmt.Errorf("bad distributed_sort.call_timeout: %s", c.CallTimeoutStr)
	}
	if _, err := S2B(c.DSorterMemThreshold); err != nil {
		return fmt.Errorf("bad distributed_sort.dsorter_mem_threshold: %s (err: %s)", c.DSorterMemThreshold, err)
	}
	return nil
}

// FIXME: change config to accept array of mpaths, not map of mpath -> " "
func (c *FSPathsConf) UnmarshalJSON(data []byte) error {
	m := make(map[string]string)
	err := jsoniter.Unmarshal(data, &m)

	if err != nil {
		return err
	}

	c.Paths = make(map[string]struct{})
	for k := range m {
		c.Paths[k] = struct{}{}
	}

	return nil
}

func (c *FSPathsConf) MarshalJSON() (data []byte, err error) {
	m := make(map[string]string)

	for k := range c.Paths {
		m[k] = " "
	}

	return MustMarshal(m), nil
}

func (c *FSPathsConf) Validate(contextConfig *Config) (err error) {
	// Don't validate if testing environment
	if contextConfig.TestingEnv() {
		return nil
	}

	if len(c.Paths) == 0 {
		return fmt.Errorf("expected at least one mountpath in fspaths config")
	}

	cleanMpaths := make(map[string]struct{})

	for k := range c.Paths {
		cleanMpath, err := ValidateMpath(k)
		if err != nil {
			return err
		}
		cleanMpaths[cleanMpath] = struct{}{}
	}

	c.Paths = cleanMpaths
	return nil
}

func (c *TestfspathConf) Validate(contextConfig *Config) (err error) {
	// Don't validate rest when count is not > 0
	if !contextConfig.TestingEnv() {
		return nil
	}

	cleanMpath, err := ValidateMpath(c.Root)
	if err != nil {
		return err
	}
	c.Root = cleanMpath

	return nil
}

func ValidateMpath(mpath string) (string, error) {
	cleanMpath := filepath.Clean(mpath)

	if cleanMpath[0] != filepath.Separator {
		return mpath, NewInvalidaMountpathError(mpath, "mountpath must be an absolute path")
	}
	if cleanMpath == string(filepath.Separator) {
		return "", NewInvalidaMountpathError(mpath, "root directory is not a valid mountpath")
	}
	return cleanMpath, nil
}

// NOTE: uncompressed block sizes - the enum currently supported by the github.com/pierrec/lz4
func (c *CompressionConf) Validate(_ *Config) (err error) {
	if c.BlockMaxSize != 64*KiB && c.BlockMaxSize != 256*KiB && c.BlockMaxSize != MiB && c.BlockMaxSize != 4*MiB {
		return fmt.Errorf("invalid compression.block_size %d", c.BlockMaxSize)
	}
	return nil
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

func (conf *Config) update(key, value string) (Validator, error) {
	// updateValue sets `to` value (required to be pointer) and runs number of
	// provided validators which would check if the value which was just set is
	// correct.
	updateValue := func(to interface{}) error {
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

		return nil
	}

	switch key {
	//
	// 1. TOP LEVEL CONFIG
	//
	case "vmodule":
		if err := SetGLogVModule(value); err != nil {
			return nil, fmt.Errorf("failed to set vmodule = %s, err: %v", value, err)
		}
	case "log_level", "log.level":
		if err := SetLogLevel(conf, value); err != nil {
			return nil, fmt.Errorf("failed to set log level = %s, err: %v", value, err)
		}

	//
	// 2. CONFIG SECTIONS THAT CAN BE OVERRIDDEN via cmn.BucketProps
	//    (that in turn include VersionConf, TierConf, CksumConf, LRUConf, MirrorConf, ECConf)
	//

	// LRU
	case "lru_enabled", HeaderBucketLRUEnabled:
		return &conf.LRU, updateValue(&conf.LRU.Enabled)
	case "lowwm", HeaderBucketLRULowWM:
		return &conf.LRU, updateValue(&conf.LRU.LowWM)
	case "highwm", HeaderBucketLRUHighWM:
		return &conf.LRU, updateValue(&conf.LRU.HighWM)
	case "dont_evict_time", HeaderBucketDontEvictTime:
		return &conf.LRU, updateValue(&conf.LRU.DontEvictTimeStr)
	case "capacity_upd_time", HeaderBucketCapUpdTime:
		return &conf.LRU, updateValue(&conf.LRU.CapacityUpdTimeStr)
	case "lru_ais_buckets", "lru.ais_buckets":
		return &conf.LRU, updateValue(&conf.LRU.EvictAISBuckets)

	// CHECKSUM
	case "checksum", HeaderBucketChecksumType:
		return &conf.Cksum, updateValue(&conf.Cksum.Type)
	case "validate_checksum_cold_get", HeaderBucketValidateColdGet:
		return &conf.Cksum, updateValue(&conf.Cksum.ValidateColdGet)
	case "validate_checksum_warm_get", HeaderBucketValidateWarmGet:
		return &conf.Cksum, updateValue(&conf.Cksum.ValidateWarmGet)
	case "validate_obj_move", HeaderBucketValidateObjMove:
		return &conf.Cksum, updateValue(&conf.Cksum.ValidateObjMove)
	case "enable_read_range_checksum", HeaderBucketEnableReadRange:
		return &conf.Cksum, updateValue(&conf.Cksum.EnableReadRange)

	// VERSION
	case "versioning_enabled", "ver_enabled", HeaderBucketVerEnabled:
		return nil, updateValue(&conf.Ver.Enabled)
	case "versioning_validate_warm_get", "ver_validate_warm_get", HeaderBucketVerValidateWarm:
		return nil, updateValue(&conf.Ver.ValidateWarmGet)

	// MIRROR
	case "mirror_enabled", HeaderBucketMirrorEnabled:
		return &conf.Mirror, updateValue(&conf.Mirror.Enabled)
	case "mirror_burst_buffer", "mirror.burst_buffer":
		return &conf.Mirror, updateValue(&conf.Mirror.Burst)
	case "mirror_util_thresh", HeaderBucketMirrorThresh:
		return &conf.Mirror, updateValue(&conf.Mirror.UtilThresh)
	case "mirror_copies", HeaderBucketCopies:
		return &conf.Mirror, updateValue(&conf.Mirror.Copies)

	// EC
	case "ec_enabled", HeaderBucketECEnabled:
		return &conf.EC, updateValue(&conf.EC.Enabled)
	case "ec_data_slices", HeaderBucketECData:
		return &conf.EC, updateValue(&conf.EC.DataSlices)
	case "ec_parity_slices", HeaderBucketECParity:
		return &conf.EC, updateValue(&conf.EC.ParitySlices)
	case "ec_objsize_limit", HeaderBucketECMinSize:
		return &conf.EC, updateValue(&conf.EC.ObjSizeLimit)
	case "ec_compression", HeaderBucketECCompression:
		return &conf.EC, updateValue(&conf.EC.Compression)

	//
	// 3. CONFIG SECTIONS
	//

	// PERIODIC
	case "stats_time", "periodic.stats_time":
		return &conf.Periodic, updateValue(&conf.Periodic.StatsTimeStr)

	// DISK
	case "disk_util_low_wm", "disk.disk_util_low_wm":
		return &conf.Disk, updateValue(&conf.Disk.DiskUtilLowWM)
	case "disk_util_high_wm", "disk.disk_util_high_wm":
		return &conf.Disk, updateValue(&conf.Disk.DiskUtilHighWM)
	case "disk_util_max_wm", "disk.disk_util_max_wm":
		return &conf.Disk, updateValue(&conf.Disk.DiskUtilMaxWM)
	case "iostat_time_long", "disk.iostat_time_long":
		return &conf.Disk, updateValue(&conf.Disk.IostatTimeLongStr)
	case "iostat_time_short", "disk.iostat_time_short":
		return &conf.Disk, updateValue(&conf.Disk.IostatTimeShortStr)

	// REBALANCE
	case "dest_retry_time", "rebalance.dest_retry_time":
		return &conf.Rebalance, updateValue(&conf.Rebalance.DestRetryTimeStr)
	case "rebalance_multiplier", "rebalance.multiplier":
		return &conf.Rebalance, updateValue(&conf.Rebalance.Multiplier)
	case "rebalance_enabled", "rebalance.enabled":
		return &conf.Rebalance, updateValue(&conf.Rebalance.Enabled)

	// TIMEOUT
	case "default_timeout", "timeout.default_timeout":
		return &conf.Timeout, updateValue(&conf.Timeout.DefaultStr)
	case "default_long_timeout", "timeout.default_long_timeout":
		return &conf.Timeout, updateValue(&conf.Timeout.DefaultLongStr)
	case "max_keepalive", "timeout.max_keepalive":
		return &conf.Timeout, updateValue(&conf.Timeout.MaxKeepaliveStr)
	case "proxy_ping", "timeout.proxy_ping":
		return &conf.Timeout, updateValue(&conf.Timeout.ProxyPingStr)
	case "cplane_operation", "timeout.cplane_operation":
		return &conf.Timeout, updateValue(&conf.Timeout.CplaneOperationStr)
	case "send_file_time", "timeout.send_file_time":
		return &conf.Timeout, updateValue(&conf.Timeout.SendFileStr)
	case "startup_time", "timeout.startup_time":
		return &conf.Timeout, updateValue(&conf.Timeout.StartupStr)
	case "list_timeout", "timeout.list_timeout":
		return &conf.Timeout, updateValue(&conf.Timeout.ListBucketStr)

	// FSHC
	case "fshc_enabled", "fshc.enabled":
		return nil, updateValue(&conf.FSHC.Enabled)

	// KEEPALIVE
	case "keepalivetracker.proxy.interval":
		return &conf.KeepaliveTracker, updateValue(&conf.KeepaliveTracker.Proxy.IntervalStr)
	case "keepalivetracker.proxy.factor":
		return &conf.KeepaliveTracker, updateValue(&conf.KeepaliveTracker.Proxy.Factor)
	case "keepalivetracker.target.interval":
		return &conf.KeepaliveTracker, updateValue(&conf.KeepaliveTracker.Target.IntervalStr)
	case "keepalivetracker.target.factor":
		return &conf.KeepaliveTracker, updateValue(&conf.KeepaliveTracker.Target.Factor)

	// DISTRIBUTED SORT
	case "distributed_sort.duplicated_records":
		return &conf.DSort, updateValue(&conf.DSort.DuplicatedRecords)
	case "distributed_sort.missing_shards":
		return &conf.DSort, updateValue(&conf.DSort.MissingShards)
	case "distributed_sort.ekm_invalid_line":
		return &conf.DSort, updateValue(&conf.DSort.EKMMalformedLine)
	case "distributed_sort.ekm_missing_key":
		return &conf.DSort, updateValue(&conf.DSort.EKMMissingKey)
	case "distributed_sort.default_max_mem_usage":
		return &conf.DSort, updateValue(&conf.DSort.DefaultMaxMemUsage)
	case "distributed_sort.call_timeout":
		return &conf.DSort, updateValue(&conf.DSort.CallTimeoutStr)
	case "distributed_sort.compression":
		return &conf.DSort, updateValue(&conf.DSort.Compression)

	// COMPRESSION
	case "compression.block_size":
		return &conf.Compression, updateValue(&conf.Compression.BlockMaxSize)
	case "compression.checksum":
		return &conf.Compression, updateValue(&conf.Compression.Checksum)

	default:
		return nil, fmt.Errorf("cannot set config key: %q - is readonly or unsupported", key)
	}
	return nil, nil
}

// ConfigPropList is config property name <-> readonly
var ConfigPropList = map[string]bool{
	"vmodule":                                false,
	"log.level":                              false,
	HeaderBucketLRUEnabled:                   false,
	HeaderBucketLRULowWM:                     false,
	HeaderBucketLRUHighWM:                    false,
	HeaderBucketDontEvictTime:                false,
	HeaderBucketCapUpdTime:                   true,
	"lru.ais_buckets":                        false,
	HeaderBucketChecksumType:                 false,
	HeaderBucketValidateColdGet:              false,
	HeaderBucketValidateWarmGet:              false,
	HeaderBucketValidateObjMove:              false,
	HeaderBucketEnableReadRange:              false,
	HeaderBucketVerEnabled:                   false,
	HeaderBucketVerValidateWarm:              false,
	HeaderBucketMirrorEnabled:                false,
	"mirror.burst_buffer":                    true,
	HeaderBucketMirrorThresh:                 false,
	HeaderBucketCopies:                       false,
	"periodic.stats_time":                    false,
	"disk.disk_util_low_wm":                  false,
	"disk.disk_util_high_wm":                 false,
	"disk.disk_util_max_wm":                  false,
	"disk.iostat_time_long":                  false,
	"disk.iostat_time_short":                 false,
	"rebalance.dest_retry_time":              false,
	"rebalance.multiplier":                   false,
	"rebalance.enabled":                      false,
	"timeout.default_timeout":                false,
	"timeout.default_long_timeout":           false,
	"timeout.max_keepalive":                  false,
	"timeout.proxy_ping":                     false,
	"timeout.cplane_operation":               false,
	"timeout.send_file_time":                 false,
	"timeout.startup_time":                   false,
	"timeout.list_timeout":                   false,
	"fshc.enabled":                           false,
	"keepalivetracker.proxy.interval":        false,
	"keepalivetracker.proxy.factor":          false,
	"keepalivetracker.target.interval":       false,
	"keepalivetracker.target.factor":         false,
	"distributed_sort.duplicated_records":    false,
	"distributed_sort.missing_shards":        false,
	"distributed_sort.default_max_mem_usage": false,
	"distributed_sort.call_timeout":          false,
	"compression.block_size":                 false,
	"compression.checksum":                   false,
	"distributed_sort.compression":           false,
	HeaderBucketECEnabled:                    false,
	HeaderBucketECData:                       false,
	HeaderBucketECParity:                     false,
	HeaderBucketECMinSize:                    false,
	HeaderBucketECCompression:                false,
}

func SetConfigMany(nvmap SimpleKVs) (err error) {
	if len(nvmap) == 0 {
		return errors.New("setConfig: empty nvmap")
	}

	conf := GCO.BeginUpdate()

	var (
		persist bool
	)

	validators := make(map[Validator]struct{})

	for name, value := range nvmap {
		if name == ActPersist {
			if persist, err = ParseBool(value); err != nil {
				err = fmt.Errorf("invalid value set for %s, err: %v", name, err)
				GCO.DiscardUpdate()
				return
			}
		} else {
			validator, err := conf.update(name, value)
			if err != nil {
				GCO.DiscardUpdate()
				return err
			}
			if validator != nil {
				validators[validator] = struct{}{}
			}

		}

		glog.Infof("%s: %s=%s", ActSetConfig, name, value)
	}

	// validate after everything is set
	for val := range validators {
		err := val.Validate(conf)
		if err != nil {
			GCO.DiscardUpdate()
			return err
		}
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

// ========== Cluster Wide Config =========

func CheckDebug(pkgName string) (logLvl glog.Level, ok bool) {
	logLvl, ok = pkgDebug[pkgName]
	return
}

// loadDebugMap sets debug verbosity for different packages based on
// environment variables. It is to help enable asserts that were originally
// used for testing/initial development and to set the verbosity of glog
func loadDebugMap() {
	var opts []string
	// Input will be in the format of AIS_DEBUG=transport=4,memsys=3 (same as GODEBUG)
	if val := os.Getenv("AIS_DEBUG"); val != "" {
		opts = strings.Split(val, ",")
	}

	for _, ele := range opts {
		pair := strings.Split(ele, "=")
		if len(pair) != 2 {
			ExitLogf("Failed to get name=val element: %q", ele)
		}
		key := pair[0]
		logLvl, err := strconv.Atoi(pair[1])
		if err != nil {
			ExitLogf("Failed to convert verbosity level = %s, err: %s", pair[1], err)
		}
		pkgDebug[key] = glog.Level(logLvl)
	}
}

func WaitStartup(config *Config, startedUp *atomic.Bool) {
	const sleep = 300 * time.Millisecond
	var i time.Duration
	for !startedUp.Load() {
		time.Sleep(sleep)
		i++
		if i*sleep > config.Timeout.Startup {
			glog.Errorf("waiting unusually long time...")
			i = 0
		}
	}
}
