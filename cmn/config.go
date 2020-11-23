// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	jsoniter "github.com/json-iterator/go"
)

const (
	KeepaliveHeartbeatType = "heartbeat"
	KeepaliveAverageType   = "average"
)

const (
	ThrottleMin = time.Millisecond
	ThrottleAvg = time.Millisecond * 10
	ThrottleMax = time.Millisecond * 100

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

// FeatureFlags
const (
	FeatureDirectAccess = 1 << iota
)

type (
	ValidationArgs struct {
		TargetCnt int // for EC
	}

	Validator interface {
		Validate(c *Config) error
	}
	PropsValidator interface {
		ValidateAsProps(args *ValidationArgs) error
	}
	FeatureFlags uint64
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

		Reg(key string, cl ConfigListener)
		Unreg(key string)

		SetConfigFile(path string)
		GetConfigFile() string
	}
	// globalConfigOwner implements ConfigOwner interface. The implementation is
	// protecting config only from concurrent updates but does not use CoW or other
	// techniques which involves cloning and updating config. This might change when
	// we will have use case for that - then Get and Put would need to be changed
	// accordingly.
	globalConfigOwner struct {
		mtx       sync.Mutex // mutex for protecting updates of config
		c         atomic.Pointer
		lmtx      sync.Mutex // mutex for protecting listeners
		listeners map[string]ConfigListener
		confFile  string
	}
	// ConfigListener is interface for listeners which require to be notified
	// about config updates.
	ConfigListener interface {
		ConfigUpdate(oldConf, newConf *Config)
	}
)

//
// global configuration
//

type (
	// Naming convention for setting/getting the particular fields is defined as
	// joining the json tags with dot. Eg. when referring to `EC.Enabled` field
	// one would need to write `ec.enabled`. For more info refer to `IterFields`.
	//
	// nolint:maligned // no performance critical code
	Config struct {
		Confdir          string          `json:"confdir"`
		Cloud            CloudConf       `json:"cloud"`
		Mirror           MirrorConf      `json:"mirror"`
		EC               ECConf          `json:"ec"`
		Log              LogConf         `json:"log"`
		Periodic         PeriodConf      `json:"periodic"`
		Timeout          TimeoutConf     `json:"timeout"`
		Client           ClientConf      `json:"client"`
		Proxy            ProxyConf       `json:"proxy"`
		LRU              LRUConf         `json:"lru"`
		Disk             DiskConf        `json:"disk"`
		Rebalance        RebalanceConf   `json:"rebalance"`
		Replication      ReplicationConf `json:"replication"`
		Cksum            CksumConf       `json:"checksum"`
		Versioning       VersionConf     `json:"versioning"`
		FSpaths          FSPathsConf     `json:"fspaths"`
		TestFSP          TestfspathConf  `json:"test_fspaths"`
		Net              NetConf         `json:"net"`
		FSHC             FSHCConf        `json:"fshc"`
		Auth             AuthConf        `json:"auth"`
		KeepaliveTracker KeepaliveConf   `json:"keepalivetracker"`
		Downloader       DownloaderConf  `json:"downloader"`
		DSort            DSortConf       `json:"distributed_sort"`
		Compression      CompressionConf `json:"compression"`
	}
	CloudConf struct {
		Conf map[string]interface{} `json:"conf,omitempty"` // implementation depends on cloud provider

		// 3rd party Cloud(s) -- set during validation
		Providers map[string]Ns `json:"-"`
	}
	RemoteAISInfo struct {
		URL     string `json:"url"`
		Alias   string `json:"alias"`
		Primary string `json:"primary"`
		Smap    int64  `json:"smap"`
		Targets int32  `json:"targets"`
		Online  bool   `json:"online"`
	}

	CloudConfAIS map[string][]string // cluster alias -> [urls...]
	CloudInfoAIS map[string]*RemoteAISInfo

	MirrorConf struct {
		Copies      int64 `json:"copies"`       // num local copies
		Burst       int   `json:"burst_buffer"` // channel buffer size
		UtilThresh  int64 `json:"util_thresh"`  // considered equivalent when below threshold
		OptimizePUT bool  `json:"optimize_put"` // optimization objective
		Enabled     bool  `json:"enabled"`      // will only generate local copies when set to true
	}
	MirrorConfToUpdate struct {
		Copies      *int64 `json:"copies"`
		Burst       *int   `json:"burst_buffer"`
		UtilThresh  *int64 `json:"util_thresh"`
		OptimizePUT *bool  `json:"optimize_put"`
		Enabled     *bool  `json:"enabled"`
	}
	ECConf struct {
		ObjSizeLimit int64  `json:"objsize_limit"` // objects below this size are replicated instead of EC'ed
		Compression  string `json:"compression"`   // see CompressAlways, etc. enum
		DataSlices   int    `json:"data_slices"`   // number of data slices
		ParitySlices int    `json:"parity_slices"` // number of parity slices/replicas
		BatchSize    int    `json:"batch_size"`    // Batch size for EC rebalance
		Enabled      bool   `json:"enabled"`       // EC is enabled
	}
	ECConfToUpdate struct {
		Enabled      *bool   `json:"enabled"`
		ObjSizeLimit *int64  `json:"objsize_limit"`
		DataSlices   *int    `json:"data_slices"`
		ParitySlices *int    `json:"parity_slices"`
		Compression  *string `json:"compression"`
	}
	LogConf struct {
		Dir      string `json:"dir"`       // log directory
		Level    string `json:"level"`     // log level aka verbosity
		MaxSize  uint64 `json:"max_size"`  // size that triggers log rotation
		MaxTotal uint64 `json:"max_total"` // max total size of all the logs in the log directory
	}
	PeriodConf struct {
		StatsTimeStr     string `json:"stats_time"`
		RetrySyncTimeStr string `json:"retry_sync_time"`
		NotifTimeStr     string `json:"notif_time"`
		// omitempty
		StatsTime     time.Duration `json:"-"`
		RetrySyncTime time.Duration `json:"-"`
		NotifTime     time.Duration `json:"-"`
	}
	// maximum intra-cluster latencies (in the increasing order)
	TimeoutConf struct {
		CplaneOperationStr string        `json:"cplane_operation"`
		CplaneOperation    time.Duration `json:"-"`
		MaxKeepaliveStr    string        `json:"max_keepalive"`
		MaxKeepalive       time.Duration `json:"-"`
		MaxHostBusyStr     string        `json:"max_host_busy"`
		MaxHostBusy        time.Duration `json:"-"`
		StartupStr         string        `json:"startup_time"`
		Startup            time.Duration `json:"-"`
		SendFileStr        string        `json:"send_file_time"`
		SendFile           time.Duration `json:"-"`
	}
	ClientConf struct {
		TimeoutStr     string        `json:"client_timeout"`
		Timeout        time.Duration `json:"-"`
		TimeoutLongStr string        `json:"client_long_timeout"`
		TimeoutLong    time.Duration `json:"-"`
		ListObjectsStr string        `json:"list_timeout"`
		ListObjects    time.Duration `json:"-"`
		Features       FeatureFlags  `json:"features,string"`
	}
	ProxyConf struct {
		PrimaryURL   string `json:"primary_url"`
		OriginalURL  string `json:"original_url"`
		DiscoveryURL string `json:"discovery_url"`
		NonElectable bool   `json:"non_electable"`
	}
	LRUConf struct {
		// LowWM: used capacity low-watermark (% of total local storage capacity)
		LowWM int64 `json:"lowwm"`

		// HighWM: used capacity high-watermark (% of total local storage capacity)
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

		// Enabled: LRU will only run when set to true
		Enabled bool `json:"enabled"`
	}
	LRUConfToUpdate struct {
		LowWM   *int64 `json:"lowwm"`
		HighWM  *int64 `json:"highwm"`
		OOS     *int64 `json:"out_of_space"`
		Enabled *bool  `json:"enabled"`
	}
	DiskConf struct {
		DiskUtilLowWM   int64         `json:"disk_util_low_wm"`  // no throttling below
		DiskUtilHighWM  int64         `json:"disk_util_high_wm"` // throttle longer when above
		DiskUtilMaxWM   int64         `json:"disk_util_max_wm"`
		IostatTimeLong  time.Duration `json:"-"`
		IostatTimeShort time.Duration `json:"-"`

		IostatTimeLongStr  string `json:"iostat_time_long"`
		IostatTimeShortStr string `json:"iostat_time_short"`
	}
	RebalanceConf struct {
		DontRunTimeStr   string        `json:"dont_run_time"`
		DontRunTime      time.Duration `json:"-"`
		DestRetryTimeStr string        `json:"dest_retry_time"` // max wait for ACKs & neighbors to complete
		Quiesce          time.Duration `json:"-"`               // (runtime)
		QuiesceStr       string        `json:"quiescent"`       // max wait for no-obj before next stage/batch
		DestRetryTime    time.Duration `json:"-"`               // (runtime)
		Compression      string        `json:"compression"`     // see CompressAlways, etc. enum
		Multiplier       uint8         `json:"multiplier"`      // stream-bundle-and-jogger multiplier
		Enabled          bool          `json:"enabled"`         // true=auto-rebalance | manual rebalancing
	}
	ReplicationConf struct {
		OnColdGet     bool `json:"on_cold_get"`     // object replication on cold GET request
		OnPut         bool `json:"on_put"`          // object replication on PUT request
		OnLRUEviction bool `json:"on_lru_eviction"` // object replication on LRU eviction
	}
	CksumConf struct {
		// Object checksum; ChecksumNone ("none") disables checksumming.
		Type string `json:"type"`

		// ValidateColdGet determines whether or not the checksum of received object
		// is checked after downloading it from remote (cloud) buckets.
		ValidateColdGet bool `json:"validate_cold_get"`

		// ValidateWarmGet: if enabled, the object's version (if in Cloud-based bucket)
		// and checksum are checked. If either value fail to match, the object
		// is removed from local storage.
		ValidateWarmGet bool `json:"validate_warm_get"`

		// ValidateObjMove determines if migrated objects should have their checksum validated.
		ValidateObjMove bool `json:"validate_obj_move"`

		// EnableReadRange: Return read range checksum otherwise return entire object checksum.
		EnableReadRange bool `json:"enable_read_range"`
	}
	CksumConfToUpdate struct {
		Type            *string `json:"type"`
		ValidateColdGet *bool   `json:"validate_cold_get"`
		ValidateWarmGet *bool   `json:"validate_warm_get"`
		ValidateObjMove *bool   `json:"validate_obj_move"`
		EnableReadRange *bool   `json:"enable_read_range"`
	}

	VersionConf struct {
		// Determines if the versioning is enabled.
		Enabled bool `json:"enabled"`

		// Validate object version upon warm GET.
		ValidateWarmGet bool `json:"validate_warm_get"`
	}
	VersionConfToUpdate struct {
		Enabled         *bool `json:"enabled"`
		ValidateWarmGet *bool `json:"validate_warm_get"`
	}

	TestfspathConf struct {
		Root     string `json:"root"`
		Count    int    `json:"count"`
		Instance int    `json:"instance"`
	}

	NetConf struct {
		IPv4             string   `json:"ipv4"`
		IPv4IntraControl string   `json:"ipv4_intra_control"`
		IPv4IntraData    string   `json:"ipv4_intra_data"`
		L4               L4Conf   `json:"l4"`
		HTTP             HTTPConf `json:"http"`
		UseIntraControl  bool     `json:"-"`
		UseIntraData     bool     `json:"-"`
	}

	L4Conf struct {
		Proto               string `json:"proto"`              // tcp, udp
		PortStr             string `json:"port"`               // listening port
		PortIntraControlStr string `json:"port_intra_control"` // listening port for intra control network
		PortIntraDataStr    string `json:"port_intra_data"`    // listening port for intra data network
		Port                int    `json:"-"`                  // (runtime)
		PortIntraControl    int    `json:"-"`                  // --/--
		PortIntraData       int    `json:"-"`                  // --/--
		SndRcvBufSize       int    `json:"sndrcv_buf_size"`    // SO_RCVBUF and SO_SNDBUF
	}

	HTTPConf struct {
		Proto       string `json:"-"`          // http or https (set depending on `UseHTTPS`)
		Certificate string `json:"server_crt"` // HTTPS: openssl certificate
		Key         string `json:"server_key"` // HTTPS: openssl key
		// http.Transport.WriteBufferSize; if zero, a default (currently 4KB) is used
		WriteBufferSize int `json:"write_buffer_size"`
		// http.Transport.ReadBufferSize; if zero, a default (currently 4KB) is used
		ReadBufferSize int  `json:"read_buffer_size"`
		UseHTTPS       bool `json:"use_https"` // use HTTPS instead of HTTP
		// skip certificate verification for HTTPS (e.g, used with self-signed certificates)
		SkipVerify bool `json:"skip_verify"`
		Chunked    bool `json:"chunked_transfer"` // https://tools.ietf.org/html/rfc7230#page-36
	}
	FSHCConf struct {
		TestFileCount int  `json:"test_files"`  // number of files to read/write
		ErrorLimit    int  `json:"error_limit"` // exceeding err limit causes disabling mountpath
		Enabled       bool `json:"enabled"`
	}
	AuthConf struct {
		Secret  string `json:"secret"`
		Enabled bool   `json:"enabled"`
	}
	// config for one keepalive tracker
	// all type of trackers share the same struct, not all fields are used by all trackers
	KeepaliveTrackerConf struct {
		IntervalStr string        `json:"interval"` // keepalive interval
		Interval    time.Duration `json:"-"`        // (runtime)
		Name        string        `json:"name"`     // "heartbeat", "average"
		Factor      uint8         `json:"factor"`   // only average
	}
	KeepaliveConf struct {
		Proxy         KeepaliveTrackerConf `json:"proxy"`  // how proxy tracks target keepalives
		Target        KeepaliveTrackerConf `json:"target"` // how target tracks primary proxies keepalives
		RetryFactor   uint8                `json:"retry_factor"`
		TimeoutFactor uint8                `json:"timeout_factor"`
	}
	DownloaderConf struct {
		TimeoutStr string        `json:"timeout"`
		Timeout    time.Duration `json:"-"`
	}
	DSortConf struct {
		DuplicatedRecords   string        `json:"duplicated_records"`
		MissingShards       string        `json:"missing_shards"`
		EKMMalformedLine    string        `json:"ekm_malformed_line"`
		EKMMissingKey       string        `json:"ekm_missing_key"`
		DefaultMaxMemUsage  string        `json:"default_max_mem_usage"`
		CallTimeoutStr      string        `json:"call_timeout"`
		Compression         string        `json:"compression"` // see CompressAlways, etc. enum
		DSorterMemThreshold string        `json:"dsorter_mem_threshold"`
		CallTimeout         time.Duration `json:"-"` // time to wait for other target
	}
	FSPathsConf struct {
		Paths StringSet `json:"paths,omitempty"`
	}
	// lz4 block and frame formats: http://fastcompression.blogspot.com/2013/04/lz4-streaming-format-final.html
	CompressionConf struct {
		BlockMaxSize int  `json:"block_size"` // *uncompressed* block max size
		Checksum     bool `json:"checksum"`   // true: checksum lz4 frames
	}
)

// interface guard
var _ ConfigOwner = (*globalConfigOwner)(nil)

var clientFeatureList = []struct {
	name  string
	value FeatureFlags
}{
	{name: "DirectAccess", value: FeatureDirectAccess},
}

///////////////////////
// globalConfigOwner //
///////////////////////

// GCO stands for global config owner which is responsible for updating
// and notifying listeners about any changes in the config. Config is loaded
// at startup and then can be accessed/updated by other services.
var GCO *globalConfigOwner

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

func (gco *globalConfigOwner) GetConfigFile() (s string) {
	gco.mtx.Lock()
	s = gco.confFile
	gco.mtx.Unlock()
	return
}

func (gco *globalConfigOwner) notifyListeners(oldConf *Config) {
	gco.lmtx.Lock()
	newConf := gco.Get()
	for _, listener := range gco.listeners {
		listener.ConfigUpdate(oldConf, newConf)
	}
	gco.lmtx.Unlock()
}

// Reg allows listeners to sign up for notifications about config updates.
func (gco *globalConfigOwner) Reg(key string, cl ConfigListener) {
	gco.lmtx.Lock()
	_, ok := gco.listeners[key]
	Assert(!ok)
	gco.listeners[key] = cl
	gco.lmtx.Unlock()
}

// Unreg
func (gco *globalConfigOwner) Unreg(key string) {
	gco.lmtx.Lock()
	_, ok := gco.listeners[key]
	Assert(ok)
	delete(gco.listeners, key)
	gco.lmtx.Unlock()
}

var (
	SupportedReactions = []string{IgnoreReaction, WarnReaction, AbortReaction}
	supportedL4Protos  = []string{tcpProto}
)

// NOTE: new validators must be run via Config.Validate() - see below
// interface guard
var (
	_ Validator = (*CloudConf)(nil)
	_ Validator = (*CksumConf)(nil)
	_ Validator = (*LRUConf)(nil)
	_ Validator = (*MirrorConf)(nil)
	_ Validator = (*ECConf)(nil)
	_ Validator = (*VersionConf)(nil)
	_ Validator = (*KeepaliveConf)(nil)
	_ Validator = (*PeriodConf)(nil)
	_ Validator = (*TimeoutConf)(nil)
	_ Validator = (*ClientConf)(nil)
	_ Validator = (*RebalanceConf)(nil)
	_ Validator = (*NetConf)(nil)
	_ Validator = (*DownloaderConf)(nil)
	_ Validator = (*DSortConf)(nil)
	_ Validator = (*FSPathsConf)(nil)
	_ Validator = (*TestfspathConf)(nil)
	_ Validator = (*CompressionConf)(nil)

	_ PropsValidator = (*CksumConf)(nil)
	_ PropsValidator = (*LRUConf)(nil)
	_ PropsValidator = (*MirrorConf)(nil)
	_ PropsValidator = (*ECConf)(nil)

	_ json.Marshaler   = (*CloudConf)(nil)
	_ json.Unmarshaler = (*CloudConf)(nil)
	_ json.Marshaler   = (*FSPathsConf)(nil)
	_ json.Unmarshaler = (*FSPathsConf)(nil)
)

/////////////////////////////////
// Config and its nested *Conf //
/////////////////////////////////

func (c *Config) Validate() error {
	opts := IterOpts{VisitAll: true}
	return IterFields(c, func(tag string, field IterField) (err error, b bool) {
		if v, ok := field.Value().(Validator); ok {
			if err := v.Validate(c); err != nil {
				return err, false
			}
		}
		return nil, false
	}, opts)
}

// TestingEnv returns true if config is set to a development environment
// where a single local filesystem is partitioned between all (locally running)
// targets and is used for both local and Cloud buckets
func (c *Config) TestingEnv() bool {
	return c.TestFSP.Count > 0
}

// validKeepaliveType returns true if the keepalive type is supported.
func validKeepaliveType(t string) bool {
	return t == KeepaliveHeartbeatType || t == KeepaliveAverageType
}

func (c *CloudConf) UnmarshalJSON(data []byte) error {
	return jsoniter.Unmarshal(data, &c.Conf)
}

func (c *CloudConf) MarshalJSON() (data []byte, err error) {
	return MustMarshal(c.Conf), nil
}

func (c *CloudConf) Validate(_ *Config) (err error) {
	for provider := range c.Conf {
		b := MustMarshal(c.Conf[provider])
		switch provider {
		case ProviderAIS:
			var aisConf CloudConfAIS
			if err := jsoniter.Unmarshal(b, &aisConf); err != nil {
				return fmt.Errorf("invalid cloud specification: %v", err)
			}
			for alias, urls := range aisConf {
				if len(urls) == 0 {
					return fmt.Errorf("no URL(s) to connect to remote AIS cluster %q", alias)
				}
				break
			}
			c.Conf[provider] = aisConf
		case "":
			continue
		default:
			c.setProvider(provider)
		}
	}
	return nil
}

func (c *CloudConf) setProvider(provider string) {
	var ns Ns
	switch provider {
	case ProviderAmazon, ProviderGoogle, ProviderAzure:
		ns = NsGlobal

	default:
		AssertMsg(false, "unknown cloud provider "+provider)
	}
	if c.Providers == nil {
		c.Providers = map[string]Ns{}
	}
	c.Providers[provider] = ns
}

func (c *CloudConf) ProviderConf(provider string, newConf ...interface{}) (conf interface{}, ok bool) {
	if len(newConf) > 0 {
		c.Conf[provider] = newConf[0]
	}
	conf, ok = c.Conf[provider]
	return
}

func (c *DiskConf) Validate(_ *Config) (err error) {
	lwm, hwm, maxwm := c.DiskUtilLowWM, c.DiskUtilHighWM, c.DiskUtilMaxWM
	if lwm <= 0 || hwm <= lwm || maxwm <= hwm || maxwm > 100 {
		return fmt.Errorf("invalid (disk_util_lwm, disk_util_hwm, disk_util_maxwm) config %+v", c)
	}

	if c.IostatTimeLong, err = time.ParseDuration(c.IostatTimeLongStr); err != nil {
		return fmt.Errorf("invalid disk.iostat_time_long format %s, err %v", c.IostatTimeLongStr, err)
	}
	if c.IostatTimeShort, err = time.ParseDuration(c.IostatTimeShortStr); err != nil {
		return fmt.Errorf("invalid disk.iostat_time_short format %s, err %v", c.IostatTimeShortStr, err)
	}
	if c.IostatTimeLong <= 0 {
		return fmt.Errorf("disk.iostat_time_long is zero")
	}
	if c.IostatTimeShort <= 0 {
		return fmt.Errorf("disk.iostat_time_short is zero")
	}
	if c.IostatTimeLong < c.IostatTimeShort {
		return fmt.Errorf("disk.iostat_time_long %v shorter than disk.iostat_time_short %v",
			c.IostatTimeLong, c.IostatTimeShort)
	}
	return nil
}

func (c *LRUConf) Validate(_ *Config) (err error) {
	lwm, hwm, oos := c.LowWM, c.HighWM, c.OOS
	if lwm <= 0 || hwm < lwm || oos < hwm || oos > 100 {
		return fmt.Errorf("invalid lru (lwm, hwm, oos) configuration (%d, %d, %d)", lwm, hwm, oos)
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

func (c *CksumConf) Validate(_ *Config) (err error) {
	return ValidateCksumType(c.Type)
}

func (c *CksumConf) ValidateAsProps(args *ValidationArgs) (err error) {
	return ValidateCksumType(c.Type)
}

func (c *CksumConf) ShouldValidate() bool {
	return c.ValidateColdGet || c.ValidateObjMove || c.ValidateWarmGet
}

func (c *VersionConf) Validate(_ *Config) error {
	if !c.Enabled && c.ValidateWarmGet {
		return errors.New("versioning.validate_warm_get requires versioning to be enabled")
	}
	return nil
}

func (c *VersionConf) ValidateAsProps() error { return c.Validate(nil) }

func (c *MirrorConf) Validate(_ *Config) error {
	if c.UtilThresh < 0 || c.UtilThresh > 100 {
		return fmt.Errorf("invalid mirror.util_thresh: %v (expected value in range [0, 100])",
			c.UtilThresh)
	}
	if c.Burst < 0 {
		return fmt.Errorf("invalid mirror.burst: %v (expected >0)", c.UtilThresh)
	}
	if c.Copies < 2 || c.Copies > 32 {
		return fmt.Errorf("invalid mirror.copies: %d (expected value in range [2, 32])", c.Copies)
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
		return fmt.Errorf("invalid ec.obj_size_limit: %d (expected >=0)", c.ObjSizeLimit)
	}
	if c.DataSlices < MinSliceCount || c.DataSlices > MaxSliceCount {
		return fmt.Errorf("invalid ec.data_slices: %d (expected value in range [%d, %d])",
			c.DataSlices, MinSliceCount, MaxSliceCount)
	}
	// TODO: warn about performance if number is OK but large?
	if c.ParitySlices < MinSliceCount || c.ParitySlices > MaxSliceCount {
		return fmt.Errorf("invalid ec.parity_slices: %d (expected value in range [%d, %d])",
			c.ParitySlices, MinSliceCount, MaxSliceCount)
	}
	if c.BatchSize == 0 {
		c.BatchSize = 64
	}
	if c.BatchSize < 4 || c.BatchSize > 128 {
		return fmt.Errorf("invalid ec.batch_size: %d (must be in the range 4..128)", c.ObjSizeLimit)
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
			"EC config (%d data, %d parity)slices requires at least %d targets (have %d)",
			c.DataSlices, c.ParitySlices, required, args.TargetCnt)
	}
	return nil
}

func (c *TimeoutConf) Validate(_ *Config) (err error) {
	if c.MaxKeepalive, err = time.ParseDuration(c.MaxKeepaliveStr); err != nil {
		return fmt.Errorf("invalid timeout.max_keepalive format %s, err %v", c.MaxKeepaliveStr, err)
	}
	if c.CplaneOperation, err = time.ParseDuration(c.CplaneOperationStr); err != nil {
		return fmt.Errorf("invalid timeout.vote_request format %s, err %v", c.CplaneOperationStr, err)
	}
	if c.SendFile, err = time.ParseDuration(c.SendFileStr); err != nil {
		return fmt.Errorf("invalid timeout.send_file_time format %s, err %v", c.SendFileStr, err)
	}
	if c.Startup, err = time.ParseDuration(c.StartupStr); err != nil {
		return fmt.Errorf("invalid timeout.startup_time format %s, err %v", c.StartupStr, err)
	}
	if c.MaxHostBusy, err = time.ParseDuration(c.MaxHostBusyStr); err != nil {
		return fmt.Errorf("invalid timeout.max_host_busy format %s, err %v", c.MaxHostBusyStr, err)
	}
	return nil
}

func (c *ClientConf) Validate(_ *Config) (err error) {
	if c.Timeout, err = time.ParseDuration(c.TimeoutStr); err != nil {
		return fmt.Errorf("invalid client.default format %s, err %v", c.TimeoutStr, err)
	}
	if c.TimeoutLong, err = time.ParseDuration(c.TimeoutLongStr); err != nil {
		return fmt.Errorf("invalid client.default_long format %s, err %v", c.TimeoutLongStr, err)
	}
	if c.ListObjects, err = time.ParseDuration(c.ListObjectsStr); err != nil {
		return fmt.Errorf("invalid client.list_timeout format %s, err %v", c.ListObjectsStr, err)
	}
	return nil
}

func (c *RebalanceConf) Validate(_ *Config) (err error) {
	if c.DontRunTimeStr != "" { // can be missing
		if c.DontRunTime, err = time.ParseDuration(c.DontRunTimeStr); err != nil {
			return fmt.Errorf("invalid rebalance.dont_run_time format %s, err %v",
				c.DontRunTimeStr, err)
		}
	}
	if c.DestRetryTime, err = time.ParseDuration(c.DestRetryTimeStr); err != nil {
		return fmt.Errorf("invalid rebalance.dest_retry_time format %s, err %v", c.DestRetryTimeStr, err)
	}
	if c.Quiesce, err = time.ParseDuration(c.QuiesceStr); err != nil {
		return fmt.Errorf("invalid rebalance.quiesce format %s, err %v", c.QuiesceStr, err)
	}
	return nil
}

func (c *PeriodConf) Validate(_ *Config) (err error) {
	if c.StatsTime, err = time.ParseDuration(c.StatsTimeStr); err != nil {
		return fmt.Errorf("invalid periodic.stats_time format %s, err %v", c.StatsTimeStr, err)
	}
	if c.RetrySyncTime, err = time.ParseDuration(c.RetrySyncTimeStr); err != nil {
		return fmt.Errorf("invalid periodic.retry_sync_time format %s, err %v", c.RetrySyncTimeStr, err)
	}
	if c.NotifTime, err = time.ParseDuration(c.NotifTimeStr); err != nil {
		return fmt.Errorf("invalid periodic.notif_time format %s, err %v", c.NotifTimeStr, err)
	}
	return nil
}

func (c *KeepaliveConf) Validate(_ *Config) (err error) {
	if c.Proxy.Interval, err = time.ParseDuration(c.Proxy.IntervalStr); err != nil {
		return fmt.Errorf("invalid keepalivetracker.proxy.interval %s", c.Proxy.IntervalStr)
	}
	if c.Target.Interval, err = time.ParseDuration(c.Target.IntervalStr); err != nil {
		return fmt.Errorf("invalid keepalivetracker.target.interval %s", c.Target.IntervalStr)
	}
	if !validKeepaliveType(c.Proxy.Name) {
		return fmt.Errorf("invalid keepalivetracker.proxy.name %s", c.Proxy.Name)
	}
	if !validKeepaliveType(c.Target.Name) {
		return fmt.Errorf("invalid keepalivetracker.target.name %s", c.Target.Name)
	}
	return nil
}

func (c *NetConf) Validate(_ *Config) (err error) {
	if !StringInSlice(c.L4.Proto, supportedL4Protos) {
		return fmt.Errorf("l4 proto is not recognized %s, expected one of: %s",
			c.L4.Proto, supportedL4Protos)
	}

	c.HTTP.Proto = httpProto // not validating: read-only, and can take only two values
	if c.HTTP.UseHTTPS {
		c.HTTP.Proto = httpsProto
	}

	// Parse ports
	if c.L4.Port, err = ParsePort(c.L4.PortStr); err != nil {
		return fmt.Errorf("invalid public port specified: %v", err)
	}
	if c.L4.PortIntraControl != 0 {
		if c.L4.PortIntraControl, err = ParsePort(c.L4.PortIntraControlStr); err != nil {
			return fmt.Errorf("invalid intra control port specified: %v", err)
		}
	}
	if c.L4.PortIntraData != 0 {
		if c.L4.PortIntraData, err = ParsePort(c.L4.PortIntraDataStr); err != nil {
			return fmt.Errorf("invalid intra data port specified: %v", err)
		}
	}

	c.IPv4 = strings.ReplaceAll(c.IPv4, " ", "")
	c.IPv4IntraControl = strings.ReplaceAll(c.IPv4IntraControl, " ", "")
	c.IPv4IntraData = strings.ReplaceAll(c.IPv4IntraData, " ", "")

	if overlap, addr := ipv4ListsOverlap(c.IPv4, c.IPv4IntraControl); overlap {
		return fmt.Errorf("public (%s) and intra-cluster control (%s) IPv4 lists overlap: %s",
			c.IPv4, c.IPv4IntraControl, addr)
	}
	if overlap, addr := ipv4ListsOverlap(c.IPv4, c.IPv4IntraData); overlap {
		return fmt.Errorf("public (%s) and intra-cluster data (%s) IPv4 lists overlap: %s",
			c.IPv4, c.IPv4IntraData, addr)
	}
	if overlap, addr := ipv4ListsOverlap(c.IPv4IntraControl, c.IPv4IntraData); overlap {
		if ipv4ListsEqual(c.IPv4IntraControl, c.IPv4IntraData) {
			glog.Warningf("control and data share one intra-cluster network (%s)", c.IPv4IntraData)
		} else {
			glog.Warningf("intra-cluster control (%s) and data (%s) IPv4 lists overlap: %s",
				c.IPv4IntraControl, c.IPv4IntraData, addr)
		}
	}
	if !c.HTTP.Chunked {
		glog.Warningln("disabled chunked transfer may cause a slow down (see also: Content-Length)")
	}
	return nil
}

func (c *DownloaderConf) Validate(_ *Config) (err error) {
	if c.Timeout, err = time.ParseDuration(c.TimeoutStr); err != nil {
		return fmt.Errorf("invalid downloader.timeout %s", c.TimeoutStr)
	}
	return nil
}

func (c *DSortConf) Validate(_ *Config) (err error) {
	return c.ValidateWithOpts(nil, false)
}

func (c *DSortConf) ValidateWithOpts(_ *Config, allowEmpty bool) (err error) {
	checkReaction := func(reaction string) bool {
		return StringInSlice(reaction, SupportedReactions) || (allowEmpty && reaction == "")
	}

	if !checkReaction(c.DuplicatedRecords) {
		return fmt.Errorf("invalid distributed_sort.duplicated_records: %s (expecting one of: %s)",
			c.DuplicatedRecords, SupportedReactions)
	}
	if !checkReaction(c.MissingShards) {
		return fmt.Errorf("invalid distributed_sort.missing_shards: %s (expecting one of: %s)",
			c.MissingShards, SupportedReactions)
	}
	if !checkReaction(c.EKMMalformedLine) {
		return fmt.Errorf("invalid distributed_sort.ekm_malformed_line: %s (expecting one of: %s)",
			c.EKMMalformedLine, SupportedReactions)
	}
	if !checkReaction(c.EKMMissingKey) {
		return fmt.Errorf("invalid distributed_sort.ekm_missing_key: %s (expecting one of: %s)",
			c.EKMMissingKey, SupportedReactions)
	}
	if !allowEmpty {
		if _, err := ParseQuantity(c.DefaultMaxMemUsage); err != nil {
			return fmt.Errorf("invalid distributed_sort.default_max_mem_usage: %s (err: %s)",
				c.DefaultMaxMemUsage, err)
		}
		if c.CallTimeout, err = time.ParseDuration(c.CallTimeoutStr); err != nil {
			return fmt.Errorf("invalid distributed_sort.call_timeout: %s", c.CallTimeoutStr)
		}
	}
	if _, err := S2B(c.DSorterMemThreshold); err != nil && (!allowEmpty || c.DSorterMemThreshold != "") {
		return fmt.Errorf("invalid distributed_sort.dsorter_mem_threshold: %s (err: %s)",
			c.DSorterMemThreshold, err)
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
	if c.BlockMaxSize != 64*KiB && c.BlockMaxSize != 256*KiB &&
		c.BlockMaxSize != MiB && c.BlockMaxSize != 4*MiB {
		return fmt.Errorf("invalid compression.block_size %d", c.BlockMaxSize)
	}
	return nil
}

func KeepaliveRetryDuration(cs ...*Config) time.Duration {
	var c *Config
	if len(cs) != 0 {
		c = cs[0]
	} else {
		c = GCO.Get()
	}
	return c.Timeout.CplaneOperation * time.Duration(c.KeepaliveTracker.RetryFactor)
}

func Printf(format string, a ...interface{}) {
	if flag.Parsed() {
		glog.InfoDepth(1, fmt.Sprintf(format, a...))
	} else {
		fmt.Printf(format+"\n", a...)
	}
}

///////////////////
// feature flags //
///////////////////

func (cflags FeatureFlags) IsSet(flag FeatureFlags) bool {
	return cflags&flag == flag
}

func (cflags FeatureFlags) String() string {
	return "0x" + strconv.FormatUint(uint64(cflags), 16)
}

func (cflags FeatureFlags) Describe() string {
	s := ""
	for _, flag := range clientFeatureList {
		if cflags&flag.value != flag.value {
			continue
		}
		if s != "" {
			s += ","
		}
		s += flag.name
	}
	return s
}

//
// misc config utils
//

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

func ConfigPropList() []string {
	propList := []string{"vmodule", "log_level", "log.level"}
	IterFields(Config{}, func(tag string, field IterField) (error, bool) {
		propList = append(propList, tag)
		return nil, false
	})
	return propList
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
