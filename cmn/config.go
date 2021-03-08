// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
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

	// Config
	OverrideConfigFname = ".ais.override_config" // file containing override node config
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
		Provider  string // For ExtraProps.
		TargetCnt int    // For EC.
	}

	Validator interface {
		Validate() error
	}
	PropsValidator interface {
		ValidateAsProps(args *ValidationArgs) error
	}
	FeatureFlags uint64
)

type (
	globalConfigOwner struct {
		mtx       sync.Mutex     // mutex for protecting updates of config
		c         atomic.Pointer // pointer to `Config` (cluster + local + override config)
		oc        atomic.Pointer // pointer to `ConfigToUpdate`, override configuration on node
		lmtx      sync.Mutex     // mutex for protecting listeners
		listeners map[string]ConfigListener

		confPath      atomic.Pointer
		localConfPath atomic.Pointer
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

// TODO: try to remove duplication *Conf v/s *ToUpdate structs
type (
	// Naming convention for setting/getting the particular fields is defined as
	// joining the json tags with dot. Eg. when referring to `EC.Enabled` field
	// one would need to write `ec.enabled`. For more info refer to `IterFields`.
	//
	Config struct {
		role          string `list:"omit"` // Proxy or Target
		ClusterConfig `json:",inline"`
		LocalConfig   `json:",inline"`
	}
	// nolint:maligned // no performance critical code
	ClusterConfig struct {
		Backend     BackendConf     `json:"backend" allow:"cluster"`
		Mirror      MirrorConf      `json:"mirror" allow:"cluster"`
		EC          ECConf          `json:"ec" allow:"cluster"`
		Log         LogConf         `json:"log"`
		Periodic    PeriodConf      `json:"periodic"`
		Timeout     TimeoutConf     `json:"timeout"`
		Client      ClientConf      `json:"client"`
		Proxy       ProxyConf       `json:"proxy" allow:"cluster"`
		LRU         LRUConf         `json:"lru"`
		Disk        DiskConf        `json:"disk"`
		Rebalance   RebalanceConf   `json:"rebalance" allow:"cluster"`
		Resilver    ResilverConf    `json:"resilver"`
		Replication ReplicationConf `json:"replication" allow:"cluster"`
		Cksum       CksumConf       `json:"checksum"`
		Versioning  VersionConf     `json:"versioning" allow:"cluster"`
		Net         NetConf         `json:"net"`
		FSHC        FSHCConf        `json:"fshc"`
		Auth        AuthConf        `json:"auth"`
		Keepalive   KeepaliveConf   `json:"keepalivetracker"`
		Downloader  DownloaderConf  `json:"downloader"`
		DSort       DSortConf       `json:"distributed_sort"`
		Compression CompressionConf `json:"compression"`
		MDWrite     MDWritePolicy   `json:"md_write"`
		LastUpdated string          `json:"lastupdate_time"`
		Version     int64           `json:"config_version,string"` // instance version of config
	}

	LocalConfig struct {
		ConfigDir string         `json:"confdir"`
		HostNet   LocalNetConfig `json:"host_net"`
		FSpaths   FSPathsConf    `json:"fspaths"`
		TestFSP   TestfspathConf `json:"test_fspaths"`
	}

	// Network config specific to node
	LocalNetConfig struct {
		Hostname             string `json:"hostname"`
		HostnameIntraControl string `json:"hostname_intra_control"`
		HostnameIntraData    string `json:"hostname_intra_data"`
		PortStr              string `json:"port"`               // listening port
		PortIntraControlStr  string `json:"port_intra_control"` // listening port for intra control network
		PortIntraDataStr     string `json:"port_intra_data"`    // listening port for intra data network
		Port                 int    `json:"-"`
		PortIntraControl     int    `json:"-"`
		PortIntraData        int    `json:"-"`

		UseIntraControl bool `json:"-"`
		UseIntraData    bool `json:"-"`
	}

	ConfigToUpdate struct {
		ConfigDir   *string                  `json:"confdir"`
		Backend     *BackendConf             `json:"backend"`
		Mirror      *MirrorConfToUpdate      `json:"mirror"`
		EC          *ECConfToUpdate          `json:"ec"`
		Log         *LogConfToUpdate         `json:"log"`
		Periodic    *PeriodConfToUpdate      `json:"periodic"`
		Timeout     *TimeoutConfToUpdate     `json:"timeout"`
		Client      *ClientConfToUpdate      `json:"client"`
		LRU         *LRUConfToUpdate         `json:"lru"`
		Disk        *DiskConfToUpdate        `json:"disk"`
		Rebalance   *RebalanceConfToUpdate   `json:"rebalance"`
		Resilver    *ResilverConfToUpdate    `json:"resilver"`
		Replication *ReplicationConfToUpdate `json:"replication"`
		Cksum       *CksumConfToUpdate       `json:"checksum"`
		Versioning  *VersionConfToUpdate     `json:"versioning"`
		Net         *NetConfToUpdate         `json:"net"`
		FSHC        *FSHCConfToUpdate        `json:"fshc"`
		Auth        *AuthConfToUpdate        `json:"auth"`
		Keepalive   *KeepaliveConfToUpdate   `json:"keepalivetracker"`
		Downloader  *DownloaderConfToUpdate  `json:"downloader"`
		DSort       *DSortConfToUpdate       `json:"distributed_sort"`
		Compression *CompressionConfToUpdate `json:"compression"`
		MDWrite     *MDWritePolicy           `json:"md_write"`
		Proxy       *ProxyConfToUpdate       `json:"proxy"`

		// Logging
		LogLevel *string `json:"log_level" copy:"skip"`
		Vmodule  *string `json:"vmodule" copy:"skip"`
	}

	BackendConf struct {
		Conf map[string]interface{} `json:"conf,omitempty"` // implementation depends on backend provider

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

	BackendConfAIS map[string][]string // cluster alias -> [urls...]
	BackendInfoAIS map[string]*RemoteAISInfo

	BackendConfHDFS struct {
		Addresses           []string `json:"addresses"`
		User                string   `json:"user"`
		UseDatanodeHostname bool     `json:"use_datanode_hostname"`
	}

	MirrorConf struct {
		Copies      int64 `json:"copies"`       // num local copies
		UtilThresh  int64 `json:"util_thresh"`  // considered equivalent when below threshold
		Burst       int   `json:"burst_buffer"` // channel buffer size
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
		DiskOnly     bool   `json:"disk_only"`     // if true, EC does not use SGL - data goes directly to drives
	}
	ECConfToUpdate struct {
		Enabled      *bool   `json:"enabled"`
		ObjSizeLimit *int64  `json:"objsize_limit"`
		DataSlices   *int    `json:"data_slices"`
		BatchSize    *int    `json:"batch_size"`
		ParitySlices *int    `json:"parity_slices"`
		Compression  *string `json:"compression"`
		DiskOnly     *bool   `json:"disk_only"`
	}
	LogConf struct {
		Dir      string `json:"dir"`       // log directory
		Level    string `json:"level"`     // log level aka verbosity
		MaxSize  uint64 `json:"max_size"`  // size that triggers log rotation
		MaxTotal uint64 `json:"max_total"` // max total size of all the logs in the log directory
	}
	LogConfToUpdate struct {
		Dir      *string `json:"dir"`       // log directory
		Level    *string `json:"level"`     // log level aka verbosity
		MaxSize  *uint64 `json:"max_size"`  // size that triggers log rotation
		MaxTotal *uint64 `json:"max_total"` // max total size of all the logs in the log directory
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

	PeriodConfToUpdate struct {
		StatsTimeStr     *string `json:"stats_time"`
		RetrySyncTimeStr *string `json:"retry_sync_time"`
		NotifTimeStr     *string `json:"notif_time"`
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

	TimeoutConfToUpdate struct {
		CplaneOperationStr *string `json:"cplane_operation"`
		MaxKeepaliveStr    *string `json:"max_keepalive"`
		MaxHostBusyStr     *string `json:"max_host_busy"`
		StartupStr         *string `json:"startup_time"`
		SendFileStr        *string `json:"send_file_time"`
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
	ClientConfToUpdate struct {
		TimeoutStr     *string       `json:"client_timeout"`
		TimeoutLongStr *string       `json:"client_long_timeout"`
		ListObjectsStr *string       `json:"list_timeout"`
		Features       *FeatureFlags `json:"features,string"`
	}
	ProxyConf struct {
		PrimaryURL   string `json:"primary_url"`
		OriginalURL  string `json:"original_url"`
		DiscoveryURL string `json:"discovery_url"`
		NonElectable bool   `json:"non_electable"`
	}
	ProxyConfToUpdate struct {
		PrimaryURL   *string `json:"primary_url"`
		OriginalURL  *string `json:"original_url"`
		DiscoveryURL *string `json:"discovery_url"`
		NonElectable *bool   `json:"non_electable"`
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
		LowWM              *int64  `json:"lowwm"`
		HighWM             *int64  `json:"highwm"`
		OOS                *int64  `json:"out_of_space"`
		DontEvictTimeStr   *string `json:"dont_evict_time"`
		CapacityUpdTimeStr *string `json:"capacity_upd_time"`
		Enabled            *bool   `json:"enabled"`
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
	DiskConfToUpdate struct {
		DiskUtilLowWM      *int64  `json:"disk_util_low_wm"`  // no throttling below
		DiskUtilHighWM     *int64  `json:"disk_util_high_wm"` // throttle longer when above
		DiskUtilMaxWM      *int64  `json:"disk_util_max_wm"`
		IostatTimeLongStr  *string `json:"iostat_time_long"`
		IostatTimeShortStr *string `json:"iostat_time_short"`
	}
	RebalanceConf struct {
		DestRetryTimeStr string        `json:"dest_retry_time"` // max wait for ACKs & neighbors to complete
		Quiesce          time.Duration `json:"-"`               // (runtime)
		QuiesceStr       string        `json:"quiescent"`       // max wait for no-obj before next stage/batch
		DestRetryTime    time.Duration `json:"-"`               // (runtime)
		Compression      string        `json:"compression"`     // see CompressAlways, etc. enum
		Multiplier       uint8         `json:"multiplier"`      // stream-bundle-and-jogger multiplier
		Enabled          bool          `json:"enabled"`         // true=auto-rebalance | manual rebalancing
	}

	RebalanceConfToUpdate struct {
		DestRetryTimeStr *string `json:"dest_retry_time"` // max wait for ACKs & neighbors to complete
		QuiesceStr       *string `json:"quiescent"`       // max wait for no-obj before next stage/batch
		Compression      *string `json:"compression"`     // see CompressAlways, etc. enum
		Multiplier       *uint8  `json:"multiplier"`      // stream-bundle-and-jogger multiplier
		Enabled          *bool   `json:"enabled"`         // true=auto-rebalance | manual rebalancing
	}

	ResilverConf struct {
		Enabled bool `json:"enabled"` // true=auto-resilver | manual resilvering
	}

	ResilverConfToUpdate struct {
		Enabled *bool `json:"enabled"` // true=auto-resilver | manual resilvering
	}

	ReplicationConf struct {
		OnColdGet     bool `json:"on_cold_get"`     // object replication on cold GET request
		OnPut         bool `json:"on_put"`          // object replication on PUT request
		OnLRUEviction bool `json:"on_lru_eviction"` // object replication on LRU eviction
	}

	ReplicationConfToUpdate struct {
		OnColdGet     *bool `json:"on_cold_get"`     // object replication on cold GET request
		OnPut         *bool `json:"on_put"`          // object replication on PUT request
		OnLRUEviction *bool `json:"on_lru_eviction"` // object replication on LRU eviction
	}

	CksumConf struct {
		// Object checksum; ChecksumNone ("none") disables checksumming.
		Type string `json:"type"`

		// ValidateColdGet determines whether or not the checksum of received object
		// is checked after downloading it from remote (cloud) buckets.
		ValidateColdGet bool `json:"validate_cold_get"`

		// ValidateWarmGet: if enabled, the object's version (if exists) and checksum are checked.
		// If either value fail to match, the object is removed from local storage.
		// NOTE: object versioning is backend-specific and is may _not_ be supported by a given
		//      (supported) backends - see docs for details.
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
		L4   L4Conf   `json:"l4"`
		HTTP HTTPConf `json:"http"`
	}
	NetConfToUpdate struct {
		L4   *L4ConfToUpdate   `json:"l4"`
		HTTP *HTTPConfToUpdate `json:"http"`
	}

	L4Conf struct {
		Proto         string `json:"proto"`           // tcp, udp
		SndRcvBufSize int    `json:"sndrcv_buf_size"` // SO_RCVBUF and SO_SNDBUF
	}
	L4ConfToUpdate struct {
		Proto               *string `json:"proto"`              // tcp, udp
		PortStr             *string `json:"port"`               // listening port
		PortIntraControlStr *string `json:"port_intra_control"` // listening port for intra control network
		PortIntraDataStr    *string `json:"port_intra_data"`    // listening port for intra data network
		SndRcvBufSize       *int    `json:"sndrcv_buf_size"`    // SO_RCVBUF and SO_SNDBUF
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
	HTTPConfToUpdate struct {
		Certificate     *string `json:"server_crt"` // HTTPS: openssl certificate
		Key             *string `json:"server_key"` // HTTPS: openssl key
		WriteBufferSize *int    `json:"write_buffer_size"`
		ReadBufferSize  *int    `json:"read_buffer_size"`
		UseHTTPS        *bool   `json:"use_https"` // use HTTPS instead of HTTP
		SkipVerify      *bool   `json:"skip_verify"`
		Chunked         *bool   `json:"chunked_transfer"` // https://tools.ietf.org/html/rfc7230#page-36
	}
	FSHCConf struct {
		TestFileCount int  `json:"test_files"`  // number of files to read/write
		ErrorLimit    int  `json:"error_limit"` // exceeding err limit causes disabling mountpath
		Enabled       bool `json:"enabled"`
	}
	FSHCConfToUpdate struct {
		TestFileCount *int  `json:"test_files"`  // number of files to read/write
		ErrorLimit    *int  `json:"error_limit"` // exceeding err limit causes disabling mountpath
		Enabled       *bool `json:"enabled"`
	}
	AuthConf struct {
		Secret  string `json:"secret"`
		Enabled bool   `json:"enabled"`
	}
	AuthConfToUpdate struct {
		Secret  *string `json:"secret"`
		Enabled *bool   `json:"enabled"`
	}
	// config for one keepalive tracker
	// all type of trackers share the same struct, not all fields are used by all trackers
	KeepaliveTrackerConf struct {
		IntervalStr string        `json:"interval"` // keepalive interval
		Interval    time.Duration `json:"-"`        // (runtime)
		Name        string        `json:"name"`     // "heartbeat", "average"
		Factor      uint8         `json:"factor"`   // only average
	}
	KeepaliveTrackerConfToUpdate struct {
		IntervalStr *string `json:"interval"` // keepalive interval
		Name        *string `json:"name"`     // "heartbeat", "average"
		Factor      *uint8  `json:"factor"`   // only average
	}

	KeepaliveConf struct {
		Proxy         KeepaliveTrackerConf `json:"proxy"`  // how proxy tracks target keepalives
		Target        KeepaliveTrackerConf `json:"target"` // how target tracks primary proxies keepalives
		RetryFactor   uint8                `json:"retry_factor"`
		TimeoutFactor uint8                `json:"timeout_factor"`
	}

	KeepaliveConfToUpdate struct {
		Proxy         *KeepaliveTrackerConfToUpdate `json:"proxy"`  // how proxy tracks target keepalives
		Target        *KeepaliveTrackerConfToUpdate `json:"target"` // how target tracks primary proxies keepalives
		RetryFactor   *uint8                        `json:"retry_factor"`
		TimeoutFactor *uint8                        `json:"timeout_factor"`
	}
	DownloaderConf struct {
		TimeoutStr string        `json:"timeout"`
		Timeout    time.Duration `json:"-"`
	}
	DownloaderConfToUpdate struct {
		TimeoutStr *string `json:"timeout"`
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

	DSortConfToUpdate struct {
		DuplicatedRecords   *string `json:"duplicated_records"`
		MissingShards       *string `json:"missing_shards"`
		EKMMalformedLine    *string `json:"ekm_malformed_line"`
		EKMMissingKey       *string `json:"ekm_missing_key"`
		DefaultMaxMemUsage  *string `json:"default_max_mem_usage"`
		CallTimeoutStr      *string `json:"call_timeout"`
		Compression         *string `json:"compression"`
		DSorterMemThreshold *string `json:"dsorter_mem_threshold"`
	}

	FSPathsConf struct {
		Paths cos.StringSet `json:"paths,omitempty"`
	}
	// lz4 block and frame formats: http://fastcompression.blogspot.com/2013/04/lz4-streaming-format-final.html
	CompressionConf struct {
		BlockMaxSize int  `json:"block_size"` // *uncompressed* block max size
		Checksum     bool `json:"checksum"`   // true: checksum lz4 frames
	}

	CompressionConfToUpdate struct {
		BlockMaxSize *int  `json:"block_size"`
		Checksum     *bool `json:"checksum"`
	}
)

////////////////////////////////////////////
// config meta-versioning & serialization //
////////////////////////////////////////////

var (
	_ jsp.Opts = (*ClusterConfig)(nil)
	_ jsp.Opts = (*LocalConfig)(nil)
	_ jsp.Opts = (*ConfigToUpdate)(nil)
)

var configJspOpts = jsp.Plain() // TODO -- FIXME: use CCSign(MetaverConfig)

func (*ClusterConfig) JspOpts() jsp.Options  { return configJspOpts }
func (*LocalConfig) JspOpts() jsp.Options    { return jsp.Plain() }
func (*ConfigToUpdate) JspOpts() jsp.Options { return configJspOpts }

///////////////////////
// globalConfigOwner //
///////////////////////

// GCO (Global Config Owner) is responsible for updating and notifying
// listeners about any changes in the config. Global Config is loaded
// at startup and then can be accessed/updated by other services.
var GCO *globalConfigOwner

var clientFeatureList = []struct {
	name  string
	value FeatureFlags
}{
	{name: "DirectAccess", value: FeatureDirectAccess},
}

func (gco *globalConfigOwner) Get() *Config {
	return (*Config)(gco.c.Load())
}

func (gco *globalConfigOwner) Put(config *Config) {
	gco.c.Store(unsafe.Pointer(config))
}

func (gco *globalConfigOwner) GetOverrideConfig() *ConfigToUpdate {
	return (*ConfigToUpdate)(gco.oc.Load())
}

func (gco *globalConfigOwner) PutOverrideConfig(config *ConfigToUpdate) {
	gco.oc.Store(unsafe.Pointer(config))
}

// NOTE: CopyStruct is a shallow copy which is OK because Config has mostly values with read-only
//       FSPaths and BackendConf being the only exceptions. May need a *proper* deep copy in the future.
// NOTE: Cloning a large (and growing) structure may adversely affect performance.
func (gco *globalConfigOwner) Clone() *Config {
	config := &Config{}

	cos.CopyStruct(config, gco.Get())
	return config
}

// When updating we need to make sure that the update is transaction and no
// other update can happen when other transaction is in progress. Therefore,
// we introduce locking mechanism which targets this problem.
//
// NOTE: BeginUpdate must be followed by CommitUpdate.
// NOTE: `ais` package must use its own cfgBegin/Commit/DiscardUpdate functions
func (gco *globalConfigOwner) BeginUpdate() *Config {
	gco.mtx.Lock()
	return gco.Clone()
}

// CommitUpdate finalizes config update and notifies listeners.
// NOTE: `ais` package must use its own cfgBegin/Commit/DiscardUpdate functions
func (gco *globalConfigOwner) CommitUpdate(config *Config) {
	oldConf := gco.Get()
	gco.c.Store(unsafe.Pointer(config))
	gco.mtx.Unlock()

	gco.notifyListeners(oldConf) // serializes via gco.lmtx
}

// DiscardUpdate discards commit updates.
func (gco *globalConfigOwner) DiscardUpdate() {
	gco.mtx.Unlock()
}

func (gco *globalConfigOwner) SetGlobalConfigPath(path string) {
	gco.confPath.Store(unsafe.Pointer(&path))
}

func (gco *globalConfigOwner) SetLocalConfigPath(path string) {
	gco.localConfPath.Store(unsafe.Pointer(&path))
}

func (gco *globalConfigOwner) GetLocalConfigPath() (s string) {
	return *(*string)(gco.localConfPath.Load())
}

func (gco *globalConfigOwner) GetGlobalConfigPath() (s string) {
	return *(*string)(gco.confPath.Load())
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
	cos.Assert(!ok)
	gco.listeners[key] = cl
	gco.lmtx.Unlock()
}

// Unreg
func (gco *globalConfigOwner) Unreg(key string) {
	gco.lmtx.Lock()
	_, ok := gco.listeners[key]
	cos.Assert(ok)
	delete(gco.listeners, key)
	gco.lmtx.Unlock()
}

func (gco *globalConfigOwner) SetConfigInMem(toUpdate *ConfigToUpdate, config *Config, asType string) (err error) {
	if toUpdate.Vmodule != nil {
		if err := SetGLogVModule(*toUpdate.Vmodule); err != nil {
			return fmt.Errorf("failed to set vmodule = %s, err: %v", *toUpdate.Vmodule, err)
		}
	}
	if toUpdate.LogLevel != nil {
		if err := SetLogLevel(config, *toUpdate.LogLevel); err != nil {
			return fmt.Errorf("failed to set log level = %s, err: %v", *toUpdate.LogLevel, err)
		}
	}
	err = config.Apply(*toUpdate, asType)
	return
}

func (gco *globalConfigOwner) Update(cluConfig ClusterConfig) (err error) {
	config := gco.Clone()
	config.ClusterConfig = cluConfig
	override := gco.GetOverrideConfig()
	if override != nil {
		err = config.Apply(*override, Daemon)
	} else {
		err = config.Validate()
	}
	if err != nil {
		return
	}
	gco.Put(config)
	return
}

var (
	SupportedReactions = []string{IgnoreReaction, WarnReaction, AbortReaction}
	supportedL4Protos  = []string{tcpProto}
)

// NOTE: new validators must be run via Config.Validate() - see below
// interface guard
var (
	_ Validator = (*BackendConf)(nil)
	_ Validator = (*CksumConf)(nil)
	_ Validator = (*LRUConf)(nil)
	_ Validator = (*MirrorConf)(nil)
	_ Validator = (*ECConf)(nil)
	_ Validator = (*LogConf)(nil)
	_ Validator = (*VersionConf)(nil)
	_ Validator = (*KeepaliveConf)(nil)
	_ Validator = (*PeriodConf)(nil)
	_ Validator = (*TimeoutConf)(nil)
	_ Validator = (*ClientConf)(nil)
	_ Validator = (*RebalanceConf)(nil)
	_ Validator = (*ResilverConf)(nil)
	_ Validator = (*NetConf)(nil)
	_ Validator = (*DownloaderConf)(nil)
	_ Validator = (*DSortConf)(nil)
	_ Validator = (*CompressionConf)(nil)
	_ Validator = (*LocalNetConfig)(nil)

	_ PropsValidator = (*CksumConf)(nil)
	_ PropsValidator = (*LRUConf)(nil)
	_ PropsValidator = (*MirrorConf)(nil)
	_ PropsValidator = (*ECConf)(nil)

	_ json.Marshaler   = (*BackendConf)(nil)
	_ json.Unmarshaler = (*BackendConf)(nil)
	_ json.Marshaler   = (*FSPathsConf)(nil)
	_ json.Unmarshaler = (*FSPathsConf)(nil)
)

/////////////////////////////////
// Config and its nested *Conf //
/////////////////////////////////

func (c *Config) Validate() error {
	if c.ConfigDir == "" {
		return errors.New("invalid confdir value (must be non-empty)")
	}
	// NOTE: these two validations require more context and so we call them explicitly;
	//       the rest all implement generic interface
	if err := c.LocalConfig.FSpaths.Validate(c); err != nil {
		return err
	}
	if err := c.LocalConfig.TestFSP.Validate(c); err != nil {
		return err
	}
	opts := IterOpts{VisitAll: true}
	return IterFields(c, func(tag string, field IterField) (err error, b bool) {
		if v, ok := field.Value().(Validator); ok {
			if err := v.Validate(); err != nil {
				return err, false
			}
		}
		return nil, false
	}, opts)
}

func (c *Config) SetRole(role string) {
	debug.Assert(role == Target || role == Proxy)
	c.role = role
}

func (c *Config) Apply(updateConf ConfigToUpdate, asType string) (err error) {
	err = c.ClusterConfig.Apply(updateConf, asType)
	if err != nil {
		return
	}
	return c.Validate()
}

func (c *ClusterConfig) Apply(updateConf ConfigToUpdate, asType string) error {
	return copyProps(updateConf, c, asType)
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

func (c *BackendConf) UnmarshalJSON(data []byte) error {
	return jsoniter.Unmarshal(data, &c.Conf)
}

func (c *BackendConf) MarshalJSON() (data []byte, err error) {
	return cos.MustMarshal(c.Conf), nil
}

func (c *BackendConf) Validate() (err error) {
	for provider := range c.Conf {
		b := cos.MustMarshal(c.Conf[provider])
		switch provider {
		case ProviderAIS:
			var aisConf BackendConfAIS
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
		case ProviderHDFS:
			var hdfsConf BackendConfHDFS
			if err := jsoniter.Unmarshal(b, &hdfsConf); err != nil {
				return fmt.Errorf("invalid cloud specification: %v", err)
			}
			if len(hdfsConf.Addresses) == 0 {
				return fmt.Errorf("no addresses provided to HDFS NameNode")
			}

			// Check connectivity and filter out non-reachable addresses.
			reachableAddrs := hdfsConf.Addresses[:0]
			for _, address := range hdfsConf.Addresses {
				conn, err := net.DialTimeout("tcp", address, 5*time.Second)
				if err != nil {
					glog.Warningf(
						"Failed to dial %q HDFS address, check connectivity to the HDFS cluster, err: %v",
						address, err,
					)
					continue
				}
				conn.Close()
				reachableAddrs = append(reachableAddrs, address)
			}
			hdfsConf.Addresses = reachableAddrs

			// Re-check if there is any address reachable.
			if len(hdfsConf.Addresses) == 0 {
				return fmt.Errorf("no address provided to HDFS NameNode is reachable")
			}

			c.Conf[provider] = hdfsConf
			c.setProvider(provider)
		case "":
			continue
		default:
			c.setProvider(provider)
		}
	}
	return nil
}

func (c *BackendConf) setProvider(provider string) {
	var ns Ns
	switch provider {
	case ProviderAmazon, ProviderAzure, ProviderGoogle, ProviderHDFS:
		ns = NsGlobal
	default:
		cos.AssertMsg(false, "unknown backend provider "+provider)
	}
	if c.Providers == nil {
		c.Providers = map[string]Ns{}
	}
	c.Providers[provider] = ns
}

func (c *BackendConf) ProviderConf(provider string, newConf ...interface{}) (conf interface{}, ok bool) {
	if len(newConf) > 0 {
		c.Conf[provider] = newConf[0]
	}
	conf, ok = c.Conf[provider]
	return
}

func (c *DiskConf) Validate() (err error) {
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

func (c *LRUConf) Validate() (err error) {
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
	return c.Validate()
}

func (c *CksumConf) Validate() (err error) {
	return cos.ValidateCksumType(c.Type)
}

func (c *CksumConf) ValidateAsProps(args *ValidationArgs) (err error) {
	return cos.ValidateCksumType(c.Type)
}

func (c *CksumConf) ShouldValidate() bool {
	return c.ValidateColdGet || c.ValidateObjMove || c.ValidateWarmGet
}

func (c *VersionConf) Validate() error {
	if !c.Enabled && c.ValidateWarmGet {
		return errors.New("versioning.validate_warm_get requires versioning to be enabled")
	}
	return nil
}

func (c *MirrorConf) Validate() error {
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
	return c.Validate()
}

func (c MDWritePolicy) Validate() (err error) {
	if c.IsImmediate() || c == WriteDelayed || c == WriteNever {
		return
	}
	return fmt.Errorf("invalid md_write policy %q", c)
}

func (c MDWritePolicy) ValidateAsProps(_ *ValidationArgs) (err error) { return c.Validate() }

func (c *ECConf) Validate() error {
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
	const insufficientNodes = "EC config (%d data, %d parity) slices requires at least %d targets (have %d)"
	if !c.Enabled {
		return nil
	}
	if err := c.Validate(); err != nil {
		return err
	}
	required := c.RequiredEncodeTargets()
	if c.ParitySlices > args.TargetCnt {
		return fmt.Errorf(insufficientNodes, c.DataSlices, c.ParitySlices, required, args.TargetCnt)
	}
	if args.TargetCnt < required {
		return NewSoftError(fmt.Sprintf(insufficientNodes, c.DataSlices, c.ParitySlices, required, args.TargetCnt))
	}
	return nil
}

func (c *LogConf) Validate() (err error) {
	if c.Dir == "" {
		return errors.New("invalid log.dir value (must be non-empty)")
	}
	return nil
}

func (c *TimeoutConf) Validate() (err error) {
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

func (c *ClientConf) Validate() (err error) {
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

func (c *RebalanceConf) Validate() (err error) {
	if c.DestRetryTime, err = time.ParseDuration(c.DestRetryTimeStr); err != nil {
		return fmt.Errorf("invalid rebalance.dest_retry_time format %s, err %v", c.DestRetryTimeStr, err)
	}
	if c.Quiesce, err = time.ParseDuration(c.QuiesceStr); err != nil {
		return fmt.Errorf("invalid rebalance.quiesce format %s, err %v", c.QuiesceStr, err)
	}
	return nil
}

func (c *ResilverConf) Validate() (err error) {
	return nil // no validation required
}

func (c *PeriodConf) Validate() (err error) {
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

func (c *KeepaliveConf) Validate() (err error) {
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

func (c *NetConf) Validate() (err error) {
	if !cos.StringInSlice(c.L4.Proto, supportedL4Protos) {
		return fmt.Errorf("l4 proto is not recognized %s, expected one of: %s",
			c.L4.Proto, supportedL4Protos)
	}

	c.HTTP.Proto = httpProto // not validating: read-only, and can take only two values
	if c.HTTP.UseHTTPS {
		c.HTTP.Proto = httpsProto
	}
	return nil
}

func (c *LocalNetConfig) Validate() (err error) {
	c.Hostname = strings.ReplaceAll(c.Hostname, " ", "")
	c.HostnameIntraControl = strings.ReplaceAll(c.HostnameIntraControl, " ", "")
	c.HostnameIntraData = strings.ReplaceAll(c.HostnameIntraData, " ", "")

	if overlap, addr := hostnameListsOverlap(c.Hostname, c.HostnameIntraControl); overlap {
		return fmt.Errorf("public (%s) and intra-cluster control (%s) Hostname lists overlap: %s",
			c.Hostname, c.HostnameIntraControl, addr)
	}
	if overlap, addr := hostnameListsOverlap(c.Hostname, c.HostnameIntraData); overlap {
		return fmt.Errorf("public (%s) and intra-cluster data (%s) Hostname lists overlap: %s",
			c.Hostname, c.HostnameIntraData, addr)
	}
	if overlap, addr := hostnameListsOverlap(c.HostnameIntraControl, c.HostnameIntraData); overlap {
		if ipv4ListsEqual(c.HostnameIntraControl, c.HostnameIntraData) {
			glog.Warningf("control and data share one intra-cluster network (%s)", c.HostnameIntraData)
		} else {
			glog.Warningf("intra-cluster control (%s) and data (%s) Hostname lists overlap: %s",
				c.HostnameIntraControl, c.HostnameIntraData, addr)
		}
	}

	// Parse ports
	if c.Port, err = ParsePort(c.PortStr); err != nil {
		return fmt.Errorf("invalid %s port specified: %v", NetworkPublic, err)
	}
	if c.PortIntraControlStr != "" {
		if c.PortIntraControl, err = ParsePort(c.PortIntraControlStr); err != nil {
			return fmt.Errorf("invalid %s port specified: %v", NetworkIntraControl, err)
		}
	}
	if c.PortIntraDataStr != "" {
		if c.PortIntraData, err = ParsePort(c.PortIntraDataStr); err != nil {
			return fmt.Errorf("invalid %s port specified: %v", NetworkIntraData, err)
		}
	}

	differentIPs := c.Hostname != c.HostnameIntraControl
	differentPorts := c.Port != c.PortIntraControl
	c.UseIntraControl = c.HostnameIntraControl != "" &&
		c.PortIntraControl != 0 && (differentIPs || differentPorts)

	differentIPs = c.Hostname != c.HostnameIntraData
	differentPorts = c.Port != c.PortIntraData
	c.UseIntraData = c.HostnameIntraData != "" &&
		c.PortIntraData != 0 && (differentIPs || differentPorts)
	return
}

func (c *DownloaderConf) Validate() (err error) {
	if c.Timeout, err = time.ParseDuration(c.TimeoutStr); err != nil {
		return fmt.Errorf("invalid downloader.timeout %s", c.TimeoutStr)
	}
	return nil
}

func (c *DSortConf) Validate() (err error) {
	return c.ValidateWithOpts(nil, false)
}

func (c *DSortConf) ValidateWithOpts(_ *Config, allowEmpty bool) (err error) {
	checkReaction := func(reaction string) bool {
		return cos.StringInSlice(reaction, SupportedReactions) || (allowEmpty && reaction == "")
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
		if _, err := cos.ParseQuantity(c.DefaultMaxMemUsage); err != nil {
			return fmt.Errorf("invalid distributed_sort.default_max_mem_usage: %s (err: %s)",
				c.DefaultMaxMemUsage, err)
		}
		if c.CallTimeout, err = time.ParseDuration(c.CallTimeoutStr); err != nil {
			return fmt.Errorf("invalid distributed_sort.call_timeout: %s", c.CallTimeoutStr)
		}
	}
	if _, err := cos.S2B(c.DSorterMemThreshold); err != nil && (!allowEmpty || c.DSorterMemThreshold != "") {
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
	return cos.MustMarshal(m), nil
}

func (c *FSPathsConf) Validate(contextConfig *Config) (err error) {
	debug.Assertf(cos.StringInSlice(contextConfig.role, []string{Proxy, Target}),
		"unexpected role: %q", contextConfig.role)

	// Don't validate if testing environment
	if contextConfig.TestingEnv() || contextConfig.role != Target {
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
func (c *CompressionConf) Validate() (err error) {
	if c.BlockMaxSize != 64*cos.KiB && c.BlockMaxSize != 256*cos.KiB &&
		c.BlockMaxSize != cos.MiB && c.BlockMaxSize != 4*cos.MiB {
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
	return c.Timeout.CplaneOperation * time.Duration(c.Keepalive.RetryFactor)
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

///////////////////////////
//    ConfigToUpdate     //
//////////////////////////

// FillFromQuery populates ConfigToUpdate from URL query values
func (ctu *ConfigToUpdate) FillFromQuery(query url.Values) error {
	var anyExists bool
	for key := range query {
		if key == ActTransient {
			continue
		}
		anyExists = true
		name, value := strings.ToLower(key), query.Get(key)
		if name == "log.level" {
			ctu.LogLevel = &value
			continue
		}

		if err := UpdateFieldValue(ctu, name, value); err != nil {
			return err
		}
	}

	if !anyExists {
		return fmt.Errorf("no properties to update")
	}
	return nil
}

func (ctu *ConfigToUpdate) Merge(update *ConfigToUpdate) {
	mergeProps(update, ctu)
}

// FillFromKVS populates `ConfigToUpdate` from key value pairs of the form `key=value`
func (ctu *ConfigToUpdate) FillFromKVS(kvs []string) (err error) {
	const format = "failed to parse `-config_custom` flag (invalid entry: %q)"
	for _, kv := range kvs {
		entry := strings.SplitN(kv, "=", 2)
		if len(entry) != 2 {
			return fmt.Errorf(format, kv)
		}
		name, value := entry[0], entry[1]
		if err := UpdateFieldValue(ctu, name, value); err != nil {
			return fmt.Errorf(format, kv)
		}
	}
	return
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

// hostnameListsOverlap checks if two comma-separated ipv4 address lists
// contain at least one common ipv4 address
func hostnameListsOverlap(alist, blist string) (overlap bool, addr string) {
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
	return cos.StrSlicesEqual(al, bl)
}

/////////
// jsp //
/////////

func LoadConfig(confPath, localConfPath, daeRole string, config *Config) (err error) {
	debug.Assert(confPath != "" && localConfPath != "")
	GCO.SetGlobalConfigPath(confPath)
	GCO.SetLocalConfigPath(localConfPath)

	// NOTE: the following two "loads" are loading plain-text config
	//       and are executed only once when the node starts up;
	//       once started, the node can then reload the last
	//       updated version of the (global|local) config
	//       from the configured location
	_, err = jsp.Load(confPath, &config.ClusterConfig, jsp.Plain())
	if err != nil {
		return fmt.Errorf("failed to load global config %q, err: %v", confPath, err)
	}
	config.SetRole(daeRole)
	_, err = jsp.Load(localConfPath, &config.LocalConfig, jsp.Plain())
	if err != nil {
		return fmt.Errorf("failed to load local config %q, err: %v", localConfPath, err)
	}

	overrideConfig, err := loadOverrideConfig(config.ConfigDir)
	if err != nil {
		return err
	}

	if overrideConfig != nil {
		GCO.PutOverrideConfig(overrideConfig)
		err = config.Apply(*overrideConfig, Daemon)
	} else {
		err = config.Validate()
	}
	if err != nil {
		return
	}

	if err = cos.CreateDir(config.Log.Dir); err != nil {
		return fmt.Errorf("failed to create log dir %q, err: %v", config.Log.Dir, err)
	}
	glog.SetLogDir(config.Log.Dir)

	// glog rotate
	glog.MaxSize = config.Log.MaxSize
	if glog.MaxSize > cos.GiB {
		glog.Warningf("log.max_size %d exceeded 1GB, setting the default 1MB", glog.MaxSize)
		glog.MaxSize = cos.MiB
	}

	if err = SetLogLevel(config, config.Log.Level); err != nil {
		return fmt.Errorf("failed to set log level %q, err: %s", config.Log.Level, err)
	}
	glog.Infof("log.dir: %q; l4.proto: %s; port: %d; verbosity: %s",
		config.Log.Dir, config.Net.L4.Proto, config.HostNet.Port, config.Log.Level)
	glog.Infof("config_file: %q periodic.stats_time: %v", confPath, config.Periodic.StatsTime)
	return
}

func SaveOverrideConfig(configDir string, config *ConfigToUpdate) error {
	return jsp.SaveMeta(path.Join(configDir, OverrideConfigFname), config)
}

func loadOverrideConfig(configDir string) (config *ConfigToUpdate, err error) {
	config = &ConfigToUpdate{}
	_, err = jsp.LoadMeta(path.Join(configDir, OverrideConfigFname), config)
	if os.IsNotExist(err) {
		err = nil
	}
	return
}
