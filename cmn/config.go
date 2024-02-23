// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/nlog"
	jsoniter "github.com/json-iterator/go"
)

type (
	Validator interface {
		Validate() error
	}
	PropsValidator interface {
		ValidateAsProps(arg ...any) error
	}
)

// Config is a single control structure (persistent and versioned)
// that contains both cluster (global) and node (local) configuration
// Naming convention for setting/getting values: (parent section json tag . child json tag)
// See also: `IterFields`, `IterFieldNameSepa`
type (
	Config struct {
		role          string `list:"omit"` // Proxy or Target
		ClusterConfig `json:",inline"`
		LocalConfig   `json:",inline"`
	}
)

// local config
type (
	LocalConfig struct {
		ConfigDir string         `json:"confdir"`
		LogDir    string         `json:"log_dir"`
		HostNet   LocalNetConfig `json:"host_net"`
		FSP       FSPConf        `json:"fspaths"`
		TestFSP   TestFSPConf    `json:"test_fspaths"`
	}

	// ais node: (local) network config
	LocalNetConfig struct {
		Hostname             string `json:"hostname"`
		HostnameIntraControl string `json:"hostname_intra_control"`
		HostnameIntraData    string `json:"hostname_intra_data"`
		Port                 int    `json:"port,string"`               // listening port
		PortIntraControl     int    `json:"port_intra_control,string"` // --/-- for intra-cluster control
		PortIntraData        int    `json:"port_intra_data,string"`    // --/-- for intra-cluster data
		// omit
		UseIntraControl bool `json:"-"`
		UseIntraData    bool `json:"-"`
	}

	// ais node: fspaths (a.k.a. mountpaths)
	FSPConf struct {
		Paths cos.StrSet `json:"paths,omitempty" list:"readonly"`
	}

	TestFSPConf struct {
		Root     string `json:"root"`
		Count    int    `json:"count"`
		Instance int    `json:"instance"`
	}
)

// global configuration
type (
	ClusterConfig struct {
		Ext        any            `json:"ext,omitempty"` // within meta-version extensions
		Backend    BackendConf    `json:"backend" allow:"cluster"`
		Mirror     MirrorConf     `json:"mirror" allow:"cluster"`
		EC         ECConf         `json:"ec" allow:"cluster"`
		Log        LogConf        `json:"log"`
		Periodic   PeriodConf     `json:"periodic"`
		Timeout    TimeoutConf    `json:"timeout"`
		Client     ClientConf     `json:"client"`
		Proxy      ProxyConf      `json:"proxy" allow:"cluster"`
		Space      SpaceConf      `json:"space"`
		LRU        LRUConf        `json:"lru"`
		Disk       DiskConf       `json:"disk"`
		Rebalance  RebalanceConf  `json:"rebalance" allow:"cluster"`
		Resilver   ResilverConf   `json:"resilver"`
		Cksum      CksumConf      `json:"checksum"`
		Versioning VersionConf    `json:"versioning" allow:"cluster"`
		Net        NetConf        `json:"net"`
		FSHC       FSHCConf       `json:"fshc"`
		Auth       AuthConf       `json:"auth"`
		Keepalive  KeepaliveConf  `json:"keepalivetracker"`
		Downloader DownloaderConf `json:"downloader"`
		Dsort      DsortConf      `json:"distributed_sort"`
		Transport  TransportConf  `json:"transport"`
		Memsys     MemsysConf     `json:"memsys"`

		// Transform (offline) or Copy src Bucket => dst bucket
		TCB TCBConf `json:"tcb"`

		// metadata write policy: (immediate | delayed | never)
		WritePolicy WritePolicyConf `json:"write_policy"`

		// standalone enumerated features that can be configured
		// to flip assorted global defaults (see cmn/feat/feat.go)
		Features feat.Flags `json:"features,string" allow:"cluster"`

		// read-only
		LastUpdated string `json:"lastupdate_time"`       // timestamp
		UUID        string `json:"uuid"`                  // UUID
		Version     int64  `json:"config_version,string"` // version
	}
	ConfigToSet struct {
		// ClusterConfig
		Backend     *BackendConf          `json:"backend,omitempty"`
		Mirror      *MirrorConfToSet      `json:"mirror,omitempty"`
		EC          *ECConfToSet          `json:"ec,omitempty"`
		Log         *LogConfToSet         `json:"log,omitempty"`
		Periodic    *PeriodConfToSet      `json:"periodic,omitempty"`
		Timeout     *TimeoutConfToSet     `json:"timeout,omitempty"`
		Client      *ClientConfToSet      `json:"client,omitempty"`
		Space       *SpaceConfToSet       `json:"space,omitempty"`
		LRU         *LRUConfToSet         `json:"lru,omitempty"`
		Disk        *DiskConfToSet        `json:"disk,omitempty"`
		Rebalance   *RebalanceConfToSet   `json:"rebalance,omitempty"`
		Resilver    *ResilverConfToSet    `json:"resilver,omitempty"`
		Cksum       *CksumConfToSet       `json:"checksum,omitempty"`
		Versioning  *VersionConfToSet     `json:"versioning,omitempty"`
		Net         *NetConfToSet         `json:"net,omitempty"`
		FSHC        *FSHCConfToSet        `json:"fshc,omitempty"`
		Auth        *AuthConfToSet        `json:"auth,omitempty"`
		Keepalive   *KeepaliveConfToSet   `json:"keepalivetracker,omitempty"`
		Downloader  *DownloaderConfToSet  `json:"downloader,omitempty"`
		Dsort       *DsortConfToSet       `json:"distributed_sort,omitempty"`
		Transport   *TransportConfToSet   `json:"transport,omitempty"`
		Memsys      *MemsysConfToSet      `json:"memsys,omitempty"`
		TCB         *TCBConfToSet         `json:"tcb,omitempty"`
		WritePolicy *WritePolicyConfToSet `json:"write_policy,omitempty"`
		Proxy       *ProxyConfToSet       `json:"proxy,omitempty"`
		Features    *feat.Flags           `json:"features,string,omitempty"`

		// LocalConfig
		FSP *FSPConf `json:"fspaths,omitempty"`
	}

	BackendConf struct {
		// provider implementation-dependent
		Conf map[string]any `json:"conf,omitempty"`
		// 3rd party Cloud(s) -- set during validation
		Providers map[string]Ns `json:"-"`
	}
	BackendConfHDFS struct {
		Addresses           []string `json:"addresses"`
		User                string   `json:"user"`
		UseDatanodeHostname bool     `json:"use_datanode_hostname"`
	}
	BackendConfAIS map[string][]string // cluster alias -> [urls...]

	MirrorConf struct {
		Copies  int64 `json:"copies"`       // num copies
		Burst   int   `json:"burst_buffer"` // xaction channel (buffer) size
		Enabled bool  `json:"enabled"`      // enabled (to generate copies)
	}
	MirrorConfToSet struct {
		Copies  *int64 `json:"copies,omitempty"`
		Burst   *int   `json:"burst_buffer,omitempty"`
		Enabled *bool  `json:"enabled,omitempty"`
	}

	ECConf struct {
		Compression string `json:"compression"` // enum { CompressAlways, ... } in api/apc/compression.go

		// ObjSizeLimit is object size threshold _separating_ intra-cluster mirroring from
		// erasure coding.
		//
		// The value 0 (zero) indicates that objects of any size
		// are to be sliced, to produce (D) data slices and (P) erasure coded parity slices.
		// On the other hand, the value -1 specifies that absolutely all objects of any size
		// must be replicated as is. In effect, the (-1) option provides data protection via
		// intra-cluster (P+1)-way replication (a.k.a. mirroring).
		//
		// In all cases, a given (D, P) configuration provides node-level redundancy,
		// whereby P nodes can be lost without incurring loss of data.
		ObjSizeLimit int64 `json:"objsize_limit"`

		// Number of data (D) slices; the value 1 will have an effect of producing
		// (P) additional full-size replicas.
		DataSlices int `json:"data_slices"`

		// Depending on the object size and `ObjSizeLimit`, the value of `ParitySlices` (or P) indicates:
		// - a number of additional parity slices (generated or _computed_ from the (D) data slices),
		// or:
		// - a number of full object replicas (copies).
		// In all cases, the same rule applies: all slices and/or all full copies are stored on different
		// storage nodes (a.k.a. targets).
		ParitySlices int `json:"parity_slices"`

		SbundleMult int `json:"bundle_multiplier"` // stream-bundle multiplier: num streams to destination

		Enabled  bool `json:"enabled"`   // EC is enabled
		DiskOnly bool `json:"disk_only"` // if true, EC does not use SGL - data goes directly to drives
	}
	ECConfToSet struct {
		ObjSizeLimit *int64  `json:"objsize_limit,omitempty"`
		Compression  *string `json:"compression,omitempty"`
		SbundleMult  *int    `json:"bundle_multiplier,omitempty"`
		DataSlices   *int    `json:"data_slices,omitempty"`
		ParitySlices *int    `json:"parity_slices,omitempty"`
		Enabled      *bool   `json:"enabled,omitempty"`
		DiskOnly     *bool   `json:"disk_only,omitempty"`
	}

	LogConf struct {
		Level     cos.LogLevel `json:"level"`      // log level (aka verbosity)
		MaxSize   cos.SizeIEC  `json:"max_size"`   // exceeding this size triggers log rotation
		MaxTotal  cos.SizeIEC  `json:"max_total"`  // (sum individual log sizes); exceeding this number triggers cleanup
		FlushTime cos.Duration `json:"flush_time"` // log flush interval
		StatsTime cos.Duration `json:"stats_time"` // (not used)
	}
	LogConfToSet struct {
		Level     *cos.LogLevel `json:"level,omitempty"`
		MaxSize   *cos.SizeIEC  `json:"max_size,omitempty"`
		MaxTotal  *cos.SizeIEC  `json:"max_total,omitempty"`
		FlushTime *cos.Duration `json:"flush_time,omitempty"`
		StatsTime *cos.Duration `json:"stats_time,omitempty"`
	}

	// NOTE: StatsTime is a one important timer
	PeriodConf struct {
		StatsTime     cos.Duration `json:"stats_time"`      // collect and publish stats; other house-keeping
		RetrySyncTime cos.Duration `json:"retry_sync_time"` // metasync retry
		NotifTime     cos.Duration `json:"notif_time"`      // (IC notifications)
	}
	PeriodConfToSet struct {
		StatsTime     *cos.Duration `json:"stats_time,omitempty"`
		RetrySyncTime *cos.Duration `json:"retry_sync_time,omitempty"`
		NotifTime     *cos.Duration `json:"notif_time,omitempty"`
	}

	// maximum intra-cluster latencies (in the increasing order)
	TimeoutConf struct {
		CplaneOperation cos.Duration `json:"cplane_operation"` // read-mostly via global cmn.Rom.CplaneOperation
		MaxKeepalive    cos.Duration `json:"max_keepalive"`    // ditto, cmn.Rom.MaxKeepalive - see below
		MaxHostBusy     cos.Duration `json:"max_host_busy"`
		Startup         cos.Duration `json:"startup_time"`
		JoinAtStartup   cos.Duration `json:"join_startup_time"` // (join cluster at startup) timeout
		SendFile        cos.Duration `json:"send_file_time"`
	}
	TimeoutConfToSet struct {
		CplaneOperation *cos.Duration `json:"cplane_operation,omitempty"`
		MaxKeepalive    *cos.Duration `json:"max_keepalive,omitempty"`
		MaxHostBusy     *cos.Duration `json:"max_host_busy,omitempty"`
		Startup         *cos.Duration `json:"startup_time,omitempty"`
		JoinAtStartup   *cos.Duration `json:"join_startup_time,omitempty"`
		SendFile        *cos.Duration `json:"send_file_time,omitempty"`
	}

	ClientConf struct {
		Timeout        cos.Duration `json:"client_timeout"`
		TimeoutLong    cos.Duration `json:"client_long_timeout"`
		ListObjTimeout cos.Duration `json:"list_timeout"`
	}
	ClientConfToSet struct {
		Timeout        *cos.Duration `json:"client_timeout,omitempty"` // readonly as far as intra-cluster
		TimeoutLong    *cos.Duration `json:"client_long_timeout,omitempty"`
		ListObjTimeout *cos.Duration `json:"list_timeout,omitempty"`
	}

	ProxyConf struct {
		PrimaryURL   string `json:"primary_url"`
		OriginalURL  string `json:"original_url"`
		DiscoveryURL string `json:"discovery_url"`
		NonElectable bool   `json:"non_electable"`
	}
	ProxyConfToSet struct {
		PrimaryURL   *string `json:"primary_url,omitempty"`
		OriginalURL  *string `json:"original_url,omitempty"`
		DiscoveryURL *string `json:"discovery_url,omitempty"`
		NonElectable *bool   `json:"non_electable,omitempty"`
	}

	SpaceConf struct {
		// Storage Cleanup watermark: used capacity (%) that triggers cleanup
		// (deleted objects and buckets, extra copies, etc.)
		CleanupWM int64 `json:"cleanupwm"`

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
	}
	SpaceConfToSet struct {
		CleanupWM *int64 `json:"cleanupwm,omitempty"`
		LowWM     *int64 `json:"lowwm,omitempty"`
		HighWM    *int64 `json:"highwm,omitempty"`
		OOS       *int64 `json:"out_of_space,omitempty"`
	}

	LRUConf struct {
		// DontEvictTimeStr denotes the period of time during which eviction of an object
		// is forbidden [atime, atime + DontEvictTime]
		DontEvictTime cos.Duration `json:"dont_evict_time"`

		// CapacityUpdTimeStr denotes the frequency at which AIStore updates local capacity utilization
		CapacityUpdTime cos.Duration `json:"capacity_upd_time"`

		// Enabled: LRU will only run when set to true
		Enabled bool `json:"enabled"`
	}
	LRUConfToSet struct {
		DontEvictTime   *cos.Duration `json:"dont_evict_time,omitempty"`
		CapacityUpdTime *cos.Duration `json:"capacity_upd_time,omitempty"`
		Enabled         *bool         `json:"enabled,omitempty"`
	}

	DiskConf struct {
		DiskUtilLowWM   int64        `json:"disk_util_low_wm"`  // no throttling below
		DiskUtilHighWM  int64        `json:"disk_util_high_wm"` // throttle longer when above
		DiskUtilMaxWM   int64        `json:"disk_util_max_wm"`
		IostatTimeLong  cos.Duration `json:"iostat_time_long"`
		IostatTimeShort cos.Duration `json:"iostat_time_short"`
	}
	DiskConfToSet struct {
		DiskUtilLowWM   *int64        `json:"disk_util_low_wm,omitempty"`
		DiskUtilHighWM  *int64        `json:"disk_util_high_wm,omitempty"`
		DiskUtilMaxWM   *int64        `json:"disk_util_max_wm,omitempty"`
		IostatTimeLong  *cos.Duration `json:"iostat_time_long,omitempty"`
		IostatTimeShort *cos.Duration `json:"iostat_time_short,omitempty"`
	}

	RebalanceConf struct {
		Compression   string       `json:"compression"`       // enum { CompressAlways, ... } in api/apc/compression.go
		DestRetryTime cos.Duration `json:"dest_retry_time"`   // max wait for ACKs & neighbors to complete
		SbundleMult   int          `json:"bundle_multiplier"` // stream-bundle multiplier: num streams to destination
		Enabled       bool         `json:"enabled"`           // true=auto-rebalance | manual rebalancing
	}
	RebalanceConfToSet struct {
		DestRetryTime *cos.Duration `json:"dest_retry_time,omitempty"`
		Compression   *string       `json:"compression,omitempty"`
		SbundleMult   *int          `json:"bundle_multiplier"`
		Enabled       *bool         `json:"enabled,omitempty"`
	}

	ResilverConf struct {
		Enabled bool `json:"enabled"` // true=auto-resilver | manual resilvering
	}
	ResilverConfToSet struct {
		Enabled *bool `json:"enabled,omitempty"`
	}

	CksumConf struct {
		// (note that `ChecksumNone` ("none") disables checksumming)
		Type string `json:"type"`

		// validate the checksum of the object that we cold-GET
		// or download from remote location (e.g., cloud bucket)
		ValidateColdGet bool `json:"validate_cold_get"`

		// validate object's version (if exists and provided) and its checksum -
		// if either value fail to match, the object is removed from ais.
		//
		// NOTE: object versioning is backend-specific and is may _not_ be supported by a given
		// (supported) backends - see docs for details.
		ValidateWarmGet bool `json:"validate_warm_get"`

		// determines whether to validate checksums of objects
		// migrated or replicated within the cluster
		ValidateObjMove bool `json:"validate_obj_move"`

		// EnableReadRange: Return read range checksum otherwise return entire object checksum.
		EnableReadRange bool `json:"enable_read_range"`
	}
	CksumConfToSet struct {
		Type            *string `json:"type,omitempty"`
		ValidateColdGet *bool   `json:"validate_cold_get,omitempty"`
		ValidateWarmGet *bool   `json:"validate_warm_get,omitempty"`
		ValidateObjMove *bool   `json:"validate_obj_move,omitempty"`
		EnableReadRange *bool   `json:"enable_read_range,omitempty"`
	}

	VersionConf struct {
		// Determines if versioning is enabled
		Enabled bool `json:"enabled"`

		// Validate remote version and, possibly, update in-cluster ("cached") copy.
		// Scenarios include (but are not limited to):
		// - warm GET
		// - prefetch bucket (*)
		// - prefetch multiple objects (see api/multiobj.go)
		// - copy bucket
		// Applies to Cloud and remote AIS buckets - generally, buckets that have remote backends
		// that in turn provide some form of object versioning.
		// (*) Xactions (ie., jobs) that read-access multiple objects (e.g., prefetch, copy-bucket)
		// may support operation-scope option to synchronize remote content (to aistore) - the option
		// not requiring changing bucket configuration.
		// See also:
		// - apc.QparamLatestVer, apc.PrefetchMsg, apc.CopyBckMsg
		ValidateWarmGet bool `json:"validate_warm_get"`

		// A stronger variant of the above that in addition entails:
		// - deleting in-cluster object if its remote ("cached") counterpart does not exist
		// See also: apc.QparamSync, apc.CopyBckMsg
		Sync bool `json:"synchronize"`
	}
	VersionConfToSet struct {
		Enabled         *bool `json:"enabled,omitempty"`
		ValidateWarmGet *bool `json:"validate_warm_get,omitempty"`
		Sync            *bool `json:"synchronize,omitempty"`
	}

	NetConf struct {
		L4   L4Conf   `json:"l4"`
		HTTP HTTPConf `json:"http"`
	}
	NetConfToSet struct {
		HTTP *HTTPConfToSet `json:"http,omitempty"`
	}

	L4Conf struct {
		Proto         string `json:"proto"`           // tcp, udp
		SndRcvBufSize int    `json:"sndrcv_buf_size"` // SO_RCVBUF and SO_SNDBUF
	}

	HTTPConf struct {
		Proto           string `json:"-"`                 // http or https (set depending on `UseHTTPS`)
		Certificate     string `json:"server_crt"`        // HTTPS: X509 certificate
		CertKey         string `json:"server_key"`        // HTTPS: X509 key
		ServerNameTLS   string `json:"domain_tls"`        // #6410
		ClientCA        string `json:"client_ca_tls"`     // #6410
		ClientAuthTLS   int    `json:"client_auth_tls"`   // #6410 tls.ClientAuthType enum
		WriteBufferSize int    `json:"write_buffer_size"` // http.Transport.WriteBufferSize; zero defaults to 4KB
		ReadBufferSize  int    `json:"read_buffer_size"`  // http.Transport.ReadBufferSize; ditto
		UseHTTPS        bool   `json:"use_https"`         // use HTTPS
		SkipVerifyCrt   bool   `json:"skip_verify"`       // skip X509 cert verification (used with self-signed certs)
		Chunked         bool   `json:"chunked_transfer"`  // (https://tools.ietf.org/html/rfc7230#page-36; not used since 02/23)
	}
	HTTPConfToSet struct {
		Certificate     *string `json:"server_crt,omitempty"`
		CertKey         *string `json:"server_key,omitempty"`
		ServerNameTLS   *string `json:"domain_tls,omitempty"`
		ClientCA        *string `json:"client_ca_tls,omitempty"`
		WriteBufferSize *int    `json:"write_buffer_size,omitempty" list:"readonly"`
		ReadBufferSize  *int    `json:"read_buffer_size,omitempty" list:"readonly"`
		ClientAuthTLS   *int    `json:"client_auth_tls,omitempty"`
		UseHTTPS        *bool   `json:"use_https,omitempty"`
		SkipVerifyCrt   *bool   `json:"skip_verify,omitempty"`
		Chunked         *bool   `json:"chunked_transfer,omitempty"`
	}

	FSHCConf struct {
		TestFileCount int  `json:"test_files"`  // number of files to read/write
		ErrorLimit    int  `json:"error_limit"` // exceeding err limit causes disabling mountpath
		Enabled       bool `json:"enabled"`
	}
	FSHCConfToSet struct {
		TestFileCount *int  `json:"test_files,omitempty"`
		ErrorLimit    *int  `json:"error_limit,omitempty"`
		Enabled       *bool `json:"enabled,omitempty"`
	}

	AuthConf struct {
		Secret  string `json:"secret"`
		Enabled bool   `json:"enabled"`
	}
	AuthConfToSet struct {
		Secret  *string `json:"secret,omitempty"`
		Enabled *bool   `json:"enabled,omitempty"`
	}

	// keepalive tracker
	KeepaliveTrackerConf struct {
		Name     string       `json:"name"`     // "heartbeat" (other enumerated values TBD)
		Interval cos.Duration `json:"interval"` // keepalive interval
		Factor   uint8        `json:"factor"`   // only average
	}
	KeepaliveTrackerConfToSet struct {
		Interval *cos.Duration `json:"interval,omitempty"`
		Name     *string       `json:"name,omitempty" list:"readonly"`
		Factor   *uint8        `json:"factor,omitempty"`
	}

	KeepaliveConf struct {
		Proxy       KeepaliveTrackerConf `json:"proxy"`  // how proxy tracks target keepalives
		Target      KeepaliveTrackerConf `json:"target"` // how target tracks primary proxies keepalives
		RetryFactor uint8                `json:"retry_factor"`
	}
	KeepaliveConfToSet struct {
		Proxy       *KeepaliveTrackerConfToSet `json:"proxy,omitempty"`
		Target      *KeepaliveTrackerConfToSet `json:"target,omitempty"`
		RetryFactor *uint8                     `json:"retry_factor,omitempty"`
	}

	DownloaderConf struct {
		Timeout cos.Duration `json:"timeout"`
	}
	DownloaderConfToSet struct {
		Timeout *cos.Duration `json:"timeout,omitempty"`
	}

	DsortConf struct {
		DuplicatedRecords   string       `json:"duplicated_records"`
		MissingShards       string       `json:"missing_shards"` // cmn.SupportedReactions enum
		EKMMalformedLine    string       `json:"ekm_malformed_line"`
		EKMMissingKey       string       `json:"ekm_missing_key"`
		DefaultMaxMemUsage  string       `json:"default_max_mem_usage"`
		CallTimeout         cos.Duration `json:"call_timeout"`
		DsorterMemThreshold string       `json:"dsorter_mem_threshold"`
		Compression         string       `json:"compression"`       // {CompressAlways,...} in api/apc/compression.go
		SbundleMult         int          `json:"bundle_multiplier"` // stream-bundle multiplier: num to destination
	}
	DsortConfToSet struct {
		DuplicatedRecords   *string       `json:"duplicated_records,omitempty"`
		MissingShards       *string       `json:"missing_shards,omitempty"`
		EKMMalformedLine    *string       `json:"ekm_malformed_line,omitempty"`
		EKMMissingKey       *string       `json:"ekm_missing_key,omitempty"`
		DefaultMaxMemUsage  *string       `json:"default_max_mem_usage,omitempty"`
		CallTimeout         *cos.Duration `json:"call_timeout,omitempty"`
		DsorterMemThreshold *string       `json:"dsorter_mem_threshold,omitempty"`
		Compression         *string       `json:"compression,omitempty"`
		SbundleMult         *int          `json:"bundle_multiplier,omitempty"`
	}

	TransportConf struct {
		MaxHeaderSize int `json:"max_header"`   // max transport header buffer (default=4K)
		Burst         int `json:"burst_buffer"` // num sends with no back pressure; see also AIS_STREAM_BURST_NUM
		// two no-new-transmissions durations:
		// * IdleTeardown: sender terminates the connection (to reestablish it upon the very first/next PDU)
		// * QuiesceTime:  safe to terminate or transition to the next (in re: rebalance) stage
		IdleTeardown cos.Duration `json:"idle_teardown"`
		QuiesceTime  cos.Duration `json:"quiescent"`
		// lz4
		// max uncompressed block size, one of [64K, 256K(*), 1M, 4M]
		// fastcompression.blogspot.com/2013/04/lz4-streaming-format-final.html
		LZ4BlockMaxSize  cos.SizeIEC `json:"lz4_block"`
		LZ4FrameChecksum bool        `json:"lz4_frame_checksum"`
	}
	TransportConfToSet struct {
		MaxHeaderSize    *int          `json:"max_header,omitempty" list:"readonly"`
		Burst            *int          `json:"burst_buffer,omitempty" list:"readonly"`
		IdleTeardown     *cos.Duration `json:"idle_teardown,omitempty"`
		QuiesceTime      *cos.Duration `json:"quiescent,omitempty"`
		LZ4BlockMaxSize  *cos.SizeIEC  `json:"lz4_block,omitempty"`
		LZ4FrameChecksum *bool         `json:"lz4_frame_checksum,omitempty"`
	}

	MemsysConf struct {
		MinFree        cos.SizeIEC  `json:"min_free"`
		DefaultBufSize cos.SizeIEC  `json:"default_buf"`
		SizeToGC       cos.SizeIEC  `json:"to_gc"`
		HousekeepTime  cos.Duration `json:"hk_time"`
		MinPctTotal    int          `json:"min_pct_total"`
		MinPctFree     int          `json:"min_pct_free"`
	}
	MemsysConfToSet struct {
		MinFree        *cos.SizeIEC  `json:"min_free,omitempty"`
		DefaultBufSize *cos.SizeIEC  `json:"default_buf,omitempty"`
		SizeToGC       *cos.SizeIEC  `json:"to_gc,omitempty"`
		HousekeepTime  *cos.Duration `json:"hk_time,omitempty"`
		MinPctTotal    *int          `json:"min_pct_total,omitempty"`
		MinPctFree     *int          `json:"min_pct_free,omitempty"`
	}

	TCBConf struct {
		Compression string `json:"compression"`       // enum { CompressAlways, ... } in api/apc/compression.go
		SbundleMult int    `json:"bundle_multiplier"` // stream-bundle multiplier: num streams to destination
	}
	TCBConfToSet struct {
		Compression *string `json:"compression,omitempty"`
		SbundleMult *int    `json:"bundle_multiplier,omitempty"`
	}

	WritePolicyConf struct {
		Data apc.WritePolicy `json:"data"`
		MD   apc.WritePolicy `json:"md"`
	}
	WritePolicyConfToSet struct {
		Data *apc.WritePolicy `json:"data,omitempty" list:"readonly"` // NOTE: NIY
		MD   *apc.WritePolicy `json:"md,omitempty"`
	}
)

// assorted named fields that require (cluster | node) restart for changes to make an effect
var ConfigRestartRequired = []string{"auth", "memsys", "net"}

// dsort
const (
	IgnoreReaction = "ignore"
	WarnReaction   = "warn"
	AbortReaction  = "abort"
)

var SupportedReactions = []string{IgnoreReaction, WarnReaction, AbortReaction}

//
// config meta-versioning & serialization
//

var (
	_ jsp.Opts = (*ClusterConfig)(nil)
	_ jsp.Opts = (*LocalConfig)(nil)
	_ jsp.Opts = (*ConfigToSet)(nil)
)

func _jspOpts() jsp.Options {
	opts := jsp.CCSign(MetaverConfig)
	opts.OldMetaverOk = 2
	return opts
}

func (*LocalConfig) JspOpts() jsp.Options   { return jsp.Plain() }
func (*ClusterConfig) JspOpts() jsp.Options { return _jspOpts() }
func (*ConfigToSet) JspOpts() jsp.Options   { return _jspOpts() }

// interface guard
var (
	_ Validator = (*BackendConf)(nil)
	_ Validator = (*CksumConf)(nil)
	_ Validator = (*LogConf)(nil)
	_ Validator = (*LRUConf)(nil)
	_ Validator = (*SpaceConf)(nil)
	_ Validator = (*MirrorConf)(nil)
	_ Validator = (*ECConf)(nil)
	_ Validator = (*VersionConf)(nil)
	_ Validator = (*KeepaliveConf)(nil)
	_ Validator = (*PeriodConf)(nil)
	_ Validator = (*TimeoutConf)(nil)
	_ Validator = (*ClientConf)(nil)
	_ Validator = (*RebalanceConf)(nil)
	_ Validator = (*ResilverConf)(nil)
	_ Validator = (*NetConf)(nil)
	_ Validator = (*HTTPConf)(nil)
	_ Validator = (*DownloaderConf)(nil)
	_ Validator = (*DsortConf)(nil)
	_ Validator = (*TransportConf)(nil)
	_ Validator = (*MemsysConf)(nil)
	_ Validator = (*TCBConf)(nil)
	_ Validator = (*WritePolicyConf)(nil)

	_ PropsValidator = (*CksumConf)(nil)
	_ PropsValidator = (*SpaceConf)(nil)
	_ PropsValidator = (*MirrorConf)(nil)
	_ PropsValidator = (*ECConf)(nil)
	_ PropsValidator = (*WritePolicyConf)(nil)

	_ json.Marshaler   = (*BackendConf)(nil)
	_ json.Unmarshaler = (*BackendConf)(nil)
	_ json.Marshaler   = (*FSPConf)(nil)
	_ json.Unmarshaler = (*FSPConf)(nil)
)

/////////////////////////////////////////////
// Config and its nested (Cluster | Local) //
/////////////////////////////////////////////

// main config validator
func (c *Config) Validate() error {
	if c.ConfigDir == "" {
		return errors.New("invalid confdir value (must be non-empty)")
	}
	if c.LogDir == "" {
		return errors.New("invalid log dir value (must be non-empty)")
	}

	// NOTE: These two validations require more context and so we call them explicitly;
	//       The rest all implement generic interface.
	if err := c.LocalConfig.HostNet.Validate(c); err != nil {
		return err
	}
	if err := c.LocalConfig.FSP.Validate(c); err != nil {
		return err
	}
	if err := c.LocalConfig.TestFSP.Validate(c); err != nil {
		return err
	}

	opts := IterOpts{VisitAll: true}
	return IterFields(c, vdate, opts)
}

func vdate(_ string, field IterField) (error, bool) {
	if v, ok := field.Value().(Validator); ok {
		if err := v.Validate(); err != nil {
			return err, false
		}
	}
	return nil, false
}

func (c *Config) SetRole(role string) {
	debug.Assert(role == apc.Target || role == apc.Proxy)
	c.role = role
}

func (c *Config) UpdateClusterConfig(updateConf *ConfigToSet, asType string) (err error) {
	err = c.ClusterConfig.Apply(updateConf, asType)
	if err != nil {
		return
	}
	return c.Validate()
}

// TestingEnv returns true if config is set to a development environment
// where a single local filesystem is partitioned between all (locally running)
// targets and is used for both local and Cloud buckets
// See also: `rom.testingEnv`
func (c *Config) TestingEnv() bool {
	return c.LocalConfig.TestingEnv()
}

///////////////////
// ClusterConfig //
///////////////////

func (c *ClusterConfig) Apply(updateConf *ConfigToSet, asType string) error {
	return copyProps(updateConf, c, asType)
}

func (c *ClusterConfig) String() string {
	if c == nil {
		return "Conf <nil>"
	}
	return fmt.Sprintf("Conf v%d[%s]", c.Version, c.UUID)
}

/////////////////
// LocalConfig //
/////////////////

func (c *LocalConfig) TestingEnv() bool {
	return c.TestFSP.Count > 0
}

func (c *LocalConfig) AddPath(mpath string) {
	debug.Assert(!c.TestingEnv())
	c.FSP.Paths.Set(mpath)
}

func (c *LocalConfig) DelPath(mpath string) {
	debug.Assert(!c.TestingEnv())
	c.FSP.Paths.Delete(mpath)
}

////////////////
// PeriodConf //
////////////////

func (c *PeriodConf) Validate() error {
	if c.StatsTime.D() < time.Second || c.StatsTime.D() > time.Minute {
		return fmt.Errorf("invalid periodic.stats_time=%s (expected range [1s, 1m])",
			c.StatsTime)
	}
	if c.RetrySyncTime.D() < 10*time.Millisecond || c.RetrySyncTime.D() > 10*time.Second {
		return fmt.Errorf("invalid periodic.retry_sync_time=%s (expected range [10ms, 10s])",
			c.StatsTime)
	}
	if c.NotifTime.D() < time.Second || c.NotifTime.D() > time.Minute {
		return fmt.Errorf("invalid periodic.notif_time=%s (expected range [1s, 1m])",
			c.StatsTime)
	}
	return nil
}

/////////////
// LogConf //
/////////////

func (c *LogConf) Validate() error {
	if err := c.Level.Validate(); err != nil {
		return err
	}
	if c.MaxSize < cos.KiB || c.MaxSize > cos.GiB {
		return fmt.Errorf("invalid log.max_size=%s (expected range [1KB, 1GB])", c.MaxSize)
	}
	if c.MaxTotal < cos.MiB || c.MaxTotal > 10*cos.GiB {
		return fmt.Errorf("invalid log.max_total=%s (expected range [1MB, 10GB])", c.MaxTotal)
	}
	if c.MaxSize > c.MaxTotal/2 {
		return fmt.Errorf("invalid log.max_total=%s, must be >= 2*(log.max_size=%s)", c.MaxTotal, c.MaxSize)
	}
	if c.FlushTime.D() > time.Hour {
		return fmt.Errorf("invalid log.flush_time=%s (expected range [0, 1h)", c.FlushTime)
	}
	if c.StatsTime.D() > 10*time.Minute {
		return fmt.Errorf("invalid log.stats_time=%s (expected range [log.stats_time, 10m])", c.StatsTime)
	}
	return nil
}

////////////////
// ClientConf //
////////////////

func (c *ClientConf) Validate() error {
	if j := c.Timeout.D(); j < time.Second || j > 2*time.Minute {
		return fmt.Errorf("invalid client.client_timeout=%s (expected range [1s, 2m])", j)
	}
	if j := c.TimeoutLong.D(); j < 30*time.Second || j < c.Timeout.D() || j > 30*time.Minute {
		return fmt.Errorf("invalid client.client_long_timeout=%s (expected range [30s, 30m])", j)
	}
	if j := c.ListObjTimeout.D(); j < 2*time.Second || j > 15*time.Minute {
		return fmt.Errorf("invalid client.list_timeout=%s (expected range [2s, 15m])", j)
	}
	return nil
}

/////////////////
// BackendConf //
/////////////////

func (c *BackendConf) keys() (v []string) {
	for k := range c.Conf {
		v = append(v, k)
	}
	return
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
		case apc.AIS:
			var aisConf BackendConfAIS
			if err := jsoniter.Unmarshal(b, &aisConf); err != nil {
				return fmt.Errorf("invalid cloud specification: %v", err)
			}
			for alias, urls := range aisConf {
				if len(urls) == 0 {
					return fmt.Errorf("no URL(s) to connect to remote AIS cluster %q", alias)
				}
			}
			c.Conf[provider] = aisConf
		case apc.HDFS:
			var hdfsConf BackendConfHDFS
			if err := jsoniter.Unmarshal(b, &hdfsConf); err != nil {
				return fmt.Errorf("invalid cloud specification: %v", err)
			}
			if len(hdfsConf.Addresses) == 0 {
				return errors.New("no addresses provided to HDFS NameNode")
			}

			// Check connectivity and filter out non-reachable addresses.
			reachableAddrs := hdfsConf.Addresses[:0]
			for _, address := range hdfsConf.Addresses {
				conn, err := net.DialTimeout("tcp", address, 5*time.Second)
				if err != nil {
					nlog.Warningf(
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
				return errors.New("no address provided to HDFS NameNode is reachable")
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
	case apc.AWS, apc.Azure, apc.GCP, apc.HDFS:
		ns = NsGlobal
	default:
		debug.Assert(false, "unknown backend provider "+provider)
	}
	if c.Providers == nil {
		c.Providers = map[string]Ns{}
	}
	c.Providers[provider] = ns
}

func (c *BackendConf) Get(provider string) (conf any) {
	if c, ok := c.Conf[provider]; ok {
		conf = c
	}
	return
}

func (c *BackendConf) Set(provider string, newConf any) {
	c.Conf[provider] = newConf
}

func (c *BackendConf) EqualClouds(o *BackendConf) bool {
	if len(o.Conf) != len(c.Conf) {
		return false
	}
	for k := range o.Conf {
		if _, ok := c.Conf[k]; !ok {
			return false
		}
	}
	return true
}

func (c *BackendConf) EqualRemAIS(o *BackendConf, sname string) bool {
	var oldRemotes, newRemotes BackendConfAIS
	oais, oko := o.Conf[apc.AIS]
	nais, okn := c.Conf[apc.AIS]
	if !oko && !okn {
		return true
	}
	if oko != okn {
		return false
	}
	erro := cos.MorphMarshal(oais, &oldRemotes)
	errn := cos.MorphMarshal(nais, &newRemotes)
	if erro != nil || errn != nil {
		nlog.Errorf("%s: failed to unmarshal remote AIS backends: %v, %v", sname, erro, errn)
		debug.AssertNoErr(errn)
		return errn != nil // "equal" when cannot make use
	}
	if len(oldRemotes) != len(newRemotes) {
		return false
	}
	for k := range oldRemotes {
		if _, ok := newRemotes[k]; !ok {
			return false
		}
	}
	return true
}

func (c BackendConfAIS) String() (s string) {
	for a, urls := range c {
		if s != "" {
			s += "; "
		}
		s += fmt.Sprintf("[%s => %v]", a, urls)
	}
	return
}

//////////////
// DiskConf //
//////////////

func (c *DiskConf) Validate() (err error) {
	lwm, hwm, maxwm := c.DiskUtilLowWM, c.DiskUtilHighWM, c.DiskUtilMaxWM
	if lwm <= 0 || hwm <= lwm || maxwm <= hwm || maxwm > 100 {
		return fmt.Errorf("invalid (disk_util_lwm, disk_util_hwm, disk_util_maxwm) config %+v", c)
	}
	if c.IostatTimeLong <= 0 {
		return errors.New("disk.iostat_time_long is zero")
	}
	if c.IostatTimeShort <= 0 {
		return errors.New("disk.iostat_time_short is zero")
	}
	if c.IostatTimeLong < c.IostatTimeShort {
		return fmt.Errorf("disk.iostat_time_long %v shorter than disk.iostat_time_short %v",
			c.IostatTimeLong, c.IostatTimeShort)
	}
	return nil
}

///////////////
// SpaceConf //
///////////////

func (c *SpaceConf) Validate() (err error) {
	if c.CleanupWM <= 0 || c.LowWM < c.CleanupWM || c.HighWM < c.LowWM || c.OOS < c.HighWM || c.OOS > 100 {
		err = fmt.Errorf("invalid %s (expecting: 0 < cleanup < low < high < OOS < 100)", c)
	}
	return
}

func (c *SpaceConf) ValidateAsProps(...any) error { return c.Validate() }

func (c *SpaceConf) String() string {
	return fmt.Sprintf("space config: cleanup=%d%%, low=%d%%, high=%d%%, OOS=%d%%",
		c.CleanupWM, c.LowWM, c.HighWM, c.OOS)
}

/////////////
// LRUConf //
/////////////

func (c *LRUConf) String() string {
	if !c.Enabled {
		return "Disabled"
	}
	return fmt.Sprintf("lru.dont_evict_time=%v, lru.capacity_upd_time=%v", c.DontEvictTime, c.CapacityUpdTime)
}

func (c *LRUConf) Validate() (err error) {
	if c.CapacityUpdTime.D() < 10*time.Second {
		err = fmt.Errorf("invalid %s (expecting: lru.capacity_upd_time >= 10s)", c)
	}
	return
}

///////////////
// CksumConf //
///////////////

func (c *CksumConf) Validate() (err error) {
	return cos.ValidateCksumType(c.Type)
}

func (c *CksumConf) ValidateAsProps(...any) (err error) {
	return c.Validate()
}

func (c *CksumConf) String() string {
	if c.Type == cos.ChecksumNone {
		return "Disabled"
	}

	toValidate := make([]string, 0)
	add := func(val bool, name string) {
		if val {
			toValidate = append(toValidate, name)
		}
	}
	add(c.ValidateColdGet, "ColdGET")
	add(c.ValidateWarmGet, "WarmGET")
	add(c.ValidateObjMove, "ObjectMove")
	add(c.EnableReadRange, "ReadRange")

	toValidateStr := "Nothing"
	if len(toValidate) > 0 {
		toValidateStr = strings.Join(toValidate, ",")
	}

	return fmt.Sprintf("Type: %s | Validate: %s", c.Type, toValidateStr)
}

/////////////////
// VersionConf //
/////////////////

func (c *VersionConf) Validate() error {
	if !c.Enabled && c.ValidateWarmGet {
		return errors.New("versioning.validate_warm_get requires versioning to be enabled")
	}
	return nil
}

func (c *VersionConf) String() string {
	if !c.Enabled {
		return "Disabled"
	}

	text := "Enabled | Validate on WarmGET: "
	if c.ValidateWarmGet {
		text += "yes"
	} else {
		text += "no"
	}

	return text
}

////////////////
// MirrorConf //
////////////////

func (c *MirrorConf) Validate() error {
	if c.Burst < 0 {
		return fmt.Errorf("invalid mirror.burst_buffer: %v (expected >0)", c.Burst)
	}
	if c.Copies < 2 || c.Copies > 32 {
		return fmt.Errorf("invalid mirror.copies: %d (expected value in range [2, 32])", c.Copies)
	}
	return nil
}

func (c *MirrorConf) ValidateAsProps(...any) error {
	if !c.Enabled {
		return nil
	}
	return c.Validate()
}

func (c *MirrorConf) String() string {
	if !c.Enabled {
		return "Disabled"
	}

	return fmt.Sprintf("%d copies", c.Copies)
}

////////////
// ECConf //
////////////

const (
	ObjSizeToAlwaysReplicate = -1 // (see `ObjSizeLimit` comment above)

	minSliceCount = 1  // minimum number of data or parity slices
	maxSliceCount = 32 // maximum --/--
)

func (c *ECConf) Validate() error {
	if c.ObjSizeLimit < -1 {
		return fmt.Errorf("invalid ec.obj_size_limit: %d (expecting greater or equal -1)", c.ObjSizeLimit)
	}
	if c.DataSlices < minSliceCount || c.DataSlices > maxSliceCount {
		err := fmt.Errorf("invalid ec.data_slices: %d (expected value in range [%d, %d])",
			c.DataSlices, minSliceCount, maxSliceCount)
		return err
	}
	if c.ParitySlices < minSliceCount || c.ParitySlices > maxSliceCount {
		return fmt.Errorf("invalid ec.parity_slices: %d (expected value in range [%d, %d])",
			c.ParitySlices, minSliceCount, maxSliceCount)
	}
	if c.SbundleMult < 0 || c.SbundleMult > 16 {
		return fmt.Errorf("invalid ec.bundle_multiplier: %v (expected range [0, 16])", c.SbundleMult)
	}
	if !apc.IsValidCompression(c.Compression) {
		return fmt.Errorf("invalid ec.compression: %q (expecting one of: %v)", c.Compression, apc.SupportedCompression)
	}
	return nil
}

func (c *ECConf) ValidateAsProps(arg ...any) (err error) {
	if !c.Enabled {
		return
	}
	if err = c.Validate(); err != nil {
		return
	}
	targetCnt, ok := arg[0].(int)
	debug.Assert(ok)
	required := c.numRequiredTargets()
	if required <= targetCnt {
		return
	}

	err = fmt.Errorf("%v: EC configuration (D = %d, P = %d) requires at least %d targets (have %d)",
		ErrNotEnoughTargets, c.DataSlices, c.ParitySlices, required, targetCnt)
	if c.ObjSizeLimit == ObjSizeToAlwaysReplicate || c.ParitySlices > targetCnt {
		return
	}
	return NewErrSoft(err.Error())
}

func (c *ECConf) String() string {
	if !c.Enabled {
		return "Disabled"
	}
	objSizeLimit := c.ObjSizeLimit
	if objSizeLimit == ObjSizeToAlwaysReplicate {
		return fmt.Sprintf("no EC - always producing %d total replicas", c.ParitySlices+1)
	}
	return fmt.Sprintf("%d:%d (objsize limit %s)", c.DataSlices, c.ParitySlices, cos.ToSizeIEC(objSizeLimit, 0))
}

func (c *ECConf) numRequiredTargets() int {
	if c.ObjSizeLimit == ObjSizeToAlwaysReplicate {
		return c.ParitySlices + 1
	}
	// (data slices + parity slices + 1 target for the _main_ replica)
	return c.DataSlices + c.ParitySlices + 1
}

func (c *ECConf) RequiredRestoreTargets() int {
	return c.DataSlices
}

/////////////////////
// WritePolicyConf //
/////////////////////

func (c *WritePolicyConf) Validate() (err error) {
	err = c.Data.Validate()
	if err == nil {
		if !c.Data.IsImmediate() {
			return fmt.Errorf("invalid write policy for data: %q not implemented yet", c.Data)
		}
		err = c.MD.Validate()
	}
	return
}

func (c *WritePolicyConf) ValidateAsProps(...any) error { return c.Validate() }

///////////////////
// KeepaliveConf //
///////////////////

func (c *KeepaliveConf) Validate() (err error) {
	if c.Proxy.Name != "heartbeat" {
		err = fmt.Errorf("invalid keepalivetracker.proxy.name %s", c.Proxy.Name)
	} else if c.Target.Name != "heartbeat" {
		err = fmt.Errorf("invalid keepalivetracker.target.name %s", c.Target.Name)
	} else if c.RetryFactor < 1 || c.RetryFactor > 10 {
		err = fmt.Errorf("invalid keepalivetracker.retry_factor %d (expecting 1 thru 10)", c.RetryFactor)
	}
	return
}

func KeepaliveRetryDuration(c *Config) time.Duration {
	d := c.Timeout.CplaneOperation.D() * time.Duration(c.Keepalive.RetryFactor)
	return min(d, c.Timeout.MaxKeepalive.D()+time.Second/2)
}

/////////////
// NetConf //
/////////////

func (c *NetConf) Validate() (err error) {
	if c.L4.Proto != "tcp" {
		return fmt.Errorf("l4 proto %q is not recognized (expecting %s)", c.L4.Proto, "tcp")
	}
	c.HTTP.Proto = "http" // not validating: read-only, and can take only two values
	if c.HTTP.UseHTTPS {
		c.HTTP.Proto = "https"
	}
	if c.HTTP.ClientAuthTLS < int(tls.NoClientCert) || c.HTTP.ClientAuthTLS > int(tls.RequireAndVerifyClientCert) {
		return fmt.Errorf("invalid client_auth_tls %d (expecting range [0 - %d])", c.HTTP.ClientAuthTLS,
			tls.RequireAndVerifyClientCert)
	}
	return nil
}

func (c *HTTPConf) Validate() error {
	if c.ServerNameTLS != "" {
		return fmt.Errorf("invalid domain_tls %q: expecting empty (domain names/SANs should be set in X.509 cert)", c.ServerNameTLS)
	}
	return nil
}

// used intra-clients; see related: EnvToTLS()
func (c *HTTPConf) ToTLS() TLSArgs {
	return TLSArgs{
		Certificate: c.Certificate,
		Key:         c.CertKey,
		ClientCA:    c.ClientCA,
		SkipVerify:  c.SkipVerifyCrt,
	}
}

////////////////////
// LocalNetConfig //
////////////////////

const HostnameListSepa = ","

func (c *LocalNetConfig) Validate(contextConfig *Config) (err error) {
	c.Hostname = strings.ReplaceAll(c.Hostname, " ", "")
	c.HostnameIntraControl = strings.ReplaceAll(c.HostnameIntraControl, " ", "")
	c.HostnameIntraData = strings.ReplaceAll(c.HostnameIntraData, " ", "")

	if addr, over := ipsOverlap(c.Hostname, c.HostnameIntraControl); over {
		return fmt.Errorf("public (%s) and intra-cluster control (%s) share the same: %q",
			c.Hostname, c.HostnameIntraControl, addr)
	}
	if addr, over := ipsOverlap(c.Hostname, c.HostnameIntraData); over {
		return fmt.Errorf("public (%s) and intra-cluster data (%s) share the same: %q",
			c.Hostname, c.HostnameIntraData, addr)
	}
	if addr, over := ipsOverlap(c.HostnameIntraControl, c.HostnameIntraData); over {
		if ipv4ListsEqual(c.HostnameIntraControl, c.HostnameIntraData) {
			nlog.Warningln("control and data share the same intra-cluster network:", c.HostnameIntraData)
		} else {
			nlog.Warningf("intra-cluster control (%s) and data (%s) share the same: %q",
				c.HostnameIntraControl, c.HostnameIntraData, addr)
		}
	}

	// Parse ports
	if _, err := ValidatePort(c.Port); err != nil {
		return fmt.Errorf("invalid %s port specified: %v", NetPublic, err)
	}
	if c.PortIntraControl != 0 {
		if _, err := ValidatePort(c.PortIntraControl); err != nil {
			return fmt.Errorf("invalid %s port specified: %v", NetIntraControl, err)
		}
	}
	if c.PortIntraData != 0 {
		if _, err := ValidatePort(c.PortIntraData); err != nil {
			return fmt.Errorf("invalid %s port specified: %v", NetIntraData, err)
		}
	}

	// NOTE: intra-cluster networks
	differentIPs := c.Hostname != c.HostnameIntraControl
	differentPorts := c.Port != c.PortIntraControl
	c.UseIntraControl = (contextConfig.TestingEnv() || c.HostnameIntraControl != "") &&
		c.PortIntraControl != 0 && (differentIPs || differentPorts)

	differentIPs = c.Hostname != c.HostnameIntraData
	differentPorts = c.Port != c.PortIntraData
	c.UseIntraData = (contextConfig.TestingEnv() || c.HostnameIntraData != "") &&
		c.PortIntraData != 0 && (differentIPs || differentPorts)
	return
}

///////////////
// DsortConf //
///////////////

func (c *DsortConf) Validate() (err error) {
	if c.SbundleMult < 0 || c.SbundleMult > 16 {
		return fmt.Errorf("invalid distributed_sort.bundle_multiplier: %v (expected range [0, 16])", c.SbundleMult)
	}
	if !apc.IsValidCompression(c.Compression) {
		return fmt.Errorf("invalid distributed_sort.compression: %q (expecting one of: %v)",
			c.Compression, apc.SupportedCompression)
	}
	return c.ValidateWithOpts(false)
}

func (c *DsortConf) ValidateWithOpts(allowEmpty bool) (err error) {
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
	}
	if _, err := cos.ParseSize(c.DsorterMemThreshold, cos.UnitsIEC); err != nil && (!allowEmpty || c.DsorterMemThreshold != "") {
		return fmt.Errorf("invalid distributed_sort.dsorter_mem_threshold: %s (err: %s)",
			c.DsorterMemThreshold, err)
	}
	return nil
}

/////////////
// FSPConf //
/////////////

func (c *FSPConf) UnmarshalJSON(data []byte) (err error) {
	m := cos.NewStrSet()
	err = jsoniter.Unmarshal(data, &m)
	if err != nil {
		return
	}
	c.Paths = m
	return
}

func (c *FSPConf) MarshalJSON() (data []byte, err error) {
	return cos.MustMarshal(c.Paths), nil
}

func (c *FSPConf) Validate(contextConfig *Config) error {
	debug.Assertf(cos.StringInSlice(contextConfig.role, []string{apc.Proxy, apc.Target}),
		"unexpected role: %q", contextConfig.role)

	// Don't validate in testing environment.
	if contextConfig.TestingEnv() || contextConfig.role != apc.Target {
		return nil
	}
	if len(c.Paths) == 0 {
		return NewErrInvalidFSPathsConf(ErrNoMountpaths)
	}

	cleanMpaths := make(map[string]struct{})
	for fspath := range c.Paths {
		mpath, err := ValidateMpath(fspath)
		if err != nil {
			return err
		}
		l := len(mpath)
		// disallow mountpath nesting
		for mpath2 := range cleanMpaths {
			if mpath2 == mpath {
				err := fmt.Errorf("%q (%q) is duplicated", mpath, fspath)
				return NewErrInvalidFSPathsConf(err)
			}
			if err := IsNestedMpath(mpath, l, mpath2); err != nil {
				return NewErrInvalidFSPathsConf(err)
			}
		}
		cleanMpaths[mpath] = struct{}{}
	}
	c.Paths = cleanMpaths
	return nil
}

func IsNestedMpath(a string, la int, b string) (err error) {
	const fmterr = "mountpath nesting is not permitted: %q contains %q"
	lb := len(b)
	if la > lb {
		if a[0:lb] == b && a[lb] == filepath.Separator {
			err = fmt.Errorf(fmterr, a, b)
		}
	} else if la < lb {
		if b[0:la] == a && b[la] == filepath.Separator {
			err = fmt.Errorf(fmterr, b, a)
		}
	}
	return
}

/////////////////
// TestFSPConf //
/////////////////

// validate root and (NOTE: testing only) generate and fill-in counted FSP.Paths
func (c *TestFSPConf) Validate(contextConfig *Config) (err error) {
	// Don't validate in production environment.
	if !contextConfig.TestingEnv() || contextConfig.role != apc.Target {
		return nil
	}

	cleanMpath, err := ValidateMpath(c.Root)
	if err != nil {
		return err
	}
	c.Root = cleanMpath

	contextConfig.FSP.Paths = make(cos.StrSet, c.Count)
	for i := 0; i < c.Count; i++ {
		mpath := filepath.Join(c.Root, fmt.Sprintf("mp%d", i+1))
		if c.Instance > 0 {
			mpath = filepath.Join(mpath, strconv.Itoa(c.Instance))
		}
		contextConfig.FSP.Paths.Set(mpath)
	}
	return nil
}

func (c *TestFSPConf) ValidateMpath(p string) (err error) {
	debug.Assert(c.Count > 0)
	for i := 0; i < c.Count; i++ {
		mpath := filepath.Join(c.Root, fmt.Sprintf("mp%d", i+1))
		if c.Instance > 0 {
			mpath = filepath.Join(mpath, strconv.Itoa(c.Instance))
		}
		if strings.HasPrefix(p, mpath) {
			return
		}
	}
	err = fmt.Errorf("%q does not appear to be a valid testing mountpath, where (root=%q, count=%d)",
		p, c.Root, c.Count)
	return
}

// common mountpath validation (NOTE: calls filepath.Clean() every time)
func ValidateMpath(mpath string) (string, error) {
	cleanMpath := filepath.Clean(mpath)

	if cleanMpath[0] != filepath.Separator {
		return mpath, NewErrInvalidaMountpath(mpath, "mountpath must be an absolute path")
	}
	if cleanMpath == string(filepath.Separator) {
		return "", NewErrInvalidaMountpath(mpath, "root directory is not a valid mountpath")
	}
	return cleanMpath, nil
}

////////////////
// MemsysConf //
////////////////

func (c *MemsysConf) Validate() (err error) {
	if c.MinFree > 0 && c.MinFree < 100*cos.MiB {
		return fmt.Errorf("invalid memsys.min_free %s (cannot be less than 100MB, optimally at least 2GB)", c.MinFree)
	}
	if c.DefaultBufSize > 128*cos.KiB {
		return fmt.Errorf("invalid memsys.default_buf %s (must be a multiple of 4KB in range [4KB, 128KB]", c.DefaultBufSize)
	}
	if c.DefaultBufSize%(4*cos.KiB) != 0 {
		return fmt.Errorf("memsys.default_buf %s must a multiple of 4KB", c.DefaultBufSize)
	}
	if c.SizeToGC > cos.TiB {
		return fmt.Errorf("invalid memsys.to_gc %s (expected range [0, 1TB))", c.SizeToGC)
	}
	if c.HousekeepTime.D() > time.Hour {
		return fmt.Errorf("invalid memsys.hk_time %s (expected range [0, 1h))", c.HousekeepTime)
	}
	if c.MinPctTotal < 0 || c.MinPctTotal > 95 {
		return fmt.Errorf("invalid memsys.min_pct_total %d%%", c.MinPctTotal)
	}
	if c.MinPctFree < 0 || c.MinPctFree > 95 {
		return fmt.Errorf("invalid memsys.min_pct_free %d%%", c.MinPctFree)
	}
	return nil
}

///////////////////
// TransportConf //
///////////////////

// NOTE: uncompressed block sizes - the enum currently supported by the github.com/pierrec/lz4
func (c *TransportConf) Validate() (err error) {
	if c.LZ4BlockMaxSize != 64*cos.KiB && c.LZ4BlockMaxSize != 256*cos.KiB &&
		c.LZ4BlockMaxSize != cos.MiB && c.LZ4BlockMaxSize != 4*cos.MiB {
		return fmt.Errorf("invalid transport.block_size %s (expected one of: [64K, 256K, 1MB, 4MB])",
			c.LZ4BlockMaxSize)
	}
	if c.Burst < 0 {
		return fmt.Errorf("invalid transport.burst_buffer: %v (expected >0)", c.Burst)
	}
	if c.MaxHeaderSize < 0 {
		return fmt.Errorf("invalid transport.max_header: %v (expected >0)", c.MaxHeaderSize)
	}
	if c.IdleTeardown.D() < time.Second {
		return fmt.Errorf("invalid transport.idle_teardown: %v (expected >= 1s)", c.IdleTeardown)
	}
	if c.QuiesceTime.D() < 8*time.Second {
		return fmt.Errorf("invalid transport.quiescent: %v (expected >= 8s)", c.QuiesceTime)
	}
	if c.MaxHeaderSize > 0 && c.MaxHeaderSize < 512 {
		return fmt.Errorf("invalid transport.max_header: %v (expected >= 512)", c.MaxHeaderSize)
	}
	return nil
}

/////////////
// TCBConf //
/////////////

func (c *TCBConf) Validate() error {
	if c.SbundleMult < 0 || c.SbundleMult > 16 {
		return fmt.Errorf("invalid tcb.bundle_multiplier: %v (expected range [0, 16])", c.SbundleMult)
	}
	if !apc.IsValidCompression(c.Compression) {
		return fmt.Errorf("invalid tcb.compression: %q (expecting one of: %v)",
			c.Compression, apc.SupportedCompression)
	}
	return nil
}

/////////////////
// TimeoutConf //
/////////////////

func (c *TimeoutConf) Validate() error {
	if c.CplaneOperation.D() < 10*time.Millisecond {
		return fmt.Errorf("invalid timeout.cplane_operation=%s", c.CplaneOperation)
	}
	if c.MaxKeepalive < 2*c.CplaneOperation {
		return fmt.Errorf("invalid timeout.max_keepalive=%s, must be >= 2*(cplane_operation=%s)",
			c.MaxKeepalive, c.CplaneOperation)
	}
	if c.MaxHostBusy.D() < 10*time.Second {
		return fmt.Errorf("invalid timeout.max_host_busy=%s (cannot be less than 10s)", c.MaxHostBusy)
	}
	if c.Startup.D() < 30*time.Second {
		return fmt.Errorf("invalid timeout.startup_time=%s (cannot be less than 30s)", c.Startup)
	}
	if c.JoinAtStartup != 0 && c.JoinAtStartup < 2*c.Startup {
		return fmt.Errorf("invalid timeout.join_startup_time=%s, must be >= 2*(timeout.startup_time=%s)",
			c.JoinAtStartup, c.Startup)
	}
	if c.SendFile.D() < time.Minute {
		return fmt.Errorf("invalid timeout.send_file_time=%s (cannot be less than 1m)", c.SendFile)
	}
	return nil
}

////////////////////
// DownloaderConf //
////////////////////

func (c *DownloaderConf) Validate() error {
	if j := c.Timeout.D(); j < time.Second || j > time.Hour {
		return fmt.Errorf("invalid downloader.timeout=%s (expected range [1s, 1h])", j)
	}
	return nil
}

///////////////////
// RebalanceConf //
///////////////////

func (c *RebalanceConf) Validate() error {
	if j := c.DestRetryTime.D(); j < time.Second || j > 10*time.Minute {
		return fmt.Errorf("invalid rebalance.dest_retry_time=%s (expected range [1s, 10m])", j)
	}
	if c.SbundleMult < 0 || c.SbundleMult > 16 {
		return fmt.Errorf("invalid rebalance.bundle_multiplier: %v (expected range [0, 16])", c.SbundleMult)
	}
	if !apc.IsValidCompression(c.Compression) {
		return fmt.Errorf("invalid rebalance.compression: %q (expecting one of: %v)",
			c.Compression, apc.SupportedCompression)
	}
	return nil
}

func (c *RebalanceConf) String() string {
	if c.Enabled {
		return "Enabled"
	}
	return "Disabled"
}

func (*ResilverConf) Validate() error { return nil }

func (c *ResilverConf) String() string {
	if c.Enabled {
		return "Enabled"
	}
	return "Disabled"
}

////////////////////
// ConfigToSet //
////////////////////

// FillFromQuery populates ConfigToSet from URL query values
func (ctu *ConfigToSet) FillFromQuery(query url.Values) error {
	var anyExists bool
	for key := range query {
		if key == apc.ActTransient {
			continue
		}
		anyExists = true
		name, value := strings.ToLower(key), query.Get(key)
		if err := UpdateFieldValue(ctu, name, value); err != nil {
			return err
		}
	}

	if !anyExists {
		return errors.New("no properties to update")
	}
	return nil
}

func (ctu *ConfigToSet) Merge(update *ConfigToSet) {
	mergeProps(update, ctu)
}

// FillFromKVS populates `ConfigToSet` from key value pairs of the form `key=value`
func (ctu *ConfigToSet) FillFromKVS(kvs []string) (err error) {
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

// checks if the two comma-separated IPv4 address lists contain at least one common IPv4
func ipsOverlap(alist, blist string) (addr string, overlap bool) {
	if alist == "" || blist == "" {
		return
	}
	alistAddrs := strings.Split(alist, HostnameListSepa)
	for _, a := range alistAddrs {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		if strings.Contains(blist, a) {
			return a, true
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

// is called at startup
func LoadConfig(globalConfPath, localConfPath, daeRole string, config *Config) error {
	debug.Assert(globalConfPath != "" && localConfPath != "")
	GCO.SetInitialGconfPath(globalConfPath)

	// first, local config
	if _, err := jsp.LoadMeta(localConfPath, &config.LocalConfig); err != nil {
		return fmt.Errorf("failed to load plain-text local config %q: %v", localConfPath, err)
	}
	nlog.SetLogDirRole(config.LogDir, daeRole)

	// Global (aka Cluster) config
	// Normally, when the node is being deployed the very first time the last updated version
	// of the config doesn't exist.
	// In this case, we load the initial plain-text global config from the command-line/environment
	// specified `globalConfPath`.
	// Once started, the node then always relies on the last updated version stored in a binary
	// form (in accordance with the associated ClusterConfig.JspOpts()).
	globalFpath := filepath.Join(config.ConfigDir, fname.GlobalConfig)
	if _, err := jsp.LoadMeta(globalFpath, &config.ClusterConfig); err != nil {
		if !os.IsNotExist(err) {
			if _, ok := err.(*jsp.ErrUnsupportedMetaVersion); ok {
				nlog.Errorf(FmtErrBackwardCompat, err)
			}
			return fmt.Errorf("failed to load global config %q: %v", globalConfPath, err)
		}

		// initial plain-text
		const itxt = "load initial global config"
		nlog.Warningf("%s %q", itxt, globalConfPath)
		_, err = jsp.Load(globalConfPath, &config.ClusterConfig, jsp.Plain())
		if err != nil {
			return fmt.Errorf("failed to %s %q: %v", itxt, globalConfPath, err)
		}
		debug.Assert(config.Version == 0)
		globalFpath = globalConfPath
	} else {
		debug.Assert(config.Version > 0 && config.UUID != "")
	}

	// initialize read-mostly (rom) config
	Rom.Set(&config.ClusterConfig)
	Rom.testingEnv = config.TestingEnv()

	config.SetRole(daeRole)

	// override config - locally updated global defaults
	if err := handleOverrideConfig(config); err != nil {
		return err
	}

	// create dirs
	if err := cos.CreateDir(config.LogDir); err != nil {
		return fmt.Errorf("failed to create log dir %q: %v", config.LogDir, err)
	}
	if config.TestingEnv() && daeRole == apc.Target {
		debug.Assert(config.TestFSP.Count == len(config.FSP.Paths))
		for mpath := range config.FSP.Paths {
			if err := cos.CreateDir(mpath); err != nil {
				return fmt.Errorf("failed to create %s mountpath in testing env: %v", mpath, err)
			}
		}
	}

	// rotate log
	nlog.MaxSize = int64(config.Log.MaxSize)
	if nlog.MaxSize > cos.GiB {
		nlog.Warningf("log.max_size %d exceeds 1GB, setting log.max_size=4MB", nlog.MaxSize)
		nlog.MaxSize = 4 * cos.MiB
	}
	// log header
	nlog.Infof("log.dir: %q; l4.proto: %s; pub port: %d; verbosity: %s",
		config.LogDir, config.Net.L4.Proto, config.HostNet.Port, config.Log.Level.String())
	nlog.Infof("config: %q; stats_time: %v; authentication: %t; backends: %v",
		globalFpath, config.Periodic.StatsTime, config.Auth.Enabled, config.Backend.keys())
	return nil
}

func handleOverrideConfig(config *Config) error {
	overrideConfig, err := loadOverrideConfig(config.ConfigDir)
	if err != nil {
		return err
	}
	if overrideConfig == nil {
		return config.Validate() // always validate
	}

	// update config with locally-stored 'OverrideConfigFname' and validate the result
	GCO.PutOverride(overrideConfig)
	if overrideConfig.FSP != nil {
		config.LocalConfig.FSP = *overrideConfig.FSP // override local config's fspaths
		overrideConfig.FSP = nil
	}
	return config.UpdateClusterConfig(overrideConfig, apc.Daemon)
}

func SaveOverrideConfig(configDir string, toUpdate *ConfigToSet) error {
	return jsp.SaveMeta(path.Join(configDir, fname.OverrideConfig), toUpdate, nil)
}

func loadOverrideConfig(configDir string) (toUpdate *ConfigToSet, err error) {
	toUpdate = &ConfigToSet{}
	_, err = jsp.LoadMeta(path.Join(configDir, fname.OverrideConfig), toUpdate)
	if os.IsNotExist(err) {
		err = nil
	}
	return
}

func ValidateRemAlias(alias string) (err error) {
	if alias == apc.QparamWhat {
		return fmt.Errorf("cannot use %q as an alias", apc.QparamWhat)
	}
	if len(alias) < 2 {
		err = fmt.Errorf("alias %q is too short: must have at least 2 letters", alias)
	} else if !cos.IsAlphaPlus(alias) {
		err = fmt.Errorf("alias %q is invalid: use only letters, numbers, dashes (-), and underscores (_)", alias)
	}
	return
}
