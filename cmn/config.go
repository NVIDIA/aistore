// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
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
	"github.com/NVIDIA/aistore/hk"

	jsoniter "github.com/json-iterator/go"
)

const (
	confDisabled = "Disabled"
)

type (
	validator interface {
		Validate() error
	}
	contextValidator interface {
		Validate(*Config) error
	}
	propsValidator interface {
		ValidateAsProps(arg ...any) error
	}
)

// Config is a single control structure (persistent and versioned)
// that contains both cluster (global) and node (local) configuration
// Naming convention for setting/getting values: (parent section json tag . child json tag)
// See also: `IterFields`, `IterFieldNameSepa`
type (
	Config struct {
		role          string `list:"omit"` // apc.Proxy | apc.Target
		LocalConfig   `json:",inline"`
		ClusterConfig `json:",inline"`
	}
)

// local config
type (
	LocalConfig struct {
		FSP       FSPConf        `json:"fspaths"`
		ConfigDir string         `json:"confdir"`
		LogDir    string         `json:"log_dir"`
		TestFSP   TestFSPConf    `json:"test_fspaths"`
		HostNet   LocalNetConfig `json:"host_net"`
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

	// ais node: fspaths (a.k.a. mountpaths) and their respective (optional) labels
	FSPConf struct {
		Paths cos.StrKVs `json:"paths,omitempty" list:"readonly"`
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
		Backend     BackendConf     `json:"backend" allow:"cluster"`
		Ext         any             `json:"ext,omitempty"` // reserved
		WritePolicy WritePolicyConf `json:"write_policy"`  // object metadata write policy: (immediate | delayed | never)
		LastUpdated string          `json:"lastupdate_time"`
		UUID        string          `json:"uuid"`
		Dsort       DsortConf       `json:"distributed_sort"`
		Proxy       ProxyConf       `json:"proxy" allow:"cluster"`
		Cksum       CksumConf       `json:"checksum"`
		Auth        AuthConf        `json:"auth"`
		Tracing     TracingConf     `json:"tracing"`
		TCB         TCBConf         `json:"tcb"`
		TCO         TCOConf         `json:"tco"`
		Arch        ArchConf        `json:"arch"`
		RateLimit   RateLimitConf   `json:"rate_limit"`
		Keepalive   KeepaliveConf   `json:"keepalivetracker"`
		Rebalance   RebalanceConf   `json:"rebalance" allow:"cluster"`
		Log         LogConf         `json:"log"`
		EC          ECConf          `json:"ec" allow:"cluster"`
		Net         NetConf         `json:"net"`
		Timeout     TimeoutConf     `json:"timeout"`
		Space       SpaceConf       `json:"space"`
		Transport   TransportConf   `json:"transport"`
		Memsys      MemsysConf      `json:"memsys"`
		Disk        DiskConf        `json:"disk"`
		FSHC        FSHCConf        `json:"fshc"`
		Chunks      ChunksConf      `json:"chunks" allow:"cluster"`
		LRU         LRUConf         `json:"lru"`
		Client      ClientConf      `json:"client"`
		Mirror      MirrorConf      `json:"mirror" allow:"cluster"`
		Periodic    PeriodConf      `json:"periodic"`
		Downloader  DownloaderConf  `json:"downloader"`
		Features    feat.Flags      `json:"features,string" allow:"cluster"` // enumerated features to flip assorted global defaults (cmn/feat/feat and docs/feat*)
		Version     int64           `json:"config_version,string"`
		Versioning  VersionConf     `json:"versioning" allow:"cluster"`
		Resilver    ResilverConf    `json:"resilver"`
		GetBatch    GetBatchConf    `json:"get_batch" allow:"cluster"`
	}
	ConfigToSet struct {
		// ClusterConfig
		Backend     *BackendConf          `json:"backend,omitempty"`
		Mirror      *MirrorConfToSet      `json:"mirror,omitempty"`
		EC          *ECConfToSet          `json:"ec,omitempty"`
		Chunks      *ChunksConfToSet      `json:"chunks,omitempty"`
		Log         *LogConfToSet         `json:"log,omitempty"`
		Periodic    *PeriodConfToSet      `json:"periodic,omitempty"`
		Tracing     *TracingConfToSet     `json:"tracing,omitempty"`
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
		RateLimit   *RateLimitConfToSet   `json:"rate_limit"`
		Features    *feat.Flags           `json:"features,string,omitempty"`
		GetBatch    *GetBatchConfToSet    `json:"get_batch,omitempty"`

		// LocalConfig
		FSP *FSPConf `json:"fspaths,omitempty"`
	}

	BackendConf struct {
		Conf      map[string]any `json:"-"` // backend implementation-dependent (custom marshaling to populate this field)
		Providers map[string]Ns  `json:"-"` // conditional (build tag) providers set during validation (BackendConf.Validate)
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
		XactConf

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

		Enabled  bool `json:"enabled"`   // EC is enabled
		DiskOnly bool `json:"disk_only"` // if true, EC does not use SGL - data goes directly to drives
	}
	ECConfToSet struct {
		XactConfToSet
		ObjSizeLimit *int64 `json:"objsize_limit,omitempty"`
		DataSlices   *int   `json:"data_slices,omitempty"`
		ParitySlices *int   `json:"parity_slices,omitempty"`
		Enabled      *bool  `json:"enabled,omitempty"`
		DiskOnly     *bool  `json:"disk_only,omitempty"`
	}

	ChunksConf struct {
		// ObjSizeLimit is the object size threshold that triggers auto-chunking.
		//  0   : never auto-chunk;
		//  > 0 : auto-chunk using the configured ChunkSize if object size >= limit.
		//
		// ObjSizeLimit is a **soft limit** (user preference).
		//
		// Multipart uploads (MPU) always result in chunked storage using
		// the client-specified part sizes, regardless of the ObjSizeLimit and/or ChunkSize.
		ObjSizeLimit cos.SizeIEC `json:"objsize_limit"`

		// The maximum size for storing objects as single contiguous files.
		//
		// This is a **hard limit** that cannot be disabled and protects the
		// cluster from issues with extremely large monolithic objects.
		//
		// Validation:
		// - objsize_limit <= max_monolithic_size (when objsize_limit enabled)
		// - when specified (ie., non-zero) allowed range is [1GiB, 1TiB]
		//
		// See also:
		// - constants minMaxMonolithicSize and MaxMonolithicSize below.
		MaxMonolithicSize cos.SizeIEC `json:"max_monolithic_size,omitempty"`

		// Default chunk size (aka "part size") used for auto-chunking (see above);
		// 0 (zero) means default; Validate() sets it to chunkSizeDflt (1GiB).
		ChunkSize cos.SizeIEC `json:"chunk_size,omitempty"`

		// Persist the partial manifest after every N received chunks.
		// 0 (zero) means keeping partial manifest strictly in memory for the entire
		// duration of the upload.
		CheckpointEvery int `json:"checkpoint_every,omitempty"`

		// Reserved bitwise field for future advanced behaviors (compression, GC, placement, etc.)
		Flags uint64 `json:"flags,omitempty"`
	}

	ChunksConfToSet struct {
		ObjSizeLimit      *cos.SizeIEC `json:"objsize_limit,omitempty"`
		MaxMonolithicSize *cos.SizeIEC `json:"max_monolithic_size,omitempty"`
		ChunkSize         *cos.SizeIEC `json:"chunk_size,omitempty"`
		CheckpointEvery   *int         `json:"checkpoint_every,omitempty"`
		Flags             *uint64      `json:"flags,omitempty"`
	}

	LogConf struct {
		Level     cos.LogLevel `json:"level"`      // log level (aka verbosity)
		MaxSize   cos.SizeIEC  `json:"max_size"`   // exceeding this size triggers log rotation
		MaxTotal  cos.SizeIEC  `json:"max_total"`  // (sum individual log sizes); exceeding this number triggers cleanup
		FlushTime cos.Duration `json:"flush_time"` // log flush interval
		StatsTime cos.Duration `json:"stats_time"` // (not used)
		ToStderr  bool         `json:"to_stderr"`  // Log only to stderr instead of files.
	}
	LogConfToSet struct {
		Level     *cos.LogLevel `json:"level,omitempty"`
		ToStderr  *bool         `json:"to_stderr,omitempty"`
		MaxSize   *cos.SizeIEC  `json:"max_size,omitempty"`
		MaxTotal  *cos.SizeIEC  `json:"max_total,omitempty"`
		FlushTime *cos.Duration `json:"flush_time,omitempty" swaggertype:"primitive,string"`
		StatsTime *cos.Duration `json:"stats_time,omitempty" swaggertype:"primitive,string"`
	}

	// TracingConf defines the configuration used for the OpenTelemetry (OTEL) trace exporter.
	// It includes settings for enabling tracing, sampling ratio, exporter endpoint, and other
	// parameters necessary for distributed tracing in AIStore.
	TracingConf struct {
		ExporterEndpoint  string                `json:"exporter_endpoint"`       // gRPC exporter endpoint
		ExporterAuth      TraceExporterAuthConf `json:"exporter_auth,omitempty"` // exporter auth config
		ServiceNamePrefix string                `json:"service_name_prefix"`     // service name prefix used by trace exporter
		ExtraAttributes   map[string]string     `json:"attributes,omitempty"`    // any extra-attributes to be added to traces

		// SamplerProbabilityStr is the percentage of traces to be sampled, expressed as a float64.
		// It's stored as a string to avoid potential floating-point precision issues during json unmarshal.
		// Valid values range from 0.0 to 1.0, where 1.0 means 100% sampling.
		SamplerProbabilityStr string `json:"sampler_probability,omitempty"`
		Enabled               bool   `json:"enabled"`
		SkipVerify            bool   `json:"skip_verify"` // allow insecure exporter gRPC connection

		SamplerProbability float64 `json:"-"`
	}

	// NOTE: Updating TracingConfig requires restart.
	TracingConfToSet struct {
		ExporterEndpoint      *string                     `json:"exporter_endpoint,omitempty"`   // gRPC exporter endpoint
		ExporterAuth          *TraceExporterAuthConfToSet `json:"exporter_auth,omitempty"`       // exporter auth config
		ServiceNamePrefix     *string                     `json:"service_name_prefix,omitempty"` // service name used by trace exporter
		ExtraAttributes       map[string]string           `json:"attributes,omitempty"`          // any extra-attributes to be added to traces
		SamplerProbabilityStr *string                     `json:"sampler_probability,omitempty"` // percentage of traces to be sampled
		Enabled               *bool                       `json:"enabled,omitempty"`
		SkipVerify            *bool                       `json:"skip_verify,omitempty"` // allow insecure exporter gRPC connection
	}

	TraceExporterAuthConf struct {
		TokenHeader string `json:"token_header"` // header used to pass exporter auth token
		TokenFile   string `json:"token_file"`   // filepath from where auth token can be obtained
	}

	TraceExporterAuthConfToSet struct {
		TokenHeader *string `json:"token_header,omitempty"` // header used to pass exporter auth token
		TokenFile   *string `json:"token_file,omitempty"`   // filepath from where auth token can be obtained
	}

	// NOTE: StatsTime is one important timer - a pulse
	PeriodConf struct {
		StatsTime     cos.Duration `json:"stats_time"`      // collect and publish stats; other house-keeping
		RetrySyncTime cos.Duration `json:"retry_sync_time"` // metasync retry
		NotifTime     cos.Duration `json:"notif_time"`      // (IC notifications)
	}
	PeriodConfToSet struct {
		StatsTime     *cos.Duration `json:"stats_time,omitempty" swaggertype:"primitive,string"`
		RetrySyncTime *cos.Duration `json:"retry_sync_time,omitempty" swaggertype:"primitive,string"`
		NotifTime     *cos.Duration `json:"notif_time,omitempty" swaggertype:"primitive,string"`
	}

	// maximum intra-cluster latencies (in the increasing order)
	TimeoutConf struct {
		CplaneOperation cos.Duration `json:"cplane_operation"`  // read-mostly via global cmn.Rom.CplaneOperation
		MaxKeepalive    cos.Duration `json:"max_keepalive"`     // ditto, cmn.Rom.MaxKeepalive - see below
		MaxHostBusy     cos.Duration `json:"max_host_busy"`     // 2-phase transactions and more
		Startup         cos.Duration `json:"startup_time"`      // primary wait for joins at (primary's) startup; indirectly, cluster startup
		JoinAtStartup   cos.Duration `json:"join_startup_time"` // (join cluster at startup) timeout; (2 * Startup) when zero
		SendFile        cos.Duration `json:"send_file_time"`    // large file or blob and/or slow network
		// intra-cluster EC streams; default SharedStreamsDflt; never timeout when negative
		EcStreams cos.Duration `json:"ec_streams_time,omitempty"`
		// object metadata timeout; for training apps an approx. duration of 2 (two) epochs
		ObjectMD cos.Duration `json:"object_md,omitempty"`
		// prior to returning 409 conflict (ie., cmn.ErrBusy)
		ColdGetConflict cos.Duration `json:"cold_get_conflict,omitempty"`
	}
	TimeoutConfToSet struct {
		CplaneOperation *cos.Duration `json:"cplane_operation,omitempty" swaggertype:"primitive,string"`
		MaxKeepalive    *cos.Duration `json:"max_keepalive,omitempty" swaggertype:"primitive,string"`
		MaxHostBusy     *cos.Duration `json:"max_host_busy,omitempty" swaggertype:"primitive,string"`
		Startup         *cos.Duration `json:"startup_time,omitempty" swaggertype:"primitive,string"`
		JoinAtStartup   *cos.Duration `json:"join_startup_time,omitempty" swaggertype:"primitive,string"`
		SendFile        *cos.Duration `json:"send_file_time,omitempty" swaggertype:"primitive,string"`
		EcStreams       *cos.Duration `json:"ec_streams_time,omitempty" swaggertype:"primitive,string"`
		ObjectMD        *cos.Duration `json:"object_md,omitempty" swaggertype:"primitive,string"`
		ColdGetConflict *cos.Duration `json:"cold_get_conflict,omitempty" swaggertype:"primitive,string"`
	}

	ClientConf struct {
		Timeout        cos.Duration `json:"client_timeout"`
		TimeoutLong    cos.Duration `json:"client_long_timeout"`
		ListObjTimeout cos.Duration `json:"list_timeout"`
	}
	ClientConfToSet struct {
		Timeout        *cos.Duration `json:"client_timeout,omitempty" swaggertype:"primitive,string"` // readonly as far as intra-cluster
		TimeoutLong    *cos.Duration `json:"client_long_timeout,omitempty" swaggertype:"primitive,string"`
		ListObjTimeout *cos.Duration `json:"list_timeout,omitempty" swaggertype:"primitive,string"`
	}

	ProxyConf struct {
		PrimaryURL   string `json:"primary_url"`
		OriginalURL  string `json:"original_url"`
		DiscoveryURL string `json:"discovery_url"`
		NonElectable bool   `json:"non_electable"` // NOTE: deprecated, not used
	}
	ProxyConfToSet struct {
		PrimaryURL   *string `json:"primary_url,omitempty"`
		OriginalURL  *string `json:"original_url,omitempty"`
		DiscoveryURL *string `json:"discovery_url,omitempty"`
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

		// Specifies a unit of space-cleanup processing;
		// Zero value _translates_ as a system default 32*1024 items
		// See also:
		// - SpaceConf.Validate()
		BatchSize int64 `json:"batch_size,omitempty"`

		// Sets the interval of time for cleanup and GC operations to skip (or ignore)
		// any stored content with mtime or atime that fall within the interval;
		// zero value _translates_ as a system default 2h (dontCleanupTimeDflt).
		// See also:
		// - SpaceConf.Validate()
		// - lru.dont_evict_time
		DontCleanupTime cos.Duration `json:"dont_cleanup_time,omitempty"`
	}
	SpaceConfToSet struct {
		CleanupWM       *int64        `json:"cleanupwm,omitempty"`
		LowWM           *int64        `json:"lowwm,omitempty"`
		HighWM          *int64        `json:"highwm,omitempty"`
		OOS             *int64        `json:"out_of_space,omitempty"`
		BatchSize       *int64        `json:"batch_size,omitempty"`
		DontCleanupTime *cos.Duration `json:"dont_cleanup_time,omitempty"`
	}

	LRUConf struct {
		// DontEvictTime denotes the period of time during which eviction of an object
		// is forbidden [atime, atime + DontEvictTime]
		DontEvictTime cos.Duration `json:"dont_evict_time"`

		// CapacityUpdTime denotes the frequency at which AIS targets update capacity (free/avail) utilization
		CapacityUpdTime cos.Duration `json:"capacity_upd_time"`

		// Specifies a unit of LRU eviction processing;
		// Zero value _translates_ as a system default 32*1024 (objects to be evicted)
		// See also:
		// - LRUConf.Validate()
		BatchSize int64 `json:"batch_size,omitempty"`

		// Enabled: LRU will only run when set to true
		Enabled bool `json:"enabled"`
	}
	LRUConfToSet struct {
		DontEvictTime   *cos.Duration `json:"dont_evict_time,omitempty" swaggertype:"primitive,string"`
		CapacityUpdTime *cos.Duration `json:"capacity_upd_time,omitempty" swaggertype:"primitive,string"`
		BatchSize       *int64        `json:"batch_size,omitempty"`
		Enabled         *bool         `json:"enabled,omitempty"`
	}

	DiskConf struct {
		DiskUtilLowWM   int64        `json:"disk_util_low_wm"`  // no throttling below
		DiskUtilHighWM  int64        `json:"disk_util_high_wm"` // throttle longer when above
		DiskUtilMaxWM   int64        `json:"disk_util_max_wm"`
		IostatTimeLong  cos.Duration `json:"iostat_time_long"`
		IostatTimeShort cos.Duration `json:"iostat_time_short"`

		// Linear history window for disk utilization smoothing
		// (not to confuse with storage capacity (aka, space) utilization).
		// How it works:
		// - previously sampled disk utilizations that fall into this window
		// get then factored-in to compute the resulting "smoothed" numbers;
		// - the idea is to eliminate instantaneous short-term spikes and further use
		// the utilization stats in throttling and least-used decisions;
		// - zero value _maps_ to a system default (see below)
		// - any positive value smaller than `IostatTimeLong` disables smoothing
		// (in other words, causes to report "raw" IO utilizations)
		IostatTimeSmooth cos.Duration `json:"iostat_time_smooth,omitempty"`
	}
	DiskConfToSet struct {
		DiskUtilLowWM   *int64        `json:"disk_util_low_wm,omitempty"`
		DiskUtilHighWM  *int64        `json:"disk_util_high_wm,omitempty"`
		DiskUtilMaxWM   *int64        `json:"disk_util_max_wm,omitempty"`
		IostatTimeLong  *cos.Duration `json:"iostat_time_long,omitempty" swaggertype:"primitive,string"`
		IostatTimeShort *cos.Duration `json:"iostat_time_short,omitempty" swaggertype:"primitive,string"`

		// see above
		IostatTimeSmooth *cos.Duration `json:"iostat_time_smooth,omitempty"`
	}

	RebalanceConf struct {
		XactConf
		DestRetryTime cos.Duration `json:"dest_retry_time"` // max wait for ACKs & neighbors to complete
		Enabled       bool         `json:"enabled"`         // true=auto-rebalance | manual rebalancing
	}
	RebalanceConfToSet struct {
		XactConfToSet
		DestRetryTime *cos.Duration `json:"dest_retry_time,omitempty" swaggertype:"primitive,string"`
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

		// - validate in-cluster object's checksum(s);
		// - upon any of the `cos.ErrBadCksum` errors try to recover from
		//   local redundant copies, and/or EC slices, and/or remote backend if exists;
		// - if all fails, remove the object and fail the GET.
		ValidateWarmGet bool `json:"validate_warm_get"`

		// validate checksums of objects migrated or replicated within the cluster
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
		// - rebalance (object migration)
		//
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
		Proto         string `json:"-"`             // http or https (set depending on `UseHTTPS`)
		Certificate   string `json:"server_crt"`    // HTTPS: X.509 certificate
		CertKey       string `json:"server_key"`    // HTTPS: X.509 key
		ServerNameTLS string `json:"domain_tls"`    // #6410
		ClientCA      string `json:"client_ca_tls"` // #6410
		// clients "idle" config: intra-cluster and backend
		IdleConnTimeout     cos.Duration `json:"idle_conn_time"`
		MaxIdleConnsPerHost int          `json:"idle_conns_per_host"`
		MaxIdleConns        int          `json:"idle_conns"`
		// cont-d
		ClientAuthTLS   int  `json:"client_auth_tls"`   // #6410 tls.ClientAuthType enum
		WriteBufferSize int  `json:"write_buffer_size"` // http.Transport.WriteBufferSize; zero defaults to 4KB
		ReadBufferSize  int  `json:"read_buffer_size"`  // http.Transport.ReadBufferSize; ditto
		UseHTTPS        bool `json:"use_https"`         // use HTTPS
		SkipVerifyCrt   bool `json:"skip_verify"`       // skip X.509 cert verification (used with self-signed certs)
		Chunked         bool `json:"chunked_transfer"`  // (https://tools.ietf.org/html/rfc7230#page-36; not used since 02/23)
	}
	HTTPConfToSet struct {
		Certificate   *string `json:"server_crt,omitempty"`
		CertKey       *string `json:"server_key,omitempty"`
		ServerNameTLS *string `json:"domain_tls,omitempty"`
		ClientCA      *string `json:"client_ca_tls,omitempty"`
		// added v3.26
		IdleConnTimeout     *cos.Duration `json:"idle_conn_time,omitempty" swaggertype:"primitive,string"`
		MaxIdleConnsPerHost *int          `json:"idle_conns_per_host,omitempty"`
		MaxIdleConns        *int          `json:"idle_conns,omitempty"`
		// cont-d
		WriteBufferSize *int  `json:"write_buffer_size,omitempty" list:"readonly"`
		ReadBufferSize  *int  `json:"read_buffer_size,omitempty" list:"readonly"`
		ClientAuthTLS   *int  `json:"client_auth_tls,omitempty"`
		UseHTTPS        *bool `json:"use_https,omitempty"`
		SkipVerifyCrt   *bool `json:"skip_verify,omitempty"`
		Chunked         *bool `json:"chunked_transfer,omitempty"`
	}

	FSHCConf struct {
		TestFileCount int `json:"test_files"` // number of files to read/write
		// critical and unexpected errors detected during FSHC run;
		// exceeding the limit "triggers" FSHC that may, in turn, disable the corresponding mountpath
		HardErrs int `json:"error_limit"`

		// - maximum number of I/O errors during the last `IOErrTime` interval;
		// - the number does not include network error (e.g., connection reset by peer)
		//   and errors returned by remote backends;
		// - exceeding this limit is also an FSHC-trggering event; subsequently,
		//   if FSHC confirms the problem it will disable the mountpath (see above)
		IOErrs int `json:"io_err_limit,omitempty"`
		// time interval (in seconds) to accumulate soft errors;
		// the total number by the end of the interval must not exceed `IOErrs` (above)
		IOErrTime cos.Duration `json:"io_err_time,omitempty"`

		// whether FSHC is enabled (note: disabling FSHC is _not_ recommended)
		Enabled bool `json:"enabled"`
	}
	FSHCConfToSet struct {
		TestFileCount *int          `json:"test_files,omitempty"`
		HardErrs      *int          `json:"error_limit,omitempty"`
		IOErrs        *int          `json:"io_err_limit,omitempty"`
		IOErrTime     *cos.Duration `json:"io_err_time,omitempty" swaggertype:"primitive,string"`
		Enabled       *bool         `json:"enabled,omitempty"`
	}

	AuthConf struct {
		Secret             string   `json:"secret"`
		PubKey             string   `json:"public_key"`
		Aud                []string `json:"aud"`
		Enabled            bool     `json:"enabled"`
		AllowS3TokenCompat bool     `json:"allow_s3_token_compat,omitempty"` // Allow X-Amz-Security-Token header to contain JWT instead of SigV4
	}
	AuthConfToSet struct {
		Secret             *string  `json:"secret,omitempty"`
		Enabled            *bool    `json:"enabled,omitempty"`
		AllowS3TokenCompat *bool    `json:"allow_s3_token_compat,omitempty"`
		PubKey             string   `json:"public_key,omitempty"`
		Aud                []string `json:"aud,omitempty"`
	}

	// keepalive
	KeepaliveConf struct {
		Proxy       KeepaliveTrackerConf `json:"proxy"`       // how proxy tracks target keepalives
		Target      KeepaliveTrackerConf `json:"target"`      // how target tracks primary proxies keepalives
		NumRetries  int                  `json:"num_retries"` // default: `kaNumRetries`
		RetryFactor uint8                `json:"retry_factor"`
	}
	KeepaliveConfToSet struct {
		Proxy       *KeepaliveTrackerConfToSet `json:"proxy,omitempty"`
		Target      *KeepaliveTrackerConfToSet `json:"target,omitempty"`
		NumRetries  *int                       `json:"num_retries,omitempty"`
		RetryFactor *uint8                     `json:"retry_factor,omitempty"`
	}
	KeepaliveTrackerConf struct {
		Name     string       `json:"name"`     // "heartbeat"
		Interval cos.Duration `json:"interval"` // keepalive interval
		Factor   uint8        `json:"factor"`   // only average
	}
	KeepaliveTrackerConfToSet struct {
		Interval *cos.Duration `json:"interval,omitempty" swaggertype:"primitive,string"`
		Name     *string       `json:"name,omitempty" list:"readonly"`
		Factor   *uint8        `json:"factor,omitempty"`
	}

	DownloaderConf struct {
		Timeout cos.Duration `json:"timeout"`
	}
	DownloaderConfToSet struct {
		Timeout *cos.Duration `json:"timeout,omitempty" swaggertype:"primitive,string"`
	}

	DsortConf struct {
		DuplicatedRecords   string `json:"duplicated_records"`
		MissingShards       string `json:"missing_shards"` // cmn.SupportedReactions enum
		EKMMalformedLine    string `json:"ekm_malformed_line"`
		EKMMissingKey       string `json:"ekm_missing_key"`
		DefaultMaxMemUsage  string `json:"default_max_mem_usage"`
		DsorterMemThreshold string `json:"dsorter_mem_threshold"`
		XactConf
		CallTimeout cos.Duration `json:"call_timeout" swaggertype:"primitive,integer"`
	}
	DsortConfToSet struct {
		XactConfToSet
		DuplicatedRecords   *string       `json:"duplicated_records,omitempty"`
		MissingShards       *string       `json:"missing_shards,omitempty"`
		EKMMalformedLine    *string       `json:"ekm_malformed_line,omitempty"`
		EKMMissingKey       *string       `json:"ekm_missing_key,omitempty"`
		DefaultMaxMemUsage  *string       `json:"default_max_mem_usage,omitempty"`
		CallTimeout         *cos.Duration `json:"call_timeout,omitempty" swaggertype:"primitive,string"`
		DsorterMemThreshold *string       `json:"dsorter_mem_threshold,omitempty"`
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
		MaxHeaderSize    *int          `json:"max_header,omitempty"`
		Burst            *int          `json:"burst_buffer,omitempty"`
		IdleTeardown     *cos.Duration `json:"idle_teardown,omitempty" swaggertype:"primitive,string"`
		QuiesceTime      *cos.Duration `json:"quiescent,omitempty" swaggertype:"primitive,string"`
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
		HousekeepTime  *cos.Duration `json:"hk_time,omitempty" swaggertype:"primitive,string"`
		MinPctTotal    *int          `json:"min_pct_total,omitempty"`
		MinPctFree     *int          `json:"min_pct_free,omitempty"`
	}

	// generic xaction --
	XactConf struct {
		Compression string `json:"compression"`       // enum { CompressAlways, ... } in api/apc/compression.go
		SbundleMult int    `json:"bundle_multiplier"` // stream-bundle multiplier: num streams to destination
		Burst       int    `json:"burst_buffer"`      // xaction channel (buffer) size
	}
	XactConfToSet struct {
		Compression *string `json:"compression,omitempty"`
		SbundleMult *int    `json:"bundle_multiplier,omitempty"`
		Burst       *int    `json:"burst_buffer,omitempty"`
	}

	// bucket-to-bucket copy/transform
	TCBConf      struct{ XactConf }
	TCBConfToSet struct{ XactConfToSet }

	// multi-object copy/transform
	TCOConf      struct{ XactConf }
	TCOConfToSet struct{ XactConfToSet }

	// multi-object archive (multiple objects => shard)
	ArchConf      struct{ XactConf }
	ArchConfToSet struct{ XactConfToSet }

	WritePolicyConf struct {
		Data apc.WritePolicy `json:"data"`
		MD   apc.WritePolicy `json:"md"`
	}
	WritePolicyConfToSet struct {
		Data *apc.WritePolicy `json:"data,omitempty" list:"readonly"` // NOTE: NIY
		MD   *apc.WritePolicy `json:"md,omitempty"`
	}
)

// global config that can be used to manage:
// * adaptive rate limit vis-à-vis Cloud backend
// * rate limiting (bursty) user workloads on the front
type (
	RateLimitConf struct {
		Backend  Adaptive `json:"backend"`
		Frontend Bursty   `json:"frontend"`
	}
	RateLimitConfToSet struct {
		Backend  *AdaptiveToSet `json:"backend,omitempty"`
		Frontend *BurstyToSet   `json:"frontend,omitempty"`
	}
	RateLimitBase struct {
		// optional per-operation MaxTokens override - a space-separated key:value list, e.g.:
		// - "put:3500"
		// - "get:5000 delete:1000"
		// optional; case-insensitive
		Verbs string `json:"per_op_max_tokens,omitempty"`
		//
		// mandatory parameters; for default values and min/max ranges, see RateLimitConf.Validate
		//
		Interval  cos.Duration `json:"interval"`
		MaxTokens int          `json:"max_tokens"`
		Enabled   bool         `json:"enabled"`
	}
	RateLimitBaseToSet struct {
		Verbs     *string       `json:"per_op_max_tokens,omitempty"`
		Interval  *cos.Duration `json:"interval,omitempty" swaggertype:"primitive,string"`
		MaxTokens *int          `json:"max_tokens,omitempty"`
		Enabled   *bool         `json:"enabled,omitempty"`
	}

	// Adaptive rate limit (a.k.a. rate shaper):
	// usage: handle status 429 and 503 from remote backends
	// - up to max tokens (originally, `MaxTokens`) during one Interval with NumRetries and
	// - `recompute` between Intervals
	// - see cmn/cos/rate_limit
	Adaptive struct {
		RateLimitBase
		NumRetries int `json:"num_retries"`
	}
	AdaptiveToSet struct {
		RateLimitBaseToSet      // NOTE: must be in the same exact order/position as above
		NumRetries         *int `json:"num_retries,omitempty"`
	}

	// rate limit that fails 'too-many requests' while permitting a certain level of burstiness
	// - usage: to restrict the rate of user GET, PUT, and DELETE requests
	// - see cmn/cos/rate_limit
	Bursty struct {
		RateLimitBase
		Size int `json:"burst_size"`
	}
	BurstyToSet struct {
		RateLimitBaseToSet
		Size *int `json:"burst_size,omitempty"`
	}
)

// ref: https://github.com/NVIDIA/aistore/releases/tag/v1.4.0#getbatch-api-ml-endpoint
type (
	GetBatchConf struct {
		// Maximum time to wait for remote targets to send their data during
		// distributed multi-object/multi-file retrieval. When a sending target
		// fails or is slow to respond, the designated target (DT) waits up to
		// this duration before attempting recovery.
		// Longer values increase tolerance for transient slowdowns but delay
		// error detection. Shorter values fail faster but may be too aggressive.
		MaxWait cos.Duration `json:"max_wait"`

		// Number of worker goroutines for pagecache read-ahead warming.
		// Helps reduce cold-read latency by pre-loading file data into memory.
		// Always auto-disabled under extreme system load.
		// - (-1): disabled
		// - 0:    default (enabled w/ 2 (numWarmupWorkersDflt) workers)
		// - >0:   enabled with N workers
		NumWarmupWorkers int `json:"warmup_workers"`
	}
	GetBatchConfToSet struct {
		MaxWait          *cos.Duration `json:"max_wait,omitempty"`
		NumWarmupWorkers *int          `json:"warmup_workers,omitempty"`
	}
)

// assorted named fields that require (cluster | node) restart for changes to make an effect
// (used by CLI)
var ConfigRestartRequired = [...]string{"auth.secret", "memsys", "net"}

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
	opts.OldMetaverOk = 3
	return opts
}

func (*LocalConfig) JspOpts() jsp.Options   { return jsp.Plain() }
func (*ClusterConfig) JspOpts() jsp.Options { return _jspOpts() }
func (*ConfigToSet) JspOpts() jsp.Options   { return _jspOpts() }

// interface guard: config validators
var (
	_ validator = (*BackendConf)(nil)
	_ validator = (*CksumConf)(nil)
	_ validator = (*LogConf)(nil)
	_ validator = (*LRUConf)(nil)
	_ validator = (*SpaceConf)(nil)
	_ validator = (*MirrorConf)(nil)
	_ validator = (*ECConf)(nil)
	_ validator = (*ChunksConf)(nil)
	_ validator = (*VersionConf)(nil)
	_ validator = (*PeriodConf)(nil)
	_ validator = (*TimeoutConf)(nil)
	_ validator = (*ClientConf)(nil)
	_ validator = (*RebalanceConf)(nil)
	_ validator = (*ResilverConf)(nil)
	_ validator = (*NetConf)(nil)
	_ validator = (*FSHCConf)(nil)
	_ validator = (*HTTPConf)(nil)
	_ validator = (*DownloaderConf)(nil)
	_ validator = (*DsortConf)(nil)
	_ validator = (*TransportConf)(nil)
	_ validator = (*MemsysConf)(nil)
	_ validator = (*TCBConf)(nil)
	_ validator = (*WritePolicyConf)(nil)
	_ validator = (*TracingConf)(nil)
	_ validator = (*GetBatchConf)(nil)

	_ validator = (*feat.Flags)(nil) // is called explicitly from main config validator

	_ contextValidator = (*KeepaliveConf)(nil)
)

// interface guard: bucket-level validators-as-props
var (
	_ propsValidator = (*CksumConf)(nil)
	_ propsValidator = (*MirrorConf)(nil)
	_ propsValidator = (*ECConf)(nil)

	_ propsValidator = (*ExtraProps)(nil)
	_ propsValidator = (*feat.Flags)(nil)

	_ propsValidator = (*WritePolicyConf)(nil)
	_ propsValidator = (*RateLimitConf)(nil)
	_ propsValidator = (*ChunksConf)(nil)
	_ propsValidator = (*LRUConf)(nil)
)

// interface guard: special (un)marshaling
var (
	_ json.Marshaler   = (*BackendConf)(nil)
	_ json.Unmarshaler = (*BackendConf)(nil)
	_ json.Marshaler   = (*FSPConf)(nil)
	_ json.Unmarshaler = (*FSPConf)(nil)
)

//
// Config and its nested (Cluster | Local) -------------------------
//

// main config validator
func (c *Config) Validate() error {
	if c.ConfigDir == "" {
		return errors.New("invalid confdir value (must be non-empty)")
	}
	if c.LogDir == "" {
		return errors.New("invalid log dir value (must be non-empty)")
	}

	//
	// NOTE: the following validations perform cross-sections checks - call them explicitly
	//
	if err := c.LocalConfig.HostNet.Validate(c); err != nil {
		return err
	}
	if err := c.LocalConfig.FSP.Validate(c); err != nil {
		return err
	}
	if err := c.LocalConfig.TestFSP.Validate(c); err != nil {
		return err
	}
	if err := c.Features.Validate(); err != nil {
		return err
	}
	opts := IterOpts{VisitAll: true}
	return IterFields(c, c.validateFld, opts)
}

func (c *Config) validateFld(_ string, field IterField) (error, bool) {
	switch v := field.Value().(type) {
	case contextValidator:
		return v.Validate(c), false
	case validator:
		return v.Validate(), false
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

/////////////////
// ConfigToSet //
/////////////////

// FillFromQuery populates ConfigToSet from URL query values
func (ctu *ConfigToSet) FillFromQuery(query url.Values) error {
	var anyExists bool
	for key := range query {
		if key == apc.QparamTransient {
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

///////////////////
// ClusterConfig //
///////////////////

func (c *ClusterConfig) Apply(updateConf *ConfigToSet, asType string) error {
	return CopyProps(updateConf, c, asType)
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
	c.FSP.Paths[mpath] = ""
}

func (c *LocalConfig) DelPath(mpath string) {
	debug.Assert(!c.TestingEnv())
	c.FSP.Paths.Delete(mpath)
}

//
// config sections: validation, default settings, helpers ------------------------------
//

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
		return fmt.Errorf("invalid log.stats_time=%s (expected range [periodic.stats_time, 10m])", c.StatsTime)
	}
	return nil
}

////////////////
// ClientConf //
////////////////

const (
	minClientTimeout = time.Second
	maxClientTimeout = 30 * time.Minute

	errExpectedRange = "(expected range [1s, 30m] or zero)"
)

func (c *ClientConf) Validate() error {
	if j := c.Timeout.D(); j != 0 && (j < minClientTimeout || j > maxClientTimeout) {
		return fmt.Errorf("invalid client_timeout=%s %s", j, errExpectedRange)
	}
	if j := c.TimeoutLong.D(); j != 0 && (j < minClientTimeout || j > maxClientTimeout) {
		return fmt.Errorf("invalid client_long_timeout=%s %s", j, errExpectedRange)
	}
	if j := c.TimeoutLong.D(); j != 0 && j < c.Timeout.D() {
		return fmt.Errorf("client_long_timeout=%s cannot be less than client_timeout=%s", j, c.Timeout.D())
	}
	if j := c.ListObjTimeout.D(); j != 0 && (j < minClientTimeout || j > maxClientTimeout) {
		return fmt.Errorf("invalid list_timeout=%s %s", j, errExpectedRange)
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
				return fmt.Errorf("invalid cloud specification: %w", err)
			}
			for alias, urls := range aisConf {
				if len(urls) == 0 {
					return fmt.Errorf("no URL(s) to connect to remote AIS cluster %q", alias)
				}
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

func (c *BackendConf) setProvider(provider string) {
	var ns Ns
	switch provider {
	case apc.AWS, apc.Azure, apc.GCP, apc.OCI, apc.HT:
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
	if c.Conf == nil {
		c.Conf = make(map[string]any, 1)
	}
	c.Conf[provider] = newConf
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

const (
	iosSmoothMaxMultiplier = 10

	iosTimeLongMax  = 60 * time.Second
	iosTimeLongMin  = 2 * time.Second
	iosTimeShortMin = time.Millisecond
)

func (c *DiskConf) Validate() (err error) {
	lwm, hwm, maxwm := c.DiskUtilLowWM, c.DiskUtilHighWM, c.DiskUtilMaxWM
	if lwm <= 0 || hwm <= lwm || maxwm <= hwm || maxwm > 100 {
		return fmt.Errorf("invalid (disk_util_lwm, disk_util_hwm, disk_util_maxwm) config %+v", c)
	}
	if c.IostatTimeShort.D() < iosTimeShortMin || c.IostatTimeShort.D() > iosTimeLongMax {
		return fmt.Errorf("invalid disk.ios_time_short %v (expecting range [%v, %v])",
			c.IostatTimeShort, iosTimeShortMin, iosTimeLongMax)
	}
	if c.IostatTimeLong.D() < iosTimeLongMin || c.IostatTimeLong.D() > iosTimeLongMax {
		return fmt.Errorf("invalid disk.ios_time_long %v (expecting range [%v, %v])",
			c.IostatTimeLong, iosTimeLongMin, iosTimeLongMax)
	}
	if c.IostatTimeLong < c.IostatTimeShort {
		return fmt.Errorf("disk.ios_time_long %v shorter than disk.ios_time_short %v",
			c.IostatTimeLong, c.IostatTimeShort)
	}

	if c.IostatTimeSmooth == 0 {
		// try to come up with a sensible multiplier
		switch {
		case c.IostatTimeLong.D() >= iosTimeLongMax/2:
			c.IostatTimeSmooth = c.IostatTimeLong
		case c.IostatTimeLong.D() > 10*time.Second:
			c.IostatTimeSmooth = 2 * c.IostatTimeLong
		case c.IostatTimeLong.D() > 5*time.Second:
			c.IostatTimeSmooth = 3 * c.IostatTimeLong
		default:
			c.IostatTimeSmooth = 4 * c.IostatTimeLong
		}
		// NOTE: a small positive value is interpreted as: disable smoothing, report raw values
	} else if c.IostatTimeSmooth < 0 || c.IostatTimeSmooth > iosSmoothMaxMultiplier*c.IostatTimeLong {
		return fmt.Errorf("invalid disk.ios_time_smooth %v (must be a non-negative value <= %d*ios_time_long)",
			c.IostatTimeSmooth, iosSmoothMaxMultiplier)
	}
	return nil
}

///////////////
// SpaceConf //
///////////////

const (
	dontCleanupTimeDflt = 2 * time.Hour
	dontCleanupTimeMin  = time.Hour
)

// common for both SpaceConf and LRUConf
const (
	GCBatchSizeMin  = 1024
	GCBatchSizeDflt = 32 * 1024
	GCBatchSizeMax  = 512 * 1024
)

func (c *SpaceConf) Validate() error {
	if c.CleanupWM <= 0 || c.LowWM < c.CleanupWM || c.HighWM < c.LowWM || c.OOS < c.HighWM || c.OOS > 100 {
		return fmt.Errorf("invalid %s (expecting: 0 < cleanup < low < high < OOS < 100)", c)
	}

	if c.DontCleanupTime == 0 {
		c.DontCleanupTime = cos.Duration(dontCleanupTimeDflt)
	} else if c.DontCleanupTime.D() < dontCleanupTimeMin {
		return fmt.Errorf("invalid %+v (expecting: space.dont_cleanup_time >= %v)", c, dontCleanupTimeMin)
	}

	if c.BatchSize == 0 {
		c.BatchSize = GCBatchSizeDflt
	} else if n := c.BatchSize; n < GCBatchSizeMin || n > GCBatchSizeMax {
		return fmt.Errorf("invalid space.batch_size=%d (expecting range [%d - %d])", n, GCBatchSizeMin, GCBatchSizeMax)
	}
	return nil
}

func (c *SpaceConf) String() string {
	return fmt.Sprintf("space config: cleanup=%d%%, low=%d%%, high=%d%%, OOS=%d%%",
		c.CleanupWM, c.LowWM, c.HighWM, c.OOS)
}

/////////////
// LRUConf //
/////////////

const (
	capUpdTimeMin    = 10 * time.Second
	dontEvictTimeMin = time.Hour
)

func (c *LRUConf) String() string {
	if !c.Enabled {
		return confDisabled
	}
	return fmt.Sprintf("lru: dont_evict_time=%v, capacity_upd_time=%v, batch_size=%d", c.DontEvictTime, c.CapacityUpdTime, c.BatchSize)
}

func (c *LRUConf) Validate() (err error) {
	if c.CapacityUpdTime.D() < capUpdTimeMin {
		return fmt.Errorf("invalid %+v (expecting: lru.capacity_upd_time >= %v)", c, capUpdTimeMin)
	}
	if c.BatchSize == 0 {
		c.BatchSize = GCBatchSizeDflt
	} else if n := c.BatchSize; n < GCBatchSizeMin || n > GCBatchSizeMax {
		return fmt.Errorf("invalid lru.batch_size=%d (expecting range [%d - %d])", n, GCBatchSizeMin, GCBatchSizeMax)
	}
	if c.DontEvictTime.D() < dontEvictTimeMin {
		err = fmt.Errorf("invalid %+v (expecting: lru.dont_evict_time >= %v)", c, dontEvictTimeMin)
	}
	return
}

func (c *LRUConf) ValidateAsProps(...any) error { return c.Validate() }

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
		return confDisabled
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
		return confDisabled
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
		return confDisabled
	}

	return fmt.Sprintf("%d copies", c.Copies)
}

////////////
// ECConf //
////////////

const (
	ObjSizeToAlwaysReplicate = -1 // (that's when we have monolithic objects; see `ObjSizeLimit` comment above)

	MinSliceCount = 1  // minimum number of data or parity slices
	MaxSliceCount = 32 // maximum --/--
)

func (c *ECConf) Validate() error {
	if c.ObjSizeLimit < 0 && c.ObjSizeLimit != ObjSizeToAlwaysReplicate {
		return fmt.Errorf("invalid ec.objsize_limit: %d (expecting an integer greater than or equal to -1)", c.ObjSizeLimit)
	}
	if c.DataSlices < MinSliceCount || c.DataSlices > MaxSliceCount {
		err := fmt.Errorf("invalid ec.data_slices: %d (expected value in range [%d, %d])",
			c.DataSlices, MinSliceCount, MaxSliceCount)
		return err
	}
	if c.ParitySlices < MinSliceCount || c.ParitySlices > MaxSliceCount {
		return fmt.Errorf("invalid ec.parity_slices: %d (expected value in range [%d, %d])",
			c.ParitySlices, MinSliceCount, MaxSliceCount)
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
	return NewErrWarning(err.Error())
}

func (c *ECConf) String() string {
	if !c.Enabled {
		return confDisabled
	}
	objSizeLimit := c.ObjSizeLimit
	if objSizeLimit == ObjSizeToAlwaysReplicate {
		return fmt.Sprintf("no EC - always producing %d total replicas", c.ParitySlices+1)
	}
	return fmt.Sprintf("%d:%d (objsize limit %s)", c.DataSlices, c.ParitySlices, cos.IEC(objSizeLimit, 0))
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

////////////////
// ChunksConf //
////////////////

const (
	MaxMonolithicSize    = cos.TiB
	minMaxMonolithicSize = cos.GiB

	chunkSizeMin  = cos.KiB
	chunkSizeDflt = cos.GiB
	chunkSizeMax  = 5 * cos.GiB

	checkpointEveryMax = 1024
)

const (
	chunksmms = "chunks.max_monolithic_size"
	chunksosl = "chunks.objsize_limit"
)

func (c *ChunksConf) AutoEnabled() bool { return c.ObjSizeLimit > 0 }

func (c *ChunksConf) Validate() error {
	switch {
	case c.MaxMonolithicSize < 0:
		return fmt.Errorf("invalid %s: %d (expecting a non-negative integer)", chunksmms, c.MaxMonolithicSize)
	case c.MaxMonolithicSize == 0:
		c.MaxMonolithicSize = MaxMonolithicSize
	default:
		if c.MaxMonolithicSize < minMaxMonolithicSize || c.MaxMonolithicSize > MaxMonolithicSize {
			return fmt.Errorf("invalid %s: %d (%s) - must be in range [%s, %s]",
				chunksmms, c.MaxMonolithicSize, c.MaxMonolithicSize,
				cos.IEC(minMaxMonolithicSize, 0), cos.IEC(MaxMonolithicSize, 0))
		}
	}

	switch {
	case c.ObjSizeLimit < 0:
		return fmt.Errorf("invalid %s %d (expecting a non-negative integer)", chunksosl, c.ObjSizeLimit)
	case c.ObjSizeLimit > c.MaxMonolithicSize:
		return fmt.Errorf("invalid %s %d (%s) - cannot be greater than %s %d (%s)",
			chunksosl, c.ObjSizeLimit, c.ObjSizeLimit, chunksmms, c.MaxMonolithicSize, c.MaxMonolithicSize)
	}

	if !c.AutoEnabled() { // including v4.0 and prior
		return nil
	}

	if c.ChunkSize == 0 {
		c.ChunkSize = chunkSizeDflt
	} else if c.ChunkSize < chunkSizeMin || c.ChunkSize > chunkSizeMax {
		return fmt.Errorf("expecting chunks.chunk_size in the range [%s, %s], got %d (%s)",
			cos.IEC(chunkSizeMin, 0), cos.IEC(chunkSizeMax, 0), c.ChunkSize, c.ChunkSize)
	}

	if c.CheckpointEvery < 0 || c.CheckpointEvery > checkpointEveryMax {
		return fmt.Errorf("chunks.checkpoint_every must be an integer in the range [0, %d] (got %d)", checkpointEveryMax, c.CheckpointEvery)
	}
	return nil
}

func (c *ChunksConf) ValidateAsProps(...any) error {
	return c.Validate()
}

func (c *ChunksConf) String() string {
	if !c.AutoEnabled() {
		return "auto-chunking disabled"
	}
	var checkpoint string
	switch c.CheckpointEvery {
	case 0:
		checkpoint = "checkpoint: never"
	case 1:
		checkpoint = "checkpoint: every chunk"
	default:
		checkpoint = fmt.Sprintf("checkpoint: every %d chunks", c.CheckpointEvery)
	}

	s := fmt.Sprintf("chunk-size: %s; %s; objsize-limit: %s", c.ChunkSize.String(), checkpoint, c.ObjSizeLimit.String())
	if c.Flags != 0 {
		s += fmt.Sprintf("; flags: 0x%x", c.Flags)
	}
	return s
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

// default number of keepalive retries
// see palive.retry in re "total number of failures prior to removing"
const kaNumRetries = 3

// interval bounds
const (
	kaliveIvalMin = 1 * time.Second
	kaliveIvalMax = 1 * time.Minute
	kaliveToutMax = 2 * time.Minute
)

func (c *KeepaliveConf) Validate(config *Config) error {
	if c.Proxy.Name != "heartbeat" {
		return fmt.Errorf("invalid keepalivetracker.proxy.name %s", c.Proxy.Name)
	}
	if c.Target.Name != "heartbeat" {
		return fmt.Errorf("invalid keepalivetracker.target.name %s", c.Target.Name)
	}
	if c.RetryFactor < 1 || c.RetryFactor > 10 {
		return fmt.Errorf("invalid keepalivetracker.retry_factor %d (expecting range [1, 10])", c.RetryFactor)
	}
	if c.NumRetries == 0 {
		c.NumRetries = kaNumRetries
	}
	if c.NumRetries < 1 || c.NumRetries > 10 {
		return fmt.Errorf("invalid keepalivetracker.num_retries %d (expecting range [1, 10])", c.NumRetries)
	}

	if err := c.Proxy.validate(&config.Timeout); err != nil {
		return err
	}
	return c.Target.validate(&config.Timeout)
}

func KeepaliveRetryDuration(c *Config) time.Duration {
	d := c.Timeout.CplaneOperation.D() * time.Duration(c.Keepalive.RetryFactor)
	return min(d, c.Timeout.MaxKeepalive.D()+time.Second)
}

func (c *KeepaliveTrackerConf) validate(timeoutConf *TimeoutConf) error {
	if c.Interval.D() < kaliveIvalMin || c.Interval.D() > kaliveIvalMax {
		return fmt.Errorf("invalid keepalivetracker.interval=%s (expected range [%v, %v])",
			c.Interval, kaliveIvalMin, kaliveIvalMax)
	}
	//
	// must be consistent w/ assorted timeout knobs
	//
	if c.Interval.D() < timeoutConf.MaxKeepalive.D() {
		return fmt.Errorf("keepalivetracker.interval=%s should be >= timeout.max_keepalive=%s",
			c.Interval, timeoutConf.MaxKeepalive)
	}
	kwin := c.Interval.D() * time.Duration(c.Factor)
	if kwin > kaliveToutMax {
		return fmt.Errorf("keepalive detection window=%v (interval * factor) exceeds %v", kwin, kaliveToutMax)
	}
	return nil
}

/////////////
// NetConf and NetConf.HTTPConf
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

// see also:
// - docs/idle_connections.md
// - DefaultMaxIdle* in cmn/client
// - `listen` in ais/htcommon
const (
	dfltMaxIdleConns   = 4096
	dfltMaxIdlePerHost = 128

	DfltMaxIdleTimeout = 30 * time.Second
)

func (c *HTTPConf) Validate() error {
	if c.ServerNameTLS != "" {
		return fmt.Errorf("invalid domain_tls %q: expecting empty (domain names/SANs should be set in X.509 cert)", c.ServerNameTLS)
	}
	if d := c.IdleConnTimeout.D(); d < 0 || d > DfltMaxIdleTimeout {
		return fmt.Errorf("invalid idle_conn_time %v (expecting range [0 - %v])", d, DfltMaxIdleTimeout)
	}
	if n := c.MaxIdleConns; n < 0 || n > dfltMaxIdleConns {
		return fmt.Errorf("invalid idle_conns %d (expecting range [0 - %d])", n, dfltMaxIdleConns)
	}
	if n := min(c.MaxIdleConns, dfltMaxIdlePerHost); c.MaxIdleConnsPerHost != 0 && c.MaxIdleConnsPerHost > n {
		return fmt.Errorf("invalid idle_conns_per_host: %d (expecting range [0 - %d])", c.MaxIdleConnsPerHost, n)
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

//////////////
// FSHCConf //
//////////////

const (
	ioErrTimeDflt = 10 * time.Second
	ioErrsLimit   = 10
)

func (c *FSHCConf) Validate() error {
	if c.TestFileCount < 4 {
		return fmt.Errorf("invalid fshc.test_files %d (expecting >= %d)", c.TestFileCount, 4)
	}
	if c.HardErrs < 2 {
		return fmt.Errorf("invalid fshc.error_limit %d (expecting >= %d)", c.HardErrs, 2)
	}

	if c.IOErrs == 0 && c.IOErrTime == 0 {
		c.IOErrs = ioErrsLimit
		c.IOErrTime = cos.Duration(ioErrTimeDflt)
	}
	if c.IOErrs == 0 {
		c.IOErrs = ioErrsLimit
	}
	if c.IOErrTime == 0 {
		c.IOErrTime = cos.Duration(ioErrTimeDflt)
	}

	if c.IOErrs < 10 {
		return fmt.Errorf("invalid fshc.io_err_limit %d (expecting >= %d)", c.IOErrs, 10)
	}
	if c.IOErrTime < cos.Duration(10*time.Second) {
		return fmt.Errorf("invalid fshc.io_err_time %d (expecting >= %v)", c.IOErrTime, 10*time.Second)
	}
	if c.IOErrTime > cos.Duration(60*time.Second) {
		return fmt.Errorf("invalid fshc.io_err_time %d (expecting <= %v)", c.IOErrTime, 60*time.Second)
	}
	return nil
}

////////////////////
// LocalNetConfig //
////////////////////

const HostnameListSepa = ","

func (c *LocalNetConfig) Validate(contextConfig *Config) error {
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
		return fmt.Errorf("invalid %s port: %w", NetPublic, err)
	}
	if c.PortIntraControl != 0 {
		if _, err := ValidatePort(c.PortIntraControl); err != nil {
			return fmt.Errorf("invalid %s port: %w", NetIntraControl, err)
		}
	}
	if c.PortIntraData != 0 {
		if _, err := ValidatePort(c.PortIntraData); err != nil {
			return fmt.Errorf("invalid %s port: %w", NetIntraData, err)
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
	return nil
}

///////////////
// DsortConf //
///////////////

const (
	IgnoreReaction = "ignore"
	WarnReaction   = "warn"
	AbortReaction  = "abort"
)

const _idsort = "invalid distributed_sort."

var SupportedReactions = []string{IgnoreReaction, WarnReaction, AbortReaction}

func (c *DsortConf) Validate() (err error) {
	if c.SbundleMult < 0 || c.SbundleMult > 16 {
		return fmt.Errorf(_idsort+"bundle_multiplier: %v (expected range [0, 16])", c.SbundleMult)
	}
	if !apc.IsValidCompression(c.Compression) {
		return fmt.Errorf(_idsort+"compression: %q (expecting one of: %v)", c.Compression, apc.SupportedCompression)
	}
	return c.ValidateWithOpts(false)
}

func (c *DsortConf) ValidateWithOpts(allowEmpty bool) (err error) {
	f := func(reaction string) bool {
		return ((allowEmpty && reaction == "") || cos.StringInSlice(reaction, SupportedReactions))
	}

	const s = "expecting one of:"
	if !f(c.DuplicatedRecords) {
		return fmt.Errorf(_idsort+"duplicated_records: %s (%s %v)", c.DuplicatedRecords, s, SupportedReactions)
	}
	if !f(c.MissingShards) {
		return fmt.Errorf(_idsort+"missing_shards: %s (%s %v)", c.MissingShards, s, SupportedReactions)
	}
	if !f(c.EKMMalformedLine) {
		return fmt.Errorf(_idsort+"ekm_malformed_line: %s (%s %v)", c.EKMMalformedLine, s, SupportedReactions)
	}
	if !f(c.EKMMissingKey) {
		return fmt.Errorf(_idsort+"ekm_missing_key: %s (%s %v)", c.EKMMissingKey, s, SupportedReactions)
	}
	if !allowEmpty {
		if _, err := cos.ParseQuantity(c.DefaultMaxMemUsage); err != nil {
			return fmt.Errorf(_idsort+"default_max_mem_usage: %s (err: %v)", c.DefaultMaxMemUsage, err)
		}
	}
	if _, err := cos.ParseSize(c.DsorterMemThreshold, cos.UnitsIEC); err != nil && (!allowEmpty || c.DsorterMemThreshold != "") {
		return fmt.Errorf(_idsort+"dsorter_mem_threshold: %s (err: %v)", c.DsorterMemThreshold, err)
	}
	return nil
}

/////////////
// FSPConf //
/////////////

func (c *FSPConf) UnmarshalJSON(data []byte) error {
	c.Paths = cos.NewStrKVs(10)
	return jsoniter.Unmarshal(data, &c.Paths)
}

func (c *FSPConf) MarshalJSON() ([]byte, error) {
	return cos.MustMarshal(c.Paths), nil
}

func (c *FSPConf) Validate(contextConfig *Config) error {
	debug.Assertf(cos.StringInSlice(contextConfig.role, []string{apc.Proxy, apc.Target}),
		"unexpected node type: %q", contextConfig.role)

	// Don't validate in testing environment.
	if contextConfig.TestingEnv() || contextConfig.role != apc.Target {
		return nil
	}
	if len(c.Paths) == 0 {
		return NewErrInvalidFSPathsConf(ErrNoMountpaths)
	}

	cleanMpaths := cos.NewStrKVs(len(c.Paths))
	for fspath, val := range c.Paths {
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
		cleanMpaths[mpath] = val
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

	contextConfig.FSP.Paths = cos.NewStrKVs(c.Count)
	for i := range c.Count {
		mpath := filepath.Join(c.Root, fmt.Sprintf("mp%d", i+1))
		if c.Instance > 0 {
			mpath = filepath.Join(mpath, strconv.Itoa(c.Instance))
		}
		contextConfig.FSP.Paths[mpath] = ""
	}
	return nil
}

func (c *TestFSPConf) ValidateMpath(p string) (err error) {
	debug.Assert(c.Count > 0)
	for i := range c.Count {
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

const (
	maxLenMountpath = 255
)

func ValidateMpath(mpath string) (string, error) {
	cleanMpath := filepath.Clean(mpath)

	if cleanMpath[0] != filepath.Separator {
		return mpath, NewErrInvalidaMountpath(mpath, "mountpath must be an absolute path")
	}
	if cleanMpath == cos.PathSeparator {
		return "", NewErrInvalidaMountpath(mpath, "root directory is not a valid mountpath")
	}
	if len(cleanMpath) > maxLenMountpath {
		return "", NewErrInvalidaMountpath(mpath, "mountpath length cannot exceed "+strconv.Itoa(maxLenMountpath))
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

const (
	DfltTransportHeader = 4 * cos.KiB   // memsys.PageSize
	MaxTransportHeader  = 128 * cos.KiB // memsys.MaxPageSlabSize

	DfltTransportBurst = 256
	MaxTransportBurst  = 4096
)

// NOTE: uncompressed block sizes - the enum currently supported by the github.com/pierrec/lz4
func (c *TransportConf) Validate() (err error) {
	if c.LZ4BlockMaxSize != 64*cos.KiB && c.LZ4BlockMaxSize != 256*cos.KiB &&
		c.LZ4BlockMaxSize != cos.MiB && c.LZ4BlockMaxSize != 4*cos.MiB {
		return fmt.Errorf("invalid transport.block_size %s, expecting one of: [64K, 256K, 1MB, 4MB]",
			c.LZ4BlockMaxSize)
	}
	if c.Burst != 0 {
		if c.Burst < 32 || c.Burst > MaxTransportBurst {
			return fmt.Errorf("invalid transport.burst_buffer: %d, expecting [32, 4KiB] range or 0 (default)", c.Burst)
		}
	}
	if c.MaxHeaderSize != 0 {
		if c.MaxHeaderSize < 512 || c.MaxHeaderSize > MaxTransportHeader {
			return fmt.Errorf("invalid transport.max_header: %v, expecting (0, 128KiB] range or 0 (default)", c.MaxHeaderSize)
		}
	}
	if c.IdleTeardown.D() < time.Second {
		return fmt.Errorf("invalid transport.idle_teardown: %v (expecting >= 1s)", c.IdleTeardown)
	}
	if c.QuiesceTime.D() < 8*time.Second {
		return fmt.Errorf("invalid transport.quiescent: %v (expecting >= 8s)", c.QuiesceTime)
	}
	return nil
}

//////////////
// XactConf //
//////////////

func (c *XactConf) Validate() error {
	if !apc.IsValidCompression(c.Compression) {
		return fmt.Errorf("invalid compression: %q (expecting one of: %v)",
			c.Compression, apc.SupportedCompression)
	}
	if c.SbundleMult < 0 || c.SbundleMult > 16 {
		return fmt.Errorf("invalid bundle_multiplier: %v (expected range [0, 16])", c.SbundleMult)
	}
	if c.Burst < 0 || c.Burst > 10_000 {
		return fmt.Errorf("invalid burst_buffer: %v (expected range [0, 10000])", c.Burst)
	}
	return nil
}

/////////////////
// TimeoutConf //
/////////////////

const (
	// TODO:
	// config bloat vs specificity; consider phasing out `timeout.ec_streams_time` and
	// using `timeout.shared_streams_time` instead
	SharedStreamsEver = -time.Second
	SharedStreamsDflt = 10 * time.Minute // NOTE: must be > hk.Prune2mIval
	SharedStreamsMin  = 5 * time.Minute
	SharedStreamsNack = max(SharedStreamsMin>>1, 3*time.Minute)

	LcacheEvictMin  = 20 * time.Minute
	LcacheEvictDflt = 2 * time.Hour
	LcacheEvictMax  = 16 * time.Hour

	ColdGetConflictDflt = 5 * time.Second
	ColdGetConflictMin  = time.Second

	// and a few more hardcoded below
)

func (c *TimeoutConf) Validate() error {
	debug.Assert(SharedStreamsDflt >= 2*hk.Prune2mIval)
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
	// must be greater than (2 * keepalive.interval*keepalive.factor)
	if c.EcStreams > 0 && c.EcStreams.D() < SharedStreamsMin {
		return fmt.Errorf("invalid timeout.ec_streams_time=%s (no timeout: %v; minimum: %s; default: %s)",
			c.EcStreams, SharedStreamsEver, SharedStreamsMin, SharedStreamsDflt)
	}
	if c.ObjectMD != 0 && (c.ObjectMD.D() < LcacheEvictMin || c.ObjectMD.D() > LcacheEvictMax) {
		return fmt.Errorf("invalid timeout.object_md=%s (expecting 0 (zero) for system default or [%v, %v] range)",
			c.ObjectMD, LcacheEvictMin, LcacheEvictMax)
	}
	if c.ColdGetConflict != 0 && (c.ColdGetConflict.D() < ColdGetConflictMin || c.ColdGetConflict > c.MaxHostBusy) {
		return fmt.Errorf("invalid timeout.cold_get_conflict=%s (expecting 0 (zero) for system default or [%v, max_host_busy=%v] range)",
			c.ColdGetConflict, ColdGetConflictMin, c.MaxHostBusy)
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
	return confDisabled
}

func (*ResilverConf) Validate() error { return nil }

func (c *ResilverConf) String() string {
	if c.Enabled {
		return "Enabled"
	}
	return confDisabled
}

/////////////////
// TracingConf //
/////////////////

const defaultSampleProbability = 1.0

func (c *TracingConf) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.ExporterEndpoint == "" {
		return errors.New("tracing.exporter_endpoint can't be empty when tracing enabled")
	}
	if c.SamplerProbabilityStr == "" {
		c.SamplerProbability = defaultSampleProbability
	} else {
		prob, err := strconv.ParseFloat(c.SamplerProbabilityStr, 64)
		if err != nil {
			return nil
		}
		c.SamplerProbability = prob
	}
	return nil
}

// only with `oteltracing` build tag
func (tac TraceExporterAuthConf) IsEnabled() bool {
	return tac.TokenFile != "" && tac.TokenHeader != ""
}

///////////////////
// RateLimitConf: adaptive (back) and bursty (front)
///////////////////

const (
	dfltRateIval      = time.Minute
	dfltRateMaxTokens = 1000
	dfltRateRetries   = 3
	dfltRateBurst     = dfltRateMaxTokens>>1 - dfltRateMaxTokens>>3
)

func (c *RateLimitConf) Validate() error {
	const tag = "rate limit"
	{
		c.Backend.NumRetries = cos.NonZero(c.Backend.NumRetries, dfltRateRetries)
		c.Backend.Interval = cos.Duration(cos.NonZero(c.Backend.Interval.D(), dfltRateIval))
		c.Backend.MaxTokens = cos.NonZero(c.Backend.MaxTokens, dfltRateMaxTokens)
	}
	{
		c.Frontend.Size = cos.NonZero(c.Frontend.Size, dfltRateBurst)
		c.Frontend.Interval = cos.Duration(cos.NonZero(c.Frontend.Interval.D(), dfltRateIval))
		c.Frontend.MaxTokens = cos.NonZero(c.Frontend.MaxTokens, dfltRateMaxTokens)
	}

	if err := c.interval(tag, "backend.interval", c.Backend.Interval.D()); err != nil {
		return err
	}
	if err := c.interval(tag, "frontend.interval", c.Frontend.Interval.D()); err != nil {
		return err
	}
	if err := c.tokens(tag, "backend.max_tokens", c.Backend.MaxTokens); err != nil {
		return err
	}
	if err := c.tokens(tag, "frontend.max_tokens", c.Frontend.MaxTokens); err != nil {
		return err
	}

	if c.Backend.NumRetries <= 0 || c.Backend.NumRetries > cos.DfltRateMaxRetries {
		return fmt.Errorf("%s: invalid backend.num_retries %d", tag, c.Backend.NumRetries)
	}
	if c.Frontend.Size <= 0 || c.Frontend.Size > c.Frontend.MaxTokens*cos.DfltRateMaxBurstPct/100 {
		return fmt.Errorf("%s: invalid frontend.burst_size %d (expecting positive integer <= (%d%% of maxTokens %d)",
			tag, c.Frontend.Size, cos.DfltRateMaxBurstPct, c.Frontend.MaxTokens)
	}

	//
	// optional, per-operation
	//
	const name = "per_op_max_tokens"
	if err := c.verbs(tag, "backend."+name, c.Backend.Verbs); err != nil {
		return err
	}
	return c.verbs(tag, "frontend."+name, c.Frontend.Verbs)
}

func (*RateLimitConf) interval(tag, name string, value time.Duration) error {
	if value < cos.DfltRateMinIval || value > cos.DfltRateMaxIval {
		return fmt.Errorf("%s: invalid %s %v (min=%v, max=%v)", tag, name, value, cos.DfltRateMinIval, cos.DfltRateMaxIval)
	}
	return nil
}

func (*RateLimitConf) tokens(tag, name string, value int) error {
	if value <= 0 || value >= math.MaxInt32 {
		return fmt.Errorf("%s: invalid %s %d", tag, name, value)
	}
	return nil
}

func (*RateLimitConf) verbs(tag, name, value string) error {
	if value == "" {
		return nil
	}
	lst := strings.Split(value, " ")
	for i, val := range lst {
		lst[i] = strings.TrimSpace(val)
	}
	for _, s := range lst {
		kv := strings.Split(strings.ToUpper(s), ":") // upper as in: http.MethodGet, et al.
		if len(kv) != 2 {
			return fmt.Errorf("%s: invalid format %s (number of items in '%v')", tag, name, kv)
		}

		// TODO: add and enforce verbs enum ("get", "put", "delete")

		tokens, err := strconv.Atoi(strings.TrimSpace(kv[1]))
		if err != nil {
			return fmt.Errorf("%s: invalid %s (number of tokens in '%v')", tag, name, kv)
		}
		if tokens <= 0 || tokens > math.MaxInt32 {
			return fmt.Errorf("%s: invalid %s (number of tokens out of range in '%v')", tag, name, kv)
		}
	}
	return nil
}

// NOTE: separately, frontend-rate-limiter validation in `makeNewBckProps`
func (c *RateLimitConf) ValidateAsProps(...any) error { return c.Validate() }

//////////////////
// GetBatchConf //
//////////////////

const (
	getBatchMaxWaitDflt = 30 * time.Second
	getBatchMaxWaitMin  = time.Second
	getBatchMaxWaitMax  = time.Minute

	numWarmupWorkersDisabled = -1
	numWarmupWorkersDflt     = 2
)

func (c *GetBatchConf) WarmupWorkers() int {
	return cos.Ternary(c.NumWarmupWorkers == numWarmupWorkersDisabled, 0, c.NumWarmupWorkers)
}

func (c *GetBatchConf) Validate() error {
	debug.Assert(numWarmupWorkersDisabled < 0 && getBatchMaxWaitDflt > getBatchMaxWaitMin && getBatchMaxWaitMin < getBatchMaxWaitMax)
	if c.MaxWait == 0 {
		c.MaxWait = cos.Duration(getBatchMaxWaitDflt)
	} else if c.MaxWait.D() < getBatchMaxWaitMin || c.MaxWait.D() > getBatchMaxWaitMax {
		return fmt.Errorf("invalid get_batch.max_wait=%s (must be in range [%v, %v])", c.MaxWait, getBatchMaxWaitMin, getBatchMaxWaitMax)
	}
	switch c.NumWarmupWorkers {
	case numWarmupWorkersDisabled:
	case 0:
		c.NumWarmupWorkers = numWarmupWorkersDflt
	default:
		if c.NumWarmupWorkers < 0 || c.NumWarmupWorkers > 10 {
			return fmt.Errorf("invalid get_batch.warmup_workers=%d (expecting range [%d, %d])", c.NumWarmupWorkers, 0, 10)
		}
	}
	return nil
}

//
// misc config utilities ---------------------------------------------------------
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
		return fmt.Errorf("failed to load plain-text local config %q: %v", localConfPath, err) // FATAL
	}

	nlog.SetPre(config.LogDir, daeRole)

	// Global (aka Cluster) config
	// Normally, when the node is being deployed the very first time the last updated version
	// of the config doesn't exist.
	// In this case, we load the initial plain-text global config from the command-line/environment
	// specified `globalConfPath`.
	// Once started, the node then always relies on the last updated version stored in a binary
	// form (in accordance with the associated ClusterConfig.JspOpts()).
	globalFpath := filepath.Join(config.ConfigDir, fname.GlobalConfig)
	if _, err := jsp.LoadMeta(globalFpath, &config.ClusterConfig); err != nil {
		if !cos.IsNotExist(err) {
			if _, ok := err.(*jsp.ErrUnsupportedMetaVersion); ok {
				cos.Errorf("ERROR: "+FmtErrBackwardCompat+"\n", err)
			}
			return fmt.Errorf("failed to load global config %q: %v", globalConfPath, err)
		}

		// initial plain-text
		const itxt = "load initial global config"
		_, err = jsp.Load(globalConfPath, &config.ClusterConfig, jsp.Plain())
		if err != nil {
			return fmt.Errorf("failed to %s %q: %v", itxt, globalConfPath, err)
		}
		if !config.TestingEnv() {
			cos.Errorln("Warning:", itxt, "from", globalConfPath)
		}
		debug.Assert(config.Version == 0, config.Version)
		globalFpath = globalConfPath
	} else {
		debug.Assert(config.Version > 0 && config.UUID != "")
	}

	nlog.SetPost(config.Log.ToStderr, int64(config.Log.MaxSize))

	// initialize atomic part of the config including most often used timeouts and features
	Rom.Set(&config.ClusterConfig)

	// read-only
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
		if cos.IsNotExist(err) {
			err = config.Validate() // always validate
		}
		return err
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
	return toUpdate, err
}

func ValidateRemAlias(alias string) (err error) {
	if alias == apc.QparamWhat {
		return fmt.Errorf("cannot use %q as an alias", apc.QparamWhat)
	}
	if len(alias) < 2 {
		err = fmt.Errorf(apc.RemAIS+" alias %q is too short: must have at least 2 letters", alias)
	} else {
		err = cos.CheckAlphaPlus(alias, apc.RemAIS+" alias")
	}
	return err
}
