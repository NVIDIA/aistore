// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

// backward compatibility, to support ClusterConfig meta-v1 => v2 upgrades
// all changes are prefixed with `v1`
// NOTE: to be removed in 3.11

const v1MetaverConfig = 1

////////////////////
// cluster config //
////////////////////

type (
	v1ClusterConfig struct {
		Backend     BackendConf     `json:"backend" allow:"cluster"`
		Mirror      v1MirrorConf    `json:"mirror" allow:"cluster"`
		EC          v1ECConf        `json:"ec" allow:"cluster"`
		Log         v1LogConf       `json:"log"`
		Periodic    PeriodConf      `json:"periodic"`
		Timeout     v1TimeoutConf   `json:"timeout"`
		Client      v1ClientConf    `json:"client"`
		Proxy       ProxyConf       `json:"proxy" allow:"cluster"`
		LRU         v1LRUConf       `json:"lru"`
		Disk        DiskConf        `json:"disk"`
		Rebalance   v1RebalanceConf `json:"rebalance" allow:"cluster"`
		Resilver    ResilverConf    `json:"resilver"`
		Cksum       CksumConf       `json:"checksum"`
		Versioning  VersionConf     `json:"versioning" allow:"cluster"`
		Net         NetConf         `json:"net"`
		FSHC        FSHCConf        `json:"fshc"`
		Auth        AuthConf        `json:"auth"`
		Keepalive   v1KeepaliveConf `json:"keepalivetracker"`
		Downloader  DownloaderConf  `json:"downloader"`
		DSort       DSortConf       `json:"distributed_sort"`
		Compression v1CompressConf  `json:"compression"`
		MDWrite     apc.WritePolicy `json:"md_write"` // <<< (changed)
		LastUpdated string          `json:"lastupdate_time"`
		UUID        string          `json:"uuid"`
		Version     int64           `json:"config_version,string"`
		Ext         interface{}     `json:"ext,omitempty"`
		Replication replicationConf `json:"replication"` // <<< (removed)
	}
	v1MirrorConf struct {
		Copies      int64 `json:"copies"`
		UtilThresh  int64 `json:"util_thresh"`
		Burst       int   `json:"burst_buffer"`
		OptimizePUT bool  `json:"optimize_put"`
		Enabled     bool  `json:"enabled"`
	}
	v1ECConf struct {
		ObjSizeLimit int64  `json:"objsize_limit"`
		Compression  string `json:"compression"`
		DataSlices   int    `json:"data_slices"`
		BatchSize    int    `json:"batch_size"`
		ParitySlices int    `json:"parity_slices"`
		Enabled      bool   `json:"enabled"`
		DiskOnly     bool   `json:"disk_only"`
	}
	v1LogConf struct {
		Level    string `json:"level"`
		MaxSize  uint64 `json:"max_size"`
		MaxTotal uint64 `json:"max_total"`
	}
	v1TimeoutConf struct {
		CplaneOperation       cos.Duration `json:"cplane_operation"`
		MaxKeepalive          cos.Duration `json:"max_keepalive"`
		MaxHostBusy           cos.Duration `json:"max_host_busy"`
		Startup               cos.Duration `json:"startup_time"`
		SendFile              cos.Duration `json:"send_file_time"`
		TransportIdleTeardown cos.Duration `json:"transport_idle_term"`
	}
	v1ClientConf struct {
		Timeout     cos.Duration `json:"client_timeout"`
		TimeoutLong cos.Duration `json:"client_long_timeout"`
		ListObjects cos.Duration `json:"list_timeout"`
		Features    feat.Flags   `json:"features,string"`
	}
	v1LRUConf struct {
		LowWM           int64        `json:"lowwm"`
		HighWM          int64        `json:"highwm"`
		OOS             int64        `json:"out_of_space"`
		DontEvictTime   cos.Duration `json:"dont_evict_time"`
		CapacityUpdTime cos.Duration `json:"capacity_upd_time"`
		Enabled         bool         `json:"enabled"`
	}
	v1RebalanceConf struct {
		DestRetryTime cos.Duration `json:"dest_retry_time"`
		Quiesce       cos.Duration `json:"quiescent"`
		Compression   string       `json:"compression"`
		Multiplier    uint8        `json:"multiplier"`
		Enabled       bool         `json:"enabled"`
	}
	v1KeepaliveConf struct {
		Proxy         KeepaliveTrackerConf `json:"proxy"`
		Target        KeepaliveTrackerConf `json:"target"`
		RetryFactor   uint8                `json:"retry_factor"`
		TimeoutFactor uint8                `json:"timeout_factor"`
	}
	v1CompressConf struct {
		BlockMaxSize int  `json:"block_size"`
		Checksum     bool `json:"checksum"`
	}
	replicationConf struct {
		OnColdGet     bool `json:"on_cold_get"`
		OnPut         bool `json:"on_put"`
		OnLRUEviction bool `json:"on_lru_eviction"`
	}
)

// (compare w/ ais/v1bmd.go)
func loadClusterConfigV1(globalFpath string, config *Config) error {
	var old v1ClusterConfig
	if _, err := jsp.LoadMeta(globalFpath, &old); err != nil {
		return err
	}

	// iterate v1 source to copy same-name/same-type fields while taking special care
	// of assorted changes
	err := IterFields(&old, func(name string, fld IterField) (error, bool /*stop*/) {
		debug.Assert(name == "ext" || fld.Value() != nil)
		switch {
		case name == "md_write":
			v, ok := fld.Value().(apc.WritePolicy)
			debug.Assert(ok)
			config.ClusterConfig.WritePolicy.MD = v
			return nil, false
		case name == "client.features":
			v, ok := fld.Value().(feat.Flags)
			debug.Assert(ok)
			config.ClusterConfig.Features = v
			return nil, false
		case strings.HasPrefix(name, "replication."):
			return nil, false
		case name == "ec.batch_size", name == "mirror.optimize_put", name == "mirror.util_thresh":
			return nil, false
		case name == "lru.lowwm":
			v, ok := fld.Value().(int64)
			debug.Assert(ok)
			config.ClusterConfig.Space.LowWM = v
			return nil, false
		case name == "lru.highwm":
			v, ok := fld.Value().(int64)
			debug.Assert(ok)
			config.ClusterConfig.Space.HighWM = v
			return nil, false
		case name == "lru.out_of_space":
			v, ok := fld.Value().(int64)
			debug.Assert(ok)
			config.ClusterConfig.Space.OOS = v
			return nil, false
		case name == "timeout.transport_idle_term":
			v, ok := fld.Value().(cos.Duration)
			debug.Assert(ok)
			config.ClusterConfig.Transport.IdleTeardown = v
			return nil, false
		case name == "rebalance.quiescent":
			v, ok := fld.Value().(cos.Duration)
			debug.Assert(ok)
			config.ClusterConfig.Transport.QuiesceTime = v
			return nil, false
		case name == "rebalance.multiplier":
			v, ok := fld.Value().(uint8)
			debug.Assert(ok)
			config.ClusterConfig.Rebalance.SbundleMult = int(v)
			return nil, false
		case name == "compression.block_size":
			v, ok := fld.Value().(int)
			debug.Assert(ok)
			config.ClusterConfig.Transport.LZ4BlockMaxSize = cos.Size(v)
			return nil, false
		case name == "compression.checksum":
			v, ok := fld.Value().(bool)
			debug.Assert(ok)
			config.ClusterConfig.Transport.LZ4FrameChecksum = v
			return nil, false
		case name == "keepalivetracker.timeout_factor":
			return nil, false
		case name == "log.max_size":
			v, ok := fld.Value().(uint64)
			debug.Assert(ok)
			config.ClusterConfig.Log.MaxSize = cos.Size(v)
			return nil, false
		case name == "log.max_total":
			v, ok := fld.Value().(uint64)
			debug.Assert(ok)
			config.ClusterConfig.Log.MaxTotal = cos.Size(v)
			return nil, false
		}

		// copy dst = fld.Value()
		return UpdateFieldValue(&config.ClusterConfig, name, fld.Value()), false /*stop*/
	}, IterOpts{OnlyRead: true})

	config.ClusterConfig.Space.CleanupWM = 65
	return err
}

/////////////////////////////////////
// bucket props (see ais/v1bmd.go) //
/////////////////////////////////////

type (
	V1BucketProps struct {
		Provider   string          `json:"provider" list:"readonly"`
		BackendBck Bck             `json:"backend_bck,omitempty"`
		Versioning VersionConf     `json:"versioning"`
		Cksum      CksumConf       `json:"checksum"`
		LRU        v1LRUConf       `json:"lru"`
		Mirror     v1MirrorConf    `json:"mirror"`
		MDWrite    apc.WritePolicy `json:"md_write"`
		EC         v1ECConf        `json:"ec"`
		Access     apc.AccessAttrs `json:"access,string"`
		Extra      ExtraProps      `json:"extra,omitempty" list:"omitempty"`
		BID        uint64          `json:"bid,string" list:"omit"`
		Created    int64           `json:"created,string" list:"readonly"`
		Renamed    string          `list:"omit"`
	}
)

// interface guard
var _ jsp.Opts = (*v1ClusterConfig)(nil)

var v1ConfigJspOpts = jsp.CCSign(v1MetaverConfig)

func (*v1ClusterConfig) JspOpts() jsp.Options { return v1ConfigJspOpts }
