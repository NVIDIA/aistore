// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

/////////////////////////////////////////////////////////////////////////////
//                  (backward compatibility)                               //
// oldClusterConfig meta-version = 1 is present here for a single reason:  //
// to support upgrades to ClusterConfig meta-version = 2                   //
/////////////////////////////////////////////////////////////////////////////

const oldMetaverConfig = 1

type (
	oldClusterConfig struct {
		Backend     BackendConf     `json:"backend" allow:"cluster"`
		Mirror      MirrorConf      `json:"mirror" allow:"cluster"`
		EC          oldECConf       `json:"ec" allow:"cluster"` // <<< (changed)
		Log         LogConf         `json:"log"`
		Periodic    PeriodConf      `json:"periodic"`
		Timeout     TimeoutConf     `json:"timeout"`
		Client      oldClientConf   `json:"client"` // <<< (changed)
		Proxy       ProxyConf       `json:"proxy" allow:"cluster"`
		LRU         oldLRUConf      `json:"lru"`
		Disk        DiskConf        `json:"disk"`
		Rebalance   RebalanceConf   `json:"rebalance" allow:"cluster"`
		Resilver    ResilverConf    `json:"resilver"`
		Cksum       CksumConf       `json:"checksum"`
		Versioning  VersionConf     `json:"versioning" allow:"cluster"`
		Net         NetConf         `json:"net"`
		FSHC        FSHCConf        `json:"fshc"`
		Auth        AuthConf        `json:"auth"`
		Keepalive   KeepaliveConf   `json:"keepalivetracker"`
		Downloader  DownloaderConf  `json:"downloader"`
		DSort       DSortConf       `json:"distributed_sort"`
		Compression CompressionConf `json:"compression"`
		MDWrite     apc.WritePolicy `json:"md_write"` // <<< (changed)
		LastUpdated string          `json:"lastupdate_time"`
		UUID        string          `json:"uuid"`
		Version     int64           `json:"config_version,string"`
		Ext         interface{}     `json:"ext,omitempty"`
		Replication replicationConf `json:"replication"` // <<< (removed)
	}
	oldLRUConf struct {
		LowWM           int64        `json:"lowwm"`
		HighWM          int64        `json:"highwm"`
		OOS             int64        `json:"out_of_space"`
		DontEvictTime   cos.Duration `json:"dont_evict_time"`
		CapacityUpdTime cos.Duration `json:"capacity_upd_time"`
		Enabled         bool         `json:"enabled"`
	}
	oldECConf struct {
		ObjSizeLimit int64  `json:"objsize_limit"`
		Compression  string `json:"compression"`
		DataSlices   int    `json:"data_slices"`
		BatchSize    int    `json:"batch_size"`
		ParitySlices int    `json:"parity_slices"`
		Enabled      bool   `json:"enabled"`
		DiskOnly     bool   `json:"disk_only"`
	}
	oldClientConf struct {
		Timeout     cos.Duration `json:"client_timeout"`
		TimeoutLong cos.Duration `json:"client_long_timeout"`
		ListObjects cos.Duration `json:"list_timeout"`
		Features    feat.Flags   `json:"features,string"`
	}
	replicationConf struct {
		OnColdGet     bool `json:"on_cold_get"`
		OnPut         bool `json:"on_put"`
		OnLRUEviction bool `json:"on_lru_eviction"`
	}
)

// interface guard
var _ jsp.Opts = (*oldClusterConfig)(nil)

var oldConfigJspOpts = jsp.CCSign(oldMetaverConfig)

func (*oldClusterConfig) JspOpts() jsp.Options { return oldConfigJspOpts }
