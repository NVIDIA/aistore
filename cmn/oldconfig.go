// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

const oldMetaverConfig = 1

type (
	oldClusterConfig struct {
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
		// obsolete
		Replication ReplicationConf `json:"replication"`
	}
)

// interface guard
var _ jsp.Opts = (*oldClusterConfig)(nil)

var oldConfigJspOpts = jsp.CCSign(oldMetaverConfig)

func (*oldClusterConfig) JspOpts() jsp.Options { return oldConfigJspOpts }