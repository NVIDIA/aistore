// Package main contains logic for the aisinit container
/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"time"

	aisapc "github.com/NVIDIA/aistore/api/apc"
	aiscmn "github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

var (
	defaultAuth = aiscmn.AuthConf{
		Enabled: false,
	}

	defaultClientConf = aiscmn.ClientConf{
		Timeout:        cos.Duration(10 * time.Second),
		TimeoutLong:    cos.Duration(5 * time.Minute),
		ListObjTimeout: cos.Duration(5 * time.Minute),
	}

	defaultTransport = aiscmn.TransportConf{
		MaxHeaderSize:    4096,
		Burst:            1024,
		IdleTeardown:     cos.Duration(4 * time.Second),
		QuiesceTime:      cos.Duration(10 * time.Second),
		LZ4BlockMaxSize:  cos.SizeIEC(256 * cos.KiB),
		LZ4FrameChecksum: false,
	}

	defaultNet = aiscmn.NetConf{
		L4: aiscmn.L4Conf{
			Proto:         "tcp",
			SndRcvBufSize: 131072,
		},
		HTTP: aiscmn.HTTPConf{
			UseHTTPS:               false,
			Chunked:                true,
			IdleConnTimeout:        cos.Duration(30 * time.Second),
			BackendIdleConnTimeout: cos.Duration(aiscmn.DefaultIdleConnTimeout), // default 6s; configurable since 5.0 w/ no upper limit
			MaxIdleConnsPerHost:    128,
			MaxIdleConns:           4096,
		},
	}

	defaultFSHC = aiscmn.FSHCConf{
		TestFileCount: 4,
		HardErrs:      2,
		IOErrs:        10,
		IOErrTime:     cos.Duration(10 * time.Second),
		Enabled:       true,
	}

	defaultKeepalive = aiscmn.KeepaliveConf{
		Proxy: aiscmn.KeepaliveTrackerConf{
			Interval: cos.Duration(10 * time.Second),
			Name:     "heartbeat",
			Factor:   3,
		},
		Target: aiscmn.KeepaliveTrackerConf{
			Interval: cos.Duration(10 * time.Second),
			Name:     "heartbeat",
			Factor:   3,
		},
		RetryFactor: 4,
		NumRetries:  3,
	}

	defaultLog = aiscmn.LogConf{
		Level:     "3",
		MaxSize:   cos.SizeIEC(64 * cos.MiB),
		MaxTotal:  cos.SizeIEC(512 * cos.MiB),
		FlushTime: cos.Duration(time.Minute),
		StatsTime: cos.Duration(3 * time.Minute),
	}

	defaultSpace = aiscmn.SpaceConf{
		CleanupWM:       65,
		LowWM:           75,
		HighWM:          90,
		OOS:             95,
		BatchSize:       32768,
		DontCleanupTime: cos.Duration(60 * time.Minute),
	}

	defaultMemsys = aiscmn.MemsysConf{
		MinFree:        cos.SizeIEC(6 * cos.GiB),
		DefaultBufSize: cos.SizeIEC(64 * cos.KiB),
		SizeToGC:       cos.SizeIEC(6 * cos.GiB),
		HousekeepTime:  cos.Duration(120 * time.Second),
	}

	defaultLRU = aiscmn.LRUConf{
		Enabled:         false,
		DontEvictTime:   cos.Duration(120 * time.Minute),
		CapacityUpdTime: cos.Duration(10 * time.Minute),
		BatchSize:       32768,
	}

	defaultRebalance = aiscmn.RebalanceConf{
		XactConf: aiscmn.XactConf{
			Compression: aisapc.CompressNever,
			SbundleMult: 2,
			Burst:       2048,
		},
		Enabled:       true,
		DestRetryTime: cos.Duration(2 * time.Minute),
	}

	defaultResilver = aiscmn.ResilverConf{
		Enabled: true,
	}

	defaultTimeout = aiscmn.TimeoutConf{
		CplaneOperation: cos.Duration(2 * time.Second),
		MaxKeepalive:    cos.Duration(5 * time.Second),
		MaxHostBusy:     cos.Duration(20 * time.Second),
		Startup:         cos.Duration(time.Minute),
		JoinAtStartup:   cos.Duration(3 * time.Minute),
		SendFile:        cos.Duration(5 * time.Minute),
		EcStreams:       cos.Duration(10 * time.Minute),
		ObjectMD:        cos.Duration(2 * time.Hour),
		ColdGetConflict: cos.Duration(5 * time.Second),
	}
	defaultVersioning = aiscmn.VersionConf{
		Enabled:         true,
		ValidateWarmGet: false,
	}
)

func newDefaultConfig() *aiscmn.ClusterConfig {
	return &aiscmn.ClusterConfig{
		Auth:       defaultAuth,
		Client:     defaultClientConf,
		Transport:  defaultTransport,
		Net:        defaultNet,
		FSHC:       defaultFSHC,
		Keepalive:  defaultKeepalive,
		Log:        defaultLog,
		Space:      defaultSpace,
		Memsys:     defaultMemsys,
		LRU:        defaultLRU,
		Rebalance:  defaultRebalance,
		Resilver:   defaultResilver,
		Timeout:    defaultTimeout,
		Versioning: defaultVersioning,
	}
}
