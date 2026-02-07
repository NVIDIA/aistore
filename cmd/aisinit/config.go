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

	defaultCksum = aiscmn.CksumConf{
		Type:            cos.ChecksumCesXxh,
		ValidateColdGet: false,
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

	defaultXconf = aiscmn.XactConf{
		Compression: aisapc.CompressNever,
		SbundleMult: 2,
		Burst:       512,
	}
	defaultTCB  = aiscmn.TCBConf{XactConf: defaultXconf}
	defaultTCO  = aiscmn.TCOConf{XactConf: defaultXconf}
	defaultArch = aiscmn.ArchConf{XactConf: defaultXconf}

	defaultDisk = aiscmn.DiskConf{
		DiskUtilLowWM:    20,
		DiskUtilHighWM:   80,
		DiskUtilMaxWM:    95,
		IostatTimeLong:   cos.Duration(2 * time.Second),
		IostatTimeShort:  cos.Duration(100 * time.Millisecond),
		IostatTimeSmooth: cos.Duration(8 * time.Second),
	}

	defaultNet = aiscmn.NetConf{
		L4: aiscmn.L4Conf{
			Proto:         "tcp",
			SndRcvBufSize: 131072,
		},
		HTTP: aiscmn.HTTPConf{
			UseHTTPS:            false,
			Chunked:             true,
			IdleConnTimeout:     cos.Duration(30 * time.Second),
			MaxIdleConnsPerHost: 128,
			MaxIdleConns:        4096,
		},
	}

	defaultFSHC = aiscmn.FSHCConf{
		TestFileCount: 4,
		HardErrs:      2,
		IOErrs:        10,
		IOErrTime:     cos.Duration(10 * time.Second),
		Enabled:       true,
	}

	defaultDownloader = aiscmn.DownloaderConf{
		Timeout: cos.Duration(time.Hour),
	}

	defaultEC = aiscmn.ECConf{
		XactConf:     defaultXconf,
		Enabled:      false,
		ObjSizeLimit: 262144,
		DataSlices:   2,
		ParitySlices: 2,
	}

	defaultChunks = aiscmn.ChunksConf{
		ObjSizeLimit:      0,
		ChunkSize:         cos.SizeIEC(cos.GiB),
		MaxMonolithicSize: cos.SizeIEC(cos.TiB),
		CheckpointEvery:   0,
		Flags:             0,
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
		DontCleanupTime: cos.Duration(120 * time.Minute),
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

	defaultMirror = aiscmn.MirrorConf{
		Enabled: false,
		Copies:  2,
		Burst:   512,
	}

	defaultPeriodic = aiscmn.PeriodConf{
		StatsTime:     cos.Duration(10 * time.Second),
		NotifTime:     cos.Duration(30 * time.Second),
		RetrySyncTime: cos.Duration(2 * time.Second),
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

	defaultWritePolicy = aiscmn.WritePolicyConf{
		Data: "",
		MD:   "",
	}

	defaultRateLimit = aiscmn.RateLimitConf{
		Backend: aiscmn.Adaptive{
			RateLimitBase: aiscmn.RateLimitBase{
				Interval:  cos.Duration(time.Minute),
				MaxTokens: 1000,
				Enabled:   false,
			},
			NumRetries: 3,
		},
		Frontend: aiscmn.Bursty{
			RateLimitBase: aiscmn.RateLimitBase{
				Interval:  cos.Duration(time.Minute),
				MaxTokens: 1000,
				Enabled:   false,
			},
			Size: 375,
		},
	}

	defaultGetBatch = aiscmn.GetBatchConf{
		XactConf:         defaultXconf,
		MaxWait:          cos.Duration(10 * time.Second),
		NumWarmupWorkers: 1,
		MaxSoftErrs:      8,
		MaxGFN:           5,
	}
)

// [removed in 4.3] Dsort: defaultDsort
func newDefaultConfig() *aiscmn.ClusterConfig {
	return &aiscmn.ClusterConfig{
		Auth:        defaultAuth,
		Cksum:       defaultCksum,
		Client:      defaultClientConf,
		Transport:   defaultTransport,
		TCB:         defaultTCB,
		TCO:         defaultTCO,
		Arch:        defaultArch,
		Disk:        defaultDisk,
		Net:         defaultNet,
		FSHC:        defaultFSHC,
		Downloader:  defaultDownloader,
		EC:          defaultEC,
		Chunks:      defaultChunks,
		Keepalive:   defaultKeepalive,
		Log:         defaultLog,
		Space:       defaultSpace,
		Memsys:      defaultMemsys,
		LRU:         defaultLRU,
		Mirror:      defaultMirror,
		Periodic:    defaultPeriodic,
		Rebalance:   defaultRebalance,
		Resilver:    defaultResilver,
		Timeout:     defaultTimeout,
		Versioning:  defaultVersioning,
		WritePolicy: defaultWritePolicy,
		RateLimit:   defaultRateLimit,
		GetBatch:    defaultGetBatch,
	}
}
