// Package main contains logic for the aisinit container
/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"time"

	aiscmn "github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

var (
	defaultAuth = aiscmn.AuthConf{
		Enabled: false,
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

	defaultMemsys = aiscmn.MemsysConf{
		MinFree:        cos.SizeIEC(6 * cos.GiB),
		DefaultBufSize: cos.SizeIEC(64 * cos.KiB),
		SizeToGC:       cos.SizeIEC(6 * cos.GiB),
		HousekeepTime:  cos.Duration(120 * time.Second),
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
		Net:        defaultNet,
		Memsys:     defaultMemsys,
		Resilver:   defaultResilver,
		Timeout:    defaultTimeout,
		Versioning: defaultVersioning,
	}
}
