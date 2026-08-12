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

// Production bootstrap defaults for a generated cluster configuration.
//
// Before changing this list or any value below, read cmd/aisinit/README.md.
// In particular, do not duplicate defaults that aisnode reconstructs through
// default-omittable section hydration, and do not replace environment-specific
// production tuning with generic cmn defaults.
//
// Every section below is retained explicitly as production bootstrap policy:
// - non-omittable value sections whose intended settings cannot all be reconstructed from zero values
// or
// - settings whose correct values depend on the deployment environment.
var (
	defaultAuth = aiscmn.AuthConf{
		ClientAuthRequired: false,
	}

	defaultNet = aiscmn.NetConf{
		L4: aiscmn.L4Conf{
			Proto:         "tcp",
			SndRcvBufSize: 128 * cos.KiB, // socket send/receive buffers
		},
		HTTP: aiscmn.HTTPConf{
			UseHTTPS:            false,
			Chunked:             true,
			IdleConnTimeout:     cos.Duration(20 * time.Second),
			MaxIdleConnsPerHost: 128,
			MaxIdleConns:        4096,
		},
	}

	// Environment-specific: production nodes and developer machines require
	// different memory tuning.
	defaultMemsys = aiscmn.MemsysConf{
		MinFree:        cos.SizeIEC(8 * cos.GiB),
		DefaultBufSize: cos.SizeIEC(64 * cos.KiB),
		SizeToGC:       cos.SizeIEC(8 * cos.GiB),
		HousekeepTime:  cos.Duration(3 * time.Minute),
	}

	// Enabled-by-default bool sections cannot reconstruct their intended value
	// from an all-zero struct.
	defaultResilver = aiscmn.ResilverConf{
		Enabled: true,
	}
	defaultVersioning = aiscmn.VersionConf{
		Enabled:         true,
		ValidateWarmGet: false,
	}

	defaultTimeout = aiscmn.TimeoutConf{
		CplaneOperation: cos.Duration(2 * time.Second),
		MaxKeepalive:    cos.Duration(5 * time.Second),
		MaxHostBusy:     cos.Duration(20 * time.Second),
		Startup:         cos.Duration(time.Minute),
		JoinAtStartup:   cos.Duration(3 * time.Minute),
		SendFile:        cos.Duration(5 * time.Minute),
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
