// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"time"
)

const (
	Proxy  = "proxy"
	Target = "target"
)

// deployment types
const (
	DeploymentK8s = "K8s"
	DeploymentDev = "dev"
)

// [convention]
// reset features flags, log modules, slices inside cluster config, and more
// (see also: docs/cli.md "Special keywords")
const ResetToken = "none"

// in re: "Slowloris Attack"
const (
	ReadHeaderTimeout    = 16 * time.Second
	EnvReadHeaderTimeout = "AIS_READ_HEADER_TIMEOUT"
)

// ulimits
const (
	UlimitProxy  = 16384
	UlimitTarget = 262144
)

// timeouts for intra-cluster requests
const (
	DefaultTimeout = time.Duration(-1)
	LongTimeout    = time.Duration(-2)
)

// locks
const (
	LockNone = iota
	LockRead
	LockWrite
)
