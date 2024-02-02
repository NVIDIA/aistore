// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
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

// in re: "Slowloris Attack"
const (
	ReadHeaderTimeout    = 16 * time.Second
	EnvReadHeaderTimeout = "AIS_READ_HEADER_TIMEOUT"
)

// timeouts for intra-cluster requests
const (
	DefaultTimeout = time.Duration(-1)
	LongTimeout    = time.Duration(-2)
)
