// Package apc: API messages and constants
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
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

// timeouts for intra-cluster requests
const (
	DefaultTimeout = time.Duration(-1)
	LongTimeout    = time.Duration(-2)
)
