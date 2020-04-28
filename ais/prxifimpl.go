// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"github.com/NVIDIA/aistore/cluster"
)

//
// implements cluster.Proxy interface
//

var _ cluster.Proxy = &proxyrunner{}
