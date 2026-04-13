// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import "github.com/NVIDIA/aistore/cmn/cos"

// extend Tcdf with disk statistics
// TODO: apc.WhatDiskRWUtilCap => fs.TcdfExt: add msgpack encoding
type TcdfExt struct {
	cos.AllDiskStats
	Tcdf
}
