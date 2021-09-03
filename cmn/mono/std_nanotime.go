//go:build !mono
// +build !mono

// Package mono provides low-level monotonic time
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package mono

import (
	"time"
)

func NanoTime() int64 { return time.Now().UnixNano() }
