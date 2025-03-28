//go:build !mono

// Package mono provides low-level monotonic time
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package mono

import (
	"time"
)

func NanoTime() int64 { return time.Now().UnixNano() }
