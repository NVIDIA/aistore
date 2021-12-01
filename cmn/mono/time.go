// Package mono provides low-level monotonic time
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package mono

import (
	"time"
)

func Since(started int64) time.Duration { return time.Duration(NanoTime() - started) }
func SinceNano(started int64) int64     { return NanoTime() - started }
