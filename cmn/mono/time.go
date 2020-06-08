// Package mono provides low-level monotonic time
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package mono

import (
	"time"
)

func Since(started int64) time.Duration { return time.Duration(NanoTime() - started) }
