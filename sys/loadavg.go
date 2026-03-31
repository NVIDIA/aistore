// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

type (
	LoadAvg struct {
		One, Five, Fifteen float64
	}
	errLoadAvg struct {
		err error
	}
)
