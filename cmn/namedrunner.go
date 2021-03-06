// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

type (
	Runner interface {
		Name() string
		Run() error
		Stop(error)
	}
)
