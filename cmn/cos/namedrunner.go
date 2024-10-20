// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cos

type (
	Runner interface {
		Name() string
		Run() error
		Stop(error)
	}
)
