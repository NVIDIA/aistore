// Package cmn provides common low-level types and utilities for all aistore projects
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
