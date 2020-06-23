// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

type (
	Runner interface {
		SetRunName(string)
		GetRunName() string
		Run() error
		Stop(error)
	}
	Named struct {
		name string
	}
)

func (r *Named) SetRunName(n string) { r.name = n }
func (r *Named) GetRunName() string  { return r.name }
